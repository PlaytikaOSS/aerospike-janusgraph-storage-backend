package com.playtika.janusgraph.aerospike.wal;

import com.aerospike.client.*;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.playtika.janusgraph.aerospike.AerospikeStoreManager;
import org.janusgraph.diskstorage.BackendException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.playtika.janusgraph.aerospike.util.AsyncUtil.shutdownAndAwaitTermination;
import static java.time.temporal.ChronoUnit.SECONDS;

/**
 * Completes hanged transactions
 */
public class WriteAheadLogCompleter {

    private static Logger logger = LoggerFactory.getLogger(WriteAheadLogCompleter.class);

    private static final Instant JAN_01_2010 = Instant.parse("2010-01-01T00:00:00.00Z");

    private static final Value EXCLUSIVE_LOCK_KEY = Value.get((byte)0);
    private static final Bin EXCLUSIVE_LOCK_BIN = new Bin("EL", true);

    private final IAerospikeClient client;
    private final WriteAheadLogManager writeAheadLogManager;
    private final long periodInMs;
    private final AerospikeStoreManager aerospikeStoreManager;
    private final WritePolicy putLockPolicy;
    private final Key exclusiveLockKey;

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    public WriteAheadLogCompleter(IAerospikeClient aerospikeClient, String walNamespace, String walSetName,
                           WriteAheadLogManager writeAheadLogManager, long periodInMs,
                           AerospikeStoreManager aerospikeStoreManager){

        this.client = aerospikeClient;
        this.writeAheadLogManager = writeAheadLogManager;
        this.aerospikeStoreManager = aerospikeStoreManager;

        putLockPolicy = new WritePolicy();
        putLockPolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
        putLockPolicy.expiration = (int)Duration.ofMillis(periodInMs).get(SECONDS);
        if(putLockPolicy.expiration < 1){
            throw new IllegalArgumentException("Wrong expiration for WAL lock: "+putLockPolicy.expiration);
        }
        //set period to by slightly longer then expiration
        this.periodInMs = Duration.ofSeconds(putLockPolicy.expiration + 1).toMillis();
        exclusiveLockKey = new Key(walNamespace, walSetName, EXCLUSIVE_LOCK_KEY);
    }

    public void start(){
        scheduledExecutorService.scheduleAtFixedRate(
                this::completeHangedTransactions,
                0, periodInMs, TimeUnit.MILLISECONDS);
    }

    public void shutdown(){
        shutdownAndAwaitTermination(scheduledExecutorService);
    }

    private void completeHangedTransactions(){
        try {
            if(acquireExclusiveLock()){
                List<WriteAheadLogManager.WalTransaction> staleTransactions = writeAheadLogManager.getStaleTransactions();
                logger.info("Got {} stale transactions", staleTransactions.size());
                for(WriteAheadLogManager.WalTransaction transaction : staleTransactions){
                    logger.info("Trying to complete transaction id={}, timestamp={}",
                            transaction.transactionId, transaction.timestamp);
                    try {
                        aerospikeStoreManager.processAndDeleteTransaction(
                                transaction.transactionId, transaction.locks, transaction.mutations, true);
                        logger.info("Successfully complete transaction id={}", transaction.transactionId);
                    }

                    //this is expected behaviour that may have place in case of transaction was interrupted:
                    // - on 'release locks' stage then transaction will fail and just need to release hanged locks
                    // - on 'delete wal transaction' stage and just need to remove transaction
                    catch (BackendException be) {
                        logger.info("Failed to complete transaction id={}",
                                transaction.transactionId, be);
                    }
                    //even in case of error need to move to the next one
                    catch (Exception e){
                        logger.error("!!! Failed to complete transaction id={}, need to be investigated",
                                transaction.transactionId, e);
                    }
                }
            }
        } catch (Throwable t) {
            logger.error("Error while running completeHangedTransactions()", t);
            throw t;
        }
    }

    private boolean acquireExclusiveLock(){
        try {
            client.add(putLockPolicy, exclusiveLockKey, EXCLUSIVE_LOCK_BIN);
            logger.info("Successfully got exclusive lock, will check for hanged transactions");
            return true;
        } catch (AerospikeException e){
            if(e.getResultCode() == ResultCode.KEY_EXISTS_ERROR){
                logger.debug("Failed to get exclusive lock, will try later");
                int expiration = client.get(null, exclusiveLockKey).expiration;
                logger.debug("lock will be released at {}", JAN_01_2010.plus(expiration, SECONDS));
                return false;
            } else {
                logger.error("Failed while getting exclusive lock", e);
                throw e;
            }
        }
    }
}
