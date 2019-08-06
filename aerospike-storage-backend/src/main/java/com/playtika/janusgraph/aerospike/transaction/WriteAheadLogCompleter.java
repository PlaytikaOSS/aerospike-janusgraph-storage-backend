package com.playtika.janusgraph.aerospike.transaction;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.playtika.janusgraph.aerospike.util.NamedThreadFactory;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.locking.PermanentLockingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static com.playtika.janusgraph.aerospike.operations.BasicOperations.JANUS_AEROSPIKE_THREAD_GROUP_NAME;
import static com.playtika.janusgraph.aerospike.transaction.WriteAheadLogManagerBasic.getBytesFromUUID;
import static com.playtika.janusgraph.aerospike.util.AsyncUtil.WAIT_TIMEOUT_IN_SECONDS;
import static java.time.temporal.ChronoUnit.SECONDS;

/**
 * Completes hanged transactions
 */
public class WriteAheadLogCompleter {

    private static Logger logger = LoggerFactory.getLogger(WriteAheadLogCompleter.class);

    private static final Instant JAN_01_2010 = Instant.parse("2010-01-01T00:00:00.00Z");

    private static final Value EXCLUSIVE_LOCK_KEY = Value.get((byte)0);

    private final IAerospikeClient client;
    private final WriteAheadLogManager writeAheadLogManager;
    private final long periodInMs;
    private final TransactionalOperations transactionalOperations;
    private final WritePolicy putLockPolicy;
    private final Key exclusiveLockKey;
    private final Bin exclusiveLockBin;
    private int generation = 0;

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
            new NamedThreadFactory(JANUS_AEROSPIKE_THREAD_GROUP_NAME, "wal")
    );

    public WriteAheadLogCompleter(WalOperations walOperations, TransactionalOperations transactionalOperations){
        this.client = walOperations.getAerospikeOperations().getClient();
        this.writeAheadLogManager = transactionalOperations.getWriteAheadLogManager();
        this.transactionalOperations = transactionalOperations;

        this.putLockPolicy = buildPutLockPolicy(walOperations.getStaleTransactionLifetimeThresholdInMs());

        this.exclusiveLockBin = new Bin("EL", getBytesFromUUID(UUID.randomUUID()));

        //set period to by slightly longer then expiration
        this.periodInMs = Duration.ofSeconds(putLockPolicy.expiration + 1).toMillis();
        exclusiveLockKey = new Key(walOperations.getWalNamespace(), walOperations.getWalSetName(), EXCLUSIVE_LOCK_KEY);
    }

    public void start(){
        scheduledExecutorService.scheduleAtFixedRate(
                this::completeHangedTransactions,
                0, periodInMs, TimeUnit.MILLISECONDS);
    }

    public void shutdown(){
        shutdownAndAwaitTermination(scheduledExecutorService, WAIT_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);
    }

    private void completeHangedTransactions() {
        try {
            if(acquireExclusiveLock()){
                List<WriteAheadLogManagerBasic.WalTransaction> staleTransactions = writeAheadLogManager.getStaleTransactions();
                logger.info("Got {} stale transactions", staleTransactions.size());
                for(WriteAheadLogManagerBasic.WalTransaction transaction : staleTransactions){
                    if(Thread.currentThread().isInterrupted()){
                        logger.info("WAL execution was interrupted");
                        break;
                    }

                    if(renewExclusiveLock()) {
                        logger.info("Trying to complete transaction id={}, timestamp={}",
                                transaction.transactionId, transaction.timestamp);
                        try {
                            transactionalOperations.processAndDeleteTransaction(
                                    transaction.transactionId, transaction.locks, transaction.mutations, true);
                            logger.info("Successfully complete transaction id={}", transaction.transactionId);
                        }
                        //this is expected behaviour that may have place in case of transaction was interrupted:
                        // - on 'release locks' stage then transaction completion will fail and just need to release hanged locks
                        // - on 'delete wal transaction' stage and just need to remove transaction
                        catch (PermanentLockingException be) {
                            logger.info("Failed to complete transaction id={} as it's already completed", transaction.transactionId, be);
                            transactionalOperations.releaseLocksAndDeleteWalTransactionOnError(
                                    transaction.locks, transaction.transactionId);
                            logger.info("released locks for transaction id={}", transaction.transactionId, be);
                        }
                        //even in case of error need to move to the next one
                        catch (Exception e) {
                            logger.error("!!! Failed to complete transaction id={}, need to be investigated",
                                    transaction.transactionId, e);
                        }
                    }
                }
            }
        }
        catch (BackendException t) {
            logger.error("Error while running completeHangedTransactions()", t);
            throw new RuntimeException(t);
        }
        catch (Throwable t) {
            logger.error("Error while running completeHangedTransactions()", t);
            throw t;
        }
    }

    private boolean acquireExclusiveLock(){
        try {
            client.add(putLockPolicy, exclusiveLockKey, exclusiveLockBin);
            generation++;
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

    private WritePolicy buildPutLockPolicy(long expirationInMs){
        WritePolicy putLockPolicy = new WritePolicy();
        putLockPolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
        putLockPolicy.expiration = (int)Duration.ofMillis(expirationInMs).get(SECONDS);
        if(putLockPolicy.expiration < 1){
            throw new IllegalArgumentException("Wrong expiration for WAL lock: "+putLockPolicy.expiration);
        }
        return putLockPolicy;
    }

    private boolean renewExclusiveLock(){
        try {
            client.touch(buildTouchLockPolicy(putLockPolicy.expiration, generation++), exclusiveLockKey);
            logger.info("Successfully renewed exclusive lock, will process transaction");
            return true;
        } catch (AerospikeException e){
            logger.error("Failed while renew exclusive lock", e);
            throw e;
        }
    }

    private WritePolicy buildTouchLockPolicy(int expiration, int generation){
        WritePolicy touchLockPolicy = new WritePolicy();
        touchLockPolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        touchLockPolicy.generation = generation;
        touchLockPolicy.expiration = expiration;
        if(touchLockPolicy.expiration < 1){
            throw new IllegalArgumentException("Wrong expiration for WAL lock: "+touchLockPolicy.expiration);
        }
        return touchLockPolicy;
    }
}
