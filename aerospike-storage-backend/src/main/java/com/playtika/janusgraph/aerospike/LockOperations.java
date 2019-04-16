package com.playtika.janusgraph.aerospike;

import com.aerospike.client.*;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.reactor.AerospikeReactorClient;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.locking.PermanentLockingException;
import org.janusgraph.diskstorage.locking.TemporaryLockingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.playtika.janusgraph.aerospike.AerospikeKeyColumnValueStore.ENTRIES_BIN_NAME;
import static com.playtika.janusgraph.aerospike.LockOperations.LockType.LOCKED;
import static com.playtika.janusgraph.aerospike.LockOperations.LockType.SAME_TRANSACTION;
import static com.playtika.janusgraph.aerospike.util.AsyncUtil.completeAll;
import static java.util.concurrent.CompletableFuture.runAsync;

class LockOperations {

    private static Logger logger = LoggerFactory.getLogger(LockOperations.class);

    private static final String TRANSACTION_BIN_NAME = "transaction";

    private static final WritePolicy checkValuesPolicy = new WritePolicy();
    static {
        checkValuesPolicy.respondAllOps = true;
    }

    private final String namespace;
    private final AerospikeReactorClient reactorClient;
    private String graphPrefix;
    private final Executor aerospikeExecutor;
    private final WritePolicy putLockPolicy;

    LockOperations(AerospikeReactorClient reactorClient,
                   String namespace, String graphPrefix,
                   Executor aerospikeExecutor) {
        this.namespace = namespace;
        this.reactorClient = reactorClient;
        this.graphPrefix = graphPrefix + ".";
        this.aerospikeExecutor = aerospikeExecutor;

        putLockPolicy = new WritePolicy();
        putLockPolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
        putLockPolicy.expiration = 0;  //never expire
    }

    Set<Key> acquireLocks(Value transactionId, Map<String, Map<Value, Map<Value, Value>>> locksByStore,
                          boolean checkTransactionId) throws BackendException {
        Map<Key, LockType> keysLocked = putLocks(transactionId, locksByStore, checkTransactionId);
        try {
            checkExpectedValues(locksByStore, keysLocked);
        } catch (Throwable t){
            releaseLocks(keysLocked.keySet());
            throw t;
        }
        return keysLocked.keySet();
    }

    private Mono<Map<Key, LockType>> putLocks(Value transactionId, Map<String, Map<Value, Map<Value, Value>>> locksByStore,
                              boolean checkTransactionId) throws BackendException {

        return Flux.fromIterable(locksByStore.entrySet())
                .flatMap(locksForStore -> {
                    String storeName = locksForStore.getKey();
                    return Flux.fromIterable(locksForStore.getValue().keySet())
                            .flatMap(key -> {
                                Key lockKey = getLockKey(storeName, key);
                                return putLock(transactionId, lockKey, checkTransactionId)
                                        .map(lockType -> new AbstractMap.SimpleEntry<>(lockKey, lockType));

                            });
                })
                .collectMap(entry -> entry.getKey(), entry -> entry.getValue());

        Map<Key, LockType> keysLocked = new ConcurrentHashMap<>();
        List<CompletableFuture<?>> futures = new ArrayList<>();
        AtomicBoolean alreadyLocked = new AtomicBoolean(false);

        try {
            for (Map.Entry<String, Map<Value, Map<Value, Value>>> locksForStore : locksByStore.entrySet()) {
                String storeName = locksForStore.getKey();
                for (Value key : locksForStore.getValue().keySet()) {
                    futures.add(runAsync(() -> {
                        if (alreadyLocked.get()) {
                            return;
                        }
                        Key lockKey = ;
                        try {
                            LockType lockType = putLock(transactionId, lockKey, checkTransactionId);
                            keysLocked.put(lockKey, lockType);

                            if (logger.isTraceEnabled()) {
                                logger.trace("acquired lock key: {}, txId:{}", lockKey, transactionId);
                            }
                        } catch (AerospikeException e) {
                            if (e.getResultCode() == ResultCode.KEY_EXISTS_ERROR) {
                                alreadyLocked.set(true);
                                logger.info("already locked key: {}, txId:{}", lockKey, transactionId);
                            } else {
                                throw e;
                            }
                        }
                    }, aerospikeExecutor));
                }
            }

            completeAll(futures);

            if (alreadyLocked.get()) {
                throw new TemporaryLockingException("Some locks not released yet");
            }
        } catch (Throwable t) {
            releaseLocks(keysLocked.keySet());
            throw t;
        }

        return keysLocked;
    }

    enum LockType {
        LOCKED,
        SAME_TRANSACTION
    }

    private Mono<LockType> putLock(Value transactionId, Key lockKey, boolean checkTransactionId) {
        Mono<LockType> lockTypeMono = reactorClient.add(putLockPolicy, lockKey, new Bin(TRANSACTION_BIN_NAME, transactionId))
                .map(key -> LOCKED);
        if(checkTransactionId){
            return lockTypeMono
                    .onErrorResume(e -> e instanceof AerospikeException
                                    && ((AerospikeException) e).getResultCode() == ResultCode.KEY_EXISTS_ERROR,
                            e -> reactorClient.get(null, lockKey)
                                    .handle((keyRecord, sink) -> {
                                        //this is used only by WriteAheadLogCompleter to skip already locked keys
                                        //check for same transaction
                                        Value transactionIdLocked = Value.get(keyRecord.record.getValue(TRANSACTION_BIN_NAME));
                                        if(transactionId.equals(transactionIdLocked)){
                                            sink.next(SAME_TRANSACTION);
                                        } else {
                                            logger.info("already locked key: {}, txId:{}", lockKey, transactionId);
                                            sink.error(new TemporaryLockingException("Lock not released yet for key="+lockKey));
                                        }
                                    }));
        } else {
            return lockTypeMono;
        }
    }

    Flux<Key> releaseLocks(Collection<Key> keys) {
        return Flux.fromIterable(keys).flatMap(reactorClient::delete);
    }

    private Mono<Void> checkExpectedValues(final Map<String, Map<Value, Map<Value, Value>>> locksByStore,
                                     final Map<Key, LockType> keysLocked) {
        return Flux.fromIterable(locksByStore.entrySet())
                .flatMap(locksForStore -> {
                    String storeName = locksForStore.getKey();
                    return Flux.fromIterable(locksForStore.getValue().entrySet())
                            .filter(locksForKey -> keysLocked.get(getLockKey(storeName, locksForKey.getKey())) != SAME_TRANSACTION)
                            .flatMap(locksForKey -> checkColumnValues(getKey(storeName, locksForKey.getKey()), locksForKey.getValue()));
                }).then();
    }


    private Mono<Void> checkColumnValues(final Key key, final Map<Value, Value> locksForKey) {
        if(locksForKey.isEmpty()){
            return Mono.empty();
        }

        int columnsNo = locksForKey.size();
        Value[] columns = new Value[columnsNo];
        Operation[] operations = new Operation[columnsNo];
        int i = 0;
        for(Value column : locksForKey.keySet()){
            columns[i] = column;
            operations[i] = MapOperation.getByKey(ENTRIES_BIN_NAME, column, MapReturnType.VALUE);
            i++;
        }
        return reactorClient.operate(checkValuesPolicy, key, operations)
                .map(keyRecord -> {
                    if(columnsNo > 1){
                        List<?> resultList = keyRecord.record.getList(ENTRIES_BIN_NAME);
                        for(int j = 0, n = resultList.size(); j < n; j++){
                            Value column = columns[j];
                            checkValue(key, column, locksForKey.get(column), (byte[])resultList.get(j));
                        }
                    }
                    else { //columnsNo == 1
                        byte[] actualValueData = (byte[])keyRecord.record.getValue(ENTRIES_BIN_NAME);
                        Value column = columns[0];
                        checkValue(key, column, locksForKey.get(column), actualValueData);
                    }
                    return true;
                })
                .then();
    }

    private void checkValue(Key key, Value column, Value expectedValue, byte[] actualValue) {
        if(!expectedValue.equals(Value.get(actualValue))){
            logger.info("Unexpected value for key {}, column {}, expected {}, actual {}", key, column, expectedValue, actualValue);
            throw Exceptions.propagate(new PermanentLockingException(
                    String.format("Unexpected value for key %s, column %s, expected %s, actual %s", key, column, expectedValue, actualValue)
            ));
        }
    }

    private Key getLockKey(String storeName, Value value) {
        return new Key(namespace, getSetName(storeName) + ".lock", value);
    }

    private Key getKey(String storeName, Value value) {
        return new Key(namespace, getSetName(storeName), value);
    }

    protected String getSetName(String storeName) {
        return graphPrefix + storeName;
    }
}
