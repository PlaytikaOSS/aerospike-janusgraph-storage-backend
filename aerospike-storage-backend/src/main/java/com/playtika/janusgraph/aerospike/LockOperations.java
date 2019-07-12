package com.playtika.janusgraph.aerospike;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.locking.PermanentLockingException;
import org.janusgraph.diskstorage.locking.TemporaryLockingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
    private final IAerospikeClient client;
    private String graphPrefix;
    private final Executor aerospikeExecutor;
    private final WritePolicy putLockPolicy;
    private final WritePolicy deleteLockPolicy;

    LockOperations(IAerospikeClient client,
                   String namespace, String graphPrefix,
                   Executor aerospikeExecutor,
                   AerospikePolicyProvider aerospikePolicyProvider) {
        this.namespace = namespace;
        this.client = client;
        this.graphPrefix = graphPrefix + ".";
        this.aerospikeExecutor = aerospikeExecutor;

        putLockPolicy = new WritePolicy(aerospikePolicyProvider.writePolicy());
        putLockPolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;

        deleteLockPolicy = aerospikePolicyProvider.deletePolicy();
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

    private Map<Key, LockType> putLocks(Value transactionId, Map<String, Map<Value, Map<Value, Value>>> locksByStore,
                              boolean checkTransactionId) throws BackendException {

        Map<Key, LockType> keysLocked = new ConcurrentHashMap<>();
        List<CompletableFuture<?>> futures = new ArrayList<>();
        AtomicReference<AerospikeException> alreadyLockedError = new AtomicReference<>();

        try {
            for (Map.Entry<String, Map<Value, Map<Value, Value>>> locksForStore : locksByStore.entrySet()) {
                String storeName = locksForStore.getKey();
                for (Value key : locksForStore.getValue().keySet()) {
                    futures.add(runAsync(() -> {
                        if (alreadyLockedError.get() != null) {
                            return;
                        }
                        Key lockKey = getLockKey(storeName, key);
                        try {
                            LockType lockType = putLock(transactionId, lockKey, checkTransactionId);
                            keysLocked.put(lockKey, lockType);

                            if (logger.isTraceEnabled()) {
                                logger.trace("acquired lock key: {}, txId:{}", lockKey, transactionId);
                            }
                        } catch (AerospikeException e) {
                            if (e.getResultCode() == ResultCode.KEY_EXISTS_ERROR) {
                                alreadyLockedError.set(e);
                                logger.info("already locked key: {}, txId:{}", lockKey, transactionId);
                            } else {
                                throw e;
                            }
                        }
                    }, aerospikeExecutor));
                }
            }

            completeAll(futures);

            if (alreadyLockedError.get() != null) {
                throw new TemporaryLockingException("Some locks not released yet", alreadyLockedError.get());
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

    private LockType putLock(Value transactionId, Key lockKey, boolean checkTransactionId) {
        //this is used only by WriteAheadLogCompleter to skip already locked keys
        if(checkTransactionId){
            try {
                client.add(putLockPolicy, lockKey, new Bin(TRANSACTION_BIN_NAME, transactionId));
                return LOCKED;
            } catch (AerospikeException e) {
                //check for same transaction
                if (e.getResultCode() == ResultCode.KEY_EXISTS_ERROR) {
                    Record record = client.get(null, lockKey);
                    if (checkTransaction(record, transactionId)) return SAME_TRANSACTION;
                }
                throw e;
            }
        } else {
            client.add(putLockPolicy, lockKey, new Bin(TRANSACTION_BIN_NAME, transactionId));
            return LOCKED;
        }
    }

    private boolean checkTransaction(Record record, Value transactionId) {
        Value transactionIdLocked = Value.get(record.getValue(TRANSACTION_BIN_NAME));
        return transactionId.equals(transactionIdLocked);
    }

    public List<Key> filterKeysLockedByTransaction(Collection<Key> keys, Value transactionId){
        List<Key> keysFiltered = new ArrayList<>(keys.size());
        Key[] keysArray = keys.toArray(new Key[0]);
        Record[] records = client.get(null, keysArray);
        for(int i = 0, m = keysArray.length; i < m; i++){
            Record record = records[i];
            if(record != null && checkTransaction(record, transactionId)){
                keysFiltered.add(keysArray[i]);
            }
        }
        return keysFiltered;
    }

    void releaseLocks(Collection<Key> keys) throws BackendException {
        List<CompletableFuture<?>> futures = new ArrayList<>(keys.size());
        for(Key lockKey : keys){
            futures.add(runAsync(() -> client.delete(deleteLockPolicy, lockKey), aerospikeExecutor));
        }
        completeAll(futures);
    }

    public List<Key> getLockKeys(Map<String, Map<Value, Map<Value, Value>>> locksByStore){
        List<Key> keys = new ArrayList<>();
        for (Map.Entry<String, Map<Value, Map<Value, Value>>> locksForStore : locksByStore.entrySet()) {
            String storeName = locksForStore.getKey();
            for (Value key : locksForStore.getValue().keySet()) {
                keys.add(getLockKey(storeName, key));
            }
        }
        return keys;
    }

    private void checkExpectedValues(final Map<String, Map<Value, Map<Value, Value>>> locksByStore,
                                     final Map<Key, LockType> keysLocked) throws PermanentBackendException {
        List<CompletableFuture<?>> futures = new ArrayList<>();
        AtomicBoolean checkFailed = new AtomicBoolean(false);

        for (Map.Entry<String, Map<Value, Map<Value, Value>>> locksForStore : locksByStore.entrySet()) {
            String storeName = locksForStore.getKey();
            for (Map.Entry<Value, Map<Value, Value>> locksForKey : locksForStore.getValue().entrySet()) {
                if(keysLocked.get(getLockKey(storeName, locksForKey.getKey())) == SAME_TRANSACTION){
                    continue;
                }
                futures.add(runAsync(() -> {
                    if(checkFailed.get()){
                        return;
                    }
                    if(!checkColumnValues(getKey(storeName, locksForKey.getKey()), locksForKey.getValue())){
                        checkFailed.set(true);
                    }
                }, aerospikeExecutor));
            }
        }

        completeAll(futures);

        if (checkFailed.get()) {
            throw new PermanentLockingException("Some values don't match expected values");
        }
    }


    private boolean checkColumnValues(final Key key, final Map<Value, Value> locksForKey) {
        if(locksForKey.isEmpty()){
            return true;
        }

        try {
            int columnsNo = locksForKey.size();
            Value[] columns = new Value[columnsNo];
            Operation[] operations = new Operation[columnsNo];
            int i = 0;
            for (Value column : locksForKey.keySet()) {
                columns[i] = column;
                operations[i] = MapOperation.getByKey(ENTRIES_BIN_NAME, column, MapReturnType.VALUE);
                i++;
            }
            Record record = client.operate(checkValuesPolicy, key, operations);
            if (record != null) {
                if (columnsNo > 1) {
                    List<?> resultList;
                    if ((resultList = record.getList(ENTRIES_BIN_NAME)) != null) {
                        for (int j = 0, n = resultList.size(); j < n; j++) {
                            Value column = columns[j];
                            if (!checkValue(key, column, locksForKey.get(column), (byte[]) resultList.get(j))) {
                                return false;
                            }
                        }
                    }
                } else if (columnsNo == 1) {
                    byte[] actualValueData = (byte[]) record.getValue(ENTRIES_BIN_NAME);
                    Value column = columns[0];
                    return checkValue(key, column, locksForKey.get(column), actualValueData);
                }
            }
            else {
                return locksForKey.values().stream()
                        .allMatch(value -> value.equals(Value.NULL));
            }
        } catch (Throwable t) {
            logger.error("Error while checkColumnValues for key={}, values={}", key, locksForKey, t);
            throw t;
        }

        return true;
    }

    private boolean checkValue(Key key, Value column, Value expectedValue, byte[] actualValue) {
        if(expectedValue.equals(Value.get(actualValue))){
            return true;
        } else {
            logger.info("Unexpected value for key {}, column {}, expected {}, actual {}", key, column, expectedValue, actualValue);
            return false;
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
