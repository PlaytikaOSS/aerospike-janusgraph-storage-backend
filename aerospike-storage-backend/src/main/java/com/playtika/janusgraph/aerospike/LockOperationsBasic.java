package com.playtika.janusgraph.aerospike;

import com.aerospike.client.*;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.playtika.janusgraph.aerospike.AerospikeKeyColumnValueStore.ENTRIES_BIN_NAME;
import static com.playtika.janusgraph.aerospike.AerospikeKeyColumnValueStore.getValue;
import static com.playtika.janusgraph.aerospike.ConfigOptions.LOCK_TTL;
import static com.playtika.janusgraph.aerospike.LockOperationsUdf.mergeLocks;

class LockOperationsBasic implements LockOperations{

    private static final Bin LOCK_BIN = new Bin("L", true);

    private final String namespace;
    private final String lockSetName;
    private final AerospikeClient client;
    private final AerospikeKeyColumnValueStore store;
    private final WritePolicy putLockPolicy;

    LockOperationsBasic(String namespace, String name,
                   AerospikeClient client,
                   AerospikeKeyColumnValueStore store,
                   Configuration configuration) {
        this.namespace = namespace;
        this.client = client;
        this.lockSetName = name +".lock";
        this.store = store;

        putLockPolicy = new WritePolicy();
        putLockPolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
        putLockPolicy.expiration = configuration.get(LOCK_TTL);
    }

    @Override
    public void acquireLocks(Map<StaticBuffer, List<AerospikeLock>> locks) throws BackendException {
        List<StaticBuffer> keysLocked = new ArrayList<>();
        try {
            for (Map.Entry<StaticBuffer, List<AerospikeLock>> locksForKey : locks.entrySet()) {
                putLockOnKey(locksForKey.getKey());
                keysLocked.add(locksForKey.getKey());
                checkColumnValues(locksForKey.getKey(), locksForKey.getValue());
            }
        } catch (Throwable t){
            releaseLockOnKeys(keysLocked);
            throw t;
        }
    }

    private void putLockOnKey(final StaticBuffer key) throws TemporaryBackendException {
        try {
            client.add(putLockPolicy, getLockKey(key), LOCK_BIN);
        } catch (AerospikeException e){
            if(e.getResultCode() == ResultCode.KEY_EXISTS_ERROR){
                throw new TemporaryBackendException(e);
            } else {
                throw e;
            }
        }
    }

    private void checkColumnValues(final StaticBuffer key, final List<AerospikeLock> locksForKey) throws PermanentBackendException {
        if(locksForKey.isEmpty()){
            return;
        }

        List<AerospikeLock> locksForKeyMerged = mergeLocks(locksForKey);
        Operation[] operations = locksForKeyMerged.stream()
                .map(lock -> MapOperation.getByKey(ENTRIES_BIN_NAME, getValue(lock.column), MapReturnType.VALUE))
                .toArray(Operation[]::new);
        Record record = client.operate(null, store.getKey(key), operations);
        if(record != null){
            if(locksForKeyMerged.size() > 1){
                List<?> resultList;
                if((resultList = record.getList(ENTRIES_BIN_NAME)) != null){
                    for(int i = 0, n = resultList.size(); i < n; i++){
                        AerospikeLock lock = locksForKeyMerged.get(i);
                        checkValue(lock, (byte[])resultList.get(i));
                    }
                }
            } else if(locksForKeyMerged.size() == 1){
                byte[] actualValueData;
                if((actualValueData = (byte[])record.getValue(ENTRIES_BIN_NAME)) != null){
                    checkValue(locksForKeyMerged.get(0), actualValueData);
                }
            }
        }
    }

    @Override
    public void releaseLockOnKeys(Collection<StaticBuffer> keys) {
        keys.forEach(key -> {
            client.delete(null, getLockKey(key));
        });
    }

    private void checkValue(AerospikeLock lock, byte[] actualValue) throws PermanentBackendException {
        StaticBuffer actualValueBuffer = new StaticArrayBuffer(actualValue);
        if(actualValueBuffer.compareTo(lock.expectedValue) != 0){
            throw new PermanentBackendException(
                    String.format("Unexpected value for key %s, column %s, expected %s, actual %s",
                            lock.key, lock.column, lock.expectedValue, actualValueBuffer));
        }
    }

    private Key getLockKey(StaticBuffer staticBuffer) {
        return new Key(namespace, lockSetName, staticBuffer.getBytes(0, staticBuffer.length()));
    }

}
