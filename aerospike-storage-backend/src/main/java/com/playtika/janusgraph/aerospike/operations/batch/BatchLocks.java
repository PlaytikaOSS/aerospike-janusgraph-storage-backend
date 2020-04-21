package com.playtika.janusgraph.aerospike.operations.batch;

import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.playtika.janusgraph.aerospike.operations.AerospikeOperations;
import nosql.batch.update.aerospike.lock.AerospikeBatchLocks;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BatchLocks implements AerospikeBatchLocks<Map<Key, ExpectedValue>> {

    private final AerospikeOperations aerospikeOperations;
    private Map<String, Map<Value, Map<Value, Value>>> locksByStore;
    private final List<Key> keysToLock;
    private final Map<Key, ExpectedValue> expectedValues;

    public BatchLocks(Map<String, Map<Value, Map<Value, Value>>> locksByStore, AerospikeOperations aerospikeOperations) {
        this.aerospikeOperations = aerospikeOperations;
        this.locksByStore = locksByStore;
        int locksCount = locksCount(locksByStore);
        keysToLock = new ArrayList<>(locksCount);
        expectedValues = new HashMap<>(locksCount);
        populateData(locksByStore);
    }

    private void populateData(Map<String, Map<Value, Map<Value, Value>>> locksByStore){
        for (Map.Entry<String, Map<Value, Map<Value, Value>>> locksForStore : locksByStore.entrySet()) {
            String storeName = locksForStore.getKey();
            for (Map.Entry<Value, Map<Value, Value>> entry : locksForStore.getValue().entrySet()) {
                Key lockKey = getLockKey(storeName, entry.getKey());
                keysToLock.add(lockKey);
                expectedValues.put(lockKey, new ExpectedValue(storeName, entry.getKey(), entry.getValue()));
            }
        }
    }

    @Override
    public List<Key> keysToLock() {
        return keysToLock;
    }

    private static int locksCount(Map<String, Map<Value, Map<Value, Value>>> locksByStore){
        int count = 0;
        for (Map.Entry<String, Map<Value, Map<Value, Value>>> locksForStore : locksByStore.entrySet()) {
            count += locksForStore.getValue().size();
        }
        return count;
    }


    private Key getLockKey(String storeName, Value value) {
        return aerospikeOperations.getKey(storeName + ".lock", value);
    }

    @Override
    public Map<Key, ExpectedValue> expectedValues() {
        return expectedValues;
    }

    public Map<String, Map<Value, Map<Value, Value>>> getLocksByStore() {
        return locksByStore;
    }
}
