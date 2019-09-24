package com.playtika.janusgraph.aerospike;

import com.aerospike.client.Value;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.playtika.janusgraph.aerospike.operations.AerospikeOperations.getValue;

final class AerospikeTransaction extends AbstractStoreTransaction {

    private final List<AerospikeLock> locks = new ArrayList<>();

    AerospikeTransaction(final BaseTransactionConfig config) {
        super(config);
    }

    @Override
    public void commit() {
        locks.clear();
    }

    @Override
    public void rollback() {
        locks.clear();
    }

    void addLock(AerospikeLock lock){
        locks.add(lock);
    }

    List<AerospikeLock> getLocks() {
        return locks;
    }

    Map<String, Map<Value, Map<Value, Value>>> getLocksByStoreKeyColumn(){
        return locks.stream()
                .collect(Collectors.groupingBy(lock -> lock.storeName,
                        Collectors.groupingBy(lock -> getValue(lock.key),
                                Collectors.toMap(
                                        lock -> getValue(lock.column),
                                        lock -> lock.expectedValue != null ? getValue(lock.expectedValue) : Value.NULL,
                                        (oldValue, newValue) -> oldValue))));
    }

    @Override
    public String toString(){
        return Integer.toHexString(hashCode());
    }
}
