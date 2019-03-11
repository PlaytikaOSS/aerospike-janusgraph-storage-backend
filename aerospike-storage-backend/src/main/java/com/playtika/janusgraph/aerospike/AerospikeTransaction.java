package com.playtika.janusgraph.aerospike;

import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;

import java.util.ArrayList;
import java.util.List;

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
}
