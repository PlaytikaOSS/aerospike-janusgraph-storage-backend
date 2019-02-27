package com.playtika.janusgraph.aerospike;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;

import java.util.ArrayList;
import java.util.List;

final class AerospikeTransaction extends AbstractStoreTransaction {

    private final AerospikeStoreManager storeManager;
    private final List<AerospikeLock> locks = new ArrayList<>();

    AerospikeTransaction(final BaseTransactionConfig config, AerospikeStoreManager storeManager) {
        super(config);
        this.storeManager = storeManager;
    }

    @Override
    public void commit() throws BackendException {
        storeManager.releaseLocks(locks);
        locks.clear();
    }

    @Override
    public void rollback() throws BackendException {
        storeManager.releaseLocks(locks);
        locks.clear();
    }

    void addLock(AerospikeLock lock){
        locks.add(lock);
    }

    List<AerospikeLock> getLocks() {
        return locks;
    }
}
