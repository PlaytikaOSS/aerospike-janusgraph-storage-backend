package com.playtika.janusgraph.aerospike.operations;

import com.aerospike.client.Key;
import com.aerospike.client.Value;
import org.janusgraph.diskstorage.BackendException;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static com.playtika.janusgraph.aerospike.operations.FlakingUtils.selectFlaking;

public class FlakingLockOperations implements LockOperations {

    private final LockOperations lockOperations;
    private final AtomicBoolean failsRelease;
    private final AtomicBoolean failsAcquire;

    public FlakingLockOperations(LockOperations lockOperations, AtomicBoolean failsRelease, AtomicBoolean failsAcquire) {
        this.lockOperations = lockOperations;
        this.failsRelease = failsRelease;
        this.failsAcquire = failsAcquire;
    }

    @Override
    public Set<Key> acquireLocks(Value transactionId,
                                 Map<String, Map<Value, Map<Value, Value>>> locksByStore,
                                 boolean checkTransactionId,
                                 Consumer<Map<Key, LockType>> onErrorCleanup) throws BackendException {

        if(failsAcquire.get()) {
            Map<String, Map<Value, Map<Value, Value>>> partialLocksByStore = selectFlaking(locksByStore,
                    "acquireLocks failed flaking in [{}] for key [{}]");

            lockOperations.acquireLocks(
                    transactionId, partialLocksByStore, checkTransactionId, onErrorCleanup);

            throw new RuntimeException();
        } else {
            return lockOperations.acquireLocks(transactionId, locksByStore, checkTransactionId, onErrorCleanup);
        }
    }

    @Override
    public List<Key> filterKeysLockedByTransaction(Map<String, Map<Value, Map<Value, Value>>> locksByStore, Value transactionId) {
        return lockOperations.filterKeysLockedByTransaction(locksByStore, transactionId);
    }

    @Override
    public void releaseLocks(Collection<Key> keys) {
        if(failsRelease.get()){
            Set<Key> keysReleased = selectFlaking(keys, "releaseLocks failed flaking for key [{}]");
            lockOperations.releaseLocks(keysReleased);

            throw new RuntimeException();
        } else {
            lockOperations.releaseLocks(keys);
        }
    }
}