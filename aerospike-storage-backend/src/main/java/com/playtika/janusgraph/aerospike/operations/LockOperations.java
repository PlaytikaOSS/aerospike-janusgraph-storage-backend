package com.playtika.janusgraph.aerospike.operations;

import com.aerospike.client.Key;
import com.aerospike.client.Value;
import org.janusgraph.diskstorage.BackendException;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public interface LockOperations {

    enum LockType {
        LOCKED,
        SAME_TRANSACTION
    }

    Set<Key> acquireLocks(Value transactionId,
                          Map<String, Map<Value, Map<Value, Value>>> locksByStore,
                          boolean checkTransactionId,
                          Consumer<Map<Key, LockType>> onErrorCleanup) throws BackendException;

    List<Key> filterKeysLockedByTransaction(
            Map<String, Map<Value, Map<Value, Value>>> locksByStore, Value transactionId);

    void releaseLocks(Collection<Key> keys);
}
