package com.playtika.janusgraph.aerospike.transaction;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.playtika.janusgraph.aerospike.operations.LockOperations;
import com.playtika.janusgraph.aerospike.operations.MutateOperations;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.PermanentBackendException;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TransactionalOperations {

    private final WriteAheadLogManager writeAheadLogManager;
    private final LockOperations lockOperations;
    private final MutateOperations mutateOperations;

    public TransactionalOperations(WriteAheadLogManager writeAheadLogManager,
                                   LockOperations lockOperations,
                                   MutateOperations mutateOperations) {
        this.writeAheadLogManager = writeAheadLogManager;
        this.lockOperations = lockOperations;
        this.mutateOperations = mutateOperations;
    }

    public void mutateTransactionally(Map<String, Map<Value, Map<Value, Value>>> locksByStore, Map<String, Map<Value, Map<Value, Value>>> mutationsByStore) throws BackendException {
        Value transactionId = writeAheadLogManager.writeTransaction(locksByStore, mutationsByStore);

        processAndDeleteTransaction(transactionId, locksByStore, mutationsByStore, false);
    }

    void processAndDeleteTransaction(Value transactionId,
                                            Map<String, Map<Value, Map<Value, Value>>> locksByStore,
                                            Map<String, Map<Value, Map<Value, Value>>> mutationsByStore,
                                            boolean checkTransactionId) throws BackendException {
        Set<Key> keysLocked = lockOperations.acquireLocks(transactionId, locksByStore, checkTransactionId,
                keyLockTypeMap -> releaseLocksAndDeleteWalTransaction(keyLockTypeMap.keySet(), transactionId));
        try {
            mutateOperations.mutateMany(mutationsByStore);
            releaseLocksAndDeleteWalTransaction(keysLocked, transactionId);
        }
        catch (AerospikeException e) {
            throw new PermanentBackendException(e);
        }
    }

    private void releaseLocksAndDeleteWalTransaction(Collection<Key> keysLocked, Value transactionId) {
        lockOperations.releaseLocks(keysLocked);
        writeAheadLogManager.deleteTransaction(transactionId);
    }

    void releaseLocksAndDeleteWalTransactionOnError(Map<String, Map<Value, Map<Value, Value>>> locksByStore, Value transactionId) throws BackendException {
        List<Key> transactionLockKeys = lockOperations.filterKeysLockedByTransaction(locksByStore, transactionId);
        releaseLocksAndDeleteWalTransaction(transactionLockKeys, transactionId);
    }

    public WriteAheadLogManager getWriteAheadLogManager() {
        return writeAheadLogManager;
    }

    public MutateOperations getMutateOperations(){
        return mutateOperations;
    }
}
