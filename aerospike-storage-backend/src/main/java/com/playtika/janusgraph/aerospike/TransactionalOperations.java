package com.playtika.janusgraph.aerospike;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.playtika.janusgraph.aerospike.wal.WriteAheadLogManager;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.playtika.janusgraph.aerospike.AerospikeKeyColumnValueStore.getValue;
import static com.playtika.janusgraph.aerospike.util.AsyncUtil.completeAll;
import static java.util.concurrent.CompletableFuture.runAsync;

public class TransactionalOperations {

    private final Function<String, AKeyColumnValueStore> databaseFactory;
    private final WriteAheadLogManager writeAheadLogManager;
    private final LockOperations lockOperations;
    private final ThreadPoolExecutor aerospikeExecutor;

    public TransactionalOperations(Function<String, AKeyColumnValueStore> databaseFactory,
                                   WriteAheadLogManager writeAheadLogManager,
                                   LockOperations lockOperations,
                                   ThreadPoolExecutor aerospikeExecutor) {
        this.databaseFactory = databaseFactory;
        this.writeAheadLogManager = writeAheadLogManager;
        this.lockOperations = lockOperations;
        this.aerospikeExecutor = aerospikeExecutor;
    }

    public void mutateManyTransactionally(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {
        Map<String, Map<Value, Map<Value, Value>>> locksByStore = groupLocksByStoreKeyColumn(
                ((AerospikeTransaction) txh).getLocks());

        Map<String, Map<Value, Map<Value, Value>>> mutationsByStore = groupMutationsByStoreKeyColumn(mutations);

        Value transactionId = writeAheadLogManager.writeTransaction(locksByStore, mutationsByStore);

        processAndDeleteTransaction(transactionId, locksByStore, mutationsByStore, false);
    }

    public void processAndDeleteTransaction(Value transactionId,
                                            Map<String, Map<Value, Map<Value, Value>>> locksByStore,
                                            Map<String, Map<Value, Map<Value, Value>>> mutationsByStore,
                                            boolean checkTransactionId) throws BackendException {
        Set<Key> keysLocked = lockOperations.acquireLocks(transactionId, locksByStore, checkTransactionId);
        try {
            mutateMany(mutationsByStore);
        } catch (AerospikeException e) {
            throw new PermanentBackendException(e);
        } finally {
            releaseLocksAndDeleteWalTransaction(keysLocked, transactionId);
        }
    }

    public void releaseLocksAndDeleteWalTransaction(Map<String, Map<Value, Map<Value, Value>>> locksByStore, Value transactionId) throws BackendException {
        List<Key> lockKeys = lockOperations.getLockKeys(locksByStore);
        List<Key> transactionLockKeys = lockOperations.filterKeysLockedByTransaction(lockKeys, transactionId);
        lockOperations.releaseLocks(transactionLockKeys);
        deleteWalTransaction(transactionId);
    }

    private void releaseLocksAndDeleteWalTransaction(Set<Key> keysLocked, Value transactionId) throws BackendException {
        releaseLocks(keysLocked);
        deleteWalTransaction(transactionId);
    }

    void releaseLocks(Set<Key> keysLocked) throws BackendException {
        lockOperations.releaseLocks(keysLocked);
    }

    void deleteWalTransaction(Value transactionId) {
        writeAheadLogManager.deleteTransaction(transactionId);
    }

    static Map<String, Map<Value, Map<Value, Value>>> groupLocksByStoreKeyColumn(List<AerospikeLock> locks){
        return locks.stream()
                .collect(Collectors.groupingBy(lock -> lock.storeName,
                        Collectors.groupingBy(lock -> getValue(lock.key),
                                Collectors.toMap(
                                        lock -> getValue(lock.column),
                                        lock -> lock.expectedValue != null ? getValue(lock.expectedValue) : Value.NULL,
                                        (oldValue, newValue) -> oldValue))));
    }

    private static Map<String, Map<Value, Map<Value, Value>>> groupMutationsByStoreKeyColumn(
            Map<String, Map<StaticBuffer, KCVMutation>> mutationsByStore){
        Map<String, Map<Value, Map<Value, Value>>> mapByStore = new HashMap<>(mutationsByStore.size());
        for(Map.Entry<String, Map<StaticBuffer, KCVMutation>> storeMutations : mutationsByStore.entrySet()) {
            Map<Value, Map<Value, Value>> map = new HashMap<>(storeMutations.getValue().size());
            for (Map.Entry<StaticBuffer, KCVMutation> mutationEntry : storeMutations.getValue().entrySet()) {
                map.put(getValue(mutationEntry.getKey()), mutationToMap(mutationEntry.getValue()));
            }
            mapByStore.put(storeMutations.getKey(), map);
        }
        return mapByStore;
    }

    static Map<Value, Value> mutationToMap(KCVMutation mutation){
        Map<Value, Value> map = new HashMap<>(mutation.getAdditions().size() + mutation.getDeletions().size());
        for(StaticBuffer deletion : mutation.getDeletions()){
            map.put(getValue(deletion), Value.NULL);
        }

        for(Entry addition : mutation.getAdditions()){
            map.put(getValue(addition.getColumn()), getValue(addition.getValue()));
        }
        return map;
    }

    private void mutateMany(
            Map<String, Map<Value, Map<Value, Value>>> mutationsByStore) throws PermanentBackendException {

        List<CompletableFuture<?>> mutations = new ArrayList<>();

        mutationsByStore.forEach((storeName, storeMutations) -> {
            final AKeyColumnValueStore store = databaseFactory.apply(storeName);
            for(Map.Entry<Value, Map<Value, Value>> mutationEntry : storeMutations.entrySet()){
                Value key = mutationEntry.getKey();
                Map<Value, Value> mutation = mutationEntry.getValue();
                mutations.add(runAsync(() -> store.mutate(key, mutation), aerospikeExecutor));
            }
        });

        completeAll(mutations);
    }


}
