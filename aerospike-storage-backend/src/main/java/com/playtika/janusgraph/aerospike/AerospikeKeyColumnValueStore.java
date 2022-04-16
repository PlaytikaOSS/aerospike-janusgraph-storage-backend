package com.playtika.janusgraph.aerospike;

import com.aerospike.client.Value;
import com.playtika.janusgraph.aerospike.operations.AerospikeOperations;
import com.playtika.janusgraph.aerospike.operations.ErrorMapper;
import com.playtika.janusgraph.aerospike.operations.IdsCleanupOperations;
import com.playtika.janusgraph.aerospike.operations.MutateOperations;
import com.playtika.janusgraph.aerospike.operations.ReadOperations;
import com.playtika.janusgraph.aerospike.operations.ScanOperations;
import com.playtika.janusgraph.aerospike.operations.batch.BatchLocks;
import com.playtika.janusgraph.aerospike.operations.batch.BatchUpdate;
import com.playtika.janusgraph.aerospike.operations.batch.BatchUpdates;
import nosql.batch.update.BatchUpdater;
import nosql.batch.update.aerospike.lock.AerospikeLock;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRangeQuery;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.playtika.janusgraph.aerospike.operations.AerospikeOperations.getValue;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;

public class AerospikeKeyColumnValueStore implements KeyColumnValueStore {

    private static final Logger logger = LoggerFactory.getLogger(AerospikeKeyColumnValueStore.class);

    private final String storeName;
    private final ReadOperations readOperations;
    private final AerospikeOperations aerospikeOperations;
    private final BatchUpdater<BatchLocks, BatchUpdates, AerospikeLock, Value> batchUpdater;
    private final MutateOperations mutateOperations;
    private final ScanOperations scanOperations;
    private final IdsCleanupOperations idsCleanupOperations;

    protected AerospikeKeyColumnValueStore(
            String storeName,
            ReadOperations readOperations,
            AerospikeOperations aerospikeOperations,
            BatchUpdater<BatchLocks, BatchUpdates, AerospikeLock, Value> batchUpdater,
            MutateOperations mutateOperations,
            ScanOperations scanOperations,
            IdsCleanupOperations idsCleanupOperations) {
        this.storeName = storeName;
        this.readOperations = readOperations;
        this.aerospikeOperations = aerospikeOperations;
        this.batchUpdater = batchUpdater;
        this.mutateOperations = mutateOperations;
        this.scanOperations = scanOperations;
        this.idsCleanupOperations = idsCleanupOperations;
    }

    @Override // This method is only supported by stores which keep keys in byte-order.
    public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh) {
        throw new UnsupportedOperationException();
    }

    /**
     * Except scan operations may be used by janusgraph to add new index on existing graph
     */
    @Override // This method is only supported by stores which do not keep keys in byte-order.
    public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) {
        logger.trace("getKeys({}, tx:{}, {})", storeName, txh, query);

        return scanOperations.getKeys(storeName, query, txh);
    }

    @Override
    public Map<StaticBuffer,EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws BackendException {
        logger.trace("getSlice({}, tx:{}, {}, start:{}, end:{})",
                storeName, txh, keys, query.getSliceStart(), query.getSliceEnd());

        return readOperations.getSlice(storeName, keys, query);
    }

    @Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws BackendException{
        logger.trace("getSlice({}, tx:{}, {})", storeName, txh, query);

        return readOperations.getSlice(storeName, query);
    }

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws BackendException {
        logger.trace("mutate({}, tx:{}, {}, {}, {})", storeName, txh, key, additions, deletions);

        AerospikeTransaction transaction = (AerospikeTransaction)txh;

        Map<Value, Value> mutationMap = mutationToMap(new KCVMutation(additions, deletions));
        Value keyValue = getValue(key);

        try {
            if (transaction.getLocks().isEmpty()) {
                //no need in transactional logic
                mutateOperations.mutate(storeName, keyValue, mutationMap);
            } else {
                updateBatch(keyValue, mutationMap, transaction);
            }
        } finally {
            if(idsCleanupOperations != null) {
                idsCleanupOperations.cleanUpOldIdsRanges(key);
            }
        }
    }

    private void updateBatch(Value keyValue,  Map<Value, Value> mutationMap, AerospikeTransaction transaction) throws BackendException {
        Map<String, Map<Value, Map<Value, Value>>> locksByStore = transaction.getLocksByStoreKeyColumn();
        if(!singleton(storeName).containsAll(locksByStore.keySet())){
            throw new IllegalArgumentException();
        }

        Map<Value, Map<Value, Value>> locks = locksByStore.getOrDefault(storeName, emptyMap());

        //expect that locks contains key
        if(!singleton(keyValue).containsAll(locks.keySet())){
            throw new IllegalArgumentException();
        }

        Map<String, Map<Value, Map<Value, Value>>> mutationsByStore = singletonMap(storeName,
                singletonMap(keyValue, mutationMap));

        try {
            batchUpdater.update(new BatchUpdate(
                    new BatchLocks(locksByStore, aerospikeOperations),
                    new BatchUpdates(mutationsByStore)));
        } catch (Throwable t) {
            throw ErrorMapper.INSTANCE.apply(t);
        }

        transaction.close();
    }

    public static Map<Value, Value> mutationToMap(KCVMutation mutation){
        Map<Value, Value> map = new HashMap<>(mutation.getAdditions().size() + mutation.getDeletions().size());
        for(StaticBuffer deletion : mutation.getDeletions()){
            map.put(getValue(deletion), Value.NULL);
        }

        for(Entry addition : mutation.getAdditions()){
            map.put(getValue(addition.getColumn()), getValue(addition.getValue()));
        }
        return map;
    }

    @Override
    public void acquireLock(final StaticBuffer key, final StaticBuffer column, final StaticBuffer expectedValue, final StoreTransaction txh) {
        //deferred locking approach
        //just add lock to transaction, actual lock will be acquired at commit phase
        ((AerospikeTransaction)txh).addLock(new DeferredLock(storeName, key, column, expectedValue));
        logger.trace("registered lock: {}:{}:{}:{}, tx:{}", storeName, key, column, expectedValue, txh);
    }

    @Override
    public synchronized void close() {}

    @Override
    public String getName() {
        return storeName;
    }



}
