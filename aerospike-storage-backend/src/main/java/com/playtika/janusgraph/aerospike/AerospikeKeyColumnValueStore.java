package com.playtika.janusgraph.aerospike;

import com.aerospike.client.*;
import com.aerospike.client.cdt.*;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import org.janusgraph.diskstorage.*;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.*;
import org.janusgraph.diskstorage.util.EntryArrayList;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import static com.playtika.janusgraph.aerospike.ConfigOptions.ALLOW_SCAN;
import static com.playtika.janusgraph.aerospike.ConfigOptions.NAMESPACE;
import static com.playtika.janusgraph.aerospike.LockOperationsUdf.UNLOCK_OPERATION;

public class AerospikeKeyColumnValueStore implements KeyColumnValueStore {

    private static final MapPolicy mapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE);

    private static final WritePolicy mutatePolicy = new WritePolicy();
    static {
        mutatePolicy.respondAllOps = true;
    }

    static final String ENTRIES_BIN_NAME = "entries";

    private final String namespace;
    private final String name; //used as set name
    private final Configuration configuration;
    private final AerospikeClient client;
    private final Executor scanExecutor;
    private final LockOperations lockOperations;

    AerospikeKeyColumnValueStore(String name,
                                 AerospikeClient client,
                                 Configuration configuration,
                                 Executor aerospikeExecutor,
                                 Executor scanExecutor) {
        this.namespace = configuration.get(NAMESPACE);
        this.name = name;
        this.client = client;
        this.configuration = configuration;
        this.scanExecutor = scanExecutor;
        this.lockOperations = new LockOperationsUdf(client, this, configuration, aerospikeExecutor);
    }

    @Override // This method is only supported by stores which keep keys in byte-order.
    public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh) {
        throw new UnsupportedOperationException();
    }

    /**
     * Used to add new index on existing graph
     * @param query
     * @param txh
     * @return
     */
    @Override // This method is only supported by stores which do not keep keys in byte-order.
    public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) {
        if(!configuration.get(ALLOW_SCAN)){
            throw new UnsupportedOperationException();
        }
        ScanPolicy scanPolicy = new ScanPolicy();
        scanPolicy.includeBinData = false;

        AerospikeKeyIterator keyIterator = new AerospikeKeyIterator(client);

        scanExecutor.execute(() -> {
            try {
                client.scanAll(scanPolicy, namespace, name, keyIterator);
            } finally {
                keyIterator.terminate();
            }
        });

        return keyIterator;
    }

    @Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws BackendException {

        try {
            Record record = client.operate(null, getKey(query.getKey()),
                    MapOperation.getByKeyRange(ENTRIES_BIN_NAME,
                            getValue(query.getSliceStart()), getValue(query.getSliceEnd()), MapReturnType.KEY_VALUE)
            );

            List<?> resultList;
            if(record != null
                    && (resultList = record.getList(ENTRIES_BIN_NAME)) != null
                    && !resultList.isEmpty()) {
                final EntryArrayList result = new EntryArrayList();
                resultList.stream()
                        .limit(query.getLimit())
                        .forEach(o -> {
                            Map.Entry<?, ?> entry = (Map.Entry<?, ?>) o;
                            result.add(StaticArrayEntry.of(
                                    StaticArrayBuffer.of((byte[]) entry.getKey()),
                                    StaticArrayBuffer.of((byte[]) entry.getValue())));
                        });
                return result;
            } else {
                return EntryList.EMPTY_LIST;
            }

        } catch (AerospikeException e) {
            throw new PermanentBackendException(e);
        }
    }

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws BackendException {
        AerospikeTransaction transaction = (AerospikeTransaction)txh;
        for(AerospikeLock lock : transaction.getLocks()){
            if(lock.key.compareTo(key) != 0){
                throw new IllegalArgumentException("Unexpected lock in direct mutation transaction:" + lock);
            }
        }

        AerospikeLocks locks = new AerospikeLocks(transaction.getLocks().size());
        locks.addLocks(transaction.getLocks());

        lockOperations.acquireLocks(locks.getLocksMap());

        mutate(key, additions, deletions, false);
    }

    void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, boolean andUnlock) {
        List<Operation> operations = new ArrayList<>(3);

        if(!deletions.isEmpty()) {
            List<Value> keysToRemove = new ArrayList<>(deletions.size());
            for (StaticBuffer deletion : deletions) {
                keysToRemove.add(getValue(deletion));
            }
            operations.add(MapOperation.removeByKeyList(ENTRIES_BIN_NAME, keysToRemove, MapReturnType.NONE));
        }

        if(!additions.isEmpty()) {
            Map<Value, Value> itemsToAdd = new HashMap<>(additions.size());
            for (Entry addition : additions) {
                itemsToAdd.put(getValue(addition.getColumn()), getValue(addition.getValue()));
            }
            operations.add(MapOperation.putItems(mapPolicy, ENTRIES_BIN_NAME, itemsToAdd));
        }

        int entriesNoOperationIndex = -1;
        if(!deletions.isEmpty()){
            entriesNoOperationIndex = operations.size();
            operations.add(MapOperation.size(ENTRIES_BIN_NAME));
        }

        if(andUnlock){
            operations.add(UNLOCK_OPERATION);
        }

        Key aerospikeKey = getKey(key);
        Record record = client.operate(null, aerospikeKey, operations.toArray(new Operation[0]));
        if(entriesNoOperationIndex != -1){
            long entriesNoAfterMutation = (Long)record.getList(ENTRIES_BIN_NAME).get(entriesNoOperationIndex);
            if(entriesNoAfterMutation == 0){
                client.delete(null, aerospikeKey);
            }
        }
    }

    @Override
    public Map<StaticBuffer,EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws BackendException {
        final Map<StaticBuffer, EntryList> result = new HashMap<>();

        for (StaticBuffer key : keys)
            result.put(key, getSlice(new KeySliceQuery(key, query), txh));

        return result;
    }

    Key getKey(StaticBuffer staticBuffer) {
        return new Key(namespace, name, staticBuffer.getBytes(0, staticBuffer.length()));
    }

    static Value getValue(StaticBuffer staticBuffer) {
        return Value.get(staticBuffer.getBytes(0, staticBuffer.length()));
    }

    @Override
    public void acquireLock(final StaticBuffer key, final StaticBuffer column, final StaticBuffer expectedValue, final StoreTransaction txh) {
        //deferred locking approach
        //just add lock to transaction, actual lock will be acquired at commit phase
        ((AerospikeTransaction)txh).addLock(new AerospikeLock(name, key, column, expectedValue));
    }

    @Override
    public synchronized void close() {}

    @Override
    public String getName() {
        return name;
    }

    public LockOperations getLockOperations() {
        return lockOperations;
    }

}
