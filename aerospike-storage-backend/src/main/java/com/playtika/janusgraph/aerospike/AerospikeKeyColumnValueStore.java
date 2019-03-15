package com.playtika.janusgraph.aerospike;

import com.aerospike.client.*;
import com.aerospike.client.cdt.*;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.playtika.janusgraph.aerospike.wal.WriteAheadLogManager;
import org.janusgraph.diskstorage.*;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.*;
import org.janusgraph.diskstorage.util.EntryArrayList;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Executor;

import static com.playtika.janusgraph.aerospike.AerospikeStoreManager.groupLocksByStoreKeyColumn;
import static com.playtika.janusgraph.aerospike.AerospikeStoreManager.mutationToMap;
import static com.playtika.janusgraph.aerospike.ConfigOptions.ALLOW_SCAN;
import static com.playtika.janusgraph.aerospike.ConfigOptions.NAMESPACE;
import static java.util.Collections.*;

public class AerospikeKeyColumnValueStore implements AKeyColumnValueStore {

    private static Logger logger = LoggerFactory.getLogger(AerospikeKeyColumnValueStore.class);

    private static final MapPolicy mapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE);

    private static final WritePolicy mutatePolicy = new WritePolicy();
    static {
        mutatePolicy.respondAllOps = true;
        mutatePolicy.sendKey = true;
    }

    static final String ENTRIES_BIN_NAME = "entries";

    private final String namespace;
    private final String name; //used as set name
    private final Configuration configuration;
    private final IAerospikeClient client;
    private final Executor scanExecutor;
    private final LockOperations lockOperations;
    private final WriteAheadLogManager writeAheadLogManager;

    AerospikeKeyColumnValueStore(String namespace,
                                 String name,
                                 IAerospikeClient client,
                                 Configuration configuration,
                                 LockOperations lockOperations,
                                 Executor scanExecutor,
                                 WriteAheadLogManager writeAheadLogManager) {
        this.namespace = namespace;
        this.name = name;
        this.client = client;
        this.configuration = configuration;
        this.scanExecutor = scanExecutor;
        this.lockOperations = lockOperations;
        this.writeAheadLogManager = writeAheadLogManager;
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

        Map<String, Map<Value, Map<Value, Value>>> locksByStore = groupLocksByStoreKeyColumn(transaction.getLocks());
        if(!singleton(name).containsAll(locksByStore.keySet())){
            throw new IllegalArgumentException();
        }

        Map<Value, Map<Value, Value>> locks = locksByStore.getOrDefault(name, emptyMap());

        Value keyValue = getValue(key);
        //expect that locks contains key
        if(!singleton(keyValue).containsAll(locks.keySet())){
            throw new IllegalArgumentException();
        }


        Map<Value, Value> mutationMap = mutationToMap(new KCVMutation(additions, deletions));
        Map<String, Map<Value, Map<Value, Value>>> mutationsByStore = singletonMap(name, singletonMap(keyValue,
                mutationMap));

        Value transactionId = writeAheadLogManager.writeTransaction(locksByStore, mutationsByStore);

        Set<Key> keysLocked = lockOperations.acquireLocks(transactionId, locksByStore, false);
        try {
            mutate(keyValue, mutationMap);
        } catch (AerospikeException e) {
            throw new PermanentBackendException(e);
        } finally {
            lockOperations.releaseLocks(keysLocked);
            writeAheadLogManager.deleteTransaction(transactionId);
        }
    }

    @Override
    public void mutate(Value key, Map<Value, Value> mutation) {
        List<Operation> operations = new ArrayList<>(3);

        List<Value> keysToRemove = new ArrayList<>(mutation.size());
        Map<Value, Value> itemsToAdd = new HashMap<>(mutation.size());
        for(Map.Entry<Value, Value> entry : mutation.entrySet()){
            if(entry.getValue() == Value.NULL){
                keysToRemove.add(entry.getKey());
            } else {
                itemsToAdd.put(entry.getKey(), entry.getValue());
            }
        }

        if(!keysToRemove.isEmpty()) {
            operations.add(MapOperation.removeByKeyList(ENTRIES_BIN_NAME, keysToRemove, MapReturnType.NONE));
        }

        if(!itemsToAdd.isEmpty()) {
            operations.add(MapOperation.putItems(mapPolicy, ENTRIES_BIN_NAME, itemsToAdd));
        }

        int entriesNoOperationIndex = -1;
        if(!keysToRemove.isEmpty()){
            entriesNoOperationIndex = operations.size();
            operations.add(MapOperation.size(ENTRIES_BIN_NAME));
        }

        Key aerospikeKey = getKey(key);
        Record record = client.operate(mutatePolicy, aerospikeKey, operations.toArray(new Operation[0]));
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

    private Key getKey(StaticBuffer staticBuffer) {
        return new Key(namespace, name, staticBuffer.getBytes(0, staticBuffer.length()));
    }

    Key getKey(Value value) {
        return new Key(namespace, name, value);
    }

    static Value getValue(StaticBuffer staticBuffer) {
        return Value.get(staticBuffer.getBytes(0, staticBuffer.length()));
    }

    @Override
    public void acquireLock(final StaticBuffer key, final StaticBuffer column, final StaticBuffer expectedValue, final StoreTransaction txh) {
        //deferred locking approach
        //just add lock to transaction, actual lock will be acquired at commit phase
        ((AerospikeTransaction)txh).addLock(new AerospikeLock(name, key, column, expectedValue));
        if(logger.isTraceEnabled()){
            logger.trace("registered lock: {}:{}:{}:{}, tx:{}", name, key, column, expectedValue, txh);
        }
    }

    @Override
    public synchronized void close() {}

    @Override
    public String getName() {
        return name;
    }

    LockOperations getLockOperations() {
        return lockOperations;
    }

}
