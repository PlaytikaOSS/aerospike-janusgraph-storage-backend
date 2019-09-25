package com.playtika.janusgraph.aerospike.operations;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.policy.WritePolicy;
import com.playtika.janusgraph.aerospike.util.AsyncUtil;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.EntryArrayList;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.playtika.janusgraph.aerospike.operations.AerospikeOperations.ENTRIES_BIN_NAME;
import static com.playtika.janusgraph.aerospike.operations.AerospikeOperations.getValue;

public class ReadOperations {

    private final AerospikeOperations aerospikeOperations;

    private final WritePolicy getPolicy;
    private final int bathReadThreshold;

    public ReadOperations(AerospikeOperations aerospikeOperations, int bathReadThreshold) {
        this.aerospikeOperations = aerospikeOperations;
        this.getPolicy = aerospikeOperations.getAerospikePolicyProvider().writePolicy();
        this.bathReadThreshold = bathReadThreshold;
    }

    public Map<StaticBuffer,EntryList> getSlice(String storeName, List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws BackendException {
        if(keys.size() == 1){
            return getSliceOfOneKey(storeName, keys, query, txh);
        } else if(keys.size() < bathReadThreshold){
            return getSliceInParallel(storeName, keys, query, txh);
        } else {
            return getSliceByBatch(storeName, keys, query, txh);
        }
    }

    private Map<StaticBuffer,EntryList> getSliceOfOneKey(String storeName, List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws BackendException {
        StaticBuffer key = keys.get(0);
        return Collections.singletonMap(key,
                getSlice(storeName, new KeySliceQuery(key, query.getSliceStart(), query.getSliceEnd()), txh));
    }

    private Map<StaticBuffer,EntryList> getSliceByBatch(String storeName, List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws BackendException {
        Key[] aerospikeKeys = keys.stream()
                .map(keyBuffer -> aerospikeOperations.getKey(storeName, keyBuffer))
                .toArray(Key[]::new);
        Record[] records = aerospikeOperations.getClient().get(null, aerospikeKeys, ENTRIES_BIN_NAME);

        Map<StaticBuffer,EntryList> resultMap = new HashMap<>(keys.size());
        for(int i = 0, n = records.length; i < n; i++){
            Record record = records[i];
            if(record != null) {
                SortedMap<?, ?> map = ((SortedMap) record.getMap(ENTRIES_BIN_NAME));
                resultMap.put(keys.get(i), recordToEntries(map, isInSlice(query), query.getLimit()));
            } else {
                resultMap.put(keys.get(i), EntryList.EMPTY_LIST);
            }
        }
        return resultMap;
    }

    private static Predicate<Entry> isInSlice(SliceQuery query) {
        return entry -> entry.getColumn().compareTo(query.getSliceStart()) >= 0
                && entry.getColumn().compareTo(query.getSliceEnd()) < 0;
    }

    private Map<StaticBuffer,EntryList> getSliceInParallel(String storeName, List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws BackendException {
        return AsyncUtil.mapAll(keys, key -> {
            try {
                return getSlice(storeName, new KeySliceQuery(key, query), txh);
            } catch (BackendException e) {
                throw new RuntimeException(e);
            }
        }, aerospikeOperations.getAerospikeExecutor());
    }

    private EntryList recordToEntries(Map<?,?> entriesMap, Predicate<? super Entry> entryPredicate, int entriesNo) {

        if(entriesMap != null && !entriesMap.isEmpty()){
            return entriesMap.entrySet().stream()
                    .map(entry -> StaticArrayEntry.of(
                            StaticArrayBuffer.of((ByteBuffer) entry.getKey()),
                            StaticArrayBuffer.of((byte[]) entry.getValue())))
                    .filter(entryPredicate)
                    .limit(entriesNo)
                    .collect(Collectors.toCollection(EntryArrayList::new));
        }
        return EntryList.EMPTY_LIST;
    }

    public EntryList getSlice(String storeName, KeySliceQuery query, StoreTransaction txh) throws BackendException {

        try {
            Record record = aerospikeOperations.getClient().operate(getPolicy,
                    aerospikeOperations.getKey(storeName, query.getKey()),
                    MapOperation.getByKeyRange(ENTRIES_BIN_NAME,
                            getValue(query.getSliceStart()), getValue(query.getSliceEnd()), MapReturnType.KEY_VALUE)
            );

            return recordToEntries(record, query.getLimit());

        } catch (AerospikeException e) {
            throw new PermanentBackendException(e);
        }
    }

    private EntryList recordToEntries(Record record, int entriesNo) {
        List<?> resultList;
        if(record != null
                && (resultList = record.getList(ENTRIES_BIN_NAME)) != null
                && !resultList.isEmpty()) {
            final EntryArrayList result = new EntryArrayList();
            resultList.stream()
                    .limit(entriesNo)
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
    }

}
