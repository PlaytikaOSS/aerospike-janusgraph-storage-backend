package com.playtika.janusgraph.aerospike.operations;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Record;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.policy.WritePolicy;
import com.playtika.janusgraph.aerospike.util.AsyncUtil;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.EntryArrayList;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;

import java.util.List;
import java.util.Map;

import static com.playtika.janusgraph.aerospike.operations.AerospikeOperations.ENTRIES_BIN_NAME;
import static com.playtika.janusgraph.aerospike.operations.AerospikeOperations.getValue;

public class ReadOperations {

    private final AerospikeOperations aerospikeOperations;

    private final WritePolicy getPolicy;

    public ReadOperations(AerospikeOperations aerospikeOperations) {
        this.aerospikeOperations = aerospikeOperations;
        this.getPolicy = aerospikeOperations.getAerospikePolicyProvider().writePolicy();
    }

    public Map<StaticBuffer,EntryList> getSlice(String storeName, List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws BackendException {
        return AsyncUtil.mapAll(keys, key -> {
            try {
                return getSlice(storeName, new KeySliceQuery(key, query), txh);
            } catch (BackendException e) {
                throw new RuntimeException(e);
            }
        }, aerospikeOperations.getAerospikeExecutor());
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
