package com.playtika.janusgraph.aerospike.operations;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Record;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.policy.WritePolicy;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.EntryArrayList;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.playtika.janusgraph.aerospike.operations.AerospikeOperations.ENTRIES_BIN_NAME;
import static com.playtika.janusgraph.aerospike.operations.AerospikeOperations.getValue;

public class ReadOperations {

    private final AerospikeOperations aerospikeOperations;

    private final WritePolicy getPolicy;

    public ReadOperations(AerospikeOperations aerospikeOperations) {
        this.aerospikeOperations = aerospikeOperations;
        this.getPolicy = aerospikeOperations.getAerospikePolicyProvider().writePolicy();
    }

    public Map<StaticBuffer, EntryList> getSlice(String storeName, List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) {
        return getSliceInParallel(storeName, keys, query, txh).block();
    }

    private Mono<Map<StaticBuffer,EntryList>> getSliceInParallel(String storeName, List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) {
        return Flux.fromIterable(keys)
                .flatMap(key -> getSlice(storeName, new KeySliceQuery(key, query), txh))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public Mono<Map.Entry<StaticBuffer, EntryList>> getSlice(String storeName, KeySliceQuery query, StoreTransaction txh) {
        return aerospikeOperations.getReactorClient().operate(getPolicy,
                aerospikeOperations.getKey(storeName, query.getKey()),
                MapOperation.getByKeyRange(ENTRIES_BIN_NAME,
                        getValue(query.getSliceStart()), getValue(query.getSliceEnd()), MapReturnType.KEY_VALUE))
                .<Map.Entry<StaticBuffer, EntryList>>map(keyRecord ->
                        new AbstractMap.SimpleEntry<>(query.getKey(), recordToEntries(keyRecord.record, query.getLimit())))
                .onErrorMap(AerospikeException.class, PermanentBackendException::new);
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
