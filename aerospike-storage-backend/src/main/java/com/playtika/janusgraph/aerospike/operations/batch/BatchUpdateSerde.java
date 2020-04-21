package com.playtika.janusgraph.aerospike.operations.batch;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.playtika.janusgraph.aerospike.operations.AerospikeOperations;
import nosql.batch.update.BatchUpdate;
import nosql.batch.update.aerospike.wal.AerospikeBatchUpdateSerde;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;

public class BatchUpdateSerde implements AerospikeBatchUpdateSerde<BatchLocks, BatchUpdates, Map<Key, ExpectedValue>> {

    private static final String LOCKS_BIN = "locks";
    private static final String MUTATIONS_BIN = "mutations";

    private final AerospikeOperations aerospikeOperations;

    public BatchUpdateSerde(AerospikeOperations aerospikeOperations) {
        this.aerospikeOperations = aerospikeOperations;
    }

    @Override
    public List<Bin> write(BatchUpdate<BatchLocks, BatchUpdates> batch) {
        return asList(new Bin(LOCKS_BIN, stringMapToValue(batch.locks().getLocksByStore())),
                new Bin(MUTATIONS_BIN, stringMapToValue(batch.updates().getUpdatesByStore())));
    }

    @Override
    public BatchUpdate<BatchLocks, BatchUpdates> read(Map<String, Object> bins) {
        return new com.playtika.janusgraph.aerospike.operations.batch.BatchUpdate(
                new BatchLocks(wrapMap((Map<String, Map<byte[], Map<byte[], byte[]>>>)bins.get(LOCKS_BIN)), aerospikeOperations),
                new BatchUpdates(wrapMap((Map<String, Map<byte[], Map<byte[], byte[]>>>)bins.get(MUTATIONS_BIN)))
        );
    }

    private static Value stringMapToValue(Map<String, Map<Value, Map<Value, Value>>> map){
        Map<Value, Map<Value, Map<Value, Value>>> locksValue = new HashMap<>(map.size());
        for(Map.Entry<String, Map<Value, Map<Value, Value>>> locksEntry : map.entrySet()){
            locksValue.put(Value.get(locksEntry.getKey()), locksEntry.getValue());
        }
        return Value.get(locksValue);
    }

    private static Map<String, Map<Value, Map<Value, Value>>> wrapMap(
            Map<String, Map<byte[], Map<byte[], byte[]>>> map){
        Map<String, Map<Value, Map<Value, Value>>> resultMap = new HashMap<>(map.size());
        for(Map.Entry<String, Map<byte[], Map<byte[], byte[]>>> mapEntry : map.entrySet()){
            resultMap.put(mapEntry.getKey(), wrapBytesBytesMap(mapEntry.getValue()));
        }
        return resultMap;
    }

    private static Map<Value, Map<Value, Value>> wrapBytesBytesMap(Map<byte[], Map<byte[], byte[]>> map){
        Map<Value, Map<Value, Value>> resultMap = new HashMap<>(map.size());
        for(Map.Entry<byte[], Map<byte[], byte[]>> mapEntry : map.entrySet()){
            resultMap.put(Value.get(mapEntry.getKey()), wrapBytesMap(mapEntry.getValue()));
        }
        return resultMap;
    }

    private static Map<Value, Value> wrapBytesMap(Map<byte[], byte[]> map){
        Map<Value, Value> resultMap = new HashMap<>(map.size());
        for(Map.Entry<byte[], byte[]> mapEntry : map.entrySet()){
            resultMap.put(Value.get(mapEntry.getKey()), Value.get(mapEntry.getValue()));
        }
        return resultMap;
    }
}
