package com.playtika.janusgraph.aerospike;

import com.aerospike.client.Value;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;

import java.util.Map;

public interface AKeyColumnValueStore extends KeyColumnValueStore {

    void mutate(Value key, Map<Value, Value> mutation);
}
