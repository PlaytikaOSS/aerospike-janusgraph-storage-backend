package com.playtika.janusgraph.aerospike.operations.batch;

import com.aerospike.client.Value;

import java.util.Map;

public class UpdateValue {
    public final String storeName;
    public final Value key;
    public final Map<Value, Value> values;

    public UpdateValue(String storeName, Value key, Map<Value, Value> values) {
        this.storeName = storeName;
        this.key = key;
        this.values = values;
    }
}
