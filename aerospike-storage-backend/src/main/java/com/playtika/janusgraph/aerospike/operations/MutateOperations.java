package com.playtika.janusgraph.aerospike.operations;

import com.aerospike.client.Value;

import java.util.Map;

public interface MutateOperations {

    void mutate(String storeName, Value key, Map<Value, Value> mutation);
}
