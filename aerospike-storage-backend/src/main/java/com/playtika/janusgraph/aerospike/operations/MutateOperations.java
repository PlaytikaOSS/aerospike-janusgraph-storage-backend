package com.playtika.janusgraph.aerospike.operations;

import com.aerospike.client.Value;
import reactor.core.publisher.Mono;

import java.util.Map;

public interface MutateOperations {

    void mutate(String storeName, Value key, Map<Value, Value> mutation);
}
