package com.playtika.janusgraph.aerospike.operations;

import com.aerospike.client.Value;
import reactor.core.publisher.Mono;

import java.util.Map;

public interface MutateOperations {

    Mono<Void> mutate(String storeName, Value key, Map<Value, Value> mutation, boolean calledByWal);
}
