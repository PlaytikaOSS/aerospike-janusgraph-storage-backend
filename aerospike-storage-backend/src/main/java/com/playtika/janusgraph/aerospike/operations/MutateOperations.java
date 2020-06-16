package com.playtika.janusgraph.aerospike.operations;

import com.aerospike.client.Value;
import org.janusgraph.diskstorage.PermanentBackendException;

import java.util.Map;

public interface MutateOperations {

    void mutateMany(Map<String, Map<Value, Map<Value, Value>>> mutationsByStore, boolean wal) throws PermanentBackendException;

    void mutate(String storeName, Value key, Map<Value, Value> mutation, boolean wal);
}
