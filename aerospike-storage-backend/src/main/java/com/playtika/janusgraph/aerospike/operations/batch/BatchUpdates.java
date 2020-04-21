package com.playtika.janusgraph.aerospike.operations.batch;

import com.aerospike.client.Value;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BatchUpdates {

    private final List<UpdateValue> updates;
    private Map<String, Map<Value, Map<Value, Value>>> updatesByStore;

    public BatchUpdates(Map<String, Map<Value, Map<Value, Value>>> updatesByStore){
        updates = updatesByStore.entrySet().stream()
                .flatMap(storeEntry -> storeEntry.getValue().entrySet().stream()
                        .map(keyEntry -> new UpdateValue(storeEntry.getKey(), keyEntry.getKey(), keyEntry.getValue())))
                .collect(Collectors.toList());
        this.updatesByStore = updatesByStore;
    }

    public List<UpdateValue> getUpdates() {
        return updates;
    }

    public Map<String, Map<Value, Map<Value, Value>>> getUpdatesByStore() {
        return updatesByStore;
    }
}
