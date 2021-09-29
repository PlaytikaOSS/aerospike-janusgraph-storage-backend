package com.playtika.janusgraph.aerospike.operations.batch;

import com.aerospike.client.Value;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.janusgraph.diskstorage.Backend.EDGESTORE_NAME;
import static org.janusgraph.diskstorage.Backend.INDEXSTORE_NAME;
import static org.janusgraph.graphdb.configuration.JanusGraphConstants.JANUSGRAPH_ID_STORE_NAME;

public class BatchUpdates {

    public static final String INDEX_STORE_NAME = INDEXSTORE_NAME;
    public static final String EDGE_STORE_NAME = EDGESTORE_NAME;
    public static final String IDS_STORE_NAME = JANUSGRAPH_ID_STORE_NAME;

    private final UpdatesOrdered updatesOrdered;
    private final Map<String, Map<Value, Map<Value, Value>>> updatesByStore;

    public BatchUpdates(Map<String, Map<Value, Map<Value, Value>>> updatesByStore){
        this.updatesOrdered = orderUpdates(updatesByStore);
        this.updatesByStore = updatesByStore;
    }

    /**
     * Order updates [indexes deletion, edge deletion, edge creation, index creation]
     * @param updatesByStore
     * @return
     */
    private static UpdatesOrdered orderUpdates(Map<String, Map<Value, Map<Value, Value>>> updatesByStore){
        List<UpdateValue> indexDeletion = new ArrayList<>();
        List<UpdateValue> edgeDeletion = new ArrayList<>();
        List<UpdateValue> edgeCreation = new ArrayList<>();
        List<UpdateValue> indexCreation = new ArrayList<>();
        List<UpdateValue> other = new ArrayList<>();

        for(Map.Entry<String, Map<Value, Map<Value, Value>>> updatesByStoreEntry : updatesByStore.entrySet()){
            String storeName = updatesByStoreEntry.getKey();
            for(Map.Entry<Value, Map<Value, Value>> keyUpdate : updatesByStoreEntry.getValue().entrySet()){
                Value key = keyUpdate.getKey();

                switch (storeName) {
                    case INDEX_STORE_NAME: {
                        Split<Map<Value, Value>> split = splitToCreationAndDeletion(keyUpdate.getValue());
                        if (split.created != null) {
                            indexCreation.add(new UpdateValue(storeName, key, split.created));
                        }
                        if (split.deleted != null) {
                            indexDeletion.add(new UpdateValue(storeName, key, split.deleted));
                        }
                        break;
                    }
                    case EDGE_STORE_NAME: {
                        Split<Map<Value, Value>> split = splitToCreationAndDeletion(keyUpdate.getValue());
                        if (split.created != null) {
                            edgeCreation.add(new UpdateValue(storeName, key, split.created));
                        }
                        if (split.deleted != null) {
                            edgeDeletion.add(new UpdateValue(storeName, key, split.deleted));
                        }
                        break;
                    }
                    default:
                        other.add(new UpdateValue(storeName, key, keyUpdate.getValue()));
                }
            }
        }
        return new UpdatesOrdered(indexDeletion, edgeDeletion, edgeCreation, indexCreation, other);
    }

    static class UpdatesOrdered {
        final List<UpdateValue> indexDeletion;
        final List<UpdateValue> edgeDeletion;
        final List<UpdateValue> edgeCreation;
        final List<UpdateValue> indexCreation;
        final List<UpdateValue> other;

        UpdatesOrdered(List<UpdateValue> indexDeletion,
                       List<UpdateValue> edgeDeletion,
                       List<UpdateValue> edgeCreation,
                       List<UpdateValue> indexCreation,
                       List<UpdateValue> other) {
            this.indexDeletion = indexDeletion;
            this.edgeDeletion = edgeDeletion;
            this.edgeCreation = edgeCreation;
            this.indexCreation = indexCreation;
            this.other = other;
        }
    }

    private static Split<Map<Value, Value>> splitToCreationAndDeletion(Map<Value, Value> updates){
        boolean onlyCreation = true;
        boolean onlyDeletion = true;
        for(Map.Entry<Value, Value> updateEntry : updates.entrySet()){
            if(updateEntry.getValue() == null){
                onlyCreation = false;
            } else {
                onlyDeletion = false;
            }
        }

        if(onlyCreation){
            return new Split<>(updates, null);
        } else if(onlyDeletion){
            return new Split<>(null, updates);
        } else {
            Map<Value, Value> creation = new HashMap<>(updates.size());
            Map<Value, Value> deletion = new HashMap<>(updates.size());
            for(Map.Entry<Value, Value> updateEntry : updates.entrySet()){
                if(updateEntry.getValue() == null){
                    deletion.put(updateEntry.getKey(), null);
                } else {
                    deletion.put(updateEntry.getKey(), updateEntry.getValue());
                }
            }
            return new Split<>(creation, deletion);
        }
    }

    private static class Split<V>{

        final V created;
        final V deleted;

        public Split(V created, V deleted) {
            this.created = created;
            this.deleted = deleted;
        }
    }

    public UpdatesOrdered getUpdates() {
        return updatesOrdered;
    }

    public Map<String, Map<Value, Map<Value, Value>>> getUpdatesByStore() {
        return updatesByStore;
    }
}
