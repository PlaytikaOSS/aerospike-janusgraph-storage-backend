package com.playtika.janusgraph.aerospike.operations;

import com.aerospike.client.Value;
import org.janusgraph.diskstorage.PermanentBackendException;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.playtika.janusgraph.aerospike.operations.FlakingUtils.selectFlaking;

public class FlakingMutateOperations implements MutateOperations {

    private MutateOperations mutateOperations;
    private AtomicBoolean fails;

    public FlakingMutateOperations(MutateOperations mutateOperations, AtomicBoolean fails) {
        this.mutateOperations = mutateOperations;
        this.fails = fails;
    }

    @Override
    public void mutateMany(Map<String, Map<Value, Map<Value, Value>>> mutationsByStore) throws PermanentBackendException {
        if(fails.get()){
            Map<String, Map<Value, Map<Value, Value>>> mutationsByStorePartial = selectFlaking(mutationsByStore,
                    "mutateMany failed flaking in [{}] for key [{}]");

            mutateOperations.mutateMany(mutationsByStorePartial);
            throw new RuntimeException();

        } else {
            mutateOperations.mutateMany(mutationsByStore);
        }
    }

    @Override
    public void mutate(String storeName, Value key, Map<Value, Value> mutation) {
        mutateOperations.mutate(storeName, key, mutation);
    }
}
