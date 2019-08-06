package com.playtika.janusgraph.aerospike.operations;

import com.aerospike.client.Value;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.playtika.janusgraph.aerospike.operations.FlakingUtils.entriesSize;
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

            if(entriesSize(mutationsByStorePartial) < entriesSize(mutationsByStore)) {
                throw new RuntimeException();
            }

        } else {
            mutateOperations.mutateMany(mutationsByStore);
        }
    }

    @Override
    public void mutate(String storeName, Value key, Map<Value, Value> mutation) {
        mutateOperations.mutate(storeName, key, mutation);
    }
}
