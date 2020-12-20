package com.playtika.janusgraph.aerospike.operations.batch;

import com.playtika.janusgraph.aerospike.operations.BasicMutateOperations;
import nosql.batch.update.UpdateOperations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class BatchUpdateOperations implements UpdateOperations<BatchUpdates> {

    private final BasicMutateOperations mutateOperations;

    public BatchUpdateOperations(BasicMutateOperations mutateOperations) {
        this.mutateOperations = mutateOperations;
    }

    @Override
    public Mono<Void> updateMany(BatchUpdates batchUpdates, boolean calledByWal) {
        return Flux.fromIterable(batchUpdates.getUpdates())
                .flatMap(updateValue -> mutateOperations.mutate(
                        updateValue.storeName, updateValue.key, updateValue.values))
                .then();
   }
}
