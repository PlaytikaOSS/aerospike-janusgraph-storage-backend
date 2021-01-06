package com.playtika.janusgraph.aerospike.operations.batch;

import com.playtika.janusgraph.aerospike.operations.BasicMutateOperations;
import nosql.batch.update.UpdateOperations;

import java.util.concurrent.ExecutorService;

import static com.playtika.janusgraph.aerospike.util.AsyncUtil.completeAll;

public class BatchUpdateOperations implements UpdateOperations<BatchUpdates> {

    private final BasicMutateOperations mutateOperations;
    private final ExecutorService executorService;

    public BatchUpdateOperations(BasicMutateOperations mutateOperations, ExecutorService executorService) {
        this.mutateOperations = mutateOperations;
        this.executorService = executorService;
    }

    @Override
    public void updateMany(BatchUpdates batchUpdates, boolean calledByWal) {
        completeAll(batchUpdates.getUpdates(),
                updateValue -> true,
                updateValue -> {
                    mutateOperations.mutate(
                            updateValue.storeName, updateValue.key, updateValue.values);
                    return true;
                },
                () -> null,
                executorService);
   }
}
