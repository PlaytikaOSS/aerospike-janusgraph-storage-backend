package com.playtika.janusgraph.aerospike.operations.batch;

import com.playtika.janusgraph.aerospike.operations.BasicMutateOperations;
import nosql.batch.update.UpdateOperations;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        BatchUpdates.UpdatesOrdered updatesOrdered = batchUpdates.getUpdates();
        completeAll(concat(updatesOrdered.indexDeletion, updatesOrdered.edgeCreation),
                updateValue -> true,
                updateValue -> {
                    mutateOperations.mutate(
                            updateValue.storeName, updateValue.key, updateValue.values);
                    return true;
                },
                () -> null,
                executorService);

        completeAll(concat(updatesOrdered.indexCreation, updatesOrdered.edgeDeletion),
                updateValue -> true,
                updateValue -> {
                    mutateOperations.mutate(
                            updateValue.storeName, updateValue.key, updateValue.values);
                    return true;
                },
                () -> null,
                executorService);

        completeAll(updatesOrdered.other,
                updateValue -> true,
                updateValue -> {
                    mutateOperations.mutate(
                            updateValue.storeName, updateValue.key, updateValue.values);
                    return true;
                },
                () -> null,
                executorService);
   }

   private static <V> List<V> concat(List<V> list1, List<V> list2){
        if(list1.isEmpty()){
            return list2;
        } else if(list2.isEmpty()){
            return list1;
        } else {
            List<V> result = new ArrayList<>(list1.size() + list2.size());
            result.addAll(list1);
            result.addAll(list2);
            return result;
        }
   }
}
