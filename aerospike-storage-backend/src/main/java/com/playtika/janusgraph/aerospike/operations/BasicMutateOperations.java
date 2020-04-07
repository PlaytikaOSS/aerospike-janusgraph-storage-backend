package com.playtika.janusgraph.aerospike.operations;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.cdt.MapWriteMode;
import com.aerospike.client.policy.WritePolicy;
import com.playtika.janusgraph.aerospike.AerospikePolicyProvider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.playtika.janusgraph.aerospike.operations.AerospikeOperations.ENTRIES_BIN_NAME;
import static com.playtika.janusgraph.aerospike.util.AsyncUtil.completeAll;
import static java.util.concurrent.CompletableFuture.runAsync;

public class BasicMutateOperations implements MutateOperations {

    static final MapPolicy mapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE);

    private final WritePolicy mutatePolicy;
    private final WritePolicy deletePolicy;
    private AerospikeOperations aerospikeOperations;

    public BasicMutateOperations(AerospikeOperations aerospikeOperations) {
        this.aerospikeOperations = aerospikeOperations;

        AerospikePolicyProvider aerospikePolicyProvider = aerospikeOperations.getAerospikePolicyProvider();
        this.mutatePolicy = buildMutationPolicy(aerospikePolicyProvider);
        this.deletePolicy = aerospikePolicyProvider.deletePolicy();
    }

    @Override
    public void mutateMany(Map<String, Map<Value, Map<Value, Value>>> mutationsByStore) {

        List<CompletableFuture<?>> mutations = new ArrayList<>();

        mutationsByStore.forEach((storeName, storeMutations) -> {
            for(Map.Entry<Value, Map<Value, Value>> mutationEntry : storeMutations.entrySet()){
                Value key = mutationEntry.getKey();
                Map<Value, Value> mutation = mutationEntry.getValue();
                mutations.add(runAsync(() -> mutate(storeName, key, mutation),
                        aerospikeOperations.getAerospikeExecutor()));
            }
        });

        completeAll(mutations);
    }

    @Override
    public void mutate(String storeName, Value keyValue, Map<Value, Value> mutation) {
        Key key = aerospikeOperations.getKey(storeName, keyValue);
        List<Operation> operations = new ArrayList<>(3);

        List<Value> keysToRemove = new ArrayList<>(mutation.size());
        Map<Value, Value> itemsToAdd = new HashMap<>(mutation.size());
        for(Map.Entry<Value, Value> entry : mutation.entrySet()){
            if(entry.getValue() == Value.NULL){
                keysToRemove.add(entry.getKey());
            } else {
                itemsToAdd.put(entry.getKey(), entry.getValue());
            }
        }

        if(!keysToRemove.isEmpty()) {
            operations.add(MapOperation.removeByKeyList(ENTRIES_BIN_NAME, keysToRemove, MapReturnType.NONE));
        }

        if(!itemsToAdd.isEmpty()) {
            operations.add(MapOperation.putItems(mapPolicy, ENTRIES_BIN_NAME, itemsToAdd));
        }

        int entriesNoOperationIndex = -1;
        if(!keysToRemove.isEmpty()){
            entriesNoOperationIndex = operations.size();
            operations.add(MapOperation.size(ENTRIES_BIN_NAME));
        }

        IAerospikeClient client = aerospikeOperations.getClient();
        Record record = client.operate(mutatePolicy, key, operations.toArray(new Operation[0]));
        if(entriesNoOperationIndex != -1){
            long entriesNoAfterMutation = (Long)record.getList(ENTRIES_BIN_NAME).get(entriesNoOperationIndex);
            if(entriesNoAfterMutation == 0){
                client.delete(deletePolicy, key);
            }
        }
    }

    private static WritePolicy buildMutationPolicy(AerospikePolicyProvider policyProvider){
        WritePolicy mutatePolicy = new WritePolicy(policyProvider.writePolicy());
        mutatePolicy.respondAllOps = true;
        return mutatePolicy;
    }


}
