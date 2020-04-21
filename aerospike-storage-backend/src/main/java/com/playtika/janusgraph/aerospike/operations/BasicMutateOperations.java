package com.playtika.janusgraph.aerospike.operations;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.cdt.MapWriteMode;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import com.playtika.janusgraph.aerospike.AerospikePolicyProvider;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.playtika.janusgraph.aerospike.operations.AerospikeOperations.ENTRIES_BIN_NAME;

public class BasicMutateOperations implements MutateOperations{

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
    public Mono<Void> mutate(String storeName, Value key, Map<Value, Value> mutation, boolean calledByWal) {
        Key aerospikeKey = aerospikeOperations.getKey(storeName, key);
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

        int entriesNoOperationIndex;
        if(!keysToRemove.isEmpty()){
            entriesNoOperationIndex = operations.size();
            operations.add(MapOperation.size(ENTRIES_BIN_NAME));
        } else {
            entriesNoOperationIndex = -1;
        }

        IAerospikeReactorClient client = aerospikeOperations.getReactorClient();
        return client.operate(mutatePolicy, aerospikeKey, operations.toArray(new Operation[0]))
                .onErrorResume(throwable -> throwable instanceof AerospikeException
                        && calledByWal
                        && ((AerospikeException)throwable).getResultCode() == ResultCode.KEY_NOT_FOUND_ERROR,
                        throwable -> Mono.empty())
                .flatMap(keyRecord -> {
                    if(entriesNoOperationIndex != -1){
                        long entriesNoAfterMutation = (Long)keyRecord.record.getList(ENTRIES_BIN_NAME).get(entriesNoOperationIndex);
                        if(entriesNoAfterMutation == 0){
                            return client.delete(deletePolicy, aerospikeKey);
                        }
                    }
                    return Mono.empty();
                }).then();
    }

    private static WritePolicy buildMutationPolicy(AerospikePolicyProvider policyProvider){
        WritePolicy mutatePolicy = new WritePolicy(policyProvider.writePolicy());
        mutatePolicy.respondAllOps = true;
        return mutatePolicy;
    }


}
