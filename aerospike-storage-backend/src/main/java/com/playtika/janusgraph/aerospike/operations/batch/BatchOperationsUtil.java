package com.playtika.janusgraph.aerospike.operations.batch;

import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.playtika.janusgraph.aerospike.operations.AerospikeOperations;
import com.playtika.janusgraph.aerospike.operations.BasicMutateOperations;
import nosql.batch.update.BatchOperations;
import nosql.batch.update.aerospike.lock.AerospikeLock;
import nosql.batch.update.aerospike.lock.AerospikeLockOperations;
import nosql.batch.update.aerospike.wal.AerospikeWriteAheadLogManager;

import java.time.Clock;
import java.util.Map;

public class BatchOperationsUtil {

    public static BatchOperations<BatchLocks, BatchUpdates, AerospikeLock, Value> batchOperations(
            AerospikeOperations aerospikeOperations,
            String walNamespace,
            String walSetName,
            Clock clock){

        AerospikeWriteAheadLogManager<BatchLocks, BatchUpdates, Map<Key, ExpectedValue>> walManager =
                walManager(aerospikeOperations, walNamespace, walSetName, clock);

        AerospikeLockOperations<BatchLocks, Map<Key, ExpectedValue>> lockOperations =
                lockOperations(aerospikeOperations);

        BatchUpdateOperations updateOperations = updateOperations(aerospikeOperations);

        return new BatchOperations<>(walManager, lockOperations, updateOperations);
    }

    public static BatchUpdateOperations updateOperations(AerospikeOperations aerospikeOperations) {
        return new BatchUpdateOperations(new BasicMutateOperations(aerospikeOperations));
    }

    public static AerospikeLockOperations<BatchLocks, Map<Key, ExpectedValue>> lockOperations(
            AerospikeOperations aerospikeOperations) {
        return new AerospikeLockOperations<>(
                aerospikeOperations.getReactorClient(),
                new BatchExpectedValueOperations(aerospikeOperations));
    }

    public static AerospikeWriteAheadLogManager<BatchLocks, BatchUpdates, Map<Key, ExpectedValue>> walManager(
            AerospikeOperations aerospikeOperations, String walNamespace, String walSetName, Clock clock) {
        return new AerospikeWriteAheadLogManager<>(
                aerospikeOperations.getClient(), aerospikeOperations.getReactorClient(), walNamespace, walSetName,
                new BatchUpdateSerde(aerospikeOperations),
                clock);
    }

}
