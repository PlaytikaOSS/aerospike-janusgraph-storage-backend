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
import java.util.concurrent.ExecutorService;

public class BatchOperationsUtil {

    public static BatchOperations<BatchLocks, BatchUpdates, AerospikeLock, Value> batchOperations(
            AerospikeOperations aerospikeOperations,
            String walNamespace,
            String walSetName,
            Clock clock,
            ExecutorService aerospikeExecutorService,
            ExecutorService batchExecutorService){

        AerospikeWriteAheadLogManager<BatchLocks, BatchUpdates, Map<Key, ExpectedValue>> walManager =
                walManager(aerospikeOperations, walNamespace, walSetName, clock, aerospikeExecutorService);

        AerospikeLockOperations<BatchLocks, Map<Key, ExpectedValue>> lockOperations =
                lockOperations(aerospikeOperations, aerospikeExecutorService);

        BatchUpdateOperations updateOperations = updateOperations(aerospikeOperations);

        return new BatchOperations<>(walManager, lockOperations, updateOperations, batchExecutorService);
    }

    public static BatchUpdateOperations updateOperations(AerospikeOperations aerospikeOperations) {
        return new BatchUpdateOperations(new BasicMutateOperations(aerospikeOperations), aerospikeOperations.getAerospikeExecutor());
    }

    public static AerospikeLockOperations<BatchLocks, Map<Key, ExpectedValue>> lockOperations(
            AerospikeOperations aerospikeOperations, ExecutorService executorService) {
        return new AerospikeLockOperations<>(
                aerospikeOperations.getClient(),
                new BatchExpectedValueOperations(aerospikeOperations),
                executorService);
    }

    public static AerospikeWriteAheadLogManager<BatchLocks, BatchUpdates, Map<Key, ExpectedValue>> walManager(
            AerospikeOperations aerospikeOperations,
            String walNamespace, String walSetName,
            Clock clock, ExecutorService executorService) {
        return new AerospikeWriteAheadLogManager<>(
                aerospikeOperations.getClient(), walNamespace, walSetName,
                new BatchUpdateSerde(aerospikeOperations),
                clock/*, executorService*/);
    }

}
