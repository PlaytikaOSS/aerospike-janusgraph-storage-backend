package com.playtika.janusgraph.aerospike.operations.batch;

public class BatchUpdate implements nosql.batch.update.BatchUpdate<BatchLocks, BatchUpdates>{

    private final BatchLocks batchLocks;
    private final BatchUpdates batchUpdates;

    public BatchUpdate(BatchLocks batchLocks, BatchUpdates batchUpdates) {
        this.batchLocks = batchLocks;
        this.batchUpdates = batchUpdates;
    }

    @Override
    public BatchLocks locks() {
        return batchLocks;
    }

    @Override
    public BatchUpdates updates() {
        return batchUpdates;
    }



}
