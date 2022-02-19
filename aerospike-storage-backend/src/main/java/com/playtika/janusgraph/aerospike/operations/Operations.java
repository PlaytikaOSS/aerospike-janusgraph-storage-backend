package com.playtika.janusgraph.aerospike.operations;

import com.aerospike.client.Value;
import com.playtika.janusgraph.aerospike.operations.batch.BatchLocks;
import com.playtika.janusgraph.aerospike.operations.batch.BatchUpdates;
import com.playtika.janusgraph.aerospike.operations.batch.WalOperations;
import nosql.batch.update.BatchUpdater;
import nosql.batch.update.aerospike.lock.AerospikeLock;
import nosql.batch.update.wal.WriteAheadLogCompleter;

public interface Operations {

    AerospikeOperations getAerospikeOperations();

    BatchUpdater<BatchLocks, BatchUpdates, AerospikeLock, Value> batchUpdater();

    WriteAheadLogCompleter<BatchLocks, BatchUpdates, AerospikeLock, Value> getWriteAheadLogCompleter();

    MutateOperations getMutateOperations();

    ReadOperations getReadOperations();

    ScanOperations getScanOperations();

    WalOperations getWalOperations();

    void close();
}
