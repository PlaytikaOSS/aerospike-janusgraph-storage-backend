package com.playtika.janusgraph.aerospike.operations;

import com.playtika.janusgraph.aerospike.transaction.TransactionalOperations;
import com.playtika.janusgraph.aerospike.transaction.WriteAheadLogCompleter;

public interface Operations {

    AerospikeOperations getAerospikeOperations();

    TransactionalOperations getTransactionalOperations();

    WriteAheadLogCompleter getWriteAheadLogCompleter();

    ReadOperations getReadOperations();

    ScanOperations getScanOperations();

    void close();
}
