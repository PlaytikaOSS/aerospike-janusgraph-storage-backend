package com.playtika.janusgraph.aerospike.operations;

import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;

public class UnsupportedScanOperations implements ScanOperations {

    @Override
    public KeyIterator getKeys(String storeName, SliceQuery query, StoreTransaction txh) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {}
}
