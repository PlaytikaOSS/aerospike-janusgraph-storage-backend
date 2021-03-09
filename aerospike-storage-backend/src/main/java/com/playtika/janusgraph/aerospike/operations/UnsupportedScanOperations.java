package com.playtika.janusgraph.aerospike.operations;

import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.keycolumnvalue.KeySlicesIterator;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;

import java.util.List;

public class UnsupportedScanOperations implements ScanOperations {

    @Override
    public KeyIterator getKeys(String storeName, SliceQuery query, StoreTransaction txh) {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeySlicesIterator getKeys(String storeName, List<SliceQuery> queries, StoreTransaction txh) {
        throw new UnsupportedOperationException();
    }

}
