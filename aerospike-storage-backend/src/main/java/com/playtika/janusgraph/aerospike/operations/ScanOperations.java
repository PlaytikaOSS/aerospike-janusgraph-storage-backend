package com.playtika.janusgraph.aerospike.operations;

import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.keycolumnvalue.KeySlicesIterator;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;

import java.util.List;

public interface ScanOperations {

    KeyIterator getKeys(String storeName, SliceQuery query, StoreTransaction txh);

    KeySlicesIterator getKeys(String storeName, List<SliceQuery> queries, StoreTransaction txh);

}
