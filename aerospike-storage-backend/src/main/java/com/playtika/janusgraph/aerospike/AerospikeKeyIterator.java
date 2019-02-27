package com.playtika.janusgraph.aerospike;

import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.util.RecordIterator;

import java.io.IOException;
import java.util.Iterator;

//Draft implementation that keeps all keys in memory
//TODO think about async version
public class AerospikeKeyIterator implements KeyIterator {

//    private Iterator<>

    @Override
    public RecordIterator<Entry> getEntries() {
        return null;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public StaticBuffer next() {
        return null;
    }
}
