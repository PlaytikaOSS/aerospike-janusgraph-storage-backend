package com.playtika.janusgraph.aerospike;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.playtika.janusgraph.aerospike.AerospikeKeyColumnValueStore.ENTRIES_BIN_NAME;

/**
 * Should be used for test purposes only
 */
public class AerospikeKeyIterator implements KeyIterator, ScanCallback {

    private BlockingQueue<KeyRecord> queue = new LinkedBlockingQueue<>(100);
    private KeyRecord next;
    private KeyRecord current;

    private static final KeyRecord TERMINATE_VALUE = new KeyRecord(null, null);

    @Override
    public RecordIterator<Entry> getEntries() {

        final Iterator<Entry> entriesIt = current.record.getMap(ENTRIES_BIN_NAME).entrySet().stream()
                .map(o -> {
                    Map.Entry<ByteBuffer, byte[]> entry = (Map.Entry<ByteBuffer, byte[]>)o;
                    final StaticBuffer column = StaticArrayBuffer.of(entry.getKey());
                    final StaticBuffer value = StaticArrayBuffer.of(entry.getValue());
                    return StaticArrayEntry.of(column, value);
                }).iterator();

        return new RecordIterator<Entry>() {
            @Override
            public boolean hasNext() {
                return entriesIt.hasNext();
            }

            @Override
            public Entry next() {
                return entriesIt.next();
            }

            @Override
            public void close() {
            }
        };
    }

    @Override
    public void close() {}

    @Override
    public boolean hasNext() {
        try {
            return next != null || (next = queue.take()) != TERMINATE_VALUE;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public StaticBuffer next() {
        if(next == null){
            try {
                next = queue.take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        if(next == TERMINATE_VALUE){
            throw new NoSuchElementException();
        }
        try {
            return new StaticArrayBuffer((byte[]) next.key.userKey.getObject());
        } finally {
            current = next;
            next = null;
        }
    }

    @Override
    public void scanCallback(Key key, Record record) throws AerospikeException {
        try {
            queue.put(new KeyRecord(key, record));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static class KeyRecord {
        final Key key;
        final Record record;

        private KeyRecord(Key key, Record record) {
            this.key = key;
            this.record = record;
        }
    }

    public void terminate(){
        try {
            queue.put(TERMINATE_VALUE);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
