package com.playtika.janusgraph.aerospike.operations;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.playtika.janusgraph.aerospike.operations.AerospikeOperations.ENTRIES_BIN_NAME;

/**
 * Should be used for test purposes only
 */
public class AerospikeKeyIterator implements KeyIterator, ScanCallback {

    private static Logger logger = LoggerFactory.getLogger(AerospikeKeyIterator.class);

    private final SliceQuery query;
    private final BlockingQueue<KeyRecord> queue = new LinkedBlockingQueue<>(100);
    private KeyRecord next;
    private Iterator<Entry> entriesIt;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private Thread thread;

    private static final KeyRecord TERMINATE_VALUE = new KeyRecord(null, null);

    public AerospikeKeyIterator(SliceQuery query) {
        this.query = query;
    }

    @Override
    public RecordIterator<Entry> getEntries() {

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
    public void close() {
        closed.set(true);
        try {
            queue.put(TERMINATE_VALUE);
            thread.interrupt();
        } catch (InterruptedException e) {
            throw new RuntimeException();
        }
    }

    @Override
    public boolean hasNext() {
        try {
            if(next == TERMINATE_VALUE){
                return false;
            }

            return next != null || (next = takeNext()) != TERMINATE_VALUE;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public StaticBuffer next() {
        if(next == null){
            try {
                next = takeNext();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        if(next == TERMINATE_VALUE){
            throw new NoSuchElementException();
        }
        try {
            return keyToBuffer(next.key);
        } finally {
            next = null;
        }
    }

    private KeyRecord takeNext() throws InterruptedException {
        do {
            next = queue.take();
            if (next == TERMINATE_VALUE) {
                entriesIt = null;
                break;
            }
            entriesIt = entriesIt(next);
        } while(!entriesIt.hasNext());

        return next;
    }

    private Iterator<Entry> entriesIt(KeyRecord keyRecord){
        return keyRecord.record.getMap(ENTRIES_BIN_NAME).entrySet().stream()
                .map(o -> {
                    Map.Entry<ByteBuffer, byte[]> entry = (Map.Entry<ByteBuffer, byte[]>)o;
                    final StaticBuffer column = StaticArrayBuffer.of(entry.getKey());
                    final StaticBuffer value = StaticArrayBuffer.of(entry.getValue());
                    return StaticArrayEntry.of(column, value);
                })
                .filter(entry -> query.contains(entry.getColumn()))
                .limit(query.getLimit())
                .iterator();
    }

    @Override
    public void scanCallback(Key key, Record record) throws AerospikeException {
        if (closed.get()) {
            logger.info("AerospikeKeyIterator get closed, terminate scan");
            throw new AerospikeException.ScanTerminated();
        }
        try {
            queue.put(new KeyRecord(key, record));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static StaticArrayBuffer keyToBuffer(Key key){
        return new StaticArrayBuffer((byte[]) key.userKey.getObject());
    }

    private static class KeyRecord {
        final Key key;
        final Record record;

        private KeyRecord(Key key, Record record) {
            this.key = key;
            this.record = record;
        }
    }

    public void setThread(Thread thread) {
        this.thread = thread;
    }
}
