package com.playtika.janusgraph.aerospike.operations;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeySlicesIterator;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.playtika.janusgraph.aerospike.operations.AerospikeOperations.ENTRIES_BIN_NAME;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toMap;

/**
 * Should be used for test purposes only
 */
public class AerospikeKeySlicesIterator implements KeySlicesIterator, ScanCallback {

    private static final Logger logger = LoggerFactory.getLogger(AerospikeKeySlicesIterator.class);

    private final List<SliceQuery> queries;
    private final BlockingQueue<KeyRecord> queue = new LinkedBlockingQueue<>(100);
    private KeyRecord next;
    private Map<SliceQuery, RecordIterator<Entry>> entriesIt;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private Thread thread;

    private static final KeyRecord TERMINATE_VALUE = new KeyRecord(null, null);

    public AerospikeKeySlicesIterator(List<SliceQuery> queries) {
        this.queries = queries;
    }

    @Override
    public Map<SliceQuery, RecordIterator<Entry>> getEntries() {
        return entriesIt;
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
            StaticArrayBuffer key = keyToBuffer(next.key);
            logger.info("key=[{}]", key);
            return key;
        } finally {
            next = null;
        }
    }

    private KeyRecord takeNext() throws InterruptedException {
//        do {
            next = queue.take();
            if (next == TERMINATE_VALUE) {
                entriesIt = null;
                return next;
            }
            entriesIt = entries(next);
//        } while(!entriesIt.hasNext());

        return next;
    }

    private Map<SliceQuery, RecordIterator<Entry>> entries(KeyRecord keyRecord){
        List<Entry> entries = keyRecord.record.getMap(ENTRIES_BIN_NAME).entrySet().stream()
                .map(o -> {
                    Map.Entry<ByteBuffer, byte[]> entry = (Map.Entry<ByteBuffer, byte[]>) o;
                    final StaticBuffer column = StaticArrayBuffer.of(entry.getKey());
                    final StaticBuffer value = StaticArrayBuffer.of(entry.getValue());
                    return StaticArrayEntry.of(column, value);
                }).collect(Collectors.toList());
        Map<SliceQuery, List<Entry>> entriesByQuery = new HashMap<>();

        entries.forEach(entry -> {
            queries.forEach(sliceQuery -> {
                if(sliceQuery.contains(entry.getColumn())) {
                    List<Entry> entriesPerQuery = entriesByQuery.computeIfAbsent(sliceQuery,
                            k -> new ArrayList<>());
                    entriesPerQuery.add(entry);
                }
            });
        });
        return queries.stream()
                .collect(toMap(
                        query -> query,
                        query -> new AerospikeRecordIterator(
                                entriesByQuery.getOrDefault(query, emptyList()), query.getLimit())
                ));
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

    private static class AerospikeRecordIterator implements RecordIterator<Entry> {
        private final Iterator<Entry> iterator;


        private AerospikeRecordIterator(List<Entry> entries, int limit) {
            Collections.sort(entries);
            this.iterator = entries.subList(0, Math.min(limit, entries.size())).iterator();
        }

        @Override
        public void close() {
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Entry next() {
            return iterator.next();
        }
    }
}
