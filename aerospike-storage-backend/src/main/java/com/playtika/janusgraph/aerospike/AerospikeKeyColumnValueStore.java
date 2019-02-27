// Copyright 2018 William Esz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.playtika.janusgraph.aerospike;

import com.aerospike.client.*;
import com.aerospike.client.cdt.*;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.reactor.AerospikeReactorClient;
import org.janusgraph.diskstorage.*;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.*;
import org.janusgraph.diskstorage.util.EntryArrayList;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.playtika.janusgraph.aerospike.ConfigOptions.NAMESPACE;
import static java.util.Collections.singletonList;

public class AerospikeKeyColumnValueStore implements KeyColumnValueStore {

    static final String ENTRIES_BIN_NAME = "entries";

    private static final MapPolicy mapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE);

    private final String namespace;
    private final String name; //used as set name
    private final AerospikeClient client;
    private final AerospikeReactorClient reactorClient;
    private final LockOperations lockOperations;

    AerospikeKeyColumnValueStore(String name,
                                 AerospikeClient client, AerospikeReactorClient reactorClient,
                                 Configuration configuration) {
        this.namespace = configuration.get(NAMESPACE);
        this.name = name;
        this.client = client;
        this.reactorClient = reactorClient;
        this.lockOperations = new LockOperations(namespace, name, reactorClient, this, configuration);
    }

    @Override // This method is only supported by stores which keep keys in byte-order.
    public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override // This method is only supported by stores which do not keep keys in byte-order.
    public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws BackendException {
        ScanPolicy scanPolicy = new ScanPolicy();
        scanPolicy.includeBinData = false;

        client.scanAll(scanPolicy, namespace, name, (key, record) -> {
            int debug = 0;
        });

        return null;
    }

    @Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws BackendException {

        try {
            Record record = client.operate(null, getKey(query.getKey()),
                    MapOperation.getByKeyRange(ENTRIES_BIN_NAME,
                            getValue(query.getSliceStart()), getValue(query.getSliceEnd()), MapReturnType.KEY_VALUE)
            );

            List<?> resultList;
            if(record != null
                    && (resultList = record.getList(ENTRIES_BIN_NAME)) != null
                    && !resultList.isEmpty()) {
                final EntryArrayList result = new EntryArrayList();
                resultList.stream()
                        .limit(query.getLimit())
                        .forEach(o -> {
                            Map.Entry<?, ?> entry = (Map.Entry<?, ?>) o;
                            result.add(StaticArrayEntry.of(
                                    StaticArrayBuffer.of((byte[]) entry.getKey()),
                                    StaticArrayBuffer.of((byte[]) entry.getValue())));
                        });
                return result;
            } else {
                return EntryList.EMPTY_LIST;
            }

        } catch (AerospikeException e) {
            throw new PermanentBackendException(e);
        }
    }

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws BackendException {
        try {
            AerospikeTransaction transaction = (AerospikeTransaction)txh;

            AerospikeLocks locks = new AerospikeLocks(transaction.getLocks().size() + 1);
            locks.addLocks(transaction.getLocks());
            locks.addLockOnKey(key);

            lockOperations.acquireLocks(locks.getLocksMap())
                    .then(mutateReactive(key, additions, deletions))
                    .onErrorMap(AerospikeException.class, PermanentBackendException::new)
                    //here we just release lock on key
                    // locks that comes from transaction will be released by commit or rollback
                    .doFinally(signalType -> lockOperations.releaseLockOnKeys(singletonList(key)).block())
                    .then().block();

        } catch (AerospikeException e) {
            throw new PermanentBackendException(e);
        }
    }

    Mono<KeyRecord> mutateReactive(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions) {

        Map<Value,Value> itemsToAdd = new HashMap<>(additions.size());
        for(Entry addition : additions){
            itemsToAdd.put(getValue(addition.getColumn()), getValue(addition.getValue()));
        }
        List<Value> keysToRemove = new ArrayList<>(deletions.size());
        for(StaticBuffer deletion : deletions){
            keysToRemove.add(getValue(deletion));
        }

        return reactorClient.operate(null, getKey(key),
                MapOperation.removeByKeyList(ENTRIES_BIN_NAME, keysToRemove, MapReturnType.NONE),
                MapOperation.putItems(mapPolicy, ENTRIES_BIN_NAME, itemsToAdd)
        );
    }

    @Override
    public Map<StaticBuffer,EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws BackendException {
        final Map<StaticBuffer, EntryList> result = new HashMap<>();

        for (StaticBuffer key : keys)
            result.put(key, getSlice(new KeySliceQuery(key, query), txh));

        return result;
    }

    Key getKey(StaticBuffer staticBuffer) {
        return new Key(namespace, name, staticBuffer.getBytes(0, staticBuffer.length()));
    }

    static Value getValue(StaticBuffer staticBuffer) {
        return Value.get(staticBuffer.getBytes(0, staticBuffer.length()));
    }

    @Override
    public void acquireLock(final StaticBuffer key, final StaticBuffer column, final StaticBuffer expectedValue, final StoreTransaction txh) {
        ((AerospikeTransaction)txh).addLock(new AerospikeLock(name, key, column, expectedValue));
    }

    @Override
    public synchronized void close() {}

    @Override
    public String getName() {
        return name;
    }

    public LockOperations getLockOperations() {
        return lockOperations;
    }
}
