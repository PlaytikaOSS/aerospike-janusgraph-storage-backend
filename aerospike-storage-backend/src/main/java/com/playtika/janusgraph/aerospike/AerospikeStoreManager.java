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

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.async.NioEventLoops;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.reactor.AerospikeReactorClient;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.janusgraph.diskstorage.*;
import org.janusgraph.diskstorage.common.AbstractStoreManager;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.*;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.playtika.janusgraph.aerospike.ConfigOptions.NAMESPACE;
import static com.playtika.janusgraph.aerospike.ConfigOptions.PESSIMISTIC_LOCKING;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_HOSTS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_PORT;


@PreInitializeConfigOptions
public class AerospikeStoreManager extends AbstractStoreManager implements KeyColumnValueStoreManager {

    private static final int DEFAULT_PORT = 3000;

    private final StoreFeatures features;

    private final AerospikeClient client;
    private final AerospikeReactorClient reactorClient;

    private final Configuration configuration;
    private final boolean pessimisticLocking;

    public AerospikeStoreManager(Configuration configuration) {
        super(configuration);
        int port = storageConfig.has(STORAGE_PORT) ? storageConfig.get(STORAGE_PORT) : DEFAULT_PORT;

        Host[] hosts = Stream.of(configuration.get(STORAGE_HOSTS))
                .map(hostname -> new Host(hostname, port)).toArray(Host[]::new);

        ClientPolicy clientPolicy = new ClientPolicy();
        NioEventLoops eventLoops = new NioEventLoops();
        clientPolicy.eventLoops = eventLoops;
//        clientPolicy.user = storageConfig.get(AUTH_USERNAME);
//        clientPolicy.password = storageConfig.get(AUTH_PASSWORD);

        client = new AerospikeClient(clientPolicy, hosts);

        reactorClient = new AerospikeReactorClient(client, eventLoops);

        this.configuration = configuration;
        pessimisticLocking = configuration.get(PESSIMISTIC_LOCKING);

        features = new StandardStoreFeatures.Builder()
                .keyConsistent(configuration)
                .persists(true)
                .locking(pessimisticLocking)
                .distributed(true)
                .multiQuery(true)
                .batchMutation(true)
                .unorderedScan(true)
                .orderedScan(false)
                .keyOrdered(false)
                .localKeyPartition(false)
                .timestamps(false)
                .transactional(false)
                .supportsInterruption(false)
                .build();
    }

    @Override
    public AerospikeKeyColumnValueStore openDatabase(String name) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name), "Database name may not be null or empty");

        return new AerospikeKeyColumnValueStore(name, client, reactorClient, configuration);
    }

    @Override
    public KeyColumnValueStore openDatabase(String name, StoreMetaData.Container metaData) {
        return openDatabase(name);
    }

    @Override
    public StoreTransaction beginTransaction(final BaseTransactionConfig config) {
        return new AerospikeTransaction(config, this);
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) {
        acquireLocks(((AerospikeTransaction)txh).getLocks(), mutations)
                .thenMany(mutateManyReactive(mutations))
                .onErrorMap(AerospikeException.class, PermanentBackendException::new)
                //here we just release lock on key
                // locks that comes from transaction will be released by commit or rollback
                .doFinally(signalType -> releaseLocks(mutations).block())
                .then().block();
    }

    private Flux<KeyRecord> mutateManyReactive(Map<String, Map<StaticBuffer, KCVMutation>> mutations) {
        return Flux.fromIterable(mutations.entrySet())
                .flatMap(entries -> {
                    final AerospikeKeyColumnValueStore store = openDatabase(entries.getKey());
                    return Flux.fromIterable(entries.getValue().entrySet())
                            .flatMap(mutationEntry -> {
                                final StaticBuffer key = mutationEntry.getKey();
                                final KCVMutation mutation = mutationEntry.getValue();
                                return store.mutateReactive(key, mutation.getAdditions(), mutation.getDeletions());
                            });
                });
    }

    private Mono<Void> acquireLocks(List<AerospikeLock> locks, Map<String, Map<StaticBuffer, KCVMutation>> mutations) {
        Map<String, List<AerospikeLock>> locksByStore = locks.stream()
                .collect(Collectors.groupingBy(lock -> lock.storeName));

        return Flux.fromIterable(locksByStore.entrySet())
                .flatMap(entry -> {
                    String storeName = entry.getKey();

                    List<AerospikeLock> locksForStore = entry.getValue();
                    Set<StaticBuffer> keysToLock = mutations.get(storeName).keySet();
                    AerospikeLocks locksAll = new AerospikeLocks(locksForStore.size() + keysToLock.size());
                    locksAll.addLocks(locksForStore);
                    if(pessimisticLocking) {
                        locksAll.addLockOnKeys(keysToLock);
                    }

                    final AerospikeKeyColumnValueStore store = openDatabase(storeName);
                    return store.getLockOperations().acquireLocks(locksAll.getLocksMap());
                }).then();
    }

    private Mono<Void> releaseLocks(Map<String, Map<StaticBuffer, KCVMutation>> mutations){
        return pessimisticLocking
                ? Flux.fromIterable(mutations.entrySet())
                .flatMap(entry -> {
                    String storeName = entry.getKey();
                    final AerospikeKeyColumnValueStore store = openDatabase(storeName);
                    return store.getLockOperations().releaseLockOnKeys(entry.getValue().keySet());
                }).then()
                : Mono.empty();
    }

    //called from AerospikeTransaction
    void releaseLocks(List<AerospikeLock> locks){
        Map<String, List<AerospikeLock>> locksByStore = locks.stream()
                .collect(Collectors.groupingBy(lock -> lock.storeName));

        Flux.fromIterable(locksByStore.entrySet())
                .flatMap(entry -> {
                    String storeName = entry.getKey();
                    List<AerospikeLock> locksForStore = entry.getValue();
                    AerospikeLocks locksAll = new AerospikeLocks(locksForStore.size());
                    locksAll.addLocks(locksForStore);

                    final AerospikeKeyColumnValueStore store = openDatabase(storeName);
                    return store.getLockOperations().releaseLockOnKeys(locksAll.getLocksMap().keySet());
                }).then().block();
    }

    @Override
    public void close() throws BackendException {
        try {
            client.close();
        } catch (AerospikeException e) {
            throw new PermanentBackendException(e);
        }
    }

    @Override
    public void clearStorage() throws BackendException {
        try {
            client.truncate(null, configuration.get(NAMESPACE), null, null);
        } catch (AerospikeException e) {
            throw new PermanentBackendException(e);
        }
    }

    @Override
    public boolean exists() throws BackendException {
        return true;
    }

    @Override
    public String getName() {
        return getClass().getSimpleName() + ":" + "HARDCODED";
    }

    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() {
        throw new UnsupportedOperationException();
    }




}
