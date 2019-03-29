package com.playtika.janusgraph.aerospike;

import com.aerospike.client.*;
import com.aerospike.client.policy.ClientPolicy;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.playtika.janusgraph.aerospike.wal.WriteAheadLogCompleter;
import com.playtika.janusgraph.aerospike.wal.WriteAheadLogManager;
import org.janusgraph.diskstorage.*;
import org.janusgraph.diskstorage.common.AbstractStoreManager;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.*;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;

import java.time.Clock;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.playtika.janusgraph.aerospike.AerospikeKeyColumnValueStore.getValue;
import static com.playtika.janusgraph.aerospike.ConfigOptions.*;
import static com.playtika.janusgraph.aerospike.util.AerospikeUtils.isEmptyNamespace;
import static com.playtika.janusgraph.aerospike.util.AerospikeUtils.truncateNamespace;
import static com.playtika.janusgraph.aerospike.util.AsyncUtil.completeAll;
import static com.playtika.janusgraph.aerospike.util.AsyncUtil.shutdownAndAwaitTermination;
import static java.util.Arrays.asList;
import static java.util.concurrent.CompletableFuture.runAsync;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;


@PreInitializeConfigOptions
public class AerospikeStoreManager extends AbstractStoreManager implements KeyColumnValueStoreManager {

    private static final int DEFAULT_PORT = 3000;
    static final int AEROSPIKE_BUFFER_SIZE = Integer.MAX_VALUE / 2;

    private final StoreFeatures features;

    private final IAerospikeClient client;

    private final Configuration configuration;
    private final String namespace;

    private final LockOperations lockOperations;

    private final WriteAheadLogManager writeAheadLogManager;
    private final WriteAheadLogCompleter writeAheadLogCompleter;

    private final ThreadPoolExecutor scanExecutor;

    private final ThreadPoolExecutor aerospikeExecutor;
    private final String graphPrefix;


    public AerospikeStoreManager(Configuration configuration) {
        super(configuration);

        Preconditions.checkArgument(configuration.get(BUFFER_SIZE) == AEROSPIKE_BUFFER_SIZE,
                "Set unlimited buffer size as we use deferred locking approach");

        client = buildAerospikeClient(configuration);

        this.configuration = configuration;
        this.namespace = configuration.get(NAMESPACE);
        this.graphPrefix = configuration.get(GRAPH_PREFIX);

        features = features(configuration);

        String walNamespace = configuration.get(WAL_NAMESPACE);
        Long staleTransactionLifetimeThresholdInMs = configuration.get(WAL_STALE_TRANSACTION_LIFETIME_THRESHOLD);
        String walSetName = graphPrefix + ".wal";
        writeAheadLogManager = new WriteAheadLogManager(client, walNamespace, walSetName,
                getClock(), staleTransactionLifetimeThresholdInMs);
        writeAheadLogCompleter = new WriteAheadLogCompleter(client, walNamespace, walSetName,
                writeAheadLogManager, staleTransactionLifetimeThresholdInMs, this);
        writeAheadLogCompleter.start();

        scanExecutor = new ThreadPoolExecutor(0, configuration.get(SCAN_PARALLELISM),
                1, TimeUnit.MINUTES, new LinkedBlockingQueue<>());

        aerospikeExecutor = new ThreadPoolExecutor(4, configuration.get(AEROSPIKE_PARALLELISM),
                1, TimeUnit.MINUTES, new LinkedBlockingQueue<>());

        lockOperations = new LockOperations(client, namespace, graphPrefix, aerospikeExecutor);
    }

    protected StandardStoreFeatures features(Configuration configuration) {
        return new StandardStoreFeatures.Builder()
                .keyConsistent(configuration)
                .persists(true)
                //here we promise to take care of locking.
                //If false janusgraph will do it via ExpectedValueCheckingStoreManager that is less effective
                .locking(true)
                //caused by deferred locking approach used in this storage backend,
                //actual locking happens just before transaction commit
                .optimisticLocking(true)
                .transactional(false)
                .distributed(true)
                .multiQuery(true)
                .batchMutation(true)
                .unorderedScan(true)
                .orderedScan(false)
                .keyOrdered(false)
                .localKeyPartition(false)
                .timestamps(false)
                .supportsInterruption(false)
                .build();
    }

    Clock getClock() {
        return Clock.systemUTC();
    }

    private IAerospikeClient buildAerospikeClient(Configuration configuration){
        int port = storageConfig.has(STORAGE_PORT) ? storageConfig.get(STORAGE_PORT) : DEFAULT_PORT;

        Host[] hosts = Stream.of(configuration.get(STORAGE_HOSTS))
                .map(hostname -> new Host(hostname, port)).toArray(Host[]::new);

        ClientPolicy clientPolicy = buildClientPolicy();

        return new AerospikeClient(clientPolicy, hosts);
    }

    private ClientPolicy buildClientPolicy() {
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.user = storageConfig.has(AUTH_USERNAME) ? storageConfig.get(AUTH_USERNAME) : null;
        clientPolicy.password = storageConfig.has(AUTH_PASSWORD) ? storageConfig.get(AUTH_PASSWORD) : null;
        clientPolicy.writePolicyDefault.sendKey = true;
        clientPolicy.readPolicyDefault.sendKey = true;
        clientPolicy.scanPolicyDefault.sendKey = true;
        return clientPolicy;
    }

    @Override
    public AKeyColumnValueStore openDatabase(String name) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name), "Database name may not be null or empty");

        return new AerospikeKeyColumnValueStore(namespace, graphPrefix, name,
                client, configuration, lockOperations, scanExecutor, writeAheadLogManager);
    }

    @Override
    public KeyColumnValueStore openDatabase(String name, StoreMetaData.Container metaData) {
        return openDatabase(name);
    }

    @Override
    public StoreTransaction beginTransaction(final BaseTransactionConfig config) {
        return new AerospikeTransaction(config);
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {
        Map<String, Map<Value, Map<Value, Value>>> locksByStore = groupLocksByStoreKeyColumn(
                ((AerospikeTransaction) txh).getLocks());

        Map<String, Map<Value, Map<Value, Value>>> mutationsByStore = groupMutationsByStoreKeyColumn(mutations);

        Value transactionId = writeAheadLogManager.writeTransaction(locksByStore, mutationsByStore);

        processAndDeleteTransaction(transactionId, locksByStore, mutationsByStore, false);
    }

    public void processAndDeleteTransaction(Value transactionId,
                                             Map<String, Map<Value, Map<Value, Value>>> locksByStore,
                                             Map<String, Map<Value, Map<Value, Value>>> mutationsByStore,
                                             boolean checkTransactionId) throws BackendException {
        Set<Key> keysLocked = lockOperations.acquireLocks(transactionId, locksByStore, checkTransactionId);
        try {
            mutateMany(mutationsByStore);
        } catch (AerospikeException e) {
            throw new PermanentBackendException(e);
        } finally {
            releaseLocks(keysLocked);
            deleteWalTransaction(transactionId);
        }
    }

    void releaseLocks(Set<Key> keysLocked) {
        lockOperations.releaseLocks(keysLocked);
    }

    void deleteWalTransaction(Value transactionId) {
        writeAheadLogManager.deleteTransaction(transactionId);
    }

    static Map<String, Map<Value, Map<Value, Value>>> groupLocksByStoreKeyColumn(List<AerospikeLock> locks){
        return locks.stream()
                .collect(Collectors.groupingBy(lock -> lock.storeName,
                        Collectors.groupingBy(lock -> getValue(lock.key),
                                Collectors.toMap(
                                        lock -> getValue(lock.column),
                                        lock -> lock.expectedValue != null ? getValue(lock.expectedValue) : Value.NULL,
                                        (oldValue, newValue) -> oldValue))));
    }

    private static Map<String, Map<Value, Map<Value, Value>>> groupMutationsByStoreKeyColumn(
            Map<String, Map<StaticBuffer, KCVMutation>> mutationsByStore){
        Map<String, Map<Value, Map<Value, Value>>> mapByStore = new HashMap<>(mutationsByStore.size());
        for(Map.Entry<String, Map<StaticBuffer, KCVMutation>> storeMutations : mutationsByStore.entrySet()) {
            Map<Value, Map<Value, Value>> map = new HashMap<>(storeMutations.getValue().size());
            for (Map.Entry<StaticBuffer, KCVMutation> mutationEntry : storeMutations.getValue().entrySet()) {
                map.put(getValue(mutationEntry.getKey()), mutationToMap(mutationEntry.getValue()));
            }
            mapByStore.put(storeMutations.getKey(), map);
        }
        return mapByStore;
    }

    static Map<Value, Value> mutationToMap(KCVMutation mutation){
        Map<Value, Value> map = new HashMap<>(mutation.getAdditions().size() + mutation.getDeletions().size());
        for(StaticBuffer deletion : mutation.getDeletions()){
            map.put(getValue(deletion), Value.NULL);
        }

        for(Entry addition : mutation.getAdditions()){
            map.put(getValue(addition.getColumn()), getValue(addition.getValue()));
        }
        return map;
    }

    private void mutateMany(
            Map<String, Map<Value, Map<Value, Value>>> mutationsByStore) throws PermanentBackendException {

        List<CompletableFuture<?>> mutations = new ArrayList<>();

        mutationsByStore.forEach((storeName, storeMutations) -> {
            final AKeyColumnValueStore store = openDatabase(storeName);
            for(Map.Entry<Value, Map<Value, Value>> mutationEntry : storeMutations.entrySet()){
                Value key = mutationEntry.getKey();
                Map<Value, Value> mutation = mutationEntry.getValue();
                mutations.add(runAsync(() -> store.mutate(key, mutation), aerospikeExecutor));
            }
        });

        completeAll(mutations);
    }

    @Override
    public void close() throws BackendException {
        completeAll(asList(
                runAsync(writeAheadLogCompleter::shutdown),
                runAsync(() -> shutdownAndAwaitTermination(scanExecutor)),
                runAsync(() -> shutdownAndAwaitTermination(aerospikeExecutor)),
                runAsync(client::close)
        ));
    }

    @Override
    public void clearStorage() throws BackendException {
        try {
            truncateNamespace(client, namespace);

        } catch (AerospikeException e) {
            throw new PermanentBackendException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean exists() throws BackendException {
        try {
            return !isEmptyNamespace(client, namespace);
        } catch (AerospikeException e) {
            throw new PermanentBackendException(e);
        }
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
