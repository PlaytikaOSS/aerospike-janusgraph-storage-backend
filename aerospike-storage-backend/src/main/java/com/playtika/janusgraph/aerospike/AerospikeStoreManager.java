package com.playtika.janusgraph.aerospike;

import com.aerospike.client.*;
import com.aerospike.client.policy.ClientPolicy;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.janusgraph.diskstorage.*;
import org.janusgraph.diskstorage.common.AbstractStoreManager;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.*;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.playtika.janusgraph.aerospike.ConfigOptions.*;
import static com.playtika.janusgraph.aerospike.util.AsyncUtil.allOf;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.CompletableFuture.runAsync;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;


@PreInitializeConfigOptions
public class AerospikeStoreManager extends AbstractStoreManager implements KeyColumnValueStoreManager {

    private static final int DEFAULT_PORT = 3000;
    static final int AEROSPIKE_BUFFER_SIZE = Integer.MAX_VALUE / 2;

    private final StoreFeatures features;

    private final AerospikeClient client;

    private final Configuration configuration;

    private final ThreadPoolExecutor scanExecutor;

    private final ThreadPoolExecutor aerospikeExecutor;

    public AerospikeStoreManager(Configuration configuration) {
        super(configuration);

        Preconditions.checkArgument(configuration.get(BUFFER_SIZE) == AEROSPIKE_BUFFER_SIZE,
                "Set unlimited buffer size as we use deferred locking approach");

        int port = storageConfig.has(STORAGE_PORT) ? storageConfig.get(STORAGE_PORT) : DEFAULT_PORT;

        Host[] hosts = Stream.of(configuration.get(STORAGE_HOSTS))
                .map(hostname -> new Host(hostname, port)).toArray(Host[]::new);

        ClientPolicy clientPolicy = new ClientPolicy();
//        clientPolicy.user = storageConfig.get(AUTH_USERNAME);
//        clientPolicy.password = storageConfig.get(AUTH_PASSWORD);
        if(configuration.get(ALLOW_SCAN)){
            clientPolicy.writePolicyDefault.sendKey = true;
            clientPolicy.readPolicyDefault.sendKey = true;
            clientPolicy.scanPolicyDefault.sendKey = true;
        }

        client = new AerospikeClient(clientPolicy, hosts);

        this.configuration = configuration;

        features = new StandardStoreFeatures.Builder()
                .keyConsistent(configuration)
                .persists(true)
                //here we promise to take care of locking.
                //If false janusgraph will do it via ExpectedValueCheckingStoreManager that is less effective
                .locking(true)
                //caused by deferred locking approach used in this storage backend,
                //actual locking happens just before transaction commit
                .optimisticLocking(true)
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

        registerUdfs(client);

        scanExecutor = new ThreadPoolExecutor(0, configuration.get(SCAN_PARALLELISM),
                1, TimeUnit.MINUTES, new LinkedBlockingQueue<>());

        aerospikeExecutor = new ThreadPoolExecutor(4, configuration.get(AEROSPIKE_PARALLELISM),
                1, TimeUnit.MINUTES, new LinkedBlockingQueue<>());
    }

    static void registerUdfs(AerospikeClient client){
        client.register(null, AerospikeStoreManager.class.getClassLoader(),
                "udf/check_and_lock.lua", "check_and_lock.lua", Language.LUA);
    }

    @Override
    public AerospikeKeyColumnValueStore openDatabase(String name) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name), "Database name may not be null or empty");

        return new AerospikeKeyColumnValueStore(name, client, configuration, aerospikeExecutor, scanExecutor);
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
        List<StoreLocks> locksByStore = acquireLocks(((AerospikeTransaction) txh).getLocks());

        try {
            Map<String, Set<StaticBuffer>> mutatedByStore = mutateMany(mutations);
            releaseLocks(locksByStore, mutatedByStore);
        } catch (AerospikeException e) {
            releaseLocks(locksByStore, emptyMap());
            throw new PermanentBackendException(e);
        }
    }

    private Map<String, Set<StaticBuffer>> mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations) throws PermanentBackendException {

        List<CompletableFuture<?>> futures = new ArrayList<>();
        Map<String, Set<StaticBuffer>> mutatedByStore = new ConcurrentHashMap<>();

        mutations.forEach((storeName, entry) -> {
            final AerospikeKeyColumnValueStore store = openDatabase(storeName);
            entry.forEach((key, mutation) -> futures.add(runAsync(() -> {
                store.mutate(key, mutation.getAdditions(), mutation.getDeletions());
                mutatedByStore.compute(storeName, (s, keys) -> {
                    Set<StaticBuffer> keysResult = keys != null ? keys : new HashSet<>();
                    keysResult.add(key);
                    return keysResult;
                });
            }, aerospikeExecutor)));
        });

        allOf(futures);

        return mutatedByStore;
    }

    private List<StoreLocks> acquireLocks(List<AerospikeLock> locks) throws BackendException {
        Map<String, List<AerospikeLock>> locksByStore = locks.stream()
                .collect(Collectors.groupingBy(lock -> lock.storeName));
        List<StoreLocks> locksAllByStore = new ArrayList<>(locksByStore.size());
        for(Map.Entry<String, List<AerospikeLock>> entry : locksByStore.entrySet()){
            String storeName = entry.getKey();
            List<AerospikeLock> locksForStore = entry.getValue();
            StoreLocks storeLocks = new StoreLocks(storeName, locksForStore);
            final AerospikeKeyColumnValueStore store = openDatabase(storeName);
            store.getLockOperations().acquireLocks(storeLocks.locksMap);
            locksAllByStore.add(storeLocks);
        }
        return locksAllByStore;
    }

    private void releaseLocks(List<StoreLocks> locksByStore, Map<String, Set<StaticBuffer>> mutatedByStore) throws PermanentBackendException {
        for(StoreLocks storeLocks : locksByStore){
            final AerospikeKeyColumnValueStore store = openDatabase(storeLocks.storeName);
            Set<StaticBuffer> mutatedForStore = mutatedByStore.get(storeLocks.storeName);
            List<StaticBuffer> keysToRelease = storeLocks.locksMap.keySet().stream()
                    //ignore mutated keys as they already have been released
                    .filter(key -> !mutatedForStore.contains(key))
                    .collect(Collectors.toList());
            store.getLockOperations().releaseLockOnKeys(keysToRelease);
        }
    }

    @Override
    public void close() throws BackendException {
        try {
            scanExecutor.shutdown();
            aerospikeExecutor.shutdown();
            client.close();
        } catch (AerospikeException e) {
            throw new PermanentBackendException(e);
        }
    }

    @Override
    public void clearStorage() throws BackendException {
        try {
            while(!emptyStorage()){
                client.truncate(null, configuration.get(NAMESPACE), null, null);
                Thread.sleep(100);
            }

        } catch (AerospikeException e) {
            throw new PermanentBackendException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean exists() throws BackendException {
        try {
            return !emptyStorage();
        } catch (AerospikeException e) {
            throw new PermanentBackendException(e);
        }
    }

    private boolean emptyStorage(){
        String namespace = configuration.get(NAMESPACE);
        String answer = Info.request(client.getNodes()[0], "sets/" + namespace);
        return Stream.of(answer.split(";"))
                .allMatch(s -> s.contains("objects=0"));
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
