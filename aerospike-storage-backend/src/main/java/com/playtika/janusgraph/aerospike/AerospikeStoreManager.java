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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.playtika.janusgraph.aerospike.ConfigOptions.*;
import static java.util.Collections.emptyMap;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;


@PreInitializeConfigOptions
public class AerospikeStoreManager extends AbstractStoreManager implements KeyColumnValueStoreManager {

    private static final int DEFAULT_PORT = 3000;

    private final StoreFeatures features;

    private final AerospikeClient client;

    private final Configuration configuration;
    private final boolean pessimisticLocking;

    public AerospikeStoreManager(Configuration configuration) {
        super(configuration);

        Preconditions.checkArgument(configuration.get(BUFFER_SIZE) == Integer.MAX_VALUE,
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
        pessimisticLocking = configuration.get(PESSIMISTIC_LOCKING);

        features = new StandardStoreFeatures.Builder()
                .keyConsistent(configuration)
                .persists(true)
                .locking(pessimisticLocking)
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
    }

    static void registerUdfs(AerospikeClient client){
        client.register(null, AerospikeStoreManager.class.getClassLoader(),
                "udf/check_and_lock.lua", "check_and_lock.lua", Language.LUA);
    }

    @Override
    public AerospikeKeyColumnValueStore openDatabase(String name) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name), "Database name may not be null or empty");

        return new AerospikeKeyColumnValueStore(name, client, configuration);
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
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {
        acquireLocks(((AerospikeTransaction) txh).getLocks(), mutations);

        try {
            mutateMany(mutations);
        } catch (AerospikeException e) {
            //here we just release lock on key
            // locks that comes from transaction will be released by rollback
            releaseLocks(mutations);
            throw new PermanentBackendException(e);
        }
    }

    private void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations) {
        mutations.forEach((storeName, entry) -> {
            final AerospikeKeyColumnValueStore store = openDatabase(storeName);
            entry.forEach((key, mutation) -> store.mutate(
                    key, mutation.getAdditions(), mutation.getDeletions(), pessimisticLocking));
        });
    }

    private void acquireLocks(List<AerospikeLock> locks, Map<String, Map<StaticBuffer, KCVMutation>> mutations) throws BackendException {
        Map<String, List<AerospikeLock>> locksByStore = locks.stream()
                .collect(Collectors.groupingBy(lock -> lock.storeName));
        for(Map.Entry<String, List<AerospikeLock>> entry : locksByStore.entrySet()){
            String storeName = entry.getKey();
            List<AerospikeLock> locksForStore = entry.getValue();
            Map<StaticBuffer, KCVMutation> mutationsForStore = mutations.getOrDefault(storeName, emptyMap());
            AerospikeLocks locksAll = new AerospikeLocks(locksForStore.size() + mutationsForStore.size());
            locksAll.addLocks(locksForStore);
            if (pessimisticLocking) {
                locksAll.addLockOnKeys(mutationsForStore.keySet());
            }

            final AerospikeKeyColumnValueStore store = openDatabase(storeName);
            store.getLockOperations().acquireLocks(locksAll.getLocksMap());
        }
    }

    private void releaseLocks(Map<String, Map<StaticBuffer, KCVMutation>> mutations){
        if(pessimisticLocking){
            mutations.forEach((storeName, mutationsForStore) -> {
                final AerospikeKeyColumnValueStore store = openDatabase(storeName);
                store.getLockOperations().releaseLockOnKeys(mutationsForStore.keySet());
            });
        }
    }

    //called from AerospikeTransaction
    void releaseLocks(List<AerospikeLock> locks){
        Map<String, List<AerospikeLock>> locksByStore = locks.stream()
                .collect(Collectors.groupingBy(lock -> lock.storeName));
        locksByStore.forEach((storeName, locksForStore) -> {
            AerospikeLocks locksAll = new AerospikeLocks(locksForStore.size());
            locksAll.addLocks(locksForStore);
            final AerospikeKeyColumnValueStore store = openDatabase(storeName);
            store.getLockOperations().releaseLockOnKeys(locksAll.getLocksMap().keySet());
        });
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
