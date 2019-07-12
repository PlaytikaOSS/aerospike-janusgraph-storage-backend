package com.playtika.janusgraph.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.ClientPolicy;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.playtika.janusgraph.aerospike.util.NamedThreadFactory;
import com.playtika.janusgraph.aerospike.wal.WriteAheadLogCompleter;
import com.playtika.janusgraph.aerospike.wal.WriteAheadLogManager;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.StoreMetaData;
import org.janusgraph.diskstorage.common.AbstractStoreManager;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.playtika.janusgraph.aerospike.ConfigOptions.*;
import static com.playtika.janusgraph.aerospike.util.AerospikeUtils.isEmptyNamespace;
import static com.playtika.janusgraph.aerospike.util.AerospikeUtils.truncateNamespace;
import static com.playtika.janusgraph.aerospike.util.AsyncUtil.shutdownAndAwaitTermination;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;


@PreInitializeConfigOptions
public class AerospikeStoreManager extends AbstractStoreManager implements KeyColumnValueStoreManager {

    private static final int DEFAULT_PORT = 3000;
    public static final int AEROSPIKE_BUFFER_SIZE = Integer.MAX_VALUE / 2;
    public static final String JANUS_AEROSPIKE_THREAD_GROUP_NAME = "janus-aerospike";

    private final StoreFeatures features;

    private final IAerospikeClient client;

    private final String namespace;

    private final LockOperations lockOperations;
    private final TransactionalOperations transactionalOperations;

    private final WriteAheadLogManager writeAheadLogManager;
    private final WriteAheadLogCompleter writeAheadLogCompleter;

    private final ThreadPoolExecutor scanExecutor;

    private final ThreadPoolExecutor aerospikeExecutor;
    private final String graphPrefix;
    private final AerospikePolicyProvider aerospikePolicyProvider;

    public AerospikeStoreManager(Configuration configuration) {
        super(configuration);

        Preconditions.checkArgument(configuration.get(BUFFER_SIZE) == AEROSPIKE_BUFFER_SIZE,
                "Set unlimited buffer size as we use deferred locking approach");

        client = buildAerospikeClient(configuration);

        this.namespace = configuration.get(NAMESPACE);
        this.graphPrefix = configuration.get(GRAPH_PREFIX);

        features = features(configuration);

        String walNamespace = configuration.get(WAL_NAMESPACE);
        Long staleTransactionLifetimeThresholdInMs = configuration.get(WAL_STALE_TRANSACTION_LIFETIME_THRESHOLD);
        String walSetName = graphPrefix + ".wal";

        scanExecutor = new ThreadPoolExecutor(0, configuration.get(SCAN_PARALLELISM),
                1, TimeUnit.MINUTES, new LinkedBlockingQueue<>(),
                new NamedThreadFactory(JANUS_AEROSPIKE_THREAD_GROUP_NAME, "scan"));

        aerospikeExecutor = new ThreadPoolExecutor(4, configuration.get(AEROSPIKE_PARALLELISM),
                1, TimeUnit.MINUTES, new LinkedBlockingQueue<>(),
                new NamedThreadFactory(JANUS_AEROSPIKE_THREAD_GROUP_NAME, "main"));

        aerospikePolicyProvider = buildPolicyProvider(configuration);

        lockOperations = new LockOperations(client, namespace, graphPrefix, aerospikeExecutor, aerospikePolicyProvider);

        writeAheadLogManager = new WriteAheadLogManager(client, walNamespace, walSetName,
                getClock(), staleTransactionLifetimeThresholdInMs, aerospikePolicyProvider);

        transactionalOperations = initTransactionalOperations(this::openDatabase, writeAheadLogManager,
                lockOperations, aerospikeExecutor);

        writeAheadLogCompleter = new WriteAheadLogCompleter(client, walNamespace, walSetName,
                writeAheadLogManager, staleTransactionLifetimeThresholdInMs, transactionalOperations);

        writeAheadLogCompleter.start();
    }

    TransactionalOperations initTransactionalOperations(Function<String, AKeyColumnValueStore> databaseFactory,
                                                        WriteAheadLogManager writeAheadLogManager,
                                                        LockOperations lockOperations, ThreadPoolExecutor aerospikeExecutor) {
        return new TransactionalOperations(databaseFactory, writeAheadLogManager, lockOperations, aerospikeExecutor);
    }

    private StandardStoreFeatures features(Configuration configuration) {
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
        return clientPolicy;
    }

    private AerospikePolicyProvider buildPolicyProvider(Configuration configuration) {
        return configuration.get(TEST_ENVIRONMENT) ? new TestAerospikePolicyProvider() : new AerospikePolicyProvider();
    }

    @Override
    public StoreTransaction beginTransaction(final BaseTransactionConfig config) {
        return new AerospikeTransaction(config);
    }

    @Override
    public AKeyColumnValueStore openDatabase(String name) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name), "Database name may not be null or empty");

        return new AerospikeKeyColumnValueStore(namespace, graphPrefix, name,
                client, lockOperations, aerospikeExecutor, scanExecutor,
                writeAheadLogManager, aerospikePolicyProvider);
    }

    @Override
    public KeyColumnValueStore openDatabase(String name, StoreMetaData.Container metaData) {
        return openDatabase(name);
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {
        transactionalOperations.mutateManyTransactionally(mutations, txh);
    }

    @Override
    public void close() {
        writeAheadLogCompleter.shutdown();
        shutdownAndAwaitTermination(scanExecutor);
        shutdownAndAwaitTermination(aerospikeExecutor);
        client.close();
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


    WriteAheadLogManager getWriteAheadLogManager() {
        return writeAheadLogManager;
    }

    public WriteAheadLogCompleter getWriteAheadLogCompleter() {
        return writeAheadLogCompleter;
    }
}
