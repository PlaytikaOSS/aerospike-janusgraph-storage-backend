package com.playtika.janusgraph.aerospike.operations;

import com.aerospike.client.IAerospikeClient;
import com.playtika.janusgraph.aerospike.AerospikePolicyProvider;
import com.playtika.janusgraph.aerospike.TestAerospikePolicyProvider;
import com.playtika.janusgraph.aerospike.transaction.TransactionalOperations;
import com.playtika.janusgraph.aerospike.transaction.WalOperations;
import com.playtika.janusgraph.aerospike.transaction.WriteAheadLogCompleter;
import com.playtika.janusgraph.aerospike.transaction.WriteAheadLogManager;
import com.playtika.janusgraph.aerospike.transaction.WriteAheadLogManagerBasic;
import com.playtika.janusgraph.aerospike.util.NamedThreadFactory;
import org.janusgraph.diskstorage.configuration.Configuration;

import java.time.Clock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.playtika.janusgraph.aerospike.ConfigOptions.*;
import static com.playtika.janusgraph.aerospike.operations.AerospikeOperations.buildAerospikeClient;

public class BasicOperations implements Operations {

    public static final String JANUS_AEROSPIKE_THREAD_GROUP_NAME = "janus-aerospike";

    private final Configuration configuration;
    private final AerospikePolicyProvider aerospikePolicyProvider;
    private final AerospikeOperations aerospikeOperations;
    private final WalOperations walOperations;
    private final WriteAheadLogManager writeAheadLogManager;
    private final LockOperations lockOperations;
    private final MutateOperations mutateOperations;
    private final TransactionalOperations transactionalOperations;

    private final WriteAheadLogCompleter writeAheadLogCompleter;

    private final ReadOperations readOperations;
    private final ScanOperations scanOperations;

    public BasicOperations(Configuration configuration) {
        this.configuration = configuration;
        this.aerospikePolicyProvider = initPolicyProvider(configuration);
        this.aerospikeOperations = initAerospikeOperations(configuration, aerospikePolicyProvider);
        this.walOperations = initWalOperations(configuration, aerospikeOperations);
        this.writeAheadLogManager = initWriteAheadLogManager(walOperations, getClock());
        this.lockOperations = initLockOperations(aerospikeOperations);
        this.mutateOperations = initMutateOperations(aerospikeOperations);
        this.transactionalOperations = initTransactionalOperations(
                () -> writeAheadLogManager, () -> lockOperations, () -> mutateOperations);
        this.writeAheadLogCompleter = buildWriteAheadLogCompleter(walOperations,
                () -> writeAheadLogManager, () -> lockOperations, () -> mutateOperations);

        this.readOperations = initReadOperations(aerospikeOperations);
        this.scanOperations = initScanOperations(configuration, aerospikeOperations);
    }

    @Override
    public AerospikeOperations getAerospikeOperations() {
        return aerospikeOperations;
    }

    @Override
    public TransactionalOperations getTransactionalOperations() {
        return transactionalOperations;
    }

    @Override
    public WriteAheadLogCompleter getWriteAheadLogCompleter() {
        return writeAheadLogCompleter;
    }

    @Override
    public ReadOperations getReadOperations() {
        return readOperations;
    }

    @Override
    public ScanOperations getScanOperations() {
        return scanOperations;
    }

    protected AerospikePolicyProvider initPolicyProvider(Configuration configuration){
        return configuration.get(TEST_ENVIRONMENT) ? new TestAerospikePolicyProvider() : new AerospikePolicyProvider();
    }

    protected AerospikeOperations initAerospikeOperations(Configuration configuration, AerospikePolicyProvider aerospikePolicyProvider) {
        ExecutorService aerospikeExecutor = new ThreadPoolExecutor(4, configuration.get(AEROSPIKE_PARALLELISM),
                1, TimeUnit.MINUTES, new LinkedBlockingQueue<>(),
                new NamedThreadFactory(JANUS_AEROSPIKE_THREAD_GROUP_NAME, "main"));

        String namespace = configuration.get(NAMESPACE);
        String graphPrefix = configuration.get(GRAPH_PREFIX);

        IAerospikeClient client = buildAerospikeClient(configuration);

        return new AerospikeOperations(graphPrefix, namespace, client,
                aerospikePolicyProvider, aerospikeExecutor);
    }

    protected WalOperations initWalOperations(Configuration configuration, AerospikeOperations aerospikeOperations){
        return new WalOperations(configuration, aerospikeOperations);
    }

    protected  Clock getClock() {
        return Clock.systemUTC();
    }

    protected TransactionalOperations initTransactionalOperations(
            Supplier<WriteAheadLogManager> writeAheadLogManager,
            Supplier<LockOperations> lockOperations,
            Supplier<MutateOperations> mutateOperations){
        return new TransactionalOperations(writeAheadLogManager.get(), lockOperations.get(), mutateOperations.get());
    }

    protected MutateOperations initMutateOperations(AerospikeOperations aerospikeOperations) {
        return new BasicMutateOperations(aerospikeOperations);
    }

    protected LockOperations initLockOperations(AerospikeOperations aerospikeOperations) {
        return new BasicLockOperations(aerospikeOperations);
    }

    protected WriteAheadLogManager initWriteAheadLogManager(WalOperations walOperations, Clock clock) {
        return new WriteAheadLogManagerBasic(walOperations, clock);
    }

    protected WriteAheadLogCompleter buildWriteAheadLogCompleter(
            WalOperations walOperations,
            Supplier<WriteAheadLogManager> writeAheadLogManager,
            Supplier<LockOperations> lockOperations,
            Supplier<MutateOperations> mutateOperations){
        return new WriteAheadLogCompleter(
                walOperations,
                initWalCompleterTransactionalOperations(writeAheadLogManager, lockOperations, mutateOperations));
    }

    protected TransactionalOperations initWalCompleterTransactionalOperations(
            Supplier<WriteAheadLogManager> writeAheadLogManager,
            Supplier<LockOperations> lockOperations,
            Supplier<MutateOperations> mutateOperations){
        return new TransactionalOperations(writeAheadLogManager.get(), lockOperations.get(), mutateOperations.get());
    }

    protected ReadOperations initReadOperations(AerospikeOperations aerospikeOperations) {
        return new ReadOperations(aerospikeOperations);
    }

    protected ScanOperations initScanOperations(Configuration configuration, AerospikeOperations aerospikeOperations){
        ExecutorService scanExecutor = new ThreadPoolExecutor(0, configuration.get(SCAN_PARALLELISM),
                1, TimeUnit.MINUTES, new LinkedBlockingQueue<>(),
                new NamedThreadFactory(JANUS_AEROSPIKE_THREAD_GROUP_NAME, "scan"));
        return new ScanOperations(aerospikeOperations, scanExecutor);
    }

    @Override
    public void close() {
        writeAheadLogCompleter.shutdown();
        scanOperations.shutdown();
        aerospikeOperations.shutdown();
    }
}
