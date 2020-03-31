package com.playtika.janusgraph.aerospike.operations;

import com.aerospike.client.IAerospikeClient;
import com.playtika.janusgraph.aerospike.AerospikePolicyProvider;
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

import static com.playtika.janusgraph.aerospike.ConfigOptions.AEROSPIKE_PARALLELISM;
import static com.playtika.janusgraph.aerospike.ConfigOptions.AEROSPIKE_READ_PARALLELISM;
import static com.playtika.janusgraph.aerospike.ConfigOptions.GRAPH_PREFIX;
import static com.playtika.janusgraph.aerospike.ConfigOptions.NAMESPACE;
import static com.playtika.janusgraph.aerospike.ConfigOptions.PARALLEL_READ_THRESHOLD;
import static com.playtika.janusgraph.aerospike.ConfigOptions.SCAN_PARALLELISM;
import static com.playtika.janusgraph.aerospike.operations.AerospikeOperations.buildAerospikeClient;

public class BasicOperations implements Operations {

    public static final String JANUS_AEROSPIKE_THREAD_GROUP_NAME = "janus-aerospike";

    private final Configuration configuration;
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
        this.aerospikeOperations = buildAerospikeOperations(configuration);
        this.walOperations = buildWalOperations(configuration, aerospikeOperations);
        this.writeAheadLogManager = buildWriteAheadLogManager(walOperations, getClock());
        this.lockOperations = buildLockOperations(aerospikeOperations);
        this.mutateOperations = buildMutateOperations(aerospikeOperations);
        this.transactionalOperations = buildTransactionalOperations(
                () -> writeAheadLogManager, () -> lockOperations, () -> mutateOperations);
        this.writeAheadLogCompleter = buildWriteAheadLogCompleter(walOperations,
                () -> writeAheadLogManager, () -> lockOperations, () -> mutateOperations);

        this.readOperations = buildReadOperations(configuration, aerospikeOperations);
        this.scanOperations = buildScanOperations(configuration, aerospikeOperations);
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

    protected AerospikePolicyProvider buildPolicyProvider(Configuration configuration) {
        return new AerospikePolicyProvider(configuration);
    }

    protected AerospikeOperations buildAerospikeOperations(Configuration configuration) {
        String namespace = configuration.get(NAMESPACE);
        String graphPrefix = configuration.get(GRAPH_PREFIX);

        AerospikePolicyProvider aerospikePolicyProvider = buildPolicyProvider(configuration);

        IAerospikeClient client = buildAerospikeClient(configuration, aerospikePolicyProvider.clientPolicy());

        ExecutorService aerospikeExecutor = buildExecutor(4, configuration.get(AEROSPIKE_PARALLELISM), "main");

        ExecutorService aerospikeGetExecutor = buildExecutor(4, configuration.get(AEROSPIKE_READ_PARALLELISM), "get");

        return new AerospikeOperations(graphPrefix, namespace, client,
                aerospikePolicyProvider, aerospikeExecutor, aerospikeGetExecutor);
    }

    private ThreadPoolExecutor buildExecutor(int i, Integer integer, String main) {
        return new ThreadPoolExecutor(i, integer,
                1, TimeUnit.MINUTES, new LinkedBlockingQueue<>(),
                new NamedThreadFactory(JANUS_AEROSPIKE_THREAD_GROUP_NAME, main));
    }

    protected WalOperations buildWalOperations(Configuration configuration, AerospikeOperations aerospikeOperations){
        return new WalOperations(configuration, aerospikeOperations);
    }

    protected  Clock getClock() {
        return Clock.systemUTC();
    }

    protected TransactionalOperations buildTransactionalOperations(
            Supplier<WriteAheadLogManager> writeAheadLogManager,
            Supplier<LockOperations> lockOperations,
            Supplier<MutateOperations> mutateOperations){
        return new TransactionalOperations(writeAheadLogManager.get(), lockOperations.get(), mutateOperations.get());
    }

    protected MutateOperations buildMutateOperations(AerospikeOperations aerospikeOperations) {
        return new BasicMutateOperations(aerospikeOperations);
    }

    protected LockOperations buildLockOperations(AerospikeOperations aerospikeOperations) {
        return new BasicLockOperations(aerospikeOperations);
    }

    protected WriteAheadLogManager buildWriteAheadLogManager(WalOperations walOperations, Clock clock) {
        return new WriteAheadLogManagerBasic(walOperations, clock);
    }

    protected WriteAheadLogCompleter buildWriteAheadLogCompleter(
            WalOperations walOperations,
            Supplier<WriteAheadLogManager> writeAheadLogManager,
            Supplier<LockOperations> lockOperations,
            Supplier<MutateOperations> mutateOperations){
        return new WriteAheadLogCompleter(
                walOperations,
                buildWalCompleterTransactionalOperations(writeAheadLogManager, lockOperations, mutateOperations));
    }

    protected TransactionalOperations buildWalCompleterTransactionalOperations(
            Supplier<WriteAheadLogManager> writeAheadLogManager,
            Supplier<LockOperations> lockOperations,
            Supplier<MutateOperations> mutateOperations){
        return new TransactionalOperations(writeAheadLogManager.get(), lockOperations.get(), mutateOperations.get());
    }

    protected ReadOperations buildReadOperations(Configuration configuration, AerospikeOperations aerospikeOperations) {
        return new ReadOperations(aerospikeOperations, configuration.get(PARALLEL_READ_THRESHOLD));
    }

    protected ScanOperations buildScanOperations(Configuration configuration, AerospikeOperations aerospikeOperations){
        Integer scanParallelism = configuration.get(SCAN_PARALLELISM);
        if(scanParallelism > 0){
            ExecutorService scanExecutor = buildExecutor(0, scanParallelism, "scan");
            return new BasicScanOperations(aerospikeOperations, scanExecutor);

        } else {
            return new UnsupportedScanOperations();
        }
    }

    @Override
    public void close() {
        writeAheadLogCompleter.shutdown();
        scanOperations.close();
        aerospikeOperations.close();
    }
}
