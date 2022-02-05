package com.playtika.janusgraph.aerospike.operations;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Value;
import com.aerospike.client.policy.ClientPolicy;
import com.playtika.janusgraph.aerospike.AerospikePolicyProvider;
import com.playtika.janusgraph.aerospike.operations.batch.BatchLocks;
import com.playtika.janusgraph.aerospike.operations.batch.BatchOperationsUtil;
import com.playtika.janusgraph.aerospike.operations.batch.BatchUpdates;
import com.playtika.janusgraph.aerospike.operations.batch.WalOperations;
import com.playtika.janusgraph.aerospike.util.NamedThreadFactory;
import nosql.batch.update.BatchOperations;
import nosql.batch.update.BatchUpdater;
import nosql.batch.update.aerospike.lock.AerospikeLock;
import nosql.batch.update.aerospike.wal.AerospikeExclusiveLocker;
import nosql.batch.update.wal.WriteAheadLogCompleter;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.playtika.janusgraph.aerospike.ConfigOptions.AEROSPIKE_EXECUTOR_MAX_THREADS;
import static com.playtika.janusgraph.aerospike.ConfigOptions.GRAPH_PREFIX;
import static com.playtika.janusgraph.aerospike.ConfigOptions.NAMESPACE;
import static com.playtika.janusgraph.aerospike.ConfigOptions.PARALLEL_READ_THRESHOLD;
import static com.playtika.janusgraph.aerospike.ConfigOptions.SCAN_PARALLELISM;
import static com.playtika.janusgraph.aerospike.ConfigOptions.START_WAL_COMPLETER;
import static com.playtika.janusgraph.aerospike.operations.AerospikeOperations.buildAerospikeClient;

public class BasicOperations implements Operations {

    private static final Logger logger = LoggerFactory.getLogger(BasicOperations.class);

    public static final String JANUS_AEROSPIKE_THREAD_GROUP_NAME = "janus-aerospike";
    public static final String JANUS_BATCH_THREAD_GROUP_NAME = "janus-batch";

    private final AerospikeOperations aerospikeOperations;
    private final MutateOperations mutateOperations;
    private final BatchUpdater<BatchLocks, BatchUpdates, AerospikeLock, Value> batchUpdater;

    private final WriteAheadLogCompleter<BatchLocks, BatchUpdates, AerospikeLock, Value> writeAheadLogCompleter;

    private final ReadOperations readOperations;
    private final ScanOperations scanOperations;

    public BasicOperations(Configuration configuration) {
        this.aerospikeOperations = buildAerospikeOperations(configuration);
        WalOperations walOperations = buildWalOperations(configuration, aerospikeOperations);
        this.mutateOperations = buildMutateOperations(aerospikeOperations);
        BatchOperations<BatchLocks, BatchUpdates, AerospikeLock, Value> batchOperations = buildBatchOperations(
                aerospikeOperations, walOperations, getClock(),
                aerospikeOperations.getAerospikeExecutor(),
                aerospikeOperations.getBatchExecutor());
        this.batchUpdater = new BatchUpdater<>(batchOperations);
        if(configuration.get(START_WAL_COMPLETER)){
            this.writeAheadLogCompleter = buildWriteAheadLogCompleter(walOperations, batchOperations);
        } else {
            this.writeAheadLogCompleter = null;
        }

        this.readOperations = buildReadOperations(configuration, aerospikeOperations);
        this.scanOperations = buildScanOperations(configuration, aerospikeOperations);
    }

    protected BatchOperations<BatchLocks, BatchUpdates, AerospikeLock, Value> buildBatchOperations(
            AerospikeOperations aerospikeOperations, WalOperations walOperations, Clock clock,
            ExecutorService executorService, ExecutorService batchExecutorService) {
        return BatchOperationsUtil.batchOperations(aerospikeOperations,
                walOperations.getWalNamespace(), walOperations.getWalSetName(), clock,
                executorService, batchExecutorService);
    }

    @Override
    public AerospikeOperations getAerospikeOperations() {
        return aerospikeOperations;
    }

    @Override
    public BatchUpdater<BatchLocks, BatchUpdates, AerospikeLock, Value> batchUpdater() {
        return batchUpdater;
    }

    @Override
    public WriteAheadLogCompleter<BatchLocks, BatchUpdates, AerospikeLock, Value> getWriteAheadLogCompleter() {
        return writeAheadLogCompleter;
    }

    @Override
    public MutateOperations mutateOperations() {
        return mutateOperations;
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

        AerospikePolicyProvider policyProvider = buildPolicyProvider(configuration);
        ClientPolicy clientPolicy = policyProvider.clientPolicy();
        IAerospikeClient client = buildAerospikeClient(configuration, clientPolicy);
        waitForClientToConnect(client);

        return new AerospikeOperations(graphPrefix, namespace, client, policyProvider,
                executorService(configuration.get(AEROSPIKE_EXECUTOR_MAX_THREADS)),
                executorService(8, 8));
    }

    private void waitForClientToConnect(IAerospikeClient client) {
        while (!client.isConnected()) {
            logger.debug("Waiting for client [{}] to connect to Aerospike cluster", client);
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException();
            }
        }
    }

    protected WalOperations buildWalOperations(Configuration configuration, AerospikeOperations aerospikeOperations){
        return new WalOperations(configuration, aerospikeOperations);
    }

    protected  Clock getClock() {
        return Clock.systemUTC();
    }

    protected MutateOperations buildMutateOperations(AerospikeOperations aerospikeOperations) {
        return new BasicMutateOperations(aerospikeOperations);
    }

    protected WriteAheadLogCompleter<BatchLocks, BatchUpdates, AerospikeLock, Value> buildWriteAheadLogCompleter(
            WalOperations walOperations,
            BatchOperations<BatchLocks, BatchUpdates, AerospikeLock, Value> batchOperations){
        return new WriteAheadLogCompleter<>(
                batchOperations,
                Duration.ofMillis(walOperations.getStaleTransactionLifetimeThresholdInMs()),
                new AerospikeExclusiveLocker(
                        walOperations.getAerospikeOperations().getClient(),
                        walOperations.getWalNamespace(),
                        walOperations.getWalSetName()),
                Executors.newScheduledThreadPool(1)
        );
    }

    protected ReadOperations buildReadOperations(Configuration configuration, AerospikeOperations aerospikeOperations) {
        Integer parallelReadThreshold = configuration.get(PARALLEL_READ_THRESHOLD);
        return new ReadOperations(aerospikeOperations, parallelReadThreshold);
    }

    protected ScanOperations buildScanOperations(Configuration configuration, AerospikeOperations aerospikeOperations){
        Integer scanParallelism = configuration.get(SCAN_PARALLELISM);
        if(scanParallelism > 0){
            return new BasicScanOperations(aerospikeOperations, new NamedThreadFactory(
                    JANUS_AEROSPIKE_THREAD_GROUP_NAME, "scan"));
        } else {
            return new UnsupportedScanOperations();
        }
    }

    @Override
    public void close() {
        if(writeAheadLogCompleter != null) {
            writeAheadLogCompleter.shutdown();
        }
        aerospikeOperations.close();
    }

    public static ExecutorService executorService(int maxThreads){
        return new ThreadPoolExecutor(0, maxThreads,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                new NamedThreadFactory(JANUS_AEROSPIKE_THREAD_GROUP_NAME, "janus-aerospike"));
    }

    public static ExecutorService executorService(int maxThreads, int queueCapacity){
        return new ThreadPoolExecutor(0, maxThreads,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(queueCapacity),
                new NamedThreadFactory(JANUS_BATCH_THREAD_GROUP_NAME, "janus-batch"),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }
}
