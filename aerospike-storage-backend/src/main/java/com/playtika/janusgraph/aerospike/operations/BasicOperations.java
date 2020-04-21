package com.playtika.janusgraph.aerospike.operations;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Value;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.reactor.IAerospikeReactorClient;
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

import static com.playtika.janusgraph.aerospike.ConfigOptions.GRAPH_PREFIX;
import static com.playtika.janusgraph.aerospike.ConfigOptions.NAMESPACE;
import static com.playtika.janusgraph.aerospike.ConfigOptions.SCAN_PARALLELISM;
import static com.playtika.janusgraph.aerospike.ConfigOptions.START_WAL_COMPLETER;
import static com.playtika.janusgraph.aerospike.operations.AerospikeOperations.buildAerospikeClient;
import static com.playtika.janusgraph.aerospike.operations.AerospikeOperations.buildAerospikeReactorClient;

public class BasicOperations implements Operations {

    private static Logger logger = LoggerFactory.getLogger(BasicOperations.class);

    public static final String JANUS_AEROSPIKE_THREAD_GROUP_NAME = "janus-aerospike";

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
        BatchOperations<BatchLocks, BatchUpdates, AerospikeLock, Value> batchOperations
                = buildBatchOperations(aerospikeOperations, walOperations, getClock());
        this.batchUpdater = new BatchUpdater<>(batchOperations);
        if(configuration.get(START_WAL_COMPLETER)){
            this.writeAheadLogCompleter = buildWriteAheadLogCompleter(walOperations, batchOperations);
        } else {
            this.writeAheadLogCompleter = null;
        }

        this.readOperations = buildReadOperations(aerospikeOperations);
        this.scanOperations = buildScanOperations(configuration, aerospikeOperations);
    }

    protected BatchOperations<BatchLocks, BatchUpdates, AerospikeLock, Value> buildBatchOperations(
            AerospikeOperations aerospikeOperations, WalOperations walOperations, Clock clock) {
        return BatchOperationsUtil.batchOperations(aerospikeOperations,
                walOperations.getWalNamespace(), walOperations.getWalSetName(), clock);
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
        IAerospikeReactorClient reactorClient = buildAerospikeReactorClient(client, clientPolicy.eventLoops);

        return new AerospikeOperations(graphPrefix, namespace, client, reactorClient, policyProvider);
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

    private ThreadPoolExecutor buildExecutor(int corePoolSize, Integer maxPoolSize, String name) {
        return new ThreadPoolExecutor(corePoolSize, maxPoolSize,
                1, TimeUnit.MINUTES, new LinkedBlockingQueue<>(),
                new NamedThreadFactory(JANUS_AEROSPIKE_THREAD_GROUP_NAME, name));
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

    protected ReadOperations buildReadOperations(AerospikeOperations aerospikeOperations) {
        return new ReadOperations(aerospikeOperations);
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
        if(writeAheadLogCompleter != null) {
            writeAheadLogCompleter.shutdown();
        }
        scanOperations.close();
        aerospikeOperations.close();
    }
}
