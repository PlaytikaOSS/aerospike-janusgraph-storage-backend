package com.playtika.janusgraph.aerospike.operations;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.policy.ClientPolicy;
import com.playtika.janusgraph.aerospike.AerospikePolicyProvider;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static nosql.batch.update.util.AsyncUtil.shutdownAndAwaitTermination;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_HOSTS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_PORT;

public class AerospikeOperations {

    private static final Logger logger = LoggerFactory.getLogger(AerospikeOperations.class);

    private static final int DEFAULT_PORT = 3000;

    public static final String ENTRIES_BIN_NAME = "entries";

    private final String namespace;
    private final String idsStoreName;
    private final String idsNamespace;
    private final String graphPrefix;
    private final IAerospikeClient client;
    private final ExecutorService aerospikeExecutor;

    private final AerospikePolicyProvider aerospikePolicyProvider;
    private final ExecutorService batchExecutor;

    private final ScheduledExecutorService statsLogger;
    private final ScheduledFuture<?> statsFuture;


    public AerospikeOperations(String graphPrefix,
                               String namespace,
                               String idsStoreName,
                               String idsNamespace,
                               IAerospikeClient client,
                               AerospikePolicyProvider aerospikePolicyProvider,
                               ExecutorService aerospikeExecutor,
                               ExecutorService batchExecutor) {
        this.graphPrefix = graphPrefix+".";
        this.namespace = namespace;
        this.idsStoreName = idsStoreName;
        this.idsNamespace = idsNamespace;
        this.client = client;
        this.aerospikeExecutor = aerospikeExecutor;
        this.aerospikePolicyProvider = aerospikePolicyProvider;
        this.batchExecutor = batchExecutor;
        this.statsLogger = Executors.newScheduledThreadPool(1);
        this.statsFuture = statsLogger.scheduleAtFixedRate(() ->
                Stream.of(client.getCluster().getNodes()).forEach(node ->
                        logger.info("node [{}] connections stats: {}", node, node.getConnectionStats())),
                5, 5, TimeUnit.MINUTES);
    }

    public IAerospikeClient getClient() {
        return client;
    }

    public ExecutorService getAerospikeExecutor() {
        return aerospikeExecutor;
    }

    public ExecutorService getBatchExecutor() {
        return batchExecutor;
    }

    public String getNamespace(String storeName) {
        return idsStoreName.equals(storeName) ? this.idsNamespace : this.namespace;
    }

    Key getKey(String storeName, StaticBuffer staticBuffer) {
        return getKey(storeName, getValue(staticBuffer));
    }

    public static Value getValue(StaticBuffer staticBuffer) {
        return staticBuffer.as((array, offset, limit) -> Value.get(array, offset, limit - offset));
    }

    public Key getKey(String storeName, Value value) {
        String namespace = getNamespace(storeName);
        return new Key(namespace, getSetName(storeName), value);
    }

    public String getSetName(String storeName) {
        return graphPrefix + storeName;
    }

    public AerospikePolicyProvider getAerospikePolicyProvider() {
        return aerospikePolicyProvider;
    }

    public String getGraphPrefix() {
        return graphPrefix;
    }

    public void close() {
        statsFuture.cancel(true);
        shutdownAndAwaitTermination(statsLogger);
        client.close();
        aerospikePolicyProvider.close();
    }

    public static IAerospikeClient buildAerospikeClient(Configuration configuration, ClientPolicy clientPolicy) {
        int port = configuration.has(STORAGE_PORT) ? configuration.get(STORAGE_PORT) : DEFAULT_PORT;

        Host[] hosts = Stream.of(configuration.get(STORAGE_HOSTS))
                .map(hostname -> new Host(hostname, port)).toArray(Host[]::new);

        return new AerospikeClient(clientPolicy, hosts);
    }

    public String getIdsStoreName() {
        return idsStoreName;
    }
}
