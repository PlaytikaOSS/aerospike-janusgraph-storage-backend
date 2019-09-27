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

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static com.playtika.janusgraph.aerospike.ConfigOptions.*;
import static com.playtika.janusgraph.aerospike.util.AsyncUtil.WAIT_TIMEOUT_IN_SECONDS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;

public class AerospikeOperations {

    private static final int DEFAULT_PORT = 3000;

    static final String ENTRIES_BIN_NAME = "entries";

    private final String namespace;
    private final String graphPrefix;
    private final IAerospikeClient client;

    private final ExecutorService aerospikeExecutor;
    private final ExecutorService aerospikeGetExecutor;
    private final AerospikePolicyProvider aerospikePolicyProvider;

    public AerospikeOperations(String graphPrefix, String namespace,
                               IAerospikeClient client,
                               AerospikePolicyProvider aerospikePolicyProvider,
                               ExecutorService aerospikeExecutor,
                               ExecutorService aerospikeGetExecutor) {
        this.graphPrefix = graphPrefix+".";
        this.namespace = namespace;
        this.client = client;
        this.aerospikePolicyProvider = aerospikePolicyProvider;
        this.aerospikeExecutor = aerospikeExecutor;
        this.aerospikeGetExecutor = aerospikeGetExecutor;
    }

    public IAerospikeClient getClient() {
        return client;
    }

    public String getNamespace() {
        return namespace;
    }

    Key getKey(String storeName, StaticBuffer staticBuffer) {
        return getKey(storeName, getValue(staticBuffer));
    }

    public static Value getValue(StaticBuffer staticBuffer) {
        return staticBuffer.as((array, offset, limit) -> Value.get(array, offset, limit - offset));
    }

    Key getKey(String storeName, Value value) {
        return new Key(namespace, getSetName(storeName), value);
    }

    String getSetName(String storeName) {
        return graphPrefix + storeName;
    }

    Executor getAerospikeExecutor() {
        return aerospikeExecutor;
    }

    public ExecutorService getAerospikeGetExecutor() {
        return aerospikeGetExecutor;
    }

    public AerospikePolicyProvider getAerospikePolicyProvider() {
        return aerospikePolicyProvider;
    }

    public String getGraphPrefix() {
        return graphPrefix;
    }

    public void close(){
        shutdownAndAwaitTermination(aerospikeExecutor, WAIT_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);
        client.close();
    }

    public static IAerospikeClient buildAerospikeClient(Configuration configuration){
        int port = configuration.has(STORAGE_PORT) ? configuration.get(STORAGE_PORT) : DEFAULT_PORT;

        Host[] hosts = Stream.of(configuration.get(STORAGE_HOSTS))
                .map(hostname -> new Host(hostname, port)).toArray(Host[]::new);

        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.user = configuration.has(AUTH_USERNAME) ? configuration.get(AUTH_USERNAME) : null;
        clientPolicy.password = configuration.has(AUTH_PASSWORD) ? configuration.get(AUTH_PASSWORD) : null;
        clientPolicy.maxConnsPerNode = configuration.get(AEROSPIKE_CONNECTIONS_PER_NODE);

        return new AerospikeClient(clientPolicy, hosts);
    }
}
