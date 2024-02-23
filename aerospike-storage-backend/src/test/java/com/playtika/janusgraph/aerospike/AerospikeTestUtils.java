package com.playtika.janusgraph.aerospike;

import com.aerospike.AerospikeContainerUtils;
import com.aerospike.AerospikeProperties;
import com.aerospike.client.AerospikeClient;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.policy.ClientPolicy;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.testcontainers.containers.GenericContainer;

import static com.playtika.janusgraph.aerospike.AerospikeStoreManager.AEROSPIKE_BUFFER_SIZE;
import static com.playtika.janusgraph.aerospike.ConfigOptions.GRAPH_PREFIX;
import static com.playtika.janusgraph.aerospike.ConfigOptions.IDS_NAMESPACE;
import static com.playtika.janusgraph.aerospike.ConfigOptions.NAMESPACE;
import static com.playtika.janusgraph.aerospike.ConfigOptions.SCAN_PARALLELISM;
import static com.playtika.janusgraph.aerospike.ConfigOptions.WAL_NAMESPACE;
import static com.playtika.janusgraph.aerospike.util.AerospikeUtils.isEmptyNamespace;
import static com.playtika.janusgraph.aerospike.util.AerospikeUtils.truncateNamespace;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.BUFFER_SIZE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_HOSTS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_PORT;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.buildGraphConfiguration;

public class AerospikeTestUtils {

    public static AerospikeProperties AEROSPIKE_PROPERTIES = new AerospikeProperties();

    public static GenericContainer getAerospikeContainer() {
        return AerospikeContainerUtils.startAerospikeContainer(AEROSPIKE_PROPERTIES);
    }

    public static AerospikeClient getAerospikeClient(GenericContainer aerospike, EventLoops eventLoops) {
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.eventLoops = eventLoops;
        return new AerospikeClient(clientPolicy, aerospike.getContainerIpAddress(),
                aerospike.getMappedPort(AEROSPIKE_PROPERTIES.getPort()));
    }

    public static ModifiableConfiguration getAerospikeConfiguration(GenericContainer container) {

        ModifiableConfiguration config = buildGraphConfiguration();
        config.set(STORAGE_HOSTS, new String[]{container.getContainerIpAddress()});
        config.set(STORAGE_PORT, container.getMappedPort(AEROSPIKE_PROPERTIES.getPort()));
        config.set(STORAGE_BACKEND, "com.playtika.janusgraph.aerospike.AerospikeStoreManager");
        config.set(NAMESPACE, AEROSPIKE_PROPERTIES.getNamespace());
        config.set(IDS_NAMESPACE, AEROSPIKE_PROPERTIES.getNamespace());
        config.set(WAL_NAMESPACE, AEROSPIKE_PROPERTIES.getNamespace());
        config.set(GRAPH_PREFIX, "test");
        //!!! need to prevent small batches mutations as we use deferred locking approach !!!
        config.set(BUFFER_SIZE, AEROSPIKE_BUFFER_SIZE);
        config.set(SCAN_PARALLELISM, 100);
        return config;
    }

    static void deleteAllRecords(GenericContainer container) throws InterruptedException {
        try(AerospikeClient client = new AerospikeClient(container.getContainerIpAddress(),
                container.getMappedPort(AEROSPIKE_PROPERTIES.getPort()))) {
            while(!isEmptyNamespace(client, AEROSPIKE_PROPERTIES.getNamespace())){
                truncateNamespace(client, AEROSPIKE_PROPERTIES.getNamespace());
                Thread.sleep(100);
            }
        }
    }

}
