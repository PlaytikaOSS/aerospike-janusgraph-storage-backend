package com.playtika.janusgraph.aerospike;

import com.aerospike.AerospikeContainerUtils;
import com.aerospike.AerospikeProperties;
import com.aerospike.client.AerospikeClient;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.testcontainers.containers.GenericContainer;

import static com.playtika.janusgraph.aerospike.AerospikeStoreManager.AEROSPIKE_BUFFER_SIZE;
import static com.playtika.janusgraph.aerospike.ConfigOptions.*;
import static com.playtika.janusgraph.aerospike.util.AerospikeUtils.isEmptyNamespace;
import static com.playtika.janusgraph.aerospike.util.AerospikeUtils.truncateNamespace;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;

public class AerospikeTestUtils {

    public static AerospikeProperties AEROSPIKE_PROPERTIES = new AerospikeProperties();

    public static GenericContainer getAerospikeContainer() {
        return AerospikeContainerUtils.startAerospikeContainer(AEROSPIKE_PROPERTIES);
    }

    public static ModifiableConfiguration getAerospikeConfiguration(GenericContainer container) {

        ModifiableConfiguration config = buildGraphConfiguration();
        config.set(STORAGE_HOSTS, new String[]{container.getContainerIpAddress()});
        config.set(STORAGE_PORT, container.getMappedPort(AEROSPIKE_PROPERTIES.getPort()));
        config.set(STORAGE_BACKEND, "com.playtika.janusgraph.aerospike.AerospikeStoreManager");
        config.set(NAMESPACE, AEROSPIKE_PROPERTIES.getNamespace());
        config.set(WAL_NAMESPACE, AEROSPIKE_PROPERTIES.getNamespace());
        config.set(GRAPH_PREFIX, "test");
        //!!! need to prevent small batches mutations as we use deferred locking approach !!!
        config.set(BUFFER_SIZE, AEROSPIKE_BUFFER_SIZE);
        config.set(TEST_ENVIRONMENT, true); //for test purposes only
        config.set(ConfigOptions.SCAN_PARALLELISM, 1);
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
