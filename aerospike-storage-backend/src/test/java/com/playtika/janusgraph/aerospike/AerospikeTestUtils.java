package com.playtika.janusgraph.aerospike;

import com.aerospike.AerospikeContainer;
import com.aerospike.client.AerospikeClient;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;

import static com.playtika.janusgraph.aerospike.AerospikeStoreManager.AEROSPIKE_BUFFER_SIZE;
import static com.playtika.janusgraph.aerospike.ConfigOptions.*;
import static com.playtika.janusgraph.aerospike.util.AerospikeUtils.isEmptyNamespace;
import static com.playtika.janusgraph.aerospike.util.AerospikeUtils.truncateNamespace;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;

public class AerospikeTestUtils {

    public static final String AEROSPIKE_IMAGE = "aerospike/aerospike-server:4.3.0.2";
    private static final String TEST_NAMESPACE = "TEST";

    public static AerospikeContainer getAerospikeContainer() {
        return new AerospikeContainer(AEROSPIKE_IMAGE).withNamespace(TEST_NAMESPACE);
    }

    public static ModifiableConfiguration getAerospikeConfiguration(AerospikeContainer container) {

        ModifiableConfiguration config = buildGraphConfiguration();
        config.set(STORAGE_HOSTS, new String[]{container.getContainerIpAddress()});
        config.set(STORAGE_PORT, container.getPort());
        config.set(STORAGE_BACKEND, "com.playtika.janusgraph.aerospike.AerospikeStoreManager");
        config.set(NAMESPACE, TEST_NAMESPACE);
        config.set(WAL_NAMESPACE, TEST_NAMESPACE);
        config.set(GRAPH_PREFIX, "test");
        //!!! need to prevent small batches mutations as we use deferred locking approach !!!
        config.set(BUFFER_SIZE, AEROSPIKE_BUFFER_SIZE);
        config.set(ALLOW_SCAN, true);  //for test purposes only
        return config;
    }

    static void deleteAllRecords(AerospikeContainer container, String namespace) throws InterruptedException {
        try(AerospikeClient client = new AerospikeClient(container.getContainerIpAddress(), container.getPort())) {
            while(!isEmptyNamespace(client, namespace)){
                truncateNamespace(client, namespace);
                Thread.sleep(100);
            }
        }
    }

}
