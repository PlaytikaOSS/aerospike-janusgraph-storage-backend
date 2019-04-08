package com.playtika.janusgraph.aerospike.benchmark;

import com.aerospike.AerospikeContainer;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.testcontainers.containers.CassandraContainer;

import java.time.Duration;

import static com.playtika.janusgraph.aerospike.AerospikeStoreManager.AEROSPIKE_BUFFER_SIZE;
import static com.playtika.janusgraph.aerospike.ConfigOptions.*;
import static com.playtika.janusgraph.aerospike.ConfigOptions.ALLOW_SCAN;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.KEYSPACE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;

public class Configurations {

    public static final String TEST_NAMESPACE = "TEST";

    static ModifiableConfiguration getAerospikeConfiguration(AerospikeContainer container) {

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

    static ModifiableConfiguration getCQLConfiguration(CassandraContainer cassandraContainer) {
        final ModifiableConfiguration config = buildGraphConfiguration();
        config.set(KEYSPACE, "test");
        config.set(PAGE_SIZE, 500);
        config.set(CONNECTION_TIMEOUT, Duration.ofSeconds(60L));
        config.set(STORAGE_BACKEND, "cql");
        config.set(STORAGE_HOSTS, new String[]{cassandraContainer.getContainerIpAddress()
                +":"+cassandraContainer.getMappedPort(9042)});
        config.set(DROP_ON_CLEAR, false);
        return config;
    }

}
