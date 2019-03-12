package com.playtika.janusgraph.aerospike;

import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphTest;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static com.playtika.janusgraph.aerospike.AerospikeStoreManager.AEROSPIKE_BUFFER_SIZE;
import static com.playtika.janusgraph.aerospike.ConfigOptions.ALLOW_SCAN;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;

public class AerospikeGraphTest extends JanusGraphTest {

    @BeforeClass
    public static void cleanTestNamespace(){
        AerospikeTestUtils.deleteAllRecords("test");
    }

    @Override
    public WriteConfiguration getConfiguration() {
        return getAerospikeConfiguration().getConfiguration();
    }

    static ModifiableConfiguration getAerospikeConfiguration() {
        ModifiableConfiguration config = buildGraphConfiguration();
        config.set(STORAGE_BACKEND, "com.playtika.janusgraph.aerospike.AerospikeStoreManager");
//        config.set(STORAGE_BACKEND, "inmemory");
        //!!! need to prevent small batches mutations as we use deferred locking approach !!!
        config.set(BUFFER_SIZE, AEROSPIKE_BUFFER_SIZE);
        config.set(ALLOW_SCAN, true);  //for test purposes only
        return config;
    }

//    private static ModifiableConfiguration getCassandraConfiguration() {
//        ModifiableConfiguration config = buildGraphConfiguration();
//        config.set(CASSANDRA_KEYSPACE, "test");
//        config.set(PAGE_SIZE,500);
//        config.set(CONNECTION_TIMEOUT, Duration.ofSeconds(60L));
//        config.set(STORAGE_BACKEND, "embeddedcassandra");
//        config.set(DROP_ON_CLEAR, false);
//        return config;
//    }

    @Ignore
    @Override
    @Test
    public void testIndexUpdatesWithReindexAndRemove() throws InterruptedException, ExecutionException {
    }
}
