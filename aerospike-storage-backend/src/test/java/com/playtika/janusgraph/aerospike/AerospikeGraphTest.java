package com.playtika.janusgraph.aerospike;

import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphTest;
import org.junit.BeforeClass;

import static com.playtika.janusgraph.aerospike.ConfigOptions.ALLOW_SCAN;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;

public class AerospikeGraphTest extends JanusGraphTest {

    @BeforeClass
    public static void cleanTestNamespace(){
        AerospikeTestUtils.deleteAllRecords("test");
    }

    @Override
    public WriteConfiguration getConfiguration() {
        return getGenericConfiguration().getConfiguration();
    }

    private static ModifiableConfiguration getGenericConfiguration() {
        ModifiableConfiguration config = buildGraphConfiguration();
        config.set(STORAGE_BACKEND, "com.playtika.janusgraph.aerospike.AerospikeStoreManager");
        //!!! need to prevent small batches mutations as we use deferred locking approach !!!
        config.set(BUFFER_SIZE, Integer.MAX_VALUE);
        config.set(ALLOW_SCAN, true);  //for test purposes only
        return config;
    }
}
