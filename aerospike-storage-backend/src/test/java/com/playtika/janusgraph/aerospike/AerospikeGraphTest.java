package com.playtika.janusgraph.aerospike;

import com.aerospike.client.AerospikeException;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphTest;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static com.playtika.janusgraph.aerospike.AerospikeStoreManager.AEROSPIKE_BUFFER_SIZE;
import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.deleteAllRecords;
import static com.playtika.janusgraph.aerospike.ConfigOptions.*;
import static com.playtika.janusgraph.aerospike.TestAerospikeStoreManager.closeAllGraphs;
import static org.assertj.core.api.Assertions.assertThat;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;
import static org.junit.Assert.fail;

public class AerospikeGraphTest extends JanusGraphTest {

    public static final String TEST_NAMESPACE = "test";

    @BeforeClass
    public static void cleanTestNamespaceAndCloseGraphs() throws InterruptedException, BackendException {
        deleteAllRecords(TEST_NAMESPACE);
        closeAllGraphs();
    }

    @Override
    public WriteConfiguration getConfiguration() {
        return getAerospikeConfiguration().getConfiguration();
    }

    static ModifiableConfiguration getAerospikeConfiguration() {
        ModifiableConfiguration config = buildGraphConfiguration();
        config.set(STORAGE_BACKEND, "com.playtika.janusgraph.aerospike.TestAerospikeStoreManager");
        config.set(NAMESPACE, TEST_NAMESPACE);
        config.set(WAL_NAMESPACE, TEST_NAMESPACE);
        config.set(GRAPH_PREFIX, "test");
        //!!! need to prevent small batches mutations as we use deferred locking approach !!!
        config.set(BUFFER_SIZE, AEROSPIKE_BUFFER_SIZE);
        config.set(ALLOW_SCAN, true);  //for test purposes only
        return config;
    }

    @Ignore
    @Override
    @Test
    //TODO waiting for https://github.com/JanusGraph/janusgraph/issues/1498
    public void testIndexUpdatesWithReindexAndRemove() {
    }

    @Override
    @Test
    public void testLargeJointIndexRetrieval() {
        try {
            super.testLargeJointIndexRetrieval();
            fail();
        } catch (Exception e){
            assertThat(e).hasRootCauseInstanceOf(AerospikeException.class)
                    .hasStackTraceContaining("Record too big");
        }
    }

    @Override
    @Test
    public void testVertexCentricQuery() {
        try {
            super.testVertexCentricQuery();
            fail();
        } catch (Exception e){
            assertThat(e).hasRootCauseInstanceOf(AerospikeException.class)
                    .hasStackTraceContaining("Record too big");
        }
    }
}
