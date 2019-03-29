package com.playtika.janusgraph.aerospike;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphPerformanceMemoryTest;
import org.junit.BeforeClass;

import static com.playtika.janusgraph.aerospike.AerospikeGraphTest.cleanTestNamespaceAndCloseGraphs;
import static com.playtika.janusgraph.aerospike.AerospikeGraphTest.getAerospikeConfiguration;

public class AerospikeGraphMemoryPerformanceTest extends JanusGraphPerformanceMemoryTest {

    @BeforeClass
    public static void before() throws InterruptedException, BackendException {
        cleanTestNamespaceAndCloseGraphs();
    }

    @Override
    public WriteConfiguration getConfiguration() {
        return getAerospikeConfiguration().getConfiguration();
    }

}