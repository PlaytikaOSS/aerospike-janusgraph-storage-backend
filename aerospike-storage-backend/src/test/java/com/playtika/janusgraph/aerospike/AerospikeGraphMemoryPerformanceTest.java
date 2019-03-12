package com.playtika.janusgraph.aerospike;

import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphPerformanceMemoryTest;

import static com.playtika.janusgraph.aerospike.AerospikeGraphTest.getAerospikeConfiguration;

public class AerospikeGraphMemoryPerformanceTest extends JanusGraphPerformanceMemoryTest {

    @Override
    public WriteConfiguration getConfiguration() {
        return getAerospikeConfiguration().getConfiguration();
    }

}