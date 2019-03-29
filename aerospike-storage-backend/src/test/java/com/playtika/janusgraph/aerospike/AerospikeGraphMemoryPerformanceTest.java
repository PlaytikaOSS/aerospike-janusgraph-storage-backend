package com.playtika.janusgraph.aerospike;

import com.aerospike.AerospikeContainer;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphPerformanceMemoryTest;
import org.junit.ClassRule;

import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeConfiguration;
import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeContainer;

public class AerospikeGraphMemoryPerformanceTest extends JanusGraphPerformanceMemoryTest {

    @ClassRule
    public static AerospikeContainer container = getAerospikeContainer();

    @Override
    public WriteConfiguration getConfiguration() {
        return getAerospikeConfiguration(container).getConfiguration();
    }

}