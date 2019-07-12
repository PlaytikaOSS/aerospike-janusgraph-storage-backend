package com.playtika.janusgraph.aerospike;

import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphOperationCountingTest;
import org.junit.ClassRule;
import org.testcontainers.containers.GenericContainer;

import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeConfiguration;
import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeContainer;

public class AerospikeGraphOperationCountingTest extends JanusGraphOperationCountingTest {

    @ClassRule
    public static GenericContainer container = getAerospikeContainer();

    @Override
    public WriteConfiguration getBaseConfiguration() {
        return getAerospikeConfiguration(container).getConfiguration();
    }
}
