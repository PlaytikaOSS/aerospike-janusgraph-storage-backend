package com.playtika.janusgraph.aerospike;

import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphOperationCountingTest;

import static com.playtika.janusgraph.aerospike.AerospikeGraphTest.getAerospikeConfiguration;

public class AerospikeGraphOperationCountingTest extends JanusGraphOperationCountingTest {

    @Override
    public WriteConfiguration getBaseConfiguration() {
        return getAerospikeConfiguration().getConfiguration();
    }
}
