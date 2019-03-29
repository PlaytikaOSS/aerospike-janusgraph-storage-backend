package com.playtika.janusgraph.aerospike;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphEventualGraphTest;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.playtika.janusgraph.aerospike.AerospikeGraphTest.cleanTestNamespaceAndCloseGraphs;
import static com.playtika.janusgraph.aerospike.AerospikeGraphTest.getAerospikeConfiguration;

public class AerospikeGraphEventualGraphTest extends JanusGraphEventualGraphTest {

    @BeforeClass
    public static void before() throws InterruptedException, BackendException {
        cleanTestNamespaceAndCloseGraphs();
    }

    @Override
    public WriteConfiguration getConfiguration() {
        return getAerospikeConfiguration().getConfiguration();
    }

    @Override
    @Test
    //illegal test as we do not use Lockers
    public void testLockException() {
        if(!features.hasLocking()){
            super.testLockException();
        }
    }

    @Override
    @Test
    //illegal test as we do not use timestamps
    public void testTimestampSetting() {
        if(features.hasTimestamps()){
            super.testTimestampSetting();
        }
    }
}
