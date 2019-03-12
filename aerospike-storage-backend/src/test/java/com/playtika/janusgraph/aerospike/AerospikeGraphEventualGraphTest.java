package com.playtika.janusgraph.aerospike;

import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphEventualGraphTest;
import org.junit.Test;

import static com.playtika.janusgraph.aerospike.AerospikeGraphTest.getAerospikeConfiguration;

public class AerospikeGraphEventualGraphTest extends JanusGraphEventualGraphTest {

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
