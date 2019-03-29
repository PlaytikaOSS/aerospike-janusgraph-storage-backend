package com.playtika.janusgraph.aerospike;

import com.aerospike.AerospikeContainer;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphEventualGraphTest;
import org.junit.ClassRule;
import org.junit.Test;

import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeConfiguration;
import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeContainer;

public class AerospikeGraphEventualGraphTest extends JanusGraphEventualGraphTest {

    @ClassRule
    public static AerospikeContainer container = getAerospikeContainer();

    @Override
    public WriteConfiguration getConfiguration() {
        return getAerospikeConfiguration(container).getConfiguration();
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
