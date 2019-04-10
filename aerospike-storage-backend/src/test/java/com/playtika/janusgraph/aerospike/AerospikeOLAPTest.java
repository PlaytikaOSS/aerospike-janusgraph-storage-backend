package com.playtika.janusgraph.aerospike;

import com.aerospike.AerospikeContainer;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.olap.OLAPTest;
import org.junit.ClassRule;
import org.junit.Ignore;

import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeConfiguration;
import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeContainer;
import static com.playtika.janusgraph.aerospike.ConfigOptions.SCAN_PARALLELISM;


@Ignore
//TODO wait for https://github.com/JanusGraph/janusgraph/issues/1524
public class AerospikeOLAPTest extends OLAPTest {

    @ClassRule
    public static AerospikeContainer container = getAerospikeContainer();

    @Override
    public WriteConfiguration getConfiguration() {
        return getAerospikeConfiguration(container)
                .set(SCAN_PARALLELISM, 100)
                .getConfiguration();
    }

}
