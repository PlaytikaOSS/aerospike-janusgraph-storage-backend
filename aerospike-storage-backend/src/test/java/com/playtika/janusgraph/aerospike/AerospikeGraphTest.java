package com.playtika.janusgraph.aerospike;

import com.aerospike.client.AerospikeException;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphTest;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeConfiguration;
import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeContainer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class AerospikeGraphTest extends JanusGraphTest {

    @ClassRule
    public static GenericContainer container = getAerospikeContainer();

    @Override
    public WriteConfiguration getConfiguration() {
        return getAerospikeConfiguration(container).getConfiguration();
    }

    @Ignore
    @Override
    @Test
    //TODO waiting for https://github.com/JanusGraph/janusgraph/issues/1522
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
