// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.playtika.janusgraph.aerospike.debuggrapdb;

import com.playtika.janusgraph.trace.DebugJanusGraph;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.olap.OLAPTest;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeConfiguration;
import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeContainer;


//TODO https://github.com/JanusGraph/janusgraph/issues/1527
//TODO wait for https://github.com/JanusGraph/janusgraph/issues/1524
@Disabled
public class AerospikeDebugOLAPTest extends OLAPTest {
    @ClassRule
    public static final GenericContainer container = getAerospikeContainer();

    @Override
    public void open(WriteConfiguration config) {
        this.graph = (StandardJanusGraph) DebugJanusGraph.open(config);
        this.features = this.graph.getConfiguration().getStoreFeatures();
        this.tx = this.graph.newTransaction();
        this.mgmt = this.graph.openManagement();
    }

    @Override
    public WriteConfiguration getConfiguration() {
        return getAerospikeConfiguration(container).getConfiguration();
    }

    //TODO
    @Ignore
    @Override
    @Test
    public void degreeCounting() {
    }

    //Throws java.lang.OutOfMemoryError: Java heap space
    @Ignore
    @Override
    @Test
    public void degreeCountingDistance() {
    }

    //TODO
    @Ignore
    @Override
    @Test
    public void testVertexScan() {
    }

    //TODO
    @Ignore
    @Override
    @Test
    public void testShortestDistance() {
    }

    //TODO
    @Ignore
    @Override
    @Test
    public void testPageRank() {
    }
}
