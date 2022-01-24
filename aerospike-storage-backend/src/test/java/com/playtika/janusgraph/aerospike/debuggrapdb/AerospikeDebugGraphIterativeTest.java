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

import com.playtika.janusgraph.aerospike.AerospikeStoreManager;
import com.playtika.janusgraph.trace.DebugJanusGraph;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.graphdb.JanusGraphIterativeBenchmark;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.junit.ClassRule;
import org.junit.jupiter.api.Disabled;
import org.testcontainers.containers.GenericContainer;

import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeConfiguration;
import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeContainer;


@Disabled
public class AerospikeDebugGraphIterativeTest extends JanusGraphIterativeBenchmark {

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

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws BackendException {
        return new AerospikeStoreManager(new BasicConfiguration(
                GraphDatabaseConfiguration.ROOT_NS,
                getConfiguration(),
                BasicConfiguration.Restriction.NONE));
    }
}
