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

package com.playtika.janusgraph.aerospike.graphdb;

import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.olap.OLAPTest;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeConfiguration;
import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeContainer;


//TODO https://github.com/JanusGraph/janusgraph/issues/1527
//TODO wait for https://github.com/JanusGraph/janusgraph/issues/1524
public class AerospikeOLAPTest extends OLAPTest {
    @ClassRule
    public static final GenericContainer container = getAerospikeContainer();

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

    //TODO unstable scan operations after upgrade to new Aerospike Server:ce-6.2.0.2 + Client:6.2.0
    @Ignore
    @Override
    @Test
    public void testShortestPath() {
    }

    //TODO
    @Ignore
    @Override
    @Test
    public void testPageRank(){
    }

    //TODO unstable scan operations after upgrade to new Aerospike Server:ce-6.2.0.2 + Client:6.2.0
    @Ignore
    @Override
    @Test
    public void testConnectedComponent() {
    }
}
