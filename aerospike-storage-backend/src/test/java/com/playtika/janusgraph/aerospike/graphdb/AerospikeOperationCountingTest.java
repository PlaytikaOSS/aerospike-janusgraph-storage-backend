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
import org.janusgraph.graphdb.JanusGraphOperationCountingTest;
import org.junit.ClassRule;
import org.testcontainers.containers.GenericContainer;

import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeConfiguration;
import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeContainer;


public class AerospikeOperationCountingTest extends JanusGraphOperationCountingTest {
    @ClassRule
    public static final GenericContainer container = getAerospikeContainer();

    @Override
    public WriteConfiguration getBaseConfiguration() {
        return getAerospikeConfiguration(container).getConfiguration();
    }

}
