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

import org.janusgraph.StorageSetup;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphTest;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

import java.util.concurrent.ExecutionException;

import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeConfiguration;
import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeContainer;
import static com.playtika.janusgraph.aerospike.diskstorage.AerospikeStoreTest.DUE_TO_RECORD_TO_BIG;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class AerospikeGraphCacheTest extends JanusGraphTest {

    @ClassRule
    public static final GenericContainer container = getAerospikeContainer();

    @Override
    public WriteConfiguration getConfiguration() {
        return StorageSetup.addPermanentCache(getAerospikeConfiguration(container));
    }

    @Tag(DUE_TO_RECORD_TO_BIG)
    @Test
    @Override
    public void testLargeJointIndexRetrieval() {
        assertThatThrownBy(super::testLargeJointIndexRetrieval)
                .isInstanceOf(JanusGraphException.class)
                .hasMessageContaining("Could not commit transaction due to exception during persistence");
    }

    @Tag(DUE_TO_RECORD_TO_BIG)
    @Test
    @Override
    public void testVertexCentricQuery() {
        assertThatThrownBy(super::testVertexCentricQuery)
                .isInstanceOf(JanusGraphException.class)
                .hasMessageContaining("Could not commit transaction due to exception during persistence");
    }
}
