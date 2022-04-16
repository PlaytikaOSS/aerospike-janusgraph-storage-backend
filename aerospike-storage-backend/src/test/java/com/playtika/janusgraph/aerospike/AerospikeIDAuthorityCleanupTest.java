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

package com.playtika.janusgraph.aerospike;

import org.janusgraph.diskstorage.IDAuthorityTest;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.junit.ClassRule;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.GenericContainer;

import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeConfiguration;
import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeContainer;
import static com.playtika.janusgraph.aerospike.ConfigOptions.IDS_BLOCK_TTL;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.IDS_STORE_NAME;


public class AerospikeIDAuthorityCleanupTest extends IDAuthorityTest {
    @ClassRule
    public static final GenericContainer container = getAerospikeContainer();

    @ParameterizedTest
    @MethodSource("configs")
    public void testSimpleIDAcquisition(WriteConfiguration baseConfig) throws Exception {
        super.testSimpleIDAcquisition(baseConfig);
    }

    @Override
    public AerospikeStoreManager openStorageManager() {
        return new AerospikeStoreManager(
                getAerospikeConfiguration(container)
                        .set(IDS_STORE_NAME, "ids")
                        .set(IDS_BLOCK_TTL, 1L));
    }
}
