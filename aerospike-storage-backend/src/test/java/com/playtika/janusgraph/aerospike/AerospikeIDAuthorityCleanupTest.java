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

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.async.NioEventLoops;
import com.playtika.janusgraph.aerospike.operations.AerospikeOperations;
import com.playtika.janusgraph.aerospike.operations.ReadOperations;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.IDAuthorityTest;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.GenericContainer;

import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.AEROSPIKE_PROPERTIES;
import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeClient;
import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeConfiguration;
import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeContainer;
import static com.playtika.janusgraph.aerospike.ConfigOptions.IDS_BLOCK_TTL;
import static com.playtika.janusgraph.aerospike.operations.BasicOperations.AEROSPIKE_PREFIX;
import static com.playtika.janusgraph.aerospike.operations.BasicOperations.BATCH_PREFIX;
import static com.playtika.janusgraph.aerospike.operations.BasicOperations.executorService;
import static com.playtika.janusgraph.aerospike.operations.IdsCleanupOperations.LOWER_SLICE;
import static com.playtika.janusgraph.aerospike.operations.IdsCleanupOperations.PROTECTED_BLOCKS_AMOUNT;
import static com.playtika.janusgraph.aerospike.operations.IdsCleanupOperations.UPPER_SLICE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.IDS_STORE_NAME;
import static org.janusgraph.graphdb.configuration.JanusGraphConstants.JANUSGRAPH_ID_STORE_NAME;


public class AerospikeIDAuthorityCleanupTest extends IDAuthorityTest {
    @ClassRule
    public static final GenericContainer container = getAerospikeContainer();
    public static final String IDS_STORE = "ids";

    @Test
    public void testSimpleIDAcquisition() throws Exception {
        super.testSimpleIDAcquisition((WriteConfiguration) configs().findFirst().get().get()[0]);

        ReadOperations readOperations = getReadOperations();

        StaticBuffer partitionKey = new StaticArrayBuffer(new byte[]{0, 0, 0, 0, 0, 0, 0, 0});
        EntryList entries = readOperations.getSlice(IDS_STORE, new KeySliceQuery(partitionKey, LOWER_SLICE, UPPER_SLICE));
        assertThat(entries).hasSize(PROTECTED_BLOCKS_AMOUNT);
    }

    @Override
    public AerospikeStoreManager openStorageManager() {
        return new AerospikeStoreManager(
                getAerospikeConfiguration(container)
                        .set(IDS_STORE_NAME, IDS_STORE)
                        .set(IDS_BLOCK_TTL, 1L));
    }

    public ReadOperations getReadOperations() {
        NioEventLoops eventLoops = new NioEventLoops();
        AerospikeClient client = getAerospikeClient(container, eventLoops);
        return new ReadOperations(
                new AerospikeOperations("test",
                        AEROSPIKE_PROPERTIES.getNamespace(), AEROSPIKE_PROPERTIES.getNamespace(),
                        JANUSGRAPH_ID_STORE_NAME,
                        client,
                        new AerospikePolicyProvider(getAerospikeConfiguration(container)),
                        executorService(4, AEROSPIKE_PREFIX),
                        executorService(4, 4, BATCH_PREFIX)),
                1);
    }

}
