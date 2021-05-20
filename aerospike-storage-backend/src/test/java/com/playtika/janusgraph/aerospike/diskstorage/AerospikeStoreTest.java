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

package com.playtika.janusgraph.aerospike.diskstorage;

import com.playtika.janusgraph.aerospike.AerospikeStoreManager;
import org.janusgraph.diskstorage.KeyColumnValueStoreTest;
import org.janusgraph.testutil.FeatureFlag;
import org.janusgraph.testutil.JanusGraphFeature;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeConfiguration;
import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeContainer;

public class AerospikeStoreTest extends KeyColumnValueStoreTest {

    public static final String DUE_TO_DEFERRED_LOCKING_AND_MUTATION = "Due to deferred locking and mutation approach";

    @ClassRule
    public static final GenericContainer container = getAerospikeContainer();

    @Override
    public AerospikeStoreManager openStorageManager() {
        return new AerospikeStoreManager(getAerospikeConfiguration(container));
    }

    //TODO check after janusgraph 0.6.0
    @Ignore
    @Override
    @Test
    public void scanTestWithSimpleJob() {
    }

    //TODO investigate
    @Ignore
    @Test
    @FeatureFlag(
            feature = JanusGraphFeature.Scan
    )
    public void testGetKeysColumnSlicesSimple() {
    }

    @Tag(DUE_TO_DEFERRED_LOCKING_AND_MUTATION)
    @Ignore
    @Override
    @Test
    public void testConcurrentGetSliceAndMutate(){
    }
}
