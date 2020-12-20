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
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.LockKeyColumnValueStoreTest;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeConfiguration;
import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeContainer;
import static com.playtika.janusgraph.aerospike.diskstorage.AerospikeStoreTest.DUE_TO_DEFERRED_LOCKING_AND_MUTATION;


public class AerospikeLockStoreTest extends LockKeyColumnValueStoreTest {

    @ClassRule
    public static final GenericContainer container = getAerospikeContainer();

    @Override
    public AerospikeStoreManager openStorageManager(int id, Configuration configuration) {
        return new AerospikeStoreManager(getAerospikeConfiguration(container));
    }

    @Tag(DUE_TO_DEFERRED_LOCKING_AND_MUTATION)
    @Ignore
    @Override
    @Test
    public void expiredLocalLockIsIgnored()  {}

    @Tag(DUE_TO_DEFERRED_LOCKING_AND_MUTATION)
    @Ignore
    @Override
    @Test
    public void singleTransactionWithMultipleLocks() {}

    @Tag(DUE_TO_DEFERRED_LOCKING_AND_MUTATION)
    @Ignore
    @Override
    @Test
    public void testLocalLockContention() throws BackendException {}

    @Tag(DUE_TO_DEFERRED_LOCKING_AND_MUTATION)
    @Ignore
    @Override
    @Test
    public void testRemoteLockContention(){}
}
