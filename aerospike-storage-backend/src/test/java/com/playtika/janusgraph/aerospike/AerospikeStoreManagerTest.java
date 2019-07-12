package com.playtika.janusgraph.aerospike;

import org.janusgraph.diskstorage.BackendException;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeConfiguration;
import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeContainer;

public class AerospikeStoreManagerTest {

    @ClassRule
    public static GenericContainer container = getAerospikeContainer();

    @Test
    public void shouldNotFailIfUdfsAlreadyRegistered() throws BackendException {
        AerospikeStoreManager manager = new AerospikeStoreManager(getAerospikeConfiguration(container));
        manager.close();
        manager = new AerospikeStoreManager(getAerospikeConfiguration(container));
        manager.close();
    }

}
