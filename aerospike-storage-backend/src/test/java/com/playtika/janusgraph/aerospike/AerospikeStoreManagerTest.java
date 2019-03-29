package com.playtika.janusgraph.aerospike;

import org.janusgraph.diskstorage.BackendException;
import org.junit.Before;
import org.junit.Test;

import static com.playtika.janusgraph.aerospike.AerospikeGraphTest.getAerospikeConfiguration;
import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.deleteAllRecords;

public class AerospikeStoreManagerTest {

    @Before
    public void cleanup() throws InterruptedException {
        deleteAllRecords("test");
    }

    @Test
    public void shouldNotFailIfUdfsAlreadyRegistered() throws BackendException {
        AerospikeStoreManager manager = new AerospikeStoreManager(getAerospikeConfiguration());
        manager.close();
        manager = new AerospikeStoreManager(getAerospikeConfiguration());
        manager.close();
    }

}
