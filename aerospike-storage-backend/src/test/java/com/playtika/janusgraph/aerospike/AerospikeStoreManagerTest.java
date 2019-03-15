package com.playtika.janusgraph.aerospike;

import org.junit.Before;
import org.junit.Test;

import static com.playtika.janusgraph.aerospike.AerospikeGraphTest.getAerospikeConfiguration;
import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.deleteAllRecords;

public class AerospikeStoreManagerTest {

    @Before
    public void cleanup(){
        deleteAllRecords("test");
    }

    @Test
    public void shouldNotFailIfUdfsAlreadyRegistered(){
        new AerospikeStoreManager(getAerospikeConfiguration());
        new AerospikeStoreManager(getAerospikeConfiguration());
    }

}
