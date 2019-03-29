package com.playtika.janusgraph.aerospike;

import com.aerospike.AerospikeContainer;
import org.janusgraph.diskstorage.BackendException;
import org.junit.ClassRule;
import org.junit.Test;

import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeConfiguration;
import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeContainer;

public class AerospikeStoreManagerTest {

    @ClassRule
    public static AerospikeContainer container = getAerospikeContainer();

    @Test
    public void shouldNotFailIfUdfsAlreadyRegistered() throws BackendException {
        AerospikeStoreManager manager = new AerospikeStoreManager(getAerospikeConfiguration(container));
        manager.close();
        manager = new AerospikeStoreManager(getAerospikeConfiguration(container));
        manager.close();
    }

}
