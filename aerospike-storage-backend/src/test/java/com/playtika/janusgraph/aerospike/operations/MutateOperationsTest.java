package com.playtika.janusgraph.aerospike.operations;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Value;
import com.playtika.janusgraph.aerospike.AerospikePolicyProvider;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import java.util.concurrent.Executors;

import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.AEROSPIKE_PROPERTIES;
import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeConfiguration;
import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeContainer;
import static java.util.Collections.singletonMap;

public class MutateOperationsTest {

    @ClassRule
    public static GenericContainer container = getAerospikeContainer();

    public static final String STORE_NAME = "testStore";
    public static final Value KEY = Value.get("testKey");
    public static final Value COLUMN_NAME = Value.get("column_name");
    public static final Value COLUMN_VALUE = Value.get(new byte[]{1, 2, 3});

    private AerospikeClient client = new AerospikeClient(null, container.getContainerIpAddress(),
            container.getMappedPort(AEROSPIKE_PROPERTIES.getPort()));

    private MutateOperations mutateOperations = new BasicMutateOperations(
            new AerospikeOperations("test", AEROSPIKE_PROPERTIES.getNamespace(), client,
                    new AerospikePolicyProvider(getAerospikeConfiguration(container)),
                    Executors.newCachedThreadPool(),
                    Executors.newCachedThreadPool()));


    @Test
    public void shouldDeleteKeyIdempotentlyIfWal()  {
        //when
        mutateOperations.mutate(STORE_NAME, KEY,
                singletonMap(COLUMN_NAME, COLUMN_VALUE), true);

        //then
        mutateOperations.mutate(STORE_NAME, KEY,
                singletonMap(COLUMN_NAME, Value.NULL), true);

        //expect
        mutateOperations.mutate(STORE_NAME, KEY,
                singletonMap(COLUMN_NAME, Value.NULL), true);
    }

    @Test(expected = AerospikeException.class)
    public void shouldFailOnDeleteIfNotWal()  {
        //when
        mutateOperations.mutate(STORE_NAME, KEY,
                singletonMap(COLUMN_NAME, COLUMN_VALUE), false);

        //then
        mutateOperations.mutate(STORE_NAME, KEY,
                singletonMap(COLUMN_NAME, Value.NULL), false);

        //expect
        mutateOperations.mutate(STORE_NAME, KEY,
                singletonMap(COLUMN_NAME, Value.NULL), false);
    }

}
