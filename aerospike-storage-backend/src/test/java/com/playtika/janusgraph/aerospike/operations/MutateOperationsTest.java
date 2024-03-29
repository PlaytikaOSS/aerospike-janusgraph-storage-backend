package com.playtika.janusgraph.aerospike.operations;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Value;
import com.aerospike.client.async.NioEventLoops;
import com.playtika.janusgraph.aerospike.AerospikePolicyProvider;
import org.janusgraph.diskstorage.BackendException;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.AEROSPIKE_PROPERTIES;
import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeClient;
import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeConfiguration;
import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeContainer;
import static com.playtika.janusgraph.aerospike.operations.BasicOperations.AEROSPIKE_PREFIX;
import static com.playtika.janusgraph.aerospike.operations.BasicOperations.BATCH_PREFIX;
import static com.playtika.janusgraph.aerospike.operations.BasicOperations.executorService;
import static java.util.Collections.singletonMap;
import static org.janusgraph.graphdb.configuration.JanusGraphConstants.JANUSGRAPH_ID_STORE_NAME;

public class MutateOperationsTest {

    @ClassRule
    public static GenericContainer container = getAerospikeContainer();

    public static final String STORE_NAME = "testStore";
    public static final Value KEY = Value.get("testKey");
    public static final Value COLUMN_NAME = Value.get("column_name");
    public static final Value COLUMN_VALUE = Value.get(new byte[]{1, 2, 3});

    private NioEventLoops eventLoops = new NioEventLoops();
    private AerospikeClient client = getAerospikeClient(getAerospikeContainer(), eventLoops);

    private MutateOperations mutateOperations = new BasicMutateOperations(
            new AerospikeOperations("test",
                    AEROSPIKE_PROPERTIES.getNamespace(), AEROSPIKE_PROPERTIES.getNamespace(),
                    JANUSGRAPH_ID_STORE_NAME,
                    client,
                    new AerospikePolicyProvider(getAerospikeConfiguration(container)),
                    executorService(4, AEROSPIKE_PREFIX),
                    executorService(4, 4, BATCH_PREFIX)));


    @Test
    public void shouldDeleteKeyIdempotently() throws BackendException {
        //when
        mutateOperations.mutate(STORE_NAME, KEY,
                singletonMap(COLUMN_NAME, COLUMN_VALUE));

        //then
        mutateOperations.mutate(STORE_NAME, KEY,
                singletonMap(COLUMN_NAME, Value.NULL));

        //expect
        mutateOperations.mutate(STORE_NAME, KEY,
                singletonMap(COLUMN_NAME, Value.NULL));
    }

}
