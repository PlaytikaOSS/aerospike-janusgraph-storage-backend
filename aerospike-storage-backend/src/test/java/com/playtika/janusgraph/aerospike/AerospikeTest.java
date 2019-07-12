package com.playtika.janusgraph.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.cdt.MapWriteMode;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;


import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.playtika.janusgraph.aerospike.AerospikeKeyColumnValueStore.*;
import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.AEROSPIKE_PROPERTIES;
import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeContainer;
import static org.assertj.core.api.Assertions.assertThat;

public class AerospikeTest {


    @ClassRule
    public static GenericContainer container = getAerospikeContainer();

    private AerospikeClient client = new AerospikeClient(null, container.getContainerIpAddress(),
            container.getMappedPort(AEROSPIKE_PROPERTIES.getPort()));

    @Test
    public void shouldNotRemoveRecordOnEmptyMap(){
        Key aerospikeKey = new Key(AEROSPIKE_PROPERTIES.getNamespace(), "test_set", Value.get(111));

        Value key = Value.get(222);
        Map<Value, Value> itemsToAdd = new HashMap<Value, Value>(){{
            put(key, Value.get(333));
        }};

        client.operate(null, aerospikeKey,
                MapOperation.putItems(mapPolicy, ENTRIES_BIN_NAME, itemsToAdd));

        assertThat(client.exists(null, aerospikeKey)).isTrue();

        Record record = client.operate(null, aerospikeKey,
                MapOperation.removeByKeyList(ENTRIES_BIN_NAME, Collections.singletonList(key), MapReturnType.NONE));

        assertThat(client.exists(null, aerospikeKey)).isTrue();
        assertThat(record.getMap(ENTRIES_BIN_NAME)).isNull();
    }
}
