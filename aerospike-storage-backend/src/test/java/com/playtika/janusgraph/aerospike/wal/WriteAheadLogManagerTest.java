package com.playtika.janusgraph.aerospike.wal;

import com.aerospike.AerospikeContainer;
import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Value;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.Clock;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeContainer;
import static com.playtika.janusgraph.aerospike.wal.WriteAheadLogManager.toBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WriteAheadLogManagerTest {

    @ClassRule
    public static AerospikeContainer container = getAerospikeContainer();

    private AerospikeClient client = new AerospikeClient(null, container.getContainerIpAddress(), container.getPort());

    static final String WAL_NAMESPACE = container.getNamespace();
    static final String WAL_SET_NAME = "wal";

    private Clock clock = mock(Clock.class);
    private WriteAheadLogManager walManager = new WriteAheadLogManager(client, WAL_NAMESPACE, WAL_SET_NAME, clock, 1000);

    @Before
    public void setUp() {
        client.truncate(null, WAL_NAMESPACE, null, null);
    }

    @Test
    public void shouldNotFailIfIndexAlreadyCreated(){
        new WriteAheadLogManager(client, WAL_NAMESPACE, WAL_SET_NAME, clock, 1000);
    }

    @Test
    public void shouldSaveLoadDelete(){
        Map<String, Map<Value, Map<Value, Value>>> locks = new HashMap<String, Map<Value, Map<Value, Value>>>(){{
            put("storeName", new HashMap<Value, Map<Value, Value>>(){{
                put(Value.get(new byte[]{1,2,3}),
                        new HashMap<Value, Value>(){{
                            put(Value.get(new byte[]{0}), Value.get(new byte[]{1}));
                            put(Value.get(new byte[]{1}), Value.NULL);
                        }});
            }});
        }};

        Map<String, Map<Value, Map<Value, Value>>> mutations = new HashMap<String, Map<Value, Map<Value, Value>>>(){{
            put("storeName", new HashMap<Value, Map<Value, Value>>(){{
                put(Value.get(new byte[]{1,2,3}),
                        new HashMap<Value, Value>(){{
                            put(Value.get(new byte[]{0}), Value.NULL);
                            put(Value.get(new byte[]{1}), Value.get(new byte[]{1}));
                        }});
            }});
        }};

        Value transactionId = walManager.writeTransaction(locks, mutations);

        when(clock.millis()).thenReturn(500L);
        walManager.writeTransaction(locks, mutations);

        when(clock.millis()).thenReturn(1100L);
        List<WriteAheadLogManager.WalTransaction> transactionList = walManager.getStaleTransactions();
        assertThat(transactionList).hasSize(1);

        assertThat(equals(transactionList.get(0).transactionId, transactionId));
        assertThat(equals(Value.get(transactionList.get(0).locks), Value.get(locks)));
        assertThat(equals(Value.get(transactionList.get(0).mutations), Value.get(mutations)));

        walManager.deleteTransaction(transactionId);
        assertThat(walManager.getStaleTransactions()).isEmpty();
    }

    private static boolean equals(Value v1, Value v2){
        return Arrays.equals(toBytes(v1), toBytes(v2));
    }

}
