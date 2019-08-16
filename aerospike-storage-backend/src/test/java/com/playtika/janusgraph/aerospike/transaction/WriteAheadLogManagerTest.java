package com.playtika.janusgraph.aerospike.transaction;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Value;
import com.playtika.janusgraph.aerospike.ConfigOptions;
import com.playtika.janusgraph.aerospike.operations.BasicOperations;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import java.time.Clock;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.*;
import static com.playtika.janusgraph.aerospike.transaction.WriteAheadLogManagerBasic.toBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WriteAheadLogManagerTest {

    @ClassRule
    public static GenericContainer container = getAerospikeContainer();

    private AerospikeClient client = new AerospikeClient(null, container.getContainerIpAddress(),
            container.getMappedPort(AEROSPIKE_PROPERTIES.getPort()));

    static final String WAL_NAMESPACE = AEROSPIKE_PROPERTIES.getNamespace();

    private Clock clock = mock(Clock.class);

    private ModifiableConfiguration configuration = getAerospikeConfiguration(container)
            .set(ConfigOptions.WAL_NAMESPACE, WAL_NAMESPACE)
            .set(ConfigOptions.WAL_STALE_TRANSACTION_LIFETIME_THRESHOLD, 1000L);

    private BasicOperations basicOperations = new BasicOperations(configuration) {
        @Override
        protected  Clock getClock() {
            return clock;
        }
    };

    @Before
    public void setUp() {
        client.truncate(null, WAL_NAMESPACE, null, null);
    }

    @Test
    public void shouldNotFailIfIndexAlreadyCreated(){
        new BasicOperations(configuration);
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

        WriteAheadLogManager walManager = basicOperations.getTransactionalOperations().getWriteAheadLogManager();
        Value transactionId = walManager.writeTransaction(locks, mutations);

        when(clock.millis()).thenReturn(500L);
        walManager.writeTransaction(locks, mutations);

        when(clock.millis()).thenReturn(1100L);
        List<WriteAheadLogManagerBasic.WalTransaction> transactionList = walManager.getStaleTransactions();
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
