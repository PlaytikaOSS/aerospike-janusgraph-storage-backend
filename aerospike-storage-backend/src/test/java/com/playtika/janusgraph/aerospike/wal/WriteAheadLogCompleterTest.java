package com.playtika.janusgraph.aerospike.wal;

import com.aerospike.AerospikeContainer;
import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Value;
import com.playtika.janusgraph.aerospike.AerospikeStoreManager;
import org.janusgraph.diskstorage.BackendException;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;

import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeContainer;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.*;

public class WriteAheadLogCompleterTest {

    @ClassRule
    public static AerospikeContainer container = getAerospikeContainer();

    private AerospikeClient client = new AerospikeClient(null, container.getContainerIpAddress(), container.getPort());

    static final String WAL_NAMESPACE = container.getNamespace();
    static final String WAL_SET_NAME = "wal";

    private Clock clock = mock(Clock.class);

    public static final long STALE_TRANSACTION_LIFETIME_THRESHOLD_IN_MS = 1000;
    private WriteAheadLogManager walManager = new WriteAheadLogManager(client, WAL_NAMESPACE, WAL_SET_NAME, clock, STALE_TRANSACTION_LIFETIME_THRESHOLD_IN_MS);

    private AerospikeStoreManager storeManager = mock(AerospikeStoreManager.class);

    WriteAheadLogCompleter writeAheadLogCompleter = new WriteAheadLogCompleter(
            client, WAL_NAMESPACE, WAL_SET_NAME,
            walManager, 10000, storeManager);

    @Test
    public void shouldCompleteStaleTransactions() throws BackendException, InterruptedException {
        writeTransaction(0);
        writeTransaction(1);
        writeTransaction(2);

        when(clock.millis()).thenReturn(STALE_TRANSACTION_LIFETIME_THRESHOLD_IN_MS + 5);

        writeAheadLogCompleter.start();

        Thread.sleep(100);

        writeAheadLogCompleter.shutdown();

        verify(storeManager, times(3)).processAndDeleteTransaction(any(), any(), any(), anyBoolean());
    }

    private void writeTransaction(long timestamp){
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

        when(clock.millis()).thenReturn(timestamp);
        walManager.writeTransaction(locks, mutations);
    }

}
