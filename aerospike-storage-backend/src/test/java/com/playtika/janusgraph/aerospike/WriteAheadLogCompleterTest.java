package com.playtika.janusgraph.aerospike;

import com.aerospike.client.Value;
import com.playtika.janusgraph.aerospike.wal.WriteAheadLogManager;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;

import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeConfiguration;
import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeContainer;
import static com.playtika.janusgraph.aerospike.ConfigOptions.WAL_STALE_TRANSACTION_LIFETIME_THRESHOLD;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.*;

public class WriteAheadLogCompleterTest {

    public static final byte[] COLUMN_1 = {0};
    public static final byte[] COLUMN_2 = {1};
    public static final String STORE_NAME = "storeName";
    public static final long STALE_THRESHOLD = 1000L;

    @ClassRule
    public static GenericContainer container = getAerospikeContainer();

    private final Value key = Value.get(new byte[]{1, 2, 3});

    @Test
    public void shouldCompleteStaleTransactions() throws InterruptedException, BackendException {
        ModifiableConfiguration configuration = getAerospikeConfiguration(container)
                .set(WAL_STALE_TRANSACTION_LIFETIME_THRESHOLD, STALE_THRESHOLD);
        AerospikeStoreManager storeManager = new AerospikeStoreManager(configuration);
        storeManager.getWriteAheadLogCompleter().shutdown();
        writeTransactions(storeManager.getWriteAheadLogManager());
        storeManager.close();
        Thread.sleep(STALE_THRESHOLD * 3);

        SpyAerospikeStoreManager spyStoreManager = new SpyAerospikeStoreManager(configuration);
        WriteAheadLogManager walManager = spyStoreManager.getWriteAheadLogManager();
        while(!walManager.getStaleTransactions().isEmpty()){
            Thread.sleep(100);
        }
        spyStoreManager.close();

        verify(spyStoreManager.spy, times(4)).processAndDeleteTransaction(any(), any(), any(), anyBoolean());
        verify(spyStoreManager.spy).releaseLocksAndDeleteWalTransaction(any(), any());
    }

    private void writeTransactions(WriteAheadLogManager walManager){
        writeTransaction(walManager, Value.NULL, Value.NULL,
                Value.get(new byte[]{1}), Value.get(new byte[]{2}));

        writeTransaction(walManager, Value.get(new byte[]{1}), Value.get(new byte[]{2}),
                Value.get(new byte[]{3}), Value.NULL);

        writeTransaction(walManager, Value.get(new byte[]{3}), Value.NULL,
                Value.NULL, Value.NULL);

        //will fail
        writeTransaction(walManager, Value.get(new byte[]{3}), Value.NULL,
                Value.NULL, Value.NULL);
    }

    private void writeTransaction(WriteAheadLogManager walManager,
                                  Value expectedValue1, Value expectedValue2,
                                  Value resultValue1, Value resultValue2) {
        Map<String, Map<Value, Map<Value, Value>>> locks = new HashMap<String, Map<Value, Map<Value, Value>>>() {{
            put(STORE_NAME, new HashMap<Value, Map<Value, Value>>() {{
                put(key,
                        new HashMap<Value, Value>() {{
                            put(Value.get(COLUMN_1), expectedValue1);
                            put(Value.get(COLUMN_2), expectedValue2);
                        }});
            }});
        }};

        Map<String, Map<Value, Map<Value, Value>>> mutations = new HashMap<String, Map<Value, Map<Value, Value>>>() {{
            put(STORE_NAME, new HashMap<Value, Map<Value, Value>>() {{
                put(key,
                        new HashMap<Value, Value>() {{
                            put(Value.get(COLUMN_1), resultValue1);
                            put(Value.get(COLUMN_2), resultValue2);
                        }});
            }});
        }};

        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            throw new RuntimeException();
        }

        walManager.writeTransaction(locks, mutations);
    }

    private static class SpyAerospikeStoreManager extends AerospikeStoreManager{

        TransactionalOperations spy;

        public SpyAerospikeStoreManager(Configuration configuration) {
            super(configuration);
        }

        @Override
        TransactionalOperations initTransactionalOperations(Function<String, AKeyColumnValueStore> databaseFactory,
                                                            WriteAheadLogManager writeAheadLogManager,
                                                            LockOperations lockOperations, ThreadPoolExecutor aerospikeExecutor) {
            spy = spy(new TransactionalOperations(databaseFactory, writeAheadLogManager, lockOperations, aerospikeExecutor));
            return spy;
        }
    }

}
