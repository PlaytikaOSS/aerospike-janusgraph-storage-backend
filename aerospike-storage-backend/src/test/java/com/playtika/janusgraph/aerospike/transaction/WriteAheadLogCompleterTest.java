package com.playtika.janusgraph.aerospike.transaction;

import com.aerospike.client.Value;
import com.playtika.janusgraph.aerospike.operations.BasicOperations;
import com.playtika.janusgraph.aerospike.operations.LockOperations;
import com.playtika.janusgraph.aerospike.operations.MutateOperations;
import com.playtika.janusgraph.aerospike.operations.Operations;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeConfiguration;
import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeContainer;
import static com.playtika.janusgraph.aerospike.ConfigOptions.WAL_STALE_TRANSACTION_LIFETIME_THRESHOLD;
import static com.playtika.janusgraph.aerospike.operations.AerospikeOperations.getValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.*;

public class WriteAheadLogCompleterTest {

    public static final Value COLUMN_1 = Value.get(new byte[]{0});
    public static final Value COLUMN_2 = Value.get(new byte[]{1});
    public static final String STORE_NAME = "storeName";
    public static final long STALE_THRESHOLD = 1000L;

    @ClassRule
    public static GenericContainer container = getAerospikeContainer();

    private final Value key = Value.get(new byte[]{1, 2, 3});

    @Test
    public void shouldCompleteStaleTransactions() throws InterruptedException, BackendException {
        ModifiableConfiguration configuration = getAerospikeConfiguration(container)
                .set(WAL_STALE_TRANSACTION_LIFETIME_THRESHOLD, STALE_THRESHOLD);
        Operations operations = new BasicOperations(configuration);
        writeTransactions(operations.getTransactionalOperations().getWriteAheadLogManager());
        operations.close();
        Thread.sleep(STALE_THRESHOLD * 3);

        SpyOperations spyOperations = new SpyOperations(configuration);
        spyOperations.getWriteAheadLogCompleter().start();
        WriteAheadLogManager walManager = spyOperations.getTransactionalOperations().getWriteAheadLogManager();
        while(!walManager.getStaleTransactions().isEmpty()){
            Thread.sleep(100);
        }
        spyOperations.close();

        verify(spyOperations.spy, times(4)).processAndDeleteTransaction(any(), any(), any(), anyBoolean());
        verify(spyOperations.spy).releaseLocksAndDeleteWalTransactionOnError(any(), any());
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
                            put(COLUMN_1, expectedValue1);
                            put(COLUMN_2, expectedValue2);
                        }});
            }});
        }};

        Map<String, Map<Value, Map<Value, Value>>> mutations = new HashMap<String, Map<Value, Map<Value, Value>>>() {{
            put(STORE_NAME, new HashMap<Value, Map<Value, Value>>() {{
                put(key,
                        new HashMap<Value, Value>() {{
                            put(COLUMN_1, resultValue1);
                            put(COLUMN_2, resultValue2);
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

    private static class SpyOperations extends BasicOperations{

        TransactionalOperations spy;

        public SpyOperations(Configuration configuration) {
            super(configuration);
        }

        protected TransactionalOperations initWalCompleterTransactionalOperations(
                Supplier<WriteAheadLogManager> writeAheadLogManager,
                Supplier<LockOperations> lockOperations,
                Supplier<MutateOperations> mutateOperations){
            return spy = spy(new TransactionalOperations(writeAheadLogManager.get(), lockOperations.get(), mutateOperations.get()));
        }
    }

}
