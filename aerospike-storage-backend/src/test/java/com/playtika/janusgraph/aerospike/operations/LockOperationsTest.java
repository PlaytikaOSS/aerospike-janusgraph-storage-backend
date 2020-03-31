package com.playtika.janusgraph.aerospike.operations;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapPolicy;
import com.playtika.janusgraph.aerospike.AerospikePolicyProvider;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.locking.PermanentLockingException;
import org.janusgraph.diskstorage.locking.TemporaryLockingException;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;

import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.AEROSPIKE_PROPERTIES;
import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeConfiguration;
import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeContainer;
import static com.playtika.janusgraph.aerospike.operations.AerospikeOperations.ENTRIES_BIN_NAME;
import static com.playtika.janusgraph.aerospike.transaction.WriteAheadLogManagerBasic.getBytesFromUUID;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class LockOperationsTest {

    @ClassRule
    public static GenericContainer container = getAerospikeContainer();

    public static final Key KEY = new Key(AEROSPIKE_PROPERTIES.getNamespace(), "test.test", "test_key");
    public static final Key LOCK_KEY = new Key(AEROSPIKE_PROPERTIES.getNamespace(), "test.test.lock", "test_key");
    public static final UUID TRANSACTION_ID = UUID.randomUUID();
    public static final Value COLUMN_NAME = Value.get("column_name");
    public static final Value COLUMN_NAME_2 = Value.get("column_name_2");
    public static final Value COLUMN_VALUE = Value.get(new byte[]{1, 2, 3});

    private AerospikeClient client = new AerospikeClient(null, container.getContainerIpAddress(),
            container.getMappedPort(AEROSPIKE_PROPERTIES.getPort()));

    private LockOperations lockOperations = new BasicLockOperations(
            new AerospikeOperations("test", AEROSPIKE_PROPERTIES.getNamespace(), client,
                    new AerospikePolicyProvider(getAerospikeConfiguration(container)),
                    Executors.newCachedThreadPool(),
                    Executors.newCachedThreadPool()));

    @Before
    public void clear() {
        client.delete(null, KEY);
        client.delete(null, LOCK_KEY);
    }

    @Test
    public void shouldCheckAndLockIfRecordDoesntExist() throws BackendException {
        assertThat(execute()).hasSize(1);
    }

    @Test(expected = TemporaryLockingException.class)
    public void shouldNotLockIfAlreadyLocked() throws BackendException {
        assertThat(execute()).hasSize(1);
        execute(UUID.randomUUID());
    }

    @Test(expected = TemporaryLockingException.class)
    public void shouldNotLockIfAlreadyLocked2() throws BackendException {
        assertThat(execute(UUID.randomUUID(), COLUMN_NAME, Value.NULL)).hasSize(1);
        execute(UUID.randomUUID(), COLUMN_NAME, Value.NULL);
    }

    @Test
    public void shouldLockIfSameTransactionId() throws BackendException {
        assertThat(execute()).hasSize(1);
        assertThat(execute()).hasSize(1);
    }

    @Test
    public void shouldLockIfMatchExpectedValue() throws BackendException {
        client.operate(null, KEY,
                MapOperation.put(new MapPolicy(), ENTRIES_BIN_NAME,
                COLUMN_NAME, COLUMN_VALUE));
        try {
            execute(TRANSACTION_ID, COLUMN_NAME, Value.get(new byte[]{1, 1}));
            fail();
        } catch (PermanentLockingException e) {
        }
        try {
            execute(TRANSACTION_ID, COLUMN_NAME, Value.get(new byte[]{1, 1}));
            fail();
        } catch (PermanentLockingException e) {
        }
        assertThat(execute(TRANSACTION_ID, COLUMN_NAME, COLUMN_VALUE)).hasSize(1);
    }

    @Test
    public void shouldLockIfMatchExpectedNullValue() throws BackendException {
        client.operate(null, KEY,
                MapOperation.put(new MapPolicy(), ENTRIES_BIN_NAME,
                        COLUMN_NAME, Value.NULL));
        try {
            execute(TRANSACTION_ID, COLUMN_NAME, Value.get(new byte[]{1, 1}));
            fail();
        } catch (PermanentLockingException e) {
        }

        assertThat(execute(TRANSACTION_ID, COLUMN_NAME, Value.NULL)).hasSize(1);
    }

    @Test
    public void shouldLockIfMatchAllExpectedValue() throws BackendException {
        client.operate(null, KEY,
                MapOperation.put(new MapPolicy(), ENTRIES_BIN_NAME,
                        COLUMN_NAME, COLUMN_VALUE));
        client.operate(null, KEY,
                MapOperation.put(new MapPolicy(), ENTRIES_BIN_NAME,
                        COLUMN_NAME_2, Value.NULL));
        try {
            execute(TRANSACTION_ID, COLUMN_NAME, Value.get(new byte[]{1, 1}));
            fail();
        } catch (PermanentLockingException e) {
        }

        assertThat(execute(TRANSACTION_ID, new HashMap<Value, Value>(){{
            put(COLUMN_NAME, COLUMN_VALUE);
            put(COLUMN_NAME_2, Value.NULL);
        }})).hasSize(1);
    }

    private Set<Key> execute() throws BackendException {
        return execute(TRANSACTION_ID);
    }

    private Set<Key> execute(UUID transactionId) throws BackendException {
        return execute(transactionId, null, null);
    }

    private Set<Key> execute(UUID transactionId, Value column, Value expectedValue) throws BackendException {
        return execute(transactionId,
                singletonMap(
                        column != null ? column : Value.NULL,
                        expectedValue != null ? expectedValue : Value.NULL));
    }

    private Set<Key> execute(UUID transactionId, Map<Value, Value> expectedValues) throws BackendException {
        return lockOperations.acquireLocks(Value.get(getBytesFromUUID(transactionId)),
                singletonMap("test", singletonMap(Value.get("test_key"), expectedValues)), true,
                keyLockTypeMap -> lockOperations.releaseLocks(keyLockTypeMap.keySet()));
    }
}
