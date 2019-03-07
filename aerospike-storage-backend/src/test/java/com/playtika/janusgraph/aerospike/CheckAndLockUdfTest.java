package com.playtika.janusgraph.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapPolicy;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.playtika.janusgraph.aerospike.AerospikeKeyColumnValueStore.ENTRIES_BIN_NAME;
import static com.playtika.janusgraph.aerospike.AerospikeStoreManager.registerUdfs;
import static com.playtika.janusgraph.aerospike.LockOperationsUdf.LockResult;
import static com.playtika.janusgraph.aerospike.LockOperationsUdf.LockResult.*;
import static com.playtika.janusgraph.aerospike.LockOperationsUdf.checkAndLock;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;

public class CheckAndLockUdfTest {

    public static final Key KEY = new Key("test", "test", "test_key");
    public static final int TTL_MSEC = 500;
    public static final Value COLUMN_NAME = Value.get("column_name");
    public static final Value COLUMN_NAME_2 = Value.get("column_name_2");
    public static final Value COLUMN_VALUE = Value.get(new byte[]{1, 2, 3});
    private static AerospikeClient client = new AerospikeClient(null, "localhost", 3000);


    @BeforeClass
    public static void register() {
        registerUdfs(client);
    }

    @Before
    public void clear() {
        client.delete(null, KEY);
    }

    @Test
    public void shouldCheckAndLockIfRecordDoesntExist(){
        assertThat(execute()).isEqualTo(LOCKED);
    }

    @Test
    public void shouldNotLockIfRecordExists(){
        assertThat(execute()).isEqualTo(LOCKED);
        assertThat(execute()).isEqualTo(ALREADY_LOCKED);
    }

    @Test
    public void shouldLockAfterTtl() throws InterruptedException {
        assertThat(execute()).isEqualTo(LOCKED);
        Thread.sleep(600);
        assertThat(execute()).isEqualTo(LOCKED);
    }

    @Test
    public void shouldLockIfMatchExpectedValue()  {
        client.operate(null, KEY,
                MapOperation.put(new MapPolicy(), ENTRIES_BIN_NAME,
                COLUMN_NAME, COLUMN_VALUE));
        assertThat(execute(TTL_MSEC, COLUMN_NAME, Value.get(new byte[]{1, 1})))
                .isEqualTo(CHECK_FAILED);
        assertThat(execute(TTL_MSEC, COLUMN_NAME, COLUMN_VALUE))
                .isEqualTo(LOCKED);
    }

    @Test
    public void shouldLockIfMatchExpectedNullValue()  {
        client.operate(null, KEY,
                MapOperation.put(new MapPolicy(), ENTRIES_BIN_NAME,
                        COLUMN_NAME, Value.NULL));
        assertThat(execute(TTL_MSEC, COLUMN_NAME, Value.get(new byte[]{1, 1})))
                .isEqualTo(CHECK_FAILED);
        assertThat(execute(TTL_MSEC, COLUMN_NAME, Value.NULL))
                .isEqualTo(LOCKED);
    }

    @Test
    public void shouldLockIfMatchAllExpectedValue()  {
        client.operate(null, KEY,
                MapOperation.put(new MapPolicy(), ENTRIES_BIN_NAME,
                        COLUMN_NAME, COLUMN_VALUE));
        client.operate(null, KEY,
                MapOperation.put(new MapPolicy(), ENTRIES_BIN_NAME,
                        COLUMN_NAME_2, Value.NULL));
        assertThat(execute(TTL_MSEC, COLUMN_NAME, Value.get(new byte[]{1, 1})))
                .isEqualTo(CHECK_FAILED);
        assertThat(execute(TTL_MSEC, new HashMap<Value, Value>(){{
            put(COLUMN_NAME, COLUMN_VALUE);
            put(COLUMN_NAME_2, Value.NULL);
        }})).isEqualTo(LOCKED);
    }

    private LockResult execute() {
        return execute(TTL_MSEC, null, null);
    }

    private LockResult execute(long lockTtl, Value column, Value expectedValue) {
        return execute(lockTtl,
                singletonMap(
                        column != null ? column : Value.NULL,
                        expectedValue != null ? expectedValue : Value.NULL));
    }

    private LockResult execute(long lockTtl, Map<Value, Value> expectedValues) {
        return checkAndLock(client, KEY, lockTtl, expectedValues);
    }
}
