package com.playtika.janusgraph.aerospike.operations.batch;

import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.policy.WritePolicy;
import com.playtika.janusgraph.aerospike.operations.AerospikeOperations;
import nosql.batch.update.aerospike.lock.AerospikeExpectedValuesOperations;
import nosql.batch.update.aerospike.lock.AerospikeLock;
import nosql.batch.update.lock.PermanentLockingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static com.playtika.janusgraph.aerospike.operations.AerospikeOperations.ENTRIES_BIN_NAME;
import static com.playtika.janusgraph.aerospike.util.AsyncUtil.completeAll;
import static nosql.batch.update.lock.Lock.LockType.SAME_BATCH;


public class BatchExpectedValueOperations
        implements AerospikeExpectedValuesOperations<Map<Key, ExpectedValue>> {

    private static final Logger logger = LoggerFactory.getLogger(BatchExpectedValueOperations.class);

    private static final WritePolicy checkValuesPolicy = new WritePolicy();
    static {
        checkValuesPolicy.respondAllOps = true;
    }

    private final AerospikeOperations aerospikeOperations;

    public BatchExpectedValueOperations(AerospikeOperations aerospikeOperations) {
        this.aerospikeOperations = aerospikeOperations;
    }

    @Override
    public void checkExpectedValues(List<AerospikeLock> locks, Map<Key, ExpectedValue> expectedValues) throws PermanentLockingException {
        if(locks.size() != expectedValues.size()){
            throw new IllegalArgumentException("locks.size() != expectedValues.size()");
        }

        completeAll(locks,
                aerospikeLock -> aerospikeLock.lockType  != SAME_BATCH,
                aerospikeLock -> {
                    ExpectedValue expectedValue = expectedValues.get(aerospikeLock.key);
                    Key keyToCheck = aerospikeOperations.getKey(expectedValue.storeName, expectedValue.key);
                    return checkColumnValues(keyToCheck, expectedValue.values);
                },
                () -> new PermanentLockingException("Some values don't match expected values"),
                aerospikeOperations.getAerospikeExecutor());
    }

    private boolean checkColumnValues(final Key key, final Map<Value, Value> valuesForKey) {
        if(valuesForKey.isEmpty()){
            return true;
        }

        int columnsNo = valuesForKey.size();
        Value[] columns = new Value[columnsNo];
        Operation[] operations = new Operation[columnsNo];
        int i = 0;
        for (Value column : valuesForKey.keySet()) {
            columns[i] = column;
            operations[i] = MapOperation.getByKey(ENTRIES_BIN_NAME, column, MapReturnType.VALUE);
            i++;
        }

        try {
            Record record = aerospikeOperations.getClient().operate(checkValuesPolicy, key, operations);

            if (record != null) {
                if (columnsNo > 1) {
                    List<?> resultList;
                    if ((resultList = record.getList(ENTRIES_BIN_NAME)) != null) {
                        for (int j = 0, n = resultList.size(); j < n; j++) {
                            Value column = columns[j];
                            if (!checkValue(key, column, valuesForKey.get(column), (byte[]) resultList.get(j))) {
                                return false;
                            }
                        }
                    }
                    return true;
                } else { //columnsNo == 1
                    byte[] actualValueData = (byte[]) record.getValue(ENTRIES_BIN_NAME);
                    Value column = columns[0];
                    return checkValue(key, column, valuesForKey.get(column), actualValueData);
                }
            } else {
                return valuesForKey.values().stream()
                        .allMatch(value -> value.equals(Value.NULL));
            }
        } catch (Throwable throwable) {
            logger.error("Error while checkColumnValues for key={}, values={}", key, valuesForKey, throwable);
            throw throwable;
        }
    }

    private boolean checkValue(Key key, Value column, Value expectedValue, byte[] actualValue) {
        if(expectedValue.equals(Value.get(actualValue))
                || expectedValue instanceof Value.ByteSegmentValue
                && expectedValue.equals(Value.get(actualValue, 0, actualValue != null ? actualValue.length : 0))){
            return true;
        } else {
            logger.info("Unexpected value for key=[{}], column=[{}], expected=[{}], actual=[{}]", key, column, expectedValue, actualValue);
            return false;
        }
    }
}
