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
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

import static com.playtika.janusgraph.aerospike.operations.AerospikeOperations.ENTRIES_BIN_NAME;
import static nosql.batch.update.lock.Lock.LockType.SAME_BATCH;


public class BatchExpectedValueOperations
        implements AerospikeExpectedValuesOperations<Map<Key, ExpectedValue>> {

    private static Logger logger = LoggerFactory.getLogger(BatchExpectedValueOperations.class);

    private static final WritePolicy checkValuesPolicy = new WritePolicy();
    static {
        checkValuesPolicy.respondAllOps = true;
    }

    private final AerospikeOperations aerospikeOperations;

    public BatchExpectedValueOperations(AerospikeOperations aerospikeOperations) {
        this.aerospikeOperations = aerospikeOperations;
    }

    @Override
    public Mono<Void> checkExpectedValues(List<AerospikeLock> locks, Map<Key, ExpectedValue> expectedValues) throws PermanentLockingException {

        if(locks.size() != expectedValues.size()){
            throw new IllegalArgumentException("locks.size() != expectedValues.size()");
        }

        return Flux.fromStream(locks.stream()
                .filter(aerospikeLock -> aerospikeLock.lockType  != SAME_BATCH))
                .flatMap(aerospikeLock -> {
                    ExpectedValue expectedValue = expectedValues.get(aerospikeLock.key);
                    Key keyToCheck = aerospikeOperations.getKey(expectedValue.storeName, expectedValue.key);
                    return checkColumnValues(keyToCheck, expectedValue.values)
                            .map(successCheck -> {
                                if(!successCheck){
                                    throw Exceptions.propagate(new PermanentLockingException(String.format(
                                            "Unexpected value for key=[%s]", keyToCheck)));
                                }
                                return true;
                            });
                }).then();
    }

    private Mono<Boolean> checkColumnValues(final Key key, final Map<Value, Value> valuesForKey) {
        if(valuesForKey.isEmpty()){
            return Mono.just(true);
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
        return aerospikeOperations.getReactorClient().operate(checkValuesPolicy, key, operations)
                .map(keyRecord -> {
                    Record record = keyRecord.record;
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
                    }
                    else {
                        return valuesForKey.values().stream()
                                .allMatch(value -> value.equals(Value.NULL));
                    }
                }).doOnError(throwable -> logger.error("Error while checkColumnValues for key={}, values={}", key, valuesForKey, throwable));

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
