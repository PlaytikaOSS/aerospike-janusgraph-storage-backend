package com.playtika.janusgraph.aerospike.transaction;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.playtika.janusgraph.aerospike.AerospikePolicyProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Used to write transactions into WAL storage
 * so failed transaction can be completed eventually by {@link WriteAheadLogCompleter}
 */
public class WriteAheadLogManagerBasic implements WriteAheadLogManager {

    private static Logger logger = LoggerFactory.getLogger(WriteAheadLogManagerBasic.class);

    private static final String UUID_BIN = "uuid";
    private static final String TIMESTAMP_BIN = "timestamp";
    private static final String LOCKS_BIN = "locks";
    private static final String MUTATIONS_BIN = "mutations";

    private final IAerospikeClient client;
    private final String walNamespace;
    private final String walSetName;
    private final String secondaryIndexName;
    private final Clock clock;
    private final long staleTransactionLifetimeThresholdInMs;
    private WritePolicy writePolicy;
    private WritePolicy deletePolicy;

    public WriteAheadLogManagerBasic(WalOperations walOperations, Clock clock) {
        this.client = walOperations.getAerospikeOperations().getClient();
        this.walNamespace = walOperations.getWalNamespace();
        this.walSetName = walOperations.getWalSetName();
        this.secondaryIndexName = walSetName;
        this.clock = clock;
        this.staleTransactionLifetimeThresholdInMs = walOperations.getStaleTransactionLifetimeThresholdInMs();
        AerospikePolicyProvider aerospikePolicyProvider = walOperations.getAerospikeOperations().getAerospikePolicyProvider();
        this.writePolicy = new WritePolicy(aerospikePolicyProvider.writePolicy());
        this.writePolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;

        this.deletePolicy = aerospikePolicyProvider.deletePolicy();

        try {
            client.createIndex(null, walNamespace, walSetName, secondaryIndexName, TIMESTAMP_BIN, IndexType.NUMERIC)
                    .waitTillComplete(200, 0);
        } catch (AerospikeException ae) {
            if(ae.getResultCode() == ResultCode.INDEX_ALREADY_EXISTS){
                logger.info("Will not create WAL secondary index as it already exists");
            } else {
                throw ae;
            }
        }
    }

    @Override
    public Value writeTransaction(Map<String, Map<Value, Map<Value, Value>>> locks,
                                  Map<String, Map<Value, Map<Value, Value>>> mutations){

        Value transactionId = Value.get(getBytesFromUUID(UUID.randomUUID()));
        try {
            client.put(writePolicy,
                    new Key(walNamespace, walSetName, transactionId),
                    new Bin(UUID_BIN, transactionId),
                    new Bin(TIMESTAMP_BIN, Value.get(clock.millis())),
                    new Bin(LOCKS_BIN, stringMapToValue(locks)),
                    new Bin(MUTATIONS_BIN, stringMapToValue(mutations)));
        } catch (AerospikeException ae) {
            if(ae.getResultCode() == ResultCode.RECORD_TOO_BIG){
                logger.error("locks data size: {}", toBytes(stringMapToValue(locks)).length);
                logger.error("mutations data size: {}", toBytes(stringMapToValue(mutations)).length);
            }
            throw ae;
        }
        return transactionId;
    }

    @Override
    public void deleteTransaction(Value transactionId) {
        client.delete(deletePolicy, new Key(walNamespace, walSetName, transactionId));
    }

    private Value stringMapToValue(Map<String, Map<Value, Map<Value, Value>>> map){
        Map<Value, Map<Value, Map<Value, Value>>> locksValue = new HashMap<>(map.size());
        for(Map.Entry<String, Map<Value, Map<Value, Value>>> locksEntry : map.entrySet()){
            locksValue.put(Value.get(locksEntry.getKey()), locksEntry.getValue());
        }
        return Value.get(locksValue);
    }

    public static byte[] getBytesFromUUID(UUID uuid) {
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());

        return bb.array();
    }

    @Override
    public List<WalTransaction> getStaleTransactions(){

        Statement statement = new Statement();
        statement.setNamespace(walNamespace);
        statement.setSetName(walSetName);
        statement.setFilter(Filter.range(TIMESTAMP_BIN,
                0,  Math.max(clock.millis() - staleTransactionLifetimeThresholdInMs, 0)));
        RecordSet recordSet = client.query(null, statement);

        List<WalTransaction> staleTransactions = new ArrayList<>();
        recordSet.iterator().forEachRemaining(keyRecord -> {
            Record record = keyRecord.record;
            staleTransactions.add(new WalTransaction(
                    Value.get(record.getValue(UUID_BIN)),
                    record.getLong(TIMESTAMP_BIN),
                    wrapMap((Map<String, Map<byte[], Map<byte[], byte[]>>>)record.getMap(LOCKS_BIN)),
                    wrapMap((Map<String, Map<byte[], Map<byte[], byte[]>>>)record.getMap(MUTATIONS_BIN))));
        });
        Collections.sort(staleTransactions);

        return staleTransactions;
    }

    private static Map<String, Map<Value, Map<Value, Value>>> wrapMap(
            Map<String, Map<byte[], Map<byte[], byte[]>>> map){
        Map<String, Map<Value, Map<Value, Value>>> resultMap = new HashMap<>(map.size());
        for(Map.Entry<String, Map<byte[], Map<byte[], byte[]>>> mapEntry : map.entrySet()){
            resultMap.put(mapEntry.getKey(), wrapBytesBytesMap(mapEntry.getValue()));
        }
        return resultMap;
    }

    private static Map<Value, Map<Value, Value>> wrapBytesBytesMap(Map<byte[], Map<byte[], byte[]>> map){
        Map<Value, Map<Value, Value>> resultMap = new HashMap<>(map.size());
        for(Map.Entry<byte[], Map<byte[], byte[]>> mapEntry : map.entrySet()){
            resultMap.put(Value.get(mapEntry.getKey()), wrapBytesMap(mapEntry.getValue()));
        }
        return resultMap;
    }

    private static Map<Value, Value> wrapBytesMap(Map<byte[], byte[]> map){
        Map<Value, Value> resultMap = new HashMap<>(map.size());
        for(Map.Entry<byte[], byte[]> mapEntry : map.entrySet()){
            resultMap.put(Value.get(mapEntry.getKey()), Value.get(mapEntry.getValue()));
        }
        return resultMap;
    }

    static byte[] toBytes(Value value){
        byte[] bytes = new byte[value.estimateSize()];
        value.write(bytes, 0);
        return bytes;
    }
}
