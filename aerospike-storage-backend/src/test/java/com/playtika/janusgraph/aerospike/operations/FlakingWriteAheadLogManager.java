package com.playtika.janusgraph.aerospike.operations;

import com.aerospike.client.Value;
import com.playtika.janusgraph.aerospike.transaction.WriteAheadLogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class FlakingWriteAheadLogManager implements WriteAheadLogManager {

    private static Logger logger = LoggerFactory.getLogger(FlakingWriteAheadLogManager.class);

    private final WriteAheadLogManager writeAheadLogManager;
    private AtomicBoolean fails;

    public FlakingWriteAheadLogManager(WriteAheadLogManager writeAheadLogManager, AtomicBoolean fails) {
        this.writeAheadLogManager = writeAheadLogManager;
        this.fails = fails;
    }

    @Override
    public Value writeTransaction(Map<String, Map<Value, Map<Value, Value>>> locks, Map<String, Map<Value, Map<Value, Value>>> mutations) {
        return writeAheadLogManager.writeTransaction(locks, mutations);
    }

    @Override
    public void deleteTransaction(Value transactionId) {
        if(!fails.get()){
            writeAheadLogManager.deleteTransaction(transactionId);
        } else {
            logger.error("deleteTransaction failed flaking for transactionId [{}]", transactionId);
        }
    }

    @Override
    public List<WalTransaction> getStaleTransactions() {
        return writeAheadLogManager.getStaleTransactions();
    }
}
