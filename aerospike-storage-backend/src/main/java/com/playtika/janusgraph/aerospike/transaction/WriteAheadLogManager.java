package com.playtika.janusgraph.aerospike.transaction;

import com.aerospike.client.Value;

import java.util.List;
import java.util.Map;

public interface WriteAheadLogManager {

    Value writeTransaction(Map<String, Map<Value, Map<Value, Value>>> locks,
                           Map<String, Map<Value, Map<Value, Value>>> mutations);

    void deleteTransaction(Value transactionId);

    List<WalTransaction> getStaleTransactions();

    final class WalTransaction implements Comparable<WriteAheadLogManagerBasic.WalTransaction>{
        final Value transactionId;
        final long timestamp;
        final Map<String, Map<Value, Map<Value, Value>>> locks;
        final Map<String, Map<Value, Map<Value, Value>>> mutations;

        WalTransaction(Value transactionId, long timestamp,
                       Map<String, Map<Value, Map<Value, Value>>> locks,
                       Map<String, Map<Value, Map<Value, Value>>> mutations) {
            this.transactionId = transactionId;
            this.timestamp = timestamp;
            this.locks = locks;
            this.mutations = mutations;
        }

        @Override
        public int compareTo(WriteAheadLogManager.WalTransaction transaction) {
            return Long.compare(timestamp, transaction.timestamp);
        }
    }
}
