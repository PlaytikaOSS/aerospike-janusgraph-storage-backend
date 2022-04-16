package com.playtika.janusgraph.aerospike.operations.batch;

import com.playtika.janusgraph.aerospike.operations.AerospikeOperations;
import org.janusgraph.diskstorage.configuration.Configuration;

import static com.playtika.janusgraph.aerospike.ConfigOptions.WAL_MAX_BATCH_SIZE;
import static com.playtika.janusgraph.aerospike.ConfigOptions.WAL_NAMESPACE;
import static com.playtika.janusgraph.aerospike.ConfigOptions.WAL_STALE_TRANSACTION_LIFETIME_THRESHOLD;

public class WalOperations {

    private final String walNamespace;
    private final Long staleTransactionLifetimeThresholdInMs;
    private final Integer maxBatchSize;
    private final String walSetName;
    private final AerospikeOperations aerospikeOperations;

    public WalOperations(Configuration configuration, AerospikeOperations aerospikeOperations) {
        this.walNamespace = configuration.get(WAL_NAMESPACE);
        this.staleTransactionLifetimeThresholdInMs = configuration.get(WAL_STALE_TRANSACTION_LIFETIME_THRESHOLD);
        this.maxBatchSize = configuration.get(WAL_MAX_BATCH_SIZE);
        this.walSetName = aerospikeOperations.getGraphPrefix() + "wal";
        this.aerospikeOperations = aerospikeOperations;
    }

    public String getWalNamespace() {
        return walNamespace;
    }

    public Long getStaleTransactionLifetimeThresholdInMs() {
        return staleTransactionLifetimeThresholdInMs;
    }

    public Integer getMaxBatchSize() {
        return maxBatchSize;
    }

    public String getWalSetName() {
        return walSetName;
    }

    public AerospikeOperations getAerospikeOperations() {
        return aerospikeOperations;
    }
}
