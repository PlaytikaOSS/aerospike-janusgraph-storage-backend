package com.playtika.janusgraph.aerospike.operations;

import com.aerospike.client.policy.ScanPolicy;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static com.playtika.janusgraph.aerospike.util.AsyncUtil.WAIT_TIMEOUT_IN_SECONDS;

public class BasicScanOperations implements ScanOperations {

    private final AerospikeOperations aerospikeOperations;
    private final ScanPolicy scanPolicy;
    private final ExecutorService scanExecutor;

    public BasicScanOperations(AerospikeOperations aerospikeOperations,
                               ExecutorService scanExecutor) {
        this.aerospikeOperations = aerospikeOperations;
        this.scanPolicy = aerospikeOperations.getAerospikePolicyProvider().scanPolicy();
        this.scanExecutor = scanExecutor;
    }

    @Override
    public KeyIterator getKeys(String storeName, SliceQuery query, StoreTransaction txh) {
        AerospikeKeyIterator keyIterator = new AerospikeKeyIterator(query);

        scanExecutor.execute(() -> {
            try {
                aerospikeOperations.getClient().scanAll(scanPolicy,
                        aerospikeOperations.getNamespace(), aerospikeOperations.getSetName(storeName), keyIterator);
            } finally {
                keyIterator.terminate();
            }
        });

        return keyIterator;
    }

    @Override
    public void close(){
        shutdownAndAwaitTermination(scanExecutor, WAIT_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);
    }
}
