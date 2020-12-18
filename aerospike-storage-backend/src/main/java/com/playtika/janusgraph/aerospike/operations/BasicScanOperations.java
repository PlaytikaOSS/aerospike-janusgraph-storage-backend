package com.playtika.janusgraph.aerospike.operations;

import com.aerospike.client.policy.ScanPolicy;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;

public class BasicScanOperations implements ScanOperations {

    public static final int WAIT_TIMEOUT_IN_SECONDS = 4;

    private static Logger logger = LoggerFactory.getLogger(BasicScanOperations.class);

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
        logger.warn("Running scan operation storeName=[{}], query=[{}], tx=[{}]", storeName, query, txh);
        if (logger.isTraceEnabled()) {
            logger.trace("Running scan operation stacktrace:\n{}",
                         Stream.of(Thread.currentThread().getStackTrace())
                               .map(StackTraceElement::toString)
                               .collect(Collectors.joining("\n")));
        }

        AerospikeKeyIterator keyIterator = new AerospikeKeyIterator(query);

        scanExecutor.execute(() -> {
            try {
                aerospikeOperations.getClient().scanAll(scanPolicy,
                        aerospikeOperations.getNamespace(), aerospikeOperations.getSetName(storeName), keyIterator);
                logger.info("Finished scan operation storeName=[{}], query=[{}], tx=[{}]", storeName, query, txh);
            } catch (Throwable t){
                logger.error("Error while running scan operation storeName=[{}], query=[{}], tx=[{}]",
                        storeName, query, txh, t);
                throw t;
            } finally {
                keyIterator.close();
            }
        });

        return keyIterator;
    }

    @Override
    public void close(){
        shutdownAndAwaitTermination(scanExecutor, WAIT_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);
    }
}
