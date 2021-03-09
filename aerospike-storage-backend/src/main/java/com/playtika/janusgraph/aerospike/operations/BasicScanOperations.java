package com.playtika.janusgraph.aerospike.operations;

import com.aerospike.client.policy.ScanPolicy;
import com.playtika.janusgraph.aerospike.util.NamedThreadFactory;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.keycolumnvalue.KeySlicesIterator;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BasicScanOperations implements ScanOperations {

    private static final Logger logger = LoggerFactory.getLogger(BasicScanOperations.class);

    private final AerospikeOperations aerospikeOperations;
    private final ScanPolicy scanPolicy;
    private final NamedThreadFactory namedThreadFactory;

    public BasicScanOperations(AerospikeOperations aerospikeOperations,
                               NamedThreadFactory namedThreadFactory) {
        this.aerospikeOperations = aerospikeOperations;
        this.scanPolicy = aerospikeOperations.getAerospikePolicyProvider().scanPolicy();
        this.namedThreadFactory = namedThreadFactory;
    }

    @Override
    public KeyIterator getKeys(String storeName, SliceQuery query, StoreTransaction txh) {
        logger.warn("Starting scan operation storeName=[{}], query=[{}], tx=[{}]", storeName, query, txh);
        if (logger.isTraceEnabled()) {
            logger.trace("Running scan operation stacktrace:\n{}",
                    Stream.of(Thread.currentThread().getStackTrace())
                            .map(StackTraceElement::toString)
                            .collect(Collectors.joining("\n")));
        }

        AerospikeKeyIterator keyIterator = new AerospikeKeyIterator(query);

        Thread scanThread = namedThreadFactory.newThread(() -> {
            try {
                logger.warn("Running scan operation storeName=[{}], query=[{}], tx=[{}]", storeName, query, txh);
                aerospikeOperations.getClient().scanAll(scanPolicy,
                        aerospikeOperations.getNamespace(), aerospikeOperations.getSetName(storeName), keyIterator);
                logger.info("Finished scan operation storeName=[{}], query=[{}], tx=[{}]", storeName, query, txh);
            } catch (Throwable t) {
                logger.error("Error while running scan operation storeName=[{}], query=[{}], tx=[{}]",
                        storeName, query, txh, t);
                throw t;
            } finally {
                keyIterator.close();
            }
        });

        keyIterator.setThread(scanThread);

        scanThread.start();

        return keyIterator;
    }

    @Override
    public KeySlicesIterator getKeys(String storeName, List<SliceQuery> queries, StoreTransaction txh) {
        logger.warn("Starting scan operation storeName=[{}], query=[{}], tx=[{}]", storeName, queries, txh);
        if (logger.isTraceEnabled()) {
            logger.trace("Running scan operation stacktrace:\n{}",
                         Stream.of(Thread.currentThread().getStackTrace())
                               .map(StackTraceElement::toString)
                               .collect(Collectors.joining("\n")));
        }

        AerospikeKeySlicesIterator keyIterator = new AerospikeKeySlicesIterator(queries);

        Thread scanThread = namedThreadFactory.newThread(() -> {
            try {
                logger.warn("Running scan operation storeName=[{}], queries=[{}], tx=[{}]", storeName, queries, txh);
                aerospikeOperations.getClient().scanAll(scanPolicy,
                        aerospikeOperations.getNamespace(), aerospikeOperations.getSetName(storeName), keyIterator);
                logger.info("Finished scan operation storeName=[{}], queries=[{}], tx=[{}]", storeName, queries, txh);
            } catch (Throwable t) {
                logger.error("Error while running scan operation storeName=[{}], queries=[{}], tx=[{}]",
                        storeName, queries, txh, t);
                throw t;
            } finally {
                keyIterator.close();
            }
        });

        keyIterator.setThread(scanThread);

        scanThread.start();

        return keyIterator;
    }

}
