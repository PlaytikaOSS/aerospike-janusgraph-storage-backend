package com.playtika.janusgraph.aerospike;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Value;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.playtika.janusgraph.aerospike.operations.AerospikeOperations;
import com.playtika.janusgraph.aerospike.operations.BasicOperations;
import com.playtika.janusgraph.aerospike.operations.ErrorMapper;
import com.playtika.janusgraph.aerospike.operations.Operations;
import com.playtika.janusgraph.aerospike.operations.batch.BatchLocks;
import com.playtika.janusgraph.aerospike.operations.batch.BatchUpdate;
import com.playtika.janusgraph.aerospike.operations.batch.BatchUpdates;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.StoreMetaData;
import org.janusgraph.diskstorage.common.AbstractStoreManager;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.playtika.janusgraph.aerospike.AerospikeKeyColumnValueStore.mutationToMap;
import static com.playtika.janusgraph.aerospike.ConfigOptions.CHECK_ALL_MUTATIONS_LOCKED;
import static com.playtika.janusgraph.aerospike.ConfigOptions.START_WAL_COMPLETER;
import static com.playtika.janusgraph.aerospike.operations.AerospikeOperations.getValue;
import static com.playtika.janusgraph.aerospike.operations.batch.BatchUpdates.EDGE_STORE_NAME;
import static com.playtika.janusgraph.aerospike.operations.batch.BatchUpdates.INDEX_STORE_NAME;
import static com.playtika.janusgraph.aerospike.operations.batch.BatchUpdates.REGULAR_STORE_NAMES;
import static com.playtika.janusgraph.aerospike.util.AerospikeUtils.isEmptySet;
import static com.playtika.janusgraph.aerospike.util.AerospikeUtils.truncateSet;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static org.janusgraph.diskstorage.Backend.SYSTEM_MGMT_LOG_NAME;
import static org.janusgraph.diskstorage.Backend.SYSTEM_TX_LOG_NAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.BUFFER_SIZE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.SYSTEM_PROPERTIES_STORE_NAME;


@PreInitializeConfigOptions
public class AerospikeStoreManager extends AbstractStoreManager implements KeyColumnValueStoreManager {

    private static final Logger logger = LoggerFactory.getLogger(AerospikeStoreManager.class);

    public static final int AEROSPIKE_BUFFER_SIZE = Integer.MAX_VALUE / 2;

    private final StoreFeatures features;

    private final Operations operations;
    private final boolean checkAllMutationsLocked;

    private final Set<String> allStoreNames = new HashSet<>(asList(
            INDEX_STORE_NAME, EDGE_STORE_NAME,
            SYSTEM_PROPERTIES_STORE_NAME, SYSTEM_TX_LOG_NAME, SYSTEM_MGMT_LOG_NAME));

    public AerospikeStoreManager(Configuration configuration) {
        super(configuration);

        Preconditions.checkArgument(configuration.get(BUFFER_SIZE) == AEROSPIKE_BUFFER_SIZE,
                "Set unlimited buffer size as we use deferred locking approach");

        features = features(configuration);

        operations = initOperations(configuration);

        if(configuration.get(START_WAL_COMPLETER)) {
            operations.getWriteAheadLogCompleter().start();
        }

        this.checkAllMutationsLocked = configuration.get(CHECK_ALL_MUTATIONS_LOCKED);

        this.allStoreNames.add(operations.getAerospikeOperations().getIdsStoreName());
    }

    protected BasicOperations initOperations(Configuration configuration) {
        return new BasicOperations(configuration);
    }

    private StandardStoreFeatures features(Configuration configuration) {
        return new StandardStoreFeatures.Builder()
                .keyConsistent(configuration)
                .persists(true)
                //here we promise to take care of locking.
                //If false janusgraph will do it via ExpectedValueCheckingStoreManager that is less effective
                .locking(true)
                //caused by deferred locking approach used in this storage backend,
                //actual locking happens just before transaction commit
                .optimisticLocking(true)
                .transactional(false)
                .distributed(true)
                .multiQuery(true)
                .batchMutation(true)
                .unorderedScan(true)
                .orderedScan(false)
                .keyOrdered(false)
                .localKeyPartition(false)
                .timestamps(false)
                .supportsInterruption(false)
                .build();
    }

    @Override
    public StoreTransaction beginTransaction(final BaseTransactionConfig config) {
        AerospikeTransaction txh = new AerospikeTransaction(config);
        logger.trace("beginTransaction(tx:{})", txh);
        return txh;
    }

    @Override
    public KeyColumnValueStore openDatabase(String name) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name), "Database name may not be null or empty");

        if(!allStoreNames.contains(name)){
            throw new IllegalArgumentException();
        }

        return new AerospikeKeyColumnValueStore(name,
                operations.getReadOperations(),
                operations.getAerospikeOperations(),
                operations.batchUpdater(),
                operations.getMutateOperations(),
                operations.getScanOperations());
    }

    @Override
    public KeyColumnValueStore openDatabase(String name, StoreMetaData.Container metaData) {
        return openDatabase(name);
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException{
        logger.trace("mutateMany(tx:{}, {})", txh, mutations);

        AerospikeTransaction transaction = (AerospikeTransaction) txh;
        Map<String, Map<Value, Map<Value, Value>>> locksByStore = transaction.getLocksByStoreKeyColumn();

        Map<String, Map<Value, Map<Value, Value>>> mutationsByStore = groupMutationsByStoreKeyColumn(mutations);

        if(!allStoreNames.containsAll(mutationsByStore.keySet())){
            throw new IllegalArgumentException();
        }

        try {
            if(checkAllMutationsLocked){
                checkMutationsForLocks(locksByStore, mutationsByStore);
            }

            operations.batchUpdater().update(new BatchUpdate(
                    new BatchLocks(locksByStore, operations.getAerospikeOperations()),
                    new BatchUpdates(mutationsByStore)));
        } catch (Throwable t) {
            throw ErrorMapper.INSTANCE.apply(t);
        }
        transaction.close();
    }

    public static void checkMutationsForLocks(Map<String, Map<Value, Map<Value, Value>>> locksByStore, Map<String, Map<Value, Map<Value, Value>>> mutationsByStore) {

        if(REGULAR_STORE_NAMES.containsAll(mutationsByStore.keySet())) {
            for (Map.Entry<String, Map<Value, Map<Value, Value>>> storeMutations : mutationsByStore.entrySet()) {
                Map<Value, Map<Value, Value>> storeLocks = locksByStore.get(storeMutations.getKey());
                for (Map.Entry<Value, Map<Value, Value>> keyMutations : storeMutations.getValue().entrySet()) {
                    Map<Value, Value> keyLocks = storeLocks.getOrDefault(keyMutations.getKey(), emptyMap());
                    for (Value columnMutated : keyMutations.getValue().keySet()) {
                        if (!keyLocks.containsKey(columnMutated)) {
                            logger.warn("Mutating not locked store=[{}], key=[{}], column=[{}]",
                                    storeMutations.getKey(), keyMutations.getKey(), columnMutated);
                            throw new IllegalStateException("Mutating not locked");
                        }
                    }
                }
            }
        }
    }

    private static Map<String, Map<Value, Map<Value, Value>>> groupMutationsByStoreKeyColumn(
            Map<String, Map<StaticBuffer, KCVMutation>> mutationsByStore){
        Map<String, Map<Value, Map<Value, Value>>> mapByStore = new HashMap<>(mutationsByStore.size());
        for(Map.Entry<String, Map<StaticBuffer, KCVMutation>> storeMutations : mutationsByStore.entrySet()) {
            Map<Value, Map<Value, Value>> map = new HashMap<>(storeMutations.getValue().size());
            for (Map.Entry<StaticBuffer, KCVMutation> mutationEntry : storeMutations.getValue().entrySet()) {
                map.put(getValue(mutationEntry.getKey()), mutationToMap(mutationEntry.getValue()));
            }
            mapByStore.put(storeMutations.getKey(), map);
        }
        return mapByStore;
    }

    @Override
    public void close() {
        operations.close();
    }

    @Override
    public void clearStorage() throws BackendException {
        try {
            AerospikeOperations aerospikeOperations = operations.getAerospikeOperations();
            for(String storeName : allStoreNames){
                truncateSet(
                        aerospikeOperations.getClient(),
                        aerospikeOperations.getNamespace(storeName),
                        aerospikeOperations.getSetName(storeName));
            }

            operations.getWalOperations().clear();

        } catch (AerospikeException e) {
            throw new PermanentBackendException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean exists() throws BackendException {
        try {
            AerospikeOperations aerospikeOperations = operations.getAerospikeOperations();
            return !isEmptySet(aerospikeOperations.getClient(),
                    aerospikeOperations.getNamespace(EDGE_STORE_NAME),
                    aerospikeOperations.getSetName(EDGE_STORE_NAME));
        } catch (AerospikeException e) {
            throw new PermanentBackendException(e);
        }
    }

    @Override
    public String getName() {
        return getClass().getSimpleName() + ":" + "HARDCODED";
    }

    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() {
        throw new UnsupportedOperationException();
    }

    public Operations getOperations() {
        return operations;
    }

}
