package com.playtika.janusgraph.aerospike;

import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.playtika.janusgraph.aerospike.operations.AerospikeOperations;
import com.playtika.janusgraph.aerospike.operations.BasicLockOperations;
import com.playtika.janusgraph.aerospike.operations.BasicMutateOperations;
import com.playtika.janusgraph.aerospike.operations.BasicOperations;
import com.playtika.janusgraph.aerospike.operations.LockOperations;
import com.playtika.janusgraph.aerospike.operations.MutateOperations;
import com.playtika.janusgraph.aerospike.transaction.WriteAheadLogManager;
import com.playtika.janusgraph.aerospike.utils.FixedClock;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.AEROSPIKE_PROPERTIES;
import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.deleteAllRecords;
import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeConfiguration;
import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeContainer;
import static com.playtika.janusgraph.aerospike.GraphConsistencyAfterFailureTest.buildGraph;
import static com.playtika.janusgraph.aerospike.GraphConsistencyAfterFailureTest.defineSchema;
import static com.playtika.janusgraph.aerospike.TransactionRetentionOnFailureTest.FailingAerospikeStoreManager.failsCheckValue;
import static com.playtika.janusgraph.aerospike.TransactionRetentionOnFailureTest.FailingAerospikeStoreManager.failsMutate;
import static com.playtika.janusgraph.aerospike.TransactionRetentionOnFailureTest.FailingAerospikeStoreManager.fixAll;
import static com.playtika.janusgraph.aerospike.TransactionRetentionOnFailureTest.FailingAerospikeStoreManager.time;
import static org.assertj.core.api.Assertions.assertThat;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;

//Checks whether transaction
// - removed on failed lock
// - retains on failed mutation
public class TransactionRetentionOnFailureTest {

    private static Logger logger = LoggerFactory.getLogger(TransactionRetentionOnFailureTest.class);

    @ClassRule
    public static GenericContainer container = getAerospikeContainer();

    private static ModifiableConfiguration configuration = getAerospikeConfiguration(container)
            .set(STORAGE_BACKEND, FailingAerospikeStoreManager.class.getName())
            .set(ConfigOptions.WAL_NAMESPACE, AEROSPIKE_PROPERTIES.getNamespace())
            .set(ConfigOptions.WAL_STALE_TRANSACTION_LIFETIME_THRESHOLD, 1000L)
            .set(ConfigOptions.START_WAL_COMPLETER, false);

    private BasicOperations basicOperations = new BasicOperations(configuration);

    @Test
    public void shouldRemoveTransactionIfLockFailed() throws InterruptedException {
        for(int i = 0; i < 20; i++) {
            deleteAllRecords(container);

            fixAll();

            JanusGraph graph = openGraph();
            defineSchema(graph);

            failsCheckValue.set(true);
            time.set(0L);
            boolean failed = false;
            try {
                buildGraph(graph);
            } catch (Exception e) {
                //here we got inconsistent graph
                failed = true;
                logger.error("As expected failed on graph");
            }

            if(failed) {
                WriteAheadLogManager walManager = basicOperations.getTransactionalOperations().getWriteAheadLogManager();
                assertThat(walManager.getStaleTransactions()).isEmpty();
            }

            graph.close();
        }

    }

    @Test
    public void shouldNotRemoveTransactionIfMutationFailed() throws InterruptedException {
        for(int i = 0; i < 20; i++) {
            deleteAllRecords(container);

            fixAll();

            JanusGraph graph = openGraph();
            defineSchema(graph);

            failsMutate.set(true);
            time.set(0L);
            boolean failed = false;
            try {
                buildGraph(graph);
            } catch (Exception e) {
                //here we got inconsistent graph
                failed = true;
                logger.error("As expected failed on graph");
            }

            if(failed) {
                WriteAheadLogManager walManager = basicOperations.getTransactionalOperations().getWriteAheadLogManager();
                assertThat(walManager.getStaleTransactions()).isNotEmpty();
            }

            graph.close();
        }

    }

    static JanusGraph openGraph() {
        return JanusGraphFactory.open(configuration);
    }

    public static class FailingAerospikeStoreManager extends AerospikeStoreManager {

        public static final AtomicBoolean failsCheckValue = new AtomicBoolean(false);
        public static final AtomicBoolean failsMutate = new AtomicBoolean(false);

        public static final AtomicLong time = new AtomicLong();

        public FailingAerospikeStoreManager(Configuration configuration) {
            super(configuration);
        }

        protected BasicOperations initOperations(Configuration configuration) {
            return new FailingOperations(configuration);
        }

        public static void fixAll(){
            failsCheckValue.set(false);
            failsMutate.set(false);
        }

        private static class FailingOperations extends BasicOperations {

            public FailingOperations(Configuration configuration) {
                super(configuration);
            }

            @Override
            protected LockOperations buildLockOperations(AerospikeOperations aerospikeOperations) {
                return new BasicLockOperations(aerospikeOperations){
                    @Override
                    protected void checkExpectedValues(final Map<String, Map<Value, Map<Value, Value>>> locksByStore,
                                                       final Map<Key, LockType> keysLocked) throws PermanentBackendException {
                        if(failsCheckValue.get()){
                            throw new PermanentBackendException("failed");
                        } else {
                            super.checkExpectedValues(locksByStore, keysLocked);
                        }
                    }
                };
            }

            @Override
            protected MutateOperations buildMutateOperations(AerospikeOperations aerospikeOperations) {
                return new BasicMutateOperations(aerospikeOperations){
                    @Override
                    public void mutateMany(Map<String, Map<Value, Map<Value, Value>>> mutationsByStore, boolean wal) {
                        if(failsMutate.get()){
                            throw new RuntimeException();
                        } else {
                            super.mutateMany(mutationsByStore, wal);
                        }
                    }
                };
            }


            @Override
            protected Clock getClock(){
                return new FixedClock(time);
            }
        }
    }

}
