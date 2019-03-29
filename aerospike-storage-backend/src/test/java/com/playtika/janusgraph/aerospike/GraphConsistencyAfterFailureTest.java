package com.playtika.janusgraph.aerospike;

import com.aerospike.AerospikeContainer;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.*;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.*;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.*;
import static com.playtika.janusgraph.aerospike.ConfigOptions.WAL_STALE_TRANSACTION_LIFETIME_THRESHOLD;
import static org.apache.tinkerpop.gremlin.structure.Direction.IN;
import static org.apache.tinkerpop.gremlin.structure.Direction.OUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;

public class GraphConsistencyAfterFailureTest {

    private static Logger logger = LoggerFactory.getLogger(GraphConsistencyAfterFailureTest.class);

    @ClassRule
    public static AerospikeContainer container = getAerospikeContainer();

    public static final String CREDENTIAL_TYPE = "credentialType";
    public static final String CREDENTIAL_VALUE = "credentialValue";
    public static final Object[] CRED_2 = {CREDENTIAL_TYPE, "one", CREDENTIAL_VALUE, "dwdw@cwd.com"};
    public static final Object[] CRED_1 = {CREDENTIAL_TYPE, "fb", CREDENTIAL_VALUE, "qd3qeqda3123dwq"};
    public static final String PLATFORM_IDENTITY_PARENT_EVENT = "pltfParentEvent";
    public static final String APP_IDENTITY_PARENT_EVENT = "appParentEvent";
    public static final String APP_IDENTITY_ID = "appIdentityId";
    public static final Object[] APP = {APP_IDENTITY_ID, "123:456"};
    public static final long STALE_TRANSACTION_THRESHOLD = 1000L;

    @Test
    public void shouldBecameConsistentAfterFailure() throws InterruptedException, BackendException {
        for(int i = 0; i < 20; i++) {
            deleteAllRecords(container, container.getNamespace());

            JanusGraph graph = openGraph();

            fails.set(false);
            defineSchema(graph);

            fails.set(true);
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
                fails.set(false);
                time.set(STALE_TRANSACTION_THRESHOLD + 1);
                //wait for WriteAheadLogCompleter had fixed graph
                Thread.sleep(STALE_TRANSACTION_THRESHOLD * 2);

                //check graph. It should be fixed at this time
                checkAndCleanGraph(graph);

                //check that graph had been correctly cleaned
                buildGraph(graph);
            }

            graph.close();
        }

    }

    protected JanusGraph openGraph() {
        ModifiableConfiguration config = getAerospikeConfiguration(container);
        config.set(STORAGE_BACKEND, FlakingAerospikeStoreManager.class.getName());
        config.set(WAL_STALE_TRANSACTION_LIFETIME_THRESHOLD, STALE_TRANSACTION_THRESHOLD);

        return JanusGraphFactory.open(config);
    }

    protected void checkAndCleanGraph(JanusGraph graph) {
        JanusGraphTransaction tx = graph.newTransaction();
        Vertex cred1 = queryVertices(tx, CRED_1).next();
        Vertex cred2 = queryVertices(tx, CRED_2).next();
        Vertex app = queryVertices(tx, APP).next();
        Vertex event = app.edges(OUT, APP_IDENTITY_PARENT_EVENT).next().inVertex();
        assertThat(cred1.edges(OUT, PLATFORM_IDENTITY_PARENT_EVENT).next().inVertex())
                .isEqualTo(event);
        assertThat(cred2.edges(OUT, PLATFORM_IDENTITY_PARENT_EVENT).next().inVertex())
                .isEqualTo(event);
        assertThat(event.edges(IN, PLATFORM_IDENTITY_PARENT_EVENT)).hasSize(2);
        cred1.remove();
        cred2.remove();
        app.remove();
        tx.commit();
    }

    private void buildGraph(JanusGraph graph) {
        JanusGraphTransaction tx = graph.newTransaction();
        JanusGraphVertex prentEvent = tx.addVertex();
        tx.addVertex(CRED_1).addEdge(PLATFORM_IDENTITY_PARENT_EVENT, prentEvent);
        tx.addVertex(CRED_2).addEdge(PLATFORM_IDENTITY_PARENT_EVENT, prentEvent);
        tx.addVertex(APP).addEdge(APP_IDENTITY_PARENT_EVENT, prentEvent);
        tx.commit();
    }

    private void defineSchema(JanusGraph graph) {
        JanusGraphManagement management = graph.openManagement();
        final PropertyKey credentialType = management.makePropertyKey(CREDENTIAL_TYPE).dataType(String.class).make();
        final PropertyKey credentialValue = management.makePropertyKey(CREDENTIAL_VALUE).dataType(String.class).make();
        final PropertyKey appIdentityId = management.makePropertyKey(APP_IDENTITY_ID).dataType(String.class).make();

        JanusGraphIndex platformIdentityIndex = management.buildIndex("platformIdentity", Vertex.class)
                .addKey(credentialType).addKey(credentialValue)
                .unique()
                .buildCompositeIndex();
        management.setConsistency(platformIdentityIndex, ConsistencyModifier.LOCK);

        JanusGraphIndex appIdentityIndex = management.buildIndex("appIdentity", Vertex.class)
                .addKey(appIdentityId)
                .unique()
                .buildCompositeIndex();
        management.setConsistency(appIdentityIndex, ConsistencyModifier.LOCK);

        management.makeEdgeLabel(PLATFORM_IDENTITY_PARENT_EVENT).multiplicity(Multiplicity.MANY2ONE).make();
        management.makeEdgeLabel(APP_IDENTITY_PARENT_EVENT).multiplicity(Multiplicity.ONE2ONE).make();
        management.commit();
    }

    private Iterator<JanusGraphVertex> queryVertices(JanusGraphTransaction tx, Object... objects) {
        JanusGraphQuery<? extends JanusGraphQuery> query = tx.query();
        for(int i = 0; i < objects.length; i = i + 2){
            query = query.has((String)objects[i], objects[i + 1]);
        }
        return query.vertices().iterator();
    }

    private static final AtomicBoolean fails = new AtomicBoolean(false);
    private static final Random random = new Random();
    private static final AtomicLong time = new AtomicLong(0);

    public static class FlakingAerospikeStoreManager extends AerospikeStoreManager {

        public FlakingAerospikeStoreManager(Configuration configuration) {
            super(configuration);
        }

        @Override
        protected Clock getClock(){
            return new FixedClock(time);
        }

        @Override
        public AKeyColumnValueStore openDatabase(String name) {
            AKeyColumnValueStore client = super.openDatabase(name);

            return new FlakingKCVStore(client);
        }

        @Override
        void releaseLocks(Set<Key> keysLocked){
            if(!fails.get()){
                super.releaseLocks(keysLocked);
            }
        }

        @Override
        void deleteWalTransaction(Value transactionId) {
            if(!fails.get()){
                super.deleteWalTransaction(transactionId);
            }
        }
    }

    private static class FlakingKCVStore implements AKeyColumnValueStore {

        private final AKeyColumnValueStore keyColumnValueStore;

        private FlakingKCVStore(AKeyColumnValueStore keyColumnValueStore) {
            this.keyColumnValueStore = keyColumnValueStore;
        }

        @Override
        public void mutate(Value key, Map<Value, Value> mutation){
            if(fails.get() && random.nextBoolean()){
                logger.error("Failed flaking");
                throw new RuntimeException();
            }
            keyColumnValueStore.mutate(key, mutation);
        }

        @Override
        public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws BackendException {
            keyColumnValueStore.mutate(key, additions, deletions, txh);
        }


        @Override
        public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws BackendException {
            return keyColumnValueStore.getSlice(query, txh);
        }

        @Override
        public Map<StaticBuffer, EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws BackendException {
            return keyColumnValueStore.getSlice(keys, query, txh);
        }

        @Override
        public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction txh) throws BackendException {
            keyColumnValueStore.acquireLock(key, column, expectedValue, txh);
        }

        @Override
        public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh) throws BackendException {
            return keyColumnValueStore.getKeys(query, txh);
        }

        @Override
        public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws BackendException {
            return keyColumnValueStore.getKeys(query, txh);
        }

        @Override
        public String getName() {
            return keyColumnValueStore.getName();
        }

        @Override
        public void close() throws BackendException {
            keyColumnValueStore.close();
        }
    }

    private static class FixedClock extends Clock{
        private final AtomicLong time;

        private FixedClock(AtomicLong time) {
            this.time = time;
        }


        @Override
        public ZoneId getZone() {
            return null;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return null;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(time.get());
        }
    }

}
