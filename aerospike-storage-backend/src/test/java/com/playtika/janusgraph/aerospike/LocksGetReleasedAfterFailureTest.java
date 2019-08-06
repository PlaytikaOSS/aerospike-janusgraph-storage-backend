package com.playtika.janusgraph.aerospike;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphQuery;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

import java.util.Iterator;

import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.*;
import static com.playtika.janusgraph.aerospike.ConfigOptions.WAL_STALE_TRANSACTION_LIFETIME_THRESHOLD;
import static com.playtika.janusgraph.aerospike.FlakingAerospikeStoreManager.*;
import static org.apache.tinkerpop.gremlin.structure.Direction.IN;
import static org.apache.tinkerpop.gremlin.structure.Direction.OUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;

//Checks corner case when transaction failed on releasing lock phase. WriteAheadLogCompleter should release locks
public class LocksGetReleasedAfterFailureTest {

    private static Logger logger = LoggerFactory.getLogger(LocksGetReleasedAfterFailureTest.class);

    @ClassRule
    public static GenericContainer container = getAerospikeContainer();

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
    public void shouldBecameConsistentAfterLocksNotReleased() throws InterruptedException {
        for(int i = 0; i < 20; i++) {
            deleteAllRecords(container);

            fixAll();

            JanusGraph graph = openGraph();
            defineSchema(graph);

            failsUnlock.set(true);
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
                fixAll();
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

}
