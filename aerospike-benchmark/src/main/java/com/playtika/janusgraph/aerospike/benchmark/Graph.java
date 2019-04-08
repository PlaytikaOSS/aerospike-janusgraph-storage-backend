package com.playtika.janusgraph.aerospike.benchmark;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.*;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;

import java.util.UUID;

public class Graph {

    public static final String CREDENTIAL_TYPE = "credentialType";
    public static final String CREDENTIAL_VALUE = "credentialValue";
    public static final String PLATFORM_IDENTITY_PARENT_EVENT = "pltfParentEvent";
    public static final String APP_IDENTITY_PARENT_EVENT = "appParentEvent";
    public static final String APP_IDENTITY_ID = "appIdentityId";


    static void buildGraph(JanusGraph graph, UUID id) {
        JanusGraphTransaction tx = graph.newTransaction();
        JanusGraphVertex prentEvent = tx.addVertex();
        tx.addVertex(CREDENTIAL_TYPE, "fb_"+id, CREDENTIAL_VALUE, "qd3qeqda3123dwq_"+id)
                .addEdge(PLATFORM_IDENTITY_PARENT_EVENT, prentEvent);
        tx.addVertex(CREDENTIAL_TYPE, "one_"+id, CREDENTIAL_VALUE, "dwdw@cwd.com_"+id)
                .addEdge(PLATFORM_IDENTITY_PARENT_EVENT, prentEvent);
        tx.addVertex(APP_IDENTITY_ID, "123:456_"+id).addEdge(APP_IDENTITY_PARENT_EVENT, prentEvent);
        tx.commit();
    }

    static void defineSchema(JanusGraph graph) {
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

}
