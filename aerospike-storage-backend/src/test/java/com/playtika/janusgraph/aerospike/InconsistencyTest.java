package com.playtika.janusgraph.aerospike;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeConfiguration;
import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeContainer;
import static org.assertj.core.api.Assertions.assertThat;

public class InconsistencyTest {

    public static final String SOURCE = "source";
    public static final String TARGET = "target";
    public static final String RELATION = "relation";
    @Rule
    public GenericContainer container = getAerospikeContainer();

    JanusGraph graph;
    GraphTraversalSource traversal;
    Random r = new Random();

    @Before
    public void buildGraph() {
        graph = JanusGraphFactory.open(getAerospikeConfiguration(container));
        traversal = graph.traversal();
    }

    @After
    public void tearDownGraph() {
        graph.close();
    }

    @Test
    public void shouldProduceGhostVertexViaHangedEdge() throws InterruptedException {

        JanusGraphManagement management = graph.openManagement();

        management.makeVertexLabel(SOURCE).make();
        management.makeVertexLabel(TARGET).make();

        EdgeLabel relationEdge = management.makeEdgeLabel(RELATION).multiplicity(Multiplicity.SIMPLE).make();
        management.setConsistency(relationEdge, ConsistencyModifier.LOCK);

        management.commit();

        Transaction tx = traversal.tx();
        tx.open();

        Object targetVertexId = traversal.addV(TARGET).next().id();
        Object targetVertexId2 = traversal.addV(TARGET).next().id();
        Object sourceVertexId = traversal.addV(SOURCE).next().id();
        Object sourceVertexId2 = traversal.addV(SOURCE).next().id();
        tx.commit();

        CompletableFuture relAddedButNotCommitted = new CompletableFuture();
        CompletableFuture targetRemovedAndCommitted = new CompletableFuture();

        //drop target vertex
        Thread dropThread = new Thread(() -> {
            try {
                relAddedButNotCommitted.get();

                Transaction txInner = traversal.tx();
                txInner.open();

                traversal.V(targetVertexId).drop().tryNext();

                txInner.commit();

                targetRemovedAndCommitted.complete(new Object());

            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        //and build relation to it in parallel
        Thread relationThread = new Thread(() -> {
            try {
                Transaction txInner = traversal.tx();
                txInner.open();
                traversal.addE(RELATION).from(traversal.V(sourceVertexId))
                        .to(traversal.V(targetVertexId)).next();

                relAddedButNotCommitted.complete(new Object());

                targetRemovedAndCommitted.get();
                txInner.commit();

            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        dropThread.start();
        relationThread.start();

        dropThread.join();
        relationThread.join();

        //check ghost vertex
        tx = traversal.tx();
        tx.open();

        Optional<Vertex> targetVertex = traversal.V(targetVertexId).tryNext();
        assertThat(targetVertex.isPresent()).isFalse();

        Optional<Vertex> targetVertexViaEdge = traversal.V(sourceVertexId).outE().inV().tryNext();

        assertThat(targetVertexViaEdge.isPresent()).isTrue();

        assertThat(targetVertexViaEdge.get().id()).isEqualTo(targetVertexId);
        assertThat(targetVertexViaEdge.get().label()).isEqualTo("vertex");

        tx.commit();

    }
}
