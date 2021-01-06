package com.playtika.janusgraph.aerospike;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.example.GraphOfTheGodsFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeConfiguration;
import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeContainer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class GraphOfTheGodsTest {

    @Rule
    public GenericContainer container = getAerospikeContainer();

    JanusGraph graph;

    @Before
    public void buildGraph() {
        graph = JanusGraphFactory.open(getAerospikeConfiguration(container));

        GraphOfTheGodsFactory.loadWithoutMixedIndex(graph, true);
    }

    @After
    public void tearDownGraph() {
        graph.close();
    }

    @Test
    public void testQueryByName()  {
        final Iterator<Vertex> results = graph.traversal().V().has("name", "jupiter");
        assertTrue("Query should return a result", results.hasNext());
        final Vertex jupiter = results.next();
        assertNotNull("Query result should be non null", jupiter);

        jupiter.remove();
        graph.tx().commit();

        final Iterator<Vertex> resultsNew = graph.traversal().V().has("name", "jupiter");
        assertFalse("Query should not return a result", resultsNew.hasNext());
    }

    @Test
    public void testQueryAllVertices()  {
        assertEquals("Expected the correct number of VERTICES",
                12, graph.traversal().V().count().tryNext().get().longValue());
    }

    @Test
    public void testQueryAllEdges()  {
        assertEquals("Expected the correct number of EDGES",
                17, graph.traversal().E().count().tryNext().get().longValue());
    }

    @Test
    public void testRemoveAllVertices()  {
        List<Object> vertexIds = new ArrayList<>();
        graph.traversal().V().forEachRemaining(vertex -> vertexIds.add(vertex.id()));
        graph.traversal().V(vertexIds).drop().iterate();
        graph.tx().commit();
    }
}
