package com.playtika.janusgraph.aerospike;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.example.GraphOfTheGodsFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.*;

public class GraphOfTheGodsTest {

    JanusGraph graph;

    @Before
    public void buildGraph(){
        AerospikeTestUtils.deleteAllRecords("test");

//        graph = JanusGraphFactory.build().set("storage.backend", "inmemory").open();
        graph = JanusGraphFactory.build()
                .set("storage.backend", "com.playtika.janusgraph.aerospike.AerospikeStoreManager")
                .set("storage.allow_scan", "true")
                .open();
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
    public void testQueryAllVertices() throws Exception {
        assertEquals("Expected the correct number of VERTICES",
                12, graph.traversal().V().count().tryNext().get().longValue());
    }

    @Test
    public void testQueryAllEdges() throws Exception {
        assertEquals("Expected the correct number of EDGES",
                17, graph.traversal().E().count().tryNext().get().longValue());
    }
}
