package com.playtika.janusgraph.utility;

import com.google.common.base.Preconditions;
import org.janusgraph.core.JanusGraphRelation;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.vertices.CacheVertex;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import static org.janusgraph.diskstorage.Backend.EDGESTORE_NAME;
import static org.janusgraph.graphdb.olap.VertexJobConverter.startTransaction;

public class GhostVertexRemover {

    private static final SliceQuery EVERYTHING_QUERY = new SliceQuery(BufferUtil.zeroBuffer(1),BufferUtil.oneBuffer(4));
    private static final int RELATION_COUNT_LIMIT = 20000;
    private static final SliceQuery EVERYTHING_QUERY_LIMIT = EVERYTHING_QUERY.updateLimit(RELATION_COUNT_LIMIT);

    private final IDManager idManager;
    private final KeyColumnValueStore edgeStore;
    private final StandardJanusGraphTx tx;

    public GhostVertexRemover(StandardJanusGraph standardJanusGraph) throws BackendException {
        idManager = standardJanusGraph.getIDManager();
        KeyColumnValueStoreManager storeManager = (KeyColumnValueStoreManager)standardJanusGraph.getBackend().getStoreManager();
        edgeStore = storeManager.openDatabase(EDGESTORE_NAME);
        tx = startTransaction(standardJanusGraph);
    }

    public void removeGhostVertex(long vertexId) throws BackendException {
        StaticBuffer key = idManager.getKey(vertexId);
        EntryList entryList = edgeStore.getSlice(new KeySliceQuery(key, EVERYTHING_QUERY), new FakeStoreTransaction());
        Map<SliceQuery, EntryList> entries = Collections.singletonMap(EVERYTHING_QUERY, entryList);

        assert entries.get(EVERYTHING_QUERY_LIMIT)!=null;
        final EntryList everything = entries.get(EVERYTHING_QUERY_LIMIT);

        JanusGraphVertex vertex = tx.getInternalVertex(vertexId);
        Preconditions.checkArgument(vertex instanceof CacheVertex,
                "The bounding transaction is not configured correctly");
        CacheVertex v = (CacheVertex)vertex;
        v.loadRelations(EVERYTHING_QUERY, input -> everything);

        int removedRelations = 0;
        Iterator<JanusGraphRelation> iterator = v.query().noPartitionRestriction().relations().iterator();
        while (iterator.hasNext()) {
            iterator.next();
            iterator.remove();
            removedRelations++;
        }
        v.remove();

        tx.commit();

    }

    private static class FakeStoreTransaction implements StoreTransaction {
        @Override
        public void commit() {
        }

        @Override
        public void rollback() {
        }

        @Override
        public BaseTransactionConfig getConfiguration() {
            return null;
        }
    }
}
