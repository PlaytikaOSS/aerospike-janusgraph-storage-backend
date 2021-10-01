package com.playtika.janusgraph.trace;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphManagerUtility;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryMetaData;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.ReadConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.cache.KCVSCache;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.builder.GraphDatabaseConfigurationBuilder;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.management.JanusGraphManager;
import org.janusgraph.graphdb.relations.EdgeDirection;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.CompositeIndexType;
import org.janusgraph.graphdb.types.system.BaseRelationType;
import org.janusgraph.graphdb.types.vertices.JanusGraphSchemaVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.GRAPH_NAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.ROOT_NS;
import static org.janusgraph.graphdb.management.JanusGraphManager.JANUS_GRAPH_MANAGER_EXPECTED_STATE_MSG;

public class DebugJanusGraph extends StandardJanusGraph {

    private static final Logger log =
            LoggerFactory.getLogger(DebugJanusGraph.class);

    private static final Predicate<InternalRelation> SCHEMA_FILTER =
            internalRelation -> internalRelation.getType() instanceof BaseRelationType && internalRelation.getVertex(0) instanceof JanusGraphSchemaVertex;

    private static final Predicate<InternalRelation> NO_SCHEMA_FILTER = internalRelation -> !SCHEMA_FILTER.apply(internalRelation);

    public DebugJanusGraph(GraphDatabaseConfiguration configuration) {
        super(configuration);
    }

    @Override
    public void commit(final Collection<InternalRelation> addedRelations,
                       final Collection<InternalRelation> deletedRelations, final StandardJanusGraphTx tx) {

        traceCommit(addedRelations, deletedRelations, NO_SCHEMA_FILTER, tx);

        super.commit(addedRelations, deletedRelations, tx);
    }

    private void traceCommit(final Collection<InternalRelation> addedRelations,
                             final Collection<InternalRelation> deletedRelations,
                             final Predicate<InternalRelation> filter,
                             final StandardJanusGraphTx tx) {

        final boolean acquireLocks = tx.getConfiguration().hasAcquireLocks();

        ListMultimap<Long, InternalRelation> mutations = ArrayListMultimap.create();
        ListMultimap<InternalVertex, InternalRelation> mutatedProperties = ArrayListMultimap.create();
        List<IndexSerializer.IndexUpdate> indexUpdates = Lists.newArrayList();
        //1) Collect deleted edges and their index updates and acquire edge locks
        for (InternalRelation del : Iterables.filter(deletedRelations,filter)) {
            Preconditions.checkArgument(del.isRemoved());
            for (int pos = 0; pos < del.getLen(); pos++) {
                InternalVertex vertex = del.getVertex(pos);
                if (pos == 0 || !del.isLoop()) {
                    if (del.isProperty()) mutatedProperties.put(vertex,del);
                    mutations.put(vertex.longId(), del);
                }
                if (acquireLock(del,pos, acquireLocks)) {
                    Entry entry = edgeSerializer.writeRelation(del, pos, tx);
                    StaticBuffer lockKey = getIDManager().getKey(vertex.longId());

                    traceDeletedEdgeAcquireLock(del, pos, vertex, entry, lockKey);
                }
            }
            Collection<IndexSerializer.IndexUpdate> delIndexUpdates = indexSerializer.getIndexUpdates(del);

            traceDeletedEdgeToIndexUpdates(del, delIndexUpdates);

            indexUpdates.addAll(delIndexUpdates);
        }

        //2) Collect added edges and their index updates and acquire edge locks
        for (InternalRelation add : Iterables.filter(addedRelations,filter)) {
            Preconditions.checkArgument(add.isNew());

            for (int pos = 0; pos < add.getLen(); pos++) {
                InternalVertex vertex = add.getVertex(pos);
                if (pos == 0 || !add.isLoop()) {
                    if (add.isProperty()) mutatedProperties.put(vertex,add);
                    mutations.put(vertex.longId(), add);
                }
                if (!vertex.isNew() && acquireLock(add,pos,acquireLocks)) {
                    Entry entry = edgeSerializer.writeRelation(add, pos, tx);
                    StaticBuffer lockKey = getIDManager().getKey(vertex.longId());

                    traceAddedEdgeAcquireLock(add, pos, vertex, entry, lockKey);
                }
            }
            Collection<IndexSerializer.IndexUpdate> addIndexUpdates = indexSerializer.getIndexUpdates(add);

            traceAddedEdgeToIndexUpdates(add, addIndexUpdates);

            indexUpdates.addAll(addIndexUpdates);
        }

        //3) Collect all index update for vertices
        for (InternalVertex v : mutatedProperties.keySet()) {
            List<InternalRelation> updatedProperties = mutatedProperties.get(v);

            traceUpdatedProperties(updatedProperties);

            Collection<IndexSerializer.IndexUpdate> propertiesIndexUpdates = indexSerializer.getIndexUpdates(v, updatedProperties);

            traceUpdatedPropertiesIndexUpdates(propertiesIndexUpdates);

            indexUpdates.addAll(propertiesIndexUpdates);
        }
        //4) Acquire index locks (deletions first)
        for (IndexSerializer.IndexUpdate update : indexUpdates) {
            if (!update.isCompositeIndex() || !update.isDeletion()) continue;
            CompositeIndexType iIndex = (CompositeIndexType) update.getIndex();
            if (acquireLock(iIndex,acquireLocks)) {

                traceDeletedIndexAcquireLock(update);
            }
        }
        for (IndexSerializer.IndexUpdate update : indexUpdates) {
            if (!update.isCompositeIndex() || !update.isAddition()) continue;
            CompositeIndexType iIndex = (CompositeIndexType) update.getIndex();
            if (acquireLock(iIndex,acquireLocks)) {

                traceAddedIndexAcquireLock(update);
            }
        }

        //5) Add relation mutations
        for (Long vertexId : mutations.keySet()) {
            Preconditions.checkArgument(vertexId > 0, "Vertex has no id: %s", vertexId);
            final List<InternalRelation> edges = mutations.get(vertexId);
            final List<Entry> additions = new ArrayList<>(edges.size());
            final List<Entry> deletions = new ArrayList<>(Math.max(10, edges.size() / 10));
            for (final InternalRelation edge : edges) {
                final InternalRelationType baseType = (InternalRelationType) edge.getType();
                assert baseType.getBaseType()==null;

                for (InternalRelationType type : baseType.getRelationIndexes()) {
                    if (type.getStatus()== SchemaStatus.DISABLED) continue;
                    for (int pos = 0; pos < edge.getArity(); pos++) {
                        if (!type.isUnidirected(Direction.BOTH) && !type.isUnidirected(EdgeDirection.fromPosition(pos)))
                            continue; //Directionality is not covered
                        if (edge.getVertex(pos).longId()==vertexId) {
                            StaticArrayEntry entry = edgeSerializer.writeRelation(edge, type, pos, tx);
                            if (edge.isRemoved()) {
                                deletions.add(entry);
                            } else {
                                Preconditions.checkArgument(edge.isNew());
                                int ttl = getTTL(edge);
                                if (ttl > 0) {
                                    entry.setMetaData(EntryMetaData.TTL, ttl);
                                }
                                additions.add(entry);
                            }
                        }
                    }
                }
            }

            StaticBuffer vertexKey = getIDManager().getKey(vertexId);
            traceMutateEdges(vertexId, additions, deletions, vertexKey);
        }

        //6) Add index updates
        for (IndexSerializer.IndexUpdate indexUpdate : indexUpdates) {
            assert indexUpdate.isAddition() || indexUpdate.isDeletion();
            if (indexUpdate.isCompositeIndex()) {
                final IndexSerializer.IndexUpdate<StaticBuffer,Entry> update = indexUpdate;
                if (update.isAddition()) {

                    traceAddedIndex(update);
                } else {

                    traceRemovedIndex(update);
                }
            }
        }
    }

    private void traceRemovedIndex(IndexSerializer.IndexUpdate<StaticBuffer, Entry> update) {
        log.trace("Will mutateIndex removal key: {}, additions: {}, deletions: {}",
                update.getKey(), KeyColumnValueStore.NO_ADDITIONS, Lists.newArrayList(update.getEntry()));
    }

    private void traceAddedIndex(IndexSerializer.IndexUpdate<StaticBuffer, Entry> update) {
        log.trace("Will mutateIndex addition key: {}, additions: {}, deletions: {}",
                update.getKey(), Lists.newArrayList(update.getEntry()), KCVSCache.NO_DELETIONS);
    }

    private void traceMutateEdges(Long vertexId, List<Entry> additions, List<Entry> deletions, StaticBuffer vertexKey) {
        log.trace("Will mutateEdges vertexId:{}, vertexKey: {}, additions: {}, deletions: {}",
                vertexId, vertexKey, additions, deletions);
    }

    private void traceAddedIndexAcquireLock(IndexSerializer.IndexUpdate update) {
        log.trace("Will acquireIndexLock added key: {}, column: {}, index: {}",
                update.getKey(), ((Entry) update.getEntry()).getColumn(), update.getIndex());
    }

    private void traceDeletedIndexAcquireLock(IndexSerializer.IndexUpdate update) {
        log.trace("Will acquireIndexLock deleted key: {}, entry: {}, element: {}, index: {}",
                update.getKey(), update.getEntry(), update.getElement(), update.getIndex());
    }

    private void traceUpdatedPropertiesIndexUpdates(Collection<IndexSerializer.IndexUpdate> propertiesIndexUpdates) {
        propertiesIndexUpdates.forEach(indexUpdate -> log.trace("Properties updated index update " +
                        "key: {}, entry: {}, element: {}, index: {}",
                indexUpdate.getKey(), indexUpdate.getEntry(), indexUpdate.getElement(), indexUpdate.getIndex()));
    }

    private void traceUpdatedProperties(List<InternalRelation> updatedProperties) {
        log.trace("Properties updated index relations: {}", updatedProperties);
    }

    private void traceAddedEdgeToIndexUpdates(InternalRelation add, Collection<IndexSerializer.IndexUpdate> addIndexUpdates) {
        addIndexUpdates.forEach(indexUpdate -> log.trace("Added edge index update relation: {} -> " +
                        "key: {}, entry: {}, element: {}, index: {}",
                add, indexUpdate.getKey(), indexUpdate.getEntry(), indexUpdate.getElement(), indexUpdate.getIndex()));
    }

    private void traceAddedEdgeAcquireLock(InternalRelation add, int pos, InternalVertex vertex, Entry entry, StaticBuffer lockKey) {
        log.trace("Will acquireEdgeLock added edge: {}, pos: {}, vertex id: {}, lock key: {}, expected value: {}",
                add, pos, vertex.longId(), lockKey, entry);
    }

    private void traceDeletedEdgeToIndexUpdates(InternalRelation del, Collection<IndexSerializer.IndexUpdate> delIndexUpdates) {
        delIndexUpdates.forEach(indexUpdate -> log.trace("Removed edge index update relation: {} -> " +
                        "key: {}, entry: {}, element: {}, index: {}",
                del, indexUpdate.getKey(), indexUpdate.getEntry(), indexUpdate.getElement(), indexUpdate.getIndex()));
    }

    private void traceDeletedEdgeAcquireLock(InternalRelation del, int pos, InternalVertex vertex, Entry entry, StaticBuffer lockKey) {
        log.trace("Will acquireEdgeLock deleted relation: {}, pos: {}, vertex id: {}, lock key: {}, expected value: {}",
                del, pos, vertex.longId(), lockKey, entry);
    }

    /**
     * Opens a {@link JanusGraph} database configured according to the provided configuration.
     * This method shouldn't be called by end users; it is used by internal server processes to
     * open graphs defined at server start that do not include the graphname property.
     *
     * @param configuration Configuration for the graph database
     * @return JanusGraph graph database
     */
    public static JanusGraph open(ReadConfiguration configuration) {
        final ModifiableConfiguration config = new ModifiableConfiguration(ROOT_NS, (WriteConfiguration) configuration, BasicConfiguration.Restriction.NONE);
        final String graphName = config.has(GRAPH_NAME) ? config.get(GRAPH_NAME) : null;
        final JanusGraphManager jgm = JanusGraphManagerUtility.getInstance();
        if (null != graphName) {
            Preconditions.checkNotNull(jgm, JANUS_GRAPH_MANAGER_EXPECTED_STATE_MSG);
            return (JanusGraph) jgm.openGraph(graphName, gName -> new DebugJanusGraph(
                    new GraphDatabaseConfigurationBuilder().build(configuration)));
        } else {
            if (jgm != null) {
                log.warn("You should supply \"graph.graphname\" in your .properties file configuration if you are opening " +
                        "a graph that has not already been opened at server start, i.e. it was " +
                        "defined in your YAML file. This will ensure the graph is tracked by the JanusGraphManager, " +
                        "which will enable autocommit and rollback functionality upon all gremlin script executions. " +
                        "Note that JanusGraphFactory#open(String === shortcut notation) does not support consuming the property " +
                        "\"graph.graphname\" so these graphs should be accessed dynamically by supplying a .properties file here " +
                        "or by using the ConfiguredGraphFactory.");
            }
            return new DebugJanusGraph(new GraphDatabaseConfigurationBuilder().build(configuration));
        }
    }
}
