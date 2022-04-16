package com.playtika.janusgraph.aerospike.operations;

import com.aerospike.client.Value;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static com.playtika.janusgraph.aerospike.AerospikeKeyColumnValueStore.mutationToMap;
import static com.playtika.janusgraph.aerospike.operations.AerospikeOperations.getValue;
import static java.util.Collections.singletonList;

//TODO remove after this issue fixed https://github.com/JanusGraph/janusgraph/issues/3007
public class IdsCleanupOperations {

    private static final Logger logger = LoggerFactory.getLogger(IdsCleanupOperations.class);

    private static final StaticBuffer LOWER_SLICE = BufferUtil.zeroBuffer(1);
    private static final StaticBuffer UPPER_SLICE = BufferUtil.oneBuffer(17);

    private static final int PROTECTED_BLOCKS_AMOUNT = 10;


    private final String idsStoreName;
    private final long ttl;
    private final ReadOperations readOperations;
    private final MutateOperations mutateOperations;
    private final TimestampProvider timestampProvider;
    private final ExecutorService executorService;

    public IdsCleanupOperations(String idsStoreName,
                                ReadOperations readOperations, MutateOperations mutateOperations,
                                long ttl,
                                TimestampProvider timestampProvider,
                                ExecutorService executorService) {
        this.idsStoreName = idsStoreName;
        this.ttl = ttl;
        this.readOperations = readOperations;
        this.mutateOperations = mutateOperations;
        this.timestampProvider = timestampProvider;
        this.executorService = executorService;
    }

    public void cleanUpOldIdsRanges(StaticBuffer key) {
        executorService.submit(() -> {
            try {
                cleanUpOldIdsRangesImpl(key);
            } catch (BackendException e) {
                logger.error("Error while running cleanup of old ranges for key=[{}]", key, e);
            }
        });
    }

    public void cleanUpOldIdsRangesImpl(StaticBuffer key) throws BackendException {
        if(ttl == Long.MAX_VALUE){
            return;
        }

        Map<StaticBuffer, EntryList> allIdBlocksMap = readOperations.getSlice(idsStoreName, singletonList(key), new SliceQuery(LOWER_SLICE, UPPER_SLICE));
        EntryList blocks = allIdBlocksMap.get(key);

        blocks.sort(TIMESTAMP_COMPARATOR);
        List<Entry> blocksToCheck = blocks.subList(0, Math.max(blocks.size() - PROTECTED_BLOCKS_AMOUNT, 0));

        List<StaticBuffer> columnsToRemove = new ArrayList<>();
        for (Entry e : blocksToCheck) {
            ByteBuffer byteBuffer = e.asByteBuffer();
            long counterVal = byteBuffer.getLong();
            long idBlockTimestamp = byteBuffer.getLong();
            byte[] instanceNameData = new byte[byteBuffer.remaining()];
            byteBuffer.get(instanceNameData);
            String instanceName = new String(instanceNameData);

            long currentTimestamp = timestampProvider.getTime(timestampProvider.getTime());
            long ttlTimestamp = timestampProvider.getTime(Instant.ofEpochMilli(ttl));
            if(idBlockTimestamp < currentTimestamp - ttlTimestamp){
                columnsToRemove.add(e.getColumn());
                logger.info("Added for removal id block - key=[{}], value=[{}], timestamp=[{}], instanceName=[{}]",
                        key, counterVal, idBlockTimestamp, instanceName);
            } else {
                logger.trace("Will retain id block - key=[{}], value=[{}], timestamp=[{}], instanceName=[{}]",
                        key, counterVal, idBlockTimestamp, instanceName);
            }
        }
        if(!columnsToRemove.isEmpty()){
            Map<Value, Value> mutationMap = mutationToMap(new KCVMutation(KeyColumnValueStore.NO_ADDITIONS, columnsToRemove));
            mutateOperations.mutate(idsStoreName, getValue(key), mutationMap);
            logger.info("Removed [{}] old id blocks for - key=[{}]", columnsToRemove.size(), key);
        }
    }

    public String getIdsStoreName() {
        return idsStoreName;
    }

    BlockTimestampComparator TIMESTAMP_COMPARATOR = new BlockTimestampComparator();
    private static class BlockTimestampComparator implements Comparator<Entry> {

        @Override
        public int compare(Entry e1, Entry e2) {
            return Long.compare(getTimestamp(e1), getTimestamp(e2));
        }
    }

    private static long getTimestamp(Entry e){
        ByteBuffer byteBuffer = e.asByteBuffer();
        byteBuffer.getLong();
        return byteBuffer.getLong();
    }
}
