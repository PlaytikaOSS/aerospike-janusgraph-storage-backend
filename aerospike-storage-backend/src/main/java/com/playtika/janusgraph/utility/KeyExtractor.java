package com.playtika.janusgraph.utility;

import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.graphdb.idmanagement.IDManager;

import static com.playtika.janusgraph.aerospike.operations.batch.BatchUpdates.EDGE_STORE_NAME;

public class KeyExtractor {

    private final IDManager idManager;

    public KeyExtractor(IDManager idManager) {
        this.idManager = idManager;
    }

    public long getVertexId(String storeName, StaticBuffer key) {
        long keyID;
        if(storeName.equals(EDGE_STORE_NAME)){
            keyID = idManager.getKeyID(key);
        } else {
            keyID = -1;
        }
        return keyID;
    }
}
