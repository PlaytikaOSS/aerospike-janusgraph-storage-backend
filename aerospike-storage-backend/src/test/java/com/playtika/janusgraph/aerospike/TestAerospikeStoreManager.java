package com.playtika.janusgraph.aerospike;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.singletonList;

//need this class as some tests do not correctly close all graphs. Some graphs remains opened after previous test
public class TestAerospikeStoreManager extends AerospikeStoreManager{

    private static final AtomicReference<List<AerospikeStoreManager>> OPEN_MANAGERS = new AtomicReference<>();

    public TestAerospikeStoreManager(Configuration configuration) {
        super(configuration);

        OPEN_MANAGERS.updateAndGet(managers -> {
            if(managers != null) {
                List<AerospikeStoreManager> updatedManagers = new ArrayList<>(managers.size() + 1);
                updatedManagers.addAll(managers);
                updatedManagers.add(this);
                return updatedManagers;
            } else {
                return singletonList(this);
            }
        });
    }

    public static void closeAllGraphs() throws BackendException {
        List<AerospikeStoreManager> managers = OPEN_MANAGERS.get();
        if(managers != null) {
            for (AerospikeStoreManager manager : managers) {
                manager.close();
            }
        }
    }
}
