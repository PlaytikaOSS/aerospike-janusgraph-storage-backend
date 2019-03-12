package com.playtika.janusgraph.aerospike;

import org.janusgraph.diskstorage.StaticBuffer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableMap;

public class StoreLocks {

    public final String storeName;
    public final Map<StaticBuffer, List<AerospikeLock>> locksMap;

    public StoreLocks(String storeName, List<AerospikeLock> locks) {
        this.storeName = storeName;
        Map<StaticBuffer, List<AerospikeLock>> locksMap = new HashMap<>(locks.size());

        for(AerospikeLock lock : locks){
            locksMap.compute(lock.key, (key, locksForKey) -> {
                if(locksForKey == null || locksForKey.isEmpty()){
                    return singletonList(lock);
                } else if(locksForKey.size() == 1){
                    List<AerospikeLock> locksForKeyNew = new ArrayList<>(locksForKey);
                    locksForKeyNew.add(lock);
                    return locksForKeyNew;
                } else {
                    locksForKey.add(lock);
                    return locksForKey;
                }
            });
        }

        this.locksMap = unmodifiableMap(locksMap);
    }
}
