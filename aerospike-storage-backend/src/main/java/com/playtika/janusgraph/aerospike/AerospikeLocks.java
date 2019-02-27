package com.playtika.janusgraph.aerospike;

import org.janusgraph.diskstorage.StaticBuffer;

import java.util.*;

import static java.util.Collections.*;

class AerospikeLocks {

    private final Map<StaticBuffer, List<AerospikeLock>> locksMap;

    AerospikeLocks(int expectedKeyToLock){
        locksMap = new HashMap<>(expectedKeyToLock);
    }

    void addLocks(List<AerospikeLock> locks){
        locks.forEach(this::addLock);
    }

    private void addLock(AerospikeLock lock){
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

    void addLockOnKeys(Collection<StaticBuffer> keysToLock){
        keysToLock.forEach(this::addLockOnKey);
    }

    void addLockOnKey(StaticBuffer keyToLock){
        locksMap.compute(keyToLock, (key, locksForKey) -> {
            if(locksForKey == null){
                return emptyList();
            } else {
                return locksForKey;
            }
        });
    }

    Map<StaticBuffer, List<AerospikeLock>> getLocksMap() {
        return unmodifiableMap(locksMap);
    }

}
