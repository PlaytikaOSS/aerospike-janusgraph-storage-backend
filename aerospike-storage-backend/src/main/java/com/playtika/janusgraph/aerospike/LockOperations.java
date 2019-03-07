package com.playtika.janusgraph.aerospike;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.StaticBuffer;

import java.util.Collection;
import java.util.List;
import java.util.Map;

interface LockOperations {

    void acquireLocks(Map<StaticBuffer, List<AerospikeLock>> locks) throws BackendException;

    void releaseLockOnKeys(Collection<StaticBuffer> keys);
}
