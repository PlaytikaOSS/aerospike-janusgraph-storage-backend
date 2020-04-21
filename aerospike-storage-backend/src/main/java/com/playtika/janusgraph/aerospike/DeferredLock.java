package com.playtika.janusgraph.aerospike;

import org.janusgraph.diskstorage.StaticBuffer;

final class DeferredLock {

    final String storeName;
    final StaticBuffer key;
    final StaticBuffer column;
    final StaticBuffer expectedValue;

    DeferredLock(String storeName, StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue) {
        this.storeName = storeName;
        this.key = key;
        this.column = column;
        this.expectedValue = expectedValue;
    }
}
