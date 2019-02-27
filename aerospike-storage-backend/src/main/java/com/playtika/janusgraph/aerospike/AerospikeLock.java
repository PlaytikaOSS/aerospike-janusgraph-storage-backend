package com.playtika.janusgraph.aerospike;

import org.janusgraph.diskstorage.StaticBuffer;

final class AerospikeLock {

    final String storeName;
    final StaticBuffer key;
    final StaticBuffer column;
    final StaticBuffer expectedValue;

    public AerospikeLock(String storeName, StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue) {
        this.storeName = storeName;
        this.key = key;
        this.column = column;
        this.expectedValue = expectedValue;
    }
}
