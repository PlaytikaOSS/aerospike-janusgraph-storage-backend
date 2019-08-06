package com.playtika.janusgraph.aerospike.utils;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicLong;

public class FixedClock extends Clock {
    private final AtomicLong time;

    public FixedClock(AtomicLong time) {
        this.time = time;
    }


    @Override
    public ZoneId getZone() {
        return null;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return null;
    }

    @Override
    public Instant instant() {
        return Instant.ofEpochMilli(time.get());
    }
}

