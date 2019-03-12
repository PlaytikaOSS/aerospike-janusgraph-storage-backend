package com.playtika.janusgraph.aerospike.util;

import org.janusgraph.diskstorage.PermanentBackendException;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class AsyncUtil {

    public static void allOf(List<CompletableFuture<?>> futures) throws PermanentBackendException {
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new PermanentBackendException(e);
        }
    }
}
