package com.playtika.janusgraph.aerospike.util;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class AsyncUtil {

    public static void allOf(List<CompletableFuture<?>> futures){
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException();
        }
    }
}
