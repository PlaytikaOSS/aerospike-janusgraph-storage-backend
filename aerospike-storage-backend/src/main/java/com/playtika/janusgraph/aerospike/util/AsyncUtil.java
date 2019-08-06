package com.playtika.janusgraph.aerospike.util;

import org.janusgraph.diskstorage.PermanentBackendException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static java.util.concurrent.CompletableFuture.supplyAsync;

public class AsyncUtil {

    public static final int WAIT_TIMEOUT_IN_SECONDS = 4;

    public static void completeAll(List<CompletableFuture<?>> futures) {
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public static <K, V> Map<K, V> mapAll(Collection<K> keys, Function<K, V> valueFunction, Executor executor) throws PermanentBackendException {
        final Map<K, CompletableFuture<V>> futures = new HashMap<>(keys.size());
        for (K key : keys)
            futures.put(key, supplyAsync(() -> valueFunction.apply(key), executor));

        return getAll(futures);
    }

    public static <K, V> Map<K, V> getAll(Map<K, CompletableFuture<V>> futureMap) throws PermanentBackendException {
        Map<K, V> resultMap = new HashMap<>(futureMap.size());
        try {
            for(Map.Entry<K, CompletableFuture<V>> entry : futureMap.entrySet()){
                resultMap.put(entry.getKey(), entry.getValue().get());
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new PermanentBackendException(e);
        }
        return resultMap;
    }
}
