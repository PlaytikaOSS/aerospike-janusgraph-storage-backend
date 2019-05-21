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

    public static final int INITIAL_WAIT_TIMEOUT_IN_SECONDS = 1;
    public static final int WAIT_TIMEOUT_IN_SECONDS = 5;

    private static Logger logger = LoggerFactory.getLogger(AsyncUtil.class);

    public static void completeAll(List<CompletableFuture<?>> futures) throws PermanentBackendException {
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new PermanentBackendException(e);
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

    public static void shutdownAndAwaitTermination(ExecutorService pool) {
        pool.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            if (!pool.awaitTermination(INITIAL_WAIT_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS)) {
                pool.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!pool.awaitTermination(WAIT_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS))
                    logger.error("Pool did not terminate");
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            pool.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }
}
