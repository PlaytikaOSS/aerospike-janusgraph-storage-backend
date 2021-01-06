package com.playtika.janusgraph.aerospike.util;

import org.janusgraph.diskstorage.PermanentBackendException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.CompletableFuture.supplyAsync;

public class AsyncUtil {

    public static final int WAIT_TIMEOUT_IN_SECONDS = 4;

    public static <K, E extends Exception> void completeAll(Collection<K> keys, Predicate<K> keyPredicate,
                                       Function<K, Boolean> resultFunction,
                                       Supplier<E> failedResultErrorSupplier,
                                       Executor executor) throws E {

        if(keys.size() == 1){
            K key = keys.iterator().next();
            if(!keyPredicate.test(key)){
                return;
            }
            boolean result = resultFunction.apply(key);
            if(!result && failedResultErrorSupplier != null) {
                throw failedResultErrorSupplier.get();
            }

            return;
        }

        List<CompletableFuture<?>> futures = new ArrayList<>(keys.size());
        AtomicReference<Throwable> failed = new AtomicReference<>();
        AtomicBoolean checkFailed = new AtomicBoolean(false);

        for (K key : keys) {
            if(!keyPredicate.test(key)){
                continue;
            }

            futures.add(runAsync(() -> {
                if(checkFailed.get()){
                    return;
                }
                try {
                    if (!resultFunction.apply(key)) {
                        checkFailed.set(true);
                    }
                } catch (Throwable t) {
                    failed.set(t);
                    checkFailed.set(true);
                }
            }, executor));
        }

        completeAll(futures);

        if(failed.get() != null){
            throw new RuntimeException(failed.get());
        }

        if(checkFailed.get() && failedResultErrorSupplier != null) {
            throw failedResultErrorSupplier.get();
        }
    }

    protected static void completeAll(List<CompletableFuture<?>> futures) {
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public static <K, V> Map<K, V> mapAll(Collection<K> keys, Function<K, V> valueFunction,
                                          Executor executor) throws PermanentBackendException {
        if(keys.size() == 1){
            K key = keys.iterator().next();
            return Collections.singletonMap(key, valueFunction.apply(key));
        }

        final Map<K, CompletableFuture<V>> futures = new HashMap<>(keys.size());
        AtomicReference<Throwable> failed = new AtomicReference<>();
        for (K key : keys)
            futures.put(key, supplyAsync(() -> {
                try {
                    if(failed.get() != null){
                        return null;
                    }
                    return valueFunction.apply(key);
                } catch (Throwable t) {
                    failed.set(t);
                    return null;
                }
            }, executor));



        Map<K, V> result = getAll(futures);
        if(failed.get() != null){
            throw new PermanentBackendException(failed.get());
        } else {
            return result;
        }
    }

    public static <K, V> Map<K, V> getAll(Map<K, CompletableFuture<V>> futureMap) throws PermanentBackendException {
        Map<K, V> resultMap = new HashMap<>(futureMap.size());
        try {
            for(Map.Entry<K, CompletableFuture<V>> entry : futureMap.entrySet()){
                resultMap.put(entry.getKey(), entry.getValue().get());
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
        return resultMap;
    }
}
