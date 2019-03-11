package com.playtika.janusgraph.aerospike;

import com.aerospike.client.*;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.configuration.Configuration;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.playtika.janusgraph.aerospike.AerospikeKeyColumnValueStore.getValue;
import static com.playtika.janusgraph.aerospike.ConfigOptions.LOCK_TTL;
import static com.playtika.janusgraph.aerospike.LockOperationsUdf.LockResult.*;
import static com.playtika.janusgraph.aerospike.util.AsyncUtil.allOf;
import static java.util.concurrent.CompletableFuture.runAsync;

class LockOperationsUdf implements LockOperations{

    public static final String PACKAGE = "check_and_lock";
    public static final String CHECK_AND_LOCK_FUNCTION_NAME = "check_and_lock";
    public static final Operation UNLOCK_OPERATION = Operation.put(new Bin("lock_time", (Long) null));

    private final AerospikeClient client;
    private final AerospikeKeyColumnValueStore store;
    private final long lockTtl;
    private final Executor aerospikeExecutor;

    LockOperationsUdf(AerospikeClient client,
                      AerospikeKeyColumnValueStore store,
                      Configuration configuration, Executor aerospikeExecutor) {
        this.client = client;
        this.store = store;

        lockTtl = configuration.get(LOCK_TTL);
        this.aerospikeExecutor = aerospikeExecutor;
    }

    @Override
    public void acquireLocks(Map<StaticBuffer, List<AerospikeLock>> locks) throws BackendException {
        Map<LockResult, List<Key>> lockResults = new ConcurrentHashMap<>();

        try {

            List<CompletableFuture<?>> futures = new ArrayList<>();
            AtomicBoolean lockFailed = new AtomicBoolean(false);

            for (Map.Entry<StaticBuffer, List<AerospikeLock>> locksForKey : locks.entrySet()) {
                futures.add(runAsync(() -> {
                    if(lockFailed.get()){
                        return;
                    }

                    Key key = store.getKey(locksForKey.getKey());
                    LockResult lockResult = checkAndLock(client, key, lockTtl,
                            buildExpectedValues(mergeLocks(locksForKey.getValue())));
                    lockResults.compute(lockResult, (result, values) -> {
                        List<Key> resultValues = values != null ? values : new ArrayList<>();
                        resultValues.add(key);
                        return resultValues;
                    });
                    if(lockResult != LOCKED){
                        lockFailed.set(true);
                    }
                }, aerospikeExecutor));
            }

            allOf(futures);

            if(lockResults.keySet().contains(CHECK_FAILED)){
                throw new PermanentBackendException("Some pre-lock checks failed:"+lockResults.keySet());
            } else if(lockResults.keySet().contains(ALREADY_LOCKED)){
                throw new TemporaryBackendException("Some locks not released yet:"+lockResults.keySet());
            }

        } catch (Throwable t){
            releaseLocks(lockResults.get(LOCKED));
            throw t;
        }
    }

    private Map<Value, Value> buildExpectedValues(List<AerospikeLock> locks){
        Map<Value, Value> expectedValues = new HashMap<>(locks.size());
        for(AerospikeLock lock : locks){
            if(lock.column != null){
                expectedValues.put(getValue(lock.column),
                        lock.expectedValue != null ? getValue(lock.expectedValue) : Value.NULL);
            }
        }
        return expectedValues;
    }

    static LockResult checkAndLock(AerospikeClient client, Key key, long lockTtl, Map<Value, Value> expectedValues) {
        return LockResult.values()[((Long) client.execute(null, key, PACKAGE, CHECK_AND_LOCK_FUNCTION_NAME,
                Value.get(lockTtl), Value.get(expectedValues))).intValue()];
    }

    enum LockResult {
        LOCKED,
        ALREADY_LOCKED, //previous lock not released yet
        CHECK_FAILED
    }

    @Override
    public void releaseLockOnKeys(Collection<StaticBuffer> keys) {
        releaseLocks(keys.stream()
                .map(store::getKey)
                .collect(Collectors.toList())
        );
    }

    private void releaseLocks(List<Key> keys) {
        if(keys != null) {
            List<CompletableFuture<?>> futures = new ArrayList<>();
            keys.forEach(key -> futures.add(runAsync(() -> {
                try {
                    client.operate(null, key, UNLOCK_OPERATION);
                } catch (AerospikeException e) {
                    if(e.getResultCode() != ResultCode.KEY_NOT_FOUND_ERROR){
                        throw e;
                    }
                }
            })));
            allOf(futures);
        }
    }

    /**
     * Merges locks for same columns
     * @param locksForKey
     * @return
     */
    static List<AerospikeLock> mergeLocks(List<AerospikeLock> locksForKey){
        if(locksForKey.size() <= 1){
            return locksForKey;
        }

        Map<StaticBuffer, AerospikeLock> columnToLockMap = new HashMap<>(locksForKey.size());
        for(AerospikeLock lock : locksForKey){
            columnToLockMap.putIfAbsent(lock.column, lock);
        }
        if(columnToLockMap.size() == locksForKey.size()){
            return locksForKey;
        } else {
            return new ArrayList<>(columnToLockMap.values());
        }
    }


}
