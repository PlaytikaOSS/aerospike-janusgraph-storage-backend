package com.playtika.janusgraph.aerospike;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.reactor.AerospikeReactorClient;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

import static com.playtika.janusgraph.aerospike.AerospikeKeyColumnValueStore.ENTRIES_BIN_NAME;
import static com.playtika.janusgraph.aerospike.AerospikeKeyColumnValueStore.getValue;
import static com.playtika.janusgraph.aerospike.ConfigOptions.LOCK_TTL;

class LockOperations {

    private static final Bin LOCK_BIN = new Bin("L", true);

    private final String namespace;
    private final String lockSetName;
    private final AerospikeReactorClient reactorClient;
    private final AerospikeKeyColumnValueStore store;
    private final WritePolicy putLockPolicy;

    LockOperations(String namespace, String name,
                   AerospikeReactorClient reactorClient,
                   AerospikeKeyColumnValueStore store,
                   Configuration configuration) {
        this.namespace = namespace;
        this.reactorClient = reactorClient;
        this.lockSetName = name +".lock";
        this.store = store;

        putLockPolicy = new WritePolicy();
        putLockPolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
        putLockPolicy.expiration = configuration.get(LOCK_TTL);
    }

    Mono<Void> acquireLocks(Map<StaticBuffer, List<AerospikeLock>> locks) {
        return Flux.fromIterable(locks.entrySet())
                .flatMap(lockEntry -> putLockOnKey(lockEntry.getKey())
                        .then(checkColumnValues(lockEntry.getKey(), lockEntry.getValue())))
                .then();
    }

    private Mono<Key> putLockOnKey(final StaticBuffer key){
        return reactorClient.add(putLockPolicy, getLockKey(key), LOCK_BIN);
    }

    private Mono<Boolean> checkColumnValues(final StaticBuffer key, final List<AerospikeLock> locksForKey){
        if(locksForKey.isEmpty()){
            return Mono.just(true);
        }
        Operation[] operations = locksForKey.stream()
                .map(lock -> MapOperation.getByKey(ENTRIES_BIN_NAME, getValue(lock.column), MapReturnType.VALUE))
                .toArray(Operation[]::new);
        return reactorClient.operate(store.getKey(key), operations)
                .map(keyRecord -> {
                    if(keyRecord.record != null){
                        if(locksForKey.size() > 1){
                            List<?> resultList;
                            if((resultList = keyRecord.record.getList(ENTRIES_BIN_NAME)) != null){
                                for(int i = 0, n = resultList.size(); i < n; i++){
                                    AerospikeLock lock = locksForKey.get(i);
                                    checkValue(lock, (byte[])resultList.get(i));
                                }
                            }
                        } else if(locksForKey.size() == 1){
                            byte[] actualValueData;
                            if((actualValueData = (byte[])keyRecord.record.getValue(ENTRIES_BIN_NAME)) != null){
                                checkValue(locksForKey.get(0), actualValueData);
                            }
                        }
                    }
                    return true;
                });
    }

    private void checkValue(AerospikeLock lock, byte[] actualValue){
        StaticBuffer actualValueBuffer = new StaticArrayBuffer(actualValue);
        if(actualValueBuffer.compareTo(lock.expectedValue) != 0){
            throw Exceptions.propagate(new PermanentBackendException(
                    String.format("Unexpected value for key %s, column %s, expected %s, actual %s",
                            lock.key, lock.column, lock.expectedValue, actualValueBuffer)));
        }
    }

    Mono<Void> releaseLockOnKeys(Iterable<StaticBuffer> keys) {
        return Flux.fromIterable(keys).flatMap(
                key -> reactorClient.delete(getLockKey(key))
        ).then();
    }

    private Key getLockKey(StaticBuffer staticBuffer) {
        return new Key(namespace, lockSetName, staticBuffer.getBytes(0, staticBuffer.length()));
    }

}
