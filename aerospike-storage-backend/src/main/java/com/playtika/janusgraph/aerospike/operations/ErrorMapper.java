package com.playtika.janusgraph.aerospike.operations;

import nosql.batch.update.lock.TemporaryLockingException;
import org.janusgraph.diskstorage.BackendException;

import java.util.function.Function;

public class ErrorMapper {

    private ErrorMapper(){}

    public static final Function<? super Throwable, ? extends BackendException> INSTANCE = throwable -> {
        if(throwable instanceof TemporaryLockingException){
            return new org.janusgraph.diskstorage.locking.TemporaryLockingException(throwable);
        } else {
            return new org.janusgraph.diskstorage.locking.PermanentLockingException(throwable);
        }
    };

}
