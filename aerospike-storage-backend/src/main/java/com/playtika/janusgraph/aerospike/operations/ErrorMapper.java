package com.playtika.janusgraph.aerospike.operations;

import nosql.batch.update.lock.PermanentLockingException;
import nosql.batch.update.lock.TemporaryLockingException;

import java.util.function.Function;

public class ErrorMapper {

    private ErrorMapper(){}

    public static final Function<? super Throwable, ? extends Throwable> INSTANCE = throwable -> {
        if(throwable instanceof TemporaryLockingException){
            return new org.janusgraph.diskstorage.locking.TemporaryLockingException(throwable);
        } else if(throwable instanceof PermanentLockingException){
            return new org.janusgraph.diskstorage.locking.PermanentLockingException(throwable);
        } else {
            return throwable;
        }
    };
}
