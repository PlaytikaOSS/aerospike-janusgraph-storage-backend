package com.playtika.janusgraph.aerospike.util;

import org.janusgraph.diskstorage.BackendException;
import reactor.core.publisher.Mono;

import static reactor.core.Exceptions.unwrap;

public class ReactorUtil {

   public static <E> E block(Mono<E> mono) throws BackendException {
       try {
           return mono.block();
       } catch (Throwable t) {
           Throwable cause = unwrap(t);
           if(cause instanceof BackendException){
               throw (BackendException)cause;
           } else if(cause instanceof RuntimeException){
               throw (RuntimeException)cause;
           } else {
               throw new RuntimeException(cause);
           }
       }
   }
}
