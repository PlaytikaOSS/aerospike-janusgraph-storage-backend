package com.playtika.janusgraph.aerospike.operations;

import com.aerospike.client.Key;
import com.aerospike.client.Value;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

public class FlakingUtils {

    private static Logger logger = LoggerFactory.getLogger(FlakingLockOperations.class);

    private static final Random random = new Random();

    static Set<Key> selectFlaking(Collection<Key> keys, String errorMessage) {
        return keys.stream()
                .filter(key -> {
                    boolean fail = random.nextBoolean();
                    if (fail) {
                        logger.error(errorMessage, key);
                    }
                    return !fail;
                })
                .collect(Collectors.toSet());
    }

    static Map<String, Map<Value, Map<Value, Value>>> selectFlaking(
            Map<String, Map<Value, Map<Value, Value>>> mutationsByStore,
            String errorMessage) {
        return mutationsByStore.entrySet()
                .stream()
                .flatMap(storeEntry -> storeEntry.getValue()
                        .entrySet().stream()
                        .filter(keyEntry -> {
                            boolean fail = random.nextBoolean();
                            if(fail){
                                logger.error(errorMessage, storeEntry.getKey(), keyEntry.getKey());
                            }
                            return !fail;
                        })
                        .map(keyEntry -> Pair.with(storeEntry.getKey(), keyEntry)))
                .collect(Collectors.groupingBy(Pair::getValue0,
                        Collectors.toMap(pair -> pair.getValue1().getKey(), pair -> pair.getValue1().getValue())));
    }

    static long entriesSize(Map<String, Map<Value, Map<Value, Value>>> mutationsByStore){
        return mutationsByStore.entrySet()
                .stream()
                .mapToLong(storeEntry -> storeEntry.getValue()
                        .entrySet().size())
                .sum();
    }
}
