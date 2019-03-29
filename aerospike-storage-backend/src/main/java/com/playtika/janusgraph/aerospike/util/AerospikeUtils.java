package com.playtika.janusgraph.aerospike.util;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;

import java.util.stream.Stream;

public class AerospikeUtils {

    public static void truncateNamespace(IAerospikeClient client, String namespace) throws InterruptedException {
        while(!isEmptyNamespace(client, namespace)){
            client.truncate(null, namespace, null, null);
            Thread.sleep(100);
        }
    }

    public static boolean isEmptyNamespace(IAerospikeClient client, String namespace){
        String answer = Info.request(client.getNodes()[0], "sets/" + namespace);
        return Stream.of(answer.split(";"))
                .allMatch(s -> s.contains("objects=0"));
    }

}
