package com.playtika.janusgraph.aerospike.util;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;

import java.util.stream.Stream;

public class AerospikeUtils {

    public static void truncateSet(IAerospikeClient client, String namespace, String setName) throws InterruptedException {
        while(!isEmptySet(client, namespace, setName)){
            client.truncate(null, namespace, setName, null);
            Thread.sleep(100);
        }
    }

    public static boolean isEmptySet(IAerospikeClient client, String namespace, String setName){
        String answer = Info.request(client.getNodes()[0], "sets/" + namespace+"/"+setName);
        return answer.isEmpty()
                || Stream.of(answer.split(";"))
                .allMatch(s -> s.contains("objects=0"));
    }

}
