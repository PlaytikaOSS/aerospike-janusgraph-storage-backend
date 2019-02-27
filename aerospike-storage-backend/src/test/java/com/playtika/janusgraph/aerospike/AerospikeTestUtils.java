package com.playtika.janusgraph.aerospike;

import com.aerospike.client.AerospikeClient;

public class AerospikeTestUtils {

    public static void deleteAllRecords(String namespace){
        try(AerospikeClient aerospikeClient = new AerospikeClient("localhost", 3000)) {
            aerospikeClient.truncate(null, namespace, null, null);
        }
    }
}
