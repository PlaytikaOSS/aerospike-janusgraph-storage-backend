package com.playtika.janusgraph.aerospike;

import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;

public class AerospikePolicyProvider {

    public WritePolicy writePolicy(){
        WritePolicy writePolicy = new WritePolicy();
        writePolicy.sendKey = true;
        writePolicy.expiration = -1;
        return writePolicy;
    }

    public WritePolicy deletePolicy(){
        WritePolicy deletePolicy = new WritePolicy();
        deletePolicy.expiration = -1;
        deletePolicy.durableDelete = true;
        return deletePolicy;
    }

    public Policy readPolicy(){
        Policy readPolicy = new Policy();
        readPolicy.sendKey = true;
        return readPolicy;
    }

    public ScanPolicy scanPolicy(){
        throw new UnsupportedOperationException();
    }
}
