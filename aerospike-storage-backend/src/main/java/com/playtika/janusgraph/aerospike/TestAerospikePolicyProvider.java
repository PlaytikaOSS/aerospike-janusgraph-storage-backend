package com.playtika.janusgraph.aerospike;

import com.aerospike.client.policy.WritePolicy;

public class TestAerospikePolicyProvider extends AerospikePolicyProvider {

    @Override
    public WritePolicy deletePolicy(){
        WritePolicy deletePolicy = super.deletePolicy();
        deletePolicy.durableDelete = false;
        return deletePolicy;
    }
}
