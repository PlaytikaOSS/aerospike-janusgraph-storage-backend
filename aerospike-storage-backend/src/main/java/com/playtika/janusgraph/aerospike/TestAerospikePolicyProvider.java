package com.playtika.janusgraph.aerospike;

import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;

public class TestAerospikePolicyProvider extends AerospikePolicyProvider {

    @Override
    public ScanPolicy scanPolicy(){
        ScanPolicy scanPolicy = new ScanPolicy();
        scanPolicy.sendKey = true;
        scanPolicy.includeBinData = true;
        return scanPolicy;
    }

    @Override
    public WritePolicy deletePolicy(){
        WritePolicy deletePolicy = super.deletePolicy();
        deletePolicy.durableDelete = false;
        return deletePolicy;
    }
}
