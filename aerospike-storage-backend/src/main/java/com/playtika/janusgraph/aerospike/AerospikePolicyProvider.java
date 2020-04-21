package com.playtika.janusgraph.aerospike;

import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.NettyEventLoops;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import io.netty.channel.nio.NioEventLoopGroup;
import org.janusgraph.diskstorage.configuration.Configuration;

import static com.playtika.janusgraph.aerospike.ConfigOptions.AEROSPIKE_CONNECTIONS_PER_NODE;
import static com.playtika.janusgraph.aerospike.ConfigOptions.AEROSPIKE_READ_TIMEOUT;
import static com.playtika.janusgraph.aerospike.ConfigOptions.AEROSPIKE_WRITE_TIMEOUT;
import static com.playtika.janusgraph.aerospike.ConfigOptions.TEST_ENVIRONMENT;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.AUTH_PASSWORD;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.AUTH_USERNAME;

public class AerospikePolicyProvider {

    public static final int MAX_RETRIES = 0;
    private Configuration configuration;
    private EventLoops eventLoops;

    public AerospikePolicyProvider(Configuration configuration) {
        this.configuration = configuration;
    }

    public ClientPolicy clientPolicy() {
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.user = configuration.has(AUTH_USERNAME) ? configuration.get(AUTH_USERNAME) : null;
        clientPolicy.password = configuration.has(AUTH_PASSWORD) ? configuration.get(AUTH_PASSWORD) : null;
        clientPolicy.maxConnsPerNode = configuration.get(AEROSPIKE_CONNECTIONS_PER_NODE);
        clientPolicy.readPolicyDefault = readPolicy();
        clientPolicy.scanPolicyDefault = scanPolicy();
        clientPolicy.queryPolicyDefault = queryPolicy();
        clientPolicy.writePolicyDefault = writePolicy();
        clientPolicy.batchPolicyDefault = batchPolicy();
        clientPolicy.eventLoops = eventLoops();
        return clientPolicy;
    }

    public BatchPolicy batchPolicy() {
        BatchPolicy batchPolicy = new BatchPolicy();
        batchPolicy.totalTimeout = configuration.get(AEROSPIKE_WRITE_TIMEOUT);
        return batchPolicy;
    }

    public QueryPolicy queryPolicy() {
        QueryPolicy queryPolicy = new QueryPolicy();
        queryPolicy.totalTimeout = configuration.get(AEROSPIKE_READ_TIMEOUT);
        queryPolicy.maxRetries = MAX_RETRIES;
        return queryPolicy;
    }

    public WritePolicy writePolicy() {
        WritePolicy writePolicy = new WritePolicy();
        writePolicy.sendKey = true;
        writePolicy.expiration = -1;
        writePolicy.totalTimeout = configuration.get(AEROSPIKE_WRITE_TIMEOUT);
        writePolicy.maxRetries = MAX_RETRIES;
        return writePolicy;
    }

    public WritePolicy deletePolicy() {
        WritePolicy deletePolicy = new WritePolicy();
        deletePolicy.expiration = -1;
        deletePolicy.totalTimeout = configuration.get(AEROSPIKE_WRITE_TIMEOUT);
        deletePolicy.durableDelete = !configuration.get(TEST_ENVIRONMENT);
        deletePolicy.maxRetries = MAX_RETRIES;
        return deletePolicy;
    }

    public Policy readPolicy() {
        Policy readPolicy = new Policy();
        readPolicy.sendKey = true;
        readPolicy.totalTimeout = configuration.get(AEROSPIKE_READ_TIMEOUT);
        readPolicy.maxRetries = MAX_RETRIES;
        return readPolicy;
    }

    public ScanPolicy scanPolicy() {
        ScanPolicy scanPolicy = new ScanPolicy();
        scanPolicy.sendKey = true;
        scanPolicy.includeBinData = true;
        scanPolicy.totalTimeout = configuration.get(AEROSPIKE_READ_TIMEOUT);
        return scanPolicy;
    }

    public EventLoops eventLoops(){
        if(eventLoops ==null){
            eventLoops = new NettyEventLoops(new NioEventLoopGroup());
        }
        return eventLoops;
    }

    public void close(){
        eventLoops.close();
    }
}
