package com.playtika.janusgraph.aerospike;

import com.aerospike.client.policy.ClientPolicy;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeConfiguration;
import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeContainer;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class AerospikePolicyProviderTest {

    @ClassRule
    public static GenericContainer<?> container = getAerospikeContainer();

    @Test
    public void shouldReturnDefaultClientPolicyMaxSocketIdle() {
        var aerospikePolicyProvider = new AerospikePolicyProvider(
                getAerospikeConfiguration(container)
        );

        var clientPolicy = aerospikePolicyProvider.clientPolicy();

        assertThat(clientPolicy.maxSocketIdle)
                .isEqualTo(new ClientPolicy().maxSocketIdle);
    }

    @Test
    public void shouldOverrideClientPolicyMaxSocketIdle() {
        var clientMaxSocketIdle = 55;
        var aerospikePolicyProvider = new AerospikePolicyProvider(
                getAerospikeConfiguration(container)
                        .set(ConfigOptions.AEROSPIKE_CLIENT_MAX_SOCKET_IDLE, clientMaxSocketIdle)
        );

        var clientPolicy = aerospikePolicyProvider.clientPolicy();

        assertThat(clientPolicy.maxSocketIdle).isEqualTo(clientMaxSocketIdle);
    }
}