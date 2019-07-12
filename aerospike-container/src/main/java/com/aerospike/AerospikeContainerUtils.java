package com.aerospike;

import com.github.dockerjava.api.model.Capability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategy;

import java.time.Duration;

public class AerospikeContainerUtils {

    private static final Logger log = LoggerFactory.getLogger(AerospikeContainerUtils.class);

    public static GenericContainer startAerospikeContainer(AerospikeProperties properties){
        AerospikeWaitStrategy aerospikeWaitStrategy = new AerospikeWaitStrategy(properties);

        log.info("Starting aerospike server. Docker image: {}", properties.dockerImage);

        Duration startupTimeout = Duration.ofSeconds(60);
        WaitStrategy waitStrategy = new WaitAllStrategy()
                .withStrategy(aerospikeWaitStrategy)
                .withStrategy(new HostPortWaitStrategy())
                .withStartupTimeout(startupTimeout);

        GenericContainer aerospike =
                new GenericContainer<>(properties.dockerImage)
                        .withExposedPorts(properties.port)
                        .withEnv("NAMESPACE", properties.namespace)
                        .withEnv("SERVICE_PORT", String.valueOf(properties.port))
                        .withEnv("MEM_GB", String.valueOf(1))
                        .withEnv("STORAGE_GB", String.valueOf(1))
                        .withCreateContainerCmdModifier(cmd -> cmd.withCapAdd(Capability.NET_ADMIN))
                        .waitingFor(waitStrategy)
                        .withStartupTimeout(startupTimeout);

        aerospike.start();
        return aerospike;
    }

}