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
        configureEnterpriseServer(properties, aerospike);
        return aerospike;
    }

    private static void configureEnterpriseServer(AerospikeProperties properties,
                                                  GenericContainer aerospikeContainer) {
        AsadmCommandExecutor asadmCommandExecutor = new AsadmCommandExecutor(aerospikeContainer);
        String namespace = properties.getNamespace();
        /*
         By default, the value of this metric is 90%, we set it to 100% to prevent stopping writes for the Aerospike
         Enterprise container during high consumption of system memory. For the Aerospike Community Edition, this metric is not used.
         Documentation: https://aerospike.com/docs/server/reference/configuration#stop-writes-sys-memory-pct
        */
        log.info("Switching off 'stop-writes-sys-memory-pct'... ");
        asadmCommandExecutor.execute(String.format("manage config namespace %s param stop-writes-sys-memory-pct to 100", namespace));
        log.info("Success switching off 'stop-writes-sys-memory-pct'");

        if (properties.isDurableDelete()) {
            log.info("Setting up 'disallow-expunge' to true...");
            asadmCommandExecutor.execute(String.format("manage config namespace %s param disallow-expunge to true", namespace));
            log.info("Success setting up 'disallow-expunge' to true");
        }
    }

}
