package com.playtika.janusgraph.aerospike;

import com.aerospike.client.AerospikeClient;
import com.github.dockerjava.api.model.Capability;
import com.playtika.test.aerospike.AerospikeProperties;
import com.playtika.test.aerospike.AerospikeWaitStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategy;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static com.playtika.janusgraph.aerospike.util.AerospikeUtils.isEmptyNamespace;
import static com.playtika.janusgraph.aerospike.util.AerospikeUtils.truncateNamespace;
import static com.playtika.test.common.utils.ContainerUtils.containerLogsConsumer;
import static java.time.temporal.ChronoUnit.SECONDS;

public class AerospikeTestUtils {

    private static Logger logger = LoggerFactory.getLogger(AerospikeTestUtils.class);

    static void deleteAllRecords(String namespace) throws InterruptedException {
        try(AerospikeClient client = new AerospikeClient("localhost", 3000)) {
            while(!isEmptyNamespace(client, namespace)){
                truncateNamespace(client, namespace);
                Thread.sleep(100);
            }
        }
    }

    private static final AtomicReference<GenericContainer> AEROSPIKE_CONTAINER = new AtomicReference<>();

    public static void startAerospikeContainer() {
        AEROSPIKE_CONTAINER.updateAndGet(genericContainer -> {
            if(genericContainer == null){
                GenericContainer aerospikeContainer = startAerospikeContainerImpl();
                //stop container on JVM exit
                Runtime.getRuntime().addShutdownHook(new Thread(aerospikeContainer::stop));
                return aerospikeContainer;
            } else {
                return genericContainer;
            }
        });
    }

    private static GenericContainer startAerospikeContainerImpl() {
        AerospikeProperties properties = new AerospikeProperties();
        AerospikeWaitStrategy aerospikeWaitStrategy = new AerospikeWaitStrategy(properties);

        logger.info("Starting aerospike server. Docker image: {}", properties.getDockerImage());
        WaitStrategy waitStrategy = new WaitAllStrategy()
                .withStrategy(aerospikeWaitStrategy)
                .withStrategy(new HostPortWaitStrategy())
                .withStartupTimeout(Duration.of(60, SECONDS));

        GenericContainer aerospike =
                new GenericContainer<>(properties.getDockerImage())
                        .withExposedPorts(properties.getPort())
                        .withLogConsumer(containerLogsConsumer(logger))
                        // see https://github.com/aerospike/aerospike-server.docker/blob/master/aerospike.template.conf
                        .withEnv("NAMESPACE", properties.getNamespace())
                        .withEnv("SERVICE_PORT", String.valueOf(properties.getPort()))
                        .withEnv("MEM_GB", String.valueOf(1))
                        .withEnv("STORAGE_GB", String.valueOf(1))
                        .withCreateContainerCmdModifier(cmd -> cmd.withCapAdd(Capability.NET_ADMIN))
                        .waitingFor(waitStrategy)
                        .withStartupTimeout(properties.getTimeoutDuration());

        aerospike.start();
        return aerospike;
    }
}
