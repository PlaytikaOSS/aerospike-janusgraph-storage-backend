package com.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.NetworkSettings;
import com.github.dockerjava.api.model.Ports;
import org.rnorth.ducttape.TimeoutException;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class AerospikeWaitStrategy extends AbstractWaitStrategy {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final AerospikeProperties properties;

    public AerospikeWaitStrategy(AerospikeProperties properties) {
        this.properties = properties;
    }

    @Override
    protected void waitUntilReady() {
        long seconds = this.startupTimeout.getSeconds();

        try {
            Unreliables.retryUntilTrue((int)seconds, TimeUnit.SECONDS, () -> {
                return (Boolean)this.getRateLimiter().getWhenReady(this::isReady);
            });
        } catch (TimeoutException var4) {
            throw new ContainerLaunchException(String.format("[%s] notifies that container[%s] is not ready after [%d] seconds, container cannot be started.", this.getContainerType(), this.waitStrategyTarget.getContainerId(), seconds));
        }
    }

    protected boolean isReady() {
        String containerId = waitStrategyTarget.getContainerId();
        log.debug("Check Aerospike container {} status", containerId);

        InspectContainerResponse containerInfo = waitStrategyTarget.getContainerInfo();
        if (containerInfo == null) {
            log.debug("Aerospike container[{}] doesn't contain info. Abnormal situation, should not happen.", containerId);
            return false;
        }

        int port = getMappedPort(containerInfo.getNetworkSettings(), properties.port);
        String host = DockerClientFactory.instance().dockerHostIpAddress();

        //TODO: Remove dependency to client https://www.aerospike.com/docs/tools/asmonitor/common_tasks.html
        try (AerospikeClient client = new AerospikeClient(host, port)) {
            return client.isConnected();
        } catch (AerospikeException.Connection e) {
            log.debug("Aerospike container: {} not yet started. {}", containerId, e.getMessage());
        }
        return false;
    }

    private int getMappedPort(NetworkSettings networkSettings, int originalPort) {
        ExposedPort exposedPort = new ExposedPort(originalPort);
        Ports ports = networkSettings.getPorts();
        Map<ExposedPort, Ports.Binding[]> bindings = ports.getBindings();
        Ports.Binding[] binding = bindings.get(exposedPort);
        return Integer.valueOf(binding[0].getHostPortSpec());
    }

    protected String getContainerType() {
        return this.getClass().getSimpleName();
    }
}