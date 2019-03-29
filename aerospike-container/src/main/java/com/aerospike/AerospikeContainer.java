package com.aerospike;

import com.github.dockerjava.api.command.InspectContainerResponse;
import org.testcontainers.containers.GenericContainer;

import java.time.Duration;

public class AerospikeContainer<SELF extends AerospikeContainer<SELF>> extends GenericContainer<SELF> {

    private int port = 3000;
    private String namespace = "TEST";
    private int memoryGigabytes = 1;
    private int storageGigabytes = 1;
    private int startupAttempts = 3;

    public AerospikeContainer(String dockerImage) {
        super(dockerImage);
    }

    @Override
    protected void configure() {
        addExposedPort(port);

        addEnv("NAMESPACE", namespace);
        addEnv("SERVICE_PORT", String.valueOf(port));
        addEnv("MEM_GB", String.valueOf(memoryGigabytes));
        addEnv("STORAGE_GB", String.valueOf(storageGigabytes));

        setStartupAttempts(startupAttempts);
    }

    @Override
    protected void containerIsStarted(InspectContainerResponse containerInfo) {
        int mappedPort = getMappedPort(port);
        String host = getContainerIpAddress();

        new AerospikeWaitStrategy(host, mappedPort)
                .withStartupTimeout(Duration.ofSeconds(120))
                .waitUntilReady(this);
    }

    public int getPort() {
        return getMappedPort(port);
    }

    public String getNamespace() {
        return namespace;
    }

    public SELF withNamespace(String namespace) {
        this.namespace = namespace;
        return self();
    }

    public SELF withPort(int port) {
        this.port = port;
        return self();
    }

    public SELF withMemoryGigabytes(int memoryGigabytes) {
        this.memoryGigabytes = memoryGigabytes;
        return self();
    }

    public SELF withStorageGigabytes(int storageGigabytes) {
        this.storageGigabytes = storageGigabytes;
        return self();
    }

    public SELF withStartupAttempts(int startupAttempts) {
        this.startupAttempts = startupAttempts;
        return self();
    }
}