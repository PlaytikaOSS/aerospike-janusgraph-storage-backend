package com.aerospike;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;

public class AsadmCommandExecutor {

    private static final Logger log = LoggerFactory.getLogger(AsadmCommandExecutor.class);

    private final GenericContainer<?> aerospikeContainer;

    public AsadmCommandExecutor(GenericContainer<?> aerospikeContainer) {
        this.aerospikeContainer = aerospikeContainer;
    }

    public void execute(String command) {
        try {
            Container.ExecResult result = aerospikeContainer.execInContainer("asadm", "--enable", "-e", command);
            logStdout(result);
            if (result.getExitCode() != 0 || isBadResponse(result)) {
                throw new IllegalStateException(String.format("Failed to execute \"asadm --enable -e '%s'\": \nstdout:\n%s\nstderr:\n%s",
                        command, result.getStdout(), result.getStderr()));
            }
        } catch (Exception ex) {
            throw new IllegalStateException(String.format("Failed to execute \"asadm\"", ex));
        }
    }

    private boolean isBadResponse(Container.ExecResult execResult) {
        String stdout = execResult.getStdout();
        /*
        Example of the stdout without error:
        ~Set Namespace Param stop-writes-sys-memory-pct to 100~
                     Node|Response
        728bb242e58c:3000|ok
        Number of rows: 1
        */
        return !stdout.contains("|ok");
    }

    private static void logStdout(Container.ExecResult result) {
        log.debug("Aerospike asadm util stdout: \n{}\n{}", result.getStdout(), result.getStderr());
    }
}
