package com.playtika.janusgraph.aerospike;

import org.janusgraph.diskstorage.configuration.ConfigOption;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_NS;

public class ConfigOptions {

    public static final ConfigOption<String> NAMESPACE = new ConfigOption<>(STORAGE_NS,
            "namespace", "Aerospike namespace to use", ConfigOption.Type.LOCAL, String.class);

    public static final ConfigOption<String> GRAPH_PREFIX = new ConfigOption<>(STORAGE_NS,
            "graph-prefix", "Graph prefix, allows to keep several graph in one namespace",
            ConfigOption.Type.LOCAL, String.class);

    public static final ConfigOption<String> WAL_NAMESPACE = new ConfigOption<>(STORAGE_NS,
            "wal-namespace", "Aerospike namespace to use for write ahead log",
            ConfigOption.Type.LOCAL, String.class);

    public static final ConfigOption<Integer> SCAN_PARALLELISM = new ConfigOption<>(STORAGE_NS,
            "scan-parallelism", "How many threads may perform scan operations simultaneously. " +
                 "Should be greater then zero if you want to enable scan feature",
            ConfigOption.Type.LOCAL, 0);

    public static final ConfigOption<Long> WAL_STALE_TRANSACTION_LIFETIME_THRESHOLD = new ConfigOption<>(STORAGE_NS,
            "wal-threshold", "After this period of time (in ms) transaction in WAL considered to be stale " +
            "and can be re-processed",
            ConfigOption.Type.LOCAL, 600000L);

    public static final ConfigOption<Boolean> TEST_ENVIRONMENT = new ConfigOption<>(STORAGE_NS,
            "test-environment", "Weather this production or test environment",
            ConfigOption.Type.LOCAL, false);

    public static final ConfigOption<Boolean> START_WAL_COMPLETER = new ConfigOption<>(STORAGE_NS,
            "start-wal-completer", "Whether WAL Completer should be started inside store manager. " +
            "You should not start WAL Completer in passive data center. " +
            "You may consider to start WAL Completer externally than you will be able to suspend and resume it manually",
            ConfigOption.Type.LOCAL, true);

    public static final ConfigOption<Integer> AEROSPIKE_MIN_CONNECTIONS_PER_NODE= new ConfigOption<>(STORAGE_NS,
            "aerospike-min-connections-per-node", "Minimum number of connections on start-up per server node",
            ConfigOption.Type.LOCAL, 10);

    public static final ConfigOption<Integer> AEROSPIKE_CONNECTIONS_PER_NODE= new ConfigOption<>(STORAGE_NS,
            "aerospike-connections-per-node", "Limits how many connections aerospike can hold per node",
            ConfigOption.Type.LOCAL, 300);

    public static final ConfigOption<Integer> AEROSPIKE_EXECUTOR_MAX_THREADS = new ConfigOption<>(STORAGE_NS,
            "aerospike-executor-max-threads", "Number of max threads in Aerospike executor",
            ConfigOption.Type.LOCAL, 600);

    public static final ConfigOption<Integer> PARALLEL_READ_THRESHOLD = new ConfigOption<>(STORAGE_NS,
            "parallel-read-threshold", "Number of keys when we should start run reads in parallel",
            ConfigOption.Type.LOCAL, 1);

    public static final ConfigOption<Integer> AEROSPIKE_READ_TIMEOUT = new ConfigOption<>(STORAGE_NS,
            "aerospike-read-timeout", "Total transaction timeout in milliseconds to aerospike read operations." +
            "If timeout is zero, there will be no time limit",
            ConfigOption.Type.LOCAL, 0);

    public static final ConfigOption<Integer> AEROSPIKE_WRITE_TIMEOUT = new ConfigOption<>(STORAGE_NS,
            "aerospike-write-timeout", "Total transaction timeout in milliseconds to aerospike write operations." +
            "If timeout is zero, there will be no time limit",
            ConfigOption.Type.LOCAL, 0);

    public static final ConfigOption<Integer> AEROSPIKE_SOCKET_TIMEOUT = new ConfigOption<>(STORAGE_NS,
            "aerospike-socket-timeout", "Socket idle timeout in milliseconds when processing a database command." +
            "If timeout is zero, there will be no time limit",
            ConfigOption.Type.LOCAL, 0);

    public static final ConfigOption<Boolean> CHECK_ALL_MUTATIONS_LOCKED = new ConfigOption<>(STORAGE_NS,
            "check-all-mutations-locked", "Checks that all mutations are locked",
            ConfigOption.Type.LOCAL, false);
}
