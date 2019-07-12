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
            "scan-parallelism", "How many threads may perform scan operations simultaneously",
            ConfigOption.Type.LOCAL, 1);

    public static final ConfigOption<Integer> AEROSPIKE_PARALLELISM = new ConfigOption<>(STORAGE_NS,
            "aerospike-parallelism", "Limits how many parallel calls allowed to aerospike",
            ConfigOption.Type.LOCAL, 100);

    public static final ConfigOption<Long> WAL_STALE_TRANSACTION_LIFETIME_THRESHOLD = new ConfigOption<>(STORAGE_NS,
            "wal-threshold", "After this period of time (in ms) transaction in WAL considered to be stale " +
            "and can be re-processed",
            ConfigOption.Type.LOCAL, 60000L);

    public static final ConfigOption<Boolean> TEST_ENVIRONMENT = new ConfigOption<>(STORAGE_NS,
            "test-environment", "Weather this production or test environment",
            ConfigOption.Type.LOCAL, false);

}
