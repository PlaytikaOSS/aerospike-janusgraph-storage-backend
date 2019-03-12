package com.playtika.janusgraph.aerospike;

import org.janusgraph.diskstorage.configuration.ConfigOption;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_NS;

public class ConfigOptions {

    public static final ConfigOption<String> NAMESPACE = new ConfigOption<>(STORAGE_NS,
            "namespace", "Aerospike namespace to use", ConfigOption.Type.LOCAL, "test");

    public static final ConfigOption<Integer> LOCK_TTL = new ConfigOption<>(STORAGE_NS,
            "lock_ttl", "How long to keep locks (in seconds)", ConfigOption.Type.LOCAL, 5);

    public static final ConfigOption<Boolean> ALLOW_SCAN = new ConfigOption<>(STORAGE_NS,
            "allow_scan", "Whether to allow scans on graph. Can't be changed after graph creation", ConfigOption.Type.LOCAL, false);

    public static final ConfigOption<Integer> SCAN_PARALLELISM = new ConfigOption<>(STORAGE_NS,
            "scan_parallelism", "How many threads may perform scan operations simultaneously", ConfigOption.Type.LOCAL, 1);

    public static final ConfigOption<Integer> AEROSPIKE_PARALLELISM = new ConfigOption<>(STORAGE_NS,
            "aerospike_parallelism", "Limits how many parallel calls allowed to aerospike", ConfigOption.Type.LOCAL, 100);

}
