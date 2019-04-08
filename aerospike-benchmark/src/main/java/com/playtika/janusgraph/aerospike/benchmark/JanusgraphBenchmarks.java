package com.playtika.janusgraph.aerospike.benchmark;

import com.aerospike.AerospikeContainer;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.testcontainers.containers.CassandraContainer;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.playtika.janusgraph.aerospike.benchmark.Configurations.getAerospikeConfiguration;
import static com.playtika.janusgraph.aerospike.benchmark.Configurations.getCQLConfiguration;
import static com.playtika.janusgraph.aerospike.benchmark.Graph.buildGraph;
import static com.playtika.janusgraph.aerospike.benchmark.Graph.defineSchema;

@Measurement(iterations = 20, time = 1)
@Warmup(iterations = 10, time = 1)
@Fork(3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class JanusgraphBenchmarks {

    private CassandraContainer cassandra;
    private AerospikeContainer aerospike;
    private JanusGraph aerospikeGraph;
    private JanusGraph cassandraGraph;

    @Setup
    public void setup() {
        cassandra = new CassandraContainer("cassandra:3.11");
        cassandra.start();
        cassandraGraph = JanusGraphFactory.open(getCQLConfiguration(cassandra));
        defineSchema(cassandraGraph);

        aerospike = new AerospikeContainer("aerospike/aerospike-server:4.3.0.2").withNamespace("TEST");
        aerospike.start();
        aerospikeGraph = JanusGraphFactory.open(getAerospikeConfiguration(aerospike));
        defineSchema(aerospikeGraph);
    }

    @TearDown
    public void tearDown() {
        cassandraGraph.close();
        cassandra.close();

        aerospikeGraph.close();
        aerospike.close();
    }

    @Benchmark
    public JanusGraph cassandra() {
        buildGraph(cassandraGraph, UUID.randomUUID());
        return cassandraGraph;
    }

    @Benchmark
    public JanusGraph aerospike() {
        buildGraph(aerospikeGraph, UUID.randomUUID());
        return aerospikeGraph;
    }


    //used to run from IDE
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .jvmArgs("-Xms1024m", "-Xmx4024m")
                .include(".*" + JanusgraphBenchmarks.class.getSimpleName() + ".*")
                //.addProfiler( StackProfiler.class )
                .build();

        new Runner(opt).run();
    }

}
