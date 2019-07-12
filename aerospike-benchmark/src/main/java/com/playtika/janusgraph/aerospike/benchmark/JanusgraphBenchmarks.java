package com.playtika.janusgraph.aerospike.benchmark;

import com.aerospike.AerospikeContainerUtils;
import com.aerospike.AerospikeProperties;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.GenericContainer;

import java.util.concurrent.TimeUnit;

import static com.playtika.janusgraph.aerospike.benchmark.Configurations.getAerospikeConfiguration;
import static com.playtika.janusgraph.aerospike.benchmark.Configurations.getCQLConfiguration;
import static com.playtika.janusgraph.aerospike.benchmark.Graph.buildRandomGraph;
import static com.playtika.janusgraph.aerospike.benchmark.Graph.defineSchema;

@Measurement(iterations = 10, time = 1)
@Warmup(iterations = 5, time = 1)
@Fork(3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class JanusgraphBenchmarks {

    private CassandraContainer cassandra;
    private GenericContainer aerospike;
    private JanusGraph aerospikeGraph;
    private JanusGraph cassandraGraph;

    @Setup
    public void setup() {
        cassandra = new CassandraContainer("cassandra:3.11");
        cassandra.start();
        cassandraGraph = JanusGraphFactory.open(getCQLConfiguration(cassandra));
        defineSchema(cassandraGraph);

        AerospikeProperties properties = new AerospikeProperties();
        aerospike = AerospikeContainerUtils.startAerospikeContainer(properties);
        aerospikeGraph = JanusGraphFactory.open(getAerospikeConfiguration(aerospike, properties));
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
        buildRandomGraph(cassandraGraph);
        return cassandraGraph;
    }

    @Benchmark
    public JanusGraph aerospike() {
        buildRandomGraph(aerospikeGraph);
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
