package com.playtika.janusgraph.aerospike.operations;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.reactor.AerospikeReactorClient;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import com.aerospike.client.reactor.retry.AerospikeReactorRetryClient;
import com.playtika.janusgraph.aerospike.AerospikePolicyProvider;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.reactivestreams.Publisher;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuples;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_HOSTS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_PORT;

public class AerospikeOperations {

    private static final int DEFAULT_PORT = 3000;

    public static final String ENTRIES_BIN_NAME = "entries";

    private final String namespace;
    private final String graphPrefix;
    private final IAerospikeClient client;

    private IAerospikeReactorClient reactorClient;
    private final AerospikePolicyProvider aerospikePolicyProvider;

    public AerospikeOperations(String graphPrefix, String namespace,
                               IAerospikeClient client,
                               IAerospikeReactorClient reactorClient,
                               AerospikePolicyProvider aerospikePolicyProvider) {
        this.graphPrefix = graphPrefix+".";
        this.namespace = namespace;
        this.client = client;
        this.reactorClient = reactorClient;
        this.aerospikePolicyProvider = aerospikePolicyProvider;
    }

    public IAerospikeClient getClient() {
        return client;
    }

    public IAerospikeReactorClient getReactorClient() {
        return reactorClient;
    }

    public String getNamespace() {
        return namespace;
    }

    Key getKey(String storeName, StaticBuffer staticBuffer) {
        return getKey(storeName, getValue(staticBuffer));
    }

    public static Value getValue(StaticBuffer staticBuffer) {
        return staticBuffer.as((array, offset, limit) -> Value.get(array, offset, limit - offset));
    }

    public Key getKey(String storeName, Value value) {
        return new Key(namespace, getSetName(storeName), value);
    }

    String getSetName(String storeName) {
        return graphPrefix + storeName;
    }

    public AerospikePolicyProvider getAerospikePolicyProvider() {
        return aerospikePolicyProvider;
    }

    public String getGraphPrefix() {
        return graphPrefix;
    }

    public void close() {
        client.close();
        aerospikePolicyProvider.close();
    }

    public static IAerospikeReactorClient buildAerospikeReactorClient(
            IAerospikeClient aerospikeClient, EventLoops eventLoops){
        return new AerospikeReactorRetryClient(
                new AerospikeReactorClient(aerospikeClient, eventLoops),
                retryOnNoMoreConnections());
    }

    //TODO Move to aerospike reactor client
    public static final int BACKOFF_NANOS = 100;

    public static Function<Flux<Throwable>, ? extends Publisher<?>> retryOnNoMoreConnections() {
        return retryOn((throwable) -> throwable instanceof AerospikeException.Connection && ((AerospikeException.Connection)throwable).getResultCode() == -7,
                BACKOFF_NANOS);
    }

    private static final Duration NEGATIVE_DURATION = Duration.ofSeconds(-1);

    public static Function<Flux<Throwable>, ? extends Publisher<?>> retryOn(Predicate<Throwable> retryOn, int backoffNanos) {
        AtomicLong backOff = new AtomicLong();
        return retry((throwable, integer) -> retryOn.test(throwable)
                ? Duration.ofNanos(backOff.addAndGet(backoffNanos)) : NEGATIVE_DURATION);
    }

    public static Function<Flux<Throwable>, ? extends Publisher<?>> retry(BiFunction<Throwable, Integer, Duration> retryDelay) {
        return (throwableFlux) -> {
            return throwableFlux.zipWith(Flux.range(1, 2147483647), (error, index) -> {
                Duration delay = retryDelay.apply(error, index);
                if (delay.isNegative()) {
                    throw Exceptions.propagate(error);
                } else {
                    return Tuples.of(delay, error);
                }
            }).concatMap((tuple2) -> {
                return !tuple2.getT1().isZero() ? Mono.delay(tuple2.getT1()).map((time) -> {
                    return tuple2.getT2();
                }) : Mono.just(tuple2.getT2());
            });
        };
    }

    public static IAerospikeClient buildAerospikeClient(Configuration configuration, ClientPolicy clientPolicy) {
        int port = configuration.has(STORAGE_PORT) ? configuration.get(STORAGE_PORT) : DEFAULT_PORT;

        Host[] hosts = Stream.of(configuration.get(STORAGE_HOSTS))
                .map(hostname -> new Host(hostname, port)).toArray(Host[]::new);

        return new AerospikeClient(clientPolicy, hosts);
    }

}
