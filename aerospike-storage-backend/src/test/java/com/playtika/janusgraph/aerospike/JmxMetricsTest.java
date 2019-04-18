package com.playtika.janusgraph.aerospike;

import com.aerospike.AerospikeContainer;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.example.GraphOfTheGodsFactory;
import org.junit.Rule;
import org.junit.Test;

import javax.management.*;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeConfiguration;
import static com.playtika.janusgraph.aerospike.AerospikeTestUtils.getAerospikeContainer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;
import static org.junit.Assert.assertEquals;

public class JmxMetricsTest {

    private static final String JANUSGRAPH_JMX_DOMAIN = "janusgraph";

    @Rule
    public AerospikeContainer aerospike = getAerospikeContainer();

    @Test
    public void shouldReturnValidJmxMetrics() throws MalformedObjectNameException, IntrospectionException, InstanceNotFoundException, ReflectionException, AttributeNotFoundException, MBeanException {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        ObjectName janusgraphObjectName = new ObjectName(JANUSGRAPH_JMX_DOMAIN + ":*");

        for (ObjectName objectName : server.queryNames(janusgraphObjectName, null)) {
            server.unregisterMBean(objectName);
        }

        ModifiableConfiguration aerospikeConfiguration = getAerospikeConfiguration(aerospike);
        aerospikeConfiguration.set(BASIC_METRICS, true);
        aerospikeConfiguration.set(METRICS_JMX_ENABLED, true);
        aerospikeConfiguration.set(METRICS_JMX_DOMAIN, JANUSGRAPH_JMX_DOMAIN);

        try(JanusGraph graph = JanusGraphFactory.open(aerospikeConfiguration)) {
            GraphOfTheGodsFactory.loadWithoutMixedIndex(graph, true);

            assertEquals("Expected the correct number of VERTICES",
                    12, graph.traversal().V().count().tryNext().get().longValue());

            List<String> metrics = new ArrayList<>();

            // now query to get the beans
            for (ObjectName objectName : server.queryNames(janusgraphObjectName, null)) {
                MBeanInfo mbeanInfo = server.getMBeanInfo(objectName);
                for (MBeanAttributeInfo attribute : mbeanInfo.getAttributes()) {
                    String attributeName = attribute.getName();
                    String attributeFullName = objectName.getKeyProperty("name") + ":" + attributeName;
                    Object attributeValue = server.getAttribute(objectName, attributeName);

                    metrics.add(attributeFullName);
                    Predicate<Object> metricPredicate = ALL_METRICS.get(attributeFullName);
                    if(metricPredicate != null) {
                        assertThat(metricPredicate.test(attributeValue))
                                .withFailMessage("\"" + attributeFullName + "\"=" + attributeValue)
                                .isTrue();
                    }
                }
            }

            assertThat(metrics).containsAll(ALL_METRICS.keySet());
        }
    }

    private static final Map<String, Predicate<Object>> ALL_METRICS = new HashMap<String, Predicate<Object>>(){{
        put("global.storeManager.openDatabase.calls:Count", greaterThen(0L));
        put("global.storeManager.startTransaction.calls:Count", greaterThen(0L));

        put("org.janusgraph.tx.begin:Count", greaterThen(0L));
        put("org.janusgraph.tx.commit:Count", greaterThen(0L));
        put("org.janusgraph.tx.rollback:Count", greaterThen(0L));

        put("org.janusgraph.sys.schema.tx.begin:Count", greaterThen(0L));
        put("org.janusgraph.sys.schema.tx.rollback:Count", greaterThen(0L));

        put("org.janusgraph.sys.schemacache.name.retrievals:Count", greaterThen(0L));
        put("org.janusgraph.sys.schemacache.name.misses:Count", greaterThen(0L));
        put("org.janusgraph.sys.schemacache.relations.retrievals:Count", greaterThen(0L));
        put("org.janusgraph.sys.schemacache.relations.misses:Count", greaterThen(0L));

        zeroMetrics("org.janusgraph.stores.getSlice.entries-histogram", this);
        put("org.janusgraph.stores.getSlice.entries-histogram:Count", greaterThen(0L));

        put("org.janusgraph.sys.stores.getSlice.entries-returned:Count", greaterThen(0L));
        timeMetrics("org.janusgraph.sys.stores.getSlice", this);
        metrics("org.janusgraph.sys.stores.getSlice.entries-histogram", this);

        timeMetrics("org.janusgraph.sys.stores.mutate", this);

        timeMetrics("org.janusgraph.sys.schema.stores.getSlice", this);
        put("org.janusgraph.sys.schema.stores.getSlice.entries-returned:Count", greaterThen(0L));
        metrics("org.janusgraph.sys.schema.stores.getSlice.entries-histogram", this);
        timeMetrics("org.janusgraph.sys.schema.query.graph.execute", this);
        timeMetrics("org.janusgraph.sys.schema.query.graph.hasDeletions", this);
        timeMetrics("org.janusgraph.sys.schema.query.graph.getNew", this);
        timeMetrics("org.janusgraph.sys.storeManager.mutate", this);

        timeMetrics("org.janusgraph.storeManager.mutate", this);

        timeMetrics("org.janusgraph.stores.getKeys", this);

        timeMetrics("org.janusgraph.stores.getKeys.iterator.hasNext", this);

        timeMetrics("org.janusgraph.stores.getKeys.iterator.next", this);

        put("org.janusgraph.stores.getSlice.entries-returned:Count", equalTo(0L));
        timeMetrics("org.janusgraph.stores.getSlice", this);
        timeMetrics("org.janusgraph.stores.acquireLock", this);

        timeMetrics("org.janusgraph.query.graph.execute", this);
        timeMetrics("org.janusgraph.query.graph.hasDeletions", this);
        timeMetrics("org.janusgraph.query.graph.getNew", this);

        timeMetrics("org.janusgraph.query.vertex.execute", this);
        timeMetrics("org.janusgraph.query.vertex.getNew", this);

        timeMetrics("org.janusgraph.query.vertex.hasDeletions", this);

    }};

    private static void zeroMetrics(String group, Map<String, Predicate<Object>> metrics){
        metrics.put(group+":Count", equalTo(0L));
        metrics.put(group+":Max", equalTo(0L));
        metrics.put(group+":Min", equalTo(0L));
        metrics.put(group+":Mean", equalTo(0.));
        metrics.put(group+":StdDev", equalTo(0.));
        metrics.put(group+":50thPercentile", equalTo(0.));
        metrics.put(group+":75thPercentile", equalTo(0.));
        metrics.put(group+":95thPercentile", equalTo(0.));
        metrics.put(group+":98thPercentile", equalTo(0.));
        metrics.put(group+":99thPercentile", equalTo(0.));
        metrics.put(group+":999thPercentile", equalTo(0.));

    }

    private static void metrics(String group, Map<String, Predicate<Object>> metrics){
        metrics.put(group+":Count", greaterThen(0L));
        metrics.put(group+":Max", greaterThen(0L));
        metrics.put(group+":Min", equalTo(0L));
        metrics.put(group+":Mean", greaterThen(0.));
        metrics.put(group+":StdDev", greaterThen(0.));
        metrics.put(group+":50thPercentile", greaterThen(0.));
        metrics.put(group+":75thPercentile", greaterThen(0.));
        metrics.put(group+":95thPercentile", greaterThen(0.));
        metrics.put(group+":98thPercentile", greaterThen(0.));
        metrics.put(group+":99thPercentile", greaterThen(0.));
        metrics.put(group+":999thPercentile", greaterThen(0.));

    }

    private static void timeMetrics(String group, Map<String, Predicate<Object>> metrics){
        metrics.put(group+".calls:Count", greaterThen(0L));
        metrics.put(group+".time:Count", greaterThen(0L));
        metrics.put(group+".time:Max", greaterThen(0.));
        metrics.put(group+".time:Min", greaterThen(0.));
        metrics.put(group+".time:Mean", greaterThen(0.));
        metrics.put(group+".time:StdDev", o -> true);
        metrics.put(group+".time:50thPercentile", greaterThen(0.));
        metrics.put(group+".time:75thPercentile", greaterThen(0.));
        metrics.put(group+".time:95thPercentile", greaterThen(0.));
        metrics.put(group+".time:98thPercentile", greaterThen(0.));
        metrics.put(group+".time:99thPercentile", greaterThen(0.));
        metrics.put(group+".time:999thPercentile", greaterThen(0.));
        metrics.put(group+".time:MeanRate", greaterThen(0.));
        metrics.put(group+".time:OneMinuteRate", o -> true);
        metrics.put(group+".time:FiveMinuteRate", o -> true);
        metrics.put(group+".time:FifteenMinuteRate", o -> true);
        metrics.put(group+".time:RateUnit", equalTo("events/second"));
        metrics.put(group+".time:DurationUnit", equalTo("milliseconds"));
    }

    private static Predicate<Object> equalTo(Long count){
        return count::equals;
    }

    private static Predicate<Object> equalTo(Double count){
        return count::equals;
    }

    private static Predicate<Object> equalTo(String s){
        return s::equals;
    }

    private static Predicate<Object> greaterThen(Double d){
        return o -> d.compareTo((Double)o) < 0;
    }

    private static Predicate<Object> greaterThen(Long d){
        return o -> d.compareTo((Long)o) < 0;
    }

}
