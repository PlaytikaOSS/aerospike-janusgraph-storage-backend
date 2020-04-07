package com.playtika.janusgraph.aerospike.operations;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class AerospikeKeyIteratorTest {

    @Test
    public void shouldNotFailOnHasNextAfterClose(){
        AerospikeKeyIterator keyIterator = new AerospikeKeyIterator(new SliceQuery(
                new StaticArrayBuffer(new byte[]{1}), new StaticArrayBuffer(new byte[]{2})
        ));

        keyIterator.scanCallback(
                new Key("ns", "set", new byte[]{7}),
                new Record(Collections.emptyMap(), 0, 100));

        assertThat(keyIterator.hasNext()).isTrue();
        assertThat(keyIterator.next()).isNotNull();

        keyIterator.close();

        assertThat(keyIterator.hasNext()).isFalse();
        assertThatThrownBy(keyIterator::next)
                .isInstanceOf(NoSuchElementException.class);

        assertThat(keyIterator.hasNext()).isFalse();
        assertThatThrownBy(keyIterator::next)
                .isInstanceOf(NoSuchElementException.class);

    }

}
