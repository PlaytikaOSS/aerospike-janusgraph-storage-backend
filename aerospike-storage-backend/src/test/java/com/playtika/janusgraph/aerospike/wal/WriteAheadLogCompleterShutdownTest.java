package com.playtika.janusgraph.aerospike.wal;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Value;
import com.playtika.janusgraph.aerospike.TransactionalOperations;
import org.janusgraph.diskstorage.BackendException;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.playtika.janusgraph.aerospike.util.AsyncUtil.INITIAL_WAIT_TIMEOUT_IN_SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WriteAheadLogCompleterShutdownTest {

    IAerospikeClient client = mock(IAerospikeClient.class);
    WriteAheadLogManager writeAheadLogManager = mock(WriteAheadLogManager.class);
    TransactionalOperations transactionalOperations = mock(TransactionalOperations.class);

    WriteAheadLogCompleter writeAheadLogCompleter = new WriteAheadLogCompleter(
            client, "walNamespace", "walSetname",
            writeAheadLogManager, 10000, transactionalOperations);

    @Test
    public void shouldShutdownCorrectly() throws BackendException, InterruptedException {
        WriteAheadLogManager.WalTransaction walTransaction = new WriteAheadLogManager.WalTransaction(
                Value.get("transId"), 1000, null, null
        );

        when(writeAheadLogManager.getStaleTransactions()).thenReturn(
                IntStream.range(0, 100)
                .mapToObj(i -> walTransaction)
                .collect(Collectors.toList()));

        AtomicInteger processProgress = new AtomicInteger();

        int sleepTime = 100;

        Mockito.doAnswer(e -> {
            processProgress.incrementAndGet();

            long start = System.currentTimeMillis();
            while(System.currentTimeMillis() - start < 100L){}

            return null;
        }).when(transactionalOperations).processAndDeleteTransaction(any(), any(), any(), anyBoolean());

        writeAheadLogCompleter.start();

        while (processProgress.get() < 1) {
            Thread.sleep(sleepTime); }

        writeAheadLogCompleter.shutdown();

        assertThat(processProgress.get()).isEqualTo(INITIAL_WAIT_TIMEOUT_IN_SECONDS * 1000 / sleepTime + 1);
    }

}
