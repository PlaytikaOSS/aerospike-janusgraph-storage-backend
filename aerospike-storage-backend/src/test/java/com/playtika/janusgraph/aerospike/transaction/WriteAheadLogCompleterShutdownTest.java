package com.playtika.janusgraph.aerospike.transaction;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Value;
import com.playtika.janusgraph.aerospike.operations.AerospikeOperations;
import com.playtika.janusgraph.aerospike.operations.LockOperations;
import com.playtika.janusgraph.aerospike.operations.MutateOperations;
import org.janusgraph.diskstorage.BackendException;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.playtika.janusgraph.aerospike.util.AsyncUtil.WAIT_TIMEOUT_IN_SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.*;

public class WriteAheadLogCompleterShutdownTest {

    IAerospikeClient client = mock(IAerospikeClient.class);
    WriteAheadLogManager writeAheadLogManager = mock(WriteAheadLogManagerBasic.class);
    TransactionalOperations transactionalOperations = spy(new TransactionalOperations(
            writeAheadLogManager, mock(LockOperations.class), mock(MutateOperations.class)));
    AerospikeOperations aerospikeOperations = mock(AerospikeOperations.class);
    WalOperations walOperations = mock(WalOperations.class);

    @Test
    public void shouldShutdownCorrectly() throws BackendException, InterruptedException {
        WriteAheadLogManagerBasic.WalTransaction walTransaction = new WriteAheadLogManagerBasic.WalTransaction(
                Value.get("transId"), 1000, null, null
        );

        when(walOperations.getAerospikeOperations()).thenReturn(aerospikeOperations);
        when(aerospikeOperations.getClient()).thenReturn(client);
        when(walOperations.getWalNamespace()).thenReturn("walNamespace");
        when(walOperations.getWalNamespace()).thenReturn("walSetname");
        when(walOperations.getStaleTransactionLifetimeThresholdInMs()).thenReturn(10000L);

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

        WriteAheadLogCompleter writeAheadLogCompleter = new WriteAheadLogCompleter(
                walOperations, transactionalOperations);
        writeAheadLogCompleter.start();

        while (processProgress.get() < 1) {
            Thread.sleep(sleepTime); }

        writeAheadLogCompleter.shutdown();

        assertThat(processProgress.get()).isLessThan(WAIT_TIMEOUT_IN_SECONDS / 2 * 1000 / sleepTime + 2);
    }

}
