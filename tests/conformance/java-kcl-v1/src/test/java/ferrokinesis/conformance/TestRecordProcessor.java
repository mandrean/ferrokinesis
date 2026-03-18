package ferrokinesis.conformance;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestRecordProcessor implements IRecordProcessor {

    private static final ConcurrentLinkedQueue<Record> receivedRecords = new ConcurrentLinkedQueue<>();
    private static final Set<String> seenSequenceNumbers = ConcurrentHashMap.newKeySet();
    private static volatile CountDownLatch latch;
    private static final AtomicBoolean initializeCalled = new AtomicBoolean(false);

    private String shardId;

    public static void reset(int expectedCount) {
        receivedRecords.clear();
        seenSequenceNumbers.clear();
        latch = new CountDownLatch(expectedCount);
        initializeCalled.set(false);
    }

    public static List<Record> getRecords() {
        return new ArrayList<>(receivedRecords);
    }

    public static boolean awaitRecords(long timeoutSeconds) throws InterruptedException {
        return latch.await(timeoutSeconds, java.util.concurrent.TimeUnit.SECONDS);
    }

    public static boolean wasInitializeCalled() {
        return initializeCalled.get();
    }

    @Override
    public void initialize(InitializationInput initializationInput) {
        this.shardId = initializationInput.getShardId();
        initializeCalled.set(true);
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        for (Record record : processRecordsInput.getRecords()) {
            if (seenSequenceNumbers.add(record.getSequenceNumber())) {
                receivedRecords.add(record);
                latch.countDown();
            }
        }
        try {
            processRecordsInput.getCheckpointer().checkpoint();
        } catch (Exception e) {
            System.err.println("Checkpoint failed for shard " + shardId + ": " + e.getMessage());
        }
    }

    @Override
    public void shutdown(ShutdownInput shutdownInput) {
        if (shutdownInput.getShutdownReason() ==
                com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason.TERMINATE) {
            try {
                shutdownInput.getCheckpointer().checkpoint();
            } catch (Exception e) {
                System.err.println("Final checkpoint failed for shard " + shardId + ": " + e.getMessage());
            }
        }
    }
}
