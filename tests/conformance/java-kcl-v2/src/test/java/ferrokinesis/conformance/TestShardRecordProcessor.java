package ferrokinesis.conformance;

import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestShardRecordProcessor implements ShardRecordProcessor {

    private static final ConcurrentLinkedQueue<KinesisClientRecord> receivedRecords =
            new ConcurrentLinkedQueue<>();
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

    public static List<KinesisClientRecord> getRecords() {
        return new ArrayList<>(receivedRecords);
    }

    public static boolean awaitRecords(long timeoutSeconds) throws InterruptedException {
        return latch.await(timeoutSeconds, TimeUnit.SECONDS);
    }

    public static boolean wasInitializeCalled() {
        return initializeCalled.get();
    }

    @Override
    public void initialize(InitializationInput initializationInput) {
        this.shardId = initializationInput.shardId();
        initializeCalled.set(true);
        System.out.println("[KCLv2] Processor initialized for shard: " + shardId);
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        int newCount = 0;
        for (KinesisClientRecord record : processRecordsInput.records()) {
            if (seenSequenceNumbers.add(record.sequenceNumber())) {
                receivedRecords.add(record);
                newCount++;
            }
        }
        try {
            processRecordsInput.checkpointer().checkpoint();
        } catch (Exception e) {
            System.err.println("[KCLv2] Checkpoint failed for shard " + shardId + ": " + e.getMessage());
        }
        for (int i = 0; i < newCount; i++) {
            latch.countDown();
        }
    }

    @Override
    public void leaseLost(LeaseLostInput leaseLostInput) {
        // No checkpoint allowed after lease loss.
        System.out.println("[KCLv2] Lease lost for shard: " + shardId);
    }

    @Override
    public void shardEnded(ShardEndedInput shardEndedInput) {
        // Must checkpoint with the SHARD_END sentinel when the shard is exhausted.
        // Only triggered if the shard is closed (split/merge); not triggered in this
        // test since we use a single open shard.
        try {
            shardEndedInput.checkpointer().checkpoint();
        } catch (Exception e) {
            System.err.println("[KCLv2] shardEnded checkpoint failed for shard " + shardId + ": " + e.getMessage());
        }
    }

    @Override
    public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
        try {
            shutdownRequestedInput.checkpointer().checkpoint();
        } catch (Exception e) {
            System.err.println("[KCLv2] shutdownRequested checkpoint failed for shard " + shardId + ": " + e.getMessage());
        }
    }
}
