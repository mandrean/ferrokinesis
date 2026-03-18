package ferrokinesis.conformance;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.Protocol;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class KinesisV2ConformanceTest {

    static {
        // Disable CBOR protocol — the mock server's CBOR support is incomplete
        System.setProperty("aws.cborEnabled", "false");
    }

    private static final String ENDPOINT =
            System.getenv().getOrDefault("KINESIS_ENDPOINT", "http://localhost:4567");
    private static final String STREAM_NAME = "java-v2-conformance";

    private static final KinesisClient client = KinesisClient.builder()
            .endpointOverride(URI.create(ENDPOINT))
            .region(Region.US_EAST_1)
            .credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create("test", "test")))
            .build();

    private static final KinesisAsyncClient asyncClient = KinesisAsyncClient.builder()
            .endpointOverride(URI.create(ENDPOINT))
            .region(Region.US_EAST_1)
            .credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create("test", "test")))
            .httpClient(NettyNioAsyncHttpClient.builder()
                    .protocol(Protocol.HTTP1_1)
                    .build())
            .build();

    // Shared across ordered tests — requires @TestMethodOrder(OrderAnnotation) (do not run in parallel)
    private static String putShardId;
    private static String shardIterator;

    private void waitForActive(String streamName) {
        for (int i = 0; i < 30; i++) {
            DescribeStreamResponse resp = client.describeStream(
                    DescribeStreamRequest.builder().streamName(streamName).build());
            if (resp.streamDescription().streamStatus() == StreamStatus.ACTIVE) {
                return;
            }
            try { Thread.sleep(200); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        fail("Stream " + streamName + " did not become ACTIVE");
    }

    @Test @Order(1)
    void createStream() {
        client.createStream(CreateStreamRequest.builder()
                .streamName(STREAM_NAME)
                .shardCount(2)
                .build());
    }

    @Test @Order(2)
    void describeStream() {
        waitForActive(STREAM_NAME);
        DescribeStreamResponse resp = client.describeStream(
                DescribeStreamRequest.builder().streamName(STREAM_NAME).build());
        StreamDescription desc = resp.streamDescription();
        assertEquals(STREAM_NAME, desc.streamName());
        assertEquals(StreamStatus.ACTIVE, desc.streamStatus());
        assertEquals(2, desc.shards().size());
    }

    @Test @Order(3)
    void listStreams() {
        ListStreamsResponse resp = client.listStreams();
        assertTrue(resp.streamNames().contains(STREAM_NAME));
    }

    @Test @Order(4)
    void putRecord() {
        PutRecordResponse resp = client.putRecord(PutRecordRequest.builder()
                .streamName(STREAM_NAME)
                .data(SdkBytes.fromUtf8String("hello from java-v2"))
                .partitionKey("pk-1")
                .build());
        assertNotNull(resp.shardId());
        assertFalse(resp.shardId().isEmpty());
        assertNotNull(resp.sequenceNumber());
        assertFalse(resp.sequenceNumber().isEmpty());
        putShardId = resp.shardId();
    }

    @Test @Order(5)
    void putRecords() {
        List<PutRecordsRequestEntry> entries = Arrays.asList(
                PutRecordsRequestEntry.builder()
                        .data(SdkBytes.fromUtf8String("batch-0")).partitionKey("pk-0").build(),
                PutRecordsRequestEntry.builder()
                        .data(SdkBytes.fromUtf8String("batch-1")).partitionKey("pk-1").build(),
                PutRecordsRequestEntry.builder()
                        .data(SdkBytes.fromUtf8String("batch-2")).partitionKey("pk-2").build()
        );
        PutRecordsResponse resp = client.putRecords(PutRecordsRequest.builder()
                .streamName(STREAM_NAME)
                .records(entries)
                .build());
        assertEquals(0, resp.failedRecordCount().intValue());
        assertEquals(3, resp.records().size());
    }

    @Test @Order(6)
    void getShardIterator() {
        assertNotNull(putShardId, "putRecord must succeed before getShardIterator");
        GetShardIteratorResponse resp = client.getShardIterator(GetShardIteratorRequest.builder()
                .streamName(STREAM_NAME)
                .shardId(putShardId)
                .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                .build());
        assertNotNull(resp.shardIterator());
        assertFalse(resp.shardIterator().isEmpty());
        shardIterator = resp.shardIterator();
    }

    @Test @Order(7)
    void getRecords() {
        assertNotNull(shardIterator, "getShardIterator must succeed before getRecords");
        GetRecordsResponse resp = client.getRecords(GetRecordsRequest.builder()
                .shardIterator(shardIterator)
                .build());
        assertTrue(resp.records().size() >= 1);
        String first = resp.records().get(0).data().asUtf8String();
        assertEquals("hello from java-v2", first);
    }

    @Test @Order(8)
    void subscribeToShard() throws Exception {
        // Get stream ARN
        DescribeStreamResponse descResp = client.describeStream(
                DescribeStreamRequest.builder().streamName(STREAM_NAME).build());
        String streamArn = descResp.streamDescription().streamARN();

        // Register consumer
        RegisterStreamConsumerResponse regResp = client.registerStreamConsumer(
                RegisterStreamConsumerRequest.builder()
                        .streamARN(streamArn)
                        .consumerName("java-v2-subscriber")
                        .build());
        String consumerArn = regResp.consumer().consumerARN();

        // Wait for consumer to become ACTIVE
        Thread.sleep(600);

        // Put a record before subscribing
        client.putRecord(PutRecordRequest.builder()
                .streamName(STREAM_NAME)
                .data(SdkBytes.fromUtf8String("subscribe-test"))
                .partitionKey("pk-sub")
                .build());

        // Subscribe to shard
        CountDownLatch latch = new CountDownLatch(1);
        List<Record> receivedRecords = java.util.Collections.synchronizedList(new ArrayList<>());
        AtomicReference<String> continuationSeqNum = new AtomicReference<>();
        AtomicReference<Long> millisBehindLatest = new AtomicReference<>();
        AtomicReference<Throwable> error = new AtomicReference<>();

        SubscribeToShardRequest subscribeRequest = SubscribeToShardRequest.builder()
                .consumerARN(consumerArn)
                .shardId("shardId-000000000000")
                .startingPosition(StartingPosition.builder()
                        .type(ShardIteratorType.TRIM_HORIZON)
                        .build())
                .build();

        SubscribeToShardResponseHandler responseHandler = SubscribeToShardResponseHandler.builder()
                .onError(error::set)
                .subscriber(event -> {
                    if (event instanceof SubscribeToShardEvent) {
                        SubscribeToShardEvent shardEvent = (SubscribeToShardEvent) event;
                        receivedRecords.addAll(shardEvent.records());
                        continuationSeqNum.set(shardEvent.continuationSequenceNumber());
                        millisBehindLatest.set(shardEvent.millisBehindLatest());
                        if (!receivedRecords.isEmpty()) {
                            latch.countDown();
                        }
                    }
                })
                .build();

        asyncClient.subscribeToShard(subscribeRequest, responseHandler).join();
        assertTrue(latch.await(10, TimeUnit.SECONDS), "Timed out waiting for events");
        assertNull(error.get(), "Event stream error: " + error.get());
        assertFalse(receivedRecords.isEmpty(), "Should have received at least one record");

        // Verify record data
        boolean found = receivedRecords.stream()
                .anyMatch(r -> "subscribe-test".equals(r.data().asUtf8String()));
        assertTrue(found, "Expected to find 'subscribe-test' record");

        // Verify metadata fields (parity with Rust SDK tests)
        assertNotNull(continuationSeqNum.get(), "continuationSequenceNumber should be set");
        assertFalse(continuationSeqNum.get().isEmpty(), "continuationSequenceNumber should not be empty");
        assertNotNull(millisBehindLatest.get(), "millisBehindLatest should be set");
        assertTrue(millisBehindLatest.get() >= 0, "millisBehindLatest should be non-negative");

        // Deregister consumer
        client.deregisterStreamConsumer(DeregisterStreamConsumerRequest.builder()
                .consumerARN(consumerArn)
                .build());
    }

    @Test @Order(9)
    void deleteStream() {
        client.deleteStream(DeleteStreamRequest.builder()
                .streamName(STREAM_NAME)
                .build());
    }
}
