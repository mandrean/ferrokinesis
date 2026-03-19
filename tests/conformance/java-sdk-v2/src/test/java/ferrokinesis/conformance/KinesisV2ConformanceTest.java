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

import java.math.BigInteger;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
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
    private static String tagsStreamArn;
    private static String configStreamArn;
    private static String consumersStreamArn;
    private static String consumerArn;
    private static String policiesStreamArn;

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
        List<software.amazon.awssdk.services.kinesis.model.Record> receivedRecords = java.util.Collections.synchronizedList(new ArrayList<>());
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

        CompletableFuture<Void> subscription =
                asyncClient.subscribeToShard(subscribeRequest, responseHandler);
        assertTrue(latch.await(10, TimeUnit.SECONDS), "Timed out waiting for events");
        assertNull(error.get(), "Event stream error: " + error.get());
        subscription.cancel(true);
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

    // ========================================================================
    // Phase 1: Tagging — stream: "java-v2-tags" (1 shard)
    // ========================================================================

    @Test @Order(10)
    void createTagsStream() {
        client.createStream(CreateStreamRequest.builder()
                .streamName("java-v2-tags")
                .shardCount(1)
                .build());
        waitForActive("java-v2-tags");
    }

    @Test @Order(11)
    void describeLimits() {
        DescribeLimitsResponse resp = client.describeLimits(DescribeLimitsRequest.builder().build());
        assertTrue(resp.shardLimit() > 0, "shardLimit should be > 0");
        assertTrue(resp.openShardCount() >= 0, "openShardCount should be >= 0");
    }

    @Test @Order(12)
    void describeStreamSummary() {
        DescribeStreamSummaryResponse resp = client.describeStreamSummary(
                DescribeStreamSummaryRequest.builder()
                        .streamName("java-v2-tags")
                        .build());
        StreamDescriptionSummary summary = resp.streamDescriptionSummary();
        assertEquals("java-v2-tags", summary.streamName());
        assertEquals(1, summary.openShardCount().intValue());
    }

    @Test @Order(13)
    void addTagsToStream() {
        client.addTagsToStream(AddTagsToStreamRequest.builder()
                .streamName("java-v2-tags")
                .tags(Map.of("env", "test", "team", "platform"))
                .build());
    }

    @Test @Order(14)
    void listTagsForStream() {
        ListTagsForStreamResponse resp = client.listTagsForStream(
                ListTagsForStreamRequest.builder()
                        .streamName("java-v2-tags")
                        .build());
        assertEquals(2, resp.tags().size());
    }

    @Test @Order(15)
    void removeTagsFromStream() {
        client.removeTagsFromStream(RemoveTagsFromStreamRequest.builder()
                .streamName("java-v2-tags")
                .tagKeys(List.of("team"))
                .build());
    }

    @Test @Order(16)
    void listTagsForStreamAfterRemove() {
        ListTagsForStreamResponse resp = client.listTagsForStream(
                ListTagsForStreamRequest.builder()
                        .streamName("java-v2-tags")
                        .build());
        assertEquals(1, resp.tags().size());
    }

    @Test @Order(17)
    void getTagsStreamArn() {
        DescribeStreamResponse resp = client.describeStream(
                DescribeStreamRequest.builder()
                        .streamName("java-v2-tags")
                        .build());
        tagsStreamArn = resp.streamDescription().streamARN();
        assertNotNull(tagsStreamArn);
        assertFalse(tagsStreamArn.isEmpty());
    }

    @Test @Order(18)
    void tagResource() {
        assertNotNull(tagsStreamArn, "tagsStreamArn must be set");
        client.tagResource(TagResourceRequest.builder()
                .resourceARN(tagsStreamArn)
                .tags(Map.of("version", "1"))
                .build());
    }

    @Test @Order(19)
    void listTagsForResource() {
        assertNotNull(tagsStreamArn, "tagsStreamArn must be set");
        ListTagsForResourceResponse resp = client.listTagsForResource(
                ListTagsForResourceRequest.builder()
                        .resourceARN(tagsStreamArn)
                        .build());
        boolean found = resp.tags().stream()
                .anyMatch(t -> "version".equals(t.key()));
        assertTrue(found, "Expected to find 'version' tag");
    }

    @Test @Order(20)
    void untagResource() {
        assertNotNull(tagsStreamArn, "tagsStreamArn must be set");
        client.untagResource(UntagResourceRequest.builder()
                .resourceARN(tagsStreamArn)
                .tagKeys(List.of("version"))
                .build());
    }

    @Test @Order(21)
    void deleteTagsStream() {
        client.deleteStream(DeleteStreamRequest.builder()
                .streamName("java-v2-tags")
                .build());
    }

    // ========================================================================
    // Phase 2: Stream Config — stream: "java-v2-config" (1 shard)
    // ========================================================================

    @Test @Order(22)
    void createConfigStream() {
        client.createStream(CreateStreamRequest.builder()
                .streamName("java-v2-config")
                .shardCount(1)
                .build());
        waitForActive("java-v2-config");
    }

    @Test @Order(23)
    void increaseRetention() {
        client.increaseStreamRetentionPeriod(IncreaseStreamRetentionPeriodRequest.builder()
                .streamName("java-v2-config")
                .retentionPeriodHours(48)
                .build());
    }

    @Test @Order(24)
    void describeAfterRetentionIncrease() {
        DescribeStreamResponse resp = client.describeStream(
                DescribeStreamRequest.builder()
                        .streamName("java-v2-config")
                        .build());
        assertEquals(48, resp.streamDescription().retentionPeriodHours().intValue());
    }

    @Test @Order(25)
    void decreaseRetention() {
        client.decreaseStreamRetentionPeriod(DecreaseStreamRetentionPeriodRequest.builder()
                .streamName("java-v2-config")
                .retentionPeriodHours(24)
                .build());
    }

    @Test @Order(26)
    void startEncryption() {
        client.startStreamEncryption(StartStreamEncryptionRequest.builder()
                .streamName("java-v2-config")
                .encryptionType(EncryptionType.KMS)
                .keyId("alias/aws/kinesis")
                .build());
    }

    @Test @Order(27)
    void stopEncryption() {
        client.stopStreamEncryption(StopStreamEncryptionRequest.builder()
                .streamName("java-v2-config")
                .encryptionType(EncryptionType.KMS)
                .keyId("alias/aws/kinesis")
                .build());
    }

    @Test @Order(28)
    void enableMonitoring() {
        client.enableEnhancedMonitoring(EnableEnhancedMonitoringRequest.builder()
                .streamName("java-v2-config")
                .shardLevelMetrics(MetricsName.INCOMING_BYTES)
                .build());
    }

    @Test @Order(29)
    void disableMonitoring() {
        client.disableEnhancedMonitoring(DisableEnhancedMonitoringRequest.builder()
                .streamName("java-v2-config")
                .shardLevelMetrics(MetricsName.INCOMING_BYTES)
                .build());
    }

    @Test @Order(30)
    void updateStreamMode() {
        DescribeStreamResponse descResp = client.describeStream(
                DescribeStreamRequest.builder()
                        .streamName("java-v2-config")
                        .build());
        configStreamArn = descResp.streamDescription().streamARN();
        assertNotNull(configStreamArn);

        client.updateStreamMode(UpdateStreamModeRequest.builder()
                .streamARN(configStreamArn)
                .streamModeDetails(StreamModeDetails.builder()
                        .streamMode(StreamMode.ON_DEMAND)
                        .build())
                .build());
    }

    @Test @Order(31)
    void describeAfterModeChange() {
        waitForActive("java-v2-config");
        DescribeStreamSummaryResponse resp = client.describeStreamSummary(
                DescribeStreamSummaryRequest.builder()
                        .streamName("java-v2-config")
                        .build());
        assertEquals(StreamMode.ON_DEMAND,
                resp.streamDescriptionSummary().streamModeDetails().streamMode());
    }

    @Test @Order(32)
    void deleteConfigStream() {
        client.deleteStream(DeleteStreamRequest.builder()
                .streamName("java-v2-config")
                .build());
    }

    // ========================================================================
    // Phase 3: Shard Management — stream: "java-v2-shards" (2 shards)
    // ========================================================================

    @Test @Order(33)
    void createShardsStream() {
        client.createStream(CreateStreamRequest.builder()
                .streamName("java-v2-shards")
                .shardCount(2)
                .build());
        waitForActive("java-v2-shards");
    }

    @Test @Order(34)
    void listShards() {
        ListShardsResponse resp = client.listShards(ListShardsRequest.builder()
                .streamName("java-v2-shards")
                .build());
        assertEquals(2, resp.shards().size());
    }

    @Test @Order(35)
    void splitShard() {
        // Get current shards to compute the new starting hash key
        ListShardsResponse listResp = client.listShards(ListShardsRequest.builder()
                .streamName("java-v2-shards")
                .build());
        Shard shard0 = listResp.shards().stream()
                .filter(s -> s.shardId().equals("shardId-000000000000"))
                .findFirst()
                .orElseThrow();

        BigInteger startHash = new BigInteger(shard0.hashKeyRange().startingHashKey());
        BigInteger endHash = new BigInteger(shard0.hashKeyRange().endingHashKey());
        BigInteger midHash = startHash.add(endHash).divide(BigInteger.valueOf(2));

        client.splitShard(SplitShardRequest.builder()
                .streamName("java-v2-shards")
                .shardToSplit("shardId-000000000000")
                .newStartingHashKey(midHash.toString())
                .build());
    }

    @Test @Order(36)
    void waitAfterSplit() {
        waitForActive("java-v2-shards");
    }

    @Test @Order(37)
    void listShardsAfterSplit() {
        ListShardsResponse resp = client.listShards(ListShardsRequest.builder()
                .streamName("java-v2-shards")
                .build());
        assertTrue(resp.shards().size() >= 4,
                "Expected >= 4 shards after split, got " + resp.shards().size());
    }

    @Test @Order(38)
    void mergeShards() {
        client.mergeShards(MergeShardsRequest.builder()
                .streamName("java-v2-shards")
                .shardToMerge("shardId-000000000002")
                .adjacentShardToMerge("shardId-000000000003")
                .build());
    }

    @Test @Order(39)
    void waitAfterMerge() {
        waitForActive("java-v2-shards");
    }

    @Test @Order(40)
    void updateShardCount() {
        client.updateShardCount(UpdateShardCountRequest.builder()
                .streamName("java-v2-shards")
                .targetShardCount(1)
                .scalingType(ScalingType.UNIFORM_SCALING)
                .build());
    }

    @Test @Order(41)
    void waitAfterUpdateShardCount() {
        waitForActive("java-v2-shards");
    }

    @Test @Order(42)
    void deleteShardsStream() {
        client.deleteStream(DeleteStreamRequest.builder()
                .streamName("java-v2-shards")
                .build());
    }

    // ========================================================================
    // Phase 4: Consumers — stream: "java-v2-consumers" (1 shard)
    // ========================================================================

    @Test @Order(43)
    void createConsumersStream() {
        client.createStream(CreateStreamRequest.builder()
                .streamName("java-v2-consumers")
                .shardCount(1)
                .build());
        waitForActive("java-v2-consumers");
    }

    @Test @Order(44)
    void getConsumersStreamArn() {
        DescribeStreamResponse resp = client.describeStream(
                DescribeStreamRequest.builder()
                        .streamName("java-v2-consumers")
                        .build());
        consumersStreamArn = resp.streamDescription().streamARN();
        assertNotNull(consumersStreamArn);
        assertFalse(consumersStreamArn.isEmpty());
    }

    @Test @Order(45)
    void registerConsumer() {
        assertNotNull(consumersStreamArn, "consumersStreamArn must be set");
        RegisterStreamConsumerResponse resp = client.registerStreamConsumer(
                RegisterStreamConsumerRequest.builder()
                        .streamARN(consumersStreamArn)
                        .consumerName("java-v2-test-consumer")
                        .build());
        consumerArn = resp.consumer().consumerARN();
        assertNotNull(consumerArn);
        assertFalse(consumerArn.isEmpty());
    }

    @Test @Order(46)
    void describeConsumer() {
        assertNotNull(consumerArn, "consumerArn must be set");
        DescribeStreamConsumerResponse resp = client.describeStreamConsumer(
                DescribeStreamConsumerRequest.builder()
                        .consumerARN(consumerArn)
                        .build());
        assertEquals("java-v2-test-consumer", resp.consumerDescription().consumerName());
    }

    @Test @Order(47)
    void listConsumers() {
        assertNotNull(consumersStreamArn, "consumersStreamArn must be set");
        ListStreamConsumersResponse resp = client.listStreamConsumers(
                ListStreamConsumersRequest.builder()
                        .streamARN(consumersStreamArn)
                        .build());
        boolean found = resp.consumers().stream()
                .anyMatch(c -> "java-v2-test-consumer".equals(c.consumerName()));
        assertTrue(found, "Expected to find 'java-v2-test-consumer' in consumers list");
    }

    @Test @Order(48)
    void deregisterConsumer() {
        assertNotNull(consumerArn, "consumerArn must be set");
        client.deregisterStreamConsumer(DeregisterStreamConsumerRequest.builder()
                .consumerARN(consumerArn)
                .build());
    }

    @Test @Order(49)
    void deleteConsumersStream() {
        client.deleteStream(DeleteStreamRequest.builder()
                .streamName("java-v2-consumers")
                .build());
    }

    // ========================================================================
    // Phase 5: Policies — stream: "java-v2-policies" (1 shard)
    // ========================================================================

    @Test @Order(50)
    void createPoliciesStream() {
        client.createStream(CreateStreamRequest.builder()
                .streamName("java-v2-policies")
                .shardCount(1)
                .build());
        waitForActive("java-v2-policies");
    }

    @Test @Order(51)
    void getPoliciesStreamArn() {
        DescribeStreamResponse resp = client.describeStream(
                DescribeStreamRequest.builder()
                        .streamName("java-v2-policies")
                        .build());
        policiesStreamArn = resp.streamDescription().streamARN();
        assertNotNull(policiesStreamArn);
        assertFalse(policiesStreamArn.isEmpty());
    }

    @Test @Order(52)
    void putResourcePolicy() {
        assertNotNull(policiesStreamArn, "policiesStreamArn must be set");
        String policy = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Sid\":\"test\",\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"*\"},\"Action\":\"kinesis:GetRecords\",\"Resource\":\"*\"}]}";
        client.putResourcePolicy(PutResourcePolicyRequest.builder()
                .resourceARN(policiesStreamArn)
                .policy(policy)
                .build());
    }

    @Test @Order(53)
    void getResourcePolicy() {
        assertNotNull(policiesStreamArn, "policiesStreamArn must be set");
        GetResourcePolicyResponse resp = client.getResourcePolicy(
                GetResourcePolicyRequest.builder()
                        .resourceARN(policiesStreamArn)
                        .build());
        assertNotNull(resp.policy());
        assertFalse(resp.policy().isEmpty(), "Policy should not be empty after putResourcePolicy");
    }

    @Test @Order(54)
    void deleteResourcePolicy() {
        assertNotNull(policiesStreamArn, "policiesStreamArn must be set");
        client.deleteResourcePolicy(DeleteResourcePolicyRequest.builder()
                .resourceARN(policiesStreamArn)
                .build());
    }

    @Test @Order(55)
    void getResourcePolicyAfterDelete() {
        assertNotNull(policiesStreamArn, "policiesStreamArn must be set");
        GetResourcePolicyResponse resp = client.getResourcePolicy(
                GetResourcePolicyRequest.builder()
                        .resourceARN(policiesStreamArn)
                        .build());
        assertTrue(resp.policy() == null || resp.policy().isEmpty(),
                "Policy should be empty after deleteResourcePolicy");
    }

    @Test @Order(56)
    void deletePoliciesStream() {
        client.deleteStream(DeleteStreamRequest.builder()
                .streamName("java-v2-policies")
                .build());
    }
}
