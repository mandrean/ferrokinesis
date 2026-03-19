package ferrokinesis.conformance;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.*;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class KinesisV1ConformanceTest {

    static {
        // Disable CBOR protocol — the mock server's CBOR support is incomplete
        System.setProperty("com.amazonaws.sdk.disableCbor", "true");
    }

    private static final String ENDPOINT =
            System.getenv().getOrDefault("KINESIS_ENDPOINT", "http://localhost:4567");
    private static final String STREAM_NAME = "java-v1-conformance";
    private static final String TAGS_STREAM = "java-v1-tags";
    private static final String CONFIG_STREAM = "java-v1-config";
    private static final String SHARDS_STREAM = "java-v1-shards";

    private static final AmazonKinesis client = AmazonKinesisClientBuilder.standard()
            .withEndpointConfiguration(new EndpointConfiguration(ENDPOINT, "us-east-1"))
            .withCredentials(new AWSStaticCredentialsProvider(
                    new BasicAWSCredentials("test", "test")))
            .build();

    // Shared across ordered tests — requires @TestMethodOrder(OrderAnnotation) (do not run in parallel)
    private static String putShardId;
    private static String shardIterator;

    private void waitForActive(String streamName) {
        for (int i = 0; i < 30; i++) {
            DescribeStreamResult result = client.describeStream(streamName);
            if ("ACTIVE".equals(result.getStreamDescription().getStreamStatus())) {
                return;
            }
            try { Thread.sleep(200); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        fail("Stream " + streamName + " did not become ACTIVE");
    }

    @Test @Order(1)
    void createStream() {
        client.createStream(STREAM_NAME, 2);
    }

    @Test @Order(2)
    void describeStream() {
        waitForActive(STREAM_NAME);
        DescribeStreamResult result = client.describeStream(STREAM_NAME);
        StreamDescription desc = result.getStreamDescription();
        assertEquals(STREAM_NAME, desc.getStreamName());
        assertEquals("ACTIVE", desc.getStreamStatus());
        assertEquals(2, desc.getShards().size());
    }

    @Test @Order(3)
    void listStreams() {
        ListStreamsResult result = client.listStreams();
        assertTrue(result.getStreamNames().contains(STREAM_NAME));
    }

    @Test @Order(4)
    void putRecord() {
        PutRecordResult result = client.putRecord(
                STREAM_NAME,
                ByteBuffer.wrap("hello from java-v1".getBytes(StandardCharsets.UTF_8)),
                "pk-1");
        assertNotNull(result.getShardId());
        assertFalse(result.getShardId().isEmpty());
        assertNotNull(result.getSequenceNumber());
        assertFalse(result.getSequenceNumber().isEmpty());
        putShardId = result.getShardId();
    }

    @Test @Order(5)
    void putRecords() {
        List<PutRecordsRequestEntry> entries = Arrays.asList(
                new PutRecordsRequestEntry()
                        .withData(ByteBuffer.wrap("batch-0".getBytes(StandardCharsets.UTF_8)))
                        .withPartitionKey("pk-0"),
                new PutRecordsRequestEntry()
                        .withData(ByteBuffer.wrap("batch-1".getBytes(StandardCharsets.UTF_8)))
                        .withPartitionKey("pk-1"),
                new PutRecordsRequestEntry()
                        .withData(ByteBuffer.wrap("batch-2".getBytes(StandardCharsets.UTF_8)))
                        .withPartitionKey("pk-2")
        );
        PutRecordsResult result = client.putRecords(new PutRecordsRequest()
                .withStreamName(STREAM_NAME)
                .withRecords(entries));
        assertEquals(0, result.getFailedRecordCount().intValue());
        assertEquals(3, result.getRecords().size());
    }

    @Test @Order(6)
    void getShardIterator() {
        assertNotNull(putShardId, "putRecord must succeed before getShardIterator");
        GetShardIteratorResult result = client.getShardIterator(
                STREAM_NAME, putShardId, "TRIM_HORIZON");
        assertNotNull(result.getShardIterator());
        assertFalse(result.getShardIterator().isEmpty());
        shardIterator = result.getShardIterator();
    }

    @Test @Order(7)
    void getRecords() {
        assertNotNull(shardIterator, "getShardIterator must succeed before getRecords");
        GetRecordsResult result = client.getRecords(
                new GetRecordsRequest().withShardIterator(shardIterator));
        assertTrue(result.getRecords().size() >= 1);
        String first = StandardCharsets.UTF_8.decode(
                result.getRecords().get(0).getData()).toString();
        assertEquals("hello from java-v1", first);
    }

    @Test @Order(8)
    void deleteStream() {
        client.deleteStream(STREAM_NAME);
    }

    // ── Phase 1: Tagging ─────────────────────────────────────────────────

    @Test @Order(9)
    void createTagsStream() {
        client.createStream(TAGS_STREAM, 1);
        waitForActive(TAGS_STREAM);
    }

    @Test @Order(10)
    void describeLimits() {
        DescribeLimitsResult result = client.describeLimits(new DescribeLimitsRequest());
        assertTrue(result.getShardLimit() > 0);
        assertTrue(result.getOpenShardCount() >= 0);
    }

    @Test @Order(11)
    void describeStreamSummary() {
        DescribeStreamSummaryResult result = client.describeStreamSummary(
                new DescribeStreamSummaryRequest().withStreamName(TAGS_STREAM));
        StreamDescriptionSummary summary = result.getStreamDescriptionSummary();
        assertEquals(TAGS_STREAM, summary.getStreamName());
        assertEquals(1, summary.getOpenShardCount().intValue());
    }

    @Test @Order(12)
    void addTagsToStream() {
        Map<String, String> tags = new HashMap<>();
        tags.put("env", "test");
        tags.put("team", "platform");
        client.addTagsToStream(new AddTagsToStreamRequest()
                .withStreamName(TAGS_STREAM)
                .withTags(tags));
    }

    @Test @Order(13)
    void listTagsForStream() {
        ListTagsForStreamResult result = client.listTagsForStream(
                new ListTagsForStreamRequest().withStreamName(TAGS_STREAM));
        assertEquals(2, result.getTags().size());
    }

    @Test @Order(14)
    void removeTagsFromStream() {
        client.removeTagsFromStream(new RemoveTagsFromStreamRequest()
                .withStreamName(TAGS_STREAM)
                .withTagKeys(Arrays.asList("team")));
    }

    @Test @Order(15)
    void listTagsForStreamAfterRemove() {
        ListTagsForStreamResult result = client.listTagsForStream(
                new ListTagsForStreamRequest().withStreamName(TAGS_STREAM));
        assertEquals(1, result.getTags().size());
        assertEquals("env", result.getTags().get(0).getKey());
        assertEquals("test", result.getTags().get(0).getValue());
    }

    @Test @Order(16)
    void deleteTagsStream() {
        client.deleteStream(TAGS_STREAM);
    }

    // ── Phase 2: Stream Config ───────────────────────────────────────────

    @Test @Order(17)
    void createConfigStream() {
        client.createStream(CONFIG_STREAM, 1);
        waitForActive(CONFIG_STREAM);
    }

    @Test @Order(18)
    void increaseRetention() {
        client.increaseStreamRetentionPeriod(new IncreaseStreamRetentionPeriodRequest()
                .withStreamName(CONFIG_STREAM)
                .withRetentionPeriodHours(48));
    }

    @Test @Order(19)
    void describeAfterRetentionIncrease() {
        waitForActive(CONFIG_STREAM);
        DescribeStreamResult result = client.describeStream(CONFIG_STREAM);
        assertEquals(48, result.getStreamDescription().getRetentionPeriodHours().intValue());
    }

    @Test @Order(20)
    void decreaseRetention() {
        client.decreaseStreamRetentionPeriod(new DecreaseStreamRetentionPeriodRequest()
                .withStreamName(CONFIG_STREAM)
                .withRetentionPeriodHours(24));
    }

    @Test @Order(21)
    void startEncryption() {
        client.startStreamEncryption(new StartStreamEncryptionRequest()
                .withStreamName(CONFIG_STREAM)
                .withEncryptionType("KMS")
                .withKeyId("alias/aws/kinesis"));
    }

    @Test @Order(22)
    void stopEncryption() {
        client.stopStreamEncryption(new StopStreamEncryptionRequest()
                .withStreamName(CONFIG_STREAM)
                .withEncryptionType("KMS")
                .withKeyId("alias/aws/kinesis"));
    }

    @Test @Order(23)
    void enableMonitoring() {
        EnableEnhancedMonitoringResult result = client.enableEnhancedMonitoring(
                new EnableEnhancedMonitoringRequest()
                        .withStreamName(CONFIG_STREAM)
                        .withShardLevelMetrics("IncomingBytes"));
        assertNotNull(result);
    }

    @Test @Order(24)
    void disableMonitoring() {
        DisableEnhancedMonitoringResult result = client.disableEnhancedMonitoring(
                new DisableEnhancedMonitoringRequest()
                        .withStreamName(CONFIG_STREAM)
                        .withShardLevelMetrics("IncomingBytes"));
        assertNotNull(result);
    }

    @Test @Order(25)
    void deleteConfigStream() {
        client.deleteStream(CONFIG_STREAM);
    }

    // ── Phase 3: Shard Management ────────────────────────────────────────

    @Test @Order(26)
    void createShardsStream() {
        client.createStream(SHARDS_STREAM, 2);
        waitForActive(SHARDS_STREAM);
    }

    @Test @Order(27)
    void listShards() {
        ListShardsResult result = client.listShards(
                new ListShardsRequest().withStreamName(SHARDS_STREAM));
        assertEquals(2, result.getShards().size());
    }

    @Test @Order(28)
    void splitShard() {
        List<Shard> shards = client.listShards(
                new ListShardsRequest().withStreamName(SHARDS_STREAM)).getShards();
        Shard shard0 = shards.get(0);
        BigInteger start = new BigInteger(shard0.getHashKeyRange().getStartingHashKey());
        BigInteger end = new BigInteger(shard0.getHashKeyRange().getEndingHashKey());
        BigInteger mid = start.add(end).divide(BigInteger.valueOf(2));
        client.splitShard(new SplitShardRequest()
                .withStreamName(SHARDS_STREAM)
                .withShardToSplit("shardId-000000000000")
                .withNewStartingHashKey(mid.toString()));
    }

    @Test @Order(29)
    void waitAfterSplit() {
        waitForActive(SHARDS_STREAM);
    }

    @Test @Order(30)
    void listShardsAfterSplit() {
        ListShardsResult result = client.listShards(
                new ListShardsRequest().withStreamName(SHARDS_STREAM));
        assertTrue(result.getShards().size() >= 4,
                "Expected >= 4 shards after split, got " + result.getShards().size());
    }

    @Test @Order(31)
    void mergeShards() {
        client.mergeShards(new MergeShardsRequest()
                .withStreamName(SHARDS_STREAM)
                .withShardToMerge("shardId-000000000002")
                .withAdjacentShardToMerge("shardId-000000000003"));
    }

    @Test @Order(32)
    void waitAfterMerge() {
        waitForActive(SHARDS_STREAM);
    }

    @Test @Order(33)
    void updateShardCount() {
        client.updateShardCount(new UpdateShardCountRequest()
                .withStreamName(SHARDS_STREAM)
                .withTargetShardCount(1)
                .withScalingType("UNIFORM_SCALING"));
    }

    @Test @Order(34)
    void waitAfterUpdateShardCount() {
        waitForActive(SHARDS_STREAM);
    }

    @Test @Order(35)
    void deleteShardsStream() {
        client.deleteStream(SHARDS_STREAM);
    }
}
