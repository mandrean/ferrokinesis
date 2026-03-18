package ferrokinesis.conformance;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

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
        GetRecordsResponse resp = client.getRecords(GetRecordsRequest.builder()
                .shardIterator(shardIterator)
                .build());
        assertTrue(resp.records().size() >= 1);
        String first = resp.records().get(0).data().asUtf8String();
        assertEquals("hello from java-v2", first);
    }

    @Test @Order(8)
    void deleteStream() {
        client.deleteStream(DeleteStreamRequest.builder()
                .streamName(STREAM_NAME)
                .build());
    }
}
