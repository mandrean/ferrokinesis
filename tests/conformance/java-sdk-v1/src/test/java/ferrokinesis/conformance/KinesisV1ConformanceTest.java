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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

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
}
