package ferrokinesis.conformance;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.Record;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class KclV1IntegrationTest {

    static {
        System.setProperty("com.amazonaws.sdk.disableCbor", "true");
    }

    private static final String KINESIS_ENDPOINT =
            System.getenv().getOrDefault("KINESIS_ENDPOINT", "http://localhost:4567");
    private static final String DYNAMODB_ENDPOINT =
            System.getenv().getOrDefault("DYNAMODB_ENDPOINT", "http://localhost:8000");
    private static final String REGION = "us-east-1";
    private static final String STREAM_NAME = "kcl-v1-integration";
    private static final String APPLICATION_NAME = "kcl-v1-test-app";
    private static final int SHARD_COUNT = 2;
    private static final int RECORD_COUNT = 10;

    private static final AWSStaticCredentialsProvider CREDENTIALS =
            new AWSStaticCredentialsProvider(new BasicAWSCredentials("test", "test"));

    private static final AmazonKinesis kinesisClient = AmazonKinesisClientBuilder.standard()
            .withEndpointConfiguration(new EndpointConfiguration(KINESIS_ENDPOINT, REGION))
            .withCredentials(CREDENTIALS)
            .build();

    private static final AmazonDynamoDB dynamoClient = AmazonDynamoDBClientBuilder.standard()
            .withEndpointConfiguration(new EndpointConfiguration(DYNAMODB_ENDPOINT, REGION))
            .withCredentials(CREDENTIALS)
            .build();

    private static Worker worker;
    private static ExecutorService executor;
    private static boolean cleanedUp = false;

    private void waitForActive(String streamName) {
        for (int i = 0; i < 30; i++) {
            DescribeStreamResult result = kinesisClient.describeStream(streamName);
            if ("ACTIVE".equals(result.getStreamDescription().getStreamStatus())) {
                return;
            }
            try { Thread.sleep(200); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        fail("Stream " + streamName + " did not become ACTIVE");
    }

    @Test @Order(1)
    void createStreamAndPutRecords() {
        kinesisClient.createStream(STREAM_NAME, SHARD_COUNT);
        waitForActive(STREAM_NAME);

        for (int i = 0; i < RECORD_COUNT; i++) {
            kinesisClient.putRecord(new PutRecordRequest()
                    .withStreamName(STREAM_NAME)
                    .withPartitionKey("pk-" + i)
                    .withData(ByteBuffer.wrap(("kcl-record-" + i).getBytes(StandardCharsets.UTF_8))));
        }
    }

    @Test @Order(2)
    void runKclWorkerAndVerify() throws Exception {
        TestRecordProcessor.reset(RECORD_COUNT);

        String workerId = "worker-" + UUID.randomUUID();
        KinesisClientLibConfiguration config = new KinesisClientLibConfiguration(
                APPLICATION_NAME,
                STREAM_NAME,
                CREDENTIALS,
                workerId)
                .withRegionName(REGION)
                .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)
                .withIdleTimeBetweenReadsInMillis(500)
                .withParentShardPollIntervalMillis(500)
                .withFailoverTimeMillis(10000);

        // Pass pre-configured clients to avoid endpoint/protocol issues
        // with KCL's withKinesisEndpoint/withDynamoDBEndpoint
        worker = new Worker.Builder()
                .recordProcessorFactory(new TestRecordProcessorFactory())
                .config(config)
                .kinesisClient(kinesisClient)
                .dynamoDBClient(dynamoClient)
                .metricsFactory(new NullMetricsFactory())
                .build();

        executor = Executors.newSingleThreadExecutor();
        executor.submit(worker);

        boolean allReceived = TestRecordProcessor.awaitRecords(60);
        assertTrue(allReceived, "KCL did not process all " + RECORD_COUNT + " records within 60 seconds");

        assertTrue(TestRecordProcessor.wasInitializeCalled(),
                "KCL never called initialize — shard enumeration may have failed");

        List<Record> records = TestRecordProcessor.getRecords();
        assertEquals(RECORD_COUNT, records.size(), "Expected exactly " + RECORD_COUNT + " records");

        Set<String> payloads = records.stream()
                .map(r -> StandardCharsets.UTF_8.decode(r.getData()).toString())
                .collect(Collectors.toSet());
        for (int i = 0; i < RECORD_COUNT; i++) {
            assertTrue(payloads.contains("kcl-record-" + i),
                    "Missing record payload: kcl-record-" + i);
        }

        for (Record record : records) {
            assertNotNull(record.getSequenceNumber(), "Record has null sequence number");
            assertFalse(record.getSequenceNumber().isEmpty(), "Record has empty sequence number");
            assertNotNull(record.getPartitionKey(), "Record has null partition key");
        }
    }

    @Test @Order(3)
    void verifyLeaseTable() {
        ScanResult result = dynamoClient.scan(APPLICATION_NAME, List.of(
                "leaseKey", "checkpoint", "leaseOwner"));

        assertFalse(result.getItems().isEmpty(), "Lease table is empty — KCL did not create leases");

        for (Map<String, AttributeValue> item : result.getItems()) {
            AttributeValue checkpoint = item.get("checkpoint");
            if (checkpoint != null && checkpoint.getS() != null) {
                String seq = checkpoint.getS();
                if (!seq.equals("TRIM_HORIZON") && !seq.equals("LATEST")) {
                    assertFalse(seq.isEmpty(),
                            "Lease has empty checkpoint sequence number for shard: " +
                                    item.get("leaseKey").getS());
                }
            }
        }
    }

    @Test @Order(4)
    void cleanup() throws Exception {
        if (worker != null) {
            worker.shutdown();
        }
        if (executor != null) {
            executor.shutdownNow();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
        kinesisClient.deleteStream(STREAM_NAME);
        cleanedUp = true;
    }

    @AfterAll
    static void safetyNetCleanup() {
        if (cleanedUp) {
            return;
        }
        try {
            if (worker != null) worker.shutdown();
        } catch (Exception e) {
            System.err.println("Safety-net worker shutdown failed: " + e.getMessage());
        }
        try {
            if (executor != null) {
                executor.shutdownNow();
                executor.awaitTermination(5, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            System.err.println("Safety-net executor shutdown failed: " + e.getMessage());
        }
        try {
            kinesisClient.deleteStream(STREAM_NAME);
        } catch (Exception e) {
            System.err.println("Safety-net stream delete failed: " + e.getMessage());
        }
    }
}
