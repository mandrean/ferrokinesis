package ferrokinesis.conformance;

import org.junit.jupiter.api.AfterAll;
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
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DeleteStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.coordinator.CoordinatorConfig.ClientVersionConfig;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.retrieval.fanout.FanOutConfig;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class KclV2IntegrationTest {

    static {
        // Disable CBOR protocol so the SDK uses JSON for Kinesis requests.
        // Ferrokinesis's CBOR event-stream encoding for SubscribeToShard is not yet
        // fully compatible with the Java SDK v2's CBOR deserializer; the JSON path
        // is proven by the java-sdk-v2 conformance suite.
        System.setProperty("aws.cborEnabled", "false");
    }

    private static final String KINESIS_ENDPOINT =
            System.getenv().getOrDefault("KINESIS_ENDPOINT", "http://localhost:4567");
    private static final String DYNAMODB_ENDPOINT =
            System.getenv().getOrDefault("DYNAMODB_ENDPOINT", "http://localhost:8000");
    private static final String REGION = "us-east-1";
    // UUID suffix avoids stream-name collisions if multiple CI jobs or local runs overlap.
    private static final String RUN_ID = UUID.randomUUID().toString().substring(0, 8);
    private static final String STREAM_NAME = "kcl-v2-integration-" + RUN_ID;
    private static final String APPLICATION_NAME = "kcl-v2-test-app-" + RUN_ID;
    private static final int SHARD_COUNT = 2;
    private static final int RECORD_COUNT = 20;

    private static final StaticCredentialsProvider CREDENTIALS =
            StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));

    // KinesisAsyncClient used by both KCL and test setup.
    // CRITICAL: Force HTTP/1.1. Ferrokinesis runs plain HTTP/1.1 (Axum, no TLS).
    // AWS SDK v2's Netty client defaults to HTTP/2 for Kinesis (especially SubscribeToShard).
    // HTTP/2 requires TLS (ALPN) or h2c — Axum supports neither without TLS.
    // Forcing HTTP/1.1 makes the Netty client use chunked transfer encoding for the
    // SubscribeToShard event stream, which ferrokinesis supports correctly.
    private static final KinesisAsyncClient kinesisAsyncClient = KinesisAsyncClient.builder()
            .endpointOverride(URI.create(KINESIS_ENDPOINT))
            .region(Region.of(REGION))
            .credentialsProvider(CREDENTIALS)
            .httpClient(NettyNioAsyncHttpClient.builder()
                    .protocol(Protocol.HTTP1_1)
                    .build())
            .build();

    // DynamoDbAsyncClient for KCL lease management. KCL 3.x requires an async client.
    private static final DynamoDbAsyncClient dynamoAsyncClient = DynamoDbAsyncClient.builder()
            .endpointOverride(URI.create(DYNAMODB_ENDPOINT))
            .region(Region.of(REGION))
            .credentialsProvider(CREDENTIALS)
            .build();

    // Sync DynamoDB client for lease table verification — avoids .join() boilerplate in assertions.
    private static final DynamoDbClient dynamoSyncClient = DynamoDbClient.builder()
            .endpointOverride(URI.create(DYNAMODB_ENDPOINT))
            .region(Region.of(REGION))
            .credentialsProvider(CREDENTIALS)
            .build();

    // CloudWatchAsyncClient required by ConfigsBuilder API even when MetricsLevel.NONE is set.
    // Port 19999 is an arbitrary unused local port — this client is never actually called with
    // MetricsLevel.NONE. If ferrokinesis gains a CloudWatch-compatible endpoint, wire it here
    // and raise MetricsLevel to SUMMARY.
    private static final CloudWatchAsyncClient cloudWatchClient = CloudWatchAsyncClient.builder()
            .endpointOverride(URI.create("http://localhost:19999"))
            .region(Region.of(REGION))
            .credentialsProvider(CREDENTIALS)
            .build();

    private static Scheduler scheduler;
    private static Thread schedulerThread;
    private static boolean cleanedUp = false;

    @Test
    @Order(1)
    void createStreamAndPutRecords() {
        kinesisAsyncClient.createStream(CreateStreamRequest.builder()
                .streamName(STREAM_NAME)
                .shardCount(SHARD_COUNT)
                .build()).join();

        waitForStreamActive();

        for (int i = 0; i < RECORD_COUNT; i++) {
            kinesisAsyncClient.putRecord(PutRecordRequest.builder()
                    .streamName(STREAM_NAME)
                    .partitionKey("pk-" + i)
                    .data(SdkBytes.fromUtf8String("kcl-v2-record-" + i))
                    .build()).join();
        }
    }

    @Test
    @Order(2)
    void runKclSchedulerAndVerify() throws Exception {
        TestShardRecordProcessor.reset(RECORD_COUNT);

        String workerId = "worker-" + UUID.randomUUID();

        // KCL defaults to LATEST for new applications. We must explicitly set TRIM_HORIZON
        // so the subscriber starts from the beginning of the stream and picks up all records
        // put in test 1.
        ConfigsBuilder configsBuilder = new ConfigsBuilder(
                STREAM_NAME,
                APPLICATION_NAME,
                kinesisAsyncClient,
                dynamoAsyncClient,
                cloudWatchClient,
                workerId,
                new TestShardRecordProcessorFactory()
        );

        // FanOutConfig drives the enhanced fan-out path. KCL 3.x will internally:
        //   1. DescribeStreamSummary — get stream ARN and status
        //   2. ListShards — enumerate shards
        //   3. RegisterStreamConsumer — register the consumer (named APPLICATION_NAME)
        //   4. DescribeStreamConsumer — poll until ACTIVE (ferrokinesis transitions in ~500ms)
        //   5. SubscribeToShard — open an event stream per shard
        FanOutConfig fanOutConfig = new FanOutConfig(kinesisAsyncClient)
                .streamName(STREAM_NAME)
                .applicationName(APPLICATION_NAME);

        Scheduler localScheduler = new Scheduler(
                configsBuilder.checkpointConfig(),
                // CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X: use KCL 2.x lease assignment algorithm.
                // KCL 3.x's default algorithm creates WorkerMetricStats and CoordinatorState DynamoDB
                // tables and uses a new load balancer that causes immediate lease loss in single-worker
                // test scenarios. The 2.x-compatible mode avoids this without sacrificing fan-out coverage.
                configsBuilder.coordinatorConfig()
                        .clientVersionConfig(ClientVersionConfig.CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X),
                configsBuilder.leaseManagementConfig(),
                configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig()
                        // MetricsLevel.NONE: no CloudWatch emulator. Raise to SUMMARY if ferrokinesis
                        // gains a CloudWatch-compatible endpoint.
                        .metricsLevel(MetricsLevel.NONE),
                configsBuilder.processorConfig(),
                configsBuilder.retrievalConfig()
                        .initialPositionInStreamExtended(
                                InitialPositionInStreamExtended.newInitialPosition(
                                        InitialPositionInStream.TRIM_HORIZON))
                        .retrievalSpecificConfig(fanOutConfig)
        );

        scheduler = localScheduler;

        schedulerThread = new Thread(localScheduler);
        schedulerThread.setDaemon(true);
        schedulerThread.setName("kcl-v2-scheduler");
        schedulerThread.start();

        // awaitRecords(120) is the catch-all backstop for all KCL-internal polling:
        // DescribeStreamConsumer (until ACTIVE), SubscribeToShard event delivery, and the
        // processRecords() → checkpoint() chain. If any step hangs, the latch times out here.
        boolean allReceived = TestShardRecordProcessor.awaitRecords(120);
        assertTrue(allReceived,
                "KCL 3.x did not process all " + RECORD_COUNT + " records within 120 seconds");

        assertTrue(TestShardRecordProcessor.wasInitializeCalled(),
                "KCL 3.x never called initialize() — shard assignment may have failed");

        List<software.amazon.kinesis.retrieval.KinesisClientRecord> records =
                TestShardRecordProcessor.getRecords();
        assertEquals(RECORD_COUNT, records.size(),
                "Expected exactly " + RECORD_COUNT + " records, got " + records.size());

        Set<String> payloads = new HashSet<>();
        for (software.amazon.kinesis.retrieval.KinesisClientRecord r : records) {
            payloads.add(StandardCharsets.UTF_8.decode(r.data()).toString());
        }
        for (int i = 0; i < RECORD_COUNT; i++) {
            assertTrue(payloads.contains("kcl-v2-record-" + i),
                    "Missing record payload: kcl-v2-record-" + i);
        }

        for (software.amazon.kinesis.retrieval.KinesisClientRecord r : records) {
            assertNotNull(r.sequenceNumber(), "Record has null sequenceNumber");
            assertFalse(r.sequenceNumber().isEmpty(), "Record has empty sequenceNumber");
            assertNotNull(r.partitionKey(), "Record has null partitionKey");
            assertNotNull(r.approximateArrivalTimestamp(),
                    "Record has null approximateArrivalTimestamp");
        }

        // Verify records came from multiple shards by checking distinct sequence number prefixes.
        // Ferrokinesis encodes the shard index as a hex prefix in the sequence number.
        Set<String> shardPrefixes = new HashSet<>();
        for (software.amazon.kinesis.retrieval.KinesisClientRecord r : records) {
            // The shard hex prefix is the first 8 characters of the sequence number.
            shardPrefixes.add(r.sequenceNumber().substring(0, 8));
        }
        assertTrue(shardPrefixes.size() >= 2,
                "Expected records from at least 2 shards, but only found prefixes: " + shardPrefixes);
    }

    @Test
    @Order(3)
    void verifyLeaseTable() {
        // KCL 3.x uses applicationName as the DynamoDB table name (same as KCL 1.x).
        // By the time this test runs the latch has fired. Because latch.countDown() now fires
        // only after checkpoint() returns, we are guaranteed the lease entry is committed to
        // DynamoDB Local before this scan runs.
        ScanResponse result = dynamoSyncClient.scan(ScanRequest.builder()
                .tableName(APPLICATION_NAME)
                .attributesToGet("leaseKey", "checkpoint", "leaseOwner")
                .build());

        assertFalse(result.items().isEmpty(),
                "Lease table is empty — KCL 3.x did not create leases");

        for (Map<String, AttributeValue> item : result.items()) {
            AttributeValue checkpoint = item.get("checkpoint");
            if (checkpoint != null && checkpoint.s() != null) {
                String seq = checkpoint.s();
                // Valid checkpoint values: sentinels or a non-empty sequence number.
                if (!seq.equals("TRIM_HORIZON") && !seq.equals("LATEST") && !seq.equals("SHARD_END")) {
                    assertFalse(seq.isEmpty(),
                            "Lease has empty checkpoint sequence number for shard: " +
                                    item.get("leaseKey").s());
                }
            }
        }
    }

    @Test
    @Order(4)
    void cleanup() throws Exception {
        if (scheduler != null) {
            Future<Boolean> shutdownFuture = scheduler.startGracefulShutdown();
            try {
                shutdownFuture.get(30, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                System.err.println("[KCLv2] Graceful shutdown timed out after 30s");
            } catch (Exception e) {
                System.err.println("[KCLv2] Graceful shutdown failed: " + e.getMessage());
            }
        }
        if (schedulerThread != null) {
            schedulerThread.join(10_000);
            assertFalse(schedulerThread.isAlive(),
                    "Scheduler thread still alive 10 s after graceful shutdown — possible hang");
        }
        kinesisAsyncClient.deleteStream(DeleteStreamRequest.builder()
                .streamName(STREAM_NAME)
                .build()).join();
        cleanedUp = true;
    }

    @AfterAll
    static void safetyNetCleanup() {
        if (cleanedUp) {
            return;
        }
        if (scheduler != null) {
            try {
                Future<Boolean> f = scheduler.startGracefulShutdown();
                f.get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                System.err.println("[KCLv2] Safety-net scheduler shutdown failed: " + e.getMessage());
            }
        }
        if (schedulerThread != null) {
            schedulerThread.interrupt();
        }
        try {
            kinesisAsyncClient.deleteStream(DeleteStreamRequest.builder()
                    .streamName(STREAM_NAME)
                    .build()).join();
        } catch (Exception e) {
            System.err.println("[KCLv2] Safety-net stream delete failed: " + e.getMessage());
        }
    }

    private void waitForStreamActive() {
        for (int i = 0; i < 30; i++) {
            DescribeStreamSummaryResponse resp = kinesisAsyncClient.describeStreamSummary(
                    DescribeStreamSummaryRequest.builder()
                            .streamName(STREAM_NAME)
                            .build()).join();
            if (resp.streamDescriptionSummary().streamStatus() == StreamStatus.ACTIVE) {
                return;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        fail("Stream " + STREAM_NAME + " did not become ACTIVE within 6 seconds");
    }
}
