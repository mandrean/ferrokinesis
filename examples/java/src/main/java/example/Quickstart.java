// ferrokinesis quickstart — AWS SDK for Java (v2)
//
// Prerequisites:
//   mvn compile
//   ferrokinesis running (docker run -p 4567:4567 ghcr.io/mandrean/ferrokinesis)
//
// Usage:
//   mvn compile exec:java -Dexec.mainClass=example.Quickstart
//   KINESIS_ENDPOINT=http://localhost:4567 mvn compile exec:java -Dexec.mainClass=example.Quickstart

package example;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.net.URI;
import java.nio.charset.StandardCharsets;

public class Quickstart {

    public static void main(String[] args) throws Exception {
        String endpoint = System.getenv().getOrDefault("KINESIS_ENDPOINT", "http://localhost:4567");
        String stream = "java-example";

        // Disable CBOR — ferrokinesis speaks JSON, not CBOR
        System.setProperty("aws.cborEnabled", "false");

        KinesisClient client = KinesisClient.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test", "test")))
                .build();

        // Create a stream
        System.out.println("==> CreateStream");
        client.createStream(CreateStreamRequest.builder()
                .streamName(stream).shardCount(2).build());

        // Wait for ACTIVE
        for (int i = 0; i < 30; i++) {
            DescribeStreamResponse resp = client.describeStream(
                    DescribeStreamRequest.builder().streamName(stream).build());
            if (resp.streamDescription().streamStatus() == StreamStatus.ACTIVE) break;
            Thread.sleep(200);
        }

        // Put a record
        System.out.println("==> PutRecord");
        PutRecordResponse put = client.putRecord(PutRecordRequest.builder()
                .streamName(stream)
                .data(SdkBytes.fromUtf8String("hello world"))
                .partitionKey("pk1")
                .build());

        // Get records
        System.out.println("==> GetRecords");
        GetShardIteratorResponse iterResp = client.getShardIterator(
                GetShardIteratorRequest.builder()
                        .streamName(stream)
                        .shardId(put.shardId())
                        .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                        .build());

        GetRecordsResponse records = client.getRecords(GetRecordsRequest.builder()
                .shardIterator(iterResp.shardIterator())
                .build());

        for (Record r : records.records()) {
            System.out.println(r.partitionKey() + ": " + r.data().asUtf8String());
        }

        // Clean up
        System.out.println("==> DeleteStream");
        client.deleteStream(DeleteStreamRequest.builder().streamName(stream).build());

        System.out.println("Done.");
    }
}
