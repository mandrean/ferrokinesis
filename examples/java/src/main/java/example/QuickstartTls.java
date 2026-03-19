// ferrokinesis quickstart — AWS SDK for Java (v2) over TLS
//
// Prerequisites:
//   mvn compile
//   ferrokinesis running with TLS:
//     cargo install ferrokinesis --features tls
//     ferrokinesis generate-cert
//     ferrokinesis --tls-cert cert.pem --tls-key key.pem
//
// Usage:
//   mvn compile exec:java -Dexec.mainClass=example.QuickstartTls
//   KINESIS_ENDPOINT=https://localhost:4567 mvn compile exec:java -Dexec.mainClass=example.QuickstartTls

package example;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import javax.net.ssl.*;
import java.net.URI;
import java.security.cert.X509Certificate;

public class QuickstartTls {

    public static void main(String[] args) throws Exception {
        String endpoint = System.getenv().getOrDefault("KINESIS_ENDPOINT", "https://localhost:4567");
        String stream = "java-example-tls";

        // Disable CBOR — ferrokinesis speaks JSON, not CBOR
        System.setProperty("aws.cborEnabled", "false");

        // Trust all certificates (for self-signed certs)
        TrustManager[] trustAll = new TrustManager[]{
                new X509TrustManager() {
                    public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
                    public void checkClientTrusted(X509Certificate[] certs, String authType) {}
                    public void checkServerTrusted(X509Certificate[] certs, String authType) {}
                }
        };
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, trustAll, new java.security.SecureRandom());

        KinesisClient client = KinesisClient.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test", "test")))
                .httpClient(UrlConnectionHttpClient.builder()
                        .tlsTrustManagersProvider(() -> trustAll)
                        .build())
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
