package conformance

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

func endpoint() string {
	if v := os.Getenv("KINESIS_ENDPOINT"); v != "" {
		return v
	}
	return "http://localhost:4567"
}

func newClient(t *testing.T) *kinesis.Client {
	t.Helper()
	ep := endpoint()
	return kinesis.New(kinesis.Options{
		Region:       "us-east-1",
		BaseEndpoint: &ep,
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
	})
}

func waitForActive(t *testing.T, client *kinesis.Client, stream string) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < 30; i++ {
		out, err := client.DescribeStream(ctx, &kinesis.DescribeStreamInput{
			StreamName: aws.String(stream),
		})
		if err == nil && out.StreamDescription.StreamStatus == types.StreamStatusActive {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("stream %q did not become ACTIVE", stream)
}

func TestConformance(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	streamName := "go-conformance"

	t.Cleanup(func() {
		client.DeleteStream(ctx, &kinesis.DeleteStreamInput{
			StreamName: aws.String(streamName),
		})
	})

	// 1. CreateStream
	t.Run("CreateStream", func(t *testing.T) {
		_, err := client.CreateStream(ctx, &kinesis.CreateStreamInput{
			StreamName: aws.String(streamName),
			ShardCount: aws.Int32(2),
		})
		if err != nil {
			t.Fatalf("CreateStream: %v", err)
		}
	})

	// 2. DescribeStream
	t.Run("DescribeStream", func(t *testing.T) {
		waitForActive(t, client, streamName)

		out, err := client.DescribeStream(ctx, &kinesis.DescribeStreamInput{
			StreamName: aws.String(streamName),
		})
		if err != nil {
			t.Fatalf("DescribeStream: %v", err)
		}
		desc := out.StreamDescription
		if aws.ToString(desc.StreamName) != streamName {
			t.Errorf("name = %q, want %q", aws.ToString(desc.StreamName), streamName)
		}
		if desc.StreamStatus != types.StreamStatusActive {
			t.Errorf("status = %v, want ACTIVE", desc.StreamStatus)
		}
		if len(desc.Shards) != 2 {
			t.Errorf("shards = %d, want 2", len(desc.Shards))
		}
	})

	// 3. ListStreams
	t.Run("ListStreams", func(t *testing.T) {
		out, err := client.ListStreams(ctx, &kinesis.ListStreamsInput{})
		if err != nil {
			t.Fatalf("ListStreams: %v", err)
		}
		found := false
		for _, name := range out.StreamNames {
			if name == streamName {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("stream %q not found in list: %v", streamName, out.StreamNames)
		}
	})

	// 4. PutRecord
	var putShardID string
	t.Run("PutRecord", func(t *testing.T) {
		out, err := client.PutRecord(ctx, &kinesis.PutRecordInput{
			StreamName:   aws.String(streamName),
			Data:         []byte("hello from go"),
			PartitionKey: aws.String("pk-1"),
		})
		if err != nil {
			t.Fatalf("PutRecord: %v", err)
		}
		if out.ShardId == nil || *out.ShardId == "" {
			t.Error("PutRecord: missing ShardId")
		}
		if out.SequenceNumber == nil || *out.SequenceNumber == "" {
			t.Error("PutRecord: missing SequenceNumber")
		}
		putShardID = aws.ToString(out.ShardId)
	})

	// 5. PutRecords
	t.Run("PutRecords", func(t *testing.T) {
		entries := make([]types.PutRecordsRequestEntry, 3)
		for i := range entries {
			entries[i] = types.PutRecordsRequestEntry{
				Data:         []byte(fmt.Sprintf("batch-%d", i)),
				PartitionKey: aws.String(fmt.Sprintf("pk-%d", i)),
			}
		}
		out, err := client.PutRecords(ctx, &kinesis.PutRecordsInput{
			StreamName: aws.String(streamName),
			Records:    entries,
		})
		if err != nil {
			t.Fatalf("PutRecords: %v", err)
		}
		if aws.ToInt32(out.FailedRecordCount) != 0 {
			t.Errorf("FailedRecordCount = %d, want 0", aws.ToInt32(out.FailedRecordCount))
		}
		if len(out.Records) != 3 {
			t.Errorf("Records = %d, want 3", len(out.Records))
		}
	})

	// 6. GetShardIterator
	var shardIterator string
	t.Run("GetShardIterator", func(t *testing.T) {
		if putShardID == "" {
			t.Fatal("PutRecord must succeed before GetShardIterator")
		}
		out, err := client.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
			StreamName:        aws.String(streamName),
			ShardId:           aws.String(putShardID),
			ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
		})
		if err != nil {
			t.Fatalf("GetShardIterator: %v", err)
		}
		if out.ShardIterator == nil || *out.ShardIterator == "" {
			t.Fatal("GetShardIterator: missing iterator")
		}
		shardIterator = aws.ToString(out.ShardIterator)
	})

	// 7. GetRecords
	t.Run("GetRecords", func(t *testing.T) {
		if shardIterator == "" {
			t.Fatal("GetShardIterator must succeed before GetRecords")
		}
		out, err := client.GetRecords(ctx, &kinesis.GetRecordsInput{
			ShardIterator: aws.String(shardIterator),
		})
		if err != nil {
			t.Fatalf("GetRecords: %v", err)
		}
		if len(out.Records) < 1 {
			t.Fatalf("expected >= 1 records, got %d", len(out.Records))
		}
		first := string(out.Records[0].Data)
		if first != "hello from go" {
			t.Errorf("first record data = %q, want %q", first, "hello from go")
		}
	})

}
