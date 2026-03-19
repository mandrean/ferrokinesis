package conformance

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
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

	// 8. DeleteStream
	t.Run("DeleteStream", func(t *testing.T) {
		_, err := client.DeleteStream(ctx, &kinesis.DeleteStreamInput{
			StreamName: aws.String(streamName),
		})
		if err != nil {
			t.Fatalf("DeleteStream: %v", err)
		}
	})

}

func TestTagging(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	streamName := "go-tags"

	// 1. CreateStream + waitForActive
	t.Run("CreateStream", func(t *testing.T) {
		_, err := client.CreateStream(ctx, &kinesis.CreateStreamInput{
			StreamName: aws.String(streamName),
			ShardCount: aws.Int32(1),
		})
		if err != nil {
			t.Fatalf("CreateStream: %v", err)
		}
		waitForActive(t, client, streamName)
	})

	// 2. DescribeLimits
	t.Run("DescribeLimits", func(t *testing.T) {
		out, err := client.DescribeLimits(ctx, &kinesis.DescribeLimitsInput{})
		if err != nil {
			t.Fatalf("DescribeLimits: %v", err)
		}
		if out.ShardLimit == nil {
			t.Error("ShardLimit is nil")
		} else if aws.ToInt32(out.ShardLimit) < 0 {
			t.Errorf("ShardLimit = %d, want >= 0", aws.ToInt32(out.ShardLimit))
		}
		if out.OpenShardCount == nil {
			t.Error("OpenShardCount is nil")
		} else if aws.ToInt32(out.OpenShardCount) < 0 {
			t.Errorf("OpenShardCount = %d, want >= 0", aws.ToInt32(out.OpenShardCount))
		}
	})

	// 3. DescribeStreamSummary
	t.Run("DescribeStreamSummary", func(t *testing.T) {
		out, err := client.DescribeStreamSummary(ctx, &kinesis.DescribeStreamSummaryInput{
			StreamName: aws.String(streamName),
		})
		if err != nil {
			t.Fatalf("DescribeStreamSummary: %v", err)
		}
		summary := out.StreamDescriptionSummary
		if aws.ToString(summary.StreamName) != streamName {
			t.Errorf("StreamName = %q, want %q", aws.ToString(summary.StreamName), streamName)
		}
		if aws.ToInt32(summary.OpenShardCount) != 1 {
			t.Errorf("OpenShardCount = %d, want 1", aws.ToInt32(summary.OpenShardCount))
		}
		if summary.StreamStatus != types.StreamStatusActive {
			t.Errorf("StreamStatus = %v, want ACTIVE", summary.StreamStatus)
		}
	})

	// 4. AddTagsToStream
	t.Run("AddTagsToStream", func(t *testing.T) {
		_, err := client.AddTagsToStream(ctx, &kinesis.AddTagsToStreamInput{
			StreamName: aws.String(streamName),
			Tags: map[string]string{
				"env":  "test",
				"team": "platform",
			},
		})
		if err != nil {
			t.Fatalf("AddTagsToStream: %v", err)
		}
	})

	// 5. ListTagsForStream — verify 2 tags
	t.Run("ListTagsForStream_2Tags", func(t *testing.T) {
		out, err := client.ListTagsForStream(ctx, &kinesis.ListTagsForStreamInput{
			StreamName: aws.String(streamName),
		})
		if err != nil {
			t.Fatalf("ListTagsForStream: %v", err)
		}
		if len(out.Tags) != 2 {
			t.Errorf("Tags count = %d, want 2", len(out.Tags))
		}
	})

	// 6. RemoveTagsFromStream — remove "team"
	t.Run("RemoveTagsFromStream", func(t *testing.T) {
		_, err := client.RemoveTagsFromStream(ctx, &kinesis.RemoveTagsFromStreamInput{
			StreamName: aws.String(streamName),
			TagKeys:    []string{"team"},
		})
		if err != nil {
			t.Fatalf("RemoveTagsFromStream: %v", err)
		}
	})

	// 7. ListTagsForStream — verify 1 tag remains
	t.Run("ListTagsForStream_1Tag", func(t *testing.T) {
		out, err := client.ListTagsForStream(ctx, &kinesis.ListTagsForStreamInput{
			StreamName: aws.String(streamName),
		})
		if err != nil {
			t.Fatalf("ListTagsForStream: %v", err)
		}
		if len(out.Tags) != 1 {
			t.Errorf("Tags count = %d, want 1", len(out.Tags))
		}
	})

	// Skip TagResource, ListTagsForResource, UntagResource — these APIs are not available
	// in this version of the Go SDK (v1.32.8). They require a newer SDK version.

	// 8. DeleteStream
	t.Run("DeleteStream", func(t *testing.T) {
		_, err := client.DeleteStream(ctx, &kinesis.DeleteStreamInput{
			StreamName: aws.String(streamName),
		})
		if err != nil {
			t.Fatalf("DeleteStream: %v", err)
		}
	})
}

func TestStreamConfig(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	streamName := "go-config"

	// 1. CreateStream + waitForActive
	t.Run("CreateStream", func(t *testing.T) {
		_, err := client.CreateStream(ctx, &kinesis.CreateStreamInput{
			StreamName: aws.String(streamName),
			ShardCount: aws.Int32(1),
		})
		if err != nil {
			t.Fatalf("CreateStream: %v", err)
		}
		waitForActive(t, client, streamName)
	})

	// 2. IncreaseStreamRetentionPeriod — set to 48 hours
	t.Run("IncreaseStreamRetentionPeriod", func(t *testing.T) {
		_, err := client.IncreaseStreamRetentionPeriod(ctx, &kinesis.IncreaseStreamRetentionPeriodInput{
			StreamName:           aws.String(streamName),
			RetentionPeriodHours: aws.Int32(48),
		})
		if err != nil {
			t.Fatalf("IncreaseStreamRetentionPeriod: %v", err)
		}
	})

	// 3. DescribeStream — verify RetentionPeriodHours=48
	t.Run("DescribeStream_Retention48", func(t *testing.T) {
		out, err := client.DescribeStream(ctx, &kinesis.DescribeStreamInput{
			StreamName: aws.String(streamName),
		})
		if err != nil {
			t.Fatalf("DescribeStream: %v", err)
		}
		if aws.ToInt32(out.StreamDescription.RetentionPeriodHours) != 48 {
			t.Errorf("RetentionPeriodHours = %d, want 48", aws.ToInt32(out.StreamDescription.RetentionPeriodHours))
		}
	})

	// 4. DecreaseStreamRetentionPeriod — back to 24
	t.Run("DecreaseStreamRetentionPeriod", func(t *testing.T) {
		_, err := client.DecreaseStreamRetentionPeriod(ctx, &kinesis.DecreaseStreamRetentionPeriodInput{
			StreamName:           aws.String(streamName),
			RetentionPeriodHours: aws.Int32(24),
		})
		if err != nil {
			t.Fatalf("DecreaseStreamRetentionPeriod: %v", err)
		}
	})

	// 5. StartStreamEncryption
	t.Run("StartStreamEncryption", func(t *testing.T) {
		_, err := client.StartStreamEncryption(ctx, &kinesis.StartStreamEncryptionInput{
			StreamName:     aws.String(streamName),
			EncryptionType: types.EncryptionTypeKms,
			KeyId:          aws.String("alias/aws/kinesis"),
		})
		if err != nil {
			t.Fatalf("StartStreamEncryption: %v", err)
		}
		waitForActive(t, client, streamName)
	})

	// 6. StopStreamEncryption
	t.Run("StopStreamEncryption", func(t *testing.T) {
		_, err := client.StopStreamEncryption(ctx, &kinesis.StopStreamEncryptionInput{
			StreamName:     aws.String(streamName),
			EncryptionType: types.EncryptionTypeKms,
			KeyId:          aws.String("alias/aws/kinesis"),
		})
		if err != nil {
			t.Fatalf("StopStreamEncryption: %v", err)
		}
	})

	// 7. EnableEnhancedMonitoring
	t.Run("EnableEnhancedMonitoring", func(t *testing.T) {
		_, err := client.EnableEnhancedMonitoring(ctx, &kinesis.EnableEnhancedMonitoringInput{
			StreamName:        aws.String(streamName),
			ShardLevelMetrics: []types.MetricsName{types.MetricsNameIncomingBytes},
		})
		if err != nil {
			t.Fatalf("EnableEnhancedMonitoring: %v", err)
		}
	})

	// 8. DisableEnhancedMonitoring
	t.Run("DisableEnhancedMonitoring", func(t *testing.T) {
		_, err := client.DisableEnhancedMonitoring(ctx, &kinesis.DisableEnhancedMonitoringInput{
			StreamName:        aws.String(streamName),
			ShardLevelMetrics: []types.MetricsName{types.MetricsNameIncomingBytes},
		})
		if err != nil {
			t.Fatalf("DisableEnhancedMonitoring: %v", err)
		}
	})

	// 9. Get stream ARN via DescribeStreamSummary
	var streamARN string
	t.Run("GetStreamARN", func(t *testing.T) {
		out, err := client.DescribeStreamSummary(ctx, &kinesis.DescribeStreamSummaryInput{
			StreamName: aws.String(streamName),
		})
		if err != nil {
			t.Fatalf("DescribeStreamSummary: %v", err)
		}
		streamARN = aws.ToString(out.StreamDescriptionSummary.StreamARN)
		if streamARN == "" {
			t.Fatal("StreamARN is empty")
		}
	})

	// 10. UpdateStreamMode — set to ON_DEMAND
	// Skip UpdateMaxRecordSize and UpdateStreamWarmThroughput — these APIs may not be available
	// in the Go SDK yet and are very new.
	t.Run("UpdateStreamMode", func(t *testing.T) {
		if streamARN == "" {
			t.Fatal("StreamARN must be available before UpdateStreamMode")
		}
		_, err := client.UpdateStreamMode(ctx, &kinesis.UpdateStreamModeInput{
			StreamARN: aws.String(streamARN),
			StreamModeDetails: &types.StreamModeDetails{
				StreamMode: types.StreamModeOnDemand,
			},
		})
		if err != nil {
			t.Fatalf("UpdateStreamMode: %v", err)
		}
	})

	// 11. DescribeStream — verify StreamMode = ON_DEMAND
	t.Run("DescribeStream_OnDemand", func(t *testing.T) {
		waitForActive(t, client, streamName)

		out, err := client.DescribeStream(ctx, &kinesis.DescribeStreamInput{
			StreamName: aws.String(streamName),
		})
		if err != nil {
			t.Fatalf("DescribeStream: %v", err)
		}
		desc := out.StreamDescription
		if desc.StreamModeDetails == nil {
			t.Fatal("StreamModeDetails is nil")
		}
		if desc.StreamModeDetails.StreamMode != types.StreamModeOnDemand {
			t.Errorf("StreamMode = %v, want ON_DEMAND", desc.StreamModeDetails.StreamMode)
		}
	})

	// 12. DeleteStream
	t.Run("DeleteStream", func(t *testing.T) {
		_, err := client.DeleteStream(ctx, &kinesis.DeleteStreamInput{
			StreamName: aws.String(streamName),
		})
		if err != nil {
			t.Fatalf("DeleteStream: %v", err)
		}
	})
}

func TestShardManagement(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	streamName := "go-shards"

	// 1. CreateStream(2 shards) + waitForActive
	t.Run("CreateStream", func(t *testing.T) {
		_, err := client.CreateStream(ctx, &kinesis.CreateStreamInput{
			StreamName: aws.String(streamName),
			ShardCount: aws.Int32(2),
		})
		if err != nil {
			t.Fatalf("CreateStream: %v", err)
		}
		waitForActive(t, client, streamName)
	})

	// 2. ListShards — verify 2 shards, get shard-0 hash key range
	var shard0StartHash, shard0EndHash string
	t.Run("ListShards_Initial", func(t *testing.T) {
		out, err := client.ListShards(ctx, &kinesis.ListShardsInput{
			StreamName: aws.String(streamName),
		})
		if err != nil {
			t.Fatalf("ListShards: %v", err)
		}
		if len(out.Shards) != 2 {
			t.Fatalf("Shards = %d, want 2", len(out.Shards))
		}
		shard0StartHash = aws.ToString(out.Shards[0].HashKeyRange.StartingHashKey)
		shard0EndHash = aws.ToString(out.Shards[0].HashKeyRange.EndingHashKey)
	})

	// 3. SplitShard — split shardId-000000000000
	t.Run("SplitShard", func(t *testing.T) {
		if shard0StartHash == "" || shard0EndHash == "" {
			t.Fatal("ListShards must succeed before SplitShard")
		}

		start := new(big.Int)
		start.SetString(shard0StartHash, 10)
		end := new(big.Int)
		end.SetString(shard0EndHash, 10)

		// NewStartingHashKey = (start + end) / 2
		mid := new(big.Int).Add(start, end)
		mid.Div(mid, big.NewInt(2))

		_, err := client.SplitShard(ctx, &kinesis.SplitShardInput{
			StreamName:         aws.String(streamName),
			ShardToSplit:       aws.String("shardId-000000000000"),
			NewStartingHashKey: aws.String(mid.String()),
		})
		if err != nil {
			t.Fatalf("SplitShard: %v", err)
		}
	})

	// 4. waitForActive
	t.Run("WaitAfterSplit", func(t *testing.T) {
		waitForActive(t, client, streamName)
	})

	// 5. ListShards — verify >= 4 total shards (including closed)
	t.Run("ListShards_AfterSplit", func(t *testing.T) {
		out, err := client.ListShards(ctx, &kinesis.ListShardsInput{
			StreamName: aws.String(streamName),
			ShardFilter: &types.ShardFilter{
				Type: types.ShardFilterTypeFromTrimHorizon,
			},
		})
		if err != nil {
			t.Fatalf("ListShards: %v", err)
		}
		if len(out.Shards) < 4 {
			t.Errorf("Shards after split = %d, want >= 4", len(out.Shards))
		}
	})

	// 6. MergeShards — merge the two child shards
	t.Run("MergeShards", func(t *testing.T) {
		_, err := client.MergeShards(ctx, &kinesis.MergeShardsInput{
			StreamName:           aws.String(streamName),
			ShardToMerge:         aws.String("shardId-000000000002"),
			AdjacentShardToMerge: aws.String("shardId-000000000003"),
		})
		if err != nil {
			t.Fatalf("MergeShards: %v", err)
		}
	})

	// 7. waitForActive
	t.Run("WaitAfterMerge", func(t *testing.T) {
		waitForActive(t, client, streamName)
	})

	// 8. ListShards — verify merge result (more total shards including closed ones)
	t.Run("ListShards_AfterMerge", func(t *testing.T) {
		out, err := client.ListShards(ctx, &kinesis.ListShardsInput{
			StreamName: aws.String(streamName),
			ShardFilter: &types.ShardFilter{
				Type: types.ShardFilterTypeFromTrimHorizon,
			},
		})
		if err != nil {
			t.Fatalf("ListShards: %v", err)
		}
		// After split (4 shards: 0-closed, 1-open, 2-open, 3-open) then merge of 2+3 => 5 total
		if len(out.Shards) < 5 {
			t.Errorf("Shards after merge = %d, want >= 5", len(out.Shards))
		}
	})

	// 9. UpdateShardCount — TargetShardCount=1
	t.Run("UpdateShardCount", func(t *testing.T) {
		_, err := client.UpdateShardCount(ctx, &kinesis.UpdateShardCountInput{
			StreamName:       aws.String(streamName),
			TargetShardCount: aws.Int32(1),
			ScalingType:      types.ScalingTypeUniformScaling,
		})
		if err != nil {
			t.Fatalf("UpdateShardCount: %v", err)
		}
	})

	// 10. waitForActive
	t.Run("WaitAfterUpdateShardCount", func(t *testing.T) {
		waitForActive(t, client, streamName)
	})

	// 11. DeleteStream
	t.Run("DeleteStream", func(t *testing.T) {
		_, err := client.DeleteStream(ctx, &kinesis.DeleteStreamInput{
			StreamName: aws.String(streamName),
		})
		if err != nil {
			t.Fatalf("DeleteStream: %v", err)
		}
	})
}

func TestStreamConsumers(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	streamName := "go-consumers"

	// 1. CreateStream + waitForActive
	t.Run("CreateStream", func(t *testing.T) {
		_, err := client.CreateStream(ctx, &kinesis.CreateStreamInput{
			StreamName: aws.String(streamName),
			ShardCount: aws.Int32(1),
		})
		if err != nil {
			t.Fatalf("CreateStream: %v", err)
		}
		waitForActive(t, client, streamName)
	})

	// 2. Get stream ARN via DescribeStream
	var streamARN string
	t.Run("GetStreamARN", func(t *testing.T) {
		out, err := client.DescribeStream(ctx, &kinesis.DescribeStreamInput{
			StreamName: aws.String(streamName),
		})
		if err != nil {
			t.Fatalf("DescribeStream: %v", err)
		}
		streamARN = aws.ToString(out.StreamDescription.StreamARN)
		if streamARN == "" {
			t.Fatal("StreamARN is empty")
		}
	})

	// 3. RegisterStreamConsumer
	var consumerARN string
	t.Run("RegisterStreamConsumer", func(t *testing.T) {
		if streamARN == "" {
			t.Fatal("StreamARN must be available before RegisterStreamConsumer")
		}
		out, err := client.RegisterStreamConsumer(ctx, &kinesis.RegisterStreamConsumerInput{
			StreamARN:    aws.String(streamARN),
			ConsumerName: aws.String("go-test-consumer"),
		})
		if err != nil {
			t.Fatalf("RegisterStreamConsumer: %v", err)
		}
		consumerARN = aws.ToString(out.Consumer.ConsumerARN)
		if consumerARN == "" {
			t.Fatal("ConsumerARN is empty")
		}
	})

	// 4. DescribeStreamConsumer — verify by ConsumerARN
	t.Run("DescribeStreamConsumer", func(t *testing.T) {
		if consumerARN == "" {
			t.Fatal("ConsumerARN must be available before DescribeStreamConsumer")
		}
		// Poll until consumer is ACTIVE
		var desc *kinesis.DescribeStreamConsumerOutput
		for i := 0; i < 30; i++ {
			var err error
			desc, err = client.DescribeStreamConsumer(ctx, &kinesis.DescribeStreamConsumerInput{
				ConsumerARN: aws.String(consumerARN),
			})
			if err != nil {
				t.Fatalf("DescribeStreamConsumer: %v", err)
			}
			if desc.ConsumerDescription.ConsumerStatus == types.ConsumerStatusActive {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		if desc.ConsumerDescription.ConsumerStatus != types.ConsumerStatusActive {
			t.Fatalf("consumer did not become ACTIVE, status = %v", desc.ConsumerDescription.ConsumerStatus)
		}
		if aws.ToString(desc.ConsumerDescription.ConsumerName) != "go-test-consumer" {
			t.Errorf("ConsumerName = %q, want %q", aws.ToString(desc.ConsumerDescription.ConsumerName), "go-test-consumer")
		}
	})

	// 5. ListStreamConsumers — verify consumer appears
	t.Run("ListStreamConsumers", func(t *testing.T) {
		if streamARN == "" {
			t.Fatal("StreamARN must be available before ListStreamConsumers")
		}
		out, err := client.ListStreamConsumers(ctx, &kinesis.ListStreamConsumersInput{
			StreamARN: aws.String(streamARN),
		})
		if err != nil {
			t.Fatalf("ListStreamConsumers: %v", err)
		}
		found := false
		for _, c := range out.Consumers {
			if aws.ToString(c.ConsumerName) == "go-test-consumer" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("consumer 'go-test-consumer' not found in ListStreamConsumers")
		}
	})

	// 6. Put a record to the stream for SubscribeToShard
	t.Run("PutRecord", func(t *testing.T) {
		_, err := client.PutRecord(ctx, &kinesis.PutRecordInput{
			StreamName:   aws.String(streamName),
			Data:         []byte("subscribe-test-data"),
			PartitionKey: aws.String("pk-sub"),
		})
		if err != nil {
			t.Fatalf("PutRecord: %v", err)
		}
	})

	// 7. SubscribeToShard
	t.Run("SubscribeToShard", func(t *testing.T) {
		if consumerARN == "" {
			t.Fatal("ConsumerARN must be available before SubscribeToShard")
		}
		out, err := client.SubscribeToShard(ctx, &kinesis.SubscribeToShardInput{
			ConsumerARN: aws.String(consumerARN),
			ShardId:     aws.String("shardId-000000000000"),
			StartingPosition: &types.StartingPosition{
				Type: types.ShardIteratorTypeTrimHorizon,
			},
		})
		if err != nil {
			t.Fatalf("SubscribeToShard: %v", err)
		}

		stream := out.GetStream()
		defer stream.Close()

		gotRecords := false
		for event := range stream.Events() {
			switch e := event.(type) {
			case *types.SubscribeToShardEventStreamMemberSubscribeToShardEvent:
				if len(e.Value.Records) > 0 {
					gotRecords = true
				}
			}
			// Only need to check the first event batch
			break
		}

		if err := stream.Err(); err != nil {
			t.Errorf("SubscribeToShard stream error: %v", err)
		}
		if !gotRecords {
			t.Error("SubscribeToShard: expected at least one record")
		}
	})

	// 8. DeregisterStreamConsumer
	t.Run("DeregisterStreamConsumer", func(t *testing.T) {
		if consumerARN == "" {
			t.Fatal("ConsumerARN must be available before DeregisterStreamConsumer")
		}
		_, err := client.DeregisterStreamConsumer(ctx, &kinesis.DeregisterStreamConsumerInput{
			ConsumerARN: aws.String(consumerARN),
		})
		if err != nil {
			t.Fatalf("DeregisterStreamConsumer: %v", err)
		}
	})

	// 9. DeleteStream
	t.Run("DeleteStream", func(t *testing.T) {
		_, err := client.DeleteStream(ctx, &kinesis.DeleteStreamInput{
			StreamName: aws.String(streamName),
		})
		if err != nil {
			t.Fatalf("DeleteStream: %v", err)
		}
	})
}

func TestPoliciesAndAccountSettings(t *testing.T) {
	// Skip DescribeAccountSettings and UpdateAccountSettings — they are available in Go SDK
	// but skipped for simplicity. These are account-level operations that could interfere
	// with other tests.

	client := newClient(t)
	ctx := context.Background()
	streamName := "go-policies"

	// 1. CreateStream + waitForActive
	t.Run("CreateStream", func(t *testing.T) {
		_, err := client.CreateStream(ctx, &kinesis.CreateStreamInput{
			StreamName: aws.String(streamName),
			ShardCount: aws.Int32(1),
		})
		if err != nil {
			t.Fatalf("CreateStream: %v", err)
		}
		waitForActive(t, client, streamName)
	})

	// 2. Get stream ARN
	var streamARN string
	t.Run("GetStreamARN", func(t *testing.T) {
		out, err := client.DescribeStream(ctx, &kinesis.DescribeStreamInput{
			StreamName: aws.String(streamName),
		})
		if err != nil {
			t.Fatalf("DescribeStream: %v", err)
		}
		streamARN = aws.ToString(out.StreamDescription.StreamARN)
		if streamARN == "" {
			t.Fatal("StreamARN is empty")
		}
	})

	// 3. PutResourcePolicy
	t.Run("PutResourcePolicy", func(t *testing.T) {
		if streamARN == "" {
			t.Fatal("StreamARN must be available before PutResourcePolicy")
		}
		policy := map[string]interface{}{
			"Version": "2012-10-17",
			"Statement": []map[string]interface{}{
				{
					"Sid":       "test",
					"Effect":    "Allow",
					"Principal": map[string]interface{}{"AWS": "*"},
					"Action":    "kinesis:GetRecords",
					"Resource":  "*",
				},
			},
		}
		policyJSON, err := json.Marshal(policy)
		if err != nil {
			t.Fatalf("json.Marshal policy: %v", err)
		}
		_, err = client.PutResourcePolicy(ctx, &kinesis.PutResourcePolicyInput{
			ResourceARN: aws.String(streamARN),
			Policy:      aws.String(string(policyJSON)),
		})
		if err != nil {
			t.Fatalf("PutResourcePolicy: %v", err)
		}
	})

	// 4. GetResourcePolicy — verify Policy is non-empty
	t.Run("GetResourcePolicy_NonEmpty", func(t *testing.T) {
		if streamARN == "" {
			t.Fatal("StreamARN must be available before GetResourcePolicy")
		}
		out, err := client.GetResourcePolicy(ctx, &kinesis.GetResourcePolicyInput{
			ResourceARN: aws.String(streamARN),
		})
		if err != nil {
			t.Fatalf("GetResourcePolicy: %v", err)
		}
		if aws.ToString(out.Policy) == "" {
			t.Error("GetResourcePolicy: Policy is empty, expected non-empty")
		}
	})

	// 5. DeleteResourcePolicy
	t.Run("DeleteResourcePolicy", func(t *testing.T) {
		if streamARN == "" {
			t.Fatal("StreamARN must be available before DeleteResourcePolicy")
		}
		_, err := client.DeleteResourcePolicy(ctx, &kinesis.DeleteResourcePolicyInput{
			ResourceARN: aws.String(streamARN),
		})
		if err != nil {
			t.Fatalf("DeleteResourcePolicy: %v", err)
		}
	})

	// 6. GetResourcePolicy — verify policy is empty/absent after deletion
	t.Run("GetResourcePolicy_AfterDelete", func(t *testing.T) {
		if streamARN == "" {
			t.Fatal("StreamARN must be available before GetResourcePolicy")
		}
		out, err := client.GetResourcePolicy(ctx, &kinesis.GetResourcePolicyInput{
			ResourceARN: aws.String(streamARN),
		})
		// The server may return an error or an empty policy — either is acceptable
		if err != nil {
			// Error is acceptable after deletion
			return
		}
		if aws.ToString(out.Policy) != "" {
			t.Errorf("GetResourcePolicy after delete: Policy = %q, want empty", aws.ToString(out.Policy))
		}
	})

	// 7. DeleteStream
	t.Run("DeleteStream", func(t *testing.T) {
		_, err := client.DeleteStream(ctx, &kinesis.DeleteStreamInput{
			StreamName: aws.String(streamName),
		})
		if err != nil {
			t.Fatalf("DeleteStream: %v", err)
		}
	})
}
