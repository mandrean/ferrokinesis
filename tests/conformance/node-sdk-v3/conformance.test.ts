import { describe, it, expect } from "vitest";
import {
  KinesisClient,
  CreateStreamCommand,
  DeleteStreamCommand,
  DescribeStreamCommand,
  DescribeStreamSummaryCommand,
  DescribeLimitsCommand,
  ListStreamsCommand,
  ListShardsCommand,
  ListTagsForStreamCommand,
  AddTagsToStreamCommand,
  RemoveTagsFromStreamCommand,
  TagResourceCommand,
  ListTagsForResourceCommand,
  UntagResourceCommand,
  PutRecordCommand,
  PutRecordsCommand,
  GetShardIteratorCommand,
  GetRecordsCommand,
  IncreaseStreamRetentionPeriodCommand,
  DecreaseStreamRetentionPeriodCommand,
  StartStreamEncryptionCommand,
  StopStreamEncryptionCommand,
  EnableEnhancedMonitoringCommand,
  DisableEnhancedMonitoringCommand,
  UpdateStreamModeCommand,
  SplitShardCommand,
  MergeShardsCommand,
  UpdateShardCountCommand,
  RegisterStreamConsumerCommand,
  DescribeStreamConsumerCommand,
  ListStreamConsumersCommand,
  DeregisterStreamConsumerCommand,
  PutResourcePolicyCommand,
  GetResourcePolicyCommand,
  DeleteResourcePolicyCommand,
  // These commands exist in the SDK but are not covered in the current test plan:
  // DescribeAccountSettingsCommand,
  // UpdateAccountSettingsCommand,
  // UpdateMaxRecordSizeCommand,
  // UpdateStreamWarmThroughputCommand,
  ShardIteratorType,
  ScalingType,
  EncryptionType,
  StreamMode,
} from "@aws-sdk/client-kinesis";
import { NodeHttpHandler } from "@smithy/node-http-handler";

// SubscribeToShard is SKIPPED — requires HTTP/2 event-stream, server is HTTP/1.1 only

const ENDPOINT = process.env.KINESIS_ENDPOINT || "http://localhost:4567";
const STREAM_NAME = "node-conformance";

const client = new KinesisClient({
  endpoint: ENDPOINT,
  region: "us-east-1",
  credentials: {
    accessKeyId: "test",
    secretAccessKey: "test",
  },
  // Force HTTP/1.1 — the server does not support HTTP/2
  requestHandler: new NodeHttpHandler(),
});

async function waitForActive(
  streamName: string,
  maxRetries = 30,
  intervalMs = 200
): Promise<void> {
  for (let i = 0; i < maxRetries; i++) {
    const resp = await client.send(
      new DescribeStreamCommand({ StreamName: streamName })
    );
    if (resp.StreamDescription?.StreamStatus === "ACTIVE") return;
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  throw new Error(`Stream ${streamName} did not become ACTIVE`);
}

describe("Kinesis conformance", () => {
  it("completes the 8-operation lifecycle", async () => {
    // 1. CreateStream
    await client.send(
      new CreateStreamCommand({ StreamName: STREAM_NAME, ShardCount: 2 })
    );

    // 2. DescribeStream — wait for ACTIVE
    await waitForActive(STREAM_NAME);
    const desc = await client.send(
      new DescribeStreamCommand({ StreamName: STREAM_NAME })
    );
    expect(desc.StreamDescription?.StreamName).toBe(STREAM_NAME);
    expect(desc.StreamDescription?.StreamStatus).toBe("ACTIVE");
    expect(desc.StreamDescription?.Shards).toHaveLength(2);

    // 3. ListStreams
    const list = await client.send(new ListStreamsCommand({}));
    expect(list.StreamNames).toContain(STREAM_NAME);

    // 4. PutRecord
    const put = await client.send(
      new PutRecordCommand({
        StreamName: STREAM_NAME,
        Data: new TextEncoder().encode("hello from node"),
        PartitionKey: "pk-1",
      })
    );
    expect(put.ShardId).toBeTruthy();
    expect(put.SequenceNumber).toBeTruthy();
    const shardId = put.ShardId!;

    // 5. PutRecords
    const batch = await client.send(
      new PutRecordsCommand({
        StreamName: STREAM_NAME,
        Records: [0, 1, 2].map((i) => ({
          Data: new TextEncoder().encode(`batch-${i}`),
          PartitionKey: `pk-${i}`,
        })),
      })
    );
    expect(batch.FailedRecordCount).toBe(0);
    expect(batch.Records).toHaveLength(3);

    // 6. GetShardIterator
    const iterResp = await client.send(
      new GetShardIteratorCommand({
        StreamName: STREAM_NAME,
        ShardId: shardId,
        ShardIteratorType: ShardIteratorType.TRIM_HORIZON,
      })
    );
    expect(iterResp.ShardIterator).toBeTruthy();

    // 7. GetRecords
    const records = await client.send(
      new GetRecordsCommand({ ShardIterator: iterResp.ShardIterator! })
    );
    expect(records.Records!.length).toBeGreaterThanOrEqual(1);
    const firstData = new TextDecoder().decode(records.Records![0].Data);
    expect(firstData).toBe("hello from node");

    // 8. DeleteStream
    await client.send(
      new DeleteStreamCommand({ StreamName: STREAM_NAME })
    );
  });
});

describe("Tagging and metadata", () => {
  const streamName = "node-tags";
  let streamArn: string;

  it("CreateStream", async () => {
    await client.send(
      new CreateStreamCommand({ StreamName: streamName, ShardCount: 1 })
    );
    await waitForActive(streamName);
  });

  it("DescribeLimits", async () => {
    const resp = await client.send(new DescribeLimitsCommand({}));
    expect(resp.ShardLimit).toBeDefined();
    expect(resp.OpenShardCount).toBeDefined();
  });

  it("DescribeStreamSummary", async () => {
    const resp = await client.send(
      new DescribeStreamSummaryCommand({ StreamName: streamName })
    );
    expect(resp.StreamDescriptionSummary?.StreamName).toBe(streamName);
    expect(resp.StreamDescriptionSummary?.OpenShardCount).toBe(1);
  });

  it("AddTagsToStream", async () => {
    await client.send(
      new AddTagsToStreamCommand({
        StreamName: streamName,
        Tags: { env: "test", team: "platform" },
      })
    );
  });

  it("ListTagsForStream", async () => {
    const resp = await client.send(
      new ListTagsForStreamCommand({ StreamName: streamName })
    );
    expect(resp.Tags).toHaveLength(2);
  });

  it("RemoveTagsFromStream", async () => {
    await client.send(
      new RemoveTagsFromStreamCommand({
        StreamName: streamName,
        TagKeys: ["team"],
      })
    );
  });

  it("ListTagsForStream after remove", async () => {
    const resp = await client.send(
      new ListTagsForStreamCommand({ StreamName: streamName })
    );
    expect(resp.Tags).toHaveLength(1);
  });

  it("TagResource", async () => {
    const desc = await client.send(
      new DescribeStreamCommand({ StreamName: streamName })
    );
    streamArn = desc.StreamDescription?.StreamARN!;
    await client.send(
      new TagResourceCommand({
        ResourceARN: streamArn,
        Tags: { version: "1" },
      })
    );
  });

  it("ListTagsForResource", async () => {
    const resp = await client.send(
      new ListTagsForResourceCommand({ ResourceARN: streamArn })
    );
    const versionTag = resp.Tags?.find((t) => t.Key === "version");
    expect(versionTag).toBeTruthy();
    expect(versionTag?.Value).toBe("1");
  });

  it("UntagResource", async () => {
    await client.send(
      new UntagResourceCommand({
        ResourceARN: streamArn,
        TagKeys: ["version"],
      })
    );
  });

  it("DeleteStream", async () => {
    await client.send(
      new DeleteStreamCommand({ StreamName: streamName })
    );
  });
});

describe("Stream configuration", () => {
  const streamName = "node-config";
  let streamArn: string;

  it("CreateStream", async () => {
    await client.send(
      new CreateStreamCommand({ StreamName: streamName, ShardCount: 1 })
    );
    await waitForActive(streamName);
  });

  it("IncreaseStreamRetentionPeriod", async () => {
    await client.send(
      new IncreaseStreamRetentionPeriodCommand({
        StreamName: streamName,
        RetentionPeriodHours: 48,
      })
    );
  });

  it("DescribeStream — verify retention 48h", async () => {
    const resp = await client.send(
      new DescribeStreamCommand({ StreamName: streamName })
    );
    expect(resp.StreamDescription?.RetentionPeriodHours).toBe(48);
  });

  it("DecreaseStreamRetentionPeriod", async () => {
    await client.send(
      new DecreaseStreamRetentionPeriodCommand({
        StreamName: streamName,
        RetentionPeriodHours: 24,
      })
    );
  });

  it("StartStreamEncryption", async () => {
    await client.send(
      new StartStreamEncryptionCommand({
        StreamName: streamName,
        EncryptionType: EncryptionType.KMS,
        KeyId: "alias/aws/kinesis",
      })
    );
  });

  it("StopStreamEncryption", async () => {
    await client.send(
      new StopStreamEncryptionCommand({
        StreamName: streamName,
        EncryptionType: EncryptionType.KMS,
        KeyId: "alias/aws/kinesis",
      })
    );
  });

  it("EnableEnhancedMonitoring", async () => {
    const resp = await client.send(
      new EnableEnhancedMonitoringCommand({
        StreamName: streamName,
        ShardLevelMetrics: ["IncomingBytes"],
      })
    );
    expect(resp.StreamName).toBe(streamName);
  });

  it("DisableEnhancedMonitoring", async () => {
    const resp = await client.send(
      new DisableEnhancedMonitoringCommand({
        StreamName: streamName,
        ShardLevelMetrics: ["IncomingBytes"],
      })
    );
    expect(resp.StreamName).toBe(streamName);
  });

  it("UpdateStreamMode", async () => {
    const desc = await client.send(
      new DescribeStreamCommand({ StreamName: streamName })
    );
    streamArn = desc.StreamDescription?.StreamARN!;
    await client.send(
      new UpdateStreamModeCommand({
        StreamARN: streamArn,
        StreamModeDetails: { StreamMode: StreamMode.ON_DEMAND },
      })
    );
  });

  it("DescribeStream — verify ON_DEMAND", async () => {
    await waitForActive(streamName);
    const resp = await client.send(
      new DescribeStreamCommand({ StreamName: streamName })
    );
    expect(resp.StreamDescription?.StreamModeDetails?.StreamMode).toBe(
      StreamMode.ON_DEMAND
    );
  });

  it("DeleteStream", async () => {
    await client.send(
      new DeleteStreamCommand({ StreamName: streamName })
    );
  });
});

describe("Shard management", () => {
  const streamName = "node-shards";

  it("CreateStream with 2 shards", async () => {
    await client.send(
      new CreateStreamCommand({ StreamName: streamName, ShardCount: 2 })
    );
    await waitForActive(streamName);
  });

  it("ListShards — verify 2 shards", async () => {
    const resp = await client.send(
      new ListShardsCommand({ StreamName: streamName })
    );
    expect(resp.Shards).toHaveLength(2);
  });

  it("SplitShard", async () => {
    const listResp = await client.send(
      new ListShardsCommand({ StreamName: streamName })
    );
    const shard0 = listResp.Shards![0];
    const start = BigInt(shard0.HashKeyRange!.StartingHashKey!);
    const end = BigInt(shard0.HashKeyRange!.EndingHashKey!);
    const midpoint = ((start + end) / 2n).toString();

    await client.send(
      new SplitShardCommand({
        StreamName: streamName,
        ShardToSplit: shard0.ShardId!,
        NewStartingHashKey: midpoint,
      })
    );
  });

  it("waitForActive after split", async () => {
    await waitForActive(streamName);
  });

  it("ListShards — verify >= 4 shards after split", async () => {
    const resp = await client.send(
      new ListShardsCommand({ StreamName: streamName })
    );
    expect(resp.Shards!.length).toBeGreaterThanOrEqual(4);
  });

  it("MergeShards", async () => {
    await client.send(
      new MergeShardsCommand({
        StreamName: streamName,
        ShardToMerge: "shardId-000000000002",
        AdjacentShardToMerge: "shardId-000000000003",
      })
    );
  });

  it("waitForActive after merge", async () => {
    await waitForActive(streamName);
  });

  it("ListShards — verify merge result", async () => {
    const resp = await client.send(
      new ListShardsCommand({ StreamName: streamName })
    );
    // After split (4 shards) + merge (creates 1 new, closes 2), we expect >= 5 total shard records
    expect(resp.Shards!.length).toBeGreaterThanOrEqual(5);
  });

  it("UpdateShardCount", async () => {
    await client.send(
      new UpdateShardCountCommand({
        StreamName: streamName,
        TargetShardCount: 1,
        ScalingType: ScalingType.UNIFORM_SCALING,
      })
    );
  });

  it("waitForActive after UpdateShardCount", async () => {
    await waitForActive(streamName);
  });

  it("DeleteStream", async () => {
    await client.send(
      new DeleteStreamCommand({ StreamName: streamName })
    );
  });
});

describe("Stream consumers", () => {
  // SubscribeToShard is SKIPPED — HTTP/1.1 only, no event-stream support
  const streamName = "node-consumers";
  let streamArn: string;

  it("CreateStream", async () => {
    await client.send(
      new CreateStreamCommand({ StreamName: streamName, ShardCount: 1 })
    );
    await waitForActive(streamName);
  });

  it("Get stream ARN", async () => {
    const desc = await client.send(
      new DescribeStreamCommand({ StreamName: streamName })
    );
    streamArn = desc.StreamDescription?.StreamARN!;
    expect(streamArn).toBeTruthy();
  });

  it("RegisterStreamConsumer", async () => {
    const resp = await client.send(
      new RegisterStreamConsumerCommand({
        StreamARN: streamArn,
        ConsumerName: "node-consumer-1",
      })
    );
    expect(resp.Consumer?.ConsumerName).toBe("node-consumer-1");
  });

  it("DescribeStreamConsumer", async () => {
    // Poll until consumer is ACTIVE (up to 30 * 200ms = 6s)
    let resp;
    for (let i = 0; i < 30; i++) {
      resp = await client.send(
        new DescribeStreamConsumerCommand({
          StreamARN: streamArn,
          ConsumerName: "node-consumer-1",
        })
      );
      if (resp.ConsumerDescription?.ConsumerStatus === "ACTIVE") break;
      await new Promise((r) => setTimeout(r, 200));
    }
    expect(resp!.ConsumerDescription?.ConsumerStatus).toBe("ACTIVE");
    expect(resp!.ConsumerDescription?.ConsumerName).toBe("node-consumer-1");
  });

  it("ListStreamConsumers", async () => {
    const resp = await client.send(
      new ListStreamConsumersCommand({ StreamARN: streamArn })
    );
    expect(resp.Consumers!.length).toBeGreaterThanOrEqual(1);
    const names = resp.Consumers!.map((c) => c.ConsumerName);
    expect(names).toContain("node-consumer-1");
  });

  it("DeregisterStreamConsumer", async () => {
    await client.send(
      new DeregisterStreamConsumerCommand({
        StreamARN: streamArn,
        ConsumerName: "node-consumer-1",
      })
    );
  });

  it("DeleteStream", async () => {
    await client.send(
      new DeleteStreamCommand({ StreamName: streamName })
    );
  });
});

describe("Resource policies", () => {
  const streamName = "node-policies";
  let streamArn: string;

  it("CreateStream", async () => {
    await client.send(
      new CreateStreamCommand({ StreamName: streamName, ShardCount: 1 })
    );
    await waitForActive(streamName);
  });

  it("Get stream ARN", async () => {
    const desc = await client.send(
      new DescribeStreamCommand({ StreamName: streamName })
    );
    streamArn = desc.StreamDescription?.StreamARN!;
    expect(streamArn).toBeTruthy();
  });

  it("PutResourcePolicy", async () => {
    const policy = JSON.stringify({
      Version: "2012-10-17",
      Statement: [
        {
          Sid: "AllowGetRecords",
          Effect: "Allow",
          Principal: { AWS: "arn:aws:iam::123456789012:root" },
          Action: "kinesis:GetRecords",
          Resource: streamArn,
        },
      ],
    });
    await client.send(
      new PutResourcePolicyCommand({
        ResourceARN: streamArn,
        Policy: policy,
      })
    );
  });

  it("GetResourcePolicy — verify non-empty", async () => {
    const resp = await client.send(
      new GetResourcePolicyCommand({ ResourceARN: streamArn })
    );
    expect(resp.Policy).toBeTruthy();
  });

  it("DeleteResourcePolicy", async () => {
    await client.send(
      new DeleteResourcePolicyCommand({ ResourceARN: streamArn })
    );
  });

  it("GetResourcePolicy — verify empty", async () => {
    const resp = await client.send(
      new GetResourcePolicyCommand({ ResourceARN: streamArn })
    );
    expect(resp.Policy).toBeFalsy();
  });

  it("DeleteStream", async () => {
    await client.send(
      new DeleteStreamCommand({ StreamName: streamName })
    );
  });
});
