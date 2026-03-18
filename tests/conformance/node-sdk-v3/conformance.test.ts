import { describe, it, expect } from "vitest";
import {
  KinesisClient,
  CreateStreamCommand,
  DescribeStreamCommand,
  ListStreamsCommand,
  PutRecordCommand,
  PutRecordsCommand,
  GetShardIteratorCommand,
  GetRecordsCommand,
  DeleteStreamCommand,
  ShardIteratorType,
} from "@aws-sdk/client-kinesis";
import { NodeHttpHandler } from "@smithy/node-http-handler";

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
  afterAll(async () => {
    try {
      await client.send(new DeleteStreamCommand({ StreamName: STREAM_NAME }));
    } catch {
      // stream may not exist if creation failed
    }
  });

  it("completes the 7-operation lifecycle", async () => {
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
  });
});
