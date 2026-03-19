// ferrokinesis quickstart — AWS SDK for Node.js (v3)
//
// Prerequisites:
//   npm install
//   ferrokinesis running (docker run -p 4567:4567 ghcr.io/mandrean/ferrokinesis)
//
// Usage:
//   node quickstart.mjs
//   KINESIS_ENDPOINT=http://localhost:5000 node quickstart.mjs

import {
  KinesisClient,
  CreateStreamCommand,
  DescribeStreamCommand,
  PutRecordCommand,
  GetShardIteratorCommand,
  GetRecordsCommand,
  DeleteStreamCommand,
} from "@aws-sdk/client-kinesis";
import { NodeHttpHandler } from "@smithy/node-http-handler";

const ENDPOINT = process.env.KINESIS_ENDPOINT || "http://localhost:4567";
const STREAM = "node-example";

const client = new KinesisClient({
  endpoint: ENDPOINT,
  region: "us-east-1",
  credentials: { accessKeyId: "test", secretAccessKey: "test" },
  requestHandler: new NodeHttpHandler(),
});

async function waitForActive(streamName) {
  for (let i = 0; i < 30; i++) {
    const resp = await client.send(
      new DescribeStreamCommand({ StreamName: streamName })
    );
    if (resp.StreamDescription?.StreamStatus === "ACTIVE") return;
    await new Promise((r) => setTimeout(r, 200));
  }
  throw new Error(`Stream ${streamName} did not become ACTIVE`);
}

// Create a stream
console.log("==> CreateStream");
await client.send(
  new CreateStreamCommand({ StreamName: STREAM, ShardCount: 2 })
);
await waitForActive(STREAM);

// Put a record
console.log("==> PutRecord");
const put = await client.send(
  new PutRecordCommand({
    StreamName: STREAM,
    Data: new TextEncoder().encode("hello world"),
    PartitionKey: "pk1",
  })
);

// Get records
console.log("==> GetRecords");
const iterResp = await client.send(
  new GetShardIteratorCommand({
    StreamName: STREAM,
    ShardId: put.ShardId,
    ShardIteratorType: "TRIM_HORIZON",
  })
);

const records = await client.send(
  new GetRecordsCommand({ ShardIterator: iterResp.ShardIterator })
);

for (const record of records.Records) {
  const data = new TextDecoder().decode(record.Data);
  console.log(`${record.PartitionKey}: ${data}`);
}

// Clean up
console.log("==> DeleteStream");
await client.send(new DeleteStreamCommand({ StreamName: STREAM }));

console.log("Done.");
