#!/usr/bin/env python3
"""ferrokinesis quickstart — boto3

Prerequisites:
    pip install -r requirements.txt
    ferrokinesis running (docker run -p 4567:4567 ghcr.io/mandrean/ferrokinesis)

Usage:
    python quickstart.py
    KINESIS_ENDPOINT=http://localhost:4567 python quickstart.py
"""
import os
import time

import boto3

ENDPOINT = os.environ.get("KINESIS_ENDPOINT", "http://localhost:4567")
STREAM = "python-example"

client = boto3.client(
    "kinesis",
    endpoint_url=ENDPOINT,
    region_name="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test",
)


def wait_for_active(stream_name, max_retries=30):
    for _ in range(max_retries):
        resp = client.describe_stream(StreamName=stream_name)
        if resp["StreamDescription"]["StreamStatus"] == "ACTIVE":
            return
        time.sleep(0.2)
    raise TimeoutError(f"Stream {stream_name!r} did not become ACTIVE")


# Create a stream
print("==> CreateStream")
client.create_stream(StreamName=STREAM, ShardCount=2)
wait_for_active(STREAM)

# Put a record
print("==> PutRecord")
put = client.put_record(StreamName=STREAM, Data=b"hello world", PartitionKey="pk1")
shard_id = put["ShardId"]

# Get records
print("==> GetRecords")
iterator = client.get_shard_iterator(
    StreamName=STREAM,
    ShardId=shard_id,
    ShardIteratorType="TRIM_HORIZON",
)["ShardIterator"]

records = client.get_records(ShardIterator=iterator)
for r in records["Records"]:
    print(f"{r['PartitionKey']}: {r['Data'].decode()}")

# Clean up
print("==> DeleteStream")
client.delete_stream(StreamName=STREAM)

print("Done.")
