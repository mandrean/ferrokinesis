import os
import time

import boto3
import pytest


ENDPOINT = os.environ.get("KINESIS_ENDPOINT", "http://localhost:4567")
STREAM_NAME = "python-conformance"


@pytest.fixture(scope="module")
def client():
    return boto3.client(
        "kinesis",
        endpoint_url=ENDPOINT,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def wait_for_active(client, stream, max_retries=30, interval=0.2):
    for _ in range(max_retries):
        resp = client.describe_stream(StreamName=stream)
        status = resp["StreamDescription"]["StreamStatus"]
        if status == "ACTIVE":
            return resp
        time.sleep(interval)
    raise TimeoutError(f"Stream {stream!r} did not become ACTIVE")


def test_conformance(client):
    # 1. CreateStream
    client.create_stream(StreamName=STREAM_NAME, ShardCount=2)

    # 2. DescribeStream — wait for ACTIVE
    desc = wait_for_active(client, STREAM_NAME)
    stream = desc["StreamDescription"]
    assert stream["StreamName"] == STREAM_NAME
    assert stream["StreamStatus"] == "ACTIVE"
    assert len(stream["Shards"]) == 2

    # 3. ListStreams
    listed = client.list_streams()
    assert STREAM_NAME in listed["StreamNames"]

    # 4. PutRecord
    put = client.put_record(
        StreamName=STREAM_NAME,
        Data=b"hello from python",
        PartitionKey="pk-1",
    )
    assert put["ShardId"]
    assert put["SequenceNumber"]
    shard_id = put["ShardId"]

    # 5. PutRecords
    records = client.put_records(
        StreamName=STREAM_NAME,
        Records=[
            {"Data": f"batch-{i}".encode(), "PartitionKey": f"pk-{i}"}
            for i in range(3)
        ],
    )
    assert records["FailedRecordCount"] == 0
    assert len(records["Records"]) == 3

    # 6. GetShardIterator
    shard_iter = client.get_shard_iterator(
        StreamName=STREAM_NAME,
        ShardId=shard_id,
        ShardIteratorType="TRIM_HORIZON",
    )
    iterator = shard_iter["ShardIterator"]
    assert iterator

    # 7. GetRecords
    got = client.get_records(ShardIterator=iterator)
    assert len(got["Records"]) >= 1
    assert got["Records"][0]["Data"] == b"hello from python"

    # 8. DeleteStream
    client.delete_stream(StreamName=STREAM_NAME)
