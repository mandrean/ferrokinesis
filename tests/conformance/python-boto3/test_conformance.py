import json
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


def test_tagging(client):
    stream_name = "python-tags"

    # 1. CreateStream
    client.create_stream(StreamName=stream_name, ShardCount=1)

    # 2. Wait for ACTIVE
    wait_for_active(client, stream_name)

    # 3. DescribeLimits
    limits = client.describe_limits()
    assert "ShardLimit" in limits
    assert "OpenShardCount" in limits

    # 4. DescribeStreamSummary
    summary = client.describe_stream_summary(StreamName=stream_name)
    assert summary["StreamDescriptionSummary"]["StreamName"] == stream_name
    assert summary["StreamDescriptionSummary"]["OpenShardCount"] == 1

    # 5. AddTagsToStream
    client.add_tags_to_stream(
        StreamName=stream_name,
        Tags={"env": "test", "team": "platform"},
    )

    # 6. ListTagsForStream — verify 2 tags
    tags_resp = client.list_tags_for_stream(StreamName=stream_name)
    assert len(tags_resp["Tags"]) == 2

    # 7. RemoveTagsFromStream
    client.remove_tags_from_stream(StreamName=stream_name, TagKeys=["team"])

    # 8. ListTagsForStream — verify 1 tag remains
    tags_resp = client.list_tags_for_stream(StreamName=stream_name)
    assert len(tags_resp["Tags"]) == 1
    assert tags_resp["Tags"][0]["Key"] == "env"

    # 9. Get stream ARN
    desc = client.describe_stream(StreamName=stream_name)
    arn = desc["StreamDescription"]["StreamARN"]

    # 10. TagResource
    client.tag_resource(
        ResourceARN=arn,
        Tags={"version": "1"},
    )

    # 11. ListTagsForResource — verify "version" tag
    tags_resp = client.list_tags_for_resource(ResourceARN=arn)
    tag_keys = [t["Key"] for t in tags_resp["Tags"]]
    assert "version" in tag_keys

    # 12. UntagResource
    client.untag_resource(ResourceARN=arn, TagKeys=["version"])

    # 13. DeleteStream
    client.delete_stream(StreamName=stream_name)


def test_stream_config(client):
    stream_name = "python-config"

    # 1. CreateStream + wait for ACTIVE
    client.create_stream(StreamName=stream_name, ShardCount=1)
    wait_for_active(client, stream_name)

    # 2. IncreaseStreamRetentionPeriod
    client.increase_stream_retention_period(
        StreamName=stream_name, RetentionPeriodHours=48
    )

    # 3. DescribeStream — verify RetentionPeriodHours=48
    desc = client.describe_stream(StreamName=stream_name)
    assert desc["StreamDescription"]["RetentionPeriodHours"] == 48

    # 4. DecreaseStreamRetentionPeriod
    client.decrease_stream_retention_period(
        StreamName=stream_name, RetentionPeriodHours=24
    )

    # 5. StartStreamEncryption
    client.start_stream_encryption(
        StreamName=stream_name,
        EncryptionType="KMS",
        KeyId="alias/aws/kinesis",
    )

    # 6. StopStreamEncryption
    client.stop_stream_encryption(
        StreamName=stream_name,
        EncryptionType="KMS",
        KeyId="alias/aws/kinesis",
    )

    # 7. EnableEnhancedMonitoring
    client.enable_enhanced_monitoring(
        StreamName=stream_name,
        ShardLevelMetrics=["IncomingBytes"],
    )

    # 8. DisableEnhancedMonitoring
    client.disable_enhanced_monitoring(
        StreamName=stream_name,
        ShardLevelMetrics=["IncomingBytes"],
    )

    # 9. Get stream ARN
    desc = client.describe_stream(StreamName=stream_name)
    arn = desc["StreamDescription"]["StreamARN"]

    # 10. UpdateStreamMode
    client.update_stream_mode(
        StreamARN=arn,
        StreamModeDetails={"StreamMode": "ON_DEMAND"},
    )

    # 11. DescribeStream — verify StreamMode == ON_DEMAND
    wait_for_active(client, stream_name)
    desc = client.describe_stream(StreamName=stream_name)
    assert (
        desc["StreamDescription"]["StreamModeDetails"]["StreamMode"] == "ON_DEMAND"
    )

    # 12. DeleteStream
    client.delete_stream(StreamName=stream_name)


def test_shard_management(client):
    stream_name = "python-shards"

    # 1. CreateStream with 2 shards + wait for ACTIVE
    client.create_stream(StreamName=stream_name, ShardCount=2)
    wait_for_active(client, stream_name)

    # 2. ListShards — verify 2 shards; get shard-0's HashKeyRange
    shards_resp = client.list_shards(StreamName=stream_name)
    shards = shards_resp["Shards"]
    assert len(shards) == 2

    shard_0 = next(s for s in shards if s["ShardId"] == "shardId-000000000000")
    starting = int(shard_0["HashKeyRange"]["StartingHashKey"])
    ending = int(shard_0["HashKeyRange"]["EndingHashKey"])

    # 3. SplitShard — split shard-0 at the midpoint
    midpoint = (starting + ending) // 2
    client.split_shard(
        StreamName=stream_name,
        ShardToSplit="shardId-000000000000",
        NewStartingHashKey=str(midpoint),
    )

    # 4. Wait for ACTIVE
    wait_for_active(client, stream_name)

    # 5. ListShards — verify total >= 4 shards
    shards_resp = client.list_shards(StreamName=stream_name)
    assert len(shards_resp["Shards"]) >= 4

    # 6. MergeShards
    client.merge_shards(
        StreamName=stream_name,
        ShardToMerge="shardId-000000000002",
        AdjacentShardToMerge="shardId-000000000003",
    )

    # 7. Wait for ACTIVE
    wait_for_active(client, stream_name)

    # 8. ListShards — verify merge result
    shards_resp = client.list_shards(StreamName=stream_name)
    assert len(shards_resp["Shards"]) >= 5

    # 9. UpdateShardCount
    client.update_shard_count(
        StreamName=stream_name,
        TargetShardCount=1,
        ScalingType="UNIFORM_SCALING",
    )

    # 10. Wait for ACTIVE
    wait_for_active(client, stream_name)

    # 11. DeleteStream
    client.delete_stream(StreamName=stream_name)


def test_stream_consumers(client):
    stream_name = "python-consumers"

    # 1. CreateStream + wait for ACTIVE
    client.create_stream(StreamName=stream_name, ShardCount=1)
    wait_for_active(client, stream_name)

    # 2. Get stream ARN
    desc = client.describe_stream(StreamName=stream_name)
    arn = desc["StreamDescription"]["StreamARN"]

    # 3. RegisterStreamConsumer
    reg = client.register_stream_consumer(
        StreamARN=arn, ConsumerName="python-test-consumer"
    )
    consumer_arn = reg["Consumer"]["ConsumerARN"]

    # 4. DescribeStreamConsumer — poll until ACTIVE
    consumer_desc = None
    for _ in range(30):
        consumer_desc = client.describe_stream_consumer(ConsumerARN=consumer_arn)
        if consumer_desc["ConsumerDescription"]["ConsumerStatus"] == "ACTIVE":
            break
        time.sleep(0.2)
    assert consumer_desc["ConsumerDescription"]["ConsumerStatus"] == "ACTIVE"
    assert (
        consumer_desc["ConsumerDescription"]["ConsumerName"]
        == "python-test-consumer"
    )

    # 5. ListStreamConsumers — verify consumer in list
    consumers = client.list_stream_consumers(StreamARN=arn)
    consumer_names = [c["ConsumerName"] for c in consumers["Consumers"]]
    assert "python-test-consumer" in consumer_names

    # 6. DeregisterStreamConsumer
    client.deregister_stream_consumer(ConsumerARN=consumer_arn)

    # 7. DeleteStream
    client.delete_stream(StreamName=stream_name)


def test_policies(client):
    stream_name = "python-policies"

    # 1. CreateStream + wait for ACTIVE
    client.create_stream(StreamName=stream_name, ShardCount=1)
    wait_for_active(client, stream_name)

    # 2. Get stream ARN
    desc = client.describe_stream(StreamName=stream_name)
    arn = desc["StreamDescription"]["StreamARN"]

    # 3. PutResourcePolicy
    policy = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "test",
                    "Effect": "Allow",
                    "Principal": {"AWS": "*"},
                    "Action": "kinesis:GetRecords",
                    "Resource": "*",
                }
            ],
        }
    )
    client.put_resource_policy(ResourceARN=arn, Policy=policy)

    # 4. GetResourcePolicy — verify Policy is non-empty
    policy_resp = client.get_resource_policy(ResourceARN=arn)
    assert policy_resp["Policy"]

    # 5. DeleteResourcePolicy
    client.delete_resource_policy(ResourceARN=arn)

    # 6. GetResourcePolicy — verify policy is empty
    policy_resp = client.get_resource_policy(ResourceARN=arn)
    assert not policy_resp["Policy"]

    # 7. DeleteStream
    client.delete_stream(StreamName=stream_name)
