#!/usr/bin/env bash
#
# Captures real AWS Kinesis responses as golden files for conformance testing.
#
# Usage:
#   AWS_PROFILE=your-profile ./scripts/capture-aws-golden.sh
#
# Requirements:
#   - AWS CLI v2 installed and configured
#   - Valid AWS credentials with Kinesis permissions
#   - jq installed
#
# This creates/deletes a test stream (ferrokinesis-conformance-test) and
# saves responses to tests/golden/. Existing golden files are overwritten.
#
set -euo pipefail

STREAM="ferrokinesis-conformance-test"
SHARD_COUNT=1
REGION="${AWS_REGION:-us-east-1}"
DIR="$(cd "$(dirname "$0")/.." && pwd)/tests/golden"

log() { echo "==> $*" >&2; }
capture() {
    local file="$1"; shift
    log "Capturing $file"
    "$@" > "$DIR/$file.raw.json" 2>/dev/null || true
}

mkdir -p "$DIR/happy" "$DIR/error"

# ── Cleanup from prior runs ──────────────────────────────────────────────────
log "Deleting any leftover stream..."
aws kinesis delete-stream --stream-name "$STREAM" --region "$REGION" 2>/dev/null || true
aws kinesis wait stream-not-exists --stream-name "$STREAM" --region "$REGION" 2>/dev/null || true

# ── Happy paths ──────────────────────────────────────────────────────────────

log "CreateStream"
aws kinesis create-stream --stream-name "$STREAM" --shard-count "$SHARD_COUNT" --region "$REGION"
aws kinesis wait stream-exists --stream-name "$STREAM" --region "$REGION"

log "DescribeStream"
aws kinesis describe-stream --stream-name "$STREAM" --region "$REGION" --output json \
    > "$DIR/happy/describe_stream.raw.json"

log "DescribeStreamSummary"
aws kinesis describe-stream-summary --stream-name "$STREAM" --region "$REGION" --output json \
    > "$DIR/happy/describe_stream_summary.raw.json"

log "ListStreams"
aws kinesis list-streams --region "$REGION" --output json \
    > "$DIR/happy/list_streams.raw.json"

log "PutRecord"
aws kinesis put-record \
    --stream-name "$STREAM" \
    --data "dGVzdA==" \
    --partition-key "pk1" \
    --region "$REGION" --output json \
    > "$DIR/happy/put_record.raw.json"

log "PutRecords"
aws kinesis put-records \
    --stream-name "$STREAM" \
    --records "Data=dGVzdA==,PartitionKey=pk1" \
    --region "$REGION" --output json \
    > "$DIR/happy/put_records.raw.json"

SHARD_ID="shardId-000000000000"

for TYPE in TRIM_HORIZON LATEST AT_TIMESTAMP; do
    FNAME=$(echo "$TYPE" | tr '[:upper:]' '[:lower:]')
    log "GetShardIterator ($TYPE)"
    EXTRA_ARGS=""
    if [ "$TYPE" = "AT_TIMESTAMP" ]; then
        EXTRA_ARGS="--timestamp 0"
    fi
    aws kinesis get-shard-iterator \
        --stream-name "$STREAM" \
        --shard-id "$SHARD_ID" \
        --shard-iterator-type "$TYPE" \
        $EXTRA_ARGS \
        --region "$REGION" --output json \
        > "$DIR/happy/get_shard_iterator_${FNAME}.raw.json"
done

# For AT/AFTER_SEQUENCE_NUMBER we need a real sequence number
SEQ_NUM=$(jq -r '.SequenceNumber' "$DIR/happy/put_record.raw.json")

for TYPE in AT_SEQUENCE_NUMBER AFTER_SEQUENCE_NUMBER; do
    FNAME=$(echo "$TYPE" | tr '[:upper:]' '[:lower:]')
    log "GetShardIterator ($TYPE)"
    aws kinesis get-shard-iterator \
        --stream-name "$STREAM" \
        --shard-id "$SHARD_ID" \
        --shard-iterator-type "$TYPE" \
        --starting-sequence-number "$SEQ_NUM" \
        --region "$REGION" --output json \
        > "$DIR/happy/get_shard_iterator_${FNAME}.raw.json"
done

log "GetRecords"
ITER=$(jq -r '.ShardIterator' "$DIR/happy/get_shard_iterator_trim_horizon.raw.json")
aws kinesis get-records \
    --shard-iterator "$ITER" \
    --region "$REGION" --output json \
    > "$DIR/happy/get_records.raw.json"

log "ListShards"
aws kinesis list-shards \
    --stream-name "$STREAM" \
    --region "$REGION" --output json \
    > "$DIR/happy/list_shards.raw.json"

# ── Error paths ──────────────────────────────────────────────────────────────

log "CreateStream (already exists)"
aws kinesis create-stream --stream-name "$STREAM" --shard-count 1 --region "$REGION" 2>"$DIR/error/create_stream_already_exists.raw.json" || true

log "DescribeStream (not found)"
aws kinesis describe-stream --stream-name "nonexistent-stream-xyz" --region "$REGION" 2>"$DIR/error/describe_stream_not_found.raw.json" || true

log "PutRecord (stream not found)"
aws kinesis put-record --stream-name "nonexistent-stream-xyz" --data "dGVzdA==" --partition-key "pk1" --region "$REGION" 2>"$DIR/error/put_record_stream_not_found.raw.json" || true

# ── Cleanup ──────────────────────────────────────────────────────────────────
log "Cleaning up stream..."
aws kinesis delete-stream --stream-name "$STREAM" --region "$REGION"

log "Done! Raw responses saved to $DIR/"
log "Review .raw.json files and convert to golden format manually."
log "Capture date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
log "AWS CLI version: $(aws --version)"
log "Region: $REGION"
