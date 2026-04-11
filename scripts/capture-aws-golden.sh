#!/usr/bin/env bash
#
# Captures raw signed AWS Kinesis HTTP responses and emits final conformance
# golden files in one pass.
#
# Usage:
#   AWS_PROFILE=your-profile ./scripts/capture-aws-golden.sh
#
# Requirements:
#   - AWS CLI v2 installed and configured
#   - curl with `--aws-sigv4` support
#   - jq installed
#
# The script creates/deletes a test stream (ferrokinesis-conformance-test),
# stores raw HTTP captures under tests/golden/raw/, and writes normalized
# golden fixtures under tests/golden/.
#
set -euo pipefail

STREAM="ferrokinesis-conformance-test"
SHARD_COUNT=1
REGION="${AWS_REGION:-us-east-1}"
AMZ_JSON="application/x-amz-json-1.1"
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DIR="$ROOT_DIR/tests/golden"
RAW_DIR="$DIR/raw"
ENDPOINT="https://kinesis.${REGION}.amazonaws.com/"

log() { echo "==> $*" >&2; }
die() { echo "error: $*" >&2; exit 1; }

require_cmd() {
    command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

ensure_credentials() {
    if [[ -n "${AWS_ACCESS_KEY_ID:-}" && -n "${AWS_SECRET_ACCESS_KEY:-}" ]]; then
        return
    fi

    log "Loading AWS credentials from AWS CLI config"
    # shellcheck disable=SC2046
    eval "$(
        aws ${AWS_PROFILE:+--profile "$AWS_PROFILE"} configure export-credentials --format env
    )"
}

aws_cli() {
    aws ${AWS_PROFILE:+--profile "$AWS_PROFILE"} "$@"
}

normalize_json_file() {
    local input="$1"
    local output="$2"
    local account_id="$3"

    if [[ ! -s "$input" ]]; then
        printf 'null\n' > "$output"
        return
    fi

    jq \
        --arg region "$REGION" \
        --arg account "$account_id" \
        '
        def normalize_string:
            if type == "string" then
                gsub($account; "000000000000")
                | gsub($region; "us-east-1")
            else
                .
            end;
        walk(normalize_string)
        ' \
        "$input" > "$output"
}

capture_request() {
    local target="$1"
    local payload="$2"
    local base="$3"
    local header_file="$RAW_DIR/${base}.headers"
    local body_file="$RAW_DIR/${base}.body.json"
    local status_file="$RAW_DIR/${base}.status"

    local -a curl_args=(
        curl
        --silent
        --show-error
        --location
        --aws-sigv4 "aws:amz:${REGION}:kinesis"
        --user "${AWS_ACCESS_KEY_ID}:${AWS_SECRET_ACCESS_KEY}"
        -H "Content-Type: ${AMZ_JSON}"
        -H "X-Amz-Target: Kinesis_20131202.${target}"
        -D "$header_file"
        -o "$body_file"
        -w '%{http_code}'
        --data "$payload"
        "$ENDPOINT"
    )
    if [[ -n "${AWS_SESSION_TOKEN:-}" ]]; then
        curl_args+=(-H "X-Amz-Security-Token: ${AWS_SESSION_TOKEN}")
    fi

    log "Capturing ${base}"
    "${curl_args[@]}" > "$status_file"
}

header_value() {
    local header_file="$1"
    local header_name="$2"
    awk -F': ' -v header_name="$(printf '%s' "$header_name" | tr '[:upper:]' '[:lower:]')" '
        BEGIN { IGNORECASE = 1 }
        {
            name = tolower($1)
            gsub(/\r/, "", name)
            if (name == header_name) {
                value = $2
                gsub(/\r/, "", value)
                print value
                exit
            }
        }
    ' "$header_file"
}

emit_golden() {
    local base="$1"
    local operation="$2"
    local scenario="$3"
    local destination="$4"
    local account_id="$5"

    local header_file="$RAW_DIR/${base}.headers"
    local body_file="$RAW_DIR/${base}.body.json"
    local status_file="$RAW_DIR/${base}.status"
    local normalized_body
    normalized_body="$(mktemp)"

    normalize_json_file "$body_file" "$normalized_body" "$account_id"

    local http_status
    http_status="$(tr -d '\r\n' < "$status_file")"
    local content_type
    content_type="$(header_value "$header_file" "Content-Type")"
    local error_type
    error_type="$(header_value "$header_file" "x-amzn-ErrorType")"

    jq \
        -n \
        --arg operation "$operation" \
        --arg scenario "$scenario" \
        --argjson http_status "$http_status" \
        --arg content_type "$content_type" \
        --arg error_type "$error_type" \
        --slurpfile body "$normalized_body" \
        '
        {
            operation: $operation,
            scenario: $scenario,
            http_status: $http_status,
            required_headers: [
                "x-amzn-RequestId",
                "x-amz-id-2",
                "Content-Type"
            ],
            body: $body[0],
            expected_headers: (
                { "Content-Type": $content_type }
                + (if $error_type != "" then { "x-amzn-ErrorType": $error_type } else {} end)
            )
        }
        ' > "$destination"

    rm -f "$normalized_body"
}

mkdir -p "$DIR/happy" "$DIR/error" "$RAW_DIR"
require_cmd aws
require_cmd curl
require_cmd jq
ensure_credentials

ACCOUNT_ID="$(aws_cli sts get-caller-identity --query Account --output text)"

log "Deleting any leftover stream..."
aws_cli kinesis delete-stream --stream-name "$STREAM" --region "$REGION" 2>/dev/null || true
aws_cli kinesis wait stream-not-exists --stream-name "$STREAM" --region "$REGION" 2>/dev/null || true

capture_request \
    "CreateStream" \
    "{\"StreamName\":\"${STREAM}\",\"ShardCount\":${SHARD_COUNT}}" \
    "happy/create_stream"
emit_golden \
    "happy/create_stream" \
    "CreateStream" \
    "happy_path" \
    "$DIR/happy/create_stream.json" \
    "$ACCOUNT_ID"
aws_cli kinesis wait stream-exists --stream-name "$STREAM" --region "$REGION"

capture_request \
    "DescribeStream" \
    "{\"StreamName\":\"${STREAM}\"}" \
    "happy/describe_stream"
emit_golden \
    "happy/describe_stream" \
    "DescribeStream" \
    "happy_path" \
    "$DIR/happy/describe_stream.json" \
    "$ACCOUNT_ID"

capture_request \
    "DescribeStreamSummary" \
    "{\"StreamName\":\"${STREAM}\"}" \
    "happy/describe_stream_summary"
emit_golden \
    "happy/describe_stream_summary" \
    "DescribeStreamSummary" \
    "happy_path" \
    "$DIR/happy/describe_stream_summary.json" \
    "$ACCOUNT_ID"

capture_request \
    "ListStreams" \
    "{}" \
    "happy/list_streams"
emit_golden \
    "happy/list_streams" \
    "ListStreams" \
    "happy_path" \
    "$DIR/happy/list_streams.json" \
    "$ACCOUNT_ID"

capture_request \
    "PutRecord" \
    "{\"StreamName\":\"${STREAM}\",\"Data\":\"dGVzdA==\",\"PartitionKey\":\"pk1\"}" \
    "happy/put_record"
emit_golden \
    "happy/put_record" \
    "PutRecord" \
    "happy_path" \
    "$DIR/happy/put_record.json" \
    "$ACCOUNT_ID"

capture_request \
    "PutRecords" \
    "{\"StreamName\":\"${STREAM}\",\"Records\":[{\"Data\":\"dGVzdA==\",\"PartitionKey\":\"pk1\"}]}" \
    "happy/put_records"
emit_golden \
    "happy/put_records" \
    "PutRecords" \
    "happy_path" \
    "$DIR/happy/put_records.json" \
    "$ACCOUNT_ID"

SHARD_ID="shardId-000000000000"
for iterator_type in TRIM_HORIZON LATEST AT_TIMESTAMP; do
    suffix="$(printf '%s' "$iterator_type" | tr '[:upper:]' '[:lower:]')"
    payload="{\"StreamName\":\"${STREAM}\",\"ShardId\":\"${SHARD_ID}\",\"ShardIteratorType\":\"${iterator_type}\""
    if [[ "$iterator_type" == "AT_TIMESTAMP" ]]; then
        payload="${payload},\"Timestamp\":0.0"
    fi
    payload="${payload}}"

    capture_request \
        "GetShardIterator" \
        "$payload" \
        "happy/get_shard_iterator_${suffix}"
    emit_golden \
        "happy/get_shard_iterator_${suffix}" \
        "GetShardIterator" \
        "happy_path" \
        "$DIR/happy/get_shard_iterator_${suffix}.json" \
        "$ACCOUNT_ID"
done

SEQ_NUM="$(jq -r '.SequenceNumber' "$RAW_DIR/happy/put_record.body.json")"
for iterator_type in AT_SEQUENCE_NUMBER AFTER_SEQUENCE_NUMBER; do
    suffix="$(printf '%s' "$iterator_type" | tr '[:upper:]' '[:lower:]')"
    capture_request \
        "GetShardIterator" \
        "{\"StreamName\":\"${STREAM}\",\"ShardId\":\"${SHARD_ID}\",\"ShardIteratorType\":\"${iterator_type}\",\"StartingSequenceNumber\":\"${SEQ_NUM}\"}" \
        "happy/get_shard_iterator_${suffix}"
    emit_golden \
        "happy/get_shard_iterator_${suffix}" \
        "GetShardIterator" \
        "happy_path" \
        "$DIR/happy/get_shard_iterator_${suffix}.json" \
        "$ACCOUNT_ID"
done

ITER="$(jq -r '.ShardIterator' "$RAW_DIR/happy/get_shard_iterator_trim_horizon.body.json")"
capture_request \
    "GetRecords" \
    "{\"ShardIterator\":\"${ITER}\"}" \
    "happy/get_records"
emit_golden \
    "happy/get_records" \
    "GetRecords" \
    "happy_path" \
    "$DIR/happy/get_records.json" \
    "$ACCOUNT_ID"

log "Waiting for iterator to expire so GetRecords expired-iterator golden is authentic..."
sleep 310
capture_request \
    "GetRecords" \
    "{\"ShardIterator\":\"${ITER}\"}" \
    "error/get_records_expired_iterator"
emit_golden \
    "error/get_records_expired_iterator" \
    "GetRecords" \
    "expired_iterator" \
    "$DIR/error/get_records_expired_iterator.json" \
    "$ACCOUNT_ID"

capture_request \
    "ListShards" \
    "{\"StreamName\":\"${STREAM}\"}" \
    "happy/list_shards"
emit_golden \
    "happy/list_shards" \
    "ListShards" \
    "happy_path" \
    "$DIR/happy/list_shards.json" \
    "$ACCOUNT_ID"

capture_request \
    "CreateStream" \
    "{\"StreamName\":\"${STREAM}\",\"ShardCount\":1}" \
    "error/create_stream_already_exists"
emit_golden \
    "error/create_stream_already_exists" \
    "CreateStream" \
    "already_exists" \
    "$DIR/error/create_stream_already_exists.json" \
    "$ACCOUNT_ID"

capture_request \
    "DescribeStream" \
    "{\"StreamName\":\"nonexistent-stream-xyz\"}" \
    "error/describe_stream_not_found"
emit_golden \
    "error/describe_stream_not_found" \
    "DescribeStream" \
    "not_found" \
    "$DIR/error/describe_stream_not_found.json" \
    "$ACCOUNT_ID"

capture_request \
    "PutRecord" \
    "{\"StreamName\":\"nonexistent-stream-xyz\",\"Data\":\"dGVzdA==\",\"PartitionKey\":\"pk1\"}" \
    "error/put_record_stream_not_found"
emit_golden \
    "error/put_record_stream_not_found" \
    "PutRecord" \
    "stream_not_found" \
    "$DIR/error/put_record_stream_not_found.json" \
    "$ACCOUNT_ID"

capture_request \
    "GetShardIterator" \
    "{\"StreamName\":\"${STREAM}\",\"ShardId\":\"shardId-000000000099\",\"ShardIteratorType\":\"TRIM_HORIZON\"}" \
    "error/get_shard_iterator_invalid_shard"
emit_golden \
    "error/get_shard_iterator_invalid_shard" \
    "GetShardIterator" \
    "invalid_shard" \
    "$DIR/error/get_shard_iterator_invalid_shard.json" \
    "$ACCOUNT_ID"

aws_cli kinesis delete-stream --stream-name "$STREAM" --region "$REGION"
aws_cli kinesis wait stream-not-exists --stream-name "$STREAM" --region "$REGION"

log "Done. Raw HTTP captures saved to $RAW_DIR"
log "Capture date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
log "AWS CLI version: $(aws --version 2>&1)"
log "Region: $REGION"
