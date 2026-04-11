#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'USAGE'
Usage: scripts/ci/memory-harness.sh [options]

Options:
  --output-dir DIR              Output directory for artifacts (required)
  --duration-secs N             Workload duration in seconds (default: 300)
  --sample-interval-secs N      Sampling interval in seconds (default: 5)
  --port N                      Server port (default: 4567)
  --profile NAME                Profile label in summary.json (default: smoke)
  --repo-root DIR               Repository root (default: current directory)
  --state-dir DIR               Durable state directory (default: <output>/state)
  --max-retained-bytes N        Retained-byte cap (default: 268435456)
  --records-per-cycle N         PutRecord calls per cycle (default: 200)
  --payload-bytes N             Raw payload bytes before base64 (default: 256)
  --skip-build                  Skip `cargo build --release --bin ferrokinesis`
USAGE
}

require_cmd() {
    local cmd=$1
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo "missing required command: $cmd" >&2
        exit 2
    fi
}

metric_value_from_text() {
    local text=$1
    local metric=$2
    awk -v metric="$metric" '$1 == metric { value = $2 } END { if (value != "") print value; else print "0" }' <<<"$text"
}

request_json() {
    local target=$1
    local payload=$2
    local out status body
    out="$(mktemp)"
    status="$(
        curl -sS -o "$out" -w "%{http_code}" \
            -H "content-type: application/x-amz-json-1.1" \
            -H "x-amz-target: Kinesis_20131202.${target}" \
            -H "x-amz-date: 20250101T000000Z" \
            -H "authorization: AWS4-HMAC-SHA256 Credential=test/20250101/us-east-1/kinesis/aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=0000000000000000000000000000000000000000000000000000000000000000" \
            --data "$payload" \
            "http://127.0.0.1:${PORT}/" || true
    )"
    body="$(cat "$out")"
    rm -f "$out"

    if [[ "$status" != "200" ]]; then
        echo "request ${target} failed with status ${status}: ${body}" >>"$HARNESS_LOG"
        return 1
    fi
    printf '%s' "$body"
}

wait_until_ready() {
    local max_attempts=${1:-60}
    local attempt=0
    while (( attempt < max_attempts )); do
        attempt=$((attempt + 1))
        if curl -sf "http://127.0.0.1:${PORT}/_health/ready" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    return 1
}

wait_stream_active() {
    local stream_name=$1
    local max_attempts=${2:-30}
    local attempt=0
    while (( attempt < max_attempts )); do
        attempt=$((attempt + 1))
        local body status
        body="$(request_json "DescribeStreamSummary" "{\"StreamName\":\"${stream_name}\"}")" || true
        status="$(jq -r '.StreamDescriptionSummary.StreamStatus // empty' <<<"$body")"
        if [[ "$status" == "ACTIVE" ]]; then
            return 0
        fi
        sleep 1
    done
    return 1
}

wait_stream_deleted() {
    local stream_name=$1
    local max_attempts=${2:-40}
    local attempt=0
    while (( attempt < max_attempts )); do
        attempt=$((attempt + 1))
        local body
        body="$(request_json "ListStreams" "{}")" || true
        if ! jq -e --arg stream_name "$stream_name" '.StreamNames | index($stream_name)' <<<"$body" >/dev/null; then
            return 0
        fi
        sleep 1
    done
    return 1
}

sample_loop() {
    while kill -0 "$SERVER_PID" >/dev/null 2>&1; do
        local ts vmrss_kb vmhwm_kb vmrss_bytes vmhwm_bytes ready_http metrics_text metrics_json
        ts="$(date -u +%s)"
        if [[ -r "/proc/${SERVER_PID}/status" ]]; then
            vmrss_kb="$(awk '/^VmRSS:/ {print $2; exit}' "/proc/${SERVER_PID}/status" 2>/dev/null || echo 0)"
            vmhwm_kb="$(awk '/^VmHWM:/ {print $2; exit}' "/proc/${SERVER_PID}/status" 2>/dev/null || echo 0)"
        else
            vmrss_kb="$(ps -o rss= -p "$SERVER_PID" 2>/dev/null | awk '{print $1}' || echo 0)"
            vmhwm_kb="$vmrss_kb"
        fi
        vmrss_bytes=$((vmrss_kb * 1024))
        vmhwm_bytes=$((vmhwm_kb * 1024))
        echo "${ts},${vmrss_bytes},${vmhwm_bytes}" >>"$RSS_FILE"

        ready_http="$(curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:${PORT}/_health/ready" || echo 000)"
        metrics_text="$(curl -sf "http://127.0.0.1:${PORT}/metrics" || true)"
        metrics_json="$(printf '%s' "$metrics_text" | jq -Rs .)"
        echo "{\"ts\":${ts},\"ready_http\":${ready_http},\"metrics\":${metrics_json}}" >>"$METRICS_FILE"

        sleep "$SAMPLE_INTERVAL_SECS"
    done
}

OUTPUT_DIR=""
DURATION_SECS=300
SAMPLE_INTERVAL_SECS=5
PORT=4567
PROFILE="smoke"
REPO_ROOT="$(pwd)"
STATE_DIR=""
MAX_RETAINED_BYTES=268435456
RECORDS_PER_CYCLE=200
PAYLOAD_BYTES=256
BUILD_BINARY=1

while [[ $# -gt 0 ]]; do
    case "$1" in
        --output-dir)
            OUTPUT_DIR=$2
            shift 2
            ;;
        --duration-secs)
            DURATION_SECS=$2
            shift 2
            ;;
        --sample-interval-secs)
            SAMPLE_INTERVAL_SECS=$2
            shift 2
            ;;
        --port)
            PORT=$2
            shift 2
            ;;
        --profile)
            PROFILE=$2
            shift 2
            ;;
        --repo-root)
            REPO_ROOT=$2
            shift 2
            ;;
        --state-dir)
            STATE_DIR=$2
            shift 2
            ;;
        --max-retained-bytes)
            MAX_RETAINED_BYTES=$2
            shift 2
            ;;
        --records-per-cycle)
            RECORDS_PER_CYCLE=$2
            shift 2
            ;;
        --payload-bytes)
            PAYLOAD_BYTES=$2
            shift 2
            ;;
        --skip-build)
            BUILD_BINARY=0
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "unknown argument: $1" >&2
            usage
            exit 2
            ;;
    esac
done

if [[ -z "$OUTPUT_DIR" ]]; then
    echo "--output-dir is required" >&2
    usage
    exit 2
fi

require_cmd cargo
require_cmd curl
require_cmd jq
require_cmd python3
require_cmd base64

OUTPUT_DIR="$(cd "$(dirname "$OUTPUT_DIR")" && pwd)/$(basename "$OUTPUT_DIR")"
mkdir -p "$OUTPUT_DIR"

HARNESS_LOG="${OUTPUT_DIR}/harness.log"
SERVER_LOG="${OUTPUT_DIR}/server.log"
RSS_FILE="${OUTPUT_DIR}/rss.csv"
METRICS_FILE="${OUTPUT_DIR}/metrics.ndjson"
SUMMARY_FILE="${OUTPUT_DIR}/summary.json"

if [[ -z "$STATE_DIR" ]]; then
    STATE_DIR="${OUTPUT_DIR}/state_dir"
fi
mkdir -p "$STATE_DIR"

echo "timestamp,vm_rss_bytes,vm_hwm_bytes" >"$RSS_FILE"
: >"$METRICS_FILE"
: >"$HARNESS_LOG"
: >"$SERVER_LOG"

cleanup() {
    if [[ -n "${SAMPLER_PID:-}" ]]; then
        kill "$SAMPLER_PID" >/dev/null 2>&1 || true
        wait "$SAMPLER_PID" >/dev/null 2>&1 || true
    fi
    if [[ -n "${SERVER_PID:-}" ]]; then
        kill "$SERVER_PID" >/dev/null 2>&1 || true
        wait "$SERVER_PID" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

{
    echo "profile=${PROFILE}"
    echo "duration_secs=${DURATION_SECS}"
    echo "sample_interval_secs=${SAMPLE_INTERVAL_SECS}"
    echo "repo_root=${REPO_ROOT}"
    echo "state_dir=${STATE_DIR}"
    echo "max_retained_bytes=${MAX_RETAINED_BYTES}"
    echo "records_per_cycle=${RECORDS_PER_CYCLE}"
    echo "payload_bytes=${PAYLOAD_BYTES}"
} >>"$HARNESS_LOG"

cd "$REPO_ROOT"
if [[ "$BUILD_BINARY" == "1" ]]; then
    echo "building release binary..." >>"$HARNESS_LOG"
    cargo build --release --bin ferrokinesis >>"$HARNESS_LOG" 2>&1
fi

SERVER_BIN="${REPO_ROOT}/target/release/ferrokinesis"
if [[ ! -x "$SERVER_BIN" ]]; then
    echo "server binary not found: $SERVER_BIN" >&2
    exit 1
fi

"$SERVER_BIN" \
    --port "$PORT" \
    --state-dir "$STATE_DIR" \
    --snapshot-interval-secs 30 \
    --max-retained-bytes "$MAX_RETAINED_BYTES" \
    --create-stream-ms 0 \
    --delete-stream-ms 0 \
    --update-stream-ms 0 \
    >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!
echo "server_pid=${SERVER_PID}" >>"$HARNESS_LOG"

if ! wait_until_ready 90; then
    echo "server failed readiness probe" >>"$HARNESS_LOG"
    exit 1
fi

sample_loop &
SAMPLER_PID=$!

CYCLES_COMPLETED=0
START_TS="$(date -u +%s)"
END_TS=$((START_TS + DURATION_SECS))
PAYLOAD_B64="$(python3 - "$PAYLOAD_BYTES" <<'PY'
import base64
import sys
size = int(sys.argv[1])
raw = b"A" * size
print(base64.b64encode(raw).decode("ascii"), end="")
PY
)"

while (( $(date -u +%s) < END_TS )); do
    STREAM_NAME="memory-gate-${PROFILE}-${CYCLES_COMPLETED}"
    echo "cycle ${CYCLES_COMPLETED}: stream=${STREAM_NAME}" >>"$HARNESS_LOG"

    request_json "CreateStream" "{\"StreamName\":\"${STREAM_NAME}\",\"ShardCount\":1}" >/dev/null
    wait_stream_active "$STREAM_NAME" 45

    DESCRIBE_BODY="$(request_json "DescribeStream" "{\"StreamName\":\"${STREAM_NAME}\"}")"
    SHARD_ID="$(jq -r '.StreamDescription.Shards[0].ShardId // empty' <<<"$DESCRIBE_BODY")"
    if [[ -z "$SHARD_ID" ]]; then
        echo "failed to resolve shard id for ${STREAM_NAME}" >>"$HARNESS_LOG"
        exit 1
    fi

    for ((i = 0; i < RECORDS_PER_CYCLE; i++)); do
        PARTITION_KEY="pk-${CYCLES_COMPLETED}-${i}"
        request_json "PutRecord" \
            "{\"StreamName\":\"${STREAM_NAME}\",\"PartitionKey\":\"${PARTITION_KEY}\",\"Data\":\"${PAYLOAD_B64}\"}" \
            >/dev/null
    done

    ITER_BODY="$(request_json "GetShardIterator" "{\"StreamName\":\"${STREAM_NAME}\",\"ShardId\":\"${SHARD_ID}\",\"ShardIteratorType\":\"TRIM_HORIZON\"}")"
    SHARD_ITERATOR="$(jq -r '.ShardIterator // empty' <<<"$ITER_BODY")"
    if [[ -z "$SHARD_ITERATOR" ]]; then
        echo "failed to acquire shard iterator for ${STREAM_NAME}" >>"$HARNESS_LOG"
        exit 1
    fi

    for _ in $(seq 1 15); do
        RECORDS_BODY="$(request_json "GetRecords" "{\"ShardIterator\":\"${SHARD_ITERATOR}\",\"Limit\":1000}")"
        SHARD_ITERATOR="$(jq -r '.NextShardIterator // empty' <<<"$RECORDS_BODY")"
        RECORD_COUNT="$(jq -r '.Records | length' <<<"$RECORDS_BODY")"
        if [[ "$RECORD_COUNT" == "0" || -z "$SHARD_ITERATOR" ]]; then
            break
        fi
    done

    request_json "DeleteStream" "{\"StreamName\":\"${STREAM_NAME}\"}" >/dev/null
    wait_stream_deleted "$STREAM_NAME" 60
    CYCLES_COMPLETED=$((CYCLES_COMPLETED + 1))
done

sleep "$SAMPLE_INTERVAL_SECS"

FINAL_METRICS="$(curl -sf "http://127.0.0.1:${PORT}/metrics" || true)"
if [[ -r "/proc/${SERVER_PID}/status" ]]; then
    FINAL_VMHWM_BYTES="$(( $(awk '/^VmHWM:/ {print $2; exit}' "/proc/${SERVER_PID}/status" 2>/dev/null || echo 0) * 1024 ))"
else
    FINAL_VMHWM_BYTES="$(( $(ps -o rss= -p "$SERVER_PID" 2>/dev/null | awk '{print $1}' || echo 0) * 1024 ))"
fi

python3 - "$RSS_FILE" "$METRICS_FILE" "$SUMMARY_FILE" "$PROFILE" "$DURATION_SECS" "$SAMPLE_INTERVAL_SECS" "$CYCLES_COMPLETED" "$FINAL_VMHWM_BYTES" <<'PY'
import json
import statistics
import sys

rss_file, metrics_file, summary_file, profile, duration_secs, sample_interval_secs, cycles_completed, final_vmhwm_bytes = sys.argv[1:]
duration_secs = int(duration_secs)
sample_interval_secs = int(sample_interval_secs)
cycles_completed = int(cycles_completed)
final_vmhwm_bytes = int(final_vmhwm_bytes)

rss_values = []
with open(rss_file, "r", encoding="utf-8") as fh:
    next(fh, None)
    for line in fh:
        line = line.strip()
        if not line:
            continue
        parts = line.split(",")
        if len(parts) != 3:
            continue
        rss_values.append(int(parts[1]))

readiness_failures = 0
rejected_writes_total = 0
retained_bytes_max = 0
retained_records_max = 0
streams_max = 0
open_shards_max = 0
active_iterators_max = 0
replay_complete_last = 0
last_snapshot_timestamp_ms_last = 0

def metric_from_text(text: str, name: str, default: int = 0) -> int:
    value = default
    for raw in text.splitlines():
        raw = raw.strip()
        if not raw or raw.startswith("#"):
            continue
        parts = raw.split()
        if len(parts) != 2:
            continue
        if parts[0] == name:
            try:
                value = int(float(parts[1]))
            except ValueError:
                pass
    return value

with open(metrics_file, "r", encoding="utf-8") as fh:
    for line in fh:
        line = line.strip()
        if not line:
            continue
        sample = json.loads(line)
        ready_http = int(sample.get("ready_http", 0))
        if ready_http != 200:
            readiness_failures += 1
        text = sample.get("metrics", "")
        rejected_writes_total = metric_from_text(text, "ferrokinesis_rejected_writes_total", rejected_writes_total)
        retained_bytes_max = max(retained_bytes_max, metric_from_text(text, "ferrokinesis_retained_bytes", 0))
        retained_records_max = max(retained_records_max, metric_from_text(text, "ferrokinesis_retained_records", 0))
        streams_max = max(streams_max, metric_from_text(text, "ferrokinesis_streams", 0))
        open_shards_max = max(open_shards_max, metric_from_text(text, "ferrokinesis_open_shards", 0))
        active_iterators_max = max(active_iterators_max, metric_from_text(text, "ferrokinesis_active_iterators", 0))
        replay_complete_last = metric_from_text(text, "ferrokinesis_replay_complete", replay_complete_last)
        last_snapshot_timestamp_ms_last = metric_from_text(
            text, "ferrokinesis_last_snapshot_timestamp_ms", last_snapshot_timestamp_ms_last
        )

rss_peak = max(rss_values) if rss_values else 0
rss_median = int(statistics.median(rss_values)) if rss_values else 0

summary = {
    "profile": profile,
    "duration_secs": duration_secs,
    "sample_interval_secs": sample_interval_secs,
    "cycles_completed": cycles_completed,
    "rss_samples_count": len(rss_values),
    "vm_rss_peak_bytes": rss_peak,
    "rss_median_bytes": rss_median,
    "post_cleanup_rss_median_bytes": rss_median,
    "vm_hwm_bytes": max(final_vmhwm_bytes, rss_peak),
    "readiness_failures": readiness_failures,
    "rejected_writes_total": rejected_writes_total,
    "retained_bytes_max": retained_bytes_max,
    "retained_records_max": retained_records_max,
    "streams_max": streams_max,
    "open_shards_max": open_shards_max,
    "active_iterators_max": active_iterators_max,
    "replay_complete_last": replay_complete_last,
    "last_snapshot_timestamp_ms_last": last_snapshot_timestamp_ms_last,
}

with open(summary_file, "w", encoding="utf-8") as fh:
    json.dump(summary, fh, indent=2, sort_keys=True)
    fh.write("\n")
PY

echo "summary written to ${SUMMARY_FILE}" >>"$HARNESS_LOG"
