#!/usr/bin/env bash
# ferrokinesis quickstart — AWS CLI over TLS
#
# Prerequisites:
#   - AWS CLI v2 installed
#   - ferrokinesis running with TLS:
#       cargo install ferrokinesis --features tls
#       ferrokinesis generate-cert
#       ferrokinesis --tls-cert cert.pem --tls-key key.pem
#
# Usage:
#   ./quickstart-tls.sh
#   ENDPOINT_URL=https://localhost:5000 ./quickstart-tls.sh
set -euo pipefail

ENDPOINT_URL="${ENDPOINT_URL:-https://localhost:4567}"
REGION="us-east-1"
STREAM="example-stream-tls"

aws() { command aws --endpoint-url "$ENDPOINT_URL" --no-verify-ssl --region "$REGION" "$@"; }

echo "==> CreateStream"
aws kinesis create-stream --stream-name "$STREAM" --shard-count 2

echo "==> PutRecord"
aws kinesis put-record \
    --stream-name "$STREAM" \
    --partition-key pk1 \
    --data "$(echo -n 'hello world' | base64)"

echo "==> GetRecords (all shards)"
aws kinesis list-shards --stream-name "$STREAM" \
  | jq -r '.Shards[].ShardId' \
  | while read -r shard; do
      iter=$(aws kinesis get-shard-iterator \
        --stream-name "$STREAM" --shard-id "$shard" \
        --shard-iterator-type TRIM_HORIZON \
        | jq -r '.ShardIterator')
      aws kinesis get-records --shard-iterator "$iter"
    done \
  | jq -s '[.[].Records[] | {SequenceNumber, Data, DataDecoded: (.Data | @base64d), PartitionKey}]'

echo "==> DeleteStream"
aws kinesis delete-stream --stream-name "$STREAM"

echo "Done."
