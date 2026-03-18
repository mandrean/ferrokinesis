#!/bin/sh
# Wait for the ferrokinesis server to become ready.
# Usage: ./wait-for-server.sh [port]

set -e

PORT="${1:-${FERROKINESIS_PORT:-4567}}"
URL="http://localhost:${PORT}/_health/ready"
MAX_ATTEMPTS=30
ATTEMPT=0

while [ "$ATTEMPT" -lt "$MAX_ATTEMPTS" ]; do
  ATTEMPT=$((ATTEMPT + 1))
  if curl -sf "$URL" > /dev/null 2>&1; then
    echo "Server ready on port ${PORT} (attempt ${ATTEMPT}/${MAX_ATTEMPTS})"
    exit 0
  fi
  echo "Waiting for server on port ${PORT}... (attempt ${ATTEMPT}/${MAX_ATTEMPTS})"
  sleep 1
done

echo "Server failed to become ready after ${MAX_ATTEMPTS} attempts"
exit 1
