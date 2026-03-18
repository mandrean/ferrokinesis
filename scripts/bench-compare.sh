#!/usr/bin/env bash
# bench-compare.sh — Run a goose load test against ferrokinesis and (optionally) kinesalite.
set -euo pipefail

USERS="${BENCH_USERS:-10}"
STARTUP_TIME="${BENCH_STARTUP_TIME:-5s}"
RUN_TIME="${BENCH_RUN_TIME:-30s}"
FERRO_PORT=4567
KINESALITE_PORT=4568
MAX_WAIT=30  # seconds to wait for a server port to become reachable

wait_for_port() {
    local port=$1
    local elapsed=0
    while ! nc -z localhost "$port" 2>/dev/null; do
        if (( elapsed >= MAX_WAIT )); then
            echo "ERROR: port $port not reachable after ${MAX_WAIT}s" >&2
            return 1
        fi
        sleep 0.5
        (( elapsed++ )) || true
    done
}

cleanup() {
    echo "Cleaning up…"
    [[ -n "${FERRO_PID:-}" ]] && kill "$FERRO_PID" 2>/dev/null || true
    [[ -n "${KINESALITE_PID:-}" ]] && kill "$KINESALITE_PID" 2>/dev/null || true
    wait 2>/dev/null || true
}
trap cleanup EXIT

echo "==> Building loadtest binary (release)…"
cargo build --release --features loadtest

LOADTEST="./target/release/loadtest"

# ── ferrokinesis ─────────────────────────────────────────────────────────────

echo "==> Starting ferrokinesis on port $FERRO_PORT…"
./target/release/ferrokinesis --port "$FERRO_PORT" \
    --create-stream-ms 0 --delete-stream-ms 0 &
FERRO_PID=$!
wait_for_port "$FERRO_PORT"

echo "==> Running load test against ferrokinesis…"
"$LOADTEST" \
    --host "http://localhost:$FERRO_PORT" \
    --users "$USERS" \
    --startup-time "$STARTUP_TIME" \
    --run-time "$RUN_TIME" \
    --report-file ferro-report.html || true

kill "$FERRO_PID" 2>/dev/null || true
wait "$FERRO_PID" 2>/dev/null || true
unset FERRO_PID

# ── kinesalite (optional) ───────────────────────────────────────────────────

if command -v npx &>/dev/null; then
    echo "==> Starting kinesalite on port $KINESALITE_PORT…"
    npx kinesalite --port "$KINESALITE_PORT" \
        --createStreamMs 0 --deleteStreamMs 0 &
    KINESALITE_PID=$!
    wait_for_port "$KINESALITE_PORT"

    echo "==> Running load test against kinesalite…"
    "$LOADTEST" \
        --host "http://localhost:$KINESALITE_PORT" \
        --users "$USERS" \
        --startup-time "$STARTUP_TIME" \
        --run-time "$RUN_TIME" \
        --report-file kinesalite-report.html || true

    kill "$KINESALITE_PID" 2>/dev/null || true
    wait "$KINESALITE_PID" 2>/dev/null || true
    unset KINESALITE_PID
else
    echo "==> kinesalite not found (npx not available); skipping comparison."
fi

echo "==> Done. HTML reports: ferro-report.html, kinesalite-report.html"
