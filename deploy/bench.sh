#!/usr/bin/env bash
#
# M5 one-click benchmark: one member, the engine container, the FIX gateway
# and the open-loop FIX client, on one machine.
#
#   deploy/bench.sh [--rate N] [--samples N] [--warmup N] [--json]
#                   [--base-port N] [--tick-us N]
#                   [--journal-sync each|batch] [--journal-batch N]
#                   [--admin] [--serve] [--admin-host IP] [--no-seed]
#                   [--keep] [--no-build]
#
# Environment:
#   DEEPBOOK_DEEPMSG_DIR  deepmsg checkout (default: <repo>/../deepmsg)
#   DEEPMSG_PROFILE       release|debug (default: release)
#   BENCH_RUN_DIR         run directory (default: /dev/shm/deepbook-bench-<pid>)
#   BENCH_METRICS         Prometheus text file (default: target/bench/last.prom)
#
# The run directory is on /dev/shm by default so the outbound journal's
# fdatasync is a tmpfs operation; a physical-disk run is a different
# measurement and should use BENCH_RUN_DIR explicitly.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
DEEPMSG_DIR=${DEEPBOOK_DEEPMSG_DIR:-$ROOT/../deepmsg}
PROFILE=${DEEPMSG_PROFILE:-release}
BASE_PORT=${BENCH_BASE_PORT:-26000}
RATE=10000
SAMPLES=50000
WARMUP=5000
TICK_US=200
JOURNAL_BATCH=64
JOURNAL_SYNC=batch
KEEP=0
JSON=0
BUILD=1
ADMIN=0
SERVE=0
SEED=1
ADMIN_PORT=""
ADMIN_HOST=127.0.0.1
BENCH_METRICS=${BENCH_METRICS:-$ROOT/target/bench/last.prom}
RUN_DIR=${BENCH_RUN_DIR:-/dev/shm/deepbook-bench-$$}

usage() {
    sed -n '2,22p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
}

while [ $# -gt 0 ]; do
    case "$1" in
        --rate) RATE=$2; shift 2 ;;
        --samples) SAMPLES=$2; shift 2 ;;
        --warmup) WARMUP=$2; shift 2 ;;
        --base-port) BASE_PORT=$2; shift 2 ;;
        --tick-us) TICK_US=$2; shift 2 ;;
        --journal-batch) JOURNAL_BATCH=$2; shift 2 ;;
        --journal-sync) JOURNAL_SYNC=$2; shift 2 ;;
        --json) JSON=1; shift ;;
        --admin) ADMIN=1; shift ;;
        --serve) ADMIN=1; SERVE=1; shift ;;
        --no-seed) SEED=0; shift ;;
        --admin-port) ADMIN_PORT=$2; shift 2 ;;
        --admin-host) ADMIN_HOST=$2; shift 2 ;;
        --bench-metrics) BENCH_METRICS=$2; shift 2 ;;
        --keep) KEEP=1; shift ;;
        --no-build) BUILD=0; shift ;;
        --help|-h) usage ;;
        *) echo "bench: unknown option $1" >&2; exit 2 ;;
    esac
done

PROFILE_FLAG=()
if [ "$PROFILE" = "release" ]; then
    PROFILE_FLAG=(--release)
fi
PROFILE_DIR=$PROFILE

deepmsg_bin() { echo "$DEEPMSG_DIR/target/$PROFILE_DIR/$1"; }
deepbook_bin() { echo "$ROOT/target/$PROFILE_DIR/$1"; }

missing=0
for name in deepmsg-archiving-media-driver deepmsg-cluster-node deepmsg-cluster-tool; do
    if [ ! -x "$(deepmsg_bin "$name")" ]; then
        echo "bench: missing $(deepmsg_bin "$name")" >&2
        missing=1
    fi
done
if [ "$missing" -ne 0 ]; then
    cat >&2 <<EOF
Build the deepmsg binaries in $DEEPMSG_DIR:

  cargo build --release \\
    -p deepmsg-archive --bin deepmsg-archiving-media-driver \\
    -p deepmsg-cluster --bin deepmsg-cluster-node \\
    -p deepmsg-cluster-tool --bin deepmsg-cluster-tool
EOF
    exit 1
fi

if [ "$BUILD" -eq 1 ]; then
    ( cd "$ROOT" && cargo build "${PROFILE_FLAG[@]}" \
        -p deepbook-gateway --bin fix-gateway \
        -p deepbook-engine-cluster --bin engine-container \
        -p deepbook-bench --bin fix-bench \
        -p deepbook-admin --bin deepbook-admin )
fi

AERON=$RUN_DIR/aeron
ARCHIVE=$RUN_DIR/archive
CLUSTER=$RUN_DIR/cluster
LOGS=$RUN_DIR/logs
SESSIONS=$RUN_DIR/fix-sessions
rm -rf "$RUN_DIR"
mkdir -p "$AERON" "$ARCHIVE" "$CLUSTER" "$LOGS" "$SESSIONS"

MEMBERS="0,localhost:$BASE_PORT,localhost:$((BASE_PORT + 10)),localhost:$((BASE_PORT + 20)),localhost:0,localhost:$((BASE_PORT + 40))"
FIX_PORT=$((BASE_PORT + 50))
CLUSTER_ID=7
if [ -z "$ADMIN_PORT" ]; then
    ADMIN_PORT=$((BASE_PORT + 60))
fi
mkdir -p "$(dirname "$BENCH_METRICS")"

PIDS=()
cleanup() {
    local status=$?
    for pid in "${PIDS[@]:-}"; do
        kill -9 "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
    if [ "$KEEP" -eq 0 ]; then
        rm -rf "$RUN_DIR"
    else
        echo "bench: kept $RUN_DIR" >&2
    fi
    exit "$status"
}
trap cleanup EXIT INT TERM

# Children inherit no DEEPMSG_*/AERON_*: those names beat -D in the driver.
SANITIZE=()
while IFS= read -r name; do
    SANITIZE+=(-u "$name")
done < <(env | grep -E '^(DEEPMSG_|AERON_)' | cut -d= -f1 || true)

DRIVER_ARGS=(
    "-Ddeepmsg.dir=$AERON" "-Daeron.dir=$AERON"
    -Ddeepmsg.dir.delete.on.start=false -Daeron.dir.delete.on.start=false
    -Ddeepmsg.dir.delete.on.shutdown=false -Daeron.dir.delete.on.shutdown=false
    "-Daeron.archive.dir=$ARCHIVE"
    -Daeron.threading.mode=SHARED -Daeron.archive.threading.mode=SHARED
    "-Daeron.archive.control.channel=aeron:udp?endpoint=localhost:$((BASE_PORT + 40))"
    -Daeron.archive.control.response.stream.id=20
    -Daeron.archive.local.control.channel=aeron:ipc?term-length=64k
    -Daeron.archive.local.control.stream.id=10
    -Daeron.conductor.idle.strategy=spin
    -Daeron.sender.idle.strategy=spin
    -Daeron.receiver.idle.strategy=spin
)

CLUSTER_ARGS=(
    "-Daeron.dir=$AERON"
    "-Daeron.cluster.dir=$CLUSTER"
    "-Daeron.cluster.id=$CLUSTER_ID"
    -Daeron.cluster.member.id=0
    "-Daeron.cluster.members=$MEMBERS"
    -Daeron.cluster.ingress.channel=aeron:ipc?term-length=64k
    -Daeron.cluster.egress.channel=aeron:ipc?term-length=64k
    -Daeron.cluster.consensus.channel=aeron:ipc?term-length=128k
    -Daeron.cluster.log.channel=aeron:ipc?term-length=512k
    -Daeron.cluster.control.channel=aeron:ipc?term-length=128k
    -Daeron.cluster.replication.channel=aeron:udp?endpoint=localhost:0
    -Daeron.cluster.leader.heartbeat.timeout=30s
    -Daeron.cluster.service.count=1
    -Daeron.cluster.service.id=0
    -Daeron.cluster.service.responder=true
    -Daeron.cluster.service.stream.id=104
    -Daeron.cluster.consensus.module.stream.id=105
    -Daeron.cluster.replay.channel=aeron:ipc
    -Daeron.cluster.replay.stream.id=103
    -Daeron.cluster.snapshot.channel=aeron:ipc?alias=snapshot
    -Daeron.cluster.snapshot.stream.id=106
    -Daeron.archive.local.control.channel=aeron:ipc?term-length=64k
    -Daeron.archive.local.control.stream.id=10
    -Daeron.archive.control.response.stream.id=20
    -Daeron.cluster.idle.strategy=org.agrona.concurrent.BusySpinIdleStrategy
)

LAST_PID=""
start() {
    local log=$1
    shift
    "${SANITIZE[@]}" "$@" >"$log" 2>&1 &
    LAST_PID=$!
    PIDS+=("$LAST_PID")
}

wait_for_line() {
    local path=$1 pattern=$2 timeout=${3:-20}
    local waited=0
    while [ "$waited" -lt "$((timeout * 100))" ]; do
        grep -q "$pattern" "$path" 2>/dev/null && return 0
        sleep 0.01
        waited=$((waited + 1))
    done
    echo "bench: $path did not show '$pattern'" >&2
    tail -40 "$path" >&2 || true
    return 1
}

echo "== machine =="
uname -srm
if command -v lscpu >/dev/null 2>&1; then
    lscpu | grep -E 'Model name|^CPU\(s\)|Thread|Core|Socket' || true
fi
echo "rustc: $(rustc --version 2>/dev/null || echo unknown)"
echo "commit: $(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)"
echo "io_uring_disabled: $(cat /proc/sys/kernel/io_uring_disabled 2>/dev/null || echo unknown)"
echo "binaries:"
sha256sum "$(deepbook_bin fix-gateway)" "$(deepbook_bin engine-container)" \
    "$(deepbook_bin fix-bench)" "$(deepbook_bin deepbook-admin)" 2>/dev/null || true
echo

echo "== start =="
start "$LOGS/driver.log" "$(deepmsg_bin deepmsg-archiving-media-driver)" "${DRIVER_ARGS[@]}"
for _ in $(seq 1 2000); do
    [ -f "$AERON/cnc.dat" ] && grep -q "archive running" "$LOGS/driver.log" && break
    sleep 0.01
done
if ! grep -q "archive running" "$LOGS/driver.log"; then
    echo "bench: the driver did not start" >&2
    tail -40 "$LOGS/driver.log" >&2
    exit 1
fi

start "$LOGS/node.log" "$(deepmsg_bin deepmsg-cluster-node)" "${CLUSTER_ARGS[@]}"
sleep 0.2
start "$LOGS/container.log" "$(deepbook_bin engine-container)" "${CLUSTER_ARGS[@]}" \
    -Ddeepbook.engine.symbol=1 \
    -Ddeepbook.engine.max-orders=1048576 \
    -Ddeepbook.engine.recent-window=1000000
wait_for_line "$LOGS/container.log" "STARTED service=deepbook-engine" 20

GATEWAY_PID=""
for attempt in $(seq 1 20); do
    start "$LOGS/gateway-$attempt.log" "$(deepbook_bin fix-gateway)" \
        --bind "127.0.0.1:$FIX_PORT" \
        --session-dir "$SESSIONS" \
        --cluster --aeron-dir "$AERON" \
        --tick-us "$TICK_US" \
        --journal-sync "$JOURNAL_SYNC" --journal-batch "$JOURNAL_BATCH" \
        --symbol AAPL --symbol-id 1 \
        --new-leader-timeout-ms 4000
    GATEWAY_PID=$LAST_PID
    if wait_for_line "$LOGS/gateway-$attempt.log" "listening on" 5; then
        break
    fi
    kill -9 "$GATEWAY_PID" 2>/dev/null || true
    sleep 0.2
done
if ! grep -q "listening on" "$LOGS/gateway-$attempt.log" 2>/dev/null; then
    echo "bench: the gateway did not start" >&2
    tail -40 "$LOGS/gateway-$attempt.log" >&2 || true
    exit 1
fi

if [ "$ADMIN" -eq 1 ]; then
    start "$LOGS/admin.log" "$(deepbook_bin deepbook-admin)" \
        --listen "$ADMIN_HOST:$ADMIN_PORT" \
        --aeron-dir "$AERON" \
        --ui-dir "$ROOT/crates/ui" \
        --cluster-id "$CLUSTER_ID" \
        --bench-metrics "$BENCH_METRICS"
    wait_for_line "$LOGS/admin.log" "listening on" 10
    echo "== admin: http://$ADMIN_HOST:$ADMIN_PORT =="
fi

# The cluster module becomes ACTIVE only after the service acks; the gateway's
# connect is the last readiness check.
echo "== bench: rate=$RATE/s samples=$SAMPLES warmup=$WARMUP tick=${TICK_US}us journal-sync=${JOURNAL_SYNC} journal-batch=${JOURNAL_BATCH} =="
BENCH_ARGS=(
    --connect "127.0.0.1:$FIX_PORT"
    --rate "$RATE"
    --samples "$SAMPLES"
    --warmup "$WARMUP"
    --account 1
    --price 1.00
    --qty 1
    --timeout-ms 120000
    --prometheus "$BENCH_METRICS"
)
if [ "$JSON" -eq 1 ]; then
    BENCH_ARGS+=(--json)
fi
if [ "$SEED" -eq 1 ]; then
    BENCH_ARGS+=(--seed-book)
fi
"$(deepbook_bin fix-bench)" "${BENCH_ARGS[@]}"

if [ "$SERVE" -eq 1 ]; then
    echo
    echo "dashboard: http://$ADMIN_HOST:$ADMIN_PORT   metrics: http://$ADMIN_HOST:$ADMIN_PORT/metrics"
    echo "press Ctrl-C to stop"
    # The trap kills the children; wait keeps the stack alive.
    while :; do sleep 1; done
fi
