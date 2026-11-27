#!/usr/bin/env bash
#
# The pre-registered FIX-vs-DeepOUCH ABBA run (ADR 0012 §7.2).
#
# It assumes the cluster, `fix-gateway --seats` and `ouch-gateway --seats`
# are already running. It runs FIX -> OUCH -> OUCH -> FIX, writes one JSON
# per arm, and prints the FIX/FIX noise floor plus the OUCH/FIX ratios.
#
#   deploy/protocol-abba.sh --fix-addr 127.0.0.1:9876 \
#       --ouch-addr 127.0.0.1:9880 --seat 1 --token <64 hex> \
#       [--fix-sender CLIENT --fix-target DEEPBOOK] \
#       [--rate 10000] [--samples 100000] [--warmup 5000] [--out DIR]
#
# Build first:
#   cargo build --release -p deepbook-bench --bin fix-bench --bin ouch-bench
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
FIX_ADDR=127.0.0.1:9876
OUCH_ADDR=127.0.0.1:9880
SEAT=""
TOKEN=""
FIX_SENDER=CLIENT
FIX_TARGET=DEEPBOOK
RATE=10000
SAMPLES=100000
WARMUP=5000
OUT=${BENCH_OUT:-$ROOT/target/bench/abba-$(date +%s)}

usage() { sed -n '2,17p' "$0" | sed 's/^# \{0,1\}//'; exit 0; }

while [ $# -gt 0 ]; do
    case "$1" in
        --fix-addr) FIX_ADDR=$2; shift 2 ;;
        --ouch-addr) OUCH_ADDR=$2; shift 2 ;;
        --seat) SEAT=$2; shift 2 ;;
        --token) TOKEN=$2; shift 2 ;;
        --fix-sender) FIX_SENDER=$2; shift 2 ;;
        --fix-target) FIX_TARGET=$2; shift 2 ;;
        --rate) RATE=$2; shift 2 ;;
        --samples) SAMPLES=$2; shift 2 ;;
        --warmup) WARMUP=$2; shift 2 ;;
        --out) OUT=$2; shift 2 ;;
        --help|-h) usage ;;
        *) echo "protocol-abba: unknown option $1" >&2; exit 2 ;;
    esac
done

[ -n "$SEAT" ] || { echo "protocol-abba: --seat is required" >&2; exit 2; }
[ -n "$TOKEN" ] || { echo "protocol-abba: --token is required" >&2; exit 2; }

BIN_DIR=$ROOT/target/release
FIX_BENCH=$BIN_DIR/fix-bench
OUCH_BENCH=$BIN_DIR/ouch-bench
for bin in "$FIX_BENCH" "$OUCH_BENCH"; do
    [ -x "$bin" ] || { echo "protocol-abba: missing $bin; cargo build --release -p deepbook-bench --bin fix-bench --bin ouch-bench" >&2; exit 1; }
done

mkdir -p "$OUT"
run_fix() {
    "$FIX_BENCH" --connect "$FIX_ADDR" --sender "$FIX_SENDER" --target "$FIX_TARGET" \
        --rate "$RATE" --samples "$SAMPLES" --warmup "$WARMUP" --json > "$OUT/$1.json"
}
run_ouch() {
    "$OUCH_BENCH" --connect "$OUCH_ADDR" --seat "$SEAT" --token "$TOKEN" \
        --rate "$RATE" --samples "$SAMPLES" --warmup "$WARMUP" --json > "$OUT/$1.json"
}

echo "protocol-abba: rate=$RATE samples=$SAMPLES warmup=$WARMUP out=$OUT"
run_fix fix-1
run_ouch ouch-1
run_ouch ouch-2
run_fix fix-2

python3 -I - "$OUT" <<'PY'
import json, pathlib, sys
out = pathlib.Path(sys.argv[1])

def read(name):
    with (out / f"{name}.json").open() as f:
        return json.load(f)

arms = {name: read(name) for name in ("fix-1", "ouch-1", "ouch-2", "fix-2")}
for name, arm in arms.items():
    rtt = arm["rtt_ns"]
    print(f"{name:7} completed={arm['completed']:7} rejected={arm['rejected']:5} "
          f"rate={arm['rate_per_s']:9.1f} p50={rtt['p50']/1000:8.1f}us "
          f"p99={rtt['p99']/1000:9.1f}us mean={rtt['mean']/1000:8.1f}us")

fix = [arms["fix-1"]["rtt_ns"], arms["fix-2"]["rtt_ns"]]
ouch = [arms["ouch-1"]["rtt_ns"], arms["ouch-2"]["rtt_ns"]]
avg = lambda xs, key: sum(x[key] for x in xs) / len(xs)
floor = {
    key: abs(fix[0][key] - fix[1][key]) / max(1, min(fix[0][key], fix[1][key]))
    for key in ("p50", "p99", "truncated_mean")
}
print("noise floor (FIX vs FIX): " + ", ".join(f"{k}={v*100:.2f}%" for k, v in floor.items()))
for key in ("p50", "p99", "truncated_mean"):
    f = avg(fix, key)
    o = avg(ouch, key)
    print(f"{key:15} FIX={f/1000:9.1f}us OUCH={o/1000:9.1f}us delta={(o-f)/f*100:+.1f}%")
print("criterion: OUCH E[X|X<p99] and p99 must beat FIX by more than 2x the noise floor;"
      " p99.9 is recorded, not gated")
PY
