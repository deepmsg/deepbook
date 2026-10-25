#!/usr/bin/env bash
# M1d: QuickFIX/J conformance against the real fix-gateway process.
#
#   1. QuickFIX/J logs on, sends a NewOrderSingle and checks the
#      ExecutionReport.
#   2. A byte-level probe requests a resend: the admin Logon reply is
#      gap-filled, the application reports are replayed with 43=Y and 122.
#   3. With a one-entry journal, a resend that starts outside the window is
#      answered with Logout.
#
# The QuickFIX/J jars are fetched into target/quickfixj (not committed). The
# gateway's session directories are fresh per scenario.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SRC="$ROOT/tests/fix_conformance"
WORK="$ROOT/target/fix_conformance"
JARS="$ROOT/target/quickfixj"
CLASSES="$WORK/classes"
MIRROR="https://maven-central.storage-download.googleapis.com/maven2"

QFJ="quickfixj-all-2.3.1.jar"
MINA="mina-core-2.1.4.jar"
SLF4J_API="slf4j-api-1.7.36.jar"
SLF4J_SIMPLE="slf4j-simple-1.7.36.jar"
CP="$JARS/$QFJ:$JARS/$MINA:$JARS/$SLF4J_API:$JARS/$SLF4J_SIMPLE"

GATEWAY_PID=""
PORT=""

fetch() {
    local name="$1" path="$2"
    if [ ! -s "$JARS/$name" ]; then
        echo "fetching $name"
        mkdir -p "$JARS"
        curl -fsSL --max-time 180 -o "$JARS/$name" "$MIRROR/$path"
    fi
}

cleanup() {
    stop_gateway
}
trap cleanup EXIT

stop_gateway() {
    if [ -n "$GATEWAY_PID" ]; then
        kill "$GATEWAY_PID" 2>/dev/null || true
        wait "$GATEWAY_PID" 2>/dev/null || true
        GATEWAY_PID=""
    fi
}

start_gateway() {
    local journal="$1" directory="$2"
    stop_gateway
    rm -rf "$directory"
    mkdir -p "$directory"
    rm -f "$WORK/gateway.log"
    "$ROOT/target/debug/fix-gateway" \
        --bind 127.0.0.1:0 \
        --session-dir "$directory" \
        --journal "$journal" \
        >"$WORK/gateway.log" 2>&1 &
    GATEWAY_PID=$!
    PORT=""
    for _ in $(seq 1 100); do
        PORT="$(sed -n 's/.*listening on 127\.0\.0\.1:\([0-9][0-9]*\).*/\1/p' "$WORK/gateway.log" | head -n1)"
        if [ -n "$PORT" ]; then
            return 0
        fi
        sleep 0.05
    done
    echo "fix_conformance: the gateway did not start" >&2
    cat "$WORK/gateway.log" >&2
    return 1
}

mkdir -p "$WORK" "$CLASSES" "$JARS"
fetch "$QFJ" "org/quickfixj/quickfixj-all/2.3.1/$QFJ"
fetch "$MINA" "org/apache/mina/mina-core/2.1.4/$MINA"
fetch "$SLF4J_API" "org/slf4j/slf4j-api/1.7.36/$SLF4J_API"
fetch "$SLF4J_SIMPLE" "org/slf4j/slf4j-simple/1.7.36/$SLF4J_SIMPLE"

echo "building the gateway"
(cd "$ROOT" && cargo build -q -p deepbook-gateway --bin fix-gateway)
echo "compiling the conformance clients"
javac -cp "$CP" -d "$CLASSES" \
    "$SRC/QuickFixInitiator.java" \
    "$SRC/RawProbe.java"

# ---- 1. QuickFIX/J logon, order, report -----------------------------------
start_gateway 4096 "$WORK/sessions-quickfixj"
rm -rf "$WORK/qfj-store" "$WORK/qfj-log"
mkdir -p "$WORK/qfj-store" "$WORK/qfj-log"
cat > "$WORK/quickfixj.cfg" <<EOF
[SESSION]
ConnectionType=initiator
BeginString=FIX.4.4
SenderCompID=CLIENT
TargetCompID=DEEPBOOK
SocketConnectHost=127.0.0.1
SocketConnectPort=$PORT
StartTime=00:00:00
EndTime=23:59:59
HeartBtInt=30
ReconnectInterval=1
ResetOnLogon=Y
FileStorePath=$WORK/qfj-store
FileLogPath=$WORK/qfj-log
EOF
java -Dorg.slf4j.simpleLogger.defaultLogLevel=error \
    -cp "$CP:$CLASSES" QuickFixInitiator "$WORK/quickfixj.cfg"
stop_gateway
echo "PASS quickfixj: logon, order and execution report"

# ---- 2. Resend: admin gap fill, application replay -------------------------
start_gateway 4096 "$WORK/sessions-resend"
java -Dorg.slf4j.simpleLogger.defaultLogLevel=error \
    -cp "$CP:$CLASSES" RawProbe resend 127.0.0.1 "$PORT"
stop_gateway
echo "PASS resend: 43=Y/122 on application, GapFill only on admin"

# ---- 3. Over-window resend ------------------------------------------------
start_gateway 1 "$WORK/sessions-over-window"
java -Dorg.slf4j.simpleLogger.defaultLogLevel=error \
    -cp "$CP:$CLASSES" RawProbe over-window 127.0.0.1 "$PORT"
stop_gateway
echo "PASS over-window: Logout, no silent loss"

echo "fix_conformance: PASS"
