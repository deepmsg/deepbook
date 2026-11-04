#!/usr/bin/env bash
#
# The controller side of the four-host cluster: three members on three
# machines, the FIX gateway and the load client on the fourth.
#
#   run-cluster.sh list
#   run-cluster.sh up
#   run-cluster.sh status
#   run-cluster.sh bench --rate N --samples N --warmup N [--json]
#   run-cluster.sh kill-leader
#   run-cluster.sh counters
#   run-cluster.sh down
#
# `up` writes one `member.properties` per member, ships it with
# `member-node.sh`, starts it under `ssh -f` + `setsid`, then waits for two
# things a quorum can hide: that every member is up, and that exactly one of
# them says it is the leader. With three members and a quorum of two, a dead
# member leaves a cluster that still elects and still trades.
#
# The client half is a driver of its own (the gateway is a cluster client), the
# gateway, and `fix-bench`. The gateway reaches the cluster through
# `--ingress-endpoints`, which is the `0=host:port,...` list the client library
# substitutes into the ingress channel.
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=hosts.env
. "$here/hosts.env"

MEMBERS=3
BASE=$CLUSTER_BASE_PORT
FIX_PORT=$((BASE + 50))
self_ip=$(hostname -I | awk '{print $1}')
stage=${TMPDIR:-/tmp}/deepbook-cluster-4host

_host() { # <member>
    case "$1" in
        0) printf '%s' "$CLUSTER_NODE0_SSH" ;;
        1) printf '%s' "$CLUSTER_NODE1_SSH" ;;
        2) printf '%s' "$CLUSTER_NODE2_SSH" ;;
    esac
}

_ip() { # <member> -> its address
    _host "$1" | sed 's/.*@//'
}

_client_host() { printf '%s' "$CLUSTER_CLIENT_SSH"; }
_client_ip() { printf '%s' "$CLUSTER_CLIENT_SSH" | sed 's/.*@//'; }

_mdir() { printf '%s/member%s' "$CLUSTER_RUN_ROOT" "$1"; }
_client_dir() { printf '%s/client' "$CLUSTER_RUN_ROOT"; }

# A command on a host. The controller *is* node0, and node0 may or may not be
# able to ssh to itself; run locally when the target is this machine.
_on() { # <ssh-target> <command>
    if [ "${1#*@}" = "$self_ip" ]; then
        bash -c "$2"
    else
        ssh -n -o BatchMode=yes -o ConnectTimeout=10 "$1" "$2"
    fi
}

# Start something that must outlive this shell. On another host that is
# `ssh -f` + `setsid`; on this one it is `setsid` directly, because a machine
# is not always able to ssh to itself and the controller *is* node0.
_daemon() { # <ssh-target> <command> <log>
    local host=$1 command=$2 log=$3
    if [ "${host#*@}" = "$self_ip" ]; then
        setsid bash -c "$command" </dev/null >"$log" 2>&1 &
    else
        ssh -f -n -o BatchMode=yes -o ConnectTimeout=10 "$host" \
            "exec setsid bash -c $(printf '%q' "$command") </dev/null >$(printf '%q' "$log") 2>&1"
    fi
}

_members_string() {
    local out="" i h
    for ((i = 0; i < MEMBERS; i++)); do
        h=$(_ip "$i")
        out+="${i},${h}:$((BASE + i)),${h}:$((BASE + 10 + i)),${h}:$((BASE + 20 + i)),"
        out+="${h}:0,${h}:$((BASE + 40 + i))|"
    done
    printf '%s' "${out%|}"
}

_ingress_endpoints() {
    local out="" i
    for ((i = 0; i < MEMBERS; i++)); do
        out+="${i}=$(_ip "$i"):$((BASE + i)),"
    done
    printf '%s' "${out%,}"
}

_member_properties() { # <member> <file>
    local i=$1 file=$2 h
    h=$(_ip "$i")
    {
        echo "member_id=$i"
        echo "host=$h"
        echo "cluster_id=$CLUSTER_ID"
        echo "members=$(_members_string)"
        echo "aeron_dir=$(_mdir "$i")/aeron"
        echo "archive_dir=$(_mdir "$i")/archive"
        echo "cluster_dir=$(_mdir "$i")/cluster"
        echo "driver=$CLUSTER_DEEPMSG_DIR/target/release/deepmsg-archiving-media-driver"
        echo "cluster_node=$CLUSTER_DEEPMSG_DIR/target/release/deepmsg-cluster-node"
        echo "container=$CLUSTER_DEEPBOOK_DIR/target/release/engine-container"
        echo "cnc_dump=$CLUSTER_DEEPMSG_DIR/target/release/cnc-dump"
        echo "archive_control=aeron:udp?endpoint=${h}:$((BASE + 40 + i))"
        echo "ingress_channel=aeron:udp?term-length=128k|endpoint=${h}:$((BASE + i))"
        echo "consensus_channel=aeron:udp?term-length=128k|endpoint=${h}:$((BASE + 10 + i))"
        echo "threading_mode=$CLUSTER_THREADING_MODE"
        echo "driver_conductor_cpu=$CLUSTER_DRIVER_CONDUCTOR_CPU"
        echo "driver_sender_cpu=$CLUSTER_DRIVER_SENDER_CPU"
        echo "driver_receiver_cpu=$CLUSTER_DRIVER_RECEIVER_CPU"
        echo "cm_cpu=$CLUSTER_CM_CPU"
        echo "container_cpu=$CLUSTER_CONTAINER_CPU"
        echo "heartbeat_timeout=$CLUSTER_HEARTBEAT"
        echo "election_timeout=$CLUSTER_ELECTION_TIMEOUT"
        echo "appointed_leader=${CLUSTER_APPOINTED_LEADER:-}"
        echo "engine_symbol=$CLUSTER_ENGINE_SYMBOL"
        echo "engine_max_orders=$CLUSTER_ENGINE_MAX_ORDERS"
        echo "engine_recent_window=$CLUSTER_ENGINE_RECENT_WINDOW"
    } >"$file"
}

# --- readiness ---------------------------------------------------------------

_is_leader() { # <member> -> 0 leader, 1 not, other failure
    local i=$1
    _on "$(_host "$i")" \
        "'$CLUSTER_DEEPMSG_DIR/target/release/deepmsg-cluster-tool' -Daeron.dir='$(_mdir "$i")/aeron' '$(_mdir "$i")/cluster' is-leader >/dev/null 2>&1"
}

_leader() { # prints the member id that says it is leader, or nothing
    local i
    for ((i = 0; i < MEMBERS; i++)); do
        if _is_leader "$i"; then printf '%s' "$i"; return 0; fi
    done
    return 1
}

_wait_leader() { # <seconds>
    local deadline=$((SECONDS + $1)) leader
    while [ "$SECONDS" -lt "$deadline" ]; do
        if leader=$(_leader); then
            echo "  leader is member $leader"
            return 0
        fi
        sleep 0.5
    done
    echo "  no member claimed leadership in $1 s" >&2
    return 1
}

# --- members -----------------------------------------------------------------

_member_start() { # <member>
    local i=$1 host mdir
    host=$(_host "$i")
    mdir=$(_mdir "$i")
    mkdir -p "$stage/member$i"
    _member_properties "$i" "$stage/member$i/member.properties"

    _on "$host" "rm -rf '$mdir'; mkdir -p '$mdir'"
    scp -q "$stage/member$i/member.properties" "$here/member-node.sh" "$host:$mdir/"
    _on "$host" "chmod +x '$mdir/member-node.sh'"
    # `ssh -f` + `setsid`: the member's processes must outlive the session that
    # started them. The exit status of a `-f` session is not the member's, so
    # readiness is the `up`/`failed` file the script writes.
    _daemon "$host" "bash '$mdir/member-node.sh' start '$mdir'" "$mdir/start.log"
}

_member_wait_up() { # <member> <seconds>
    local i=$1 host mdir deadline=$((SECONDS + $2))
    host=$(_host "$i")
    mdir=$(_mdir "$i")
    while [ "$SECONDS" -lt "$deadline" ]; do
        if _on "$host" "test -f '$mdir/up'" 2>/dev/null; then
            echo "  member $i up"
            return 0
        fi
        if _on "$host" "test -f '$mdir/failed'" 2>/dev/null; then
            echo "  member $i failed to start:" >&2
            _on "$host" "tail -20 '$mdir/start.log'" >&2 || true
            return 1
        fi
        sleep 0.5
    done
    echo "  member $i did not come up in $2 s" >&2
    _on "$host" "tail -20 '$mdir/start.log'" >&2 || true
    return 1
}

_member_stop() { # <member>
    local i=$1 host mdir
    host=$(_host "$i")
    mdir=$(_mdir "$i")
    _on "$host" "bash '$mdir/member-node.sh' stop '$mdir' >/dev/null 2>&1 || true" || true
    # The backstop, for a member whose pid list was interrupted. The bracket
    # keeps `pkill` from matching the shell that carries the pattern.
    _on "$host" "
        pkill -9 -f '[d]eepmsg-cluster-node' 2>/dev/null || true
        pkill -9 -f '[d]eepmsg-archiving-media-driver' 2>/dev/null || true
        pkill -9 -f '[e]ngine-container' 2>/dev/null || true" || true
}

# --- the client half ---------------------------------------------------------

_client_driver_start() {
    local cdir
    cdir=$(_client_dir)
    local args=(
        "-Ddeepmsg.dir=$cdir/aeron" "-Daeron.dir=$cdir/aeron"
        -Ddeepmsg.dir.delete.on.start=false -Daeron.dir.delete.on.start=false
        "-Daeron.archive.dir=$cdir/archive"
        "-Daeron.archive.control.channel=aeron:udp?endpoint=$(_client_ip):$((BASE + 43))"
        -Daeron.archive.control.response.stream.id=20
        -Daeron.archive.local.control.channel=aeron:ipc?term-length=64k
        -Daeron.archive.local.control.stream.id=10
        "-Daeron.threading.mode=$CLUSTER_THREADING_MODE"
    )
    if [ "$CLUSTER_THREADING_MODE" = DEDICATED ]; then
        args+=("-Daeron.conductor.cpu.affinity=$CLUSTER_DRIVER_CONDUCTOR_CPU"
               "-Daeron.sender.cpu.affinity=$CLUSTER_DRIVER_SENDER_CPU"
               "-Daeron.receiver.cpu.affinity=$CLUSTER_DRIVER_RECEIVER_CPU")
    fi
    _client_script "$cdir" driver.sh - "$CLUSTER_DEEPMSG_DIR/target/release/deepmsg-archiving-media-driver" \
        "${args[@]}"
    _client_start "$cdir/driver.sh" "$cdir/driver.log" "$cdir/driver.pid"
    for _ in $(seq 1 200); do
        _on "$(_client_host)" "test -f '$cdir/aeron/cnc.dat'" 2>/dev/null && break
        sleep 0.1
    done
    _on "$(_client_host)" "test -f '$cdir/aeron/cnc.dat'" || {
        echo "the client's driver never published its CnC file" >&2
        _on "$(_client_host)" "tail -20 '$cdir/driver.log'" >&2 || true
        return 1
    }
}

_gateway_start() {
    local cdir
    cdir=$(_client_dir)
    local args=(
        --bind "0.0.0.0:$FIX_PORT"
        --session-dir "$cdir/sessions"
        --cluster
        --aeron-dir "$cdir/aeron"
        --ingress-channel "aeron:udp?term-length=128k"
        --ingress-endpoints "$(_ingress_endpoints)"
        --egress-channel "aeron:udp?endpoint=$(_client_ip):0"
        --tick-us 200
        --backend epoll
        --journal-sync batch --journal-batch 64
        --symbol AAPL --symbol-id "$CLUSTER_ENGINE_SYMBOL"
        --new-leader-timeout-ms "$CLUSTER_GATEWAY_NEW_LEADER_MS"
    )
    _client_script "$cdir" gateway.sh "$CLUSTER_GATEWAY_CPU" "$CLUSTER_DEEPBOOK_DIR/target/release/fix-gateway" "${args[@]}"
    _client_start "$cdir/gateway.sh" "$cdir/gateway.log" "$cdir/gateway.pid"
    for _ in $(seq 1 300); do
        _on "$(_client_host)" "grep -q 'listening on' '$cdir/gateway.log' 2>/dev/null" && return 0
        sleep 0.1
    done
    echo "the gateway never announced 'listening on'" >&2
    _on "$(_client_host)" "tail -20 '$cdir/gateway.log'" >&2 || true
    return 1
}

# A script the client host runs, so the process outlives this ssh.
_client_script() { # <dir> <name> <cpu|-> <binary> [args...]
    local cdir=$1 name=$2 cpu=$3 binary=$4
    shift 4
    _on "$(_client_host)" "mkdir -p '$cdir/sessions'"
    {
        echo '#!/usr/bin/env bash'
        echo 'set -euo pipefail'
        echo "echo \$\$ >\"$cdir/${name%.sh}.pid\""
        if [ "$cpu" = "-" ]; then
            printf 'exec "%s"' "$binary"
        else
            printf 'exec taskset -c %s "%s"' "$cpu" "$binary"
        fi
        printf ' %q' "$@"
        echo
    } >"$stage/$name"
    scp -q "$stage/$name" "$(_client_host):$cdir/$name"
    _on "$(_client_host)" "chmod +x '$cdir/$name'"
}

_client_start() { # <script> <log> <pidfile>
    local script=$1 log=$2 pidfile=$3
    _daemon "$(_client_host)" "bash '$script'" "$log"
    for _ in $(seq 1 100); do
        _on "$(_client_host)" "test -s '$pidfile'" 2>/dev/null && return 0
        sleep 0.1
    done
    echo "the client host never wrote $pidfile" >&2
    return 1
}

_client_kill() { # <name>
    local cdir name=$1
    cdir=$(_client_dir)
    _on "$(_client_host)" "if [ -s '$cdir/$name.pid' ]; then kill -9 \$(cat '$cdir/$name.pid') 2>/dev/null || true; rm -f '$cdir/$name.pid'; fi" || true
}

# --- commands ----------------------------------------------------------------

cmd_list() {
    echo "members         $MEMBERS, one per host"
    local i
    for ((i = 0; i < MEMBERS; i++)); do
        echo "  $i: $(_host "$i")  ingress :$((BASE + i))  consensus :$((BASE + 10 + i))  log :$((BASE + 20 + i))  archive :$((BASE + 40 + i))"
    done
    echo "members string  $(_members_string)"
    echo "client          $(_client_host)"
    echo "  gateway       fix-gateway on :$FIX_PORT, ingress endpoints $(_ingress_endpoints)"
    echo "  ingress channel aeron:udp?term-length=128k (the endpoint is substituted per leader)"
    echo "  egress        aeron:udp?endpoint=$(_client_ip):0"
    echo "run root        $CLUSTER_RUN_ROOT (on each host)"
    echo "binaries        $CLUSTER_DEEPMSG_DIR/target/release, $CLUSTER_DEEPBOOK_DIR/target/release"
    echo "timing          heartbeat $CLUSTER_HEARTBEAT, election $CLUSTER_ELECTION_TIMEOUT, gateway new-leader $CLUSTER_GATEWAY_NEW_LEADER_MS ms"
}

_check_binaries() {
    local i host bin problems=0
    for ((i = 0; i < MEMBERS; i++)); do
        host=$(_host "$i")
        for bin in deepmsg-archiving-media-driver deepmsg-cluster-node; do
            _on "$host" "test -x '$CLUSTER_DEEPMSG_DIR/target/release/$bin'" 2>/dev/null || {
                echo "missing on $host: $CLUSTER_DEEPMSG_DIR/target/release/$bin" >&2; problems=1; }
        done
        _on "$host" "test -x '$CLUSTER_DEEPBOOK_DIR/target/release/engine-container'" 2>/dev/null || {
            echo "missing on $host: $CLUSTER_DEEPBOOK_DIR/target/release/engine-container" >&2; problems=1; }
    done
    for bin in fix-gateway fix-bench; do
        _on "$(_client_host)" "test -x '$CLUSTER_DEEPBOOK_DIR/target/release/$bin'" 2>/dev/null || {
            echo "missing on the client: $CLUSTER_DEEPBOOK_DIR/target/release/$bin" >&2; problems=1; }
    done
    [ "$problems" -eq 0 ] || {
        echo "build the missing binaries first: just deps / cargo build --release" >&2
        return 1
    }
}

cmd_up() {
    local i
    _check_binaries || return 1
    mkdir -p "$stage"
    for ((i = 0; i < MEMBERS; i++)); do _member_stop "$i"; done
    _client_kill gateway
    _client_kill driver

    echo "== members"
    for ((i = 0; i < MEMBERS; i++)); do _member_start "$i"; done
    for ((i = 0; i < MEMBERS; i++)); do _member_wait_up "$i" 90 || return 1; done

    echo "== leader"
    _wait_leader 90 || return 1

    echo "== client"
    _client_driver_start || return 1
    _gateway_start || return 1
    echo "== up"
}

cmd_status() {
    local i
    for ((i = 0; i < MEMBERS; i++)); do
        printf 'member %s: ' "$i"
        if _is_leader "$i"; then echo "leader"; else
            if _on "$(_host "$i")" "bash '$(_mdir "$i")/member-node.sh' check '$(_mdir "$i")' >/dev/null 2>&1"; then
                echo "follower"
            else
                echo "not running"
            fi
        fi
    done
    printf 'gateway: '
    _on "$(_client_host)" "kill -0 \$(cat '$(_client_dir)/gateway.pid' 2>/dev/null) 2>/dev/null" && echo running || echo "not running"
}

cmd_bench() {
    local rate=10000 samples=200000 warmup=20000 json=0
    while [ $# -gt 0 ]; do
        case "$1" in
            --rate) rate=$2; shift 2 ;;
            --samples) samples=$2; shift 2 ;;
            --warmup) warmup=$2; shift 2 ;;
            --json) json=1; shift ;;
            *) echo "bench: unknown option $1" >&2; return 2 ;;
        esac
    done
    local cdir out
    cdir=$(_client_dir)
    out=$cdir/bench-$(date +%Y%m%d-%H%M%S).txt
    local args=(--connect "127.0.0.1:$FIX_PORT" --rate "$rate" --samples "$samples"
                --warmup "$warmup" --account 1 --price 1.00 --qty 1 --timeout-ms 120000 --seed-book)
    [ "$json" -eq 1 ] && args+=(--json)
    echo "== fix-bench rate=$rate samples=$samples warmup=$warmup"
    _on "$(_client_host)" "taskset -c $CLUSTER_CLIENT_CPU '$CLUSTER_DEEPBOOK_DIR/target/release/fix-bench' $(printf '%q ' "${args[@]}")" | tee "$out"
    echo "saved $out"
}

cmd_kill_leader() {
    local leader start now
    leader=$(_leader) || { echo "no leader to kill" >&2; return 1; }
    echo "leader is member $leader; killing its three processes"
    start=$(date +%s.%N)
    _on "$(_host "$leader")" "kill -9 \$(cat '$(_mdir "$leader")/pids') 2>/dev/null || true"
    local new=""
    for _ in $(seq 1 600); do
        new=$(_leader) || true
        if [ -n "$new" ] && [ "$new" != "$leader" ]; then break; fi
        sleep 0.05
    done
    now=$(date +%s.%N)
    if [ -n "$new" ] && [ "$new" != "$leader" ]; then
        awk -v a="$start" -v b="$now" 'BEGIN { printf "new leader is member %s after %.2f s\n", "'"$new"'", b - a }'
    else
        echo "no new leader within 30 s" >&2
        return 1
    fi
}

cmd_counters() {
    local i
    for ((i = 0; i < MEMBERS; i++)); do
        _on "$(_host "$i")" "bash '$(_mdir "$i")/member-node.sh' counters '$(_mdir "$i")'" || true
    done
}

cmd_down() {
    local i
    for ((i = 0; i < MEMBERS; i++)); do _member_stop "$i"; done
    _client_kill gateway
    _client_kill driver
    _on "$(_client_host)" "pkill -9 -f '[g]ateway.sh' 2>/dev/null || true" || true
    echo "down"
}

action=${1:-list}
shift || true
case "$action" in
    list)        cmd_list ;;
    up)          cmd_up ;;
    status)      cmd_status ;;
    bench)       cmd_bench "$@" ;;
    kill-leader) cmd_kill_leader ;;
    counters)    cmd_counters ;;
    down)        cmd_down ;;
    *) echo "usage: run-cluster.sh list|up|status|bench|kill-leader|counters|down" >&2; exit 2 ;;
esac
