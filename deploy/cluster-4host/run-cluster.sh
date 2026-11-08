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
#   run-cluster.sh observability up|status|down
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
# ★ The driver's term buffers and its CnC file are memory-mapped, and a
# memory-mapped file on a disk-backed filesystem is a page fault in whatever
# path touches it. `CLUSTER_RUN_ROOT` is on the ESSD; this is on a tmpfs. The
# archive and cluster directories stay with the run root because they have to
# survive the run.
_adir() { printf '%s/member%s/aeron' "$CLUSTER_AERON_ROOT" "$1"; }
_client_aeron() { printf '%s/client/aeron' "$CLUSTER_AERON_ROOT"; }
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
        echo "node_id=node$i"
        echo "host=$h"
        echo "cluster_id=$CLUSTER_ID"
        echo "members=$(_members_string)"
        echo "aeron_dir=$(_adir "$i")"
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
        echo "image_liveness=${CLUSTER_IMAGE_LIVENESS:-}"
        echo "election_timeout=$CLUSTER_ELECTION_TIMEOUT"
        echo "appointed_leader=${CLUSTER_APPOINTED_LEADER:-}"
        echo "engine_symbol=$CLUSTER_ENGINE_SYMBOL"
        echo "engine_max_orders=$CLUSTER_ENGINE_MAX_ORDERS"
        echo "engine_recent_window=$CLUSTER_ENGINE_RECENT_WINDOW"
        echo "log_dir=$CLUSTER_LOG_DIR"
        echo "log_level=$CLUSTER_LOG_LEVEL"
        echo "log_roll=$CLUSTER_LOG_ROLL"
    } >"$file"
}

# --- readiness ---------------------------------------------------------------

_is_leader() { # <member> -> 0 leader, 1 not, other failure
    local i=$1
    _on "$(_host "$i")" \
        "'$CLUSTER_DEEPMSG_DIR/target/release/deepmsg-cluster-tool' -Daeron.dir='$(_adir "$i")' '$(_mdir "$i")/cluster' is-leader >/dev/null 2>&1"
}

_member_alive() { # <member> -> 0 when every pid in its own pid file is still there
    local i=$1 d
    d=$(_mdir "$i")
    _on "$(_host "$i")" \
        "test -s '$d/pids' || exit 1; for p in \$(cat '$d/pids'); do kill -0 \"\$p\" 2>/dev/null || exit 1; done"
}

_leader() { # prints the member id that says it is leader, or nothing
    local i
    for ((i = 0; i < MEMBERS; i++)); do
        # ★ A dead member's CnC file is still on disk and still says role=2:
        # `is-leader` alone reported the killed leader for the whole 30 s of a
        # failover, and nothing else was ever elected. The pid file is the
        # liveness, and it is checked first because it is the cheaper question.
        _member_alive "$i" || continue
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

# ★ A *restart* is not a start. The run root holds the archive (which holds the
# snapshot) and the cluster directory (which holds the leadership term); wiping
# them turns every recovery question into a fresh-join question. `--keep` keeps
# both and rewrites only the properties and the script.
_member_restart() { # <member> [--keep]
    local i=$1 keep=${2:-} host mdir
    host=$(_host "$i")
    mdir=$(_mdir "$i")
    mkdir -p "$stage/member$i"
    _member_properties "$i" "$stage/member$i/member.properties"
    if [ "$keep" = "--keep" ]; then
        _on "$host" "mkdir -p '$mdir'"
    else
        _on "$host" "rm -rf '$mdir'; mkdir -p '$mdir'"
    fi
    scp -q "$stage/member$i/member.properties" "$here/member-node.sh" "$host:$mdir/"
    _on "$host" "chmod +x '$mdir/member-node.sh'"
    # The readiness file from before the kill is still there and would make
    # `_member_wait_up` answer at once.
    _on "$host" "rm -f '$mdir/up' '$mdir/failed'"
    _daemon "$host" "bash '$mdir/member-node.sh' start '$mdir' $keep" "$mdir/start.log"
}

_member_commit_pos() { # <member> -> the position it has applied, or empty
    local i=$1
    _on "$(_host "$i")" \
        "'$CLUSTER_DEEPMSG_DIR/target/release/cnc-dump' --all '$(_adir "$i")' 2>/dev/null | awk '/Cluster commit-pos/ {print \$3; exit}'"
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
        "-Ddeepmsg.dir=$(_client_aeron)" "-Daeron.dir=$(_client_aeron)"
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
        _on "$(_client_host)" "test -f '$(_client_aeron)/cnc.dat'" 2>/dev/null && break
        sleep 0.1
    done
    _on "$(_client_host)" "test -f '$(_client_aeron)/cnc.dat'" || {
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
        --aeron-dir "$(_client_aeron)"
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
        # The application logs are the process's own files; the environment is
        # what turns the facade on, and every process on the client shares the
        # `client` node id (the component in the file name is what separates
        # them).
        if [ -n "$CLUSTER_LOG_DIR" ]; then
            echo "mkdir -p $(printf '%q' "$CLUSTER_LOG_DIR")"
            echo "export DEEPMSG_LOG_DIR=$(printf '%q' "$CLUSTER_LOG_DIR")"
            echo "export DEEPMSG_NODE_ID=client"
            echo "export DEEPMSG_LOG_LEVEL=$(printf '%q' "$CLUSTER_LOG_LEVEL")"
            if [ -n "$CLUSTER_LOG_ROLL" ]; then
                echo "export DEEPMSG_LOG_ROLL=$(printf '%q' "$CLUSTER_LOG_ROLL")"
            fi
        fi
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

# The environment a process the client host starts *directly* (not through
# `_client_script`) needs: the same log directory and node id.
_client_env() {
    if [ -n "$CLUSTER_LOG_DIR" ]; then
        printf 'DEEPMSG_LOG_DIR=%q DEEPMSG_NODE_ID=client DEEPMSG_LOG_LEVEL=%q ' \
            "$CLUSTER_LOG_DIR" "$CLUSTER_LOG_LEVEL"
    fi
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
    echo "observability   ${CLUSTER_OBS_SSH:-not deployed (CLUSTER_OBS_SSH is empty)}"
    echo "binaries        $CLUSTER_DEEPMSG_DIR/target/release, $CLUSTER_DEEPBOOK_DIR/target/release"
    echo "timing          heartbeat $CLUSTER_HEARTBEAT, election $CLUSTER_ELECTION_TIMEOUT, gateway new-leader $CLUSTER_GATEWAY_NEW_LEADER_MS ms"
    echo "leader          ${CLUSTER_APPOINTED_LEADER:-elected}${CLUSTER_APPOINTED_LEADER:+ (appointed; empty = elected)}"
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
    _on "$(_client_host)" "rm -rf '$(_client_aeron)'" || true

    echo "== members"
    for ((i = 0; i < MEMBERS; i++)); do _member_start "$i"; done
    for ((i = 0; i < MEMBERS; i++)); do _member_wait_up "$i" 90 || return 1; done

    echo "== leader"
    _wait_leader 90 || return 1

    echo "== client"
    if [ -n "$CLUSTER_LOG_DIR" ]; then
        _on "$(_client_host)" "mkdir -p '$CLUSTER_LOG_DIR' && chmod 0755 '$CLUSTER_LOG_DIR'" || true
    fi
    _client_driver_start || return 1
    _gateway_start || return 1
    echo "== up"
}

cmd_status() {
    local i
    for ((i = 0; i < MEMBERS; i++)); do
        printf 'member %s: ' "$i"
        if ! _member_alive "$i"; then
            echo "not running"
        elif _is_leader "$i"; then
            echo "leader"
        elif _on "$(_host "$i")" "bash '$(_mdir "$i")/member-node.sh' check '$(_mdir "$i")' >/dev/null 2>&1"; then
            echo "follower"
        else
            echo "not running"
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
    if [ -n "$CLUSTER_LOG_DIR" ]; then
        _on "$(_client_host)" "mkdir -p '$CLUSTER_LOG_DIR'" || true
    fi
    _on "$(_client_host)" "$(_client_env)taskset -c $CLUSTER_CLIENT_CPU '$CLUSTER_DEEPBOOK_DIR/target/release/fix-bench' $(printf '%q ' "${args[@]}")" | tee "$out"
    echo "saved $out"
}

cmd_kill_leader() {
    local leader start now rate="" samples="" settle=5
    while [ $# -gt 0 ]; do
        case "$1" in
            --with-bench) rate=${2:-2000}; samples=${3:-60000}; shift 3 ;;
            --settle) settle=$2; shift 2 ;;
            *) echo "kill-leader: unknown option $1" >&2; return 2 ;;
        esac
    done
    leader=$(_leader) || { echo "no leader to kill" >&2; return 1; }
    # ★ A failover is only interesting while the cluster is carrying orders:
    # the election time says how long the module needed, and the client's own
    # numbers say what the *trading system* lost. Without this the run says
    # nothing about fills.
    if [ -n "$rate" ]; then
        local cdir
        cdir=$(_client_dir)
        echo "fix-bench ${rate}/s x ${samples} samples, starting ${settle}s before the kill"
        if [ -n "$CLUSTER_LOG_DIR" ]; then
            _on "$(_client_host)" "mkdir -p '$CLUSTER_LOG_DIR'" || true
        fi
        _daemon "$(_client_host)" \
            "$(_client_env)taskset -c $CLUSTER_CLIENT_CPU '$CLUSTER_DEEPBOOK_DIR/target/release/fix-bench' \
             --connect '127.0.0.1:$FIX_PORT' --rate $rate --samples $samples --warmup 2000 \
             --account 1 --price 1.00 --qty 1 --timeout-ms 120000 --seed-book" \
            "$cdir/kill-leader-bench.txt"
        sleep "$settle"
    fi
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
    if [ -n "$rate" ]; then
        echo "waiting for the client's own account..."
        for _ in $(seq 1 240); do
            _on "$(_client_host)" "grep -q '^sent=' '$(_client_dir)/kill-leader-bench.txt' 2>/dev/null" && break
            sleep 0.5
        done
        echo "--- the client's own account"
        _on "$(_client_host)" "cat '$(_client_dir)/kill-leader-bench.txt'"
    fi
}

cmd_scenario() { # <script-on-the-controller> [--pace-ms N]
    local script="" pace=""
    while [ $# -gt 0 ]; do
        case "$1" in
            --pace-ms) pace=$2; shift 2 ;;
            *) script=$1; shift ;;
        esac
    done
    [ -n "$script" ] || { echo "usage: run-cluster.sh scenario <file> [--pace-ms N]" >&2; return 2; }
    [ -f "$script" ] || { echo "no such script: $script" >&2; return 1; }
    local cdir
    cdir=$(_client_dir)
    _on "$(_client_host)" "mkdir -p '$cdir'" || return 1
    scp -q "$script" "$(_client_host):$cdir/scenario.txt" || return 1
    echo "== fix-scenario $(basename "$script")"
    _on "$(_client_host)" "taskset -c $CLUSTER_CLIENT_CPU '$CLUSTER_DEEPBOOK_DIR/target/release/fix-scenario' \
        --connect '127.0.0.1:$FIX_PORT' --script '$cdir/scenario.txt' ${pace:+--pace-ms $pace}"
}

cmd_restart() { # <member> [--keep]
    local i=${1:?usage: run-cluster.sh restart <member> [--keep]}
    local keep="${2:-}" leader target t0 t1 pos
    leader=$(_leader) || { echo "no leader; there is nothing to catch up with" >&2; return 1; }
    if [ "$leader" = "$i" ]; then
        echo "member $i is the leader; restarting it is a failover, use kill-leader" >&2
        return 1
    fi
    target=$(_member_commit_pos "$leader")
    echo "restarting member $i${keep:+ (keeping its directories)}; leader $leader at commit-pos ${target:-?}"
    _member_stop "$i"
    t0=$(date +%s.%N)
    _member_restart "$i" "$keep"
    _member_wait_up "$i" 90 || return 1
    for _ in $(seq 1 1200); do
        pos=$(_member_commit_pos "$i")
        if [ -n "$pos" ] && [ -n "$target" ] &&
           awk -v a="$pos" -v b="$target" 'BEGIN { exit !(a + 0 >= b + 0) }'; then
            break
        fi
        sleep 0.25
    done
    t1=$(date +%s.%N)
    awk -v a="$t0" -v b="$t1" -v p="${pos:-?}" -v t="${target:-?}" \
        'BEGIN { printf "member caught up: commit-pos %s (leader was %s) after %.2f s\n", p, t, b - a }'
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
    # ★ `"$@"`, or every option the command takes is dropped on the floor:
    # `kill-leader --with-bench` ran without a benchmark and said so only by
    # the absence of a line nobody was looking for.
    kill-leader) cmd_kill_leader "$@" ;;
    restart)     cmd_restart "$@" ;;
    scenario)    cmd_scenario "$@" ;;
    counters)    cmd_counters ;;
    # The stack the applications ship to; see `observability.sh` and the
    # README's observability section. `up` never deletes /var/log/deepmsg.
    observability) shift; "$here/observability.sh" "$@" ;;
    down)        cmd_down ;;
    *) echo "usage: run-cluster.sh list|up|status|bench|kill-leader|restart|scenario|counters|observability|down" >&2; exit 2 ;;
esac
