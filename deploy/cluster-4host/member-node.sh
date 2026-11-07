#!/usr/bin/env bash
#
# One member of the four-host cluster, on the machine that member lives on.
#
#   member-node.sh start|stop|check|counters <dir>
#
# `<dir>` holds `member.properties` (written by `run-cluster.sh`, one
# `key=value` a line) and receives the three logs and the pid list. The same
# file is what a remote member gets: the batch scps the directory, then runs
# this script there under `ssh -f` + `setsid` so the processes outlive the ssh
# session that started them.
#
# Three processes, the same three the single-machine `deploy/bench.sh` starts:
#
#   driver       deepmsg-archiving-media-driver (media driver + archive in one)
#   module       deepmsg-cluster-node (consensus module)
#   container    engine-container (deepbook's service container)
#
# The channel shapes are `crates/spike/src/nodes.rs`'s, with `localhost`
# replaced by the member's own address (`host` in the properties). The log
# channel deliberately has **no endpoint**: a three-member cluster publishes to
# the others and the module turns that into `control-mode=manual` itself
# (`ConsensusModule.java:2145`). An endpoint there makes followers time out
# with "failed to join live log".
set -euo pipefail

action=${1:?usage: member-node.sh start|stop|check|counters <dir> [--keep]}
dir=${2:?usage: member-node.sh start|stop|check|counters <dir> [--keep]}
# ★ `--keep` is what makes a *restart* different from a *fresh join*. The
# default wipes the member's three directories, which is right for a cell that
# starts from nothing and wrong for every question about recovery: a wiped
# archive has no snapshot to recover from and a wiped cluster directory has no
# leadership term to resume.
keep=0
[ "${3:-}" = "--keep" ] && keep=1
props=$dir/member.properties

_need() {
    local key=$1
    if [ -z "${!key:-}" ]; then
        echo "member-node.sh: '$key' is missing from $props" >&2
        exit 1
    fi
}

_load() {
    [ -f "$props" ] || { echo "member-node.sh: no $props" >&2; exit 1; }
    local key value
    while IFS='=' read -r key value; do
        case "$key" in
            ''|'#'*) continue ;;
        esac
        printf -v "$key" '%s' "$value"
    done <"$props"
    _need member_id; _need members; _need host; _need cluster_id
    _need aeron_dir; _need archive_dir; _need cluster_dir
    _need driver; _need cluster_node; _need container
    _need archive_control; _need ingress_channel; _need consensus_channel
    _need threading_mode
    _need driver_conductor_cpu; _need driver_sender_cpu; _need driver_receiver_cpu
    _need cm_cpu; _need container_cpu
    _need node_id
}

PIDS=()
CELL_PIDS_FILE=$dir/pids

_stop_one() {
    local pid=$1
    kill -TERM "$pid" 2>/dev/null || true
    for _ in $(seq 1 50); do
        kill -0 "$pid" 2>/dev/null || return 0
        sleep 0.1
    done
    kill -KILL "$pid" 2>/dev/null || true
}

_await_file() { # <path> <what> [tenths]
    local path=$1 what=$2 tries=${3:-200}
    for _ in $(seq 1 "$tries"); do
        [ -f "$path" ] && return 0
        sleep 0.1
    done
    echo "member-node.sh: $what never appeared ($path)" >&2
    return 1
}

_await_line() { # <path> <pattern> <what> [tenths]
    local path=$1 pattern=$2 what=$3 tries=${4:-200}
    for _ in $(seq 1 "$tries"); do
        grep -q "$pattern" "$path" 2>/dev/null && return 0
        sleep 0.1
    done
    echo "member-node.sh: $what never appeared in $path" >&2
    return 1
}

_driver_args() {
    printf '%s\n' \
        "-Ddeepmsg.dir=$aeron_dir" \
        "-Daeron.dir=$aeron_dir" \
        "-Ddeepmsg.dir.delete.on.start=false" \
        "-Daeron.dir.delete.on.start=false" \
        "-Daeron.archive.dir=$archive_dir" \
        "-Daeron.archive.control.channel=$archive_control" \
        "-Daeron.archive.control.response.stream.id=20" \
        "-Daeron.archive.local.control.channel=aeron:ipc?term-length=64k" \
        "-Daeron.archive.local.control.stream.id=10" \
        "-Daeron.archive.file.sync.level=${archive_sync:-0}" \
        "-Daeron.archive.catalog.file.sync.level=${archive_sync:-0}" \
        "-Daeron.threading.mode=$threading_mode" \
        "-Daeron.conductor.idle.strategy=spin" \
        "-Daeron.sender.idle.strategy=spin" \
        "-Daeron.receiver.idle.strategy=spin"

    if [ "$threading_mode" = DEDICATED ]; then
        printf '%s\n' \
            "-Daeron.conductor.cpu.affinity=$driver_conductor_cpu" \
            "-Daeron.sender.cpu.affinity=$driver_sender_cpu" \
            "-Daeron.receiver.cpu.affinity=$driver_receiver_cpu"
    fi
    # ★ How long a follower's *driver* keeps an image it has stopped hearing
    # from before it reports it unavailable. The failover measurement of
    # 2026-11-04 15:3x says a survivor's election state does not move until
    # 13.65 s after the leader dies, whatever `leader.heartbeat.timeout` is --
    # and the default here is 10 s, which is the leading candidate for that
    # constant. Empty keeps the build's default.
    if [ -n "${image_liveness:-}" ]; then
        printf '%s\n' "-Daeron.image.liveness.timeout=$image_liveness"
    fi
}

# What the consensus module and the container both read. `crates/spike`'s
# `module_args`, with the member's own address in the endpoints.
_cluster_args() {
    printf '%s\n' \
        "-Daeron.dir=$aeron_dir" \
        "-Daeron.cluster.dir=$cluster_dir" \
        "-Daeron.cluster.id=$cluster_id" \
        "-Daeron.cluster.member.id=$member_id" \
        "-Daeron.cluster.members=$members" \
        "-Daeron.cluster.ingress.channel=$ingress_channel" \
        "-Daeron.cluster.consensus.channel=$consensus_channel" \
        "-Daeron.cluster.log.channel=aeron:udp?term-length=512k" \
        "-Daeron.cluster.egress.channel=aeron:udp?term-length=128k" \
        "-Daeron.cluster.control.channel=aeron:ipc?term-length=128k" \
        "-Daeron.cluster.replication.channel=aeron:udp?endpoint=$host:0" \
        "-Daeron.cluster.leader.heartbeat.timeout=${heartbeat_timeout:-2s}" \
        "-Daeron.cluster.election.timeout=${election_timeout:-250ms}" \
        ${appointed_leader:+"-Daeron.cluster.appointed.leader.id=$appointed_leader"} \
        "-Daeron.cluster.service.count=1" \
        "-Daeron.cluster.service.id=0" \
        "-Daeron.cluster.service.responder=true" \
        "-Daeron.cluster.service.stream.id=104" \
        "-Daeron.cluster.consensus.module.stream.id=105" \
        "-Daeron.cluster.replay.channel=aeron:ipc" \
        "-Daeron.cluster.replay.stream.id=103" \
        "-Daeron.cluster.snapshot.channel=aeron:ipc?alias=snapshot" \
        "-Daeron.cluster.snapshot.stream.id=106" \
        "-Daeron.archive.local.control.channel=aeron:ipc?term-length=64k" \
        "-Daeron.archive.local.control.stream.id=10" \
        "-Daeron.archive.control.response.stream.id=20" \
        "-Daeron.cluster.idle.strategy=org.agrona.concurrent.BusySpinIdleStrategy"
}

_engine_args() {
    printf '%s\n' \
        "-Ddeepbook.marketdata.enabled=false" \
        "-Ddeepbook.engine.symbol=${engine_symbol:-1}" \
        "-Ddeepbook.engine.max-orders=${engine_max_orders:-1048576}" \
        "-Ddeepbook.engine.recent-window=${engine_recent_window:-1000000}"
}

# A core that does not exist fails inside `taskset`, whose message lands in a
# log nobody reads until the member is "up but not up". Say it by name first.
_check_cpus() {
    local n c
    n=$(nproc)
    for c in "$driver_conductor_cpu" "$driver_sender_cpu" "$driver_receiver_cpu" "$cm_cpu" "$container_cpu"; do
        if [ "$c" -ge "$n" ]; then
            echo "member-node.sh: core $c does not exist on this machine (nproc=$n)" >&2
            return 1
        fi
    done
}

_start() {
    local -a args=()
    local line
    _check_cpus || return 1

    # A run starts from nothing: a leftover cluster directory is a leftover
    # leadership term, and a leftover aeron directory is a leftover window.
    # `--keep` keeps all three, which is the only shape in which "recover from
    # the snapshot" is a question at all.
    if [ "$keep" -eq 0 ]; then
        rm -rf "$aeron_dir" "$archive_dir" "$cluster_dir"
    fi
    mkdir -p "$aeron_dir" "$archive_dir" "$cluster_dir"

    # The application logs are a deployment concern, not a run artifact: the
    # directory is outside the run root (see hosts.env) and the collector reads
    # it as another user. An empty `log_dir` keeps the facade off, which is
    # what a cell measuring the log's own cost wants.
    if [ -n "${log_dir:-}" ]; then
        mkdir -p "$log_dir"
        chmod 0755 "$log_dir"
        export DEEPMSG_LOG_DIR=$log_dir
        export DEEPMSG_NODE_ID=$node_id
        export DEEPMSG_LOG_LEVEL=${log_level:-info}
        if [ -n "${log_roll:-}" ]; then
            export DEEPMSG_LOG_ROLL=$log_roll
        fi
    fi

    args=("$driver")
    while IFS= read -r line; do args+=("$line"); done < <(_driver_args)
    "${args[@]}" >"$dir/driver.log" 2>&1 &
    PIDS+=("$!"); echo "$!" >>"$CELL_PIDS_FILE"
    _await_file "$aeron_dir/cnc.dat" "the driver's CnC file" || { tail -20 "$dir/driver.log" >&2 || true; return 1; }
    _await_line "$dir/driver.log" "archive running" "the archive in the driver" 200 || { tail -20 "$dir/driver.log" >&2 || true; return 1; }

    args=()
    while IFS= read -r line; do args+=("$line"); done < <(_cluster_args)
    taskset -c "$cm_cpu" "$cluster_node" "${args[@]}" >"$dir/module.log" 2>&1 &
    PIDS+=("$!"); echo "$!" >>"$CELL_PIDS_FILE"

    # ★ The container must not be started before the module has published its
    # mark file: a container that looks for a cluster that is not there yet
    # exits, and the member is then half up.
    _await_file "$cluster_dir/cluster-mark.dat" "the module's mark file" 200 || {
        tail -20 "$dir/module.log" >&2 || true
        return 1
    }

    while IFS= read -r line; do args+=("$line"); done < <(_cluster_args)
    while IFS= read -r line; do args+=("$line"); done < <(_engine_args)
    taskset -c "$container_cpu" "$container" "${args[@]}" >"$dir/container.log" 2>&1 &
    PIDS+=("$!"); echo "$!" >>"$CELL_PIDS_FILE"
    _await_line "$dir/container.log" "STARTED service=deepbook-engine" "the engine container" 300 || {
        tail -20 "$dir/container.log" >&2 || true
        tail -20 "$dir/module.log" >&2 || true
        return 1
    }

    local pid
    for pid in "${PIDS[@]}"; do
        kill -0 "$pid" 2>/dev/null || {
            echo "member-node.sh: process $pid on member $member_id did not stay up" >&2
            return 1
        }
    done
    echo "member $member_id up: ${PIDS[*]}"
}

action_start() {
    _load
    rm -f "$dir/up" "$dir/failed"
    : >"$CELL_PIDS_FILE"
    if _start; then
        : >"$dir/up"
    else
        : >"$dir/failed"
        local pid
        for pid in "${PIDS[@]:-}"; do _stop_one "$pid"; done
        return 1
    fi
}

action_stop() {
    local pid
    if [ -f "$CELL_PIDS_FILE" ]; then
        while read -r pid; do
            [ -n "$pid" ] && _stop_one "$pid"
        done <"$CELL_PIDS_FILE"
        rm -f "$CELL_PIDS_FILE"
    fi
    echo "member ${member_id:-?} stopped"
}

action_check() {
    _load
    [ -f "$CELL_PIDS_FILE" ] || { echo "member ${member_id:-?}: no pid list"; return 1; }
    local dead=0 pid
    while read -r pid; do
        [ -n "$pid" ] || continue
        if ! kill -0 "$pid" 2>/dev/null; then
            echo "member $member_id: pid $pid is gone"
            dead=1
        fi
    done <"$CELL_PIDS_FILE"
    return "$dead"
}

# The cell's server-side evidence, taken while the processes are still up.
# It needs deepmsg's `cnc-dump` (the same CnC the cluster module writes to); a
# missing tool is an error, not an empty file.
action_counters() {
    _load
    local out=$dir/counters.txt
    if [ -z "${cnc_dump:-}" ] || [ ! -x "${cnc_dump:-}" ]; then
        echo "member ${member_id:-?}: no executable 'cnc_dump' in $props" >&2
        return 1
    fi
    "$cnc_dump" --all "$aeron_dir" >"$out" 2>&1 || {
        echo "member ${member_id:-?}: cnc-dump failed (see $out)" >&2
        return 1
    }
    echo "member ${member_id:-?} counters -> $out"
}

case "$action" in
    start)    action_start ;;
    stop)     action_stop ;;
    check)    action_check ;;
    counters) action_counters ;;
    *)        echo "member-node.sh: unknown action '$action'" >&2; exit 2 ;;
esac
