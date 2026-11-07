#!/usr/bin/env bash
#
# The observability stack of the four-host cluster, on the machines that run
# it. The units, configs and pinned tool releases are deepmsg's
# (`deploy/observability/`); this script is the deployment half: it installs
# them, writes each host's environment, and checks that what should be running
# is.
#
#   observability.sh up       install and start the stack
#   observability.sh status    what is running, and the ports and directories
#                              that must be there for it to be of any use
#   observability.sh down      stop it; the data and the logs stay
#
# Five machines:
#
#   node0/1/2 + client   deepmsg-app-agent, deepmsg-counters-exporter,
#                        deepmsg-errorlog-exporter
#   CLUSTER_OBS_SSH      victoria-logs, victoria-metrics, vmauth, and Grafana
#                        when the distribution package is installed
#
# The applications are not units: they are started by `run-cluster.sh`, which
# exports the log directory and node id they need (see `member-node.sh`). The
# agent only ever reads what they write.
#
# Environment, all required by `up`:
#
#   CLUSTER_OBS_SSH            the fifth host, `user@private-address`
#   CLUSTER_OBS_WRITE_TOKEN    the bearer token vmauth accepts for writes
#   CLUSTER_OBS_READ_TOKEN     the token Grafana's datasources read with
#   CLUSTER_OBS_GRAFANA_PASSWORD  Grafana's admin password (optional)
#
# The units and configs are read from the controller's checkout (default: the
# sibling `deepmsg`; set `DEEPBOOK_DEEPMSG_DIR`), and the hosts' own checkouts
# are at `CLUSTER_DEEPMSG_DIR`. `CLUSTER_OBS_DRY_RUN=1` prints every
# remote command and file it would run or write, and changes nothing; the
# generated environment file is still written under $TMPDIR, which is what a
# dry run is for.
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=hosts.env
. "$here/hosts.env"

MEMBERS=3
# Where the controller reads the units and configs it ships. The hosts' own
# checkouts are at CLUSTER_DEEPMSG_DIR; a controller that is not one of them
# (a laptop) needs its own. Default: the sibling checkout.
OBS_SOURCE=${DEEPBOOK_DEEPMSG_DIR:-$(cd "$here/../../.." && pwd)/deepmsg}
stage=${TMPDIR:-/tmp}/deepbook-observability
self_ip=$(hostname -I | awk '{print $1}')

_die() { echo "observability: $*" >&2; exit 1; }

_host() {
    case "$1" in
        0) printf '%s' "$CLUSTER_NODE0_SSH" ;;
        1) printf '%s' "$CLUSTER_NODE1_SSH" ;;
        2) printf '%s' "$CLUSTER_NODE2_SSH" ;;
        client) printf '%s' "$CLUSTER_CLIENT_SSH" ;;
    esac
}

_node_id() {
    case "$1" in
        client) printf 'client' ;;
        *) printf 'node%s' "$1" ;;
    esac
}

# The driver's aeron directory, exactly what `run-cluster.sh` gives the driver.
_adir() {
    case "$1" in
        client) printf '%s/client/aeron' "$CLUSTER_AERON_ROOT" ;;
        *) printf '%s/member%s/aeron' "$CLUSTER_AERON_ROOT" "$1" ;;
    esac
}

# The member's cluster directory; empty on the client, which runs no cluster.
_cluster_dir() {
    case "$1" in
        client) printf '' ;;
        *) printf '%s/member%s/cluster' "$CLUSTER_RUN_ROOT" "$1" ;;
    esac
}

# The application endpoint the overlay scrapes (D3's page).
_app_endpoint() {
    case "$1" in
        client) printf '127.0.0.1:26101' ;;
        *) printf '127.0.0.1:26100' ;;
    esac
}

_obs_host() {
    [ -n "$CLUSTER_OBS_SSH" ] || _die "CLUSTER_OBS_SSH is not set"
    printf '%s' "$CLUSTER_OBS_SSH"
}

_obs_ip() { _host_ip "$(_obs_host)"; }

_host_ip() { printf '%s' "$1" | sed 's/.*@//'; }

# A command on a host; the controller may be one of them.
_on() { # <ssh-target> <command>
    if [ "${CLUSTER_OBS_DRY_RUN:-0}" = 1 ]; then
        echo "[dry-run] $1: $(printf '%s' "$2" | tr '\n' ' ' | cut -c1-120)" >&2
        return 0
    fi
    if [ "$(_host_ip "$1")" = "$self_ip" ]; then
        bash -c "$2"
    else
        ssh -n -o BatchMode=yes -o ConnectTimeout=10 "$1" "$2"
    fi
}

# Copy one file to a host and set its mode there.
_put() { # <source> <host> <destination> [mode]
    local source=$1 host=$2 destination=$3 mode=${4:-0644}
    if [ "${CLUSTER_OBS_DRY_RUN:-0}" = 1 ]; then
        echo "[dry-run] put $source -> $host:$destination ($mode)" >&2
        return 0
    fi
    if [ "$(_host_ip "$host")" = "$self_ip" ]; then
        install -m "$mode" "$source" "$destination"
    else
        scp -q "$source" "$host:$destination"
        _on "$host" "chmod $mode '$destination'"
    fi
}

# The same for a directory tree.
_put_tree() { # <source/.> <host> <destination/>
    local source=$1 host=$2 destination=$3
    if [ "${CLUSTER_OBS_DRY_RUN:-0}" = 1 ]; then
        echo "[dry-run] put tree $source -> $host:$destination" >&2
        return 0
    fi
    if [ "$(_host_ip "$host")" = "$self_ip" ]; then
        cp -r "$source" "$destination"
    else
        scp -q -r "$source" "$host:$destination"
    fi
}

_deepmsg_dir_check() { # <host>
    _on "$1" "test -d '$CLUSTER_DEEPMSG_DIR'" ||
        _die "$1 has no deepmsg checkout at $CLUSTER_DEEPMSG_DIR"
    _on "$1" "test -x '$CLUSTER_DEEPMSG_DIR/deploy/observability/e2e/fetch-tools.sh'" ||
        _die "$1 has no $CLUSTER_DEEPMSG_DIR/deploy/observability/e2e/fetch-tools.sh"
}

# The pinned releases are fetched and verified by deepmsg's own script, so the
# SHA256 table lives in one place.
_fetch_tools() { # <host>
    _on "$1" "set -eu
        DEEPMSG_OBS_TOOLS='$CLUSTER_DEEPMSG_DIR/target/observability-tools' \
            '$CLUSTER_DEEPMSG_DIR/deploy/observability/e2e/fetch-tools.sh' >/dev/null" ||
        _die "$1 could not fetch the pinned tools"
}

_install_binary() { # <host> <source> <name>
    _on "$1" "install -m 0755 '$2' /usr/local/bin/$3"
}

# --- the four application hosts ---------------------------------------------

install_app_host() { # <host> <member|client>
    local host=$1 which=$2 node aeron cluster endpoint units unit
    node=$(_node_id "$which")
    aeron=$(_adir "$which")
    cluster=$(_cluster_dir "$which")
    endpoint=$(_app_endpoint "$which")

    _on "$host" "set -eu
        command -v systemctl >/dev/null || { echo 'no systemd on this host' >&2; exit 1; }
        id -u deepmsg >/dev/null 2>&1 ||
            useradd --system --no-create-home --shell /usr/sbin/nologin deepmsg
        mkdir -p /etc/deepmsg /var/lib/deepmsg-otelcol /var/log/deepmsg
        chown deepmsg:deepmsg /var/lib/deepmsg-otelcol
        chmod 0750 /var/lib/deepmsg-otelcol
        chmod 0755 /var/log/deepmsg" || _die "$host: users or directories"

    _deepmsg_dir_check "$host"
    _fetch_tools "$host"
    _install_binary "$host" \
        "$CLUSTER_DEEPMSG_DIR/target/observability-tools/bin/otelcol-contrib" \
        otelcol-contrib
    _install_binary "$host" \
        "$CLUSTER_DEEPMSG_DIR/target/release/deepmsg-counters-exporter" \
        deepmsg-counters-exporter
    _install_binary "$host" \
        "$CLUSTER_DEEPMSG_DIR/target/release/deepmsg-errorlog-exporter" \
        deepmsg-errorlog-exporter

    _put "$OBS_SOURCE/deploy/observability/agent/otelcol.yaml" \
        "$host" /etc/deepmsg/otelcol.yaml
    _put "$OBS_SOURCE/deploy/observability/agent/otelcol-app.yaml" \
        "$host" /etc/deepmsg/otelcol-app.yaml

    # The tokens are in this file: 0600, and systemd reads it as root before
    # the units drop to their user.
    mkdir -p "$stage"
    cat >"$stage/observability.env" <<EOF
DEEPMSG_LOG_DIR=/var/log/deepmsg
DEEPMSG_OBS_STATE_DIR=/var/lib/deepmsg-otelcol
DEEPMSG_NODE_ID=$node
DEEPMSG_AERON_DIR=$aeron
DEEPMSG_CLUSTER_DIR=$cluster
DEEPMSG_COUNTERS_ENDPOINT=127.0.0.1:9101
DEEPMSG_OTELCOL_TELEMETRY_ENDPOINT=127.0.0.1:8888
DEEPMSG_APP_METRICS_ENDPOINT=$endpoint
DEEPMSG_VL_LOGS_ENDPOINT=http://$(_obs_ip):8427/insert/opentelemetry/v1/logs
DEEPMSG_VM_WRITE_ENDPOINT=http://$(_obs_ip):8427/api/v1/write
DEEPMSG_OBS_WRITE_TOKEN=$CLUSTER_OBS_WRITE_TOKEN
DEEPMSG_HOSTMETRICS_ROOT=/
EOF
    _put "$stage/observability.env" "$host" /etc/deepmsg/observability.env 0600

    units=(
        deepmsg-app-agent
        deepmsg-log-agent
        deepmsg-counters-exporter
        deepmsg-errorlog-exporter
    )
    for unit in "${units[@]}"; do
        _put "$OBS_SOURCE/deploy/observability/systemd/$unit.service" \
            "$host" "/etc/systemd/system/$unit.service"
    done

    # A member's cluster directory lives under the run root, which is under
    # /root. `ProtectHome=yes` would hide it from the exporter; read-only is
    # all the exporter needs, and the unit says so of everything it reads.
    if [ -n "$cluster" ]; then
        _on "$host" "set -eu
            mkdir -p /etc/systemd/system/deepmsg-errorlog-exporter.service.d
            printf '[Service]\nProtectHome=read-only\n' \
                > /etc/systemd/system/deepmsg-errorlog-exporter.service.d/10-deepbook-cluster-dir.conf"
    fi

    # ★ `deepmsg-app-agent` and `deepmsg-log-agent` share a state directory,
    # so exactly one is enabled: every host in this deployment has an
    # application /metrics endpoint, so it is the overlay. The base unit is
    # installed for a host that later loses its endpoint.
    _on "$host" "set -eu
        systemctl daemon-reload
        systemctl disable --now deepmsg-log-agent >/dev/null 2>&1 || true
        systemctl enable deepmsg-app-agent deepmsg-counters-exporter \
            deepmsg-errorlog-exporter >/dev/null
        systemctl restart deepmsg-app-agent deepmsg-counters-exporter \
            deepmsg-errorlog-exporter" || _die "$host: the units did not start"
}

# --- the fifth host ----------------------------------------------------------

install_obs_host() {
    local host obs_ip unit grafana=0
    host=$(_obs_host)
    obs_ip=$(_obs_ip)

    _on "$host" "set -eu
        command -v systemctl >/dev/null || { echo 'no systemd on this host' >&2; exit 1; }
        for user in victoria vmauth; do
            id -u \$user >/dev/null 2>&1 ||
                useradd --system --no-create-home --shell /usr/sbin/nologin \$user
        done
        mkdir -p /etc/deepmsg /var/lib/victoria-logs /var/lib/victoria-metrics
        chown victoria:victoria /var/lib/victoria-logs /var/lib/victoria-metrics
        chmod 0750 /var/lib/victoria-logs /var/lib/victoria-metrics" ||
        _die "$host: users or directories"

    _deepmsg_dir_check "$host"
    _fetch_tools "$host"
    _install_binary "$host" \
        "$CLUSTER_DEEPMSG_DIR/target/observability-tools/bin/victoria-logs" \
        victoria-logs
    _install_binary "$host" \
        "$CLUSTER_DEEPMSG_DIR/target/observability-tools/bin/victoria-metrics" \
        victoria-metrics
    _install_binary "$host" \
        "$CLUSTER_DEEPMSG_DIR/target/observability-tools/bin/vmauth" \
        vmauth

    # Two users, two powers: the agents write, Grafana reads. Both backends
    # stay on loopback behind vmauth, which is the only thing the agents reach
    # over the network.
    sed -e "s|__WRITE_TOKEN__|$CLUSTER_OBS_WRITE_TOKEN|" \
        -e "s|__READ_TOKEN__|$CLUSTER_OBS_READ_TOKEN|" \
        -e 's|__VL_HOST__|127.0.0.1|g' \
        -e 's|__VM_HOST__|127.0.0.1|g' \
        "$OBS_SOURCE/deploy/observability/backends/vmauth.example.yaml" \
        >"$stage/vmauth.yaml"
    _put "$stage/vmauth.yaml" "$host" /etc/deepmsg/vmauth.yaml 0640
    _on "$host" "chown root:vmauth /etc/deepmsg/vmauth.yaml"

    for unit in victoria-logs victoria-metrics vmauth; do
        _put "$OBS_SOURCE/deploy/observability/systemd/$unit.service" \
            "$host" "/etc/systemd/system/$unit.service"
    done
    # The cluster profile: the agents connect from the other four hosts, so
    # vmauth listens on the private address, not loopback.
    _on "$host" "sed -i 's|-httpListenAddr=127.0.0.1:8427|-httpListenAddr=$obs_ip:8427|' \
        /etc/systemd/system/vmauth.service"

    # Grafana is a distribution package, not one of the pinned tarballs; its
    # unit and provisioning are installed only where the binary already is.
    if _on "$host" "command -v grafana >/dev/null 2>&1 || test -x /usr/local/bin/grafana"; then
        grafana=1
        _on "$host" "set -eu
            id -u grafana >/dev/null 2>&1 ||
                useradd --system --no-create-home --shell /usr/sbin/nologin grafana
            mkdir -p /var/lib/grafana /var/log/grafana /etc/deepmsg/grafana
            chown grafana:grafana /var/lib/grafana /var/log/grafana
            chmod 0750 /var/lib/grafana /var/log/grafana"
        _put "$OBS_SOURCE/deploy/observability/grafana/grafana.ini" \
            "$host" /etc/deepmsg/grafana.ini
        for entry in provisioning/datasources provisioning/dashboards; do
            _on "$host" "mkdir -p /etc/deepmsg/grafana/$entry"
            _put_tree "$OBS_SOURCE/deploy/observability/grafana/$entry/." \
                "$host" "/etc/deepmsg/grafana/$entry/"
        done
        _put "$OBS_SOURCE/deploy/observability/systemd/grafana.service" \
            "$host" /etc/systemd/system/grafana.service

        cat >"$stage/observability.env" <<EOF
DEEPMSG_VMAUTH_URL=http://$obs_ip:8427
DEEPMSG_GRAFANA_READ_TOKEN=$CLUSTER_OBS_READ_TOKEN
DEEPMSG_GRAFANA_DASHBOARDS_DIR=/etc/deepmsg/grafana/provisioning/dashboards
DEEPMSG_GRAFANA_WEBHOOK_URL=http://127.0.0.1:9099/alerts
GF_SECURITY_ADMIN_PASSWORD=${CLUSTER_OBS_GRAFANA_PASSWORD:-replace-me}
EOF
        _put "$stage/observability.env" "$host" /etc/deepmsg/observability.env 0600
    else
        echo "observability: grafana is not installed on $host; the dashboards wait for it" >&2
    fi

    _on "$host" "set -eu
        systemctl daemon-reload
        systemctl enable victoria-logs victoria-metrics vmauth >/dev/null
        systemctl restart victoria-logs victoria-metrics vmauth" ||
        _die "$host: the backends did not start"
    if [ "$grafana" -eq 1 ]; then
        _on "$host" "systemctl enable grafana >/dev/null && systemctl restart grafana" ||
            _die "$host: grafana did not start"
    fi
}

# --- status ------------------------------------------------------------------

status_app_host() { # <host> <member|client>
    local host=$1 which=$2 endpoint port fails=0
    endpoint=$(_app_endpoint "$which")
    port=${endpoint#127.0.0.1:}
    if ! _on "$host" "set -u
        for unit in deepmsg-app-agent deepmsg-counters-exporter deepmsg-errorlog-exporter; do
            state=\$(systemctl is-active \$unit 2>/dev/null || true)
            [ \"\$state\" = active ] || { echo \"\$unit is \$state\"; exit 1; }
        done
        systemctl is-enabled --quiet deepmsg-app-agent 2>/dev/null || {
            echo 'deepmsg-app-agent is not enabled'; exit 1; }
        test -s /etc/deepmsg/observability.env || { echo 'no /etc/deepmsg/observability.env'; exit 1; }
        test -d /var/log/deepmsg || { echo 'no /var/log/deepmsg'; exit 1; }
        ss -ltn 2>/dev/null | grep -q '127.0.0.1:9101' || { echo 'nothing listens on 9101'; exit 1; }
        ss -ltn 2>/dev/null | grep -q \"127.0.0.1:\$port\" || {
            echo \"nothing listens on \$port (the application is down?)\"; exit 1; }
        echo 'ok'"; then
        fails=1
    fi
    return "$fails"
}

status_obs_host() {
    local host fails=0
    host=$(_obs_host)
    if ! _on "$host" "set -u
        for unit in victoria-logs victoria-metrics vmauth; do
            state=\$(systemctl is-active \$unit 2>/dev/null || true)
            [ \"\$state\" = active ] || { echo \"\$unit is \$state\"; exit 1; }
        done
        ss -ltn 2>/dev/null | grep -q '127.0.0.1:9428' || { echo 'VictoriaLogs is not listening'; exit 1; }
        ss -ltn 2>/dev/null | grep -q '127.0.0.1:8428' || { echo 'VictoriaMetrics is not listening'; exit 1; }
        ss -ltn 2>/dev/null | grep -q ':8427' || { echo 'vmauth is not listening'; exit 1; }
        echo 'ok'"; then
        fails=1
    fi
    return "$fails"
}

# --- commands ----------------------------------------------------------------

cmd_up() {
    [ -d "$OBS_SOURCE/deploy/observability" ] ||
        _die "no deepmsg checkout at $OBS_SOURCE (set DEEPBOOK_DEEPMSG_DIR)"
    [ -n "$CLUSTER_OBS_SSH" ] || _die "CLUSTER_OBS_SSH is not set (the fifth host)"
    [ -n "${CLUSTER_OBS_WRITE_TOKEN:-}" ] || _die "CLUSTER_OBS_WRITE_TOKEN is not set"
    [ -n "${CLUSTER_OBS_READ_TOKEN:-}" ] || _die "CLUSTER_OBS_READ_TOKEN is not set"
    mkdir -p "$stage"

    local i which
    for ((i = 0; i < MEMBERS; i++)); do
        which=$i
        echo "== $(_node_id "$which") ($(_host "$which"))"
        install_app_host "$(_host "$which")" "$which"
    done
    echo "== client ($(_host client))"
    install_app_host "$(_host client)" client
    echo "== observability ($(_obs_host))"
    install_obs_host
    echo "== up"
}

cmd_status() {
    local i which fails=0
    for ((i = 0; i < MEMBERS; i++)); do
        which=$i
        if status_app_host "$(_host "$which")" "$which"; then
            echo "$(_node_id "$which"): ok"
        else
            echo "$(_node_id "$which"): FAILED" >&2
            fails=1
        fi
    done
    if status_app_host "$(_host client)" client; then
        echo "client: ok"
    else
        echo "client: FAILED" >&2
        fails=1
    fi
    if [ -n "$CLUSTER_OBS_SSH" ]; then
        if status_obs_host; then
            echo "observability: ok"
        else
            echo "observability: FAILED" >&2
            fails=1
        fi
    else
        echo "observability: CLUSTER_OBS_SSH is not set, not checked" >&2
        fails=1
    fi
    [ "$fails" -eq 0 ] || _die "the stack is not up"
}

cmd_down() {
    local i unit
    for ((i = 0; i < MEMBERS; i++)); do
        _on "$(_host "$i")" "for unit in deepmsg-app-agent deepmsg-counters-exporter \
            deepmsg-errorlog-exporter; do systemctl disable --now \$unit >/dev/null 2>&1 || true; done" || true
    done
    _on "$(_host client)" "for unit in deepmsg-app-agent deepmsg-counters-exporter \
        deepmsg-errorlog-exporter; do systemctl disable --now \$unit >/dev/null 2>&1 || true; done" || true
    if [ -n "$CLUSTER_OBS_SSH" ]; then
        _on "$(_obs_host)" "for unit in grafana vmauth victoria-metrics victoria-logs; do \
            systemctl disable --now \$unit >/dev/null 2>&1 || true; done" || true
    fi
    # ★ The logs and the backends' data stay: `down` stops processes, it does
    # not delete the evidence. `/var/log/deepmsg` is outside the run root on
    # purpose (hosts.env).
    echo "observability: stopped; /var/log/deepmsg and the backend data are untouched"
}

case "${1:-}" in
    up) cmd_up ;;
    status) cmd_status ;;
    down) cmd_down ;;
    "" | -h | --help)
        sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'
        ;;
    *) _die "unknown command '$1' (up, status, down)" ;;
esac
