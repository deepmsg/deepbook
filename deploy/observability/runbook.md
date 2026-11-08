# deepbook observability runbook

The four-host cluster ships JSON logs to VictoriaLogs and metrics to
VictoriaMetrics; these six alerts turn those series into pages. The rules are
`deploy/observability/grafana/alerting/deepbook-rules.yaml`; `observability.sh
up` copies them into the log host's Grafana provisioning directory next to
deepmsg's six. The dashboard is `deepbook-engine` (folder `deepbook`);
deepmsg's `cluster-health` (folder `deepmsg`) carries the cluster counters.

Every check below runs on the log host (`CLUSTER_OBS_SSH`), where vmauth
listens on `127.0.0.1:8427` and Grafana on port 3000. Both backends answer
through vmauth with the **read** token (`$CLUSTER_OBS_READ_TOKEN`); the
agents' write token is not a query key. `run-cluster.sh` commands run on the
controller (node0).

## The alerts

| anchor | severity | expression | for |
|---|---|---|---|
| [`deepbook-engine-count`](#deepbook-engine-count) | P1 | `count(deepbook_engine_up) < 3` | 1m |
| [`deepbook-no-leader`](#deepbook-no-leader) | P1 | `max(deepmsg_cluster_is_leader) < 1` | 30s |
| [`deepbook-commit-stalled`](#deepbook-commit-stalled) | P1 | three engines up and orders arriving, but `increase(deepmsg_cluster_commit_position[2m]) == 0` | 2m |
| [`deepbook-gateway-lost`](#deepbook-gateway-lost) | P1 | `absent(deepbook_gateway_sessions) or deepbook_gateway_sessions == 0` | 1m |
| [`deepbook-reject-rate`](#deepbook-reject-rate) | P2 | `rate(deepbook_gateway_rejects_total[5m]) > 0` | 5m |
| [`deepbook-log-dropped`](#deepbook-log-dropped) | P2 | LogsQL `event:"log.dropped" \| stats count()` > 0 | 1m |

The two checks every section uses:

```sh
# metrics (PromQL through vmauth)
curl -s -H "Authorization: Bearer $CLUSTER_OBS_READ_TOKEN" \
  'http://127.0.0.1:8427/api/v1/query?query=count(deepbook_engine_up)'
# logs (LogsQL through vmauth)
curl -s -H "Authorization: Bearer $CLUSTER_OBS_READ_TOKEN" \
  'http://127.0.0.1:8427/select/logsql/query' \
  --data-urlencode 'query=event:"role.change"' --data-urlencode 'limit=20'
```

## Where the numbers come from

| series | source | host |
|---|---|---|
| `deepbook_engine_*` | `engine-container`'s loopback page, `127.0.0.1:26100` | node0/1/2 |
| `deepbook_gateway_*` | `fix-gateway`'s loopback page, `127.0.0.1:26101` | client |
| `deepmsg_cluster_*`, `deepmsg_driver_*` | the counters exporter, `127.0.0.1:9101`, reading the CnC file | all four |

★ **The CnC keeps answering after the process dies.** `deepmsg_cluster_is_leader`
and `deepmsg_cluster_commit_position` are memory-mapped records, not
heartbeats: a dead leader's CnC still says `leader`. `deepbook_engine_up` is a
constant the engine process serves itself, so its absence — not the CnC's
answer — is the liveness signal. That is why there are two leader rules.

The applications' own logs are `/var/log/deepmsg/<component>-<node>.json`;
their stderr and the script-level logs (`driver.log`, `module.log`,
`container.log`, `gateway.log`) live under the run root,
`/root/deepbook/cluster-4host/`. `run-cluster.sh up` removes the run root, and
the JSON logs are outside it on purpose.

<a id="deepbook-engine-count"></a>
## Fewer than three engines are up (P1)

**What it means:** fewer than three members report `deepbook_engine_up`. Either
an engine process is gone, or the member's agent stopped shipping its metrics.
No data at all also fires: zero reporting members is this rule's meaning.

**First checks:**

- Dashboard `deepbook-engine`, panel **engines up** (one line per member).
- `deploy/cluster-4host/run-cluster.sh status` — the member line says
  `not running`, `follower` or `leader`.
- On a suspect member: `ss -ltn | grep 26100`,
  `systemctl status deepmsg-app-agent deepmsg-counters-exporter`,
  `tail -20 /root/deepbook/cluster-4host/member<N>/container.log`.

**Action:** if the member is a follower, `run-cluster.sh restart <member>
--keep`. `--keep` keeps the archive and cluster directory, so the member
rejoins as the member it was; without it the restart is a fresh join. If the
leader is the missing one, the cluster elects without it; restart it with
`--keep` once a new leader exists (`run-cluster.sh restart` refuses to restart
the leader — forcing a failover is `kill-leader`). If the process is up but the
agent is down, `systemctl restart deepmsg-app-agent`; the series returns at the
next scrape (15 s).

**Rollback:** none — the restart is the action.

<a id="deepbook-no-leader"></a>
## The cluster reports no leader (P1)

**What it means:** every member's cluster counter says it is not the leader
(or no member answered). This is the cluster's *belief*, read from the CnC; it
cannot see a dead leader.

**First checks:**

- Dashboard `deepbook-engine`, panel **leaders (cluster counters)**, and
  deepmsg's `cluster-health` role/election panels.
- `deploy/cluster-4host/run-cluster.sh counters` — the CnC counters per member.
- VictoriaLogs: `event:"role.change" or event:"election.complete"`.

**Action:** give the election its time. On these machines a failover was
measured at 13.6 s with the default image liveness and 5.6 s with
`CLUSTER_IMAGE_LIVENESS=2s` (2026-11-04). If the counter has been 0 for longer
than that, read the members' `module.log`: members disagreeing about the term
restart cleanly (`run-cluster.sh restart <member> --keep`, one at a time — not
the suspicious member last). If no member can be elected at all, the cluster
needs a fresh start: `run-cluster.sh up` (wipes the run root, including the
snapshots; the JSON logs stay).

**Rollback:** none.

<a id="deepbook-commit-stalled"></a>
## Commit position is not moving while orders arrive (P1)

**What it means:** all three engines report, the gateway is forwarding orders
(`deepbook_gateway_orders_total` is rising), and the cluster's commit position
has not advanced for two minutes. The consensus module is accepting work but
not committing it. An idle cluster does not fire — the order leg is what makes
the stall meaningful.

**First checks:**

- Dashboard `deepbook-engine`, panel **commit position and applied commands**,
  and **applied, filled and rejected per second**.
- VictoriaLogs: `component:"engine" | event:"engine.stats"` — the 5 s
  aggregation carries the same counters.
- `deploy/cluster-4host/run-cluster.sh counters`; the leader's `module.log`
  (journal) and `container.log`.

**Action:** a stalled commit is usually the journal: a full disk or a wedged
fsync (`df -h /root`, `iostat -x 1`). If the leader is alive but not
committing, force a failover with `run-cluster.sh kill-leader`; the new leader
replays from its archive. If the journal filesystem is full, free space first —
a failover onto the same full disk does not commit either.

**Rollback:** none.

<a id="deepbook-gateway-lost"></a>
## The gateway is not publishing a session (P1)

**What it means:** `deepbook_gateway_sessions` is missing (the page stopped
being scraped) or zero (the gateway is up with no FIX session). On the rig the
only client is the bench, so **outside a run this alert is expected**; during a
run it means the gateway or the session died.

**First checks:**

- `deploy/cluster-4host/run-cluster.sh status` — the `gateway:` line.
- On the client: `ss -ltn | grep 25050` (FIX, `base + 50`),
  `ps -o pid,stat,etime,cmd -p $(cat /root/deepbook/cluster-4host/client/gateway.pid)`,
  `tail -20 /root/deepbook/cluster-4host/client/gateway.log`.
- `systemctl status deepmsg-app-agent` on the client — if the agent is down,
  the series is stale and then absent, which is not the gateway's fault.

**Action:** restart the gateway from its generated script (the same one
`run-cluster.sh up` starts; it rewrites the pid file and appends to the log):

```sh
ssh root@172.16.1.17 'setsid bash /root/deepbook/cluster-4host/client/gateway.sh \
  >> /root/deepbook/cluster-4host/client/gateway.log 2>&1 < /dev/null &'
```

The FIX client reconnects and reopens its session. Do not run `run-cluster.sh
up` just for the gateway: it removes the run root, and with it the evidence.

**Rollback:** none.

<a id="deepbook-reject-rate"></a>
## The gateway is rejecting commands (P2)

**What it means:** the gateway refuses commands before the cluster sees them —
pre-trade risk, an unknown symbol, a refused cancel. The counter is
`deepbook_gateway_rejects_total`; a sustained rate for the whole five-minute
window plus five minutes is what pages.

**First checks:**

- Dashboard `deepbook-engine`, panel **gateway commands, reports and rejects
  per second**.
- `tail -50 /root/deepbook/cluster-4host/client/gateway.log` — the FIX reject
  carries the reason code and text.
- The engine's `deepbook_engine_rejected_total` on the same panel: a command
  the engine refused is counted there, not at the gateway.

**Action:** if the rejects are the bench's own (risk limits at the configured
rate), nothing is broken; note the reason and move on. If an unexpected client
is being refused, check the gateway's risk configuration and the symbol ids in
the generated `gateway.sh`. No restart is needed for a reject stream.

**Rollback:** none.

<a id="deepbook-log-dropped"></a>
## Application log lines are being dropped (P2)

**What it means:** a log writer's bounded queue is full and lines are being
dropped. The processes keep running — the contract is that an application never
blocks on logging — so what is lost is the record of what they did, not the
work itself.

**First checks:**

- VictoriaLogs: `event:"log.dropped"` — the line names `component` and
  `node_id`.
- On that host: `systemctl status deepmsg-app-agent`,
  `journalctl -u deepmsg-app-agent -n 50`, and the agent's own counters
  (`otelcol_exporter_send_failed_*`, `otelcol_exporter_queue_size`).
- Disk latency under `/var/log/deepmsg` (`iostat -x 1`).

**Action:** if the agent is down or wedged, `systemctl restart
deepmsg-app-agent`; the applications do not need a restart. If it recurs with
the agent healthy, the node is writing faster than the writer drains: drop
`CLUSTER_LOG_LEVEL` from `debug` to `info` (the launchers export it) and check
the disk. Lines already dropped cannot be recovered.

**Rollback:** none.

## Drills

Run these after a batch, never during a reading (they move the numbers they
would otherwise be evidence about). Each drill is only evidence if the
notifications reach the receiver behind `$CLUSTER_OBS_WEBHOOK_URL` — check the
receiver, not just the Grafana UI.

<a id="drill-kill-leader"></a>
### Kill the leader

```sh
deploy/cluster-4host/run-cluster.sh kill-leader --with-bench 2000 60000
```

Expect: the dead member's engine series goes stale, then
`deepbook-engine-count` fires and resolves once the member is restarted with
`--keep`; `engine.role` (component `engine`) and `role.change`/`election.complete`
(component `cluster`) appear in VictoriaLogs within the failover; the leader
count returns to exactly one; the gateway's session is not lost (the pinned
client reconnects).

<a id="drill-agent-off"></a>
### Stop one member's agent

On one member: `systemctl stop deepmsg-app-agent`. Expect: that node's series
go stale and `deepbook-engine-count` fires; deepmsg's `driver-metrics-absent`
fires as well; the application endpoints keep serving all along
(`curl -s 127.0.0.1:26100` answers). `systemctl start deepmsg-app-agent`
resolves both without touching the applications.

<a id="drill-gateway-kill"></a>
### Kill the gateway

On the client: `kill -9 $(cat /root/deepbook/cluster-4host/client/gateway.pid)`.
Expect: `deepbook-gateway-lost` fires once the series is stale or absent; the
client's FIX session ends. Restart with the command in
[`deepbook-gateway-lost`](#deepbook-gateway-lost); the alert resolves when the
new session is counted.

## Not provisioned

`deepbook_gateway_new_leader_timeouts_total` does not exist. The pinned deepmsg
client closes its cluster session when its `newLeaderTimeoutNs` expires but
does not expose *why* it closed, and counting every close as a leader timeout
would be a lie (`crates/gateway/src/metrics.rs`). The rule arrives when the
client exposes a close reason; the dashboard carries the same note.
