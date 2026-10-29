//! P1-3: deepbook's own stack on three nodes, and a leader kill.
//!
//! The M0 spike proved deepmsg hands a client to a new leader (gate 4, on
//! `basic-auction`). This run asks the same question of the product: every
//! member runs `engine-container`, the client is `fix-gateway`, and the check
//! kills the leader's three processes halfway through a FIX session.
//!
//! ```text
//! node-<id>/   deepmsg-archiving-media-driver + deepmsg-cluster-node
//!              + engine-container            (one driver per member)
//! client/      a driver the gateway owns, so a killed member cannot take the
//!              gateway's egress subscription down
//! ```
//!
//! # What the run checks
//!
//! 1. **The book survives.** A resting sell is placed before the kill and a
//!    buy is filled against it in three slices: one before the kill, one as
//!    soon as the new leader serves, one after. The last slice can only fill
//!    if the first two were on the replicated log and replayed by the new
//!    leader.
//! 2. **The FIX session survives.** The connection is never reopened; the
//!    gateway moves its cluster session underneath it. The post-kill order is
//!    resent under the same `ClOrdID` until a report arrives, which is the
//!    client behaviour ADR 0003 makes safe.
//! 3. **No duplicate fills.** Each `ClOrdID` produces at most one `Trade`
//!    report across both leaders, including the resends.
//!
//! The timings below are a report, not a gate: the heartbeat timeout is the
//! test configuration's 2 s (see [`crate::nodes`]), not the product's 10 s.
//!
//! # Market data
//!
//! Every member applies the log, so every member would publish its own
//! market-data stream from its own driver. The run turns market data off
//! (`deepbook.marketdata.enabled=false`) and observes the engine through FIX
//! reports; publishing only from the leader needs `on_role_change`, which the
//! pinned deepmsg does not hand a `Cluster`.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Child;
use std::time::{Duration, Instant};

use deepbook_fix::{ExecType, ExecutionReport, OrdStatus};

use crate::config::{Config, Ports};
use crate::fix::FixClient;
use crate::nodes::{ClusterNodes, LEADER_HEARTBEAT_TIMEOUT, Service};
use crate::process::{spawn, tail};
use crate::report::millis;

/// How long the cluster may take to elect its first leader.
const ELECTION_TIMEOUT: Duration = Duration::from_secs(60);
/// How long the new leader may take to be elected and serve after the kill.
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(30);
/// The wait for one expected report.
const REPORT_TIMEOUT: Duration = Duration::from_secs(10);
/// How often the post-kill order is resent while it has no answer.
const RESEND_INTERVAL: Duration = Duration::from_millis(250);
/// The resting order's price in ticks, and its quantity.
const REST_PRICE: i64 = 10_025;
const REST_QTY: i64 = 10;

/// Run the failover check.
///
/// # Errors
///
/// The three members did not start or elect, the gateway did not start, an
/// order was not acknowledged, the new leader did not serve within
/// [`RECOVERY_TIMEOUT`], the book did not survive, or an order produced more
/// than one trade.
pub fn run(config: Config) -> Result<(), String> {
    let mut cluster = ClusterNodes::start(config, Service::Engine)?;
    let ports = cluster.ports();
    let members = cluster.members();
    let leader = cluster.wait_for_leader(ELECTION_TIMEOUT)?;
    println!(
        "failover: leader={leader} members={members} heartbeat={LEADER_HEARTBEAT_TIMEOUT} \
         run_dir={}",
        cluster.run_dir().display()
    );

    let gateway = Gateway::start(
        cluster.run_dir(),
        &cluster.client_aeron_dir(),
        ports,
        members,
    )?;
    let mut fix = FixClient::connect(gateway.address)?;
    println!("failover: fix session up at {}", gateway.address);

    // ---- before: a resting sell and one fill against it --------------------
    fix.new_order("S1", 1, true, REST_QTY, REST_PRICE)?;
    let ack = wait_report(&mut fix, "S1", |report| report.exec_type == ExecType::New)?;
    if ack.leaves_qty != REST_QTY || ack.ord_status != OrdStatus::New {
        return Err(format!("the resting sell was not accepted: {ack:?}"));
    }

    fix.new_order("B1", 2, false, 4, REST_PRICE)?;
    let fill = wait_report(&mut fix, "B1", |report| report.exec_type == ExecType::Trade)?;
    if fill.last_qty != Some(4) || fill.cum_qty != 4 {
        return Err(format!("the first fill is not 4 lots: {fill:?}"));
    }
    println!(
        "failover: pre-kill S1 leaves={} B1 cum={} member={leader}",
        REST_QTY - 4,
        fill.cum_qty
    );

    // ---- the kill ----------------------------------------------------------
    let killed_at = Instant::now();
    cluster.kill_node(leader)?;
    println!("failover: killed leader {leader}");

    let new_leader = cluster.wait_for_leader(RECOVERY_TIMEOUT)?;
    let elected = killed_at.elapsed();
    if new_leader == leader {
        return Err(format!(
            "member {leader} still leads after its processes were killed"
        ));
    }
    println!(
        "failover: new_leader={new_leader} elected={:.1}ms",
        millis(elected)
    );

    // ---- after: the same session fills against the replicated book ---------
    // The order is resent under one ClOrdID until a report arrives. An offer
    // into the old leader's publication can be accepted and lost, so one send
    // is not enough; the engine's idempotency window makes the resend safe.
    let mut b2_trades = 0;
    let deadline = killed_at + RECOVERY_TIMEOUT;
    let mut next_send = Instant::now();
    while Instant::now() < deadline {
        if Instant::now() >= next_send {
            fix.new_order("B2", 3, false, 3, REST_PRICE)?;
            next_send = Instant::now() + RESEND_INTERVAL;
        }
        let Some(report) = fix.read_report(Duration::from_millis(50))? else {
            continue;
        };
        if report.cl_ord_id == "B2" && report.exec_type == ExecType::Trade {
            b2_trades += 1;
            break;
        }
    }
    let recovered = killed_at.elapsed();
    if b2_trades != 1 {
        return Err(format!(
            "the post-kill buy did not fill within {RECOVERY_TIMEOUT:?} \
             (trades={b2_trades}); the committed book did not survive the failover"
        ));
    }
    println!(
        "failover: post-kill fill at {:.1}ms, session held through the failover",
        millis(recovered)
    );

    // ---- and the rest of the resting order fills normally ------------------
    fix.new_order("B3", 4, false, 3, REST_PRICE)?;
    let mut b3_trades = 0;
    let mut s1_filled = false;
    let deadline = Instant::now() + REPORT_TIMEOUT;
    while Instant::now() < deadline && (!s1_filled || b3_trades == 0) {
        let Some(report) = fix.read_report(Duration::from_millis(100))? else {
            continue;
        };
        if report.cl_ord_id == "B3" && report.exec_type == ExecType::Trade {
            b3_trades += 1;
        }
        if report.cl_ord_id == "S1" && report.ord_status == OrdStatus::Filled {
            if report.cum_qty != REST_QTY {
                return Err(format!("S1 filled at {} not {REST_QTY}", report.cum_qty));
            }
            s1_filled = true;
        }
    }
    if b3_trades != 1 {
        return Err(format!(
            "the third buy produced {b3_trades} trades, not one"
        ));
    }
    if !s1_filled {
        return Err("the resting sell never showed Filled after the third buy".to_owned());
    }

    fix.logout();
    println!(
        "failover: ok old_leader={leader} new_leader={new_leader} \
         elected={:.1}ms recovered={:.1}ms trades=one-per-order s1_filled=true",
        millis(elected),
        millis(recovered)
    );
    Ok(())
}

/// Read reports until one for `cl_ord_id` satisfies `wanted`.
fn wait_report(
    fix: &mut FixClient,
    cl_ord_id: &str,
    wanted: impl Fn(&ExecutionReport) -> bool,
) -> Result<ExecutionReport, String> {
    let deadline = Instant::now() + REPORT_TIMEOUT;
    while Instant::now() < deadline {
        let Some(report) = fix.read_report(Duration::from_millis(100))? else {
            continue;
        };
        if report.cl_ord_id == cl_ord_id && wanted(&report) {
            return Ok(report);
        }
    }
    Err(format!("no {cl_ord_id} report before {REPORT_TIMEOUT:?}"))
}

/// The gateway as a child process, with its own log.
struct Gateway {
    child: Child,
    address: SocketAddr,
}

impl Gateway {
    fn start(root: &Path, aeron_dir: &Path, ports: Ports, members: usize) -> Result<Self, String> {
        let binary = gateway_binary()?;
        let address: SocketAddr = format!("127.0.0.1:{}", ports.base + 50)
            .parse()
            .expect("the FIX address parses");
        let log = root.join("gateway.log");
        let endpoints = (0..members)
            .map(|member| format!("{member}=localhost:{}", ports.ingress_of(member)))
            .collect::<Vec<_>>()
            .join(",");
        let egress = format!(
            "aeron:udp?term-length=128k|endpoint=localhost:{}",
            ports.client_egress()
        );
        let arguments = vec![
            "--bind".to_owned(),
            address.to_string(),
            "--session-dir".to_owned(),
            root.join("gateway-sessions").display().to_string(),
            "--cluster".to_owned(),
            "--aeron-dir".to_owned(),
            aeron_dir.display().to_string(),
            "--ingress-channel".to_owned(),
            "aeron:udp?term-length=128k".to_owned(),
            "--ingress-endpoints".to_owned(),
            endpoints,
            "--egress-channel".to_owned(),
            egress,
            "--message-timeout-ms".to_owned(),
            "5000".to_owned(),
            // If the one-shot NewLeaderEvent is lost the client gives up after
            // this and the gateway opens a fresh session on a new egress port
            // (see `ClusterEngine::reconnect`). The event arrives in every run
            // so far, so this path is the fallback rather than the norm.
            "--new-leader-timeout-ms".to_owned(),
            "3000".to_owned(),
            "--tick-us".to_owned(),
            "1000".to_owned(),
            "--symbol".to_owned(),
            "AAPL".to_owned(),
            "--symbol-id".to_owned(),
            "1".to_owned(),
        ];
        let mut child = spawn(&binary, &arguments, &log)?;
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline {
            if let Ok(text) = std::fs::read_to_string(&log) {
                if text.contains("listening on") {
                    return Ok(Self { child, address });
                }
            }
            if let Some(status) = child
                .try_wait()
                .map_err(|error| format!("gateway wait: {error}"))?
            {
                return Err(format!(
                    "the gateway exited while starting ({status}):\n{}",
                    tail(&log)
                ));
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        Err(format!(
            "the gateway did not listen within 20s:\n{}",
            tail(&log)
        ))
    }
}

impl Drop for Gateway {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// `fix-gateway` from `DEEPBOOK_FIX_GATEWAY` or beside the running binary.
fn gateway_binary() -> Result<PathBuf, String> {
    if let Ok(path) = std::env::var("DEEPBOOK_FIX_GATEWAY") {
        let path = PathBuf::from(path);
        if path.is_file() {
            return Ok(path);
        }
        return Err(format!(
            "DEEPBOOK_FIX_GATEWAY={} is not a file",
            path.display()
        ));
    }
    let exe = std::env::current_exe().map_err(|error| format!("current exe: {error}"))?;
    let candidate = exe.with_file_name(format!("fix-gateway{}", std::env::consts::EXE_SUFFIX));
    if candidate.is_file() {
        Ok(candidate)
    } else {
        Err(format!(
            "fix-gateway is not next to {}; build it with the same profile:\n  \
             cargo build -p deepbook-gateway --bin fix-gateway",
            exe.display()
        ))
    }
}
