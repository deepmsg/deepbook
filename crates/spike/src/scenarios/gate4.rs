//! M0 gate 4: three nodes, kill the leader, measure the client's recovery.
//!
//! The gate has two claims, and the run checks both:
//!
//! 1. **the client recovers in under five seconds** — measured from the
//!    `SIGKILL` of the leader's three processes to the first ingress offer the
//!    new leader accepts, and again to that bid's egress;
//! 2. **no duplicate fills** — every correlation id the run sent produced
//!    exactly one egress.
//!
//! The first bid after the kill is deliberately **lower** than the pre-kill
//! best. It must lose, and its egress must report the pre-kill winner and
//! price: that is how the run shows that the bids committed before the kill
//! were not lost, without a separate state dump.
//!
//! `basic-auction` has no fill ledger, so "fill" here is the service's reply
//! for one bid. A duplicate reply for one correlation id is the observable a
//! duplicate fill would leave.
//!
//! The heartbeat timeout is 2 s, not the product's 10 s default; see
//! [`crate::nodes`] for why the five-second budget lives in that
//! configuration.
//!
//! # The reconnect fallback
//!
//! The new leader sends **one** `NewLeaderEvent` to each open session. On the
//! pinned deepmsg rev that event is sometimes lost when the old leader's
//! egress image has already closed: the new leader is active and serves a
//! fresh session at once, but the old session never hears about it (measured
//! five times in twenty-odd runs, always when member 0 was the new leader).
//! The reference's own failover tests handle the same situation with
//! `reconnectClient()`, so this check does too: if no event arrives shortly
//! after the new leader is active, it opens a **fresh session** and measures
//! that path. The output says which path it took; the event's loss is a client
//! finding, not a cluster one.

use std::cell::RefCell;
use std::rc::Rc;
use std::time::{Duration, Instant};

use crate::client::{self, Arrivals, BidEgress, Channels};
use crate::config::Config;
use crate::nodes::{ClusterNodes, LEADER_HEARTBEAT_TIMEOUT};
use crate::process::connect_timeout;
use crate::report::{self, millis};

/// The gate's budget.
const RECOVERY_BUDGET: Duration = Duration::from_secs(5);
/// How long the new leader may take to accept the probe before the run fails.
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(30);
/// The egress budget for one ordinary bid.
const EGRESS_TIMEOUT: Duration = Duration::from_secs(10);
/// How long the client waits for the `NewLeaderEvent` after the new leader is
/// active before it opens a fresh session. The event arrives with the leader
/// in every successful run; this is only a scheduling margin.
const FALLBACK_GRACE: Duration = Duration::from_millis(200);

/// Pre-kill bids start here and rise by one: every one is a new best.
const PRE_PRICE: i64 = 100;
/// Post-kill bids start here, above every pre-kill price for any sane `bids`.
const POST_PRICE: i64 = 10_000;

/// The correlation id of the bid that proves the new leader is serving.
const PROBE_CORRELATION: u64 = 1_000_000;
/// The first correlation id after the probe.
const POST_CORRELATION: u64 = 1_000_001;

/// Run the leader-kill check with `bids` bids before and after the kill.
///
/// # Errors
///
/// The cluster did not start or elect, a bid or its egress failed, recovery
/// took longer than [`RECOVERY_BUDGET`], the pre-kill state did not survive,
/// or a correlation id produced anything but one egress.
pub fn run(config: Config, bids: usize) -> Result<(), String> {
    if bids == 0 {
        return Err("bids must be non-zero".to_owned());
    }

    let mut cluster = ClusterNodes::start(config)?;
    let ports = cluster.ports();
    let members = cluster.members();
    let leader = cluster.wait_for_leader(Duration::from_secs(60))?;
    println!(
        "gate4: leader={leader} members={members} heartbeat={LEADER_HEARTBEAT_TIMEOUT} run_dir={}",
        cluster.run_dir().display()
    );

    let channels = Channels::udp(ports, members);
    let mut connected =
        client::connect_with(&cluster.client_aeron_dir(), &channels, connect_timeout())?;
    // Every session's egress listener. A reconnect keeps the old one in this
    // list so the duplicate accounting covers both sessions.
    let mut listeners: Vec<Rc<RefCell<Arrivals>>> = vec![Rc::clone(&connected.arrivals)];

    // ---- before: every bid is a new best ----------------------------------
    let mut before = None;
    for index in 0..bids {
        let correlation = u64::try_from(index).map_err(|_| "bid index".to_owned())?;
        let price = PRE_PRICE + i64::try_from(index).map_err(|_| "price".to_owned())?;
        connected.bid(correlation, correlation, price)?;
        let egress = connected.wait_for(correlation, EGRESS_TIMEOUT)?;
        let bid = BidEgress::decode(&egress.payload)
            .ok_or_else(|| format!("egress {correlation} is not a bid record"))?;
        if !bid.succeeded {
            return Err(format!("bid {correlation} at {price} did not win: {bid:?}"));
        }
        before = Some(bid);
    }
    let before = before.ok_or_else(|| "no bid produced an egress".to_owned())?;
    let expected_before_best = PRE_PRICE + i64::try_from(bids).map_err(|_| "bids".to_owned())? - 1;
    let expected_before_winner = i64::try_from(bids - 1).map_err(|_| "bids".to_owned())?;
    if before.best_price != expected_before_best || before.winner != expected_before_winner {
        return Err(format!(
            "unexpected pre-kill state: {before:?}, \
             expected winner={expected_before_winner} best={expected_before_best}"
        ));
    }
    println!(
        "gate4: pre-kill winner={} best={} member={leader} sessions={:?} client_session={}",
        before.winner,
        before.best_price,
        cluster.session_ids(leader),
        connected.session.cluster_session_id()
    );

    // ---- the kill ---------------------------------------------------------
    // The listener is reset first so the NewLeaderEvent that follows is the
    // failover's, not any event the connect delivered.
    connected.reset_new_leader();
    let killed_at = Instant::now();
    cluster.kill_node(leader)?;
    println!("gate4: killed leader {leader}");

    // The cluster elects first, then the client is told. Both the cluster and
    // the client are driven from here on: the client must be polled from the
    // kill onward, or its new-leader budget starts whenever the next poll
    // happens. Each distinct member state is printed, so a slow election says
    // where the time went.
    let deadline = killed_at + RECOVERY_TIMEOUT;
    let mut last_states = String::new();
    let mut next_trace = killed_at + Duration::from_secs(1);
    let mut reported_processes = false;
    let mut new_leader = None;
    let mut elected = Duration::ZERO;
    let mut elected_at = None;
    let mut told = None;
    let mut reconnected = false;
    let mut session_closed_reported = false;

    loop {
        connected.poll();
        if new_leader.is_none() {
            if let Some(candidate) = cluster.agreed_leader() {
                elected_at = Some(Instant::now());
                elected = Instant::now().duration_since(killed_at);
                println!(
                    "gate4: new_leader={candidate} elected={:.1}ms sessions={:?} client_session={}",
                    millis(elected),
                    cluster.session_ids(candidate),
                    connected.session.cluster_session_id()
                );
                new_leader = Some(candidate);
            }
        }
        if told.is_none() {
            if let Some(at) = connected.arrivals.borrow().new_leader_at {
                told = Some(at.duration_since(killed_at));
                println!(
                    "gate4: client told about the new leader at {:.1}ms",
                    millis(told.unwrap_or_default())
                );
            }
        }
        if new_leader.is_some() && told.is_some() {
            break;
        }

        let now = Instant::now();

        // The one-shot event can be lost. Once the new leader is active (or
        // the old session has closed) the client opens a fresh session; the
        // gate then measures that recovery, which is what the reference's own
        // `reconnectClient()` tests measure.
        let session_closed = connected.session.is_closed();
        if session_closed && !session_closed_reported {
            session_closed_reported = true;
            println!(
                "gate4: the old session closed at {:.1}ms; {}",
                millis(now.duration_since(killed_at)),
                connected.session_event_summary(killed_at)
            );
        }
        let past_grace =
            elected_at.is_some_and(|at| now.saturating_duration_since(at) >= FALLBACK_GRACE);
        if new_leader.is_some() && told.is_none() && (session_closed || past_grace) {
            println!(
                "gate4: no NewLeaderEvent at {:.1}ms; reconnecting a fresh session",
                millis(now.duration_since(killed_at))
            );
            let fresh_channels = Channels::udp_with_egress_offset(ports, members, 1);
            connected = client::connect_with(
                &cluster.client_aeron_dir(),
                &fresh_channels,
                Duration::from_secs(5),
            )
            .map_err(|error| format!("reconnect after the failover: {error}"))?;
            listeners.push(Rc::clone(&connected.arrivals));
            reconnected = true;
            told = Some(Instant::now().duration_since(killed_at));
            println!(
                "gate4: fresh session ready at {:.1}ms",
                millis(told.unwrap_or_default())
            );
            continue;
        }

        let states = cluster.diagnostic();
        if states != last_states || now >= next_trace {
            println!(
                "gate4: states +{:.1}ms {states}",
                millis(now.duration_since(killed_at))
            );
            last_states = states;
            while next_trace <= now {
                next_trace += Duration::from_secs(1);
            }
        }
        if now.duration_since(killed_at) >= Duration::from_secs(7) && !reported_processes {
            reported_processes = true;
            println!("gate4: processes {}", cluster.process_report());
        }
        if now >= deadline {
            return Err(format!(
                "no active leader and client handover in {RECOVERY_TIMEOUT:?}: {last_states}; \
                 processes: {}",
                cluster.process_report()
            ));
        }
        std::thread::sleep(Duration::from_millis(1));
    }

    let new_leader = new_leader.ok_or_else(|| "no new leader".to_owned())?;
    let told = told.ok_or_else(|| "the client was not told".to_owned())?;
    if Instant::now().duration_since(killed_at) > RECOVERY_BUDGET {
        return Err(format!(
            "the client took more than the {}ms budget to reach the new leader \
             (leader at {:.1}ms, told at {:.1}ms)",
            RECOVERY_BUDGET.as_millis(),
            millis(elected),
            millis(told)
        ));
    }

    // ---- the probe: a lower bid on the new leader -------------------------
    // Sent only after the client has a leader — the event, or a fresh session
    // when the event was lost: an offer into the old leader's publication can
    // still answer `Appended::Ok` for a moment, and that bid would be lost.
    let offered_at = connected.bid_retrying(PROBE_CORRELATION, 9_999, 1, RECOVERY_TIMEOUT)?;
    let accepted = offered_at.duration_since(killed_at);
    let probe_egress = connected.wait_for(PROBE_CORRELATION, RECOVERY_TIMEOUT)?;
    let recovered = probe_egress.at.duration_since(killed_at);
    let probe = BidEgress::decode(&probe_egress.payload)
        .ok_or_else(|| "the post-kill egress is not a bid record".to_owned())?;

    println!(
        "gate4: new_leader={new_leader} elected={:.1}ms told={:.1}ms \
         accepted={:.1}ms egress={:.1}ms reconnected={reconnected}",
        millis(elected),
        millis(told),
        millis(accepted),
        millis(recovered)
    );

    if new_leader == leader {
        return Err(format!(
            "member {leader} still leads after its processes were killed"
        ));
    }
    if probe.succeeded {
        return Err(format!(
            "a bid at price 1 won after the kill; the committed state was lost: {probe:?}"
        ));
    }
    if probe.best_price != before.best_price || probe.winner != before.winner {
        return Err(format!(
            "the pre-kill state did not survive the failover: \
             before=(winner={}, best={}), after=(winner={}, best={})",
            before.winner, before.best_price, probe.winner, probe.best_price
        ));
    }
    if accepted > RECOVERY_BUDGET {
        return Err(format!(
            "the new leader accepted a bid after {:.1}ms, over the {}ms budget",
            millis(accepted),
            RECOVERY_BUDGET.as_millis()
        ));
    }
    if recovered > RECOVERY_BUDGET {
        return Err(format!(
            "the client saw the probe's egress after {:.1}ms, over the {}ms budget",
            millis(recovered),
            RECOVERY_BUDGET.as_millis()
        ));
    }

    // ---- after: keep bidding on the new leader ----------------------------
    let mut last = probe;
    for index in 0..bids {
        let correlation =
            POST_CORRELATION + u64::try_from(index).map_err(|_| "bid index".to_owned())?;
        let price = POST_PRICE + i64::try_from(index).map_err(|_| "price".to_owned())?;
        connected.bid_retrying(correlation, correlation, price, EGRESS_TIMEOUT)?;
        let egress = connected.wait_for(correlation, EGRESS_TIMEOUT)?;
        let bid = BidEgress::decode(&egress.payload)
            .ok_or_else(|| format!("egress {correlation} is not a bid record"))?;
        if !bid.succeeded {
            return Err(format!(
                "post-kill bid {correlation} at {price} lost: {bid:?}"
            ));
        }
        last = bid;
    }
    let expected_best = POST_PRICE + i64::try_from(bids).map_err(|_| "bids".to_owned())? - 1;
    let expected_winner =
        i64::try_from(POST_CORRELATION + u64::try_from(bids).map_err(|_| "bids".to_owned())? - 1)
            .map_err(|_| "winner".to_owned())?;
    if last.best_price != expected_best || last.winner != expected_winner {
        return Err(format!(
            "unexpected final state: {last:?}, \
             expected winner={expected_winner} best={expected_best}"
        ));
    }

    // ---- no duplicate fills ------------------------------------------------
    let bid_count = u64::try_from(bids).map_err(|_| "bids".to_owned())?;
    let mut expected: Vec<u64> = (0..bid_count).collect();
    expected.push(PROBE_CORRELATION);
    expected.extend((0..bid_count).map(|index| POST_CORRELATION + index));

    let mut duplicates = Vec::new();
    let mut missing = Vec::new();
    for correlation in &expected {
        let count: usize = listeners
            .iter()
            .map(|arrivals| {
                arrivals
                    .borrow()
                    .counts
                    .get(correlation)
                    .copied()
                    .unwrap_or(0)
            })
            .sum();
        match count {
            1 => {}
            0 => missing.push(*correlation),
            count => duplicates.push((*correlation, count)),
        }
    }
    let messages: usize = listeners
        .iter()
        .map(|arrivals| arrivals.borrow().messages)
        .sum();
    if !duplicates.is_empty() || !missing.is_empty() {
        return Err(format!(
            "egress accounting failed: duplicates={duplicates:?} missing={missing:?} \
             ({messages} messages arrived)"
        ));
    }

    println!(
        "gate4: ok elected={:.1}ms told={:.1}ms accepted={:.1}ms recovered={:.1}ms \
         new_leader={new_leader} bids={} duplicates=0 reconnected={reconnected}",
        millis(elected),
        millis(told),
        millis(accepted),
        millis(recovered),
        expected.len()
    );
    println!("machine: {}", report::machine());
    Ok(())
}
