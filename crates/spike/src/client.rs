//! The Rust cluster client and the `basic-auction` wire records.
//!
//! `basic-auction` is the reference tutorial business that ships in deepmsg's
//! compiled-in registry:
//!
//! | Record | Layout |
//! |---|---|
//! | bid (in) | `correlationId: i64 @0`, `customerId: i64 @8`, `price: i64 @16` — 24 B |
//! | egress (out) | `correlationId @0`, `winningCustomerId @8`, `bestPrice @16`, `bidSucceeded: u8 @24` — 25 B |
//!
//! The single-node checks speak IPC to the one driver every process shares;
//! gate 4 names one UDP ingress endpoint per member and an egress endpoint on
//! the client's own driver (see [`Channels`]).

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::Path;
use std::rc::Rc;
use std::time::{Duration, Instant};

use deepmsg_client::client::Client;
use deepmsg_client::fragment_assembler::MessageHeader;
use deepmsg_cluster_client::{AeronCluster, AeronClusterContext, EgressListener};
use deepmsg_core::logbuffer::append::Appended;

use crate::config::Ports;

/// The client-facing channels. IPC because every process is on one machine and
/// shares the driver; a multi-host deployment would use the UDP forms.
const INGRESS: &str = "aeron:ipc?term-length=64k";
const EGRESS: &str = "aeron:ipc?term-length=64k";

/// The channels a client names for one cluster shape.
///
/// The single-node spike speaks IPC to the one driver every process shares.
/// Gate 4's three members each own a driver, so the client names the UDP
/// ingress template, one endpoint per member, and an egress endpoint of its
/// own; the cluster client then follows a `NewLeaderEvent` from member to
/// member without the caller rebuilding anything.
#[derive(Clone, Debug)]
pub struct Channels {
    /// `aeron.cluster.ingress.channel`; no endpoint for the UDP form.
    pub ingress: String,
    /// `<memberId>=<endpoint>,…` for the UDP form; `None` for IPC.
    pub ingress_endpoints: Option<String>,
    /// `aeron.cluster.egress.channel`; the client's own endpoint for UDP.
    pub egress: String,
    /// `newLeaderTimeoutNs`; `None` leaves it for the connect to compute as
    /// the reference does (`2 * leaderHeartbeatTimeoutNs`).
    pub new_leader_timeout_ns: Option<i64>,
}

impl Channels {
    /// The single-node shape: one driver, IPC both ways.
    #[must_use]
    pub fn ipc() -> Self {
        Self {
            ingress: INGRESS.to_owned(),
            ingress_endpoints: None,
            egress: EGRESS.to_owned(),
            new_leader_timeout_ns: None,
        }
    }

    /// The multi-member shape: a UDP ingress endpoint per member and an egress
    /// endpoint on this host, so a killed member's driver cannot take the
    /// client's egress subscription with it.
    #[must_use]
    pub fn udp(ports: Ports, members: usize) -> Self {
        Self::udp_with_egress_offset(ports, members, 0)
    }

    /// [`Self::udp`] with the egress port moved by `offset`.
    ///
    /// A fresh session after a failover takes an offset of its own. The pinned
    /// deepmsg client cannot refine a wildcard egress endpoint into a unique
    /// one (the reference lets the media driver do that with
    /// `response-correlation-id`), so two sessions that name the same egress
    /// endpoint collide while both are open.
    #[must_use]
    pub fn udp_with_egress_offset(ports: Ports, members: usize, offset: u16) -> Self {
        let endpoints = (0..members)
            .map(|member| format!("{member}=localhost:{}", ports.ingress_of(member)))
            .collect::<Vec<_>>()
            .join(",");
        Self {
            ingress: "aeron:udp?term-length=128k".to_owned(),
            ingress_endpoints: Some(endpoints),
            egress: format!(
                "aeron:udp?term-length=128k|endpoint=localhost:{}",
                ports.client_egress() + offset
            ),
            // The derived default is `2 * heartbeat` = 4 s here, which is the
            // same size as the gate being measured: a client told at 4.1 s
            // would close the session instead of recovering. The gate is the
            // end-to-end number, so the client's patience is two gate budgets
            // (10 s, still half the reference's derived default under its 10 s
            // heartbeat).
            new_leader_timeout_ns: Some(10_000_000_000),
        }
    }
}

/// One egress record and when the listener saw it.
#[derive(Clone, Debug)]
pub struct Egress {
    /// The 25-byte `basic-auction` record.
    pub payload: Vec<u8>,
    /// When `on_message` ran.
    pub at: Instant,
}

/// Everything the egress listener has seen, by bid correlation id.
#[derive(Default)]
pub struct Arrivals {
    /// The first record for each correlation id.
    pub by_correlation: HashMap<u64, Egress>,
    /// How many times each correlation id's record arrived, duplicates
    /// included. Gate 4's "no duplicate fills" is this count, not the map.
    pub counts: HashMap<u64, usize>,
    /// Every message the listener was handed, including duplicates.
    pub messages: usize,
    /// When the listener was first told about a new leader after the caller
    /// last reset it, and which member that is.
    pub new_leader_at: Option<Instant>,
    /// The member the `NewLeaderEvent` named.
    pub new_leader_member: Option<i32>,
    /// The session events the listener saw, for a failure's message.
    pub session_events: Vec<(Instant, String, String)>,
    /// Errors `poll_egress` returned. They are part of a failure's message:
    /// an error while following a new leader is why the session stays put.
    pub poll_errors: Vec<String>,
}

impl Arrivals {
    /// Forget any new-leader event so a caller can time the next one.
    pub fn reset_new_leader(&mut self) {
        self.new_leader_at = None;
        self.new_leader_member = None;
    }
}

struct Listener(Rc<RefCell<Arrivals>>);

impl EgressListener for Listener {
    fn on_message(
        &mut self,
        _cluster_session_id: i64,
        _timestamp: i64,
        payload: &[u8],
        _header: &MessageHeader,
    ) {
        if payload.len() < 8 {
            return;
        }
        let correlation = u64::from_le_bytes(payload[..8].try_into().expect("eight bytes"));
        let mut arrivals = self.0.borrow_mut();
        arrivals.messages += 1;
        *arrivals.counts.entry(correlation).or_insert(0) += 1;
        arrivals
            .by_correlation
            .entry(correlation)
            .or_insert(Egress {
                payload: payload.to_vec(),
                at: Instant::now(),
            });
    }

    fn on_new_leader(
        &mut self,
        _cluster_session_id: i64,
        _leadership_term_id: i64,
        leader_member_id: i32,
        _ingress_endpoints: &str,
    ) {
        let mut arrivals = self.0.borrow_mut();
        if arrivals.new_leader_at.is_none() {
            arrivals.new_leader_at = Some(Instant::now());
            arrivals.new_leader_member = Some(leader_member_id);
        }
    }

    fn on_session_event(
        &mut self,
        _correlation_id: i64,
        _cluster_session_id: i64,
        _leadership_term_id: i64,
        _leader_member_id: i32,
        code: deepmsg_codec::cluster::event_code::EventCode,
        detail: &str,
    ) {
        self.0.borrow_mut().session_events.push((
            Instant::now(),
            format!("{code:?}"),
            detail.to_owned(),
        ));
    }
}

/// A client and its session, plus the listener's shared inbox.
pub struct Connected {
    /// The media client.
    pub client: Client,
    /// The cluster session.
    pub session: AeronCluster,
    /// What the egress listener saw.
    pub arrivals: Rc<RefCell<Arrivals>>,
}

impl Connected {
    /// Drive the client one turn.
    pub fn poll(&mut self) {
        let _ = self.client.poll();
        if let Err(error) = self.session.poll_egress(&mut self.client) {
            let message = error.to_string();
            let mut arrivals = self.arrivals.borrow_mut();
            if !arrivals.poll_errors.iter().any(|seen| seen == &message) {
                arrivals.poll_errors.push(message);
            }
        }
    }

    /// Offer one `basic-auction` bid and answer the instant before the offer
    /// that succeeded.
    ///
    /// A caller that treats any `Some` as success loses one bid per term: the
    /// ingress publication returns `EndOfLog` when it rotates. Only
    /// `Appended::Ok` advances.
    ///
    /// # Errors
    ///
    /// A publication error other than the four retryable ones, or no ingress
    /// publication at all.
    pub fn bid(
        &mut self,
        correlation_id: u64,
        customer_id: u64,
        price: i64,
    ) -> Result<Instant, String> {
        let payload = bid_payload(correlation_id, customer_id, price);

        loop {
            let now = Instant::now();
            match self.session.offer(&mut self.client, &payload) {
                Some(Appended::Ok { .. }) => return Ok(now),
                Some(
                    Appended::EndOfLog
                    | Appended::MidRotation
                    | Appended::BackPressured
                    | Appended::NotConnected,
                ) => self.poll(),
                Some(other) => return Err(format!("the ingress offer failed: {other:?}")),
                None => return Err("the session has no ingress publication".to_owned()),
            }
        }
    }

    /// Offer one bid until the session accepts it, polling through a leader
    /// change.
    ///
    /// Gate 4 needs this: after the leader is killed the session has no
    /// ingress publication for a moment (`offer` answers `None`) and then
    /// points at the new leader. The instant returned is the one before the
    /// `Appended::Ok`, so a caller can time recovery from the kill.
    ///
    /// # Errors
    ///
    /// A publication error other than the retryable ones, or no acceptance
    /// within `timeout`.
    pub fn bid_retrying(
        &mut self,
        correlation_id: u64,
        customer_id: u64,
        price: i64,
        timeout: Duration,
    ) -> Result<Instant, String> {
        let payload = bid_payload(correlation_id, customer_id, price);
        let deadline = Instant::now() + timeout;
        let mut last;

        loop {
            self.poll();
            let now = Instant::now();
            match self.session.offer(&mut self.client, &payload) {
                Some(Appended::Ok { .. }) => return Ok(now),
                Some(
                    Appended::EndOfLog
                    | Appended::MidRotation
                    | Appended::BackPressured
                    | Appended::NotConnected,
                ) => last = "the ingress was not ready".to_owned(),
                Some(other) => return Err(format!("the ingress offer failed: {other:?}")),
                None => last = "the session has no ingress publication".to_owned(),
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "no ingress accepted correlation {correlation_id} in {timeout:?} ({last})"
                ));
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// Forget any new-leader event so the next one can be timed.
    pub fn reset_new_leader(&self) {
        self.arrivals.borrow_mut().reset_new_leader();
    }

    /// Poll until the listener hears a `NewLeaderEvent`, and answer when it
    /// did.
    ///
    /// Gate 4 waits for this before offering the probe: an offer that lands in
    /// the old leader's publication can still answer `Appended::Ok` while the
    /// publication has not yet noticed its receiver is gone, and that bid is
    /// then lost. The event is the client's own word that it has moved.
    ///
    /// # Errors
    ///
    /// The session closing, or no event inside `timeout`.
    pub fn wait_for_new_leader(&mut self, timeout: Duration) -> Result<Instant, String> {
        let started = Instant::now();
        let deadline = started + timeout;
        loop {
            self.poll();
            if let Some(at) = self.arrivals.borrow().new_leader_at {
                return Ok(at);
            }
            if self.session.is_closed() {
                return Err(format!(
                    "the client closed its session after {}ms, before a new leader event: {}",
                    started.elapsed().as_millis(),
                    self.session_event_summary(started)
                ));
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "the client was not told about a new leader in {timeout:?}: {}",
                    self.session_event_summary(started)
                ));
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// The session events and poll errors the client saw, as one line for a
    /// failure message.
    #[must_use]
    pub fn session_event_summary(&self, since: Instant) -> String {
        let arrivals = self.arrivals.borrow();
        let mut parts: Vec<String> = arrivals
            .session_events
            .iter()
            .map(|(at, code, detail)| {
                format!(
                    "+{}ms {code} {detail}",
                    at.saturating_duration_since(since).as_millis()
                )
            })
            .collect();
        if !arrivals.poll_errors.is_empty() {
            parts.push(format!("poll errors: {:?}", arrivals.poll_errors));
        }
        if parts.is_empty() {
            "no session events".to_owned()
        } else {
            parts.join("; ")
        }
    }

    /// Wait until the egress for `correlation_id` has arrived.
    ///
    /// # Errors
    ///
    /// The timeout.
    pub fn wait_for(&mut self, correlation_id: u64, timeout: Duration) -> Result<Egress, String> {
        let deadline = Instant::now() + timeout;
        loop {
            self.poll();
            if let Some(egress) = self
                .arrivals
                .borrow()
                .by_correlation
                .get(&correlation_id)
                .cloned()
            {
                return Ok(egress);
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "no egress for correlation {correlation_id} in {timeout:?}"
                ));
            }
            std::thread::sleep(Duration::from_micros(50));
        }
    }
}

/// The 24-byte `basic-auction` bid record.
fn bid_payload(correlation_id: u64, customer_id: u64, price: i64) -> [u8; 24] {
    let mut payload = [0_u8; 24];
    payload[0..8].copy_from_slice(&correlation_id.to_le_bytes());
    payload[8..16].copy_from_slice(&customer_id.to_le_bytes());
    payload[16..24].copy_from_slice(&price.to_le_bytes());
    payload
}

/// Connect a `Client` and an `AeronCluster` session over IPC, retrying while
/// the module and the container come up.
///
/// # Errors
///
/// The last connect error when the deadline passes.
pub fn connect(aeron_dir: &Path, timeout: Duration) -> Result<Connected, String> {
    connect_with(aeron_dir, &Channels::ipc(), timeout)
}

/// [`connect`] over the channels the caller names.
///
/// The context's `new_leader_timeout_ns` is deliberately left unset: the
/// client computes `2 * leaderHeartbeatTimeoutNs` from the `NewLeaderEvent`,
/// which is the reference's rule (`AeronCluster.java:2574-2578`).
///
/// # Errors
///
/// The last connect error when the deadline passes.
pub fn connect_with(
    aeron_dir: &Path,
    channels: &Channels,
    timeout: Duration,
) -> Result<Connected, String> {
    let arrivals = Rc::new(RefCell::new(Arrivals::default()));
    let deadline = Instant::now() + timeout;
    #[allow(unused_assignments)] // the first value is only a definite-assignment seed
    let mut last = String::new();

    loop {
        let mut client = match Client::connect(aeron_dir) {
            Ok(client) => client,
            Err(error) => {
                last = format!("media client: {error}");
                if Instant::now() >= deadline {
                    return Err(last);
                }
                std::thread::sleep(Duration::from_millis(100));
                continue;
            }
        };

        let context = AeronClusterContext::default()
            .ingress_channel(Some(&channels.ingress))
            .ingress_endpoints(channels.ingress_endpoints.as_deref())
            .egress_channel(Some(&channels.egress))
            .new_leader_timeout_ns(channels.new_leader_timeout_ns)
            .client_name("deepbook-spike")
            .message_timeout_ns(5_000_000_000)
            .egress_listener(Box::new(Listener(Rc::clone(&arrivals))));

        match AeronCluster::connect(context, &mut client) {
            Ok(session) => {
                return Ok(Connected {
                    client,
                    session,
                    arrivals,
                });
            }
            Err(error) => last = format!("cluster client: {error}"),
        }

        if Instant::now() >= deadline {
            return Err(last);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// The `basic-auction` egress record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BidEgress {
    /// The bid's correlation id.
    pub correlation_id: i64,
    /// The customer with the best price after this bid.
    pub winner: i64,
    /// The best price after this bid.
    pub best_price: i64,
    /// Whether this bid beat the previous best.
    pub succeeded: bool,
}

impl BidEgress {
    /// Decode a 25-byte egress record.
    #[must_use]
    pub fn decode(payload: &[u8]) -> Option<Self> {
        if payload.len() < 25 {
            return None;
        }
        Some(Self {
            correlation_id: i64::from_le_bytes(payload[0..8].try_into().ok()?),
            winner: i64::from_le_bytes(payload[8..16].try_into().ok()?),
            best_price: i64::from_le_bytes(payload[16..24].try_into().ok()?),
            succeeded: payload[24] != 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_bid_record_is_three_little_endian_i64s() {
        let mut payload = [0_u8; 24];
        payload[0..8].copy_from_slice(&7_i64.to_le_bytes());
        payload[8..16].copy_from_slice(&42_i64.to_le_bytes());
        payload[16..24].copy_from_slice(&101_i64.to_le_bytes());
        assert_eq!(7, i64::from_le_bytes(payload[0..8].try_into().expect("8")));
        assert_eq!(
            42,
            i64::from_le_bytes(payload[8..16].try_into().expect("8"))
        );
        assert_eq!(
            101,
            i64::from_le_bytes(payload[16..24].try_into().expect("8"))
        );
    }

    #[test]
    fn the_auction_egress_decodes() {
        let mut payload = [0_u8; 25];
        payload[0..8].copy_from_slice(&9_i64.to_le_bytes());
        payload[8..16].copy_from_slice(&3_i64.to_le_bytes());
        payload[16..24].copy_from_slice(&100_i64.to_le_bytes());
        payload[24] = 1;
        let egress = BidEgress::decode(&payload).expect("decodes");
        assert_eq!(
            BidEgress {
                correlation_id: 9,
                winner: 3,
                best_price: 100,
                succeeded: true
            },
            egress
        );
    }
}
