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
//! The client's ingress and egress are IPC; all three server processes share the
//! same driver.

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::Path;
use std::rc::Rc;
use std::time::{Duration, Instant};

use deepmsg_client::client::Client;
use deepmsg_client::fragment_assembler::MessageHeader;
use deepmsg_cluster_client::{AeronCluster, AeronClusterContext, EgressListener};
use deepmsg_core::logbuffer::append::Appended;

/// The client-facing channels. IPC because every process is on one machine and
/// shares the driver; a multi-host deployment would use the UDP forms.
const INGRESS: &str = "aeron:ipc?term-length=64k";
const EGRESS: &str = "aeron:ipc?term-length=64k";

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
    /// The records, keyed by the correlation id in the first eight bytes.
    pub by_correlation: HashMap<u64, Egress>,
    /// Every message the listener was handed, including duplicates.
    pub messages: usize,
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
        arrivals
            .by_correlation
            .entry(correlation)
            .or_insert(Egress {
                payload: payload.to_vec(),
                at: Instant::now(),
            });
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
        let _ = self.session.poll_egress(&mut self.client);
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
        let mut payload = [0_u8; 24];
        payload[0..8].copy_from_slice(&correlation_id.to_le_bytes());
        payload[8..16].copy_from_slice(&customer_id.to_le_bytes());
        payload[16..24].copy_from_slice(&price.to_le_bytes());

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

/// Connect a `Client` and an `AeronCluster` session, retrying while the module
/// and the container come up.
///
/// # Errors
///
/// The last connect error when the deadline passes.
pub fn connect(aeron_dir: &Path, timeout: Duration) -> Result<Connected, String> {
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
            .ingress_channel(Some(INGRESS))
            .egress_channel(Some(EGRESS))
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
