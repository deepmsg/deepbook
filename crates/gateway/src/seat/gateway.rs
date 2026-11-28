//! The DeepOUCH seat host (ADR 0012 §6.2–§6.4).
//!
//! One thread owns every OUCH connection, the cluster client and the event
//! loop. The connection state machine is:
//!
//! ```text
//! AwaitingLogin -> Binding -> CatchingUp -> Live -> Closing
//! ```
//!
//! `Binding` has sent `BindSeat`; `CatchingUp` journals the cluster's replay
//! without writing it to the socket; `Live` sends frames and replays the
//! journal at login time. The cluster never blocks on one slow client: the
//! event loop keeps draining egress and a seat over its outbound water mark is
//! disconnected and unbound.

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use deepbook_engine_cluster::envelope::{
    BindRejected, FenceEvicted, unbind_reason as unbind_reasons,
};
use deepbook_engine_core::{
    CancelOrder, Command, NewOrder, OrdType, ReplaceOrder, TimeInForce, encode_command,
};
use deepbook_io::{BackendKind, Event, EventLoop};
use deepbook_ouch::ids::to_client_order_id;
use deepbook_ouch::wire::{
    self, FrameError, Message, login_rejected, logout, ord_type as wire_ord_type,
    side as wire_side, tif as wire_tif,
};
use deepbook_types::Side;

use crate::cluster::{ClusterEngineConfig, SeatClusterEngine, SeatEgress};
use crate::metrics::GatewayMetrics;
use crate::seat::config::{SeatProtocol, SeatRegistry};
use crate::seat::journal::latest;
use crate::seat::session::{EventOutcome, SeatSession};

/// One seat's outbound socket high-water mark.
pub const MAX_OUTBOUND_BYTES: usize = 16 << 20;
/// Unauthenticated connections kept at once.
const MAX_UNAUTHENTICATED: usize = 256;
/// How long a connection may sit without a `LoginRequest`.
const LOGON_TIMEOUT: Duration = Duration::from_secs(3);
/// Server heartbeat cadence.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);
/// Peer silence that ends the session.
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(3);
/// Malformed-frame budget capacity and refill rate.
const MALFORMED_BURST: u32 = 8;
const MALFORMED_REFILL: u32 = 2;

/// How the OUCH host is configured.
#[derive(Clone, Debug)]
pub struct SeatGatewayConfig {
    /// The address to listen on.
    pub bind: SocketAddr,
    /// The shared seat registry.
    pub registry: SeatRegistry,
    /// The cluster endpoints.
    pub cluster: ClusterEngineConfig,
    /// The event-loop tick and heartbeat granularity.
    pub tick: Duration,
    /// The I/O backend.
    pub backend: BackendKind,
    /// A seat's outbound high-water mark.
    pub max_outbound_bytes: usize,
}

impl SeatGatewayConfig {
    /// The design defaults.
    #[must_use]
    pub fn new(bind: SocketAddr, registry: SeatRegistry, cluster: ClusterEngineConfig) -> Self {
        Self {
            bind,
            registry,
            cluster,
            tick: Duration::from_millis(100),
            backend: BackendKind::Epoll,
            max_outbound_bytes: MAX_OUTBOUND_BYTES,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ConnState {
    AwaitingLogin,
    Binding,
    CatchingUp,
    Live,
    Closing,
}

struct Connection {
    _peer: SocketAddr,
    buffer: Vec<u8>,
    state: ConnState,
    seat_id: Option<u16>,
    requested_seq: u64,
    session: Option<SeatSession>,
    outbound: Outbound,
    writing: bool,
    close_after_flush: bool,
    /// Whether `LoginAccepted` has been sent; a cluster reconnect must not
    /// repeat the login handshake on a live client.
    accepted: bool,
    /// The reason to give the service when this connection closes.
    unbind_reason: u8,
    accepted_at: Instant,
    last_inbound: Instant,
    last_outbound: Instant,
    catchup_target: u64,
}

impl Connection {
    fn new(peer: SocketAddr, now: Instant) -> Self {
        Self {
            _peer: peer,
            buffer: Vec::new(),
            state: ConnState::AwaitingLogin,
            seat_id: None,
            requested_seq: 0,
            session: None,
            outbound: Outbound::default(),
            writing: false,
            close_after_flush: false,
            accepted: false,
            unbind_reason: unbind_reasons::CLIENT_CLOSED,
            accepted_at: now,
            last_inbound: now,
            last_outbound: now,
            catchup_target: 0,
        }
    }

    fn push(&mut self, message: &Message) -> Result<(), FrameError> {
        let frame = wire::encode(message)?;
        self.outbound.push(&frame);
        self.last_outbound = Instant::now();
        Ok(())
    }
}

#[derive(Debug, Default)]
struct Outbound {
    bytes: Vec<u8>,
    start: usize,
}

impl Outbound {
    fn push(&mut self, frame: &[u8]) {
        self.bytes.extend_from_slice(frame);
    }

    fn pending(&self) -> usize {
        self.bytes.len().saturating_sub(self.start)
    }

    fn next_chunk(&self) -> &[u8] {
        &self.bytes[self.start..]
    }

    fn written(&mut self, count: usize) {
        self.start = self.start.saturating_add(count);
        if self.start >= self.bytes.len() {
            self.bytes.clear();
            self.start = 0;
        }
    }
}

struct MalformedBudget {
    tokens: u32,
    last: Instant,
}

impl MalformedBudget {
    fn new(now: Instant) -> Self {
        Self {
            tokens: MALFORMED_BURST,
            last: now,
        }
    }

    fn allow(&mut self, now: Instant) -> bool {
        let elapsed = now.saturating_duration_since(self.last);
        let whole_seconds = elapsed.as_secs();
        if whole_seconds > 0 {
            let refill = u32::try_from(whole_seconds)
                .unwrap_or(u32::MAX)
                .saturating_mul(MALFORMED_REFILL);
            self.tokens = self.tokens.saturating_add(refill).min(MALFORMED_BURST);
            self.last += Duration::from_secs(whole_seconds);
        }
        if self.tokens > 0 {
            self.tokens -= 1;
            true
        } else {
            false
        }
    }
}

/// The multi-seat DeepOUCH host.
pub struct SeatGateway {
    config: SeatGatewayConfig,
    event_loop: Box<dyn EventLoop>,
    cluster: SeatClusterEngine,
    connections: HashMap<u64, Connection>,
    owners: HashMap<u16, u64>,
    malformed: HashMap<u16, MalformedBudget>,
    /// The previous tick's cluster connectivity, for the loss transition.
    cluster_connected: bool,
}

impl fmt::Debug for SeatGateway {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SeatGateway")
            .field("bind", &self.config.bind)
            .field("connections", &self.connections.len())
            .field("seats", &self.owners.len())
            .finish_non_exhaustive()
    }
}

impl SeatGateway {
    /// Bind the listener and connect the cluster.
    ///
    /// # Errors
    ///
    /// The address cannot be bound, the backend is unavailable, or the cluster
    /// cannot be reached.
    pub fn new(config: SeatGatewayConfig, metrics: Arc<GatewayMetrics>) -> io::Result<Self> {
        let event_loop = deepbook_io::open(config.backend, config.bind)?;
        let cluster = SeatClusterEngine::connect_with(config.cluster.clone(), metrics)
            .map_err(|error| io::Error::other(error.to_string()))?;
        Ok(Self {
            config,
            event_loop,
            cluster,
            connections: HashMap::new(),
            owners: HashMap::new(),
            malformed: HashMap::new(),
            cluster_connected: true,
        })
    }

    /// The bound address.
    ///
    /// # Errors
    ///
    /// The listener has no address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.event_loop.local_addr()
    }

    /// Run until the process stops.
    ///
    /// # Errors
    ///
    /// The poller fails.
    pub fn run(&mut self) -> io::Result<()> {
        self.event_loop.start_accept()?;
        let mut events = Vec::new();
        loop {
            events.clear();
            self.event_loop.wait(self.config.tick, &mut events)?;
            for event in events.drain(..) {
                self.on_io_event(event);
            }
            self.drain_cluster();
            self.tick();
            self.flush_all();
        }
    }

    /// One accept/read/write/tick turn; useful for tests and embedders.
    ///
    /// # Errors
    ///
    /// The poller fails.
    pub fn poll_once(&mut self) -> io::Result<()> {
        let mut events = Vec::new();
        self.event_loop.wait(self.config.tick, &mut events)?;
        for event in events.drain(..) {
            self.on_io_event(event);
        }
        self.drain_cluster();
        self.tick();
        self.flush_all();
        Ok(())
    }

    fn on_io_event(&mut self, event: Event) {
        match event {
            Event::Accepted { id, peer } => {
                let now = Instant::now();
                if self.connections.len() >= MAX_UNAUTHENTICATED {
                    eprintln!(
                        "ouch-gateway: refusing {peer}: too many unauthenticated connections"
                    );
                    self.event_loop.close(id);
                    let _ = self.event_loop.start_accept();
                    return;
                }
                self.connections.insert(id, Connection::new(peer, now));
                if let Err(error) = self.event_loop.start_read(id) {
                    eprintln!("ouch-gateway: read arm for {peer}: {error}");
                    self.close(id, unbind_reasons::CLIENT_CLOSED);
                }
                let _ = self.event_loop.start_accept();
            }
            Event::AcceptFailed(error) => {
                eprintln!("ouch-gateway: accept: {error}");
                let _ = self.event_loop.start_accept();
            }
            Event::Read { id, result } => match result {
                Ok(0) => self.close(id, unbind_reasons::CLIENT_CLOSED),
                Ok(count) => {
                    let bytes = self
                        .event_loop
                        .read_data(id)
                        .get(..count)
                        .unwrap_or(&[])
                        .to_vec();
                    if let Some(connection) = self.connections.get_mut(&id) {
                        connection.last_inbound = Instant::now();
                        connection.buffer.extend_from_slice(&bytes);
                    }
                    self.process_frames(id);
                    if self.connections.contains_key(&id) {
                        if let Err(error) = self.event_loop.start_read(id) {
                            eprintln!("ouch-gateway: read arm: {error}");
                            self.close(id, unbind_reasons::CLIENT_CLOSED);
                        }
                    }
                }
                Err(error) => {
                    eprintln!("ouch-gateway: read: {error}");
                    self.close(id, unbind_reasons::CLIENT_CLOSED);
                }
            },
            Event::Written { id, result } => match result {
                Ok(count) => {
                    if let Some(connection) = self.connections.get_mut(&id) {
                        connection.outbound.written(count);
                        connection.writing = false;
                    }
                    self.flush(id);
                }
                Err(error) => {
                    eprintln!("ouch-gateway: write: {error}");
                    self.close(id, unbind_reasons::CLIENT_CLOSED);
                }
            },
        }
    }

    fn process_frames(&mut self, id: u64) {
        loop {
            let (frame, closed) = {
                let Some(connection) = self.connections.get_mut(&id) else {
                    return;
                };
                if connection.buffer.len() < wire::HEADER_LEN {
                    break;
                }
                let total = match wire::frame_len(&connection.buffer) {
                    Ok(total) => total,
                    Err(error) => {
                        self.protocol_violation(id, error);
                        return;
                    }
                };
                if connection.buffer.len() < total {
                    break;
                }
                let frame: Vec<u8> = connection.buffer.drain(..total).collect();
                (frame, false)
            };
            let _ = closed;
            if !self.handle_frame(id, &frame) {
                break;
            }
        }
    }

    /// Handle one complete frame; `false` means the connection is gone.
    fn handle_frame(&mut self, id: u64, frame: &[u8]) -> bool {
        let message = match wire::decode_exact(frame) {
            Ok(message) => message,
            Err(error) => {
                self.protocol_violation(id, error);
                return false;
            }
        };
        let state = self
            .connections
            .get(&id)
            .map_or(ConnState::Closing, |c| c.state);
        match state {
            ConnState::AwaitingLogin => match message {
                Message::LoginRequest {
                    seat_id,
                    token,
                    requested_seq,
                } => {
                    self.handle_login(id, seat_id, token, requested_seq);
                    true
                }
                Message::ClientHeartbeat => true,
                _ => {
                    self.protocol_violation_id(id, "login frame expected");
                    false
                }
            },
            ConnState::Binding | ConnState::CatchingUp => {
                if message == Message::ClientHeartbeat {
                    true
                } else {
                    self.protocol_violation_id(id, "business frame before SeatBound");
                    false
                }
            }
            ConnState::Live => match message {
                Message::ClientHeartbeat => true,
                Message::Logout { .. } => {
                    self.close(id, unbind_reasons::CLIENT_CLOSED);
                    false
                }
                Message::EnterOrder { .. }
                | Message::CancelOrder { .. }
                | Message::ReplaceOrder { .. } => {
                    self.handle_order(id, message);
                    self.connections.contains_key(&id)
                }
                _ => {
                    self.protocol_violation_id(id, "unexpected client frame");
                    false
                }
            },
            ConnState::Closing => false,
        }
    }

    fn handle_login(&mut self, id: u64, seat_id: u16, token: [u8; 32], requested_seq: u64) {
        let Some(seat) = self.config.registry.get(seat_id).cloned() else {
            self.login_rejected(id, login_rejected::UNKNOWN_SEAT);
            return;
        };
        if seat.protocol != SeatProtocol::Ouch {
            self.login_rejected(id, login_rejected::UNKNOWN_SEAT);
            return;
        }
        let Some(expected) = seat.token else {
            self.login_rejected(id, login_rejected::UNKNOWN_SEAT);
            return;
        };
        if !constant_time_equal(&token, &expected) {
            self.login_rejected(id, login_rejected::INVALID_TOKEN);
            return;
        }
        if self.owners.contains_key(&seat_id) {
            self.login_rejected(id, login_rejected::SEAT_IN_USE);
            return;
        }
        if !self.cluster.is_connected() {
            self.login_rejected(id, login_rejected::CLUSTER_DOWN);
            return;
        }
        let last = latest(&seat.journal_dir)
            .ok()
            .flatten()
            .map_or(0, |(_, last)| last);
        let session = SeatSession::new(seat.clone());
        if let Some(connection) = self.connections.get_mut(&id) {
            connection.seat_id = Some(seat_id);
            connection.requested_seq = requested_seq;
            connection.session = Some(session);
            connection.state = ConnState::Binding;
        }
        self.owners.insert(seat_id, id);
        eprintln!("ouch-gateway: login seat {seat_id} connection {id}");
        self.cluster.bind(seat_id, seat.account_id, last);
    }

    fn login_rejected(&mut self, id: u64, reason: u8) {
        if let Some(connection) = self.connections.get_mut(&id) {
            let _ = connection.push(&Message::LoginRejected { reason });
            connection.close_after_flush = true;
        }
        self.flush(id);
        self.close_if_flushed(id);
    }

    fn handle_order(&mut self, id: u64, message: Message) {
        let Some(seat_id) = self.connections.get(&id).and_then(|c| c.seat_id) else {
            return;
        };
        let Some(seat) = self.config.registry.get(seat_id) else {
            return;
        };
        let account = seat.account_id;
        let epoch = self
            .connections
            .get(&id)
            .and_then(|c| c.session.as_ref().map(SeatSession::epoch))
            .unwrap_or(0);
        match message {
            Message::EnterOrder {
                cl_ord_id,
                price,
                qty,
                symbol,
                side,
                ord_type,
                tif,
                flags,
            } => {
                if flags & 0xFE != 0 {
                    self.protocol_violation_id(id, "reserved order flags");
                    return;
                }
                let Some(cl) = to_client_order_id(cl_ord_id) else {
                    self.malformed(id, seat_id, epoch, cl_ord_id, 0, false, 101);
                    return;
                };
                let Some(side) = side_from_wire(side) else {
                    self.malformed(id, seat_id, epoch, cl_ord_id, 0, false, 104);
                    return;
                };
                let Some(ord_type) = ord_type_from_wire(ord_type) else {
                    self.malformed(id, seat_id, epoch, cl_ord_id, 0, false, 104);
                    return;
                };
                let Some(tif) = tif_from_wire(tif) else {
                    self.malformed(id, seat_id, epoch, cl_ord_id, 0, false, 104);
                    return;
                };
                let Some(qty) = qty_from_wire(qty) else {
                    self.malformed(id, seat_id, epoch, cl_ord_id, 0, false, 102);
                    return;
                };
                let price = match (ord_type, price) {
                    (OrdType::Limit, price) if price > 0 => Some(price),
                    (OrdType::Market, 0) => None,
                    _ => {
                        self.malformed(id, seat_id, epoch, cl_ord_id, 0, false, 103);
                        return;
                    }
                };
                let command = Command::NewOrder(NewOrder {
                    symbol,
                    account,
                    cl_ord_id: cl,
                    side,
                    price,
                    qty,
                    ord_type,
                    time_in_force: tif,
                    post_only: flags & 0x01 != 0,
                });
                self.send_command(id, seat_id, epoch, &command);
            }
            Message::CancelOrder {
                cl_ord_id,
                orig_cl_ord_id,
                ..
            } => {
                let Some(cl) = to_client_order_id(cl_ord_id) else {
                    self.malformed(id, seat_id, epoch, cl_ord_id, orig_cl_ord_id, true, 101);
                    return;
                };
                let Some(orig) = to_client_order_id(orig_cl_ord_id) else {
                    self.malformed(id, seat_id, epoch, cl_ord_id, orig_cl_ord_id, true, 101);
                    return;
                };
                let command = Command::CancelOrder(CancelOrder {
                    account,
                    cl_ord_id: cl,
                    orig_cl_ord_id: orig,
                });
                self.send_command(id, seat_id, epoch, &command);
            }
            Message::ReplaceOrder {
                cl_ord_id,
                orig_cl_ord_id,
                new_price,
                new_qty,
                symbol,
                ord_type,
                tif,
                flags,
            } => {
                if flags & 0xFE != 0 {
                    self.protocol_violation_id(id, "reserved replace flags");
                    return;
                }
                let Some(cl) = to_client_order_id(cl_ord_id) else {
                    self.malformed(id, seat_id, epoch, cl_ord_id, orig_cl_ord_id, true, 101);
                    return;
                };
                let Some(orig) = to_client_order_id(orig_cl_ord_id) else {
                    self.malformed(id, seat_id, epoch, cl_ord_id, orig_cl_ord_id, true, 101);
                    return;
                };
                let Some(ord_type) = ord_type_from_wire(ord_type) else {
                    self.malformed(id, seat_id, epoch, cl_ord_id, orig_cl_ord_id, true, 104);
                    return;
                };
                let Some(tif) = tif_from_wire(tif) else {
                    self.malformed(id, seat_id, epoch, cl_ord_id, orig_cl_ord_id, true, 104);
                    return;
                };
                let Some(qty) = qty_from_wire(new_qty) else {
                    self.malformed(id, seat_id, epoch, cl_ord_id, orig_cl_ord_id, true, 102);
                    return;
                };
                let price = match (ord_type, new_price) {
                    (OrdType::Limit, price) if price > 0 => Some(price),
                    (OrdType::Market, 0) => None,
                    _ => {
                        self.malformed(id, seat_id, epoch, cl_ord_id, orig_cl_ord_id, true, 103);
                        return;
                    }
                };
                let _ = symbol;
                let command = Command::ReplaceOrder(ReplaceOrder {
                    account,
                    cl_ord_id: cl,
                    orig_cl_ord_id: orig,
                    price,
                    qty,
                    ord_type,
                    time_in_force: tif,
                    post_only: flags & 0x01 != 0,
                });
                self.send_command(id, seat_id, epoch, &command);
            }
            _ => {}
        }
    }

    fn send_command(&mut self, id: u64, seat_id: u16, epoch: u64, command: &Command) {
        let bytes = encode_command(command);
        self.cluster.seat_command(seat_id, epoch, &bytes);
        if let Some(connection) = self.connections.get_mut(&id) {
            connection.last_outbound = Instant::now();
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn malformed(
        &mut self,
        id: u64,
        seat_id: u16,
        epoch: u64,
        raw_cl: u64,
        raw_orig: u64,
        is_cancel: bool,
        reason: u8,
    ) {
        let now = Instant::now();
        let budget = self
            .malformed
            .entry(seat_id)
            .or_insert_with(|| MalformedBudget::new(now));
        if !budget.allow(now) {
            self.protocol_violation_id(id, "malformed budget exhausted");
            return;
        }
        self.cluster
            .reject_malformed(seat_id, epoch, raw_cl, raw_orig, is_cancel, reason);
    }

    fn protocol_violation(&mut self, id: u64, error: FrameError) {
        self.protocol_violation_id(id, &error.to_string());
    }

    fn protocol_violation_id(&mut self, id: u64, detail: &str) {
        eprintln!("ouch-gateway: protocol violation on {id}: {detail}");
        if let Some(connection) = self.connections.get_mut(&id) {
            let _ = connection.push(&Message::Logout {
                reason: logout::PROTOCOL_VIOLATION,
            });
            connection.close_after_flush = true;
        }
        self.flush(id);
        self.close_if_flushed(id);
    }

    fn drain_cluster(&mut self) {
        while let Some(egress) = self.cluster.poll_egress() {
            match egress {
                SeatEgress::Bound(bound) => self.on_bound(bound),
                SeatEgress::Rejected(rejected) => self.on_bind_rejected(rejected),
                SeatEgress::Event(event) => self.on_seat_event(event),
                SeatEgress::Fenced(fenced) => self.on_fenced(fenced),
            }
        }
    }

    fn on_bound(&mut self, bound: deepbook_engine_cluster::envelope::SeatBound) {
        let Some(id) = self.owners.get(&bound.seat_id).copied() else {
            // The client went away between BindSeat and SeatBound; release the
            // binding so the outbox does not grow for a dead peer.
            self.cluster
                .unbind_seat(bound.seat_id, bound.epoch, unbind_reasons::CLIENT_CLOSED);
            return;
        };
        let ready = {
            let Some(connection) = self.connections.get_mut(&id) else {
                return;
            };
            let Some(session) = connection.session.as_mut() else {
                return;
            };
            match session.on_seat_bound(bound) {
                Ok(outcome) => {
                    connection.catchup_target = outcome.next_seq;
                    connection.state = ConnState::CatchingUp;
                    session.last_applied_seq().saturating_add(1) >= outcome.next_seq
                }
                Err(error) => {
                    eprintln!("ouch-gateway: seat {} bound: {error}", bound.seat_id);
                    self.login_rejected(id, login_rejected::SEQ_OUT_OF_RANGE);
                    return;
                }
            }
        };
        if ready {
            self.finish_catchup(id);
        }
    }

    fn on_bind_rejected(&mut self, rejected: BindRejected) {
        let Some(id) = self.owners.get(&rejected.seat_id).copied() else {
            return;
        };
        let reason = match rejected.reason {
            1 => login_rejected::ACCOUNT_CONFLICT,
            2 | 3 => login_rejected::UNKNOWN_SEAT,
            _ => login_rejected::UNKNOWN_SEAT,
        };
        self.login_rejected(id, reason);
    }

    fn on_seat_event(&mut self, event: crate::cluster::OwnedSeatEvent) {
        let Some(id) = self.owners.get(&event.seat_id).copied() else {
            return;
        };
        let (ready, failed) = {
            let Some(connection) = self.connections.get_mut(&id) else {
                return;
            };
            let Some(session) = connection.session.as_mut() else {
                return;
            };
            match session.on_event(&event.borrowed()) {
                Ok(EventOutcome::Duplicate) => (false, false),
                Ok(EventOutcome::Frame { frame, .. }) => {
                    if connection.state == ConnState::Live
                        || (connection.state == ConnState::CatchingUp && connection.accepted)
                    {
                        connection.outbound.push(&frame);
                        connection.last_outbound = Instant::now();
                    }
                    let ready = connection.state == ConnState::CatchingUp
                        && session.last_applied_seq().saturating_add(1)
                            >= connection.catchup_target;
                    (ready, false)
                }
                Err(error) => {
                    eprintln!("ouch-gateway: seat {} event: {error}", event.seat_id);
                    (false, true)
                }
            }
        };
        if failed {
            self.logout_with(id, logout::RESYNC_REQUIRED, unbind_reasons::CLIENT_CLOSED);
            return;
        }
        if ready {
            self.finish_catchup(id);
        }
    }

    fn on_fenced(&mut self, fenced: FenceEvicted) {
        let Some(id) = self.owners.remove(&fenced.seat_id) else {
            return;
        };
        if let Some(connection) = self.connections.get_mut(&id) {
            if let Some(session) = connection.session.as_mut() {
                session.mark_unbound();
            }
            let _ = connection.push(&Message::Logout {
                reason: logout::FENCED,
            });
            connection.close_after_flush = true;
        }
        self.flush(id);
        self.close_if_flushed(id);
    }

    fn finish_catchup(&mut self, id: u64) {
        let (seat_id, generation, requested_seq, already_accepted) = {
            let Some(connection) = self.connections.get(&id) else {
                return;
            };
            let Some(session) = connection.session.as_ref() else {
                return;
            };
            (
                connection.seat_id.unwrap_or(0),
                session.generation(),
                connection.requested_seq,
                connection.accepted,
            )
        };
        if already_accepted {
            // A cluster reconnect: the client already has its login; only the
            // journal caught up. Resume live without a second LoginAccepted.
            eprintln!("ouch-gateway: seat {seat_id} caught up (already accepted)");
            if let Some(connection) = self.connections.get_mut(&id) {
                connection.state = ConnState::Live;
                connection.last_inbound = Instant::now();
                connection.last_outbound = Instant::now();
            }
            return;
        }
        let login = {
            let Some(session) = self
                .connections
                .get(&id)
                .and_then(|connection| connection.session.as_ref())
            else {
                return;
            };
            session.login(requested_seq)
        };
        match login {
            Ok(outcome) => {
                if let Some(connection) = self.connections.get_mut(&id) {
                    let accepted = Message::LoginAccepted {
                        seat_id,
                        generation,
                        next_seq: outcome.first_seq,
                    };
                    let _ = connection.push(&accepted);
                    if !outcome.replay.is_empty() {
                        connection.outbound.push(&outcome.replay);
                    }
                    connection.state = ConnState::Live;
                    connection.accepted = true;
                    connection.last_inbound = Instant::now();
                    connection.last_outbound = Instant::now();
                    eprintln!(
                        "ouch-gateway: seat {seat_id} live at seq {}",
                        outcome.first_seq
                    );
                }
            }
            Err(error) => {
                eprintln!("ouch-gateway: seat {seat_id} login: {error}");
                self.login_rejected(id, login_rejected::SEQ_OUT_OF_RANGE);
            }
        }
    }

    fn tick(&mut self) {
        let now = Instant::now();
        let connected = self.cluster.is_connected();
        if self.cluster_connected && !connected {
            eprintln!("ouch-gateway: cluster lost; closing live seats");
            for connection in self.connections.values_mut() {
                if connection.session.is_some() && !connection.close_after_flush {
                    let _ = connection.push(&Message::Logout {
                        reason: logout::SERVER_SHUTDOWN,
                    });
                    connection.close_after_flush = true;
                    connection.unbind_reason = unbind_reasons::CLIENT_CLOSED;
                }
            }
        }
        self.cluster_connected = connected;
        let mut closes = Vec::new();
        let mut acks = Vec::new();
        for (&id, connection) in &mut self.connections {
            match connection.state {
                ConnState::AwaitingLogin => {
                    if now.saturating_duration_since(connection.accepted_at) >= LOGON_TIMEOUT {
                        closes.push((id, unbind_reasons::CLIENT_CLOSED));
                    }
                }
                ConnState::Live => {
                    if now.saturating_duration_since(connection.last_inbound) >= HEARTBEAT_TIMEOUT {
                        closes.push((id, unbind_reasons::CLIENT_CLOSED));
                        continue;
                    }
                    if now.saturating_duration_since(connection.last_outbound) >= HEARTBEAT_INTERVAL
                        && connection.outbound.pending() == 0
                    {
                        let _ = connection.push(&Message::ServerHeartbeat);
                    }
                }
                ConnState::Binding | ConnState::CatchingUp | ConnState::Closing => {}
            }
            if let Some(session) = connection.session.as_mut() {
                if let Ok(Some(ack)) = session.ack_due() {
                    acks.push(ack);
                }
            }
            if connection.state == ConnState::Live
                && connection.outbound.pending() > self.config.max_outbound_bytes
                && !connection.close_after_flush
            {
                connection.unbind_reason = unbind_reasons::SLOW_CONSUMER;
                let _ = connection.push(&Message::Logout {
                    reason: logout::SLOW_CONSUMER,
                });
                connection.close_after_flush = true;
            }
        }
        for ack in acks {
            self.cluster.ack(ack.seat_id, ack.epoch, ack.up_to_seq);
        }
        for (id, reason) in closes {
            self.close(id, reason);
        }
    }

    fn flush_all(&mut self) {
        let ids: Vec<u64> = self.connections.keys().copied().collect();
        for id in ids {
            self.flush(id);
            self.close_if_flushed(id);
        }
    }

    fn flush(&mut self, id: u64) {
        let Some(connection) = self.connections.get_mut(&id) else {
            return;
        };
        if connection.writing || connection.outbound.pending() == 0 {
            return;
        }
        let chunk = connection.outbound.next_chunk().to_vec();
        match self.event_loop.submit_write(id, &chunk) {
            Ok(()) => connection.writing = true,
            Err(error) => {
                eprintln!("ouch-gateway: write arm: {error}");
                self.close(id, unbind_reasons::CLIENT_CLOSED);
            }
        }
    }

    fn close_if_flushed(&mut self, id: u64) {
        let reason = self.connections.get(&id).and_then(|connection| {
            (connection.close_after_flush && connection.outbound.pending() == 0)
                .then_some(connection.unbind_reason)
        });
        if let Some(reason) = reason {
            self.close(id, reason);
        }
    }

    fn logout_with(&mut self, id: u64, reason: u8, unbind: u8) {
        if let Some(connection) = self.connections.get_mut(&id) {
            let _ = connection.push(&Message::Logout { reason });
            connection.close_after_flush = true;
            connection.unbind_reason = unbind;
        }
        self.flush(id);
        self.close_if_flushed(id);
    }

    fn close(&mut self, id: u64, unbind_reason: u8) {
        let Some(connection) = self.connections.remove(&id) else {
            return;
        };
        eprintln!(
            "ouch-gateway: close connection {id} seat={:?} state={:?} reason={unbind_reason}",
            connection.seat_id, connection.state
        );
        if let Some(seat_id) = connection.seat_id {
            if self.owners.get(&seat_id) == Some(&id) {
                self.owners.remove(&seat_id);
                if let Some(session) = connection.session.as_ref() {
                    if session.is_bound() {
                        self.cluster
                            .unbind_seat(seat_id, session.epoch(), unbind_reason);
                    }
                }
            }
        }
        self.event_loop.close(id);
    }
}

fn constant_time_equal(left: &[u8; 32], right: &[u8; 32]) -> bool {
    let mut difference = 0_u8;
    for (left, right) in left.iter().zip(right.iter()) {
        difference |= left ^ right;
    }
    difference == 0
}

fn side_from_wire(value: u8) -> Option<Side> {
    match value {
        wire_side::BUY => Some(Side::Buy),
        wire_side::SELL => Some(Side::Sell),
        _ => None,
    }
}

fn ord_type_from_wire(value: u8) -> Option<OrdType> {
    match value {
        wire_ord_type::LIMIT => Some(OrdType::Limit),
        wire_ord_type::MARKET => Some(OrdType::Market),
        _ => None,
    }
}

fn tif_from_wire(value: u8) -> Option<TimeInForce> {
    match value {
        wire_tif::DAY => Some(TimeInForce::Day),
        wire_tif::GTC => Some(TimeInForce::GoodTillCanceled),
        wire_tif::IOC => Some(TimeInForce::ImmediateOrCancel),
        wire_tif::FOK => Some(TimeInForce::FillOrKill),
        _ => None,
    }
}

fn qty_from_wire(value: u64) -> Option<i64> {
    if value == 0 {
        return None;
    }
    i64::try_from(value).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_compare_in_constant_time() {
        let token = [7_u8; 32];
        assert!(constant_time_equal(&token, &token));
        let mut other = token;
        other[31] ^= 1;
        assert!(!constant_time_equal(&token, &other));
    }

    #[test]
    fn the_malformed_budget_refills() {
        let now = Instant::now();
        let mut budget = MalformedBudget::new(now);
        for _ in 0..MALFORMED_BURST {
            assert!(budget.allow(now));
        }
        assert!(!budget.allow(now));
        assert!(budget.allow(now + Duration::from_secs(1)));
        assert!(budget.allow(now + Duration::from_secs(1)));
        assert!(!budget.allow(now + Duration::from_secs(1)));
    }

    #[test]
    fn wire_values_map_to_engine_values() {
        assert_eq!(side_from_wire(1), Some(Side::Buy));
        assert_eq!(side_from_wire(2), Some(Side::Sell));
        assert_eq!(side_from_wire(0), None);
        assert_eq!(ord_type_from_wire(1), Some(OrdType::Limit));
        assert_eq!(ord_type_from_wire(2), Some(OrdType::Market));
        assert_eq!(tif_from_wire(0), Some(TimeInForce::Day));
        assert_eq!(tif_from_wire(3), Some(TimeInForce::FillOrKill));
        assert_eq!(qty_from_wire(0), None);
        assert_eq!(qty_from_wire(u64::MAX), None);
        assert_eq!(qty_from_wire(7), Some(7));
    }
}
