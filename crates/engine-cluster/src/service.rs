//! The deepmsg `ClusteredService` adapter.
//!
//! It decodes the command record the gateway offered, applies it to
//! [`Engine`] with the log position from the callback header, and offers each
//! engine event back to the originating cluster session. Multi-seat traffic
//! (ADR 0012) arrives as service envelopes, is routed by account into a
//! [`SeatOutbox`], and is snapshotted as the `DBS3` service envelope around the
//! unchanged `DBS2` engine record. Legacy bare commands and `DBS1`/`DBS2`/
//! `MDS1` snapshots stay readable.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use deepbook_engine_core::wire::{decode_command, encode_command, encode_event};
use deepbook_engine_core::{
    Command, Engine, EngineConfig, EngineSnapshot, Event, EventHeader, ExecType,
    ExecutionReportEvent, RestingOrderSummary,
};
use deepbook_marketdata::{
    DeepmsgConfig, DeepmsgIncrementalPublisher, Level, MarketDataService,
    ServiceConfig as MarketServiceConfig, Snapshot,
};
use deepbook_types::{AccountId, LogPos, OrderId, Seq};
use deepmsg_cluster_service::service::{SnapshotReader, SnapshotWriter};
use deepmsg_cluster_service::{
    Cluster, ClusteredService, ContainerError, LogHeader, Role, SessionId,
};
use deepmsg_codec_cluster::close_reason::CloseReason;

use crate::envelope::{
    AckSeat, BindRejected, BindSeat, Egress, FenceEvicted, Ingress, RejectMalformed, SeatBound,
    SeatCommand, SeatEvent, UnbindSeat, bind_reject_reason, fence_reason,
};
use crate::metrics::{BookShape, EngineMetrics};
use crate::obs;
use crate::outbox::{
    MAX_TOTAL_OUTBOX_BYTES, SeatOutbox, SeatSnapshot, eviction_plan, total_buffered_bytes,
};
use crate::seats::SeatRegistry;
use crate::snapshot::{
    CodecError, DBS3_MAGIC, ServiceSnapshot, decode, decode_service, encode, encode_service,
};

/// A service-layer failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServiceError {
    /// The command record was not understood.
    Wire(deepbook_engine_core::WireError),
    /// The snapshot record was not understood.
    Codec(CodecError),
    /// The snapshot did not fit the configured engine.
    Snapshot(deepbook_engine_core::SnapshotError),
    /// The market-data transport is unavailable.
    MarketData(String),
    /// The deployment configuration is refused.
    Configuration(String),
}

impl fmt::Display for ServiceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Wire(error) => write!(formatter, "{error}"),
            Self::Codec(error) => write!(formatter, "{error}"),
            Self::Snapshot(error) => write!(formatter, "{error}"),
            Self::MarketData(detail) => write!(formatter, "market data: {detail}"),
            Self::Configuration(detail) => write!(formatter, "configuration: {detail}"),
        }
    }
}

impl std::error::Error for ServiceError {}

/// Where the engine publishes its market-data stream.
#[derive(Clone, Debug)]
pub struct MarketDataConfig {
    /// The transport endpoints (aeron dir, channels, stream ids).
    pub deepmsg: DeepmsgConfig,
    /// Incrementals kept for recovery.
    pub recent_capacity: usize,
}

/// The market-data half of the service, when it is enabled.
struct MarketDataState {
    service: MarketDataService,
    publisher: DeepmsgIncrementalPublisher,
    /// The latest snapshot, read by the recovery-responder thread.
    snapshot: Arc<Mutex<Snapshot>>,
    recent_capacity: usize,
    incrementals: u64,
}

/// A market-data snapshot is broadcast after this many incrementals so a
/// subscriber that missed the recovery request still gets a base.
const SNAPSHOT_EVERY: u64 = 1_024;
/// The container snapshot's market-data prefix: magic, version, `md_seq`.
const MARKET_MAGIC: [u8; 4] = *b"MDS1";
const MARKET_VERSION: u8 = 1;
const MARKET_PREFIX: usize = 4 + 1 + 8;

/// The engine service.
pub struct EngineService {
    config: EngineConfig,
    engine: Engine,
    market_data: Option<MarketDataState>,
    /// The log position of the snapshot this service was restored from.
    ///
    /// The cluster replays the commands after that position into the service.
    /// Events at or below it are already reflected in the mirror seeded from
    /// the restored engine, so republishing them would duplicate data the
    /// subscriber already has. Live events are always after it. `-1` means
    /// "no snapshot, publish everything".
    restored_log_pos: LogPos,
    /// Encoded events that the cluster session could not accept yet.
    ///
    /// An egress publication is a bounded ring: when it is full, `offer`
    /// returns back-pressure and the event would otherwise be lost. The
    /// engine keeps the encoded events per session and retries them on the
    /// next callback. This is a process-local queue: a service restart drops
    /// it (the engine log can replay the commands, and the gateway's FIX
    /// journal covers the reports it already received).
    pending_egress: HashMap<SessionId, VecDeque<Vec<u8>>>,
    /// The seats the service has pinned (ADR 0012), keyed by `seat_id`.
    seats: BTreeMap<u16, SeatOutbox>,
    /// The shared registry, when the deployment is multi-seat.
    seat_registry: Option<SeatRegistry>,
    /// The immutable `account_id -> seat_id` pin, for conflict detection.
    seat_accounts: BTreeMap<AccountId, u16>,
    /// Which cluster session currently owns a seat. Transient: never
    /// snapshotted, and never allowed to influence deterministic state.
    seat_sessions: BTreeMap<u16, SessionId>,
    /// Control egress (`SeatBound`, `BindRejected`, `FenceEvicted`) awaiting a
    /// free cluster egress ring. Seat events are not queued here; they live in
    /// the outbox and are offered from `send_cursor`.
    control_egress: VecDeque<(SessionId, Vec<u8>)>,
    /// The cluster sessions currently open, in open order.
    ///
    /// A command that originates in the service itself (the day-expiry
    /// command) has no originating session; its reports go to every open
    /// session. With one FIX peer per gateway (ADR 0006) that is the one
    /// gateway; multi-peer routing is a later slice.
    sessions: Vec<SessionId>,
    /// Whether a day-expiry timer is armed.
    expiry_scheduled: bool,
    /// The cumulative numbers the container's log tick reads.
    metrics: Arc<EngineMetrics>,
    /// When the book's shape was last read for the metrics page. The book is
    /// walked at most once a second: a scrape wants a recent shape, not the
    /// cost of walking every level on every command.
    last_book_refresh: Instant,
}

/// The most events queued for one cluster session. A gateway that stays
/// back-pressured for this long is not consuming; dropping the oldest keeps
/// the service bounded and is logged. The bound is the container's default
/// slab capacity, so one end-of-day expiry (one cancel report and one book
/// update per order) fits.
const MAX_PENDING_EGRESS: usize = 65_536;
/// The most resting orders one resync batch may synthesize. Matches the
/// multi-seat configuration ceiling in ADR 0012 §3.5.
const MAX_RESYNC_ORDERS: usize = 4096;
/// Synthetic resync reports use `event_seq | 0x8000_0000` so their FIX
/// `ExecID` cannot collide with a real per-command event sequence.
const RESYNC_EVENT_SEQ: u32 = 0x8000_0000;

/// Timer correlation ids.
const EXPIRY_TIMER: i64 = 1;
const EXPIRY_RETRY_TIMER: i64 = 2;
/// Nanoseconds in a day; the expiry deadline is the next UTC midnight.
const DAY_NANOS: i64 = 86_400_000_000_000;
/// How long to wait before retrying an expiry command the ingress refused.
const EXPIRY_RETRY_NANOS: i64 = 1_000_000_000;
/// How often the book's shape is read for the metrics page.
const BOOK_REFRESH_INTERVAL: Duration = Duration::from_secs(1);

impl EngineService {
    /// A fresh engine with the deployment configuration.
    #[must_use]
    pub fn new(config: EngineConfig) -> Self {
        Self {
            config,
            engine: Engine::new(config),
            market_data: None,
            restored_log_pos: -1,
            pending_egress: HashMap::new(),
            seats: BTreeMap::new(),
            seat_accounts: BTreeMap::new(),
            seat_sessions: BTreeMap::new(),
            control_egress: VecDeque::new(),
            seat_registry: None,
            sessions: Vec::new(),
            expiry_scheduled: false,
            metrics: Arc::new(EngineMetrics::default()),
            last_book_refresh: Instant::now(),
        }
    }

    /// Attach the shared seat registry (multi-seat mode).
    ///
    /// The deployment must cap `max_open_orders_per_account` at the resync
    /// batch bound, or a full resync could overflow the seat outbox on its own.
    ///
    /// # Errors
    ///
    /// [`ServiceError::Configuration`] when the risk cap is above the bound.
    pub fn with_seats(mut self, registry: SeatRegistry) -> Result<Self, ServiceError> {
        if self.config.risk.max_open_orders_per_account > MAX_RESYNC_ORDERS {
            return Err(ServiceError::Configuration(format!(
                "max-open-orders {} is above the multi-seat bound {MAX_RESYNC_ORDERS}",
                self.config.risk.max_open_orders_per_account
            )));
        }
        self.seat_registry = Some(registry);
        Ok(self)
    }

    /// A fresh engine that also publishes market data on `config.deepmsg`.
    ///
    /// # Errors
    ///
    /// The media driver or one of the publications is unavailable.
    pub fn with_market_data(
        config: EngineConfig,
        market: MarketDataConfig,
    ) -> Result<Self, ServiceError> {
        let publisher = DeepmsgIncrementalPublisher::connect(&market.deepmsg)
            .map_err(|error| ServiceError::MarketData(error.to_string()))?;
        let service = MarketDataService::new(MarketServiceConfig {
            symbol: config.symbol,
            recent_capacity: market.recent_capacity,
        });
        let snapshot = Arc::new(Mutex::new(service.snapshot()));
        Ok(Self {
            config,
            engine: Engine::new(config),
            market_data: Some(MarketDataState {
                service,
                publisher,
                snapshot,
                recent_capacity: market.recent_capacity,
                incrementals: 0,
            }),
            restored_log_pos: -1,
            pending_egress: HashMap::new(),
            seats: BTreeMap::new(),
            seat_accounts: BTreeMap::new(),
            seat_sessions: BTreeMap::new(),
            control_egress: VecDeque::new(),
            seat_registry: None,
            sessions: Vec::new(),
            expiry_scheduled: false,
            metrics: Arc::new(EngineMetrics::default()),
            last_book_refresh: Instant::now(),
        })
    }

    /// Restore from a snapshot record (`DBS1` or SBE `DBS2`).
    ///
    /// # Errors
    ///
    /// The record is corrupt or does not fit the configuration.
    pub fn from_snapshot(config: EngineConfig, bytes: &[u8]) -> Result<Self, ServiceError> {
        let (snapshot, _market_seq, seats) = decode_service_snapshot(bytes)?;
        let engine = Engine::restore(config, &snapshot).map_err(ServiceError::Snapshot)?;
        let mut service = Self {
            config,
            engine,
            market_data: None,
            restored_log_pos: snapshot.last_log_pos,
            pending_egress: HashMap::new(),
            seats: BTreeMap::new(),
            seat_accounts: BTreeMap::new(),
            seat_sessions: BTreeMap::new(),
            control_egress: VecDeque::new(),
            seat_registry: None,
            sessions: Vec::new(),
            expiry_scheduled: false,
            metrics: Arc::new(EngineMetrics::default()),
            last_book_refresh: Instant::now(),
        };
        service.install_seats(seats)?;
        Ok(service)
    }

    /// The live market-data snapshot, when market data is enabled.
    ///
    /// The container's recovery-responder thread reads this; it is not on the
    /// engine's command path.
    #[must_use]
    pub fn market_snapshot_handle(&self) -> Option<Arc<Mutex<Snapshot>>> {
        self.market_data
            .as_ref()
            .map(|market| Arc::clone(&market.snapshot))
    }

    /// The engine's cumulative numbers, for the container's log tick.
    #[must_use]
    pub fn metrics_handle(&self) -> Arc<EngineMetrics> {
        Arc::clone(&self.metrics)
    }

    /// The engine, for a test or an operator.
    #[must_use]
    pub const fn engine(&self) -> &Engine {
        &self.engine
    }

    /// The engine's book hash.
    #[must_use]
    pub fn book_hash(&self) -> u64 {
        self.engine.book_hash()
    }

    /// The snapshot record.
    ///
    /// # Errors
    ///
    /// A count or an encoded length does not fit the record.
    pub fn snapshot_bytes(&self) -> Result<Vec<u8>, ServiceError> {
        let engine_blob = encode(&self.engine.snapshot()).map_err(ServiceError::Codec)?;
        let snapshot = ServiceSnapshot {
            engine_blob,
            md_seq: self
                .market_data
                .as_ref()
                .map(|market| market.service.last_md_seq()),
            seats: self.seats.values().map(SeatOutbox::snapshot).collect(),
        };
        encode_service(&snapshot).map_err(ServiceError::Codec)
    }

    /// Decode and apply one command at `log_pos`.
    ///
    /// # Errors
    ///
    /// The command record was not understood.
    pub fn apply(&mut self, payload: &[u8], log_pos: LogPos) -> Result<Vec<Event>, ServiceError> {
        let command = decode_command(payload).map_err(ServiceError::Wire)?;
        let mut events = Vec::new();
        self.engine.on_command(&command, log_pos, &mut events);
        self.metrics.note_applied(log_pos);
        for event in &events {
            match event {
                Event::Trade(_) => self.metrics.note_filled(),
                Event::CancelReject(_) => self.metrics.note_rejected(),
                Event::ExecutionReport(report) if report.exec_type == ExecType::Rejected => {
                    self.metrics.note_rejected();
                }
                _ => {}
            }
        }
        self.refresh_book_metrics(false);
        Ok(events)
    }

    /// Restore the pinned seats from a `DBS3` record.
    fn install_seats(&mut self, snapshots: Vec<SeatSnapshot>) -> Result<(), ServiceError> {
        self.seats.clear();
        self.seat_accounts.clear();
        self.seat_sessions.clear();
        for snapshot in snapshots {
            let seat = SeatOutbox::from_snapshot(snapshot)
                .map_err(|_| ServiceError::Codec(CodecError::Inconsistent))?;
            let seat_id = seat.seat_id();
            let account_id = seat.account_id();
            if self.seats.contains_key(&seat_id) || self.seat_accounts.contains_key(&account_id) {
                return Err(ServiceError::Codec(CodecError::Inconsistent));
            }
            self.seat_accounts.insert(account_id, seat_id);
            self.seats.insert(seat_id, seat);
        }
        Ok(())
    }

    /// Apply one service envelope (ADR 0012). Decode errors are logged and
    /// dropped: a trusted gateway sending a corrupt envelope must not take the
    /// cluster service down.
    fn handle_ingress(&mut self, session: SessionId, log_pos: LogPos, ingress: Ingress<'_>) {
        match ingress {
            Ingress::BindSeat(message) => self.bind_seat(session, log_pos, message),
            Ingress::SeatCommand(message) => self.seat_command(session, log_pos, message),
            Ingress::AckSeat(message) => self.ack_seat(session, message),
            Ingress::RejectMalformed(message) => self.reject_malformed(session, message),
            Ingress::UnbindSeat(message) => self.unbind_seat(session, message),
        }
    }

    fn bind_seat(&mut self, session: SessionId, log_pos: LogPos, message: BindSeat) {
        if message.seat_id == 0 {
            self.reject_bind(session, message.seat_id, bind_reject_reason::INVALID_SEAT);
            return;
        }
        if let Some(registry) = &self.seat_registry {
            match registry.get(message.seat_id) {
                None => {
                    self.reject_bind(session, message.seat_id, bind_reject_reason::UNKNOWN_SEAT);
                    return;
                }
                Some(seat) if seat.account_id != message.account_id => {
                    self.reject_bind(
                        session,
                        message.seat_id,
                        bind_reject_reason::ACCOUNT_CONFLICT,
                    );
                    return;
                }
                Some(_) => {}
            }
        }
        if let Some(owner) = self.seat_accounts.get(&message.account_id).copied() {
            if owner != message.seat_id {
                self.reject_bind(
                    session,
                    message.seat_id,
                    bind_reject_reason::ACCOUNT_CONFLICT,
                );
                return;
            }
        }
        if let Some(existing) = self.seats.get(&message.seat_id) {
            if existing.account_id() != message.account_id {
                self.reject_bind(
                    session,
                    message.seat_id,
                    bind_reject_reason::ACCOUNT_CONFLICT,
                );
                return;
            }
        }

        let resync = self
            .seats
            .get(&message.seat_id)
            .is_some_and(|seat| seat.needs_resync(message.last_journal_seq));
        let mut summaries: Vec<RestingOrderSummary> = Vec::new();
        if resync {
            self.engine
                .resting_orders_for_account(message.account_id, &mut summaries);
            if summaries.len() > MAX_RESYNC_ORDERS {
                eprintln!(
                    "engine: seat {} has {} resting orders; refusing the resync",
                    message.seat_id,
                    summaries.len()
                );
                self.reject_bind(session, message.seat_id, bind_reject_reason::INVALID_SEAT);
                return;
            }
        }

        if let std::collections::btree_map::Entry::Vacant(vacant) =
            self.seats.entry(message.seat_id)
        {
            vacant.insert(SeatOutbox::new(message.seat_id, message.account_id));
            self.seat_accounts
                .insert(message.account_id, message.seat_id);
        }
        let previous_session = self.seat_sessions.insert(message.seat_id, session);

        let (epoch, generation, replay_from, next_seq) = {
            let seat = self
                .seats
                .get_mut(&message.seat_id)
                .expect("the seat was created above");
            let epoch = seat.epoch().saturating_add(1);
            seat.set_epoch(epoch);
            let replay_from = if resync {
                let replay_from = seat.begin_resync();
                let mut ordinal = 0_u32;
                for summary in &summaries {
                    let avg_px = if summary.filled == 0 {
                        0
                    } else {
                        i64::try_from(summary.notional / i128::from(summary.filled)).unwrap_or(0)
                    };
                    let report = ExecutionReportEvent {
                        header: EventHeader {
                            log_pos,
                            event_seq: RESYNC_EVENT_SEQ | ordinal,
                        },
                        order_id: summary.order_id,
                        cl_ord_id: summary.cl_ord_id,
                        account: message.account_id,
                        symbol: self.config.symbol,
                        side: summary.side,
                        exec_type: ExecType::New,
                        state: summary.state,
                        price: Some(summary.price),
                        order_qty: summary.order_qty,
                        last_qty: 0,
                        last_px: None,
                        leaves_qty: summary.order_qty - summary.filled,
                        cum_qty: summary.filled,
                        avg_px,
                        reason: None,
                    };
                    let bytes = encode_event(&Event::ExecutionReport(report));
                    seat.push_engine_event(0, true, None, bytes);
                    ordinal = ordinal.saturating_add(1);
                }
                replay_from
            } else {
                let effective = message
                    .last_journal_seq
                    .min(seat.next_seq().saturating_sub(1));
                effective.saturating_add(1)
            };
            seat.set_send_cursor(replay_from.saturating_sub(1));
            (epoch, seat.generation(), replay_from, seat.next_seq())
        };

        // The resync batch can push the global budget over its cap; the
        // deterministic eviction may pick this seat. If it did (or the batch
        // alone exceeded the seat cap), the bind is answered with a fence, not
        // a `SeatBound` whose entries never existed.
        self.enforce_outbox_limits();
        if self
            .seats
            .get(&message.seat_id)
            .is_some_and(SeatOutbox::overflowed)
        {
            return;
        }
        eprintln!(
            "engine: seat {} bind epoch {} resync {} replay {} next {}",
            message.seat_id, epoch, resync, replay_from, next_seq
        );
        if let Some(previous) = previous_session {
            if previous != session {
                self.queue_control(
                    previous,
                    Egress::FenceEvicted(FenceEvicted {
                        seat_id: message.seat_id,
                        new_epoch: epoch,
                        reason: fence_reason::SUPERSEDED,
                    }),
                );
            }
        }
        self.queue_control(
            session,
            Egress::SeatBound(SeatBound {
                seat_id: message.seat_id,
                epoch,
                generation,
                replay_from_seq: replay_from,
                next_seq,
                resynced: resync,
            }),
        );
    }

    fn seat_command(&mut self, session: SessionId, log_pos: LogPos, message: SeatCommand<'_>) {
        let Some(seat) = self.seats.get(&message.seat_id) else {
            return;
        };
        if seat.epoch() != message.epoch
            || self.seat_sessions.get(&message.seat_id) != Some(&session)
        {
            return;
        }
        let account = seat.account_id();
        let command = match decode_command(message.command) {
            Ok(command) => command,
            Err(error) => {
                eprintln!("engine: seat {} command: {error}", message.seat_id);
                return;
            }
        };
        if command.account() != Some(account) {
            eprintln!(
                "engine: seat {} tried to act for another account",
                message.seat_id
            );
            return;
        }
        let events = match self.apply(message.command, log_pos) {
            Ok(events) => events,
            Err(error) => {
                eprintln!("engine: seat {} command: {error}", message.seat_id);
                return;
            }
        };
        self.publish_market_data(&events);
        self.route_events(&events, Some(&command));
    }

    /// Route order-state events to the seat that owns each account. One
    /// command can touch several seats: a taker on seat A fills a maker on
    /// seat B, and both reports must reach their owner.
    fn route_events(&mut self, events: &[Event], command: Option<&Command>) {
        let mut liquidity: HashMap<OrderId, u8> = HashMap::new();
        for event in events {
            if let Event::Trade(trade) = event {
                liquidity.insert(trade.maker_order_id, 1);
                liquidity.insert(trade.taker_order_id, 2);
            }
        }
        let replace_req = match command {
            Some(Command::ReplaceOrder(replace)) => Some((replace.account, replace.cl_ord_id)),
            _ => None,
        };
        for event in events {
            let (account, bytes, tag, request) = match event {
                Event::ExecutionReport(report) => {
                    let tag = if report.exec_type == ExecType::Trade {
                        liquidity.get(&report.order_id).copied().unwrap_or(0)
                    } else {
                        0
                    };
                    let request = if report.exec_type == ExecType::Replaced {
                        replace_req
                            .filter(|(account, _)| *account == report.account)
                            .map(|(_, id)| id)
                    } else {
                        None
                    };
                    (report.account, encode_event(event), tag, request)
                }
                Event::CancelReject(reject) => (reject.account, encode_event(event), 0, None),
                _ => continue,
            };
            let Some(seat_id) = self.seat_accounts.get(&account).copied() else {
                continue;
            };
            if let Some(seat) = self.seats.get_mut(&seat_id) {
                seat.push_engine_event(tag, false, request, bytes);
            }
        }
        self.enforce_outbox_limits();
    }

    fn ack_seat(&mut self, session: SessionId, message: AckSeat) {
        let Some(seat) = self.seats.get_mut(&message.seat_id) else {
            return;
        };
        if seat.epoch() != message.epoch
            || self.seat_sessions.get(&message.seat_id) != Some(&session)
        {
            return;
        }
        let effective = seat.ack(message.up_to_seq);
        eprintln!(
            "engine: seat {} ack {} -> {} acked {}",
            message.seat_id,
            message.up_to_seq,
            effective,
            seat.acked_seq()
        );
    }

    fn reject_malformed(&mut self, session: SessionId, message: RejectMalformed) {
        let Some(seat) = self.seats.get(&message.seat_id) else {
            return;
        };
        if seat.epoch() != message.epoch
            || self.seat_sessions.get(&message.seat_id) != Some(&session)
        {
            return;
        }
        if let Some(seat) = self.seats.get_mut(&message.seat_id) {
            seat.push_malformed_reject(
                message.raw_cl_ord_id,
                message.raw_orig_cl_ord_id,
                message.is_cancel_or_replace,
                message.reason,
            );
        }
        self.enforce_outbox_limits();
    }

    fn unbind_seat(&mut self, session: SessionId, message: UnbindSeat) {
        let Some(seat) = self.seats.get(&message.seat_id) else {
            return;
        };
        if seat.epoch() != message.epoch {
            return;
        }
        if self.seat_sessions.get(&message.seat_id) == Some(&session) {
            self.seat_sessions.remove(&message.seat_id);
        }
    }

    fn reject_bind(&mut self, session: SessionId, seat_id: u16, reason: u8) {
        self.queue_control(
            session,
            Egress::BindRejected(BindRejected { seat_id, reason }),
        );
    }

    fn queue_control(&mut self, session: SessionId, message: Egress<'_>) {
        match message.encode() {
            Ok(frame) => self.control_egress.push_back((session, frame)),
            Err(error) => eprintln!("engine: service egress: {error}"),
        }
    }

    /// Apply the per-seat and global outbox budgets (ADR 0012 §3.6).
    fn enforce_outbox_limits(&mut self) {
        let over: Vec<u16> = self
            .seats
            .iter()
            .filter(|(_, seat)| seat.over_capacity())
            .map(|(id, _)| *id)
            .collect();
        for seat_id in over {
            self.overflow_seat(seat_id);
        }
        if total_buffered_bytes(self.seats.values()) <= MAX_TOTAL_OUTBOX_BYTES {
            return;
        }
        let victims = eviction_plan(self.seats.values(), MAX_TOTAL_OUTBOX_BYTES);
        for seat_id in victims {
            self.overflow_seat(seat_id);
        }
    }

    fn overflow_seat(&mut self, seat_id: u16) {
        eprintln!("engine: seat {seat_id} outbox overflow");
        let Some(epoch) = self.seats.get_mut(&seat_id).map(|seat| {
            seat.overflow();
            seat.epoch()
        }) else {
            return;
        };
        if let Some(session) = self.seat_sessions.remove(&seat_id) {
            self.queue_control(
                session,
                Egress::FenceEvicted(FenceEvicted {
                    seat_id,
                    new_epoch: epoch,
                    reason: fence_reason::OUTBOX_OVERFLOW,
                }),
            );
        }
    }

    /// Read the book's shape for the metrics page, at most once a second.
    fn refresh_book_metrics(&mut self, force: bool) {
        if !force && self.last_book_refresh.elapsed() < BOOK_REFRESH_INTERVAL {
            return;
        }
        self.last_book_refresh = Instant::now();
        let view = self.engine.view();
        self.metrics.note_book(BookShape {
            bid_levels: view.bids.len() as u64,
            ask_levels: view.asks.len() as u64,
            bid_qty: view.bids.iter().map(|level| level.qty).sum(),
            ask_qty: view.asks.iter().map(|level| level.qty).sum(),
            md_seq: i64::try_from(view.md_seq).unwrap_or(i64::MAX),
        });
    }

    /// Feed the engine's trades and book updates into the market-data stream
    /// and answer any pending recovery request.
    ///
    /// The offers are best effort: a slow or absent subscriber must not
    /// backpressure the cluster service. A dropped incremental leaves a gap
    /// that the subscriber repairs through the recovery channel.
    fn publish_market_data(&mut self, events: &[Event]) {
        let Some(market) = self.market_data.as_mut() else {
            return;
        };
        let MarketDataState {
            service,
            publisher,
            snapshot,
            incrementals,
            ..
        } = market;
        for event in events {
            // Commands replayed after the snapshot were already reflected in
            // the engine state that seeded the mirror; publishing them again
            // would duplicate trades on the tape and advance `md_seq` for
            // data the subscriber already has.
            if event.header().log_pos <= self.restored_log_pos {
                continue;
            }
            let seq = service.last_md_seq() + 1;
            let incremental = match event {
                Event::BookUpdate(update) => {
                    service.on_book_update(seq, update.side, update.price, update.qty)
                }
                Event::Trade(trade) => {
                    service.on_trade(seq, trade.price, trade.qty, trade.aggressor_side)
                }
                _ => continue,
            };
            match incremental {
                Ok(incremental) => {
                    *incrementals = incrementals.saturating_add(1);
                    if let Err(error) = publisher.try_publish_incremental(incremental) {
                        eprintln!("engine: market data publish: {error}");
                    }
                }
                Err(error) => eprintln!("engine: market data: {error}"),
            }
        }
        let latest = service.snapshot();
        if let Ok(mut cell) = snapshot.lock() {
            *cell = latest.clone();
        }
        if *incrementals >= SNAPSHOT_EVERY {
            *incrementals = 0;
            if let Err(error) = publisher.try_publish_snapshot(&latest) {
                eprintln!("engine: market data snapshot: {error}");
            }
        }
    }

    /// Encode and enqueue events for one cluster session.
    ///
    /// The cluster's egress publication is a bounded ring; an event that does
    /// not fit must be retried, not dropped. The queue is FIFO so the reports
    /// reach the FIX session in event order.
    fn queue_egress(&mut self, session: SessionId, events: &[Event]) {
        let queue = self.pending_egress.entry(session).or_default();
        for event in events {
            if queue.len() >= MAX_PENDING_EGRESS {
                queue.pop_front();
                eprintln!(
                    "engine: egress queue for cluster session {} is full; dropping the oldest",
                    session.0
                );
            }
            queue.push_back(encode_event(event));
        }
    }

    /// Offer every queued event that the egress publication accepts now.
    ///
    /// Back pressure leaves the event for the next callback; the queue is
    /// discarded only when `on_session_close` says the session is gone. This
    /// runs after every command (and on the day timer once it is scheduled),
    /// which is enough: a gateway that is not reading is also not sending
    /// commands.
    fn flush_legacy(&mut self, cluster: &mut Cluster<'_>) {
        if self.pending_egress.is_empty() {
            return;
        }
        let sessions: Vec<SessionId> = self.pending_egress.keys().copied().collect();
        for session in sessions {
            let Some(queue) = self.pending_egress.get_mut(&session) else {
                continue;
            };
            while let Some(payload) = queue.front() {
                match offer_once(cluster, session, payload) {
                    OfferOutcome::Sent => {
                        queue.pop_front();
                    }
                    // Back pressure, and a session that is not visible for a
                    // moment (a leader change): keep the event and retry. The
                    // queue is removed explicitly on `on_session_close`; this
                    // path must never drop a report.
                    OfferOutcome::BackPressured => break,
                }
            }
        }
        self.pending_egress.retain(|_, queue| !queue.is_empty());
    }

    /// Offer every queued control frame, stopping at the first back-pressure.
    ///
    /// Returns `true` when the control queue is empty, which is the barrier
    /// that keeps `SeatBound` ahead of the first `SeatEvent` for that session.
    fn flush_control(&mut self, cluster: &mut Cluster<'_>) -> bool {
        while let Some((session, frame)) = self.control_egress.front() {
            match offer_once(cluster, *session, frame) {
                OfferOutcome::Sent => {
                    self.control_egress.pop_front();
                }
                OfferOutcome::BackPressured => return false,
            }
        }
        true
    }

    /// Offer each bound seat's retained events from its `send_cursor`.
    fn flush_seats(&mut self, cluster: &mut Cluster<'_>) {
        let seat_ids: Vec<u16> = self.seats.keys().copied().collect();
        for seat_id in seat_ids {
            let Some(session) = self.seat_sessions.get(&seat_id).copied() else {
                continue;
            };
            loop {
                let (next, frame) = {
                    let Some(seat) = self.seats.get(&seat_id) else {
                        break;
                    };
                    let next = seat.send_cursor().saturating_add(1);
                    let Some(entry) = seat.entry_at(next) else {
                        break;
                    };
                    let encoded = Egress::SeatEvent(SeatEvent {
                        seat_id,
                        epoch: seat.epoch(),
                        generation: seat.generation(),
                        seat_seq: entry.seat_seq,
                        liquidity: entry.liquidity,
                        payload_kind: entry.payload_kind,
                        resync: entry.is_resync(),
                        replace_req_cl_ord_id: entry.replace_req_cl_ord_id,
                        payload: &entry.payload,
                    })
                    .encode();
                    match encoded {
                        Ok(frame) => (next, frame),
                        Err(error) => {
                            eprintln!("engine: seat {seat_id} egress: {error}");
                            break;
                        }
                    }
                };
                match offer_once(cluster, session, &frame) {
                    OfferOutcome::Sent => {
                        if let Some(seat) = self.seats.get_mut(&seat_id) {
                            seat.advance_send_cursor(next);
                        }
                    }
                    OfferOutcome::BackPressured => break,
                }
            }
        }
    }

    /// Offer control frames, then the legacy queue, then the seat outboxes.
    fn flush_egress(&mut self, cluster: &mut Cluster<'_>) {
        if !self.flush_control(cluster) {
            return;
        }
        self.flush_legacy(cluster);
        self.flush_seats(cluster);
    }

    /// Offer the end-of-day command as a service message.
    ///
    /// The command goes through the log, so a replica that was not the leader
    /// applies the same expiries at the same position. A refused offer is
    /// retried on a short timer rather than forcing the expiry inline.
    fn request_expiry(&mut self, cluster: &mut Cluster<'_>, timestamp: i64) {
        let offered = matches!(
            cluster.offer(&encode_command(&Command::ExpireDayOrders)),
            Ok(position) if position > 0
        );
        if !offered {
            eprintln!("engine: day expiry was not accepted; retrying");
            let _ = cluster.schedule_timer(
                EXPIRY_RETRY_TIMER,
                timestamp.saturating_add(EXPIRY_RETRY_NANOS),
            );
        }
    }
}

impl ClusteredService for EngineService {
    fn on_start(
        &mut self,
        cluster: &mut Cluster<'_>,
        snapshot: Option<&mut SnapshotReader<'_>>,
    ) -> Result<(), ContainerError> {
        let restored_from_snapshot = snapshot.is_some();
        let mut restored_bytes = 0_usize;
        obs::start(self.config.symbol);
        // Sessions and queued egress are process-local; a start begins with
        // none of them, whatever the previous incarnation left behind.
        self.pending_egress.clear();
        self.seat_sessions.clear();
        self.control_egress.clear();
        self.sessions.clear();
        if let Some(reader) = snapshot {
            let bytes = read_snapshot(cluster, reader)?;
            restored_bytes = bytes.len();
            let (decoded, market_seq, seats) =
                decode_service_snapshot(&bytes).map_err(container)?;
            self.engine = Engine::restore(self.config, &decoded).map_err(container)?;
            self.install_seats(seats).map_err(container)?;
            self.restored_log_pos = decoded.last_log_pos;
            // ★ Said out loud, and said on **both** branches, because "did this
            // member come back from its snapshot or replay the log from the
            // beginning" has no other observable: with a small log the two take
            // the same time, and a recovery that quietly became a full replay
            // is exactly what a four-host run is supposed to catch.
            eprintln!(
                "engine: restored from snapshot at log-pos {} ({} bytes)",
                decoded.last_log_pos,
                bytes.len()
            );
            let restored_book =
                market_snapshot_from_engine(&self.engine, market_seq.unwrap_or_default());
            if let Some(market) = self.market_data.as_mut() {
                if let Some(seq) = market_seq {
                    market.service = MarketDataService::with_sequence(
                        MarketServiceConfig {
                            symbol: self.config.symbol,
                            recent_capacity: market.recent_capacity,
                        },
                        seq,
                    );
                }
                // The engine snapshot carries the book but not the market-data
                // mirror; seed the mirror before the first snapshot is served.
                if let Err(error) = market.service.install_snapshot(&restored_book) {
                    eprintln!("engine: market data restore: {error}");
                }
                let snapshot = market.service.snapshot();
                if let Ok(mut cell) = market.snapshot.lock() {
                    *cell = snapshot.clone();
                }
                if let Err(error) = market.publisher.try_publish_snapshot(&snapshot) {
                    eprintln!("engine: market data snapshot: {error}");
                }
            }
        } else if let Some(market) = self.market_data.as_mut() {
            let snapshot = market.service.snapshot();
            if let Ok(mut cell) = market.snapshot.lock() {
                *cell = snapshot.clone();
            }
            if let Err(error) = market.publisher.try_publish_snapshot(&snapshot) {
                eprintln!("engine: market data snapshot: {error}");
            }
        }
        if !restored_from_snapshot {
            eprintln!("engine: no snapshot; the engine starts empty and replays the log");
        }
        self.refresh_book_metrics(true);
        obs::recover(
            restored_from_snapshot,
            if restored_from_snapshot {
                self.restored_log_pos
            } else {
                -1
            },
            restored_bytes,
        );
        Ok(())
    }

    fn on_session_open(
        &mut self,
        _cluster: &mut Cluster<'_>,
        session: SessionId,
        _timestamp: i64,
    ) -> Result<(), ContainerError> {
        if !self.sessions.contains(&session) {
            self.sessions.push(session);
        }
        Ok(())
    }

    fn on_session_close(
        &mut self,
        _cluster: &mut Cluster<'_>,
        session: SessionId,
        _timestamp: i64,
        _reason: CloseReason,
    ) -> Result<(), ContainerError> {
        // The cluster session is gone; nothing can deliver these reports.
        self.pending_egress.remove(&session);
        self.control_egress.retain(|(target, _)| *target != session);
        let unbound: Vec<u16> = self
            .seat_sessions
            .iter()
            .filter(|(_, owner)| **owner == session)
            .map(|(seat_id, _)| *seat_id)
            .collect();
        for seat_id in unbound {
            self.seat_sessions.remove(&seat_id);
        }
        self.sessions.retain(|open| *open != session);
        Ok(())
    }

    fn on_session_message(
        &mut self,
        cluster: &mut Cluster<'_>,
        session: Option<SessionId>,
        timestamp: i64,
        payload: &[u8],
        header: &LogHeader,
    ) -> Result<(), ContainerError> {
        // The first command after a start (or a leader change) arms the
        // day-expiry timer. `on_start` has no cluster time to compute a
        // deadline, and a follower must not arm it.
        if !self.expiry_scheduled
            && timestamp > 0
            && matches!(
                cluster.schedule_timer(EXPIRY_TIMER, next_midnight(timestamp)),
                Ok(true)
            )
        {
            self.expiry_scheduled = true;
        }

        if payload.first() == Some(&crate::envelope::MAGIC) {
            if let Some(session) = session {
                match Ingress::decode(payload) {
                    Ok(ingress) => self.handle_ingress(session, header.position, ingress),
                    Err(error) => eprintln!("engine: service envelope: {error}"),
                }
            }
            self.flush_egress(cluster);
            return Ok(());
        }

        let events = self.apply(payload, header.position).map_err(container)?;
        self.publish_market_data(&events);
        match session {
            Some(session) => self.queue_egress(session, &events),
            // A service-originated command (the day expiry) has no originating
            // session. With seats, route each report to the account's owner;
            // the legacy single-session mode still broadcasts.
            None if self.seats.is_empty() => {
                let targets = self.sessions.clone();
                for target in targets {
                    self.queue_egress(target, &events);
                }
            }
            None => self.route_events(&events, None),
        }
        self.flush_egress(cluster);
        Ok(())
    }

    fn on_timer_event(
        &mut self,
        cluster: &mut Cluster<'_>,
        correlation_id: i64,
        timestamp: i64,
    ) -> Result<(), ContainerError> {
        match correlation_id {
            EXPIRY_TIMER => {
                // Timers are one-shot: arm tomorrow's before doing any work.
                self.expiry_scheduled = matches!(
                    cluster.schedule_timer(EXPIRY_TIMER, next_midnight(timestamp)),
                    Ok(true)
                );
                self.request_expiry(cluster, timestamp);
            }
            EXPIRY_RETRY_TIMER => self.request_expiry(cluster, timestamp),
            _ => {}
        }
        self.flush_egress(cluster);
        Ok(())
    }

    fn on_take_snapshot(&mut self, writer: &mut SnapshotWriter<'_>) -> Result<(), ContainerError> {
        let bytes = self.snapshot_bytes().map_err(container)?;
        // The snapshot's identity, into the container's own log. Three members
        // that applied the same log must produce the same bytes, and this is
        // the only point where all three hold them at the same position, so
        // this line is what makes a four-host run checkable: compare the three
        // logs, and they must agree.
        //
        // `DefaultHasher` rather than a digest crate because the three members
        // run the same binary, so the value is comparable without adding a
        // dependency to the product for a diagnostic.
        let (hash, state_hash) = {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            bytes.hash(&mut hasher);
            let hash = hasher.finish();
            eprintln!("engine: snapshot len={} hash={hash:016x}", bytes.len());
            // ★ A second digest with `last_log_pos` zeroed. The plain one is
            // only comparable between members that applied the *same* log: a
            // leadership change appends control records, every later command's
            // position moves, and two runs that traded identically then hash
            // differently. This one is the trading state alone, so it can be
            // compared across a failover -- which is the question a four-host
            // run actually asks.
            let mut state_hash = None;
            if let Ok((mut snapshot, _seq, seats)) = decode_service_snapshot(&bytes) {
                snapshot.last_log_pos = 0;
                if let Ok(body) = encode(&snapshot) {
                    let mut state = std::collections::hash_map::DefaultHasher::new();
                    body.hash(&mut state);
                    for seat in &seats {
                        seat.seat_id.hash(&mut state);
                        seat.account_id.hash(&mut state);
                    }
                    state_hash = Some(state.finish());
                    eprintln!("engine: state hash={:016x}", state.finish());
                }
            }
            (hash, state_hash)
        };
        self.metrics.note_snapshot();
        obs::snapshot(bytes.len(), hash, state_hash);
        let chunk = writer.max_payload_length().max(1);
        for part in bytes.chunks(chunk) {
            loop {
                if writer.offer(part) > 0 {
                    break;
                }
                writer.idle();
            }
        }
        if let Some(market) = self.market_data.as_mut() {
            let snapshot = market.service.snapshot();
            if let Ok(mut cell) = market.snapshot.lock() {
                *cell = snapshot.clone();
            }
            if let Err(error) = market.publisher.try_publish_snapshot(&snapshot) {
                eprintln!("engine: market data snapshot: {error}");
            }
        }
        Ok(())
    }

    fn on_role_change(&mut self, new_role: Role) -> Result<(), ContainerError> {
        self.metrics.set_leader(new_role == Role::Leader);
        obs::role(role_name(new_role), new_role == Role::Leader);
        if new_role == Role::Leader {
            // Timers belong to the leader. The previous leader's timer is not
            // ours; arm a new one on the next command (the callback has no
            // cluster handle, so it cannot schedule here).
            self.expiry_scheduled = false;
            // `send_cursor` is the previous leader's process-local progress;
            // re-offer every unacknowledged entry. Gateways deduplicate by
            // `seat_seq`, so this can duplicate a delivery but never loses one.
            for seat in self.seats.values_mut() {
                let acked = seat.acked_seq();
                seat.set_send_cursor(acked);
            }
        }
        Ok(())
    }

    fn on_terminate(&mut self, _cluster: &mut Cluster<'_>) -> Result<(), ContainerError> {
        obs::stop();
        Ok(())
    }
}

/// The role's name in the log; the same words `aeron.cluster` uses.
const fn role_name(role: Role) -> &'static str {
    match role {
        Role::Follower => "follower",
        Role::Candidate => "candidate",
        Role::Leader => "leader",
    }
}

/// Build the market-data snapshot for a restored engine.
///
/// The engine's `BookView` is the same state market data mirrors from
/// `BookUpdate` events, so it is the right base for a mirror that lost its
/// levels in a restart. `md_seq` is the market-data stream's sequence, not the
/// engine's; the caller passes the sequence the stream resumes at.
fn market_snapshot_from_engine(engine: &Engine, md_seq: Seq) -> Snapshot {
    let view = engine.view();
    let levels = |levels: Vec<deepbook_engine_core::LevelView>| {
        levels
            .into_iter()
            .map(|level| Level {
                price: level.price,
                qty: level.qty,
            })
            .collect()
    };
    Snapshot {
        md_seq,
        symbol: view.symbol,
        bids: levels(view.bids),
        asks: levels(view.asks),
    }
}

/// Prepend the market-data sequence to an encoded engine snapshot.
///
/// The M4 record is unchanged after the 13-byte prefix, and a record without
/// Decode a container snapshot.
///
/// `DBS3` carries the engine blob plus the pinned seats; the legacy `MDS1`
/// prefix and bare `DBS1`/`DBS2` records carry no seats.
fn decode_service_snapshot(
    bytes: &[u8],
) -> Result<(EngineSnapshot, Option<Seq>, Vec<SeatSnapshot>), ServiceError> {
    if bytes.starts_with(&DBS3_MAGIC) {
        let snapshot = decode_service(bytes).map_err(ServiceError::Codec)?;
        let engine = decode(&snapshot.engine_blob).map_err(ServiceError::Codec)?;
        return Ok((engine, snapshot.md_seq, snapshot.seats));
    }
    if bytes.starts_with(&MARKET_MAGIC) {
        if bytes.len() < MARKET_PREFIX {
            return Err(ServiceError::Codec(CodecError::Truncated));
        }
        if bytes[4] != MARKET_VERSION {
            return Err(ServiceError::Codec(CodecError::BadVersion(bytes[4])));
        }
        let mut seq = [0_u8; 8];
        seq.copy_from_slice(&bytes[5..MARKET_PREFIX]);
        let engine = decode(&bytes[MARKET_PREFIX..]).map_err(ServiceError::Codec)?;
        return Ok((engine, Some(Seq::from_le_bytes(seq)), Vec::new()));
    }
    let engine = decode(bytes).map_err(ServiceError::Codec)?;
    Ok((engine, None, Vec::new()))
}

fn read_snapshot(
    cluster: &mut Cluster<'_>,
    reader: &mut SnapshotReader<'_>,
) -> Result<Vec<u8>, ContainerError> {
    let mut bytes = Vec::new();
    while !reader.is_end_of_stream() {
        let fragments = reader.poll(16, |fragment| {
            if let Some((buffer, offset, length)) = fragment.payload_region() {
                for index in 0..length {
                    if let Some(byte) = buffer.load_u8(offset + index) {
                        bytes.push(byte);
                    }
                }
            }
        });
        if fragments == 0 {
            cluster.idle(0)?;
        }
    }
    Ok(bytes)
}

/// What one attempt to offer an egress record achieved.
enum OfferOutcome {
    /// The publication took the record.
    Sent,
    /// The ring is full, the session is mid-rotation, or the session is not
    /// visible for the moment; retry later.
    BackPressured,
}

fn offer_once(cluster: &mut Cluster<'_>, session: SessionId, payload: &[u8]) -> OfferOutcome {
    let Some(mut view) = cluster.session(session) else {
        return OfferOutcome::BackPressured;
    };
    match view.offer(payload) {
        // A positive position means the record was appended. A negative
        // answer is the publication's back-pressure code, not a fatal error:
        // the event stays queued and is retried on the next callback.
        Ok(offered) if offered > 0 => OfferOutcome::Sent,
        Ok(_) | Err(_) => OfferOutcome::BackPressured,
    }
}

/// The UTC midnight after `timestamp_ns`.
///
/// The timer timestamp is cluster time in epoch nanoseconds, so the expiry
/// runs at the same boundary on every replica regardless of process uptime.
fn next_midnight(timestamp_ns: i64) -> i64 {
    if timestamp_ns <= 0 {
        return DAY_NANOS;
    }
    (timestamp_ns / DAY_NANOS + 1).saturating_mul(DAY_NANOS)
}

fn container(error: impl fmt::Display) -> ContainerError {
    ContainerError::Cluster(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use deepbook_engine_core::ClientOrderId;
    use deepbook_engine_core::wire::{decode_event, encode_command, encode_event};
    use deepbook_engine_core::{Command, NewOrder, OrdType, RiskConfig, TimeInForce};
    use deepbook_types::{AccountId, Price, Qty, Side};

    fn config() -> EngineConfig {
        EngineConfig {
            symbol: 1,
            max_orders: 64,
            recent_order_window: 64,
            risk: RiskConfig::disabled(),
        }
    }

    fn cl(value: &str) -> ClientOrderId {
        value.parse().expect("client order id")
    }

    fn limit(account: AccountId, id: &str, side: Side, price: Price, qty: Qty) -> Command {
        Command::NewOrder(NewOrder {
            symbol: 1,
            account,
            cl_ord_id: cl(id),
            side,
            price: Some(price),
            qty,
            ord_type: OrdType::Limit,
            time_in_force: TimeInForce::GoodTillCanceled,
            post_only: false,
        })
    }

    #[test]
    fn a_command_becomes_events() {
        let mut service = EngineService::new(config());
        let events = service
            .apply(&encode_command(&limit(1, "S1", Side::Sell, 100, 10)), 1)
            .expect("apply");
        assert!(!events.is_empty());
        let bytes = encode_event(&events[0]);
        assert!(deepbook_engine_core::decode_event(&bytes).is_ok());
    }

    #[test]
    fn a_snapshot_restores_the_service_and_continues_identically() {
        let mut service = EngineService::new(config());
        let commands = [
            limit(1, "S1", Side::Sell, 100, 10),
            limit(2, "B1", Side::Buy, 100, 4),
            limit(3, "S2", Side::Sell, 101, 20),
            Command::KillSwitch { account: Some(1) },
        ];
        for (index, command) in commands.iter().enumerate() {
            service
                .apply(&encode_command(command), index as LogPos + 1)
                .expect("apply");
        }

        let snapshot = service.snapshot_bytes().expect("snapshot");
        let mut restored = EngineService::from_snapshot(config(), &snapshot).expect("restore");
        assert_eq!(service.book_hash(), restored.book_hash());

        let continuation = [
            limit(3, "B2", Side::Buy, 100, 5),
            limit(4, "S3", Side::Sell, 99, 7),
        ];
        for (index, command) in continuation.iter().enumerate() {
            let log = index as LogPos + 1;
            let first = service.apply(&encode_command(command), log).expect("first");
            let second = restored
                .apply(&encode_command(command), log)
                .expect("second");
            assert_eq!(first, second);
        }
        assert_eq!(service.book_hash(), restored.book_hash());
    }

    #[test]
    fn the_expiry_deadline_is_the_next_midnight() {
        assert_eq!(DAY_NANOS, next_midnight(0));
        assert_eq!(DAY_NANOS, next_midnight(1));
        assert_eq!(DAY_NANOS, next_midnight(DAY_NANOS - 1));
        assert_eq!(2 * DAY_NANOS, next_midnight(DAY_NANOS));
        assert_eq!(2 * DAY_NANOS, next_midnight(DAY_NANOS + 1));
    }

    #[test]
    fn the_market_snapshot_comes_from_the_restored_book() {
        let mut service = EngineService::new(config());
        service
            .apply(&encode_command(&limit(1, "S1", Side::Sell, 100, 10)), 1)
            .expect("sell");
        service
            .apply(&encode_command(&limit(2, "B1", Side::Buy, 99, 4)), 2)
            .expect("buy");

        let restored = market_snapshot_from_engine(service.engine(), 7);
        assert_eq!(7, restored.md_seq);
        assert_eq!(1, restored.symbol);
        assert_eq!(vec![Level { price: 99, qty: 4 }], restored.bids);
        assert_eq!(
            vec![Level {
                price: 100,
                qty: 10
            }],
            restored.asks
        );

        let mut market = MarketDataService::new(MarketServiceConfig {
            symbol: 1,
            recent_capacity: 8,
        });
        market.install_snapshot(&restored).expect("install");
        assert_eq!(0, market.last_md_seq());
        assert_eq!(restored.bids, market.snapshot().bids);
        assert_eq!(restored.asks, market.snapshot().asks);
    }

    #[test]
    fn the_market_sequence_round_trips_in_the_container_snapshot() {
        let mut service = EngineService::new(config());
        let commands = [
            limit(1, "S1", Side::Sell, 100, 10),
            limit(2, "B1", Side::Buy, 100, 4),
        ];
        for (index, command) in commands.iter().enumerate() {
            service
                .apply(&encode_command(command), index as LogPos + 1)
                .expect("apply");
        }

        let body = encode(&service.engine.snapshot()).expect("snapshot");
        let wrapped = encode_service(&ServiceSnapshot {
            engine_blob: body,
            md_seq: Some(42),
            seats: Vec::new(),
        })
        .expect("encode");
        let (decoded, market_seq, seats) = decode_service_snapshot(&wrapped).expect("decode");
        assert_eq!(Some(42), market_seq);
        assert!(seats.is_empty());
        let restored = Engine::restore(config(), &decoded).expect("restore");
        assert_eq!(service.book_hash(), restored.book_hash());

        // A record written before market data was enabled has no market seq.
        let bare = service.snapshot_bytes().expect("bare snapshot");
        let (_, no_market_seq, no_seats) = decode_service_snapshot(&bare).expect("bare");
        assert_eq!(None, no_market_seq);
        assert!(no_seats.is_empty());
    }

    fn bind(
        service: &mut EngineService,
        session: i64,
        seat_id: u16,
        account_id: AccountId,
        last_journal_seq: u64,
    ) {
        service.handle_ingress(
            SessionId(session),
            1,
            Ingress::BindSeat(BindSeat {
                seat_id,
                account_id,
                last_journal_seq,
            }),
        );
    }

    fn seat_command(
        service: &mut EngineService,
        session: i64,
        log_pos: LogPos,
        seat_id: u16,
        epoch: u64,
        command: &Command,
    ) {
        let encoded = encode_command(command);
        service.handle_ingress(
            SessionId(session),
            log_pos,
            Ingress::SeatCommand(SeatCommand {
                seat_id,
                epoch,
                command: &encoded,
            }),
        );
    }

    fn report(entry: &crate::outbox::OutboxEntry) -> ExecutionReportEvent {
        match decode_event(&entry.payload).expect("event") {
            Event::ExecutionReport(report) => report,
            other => panic!("expected an execution report, got {other:?}"),
        }
    }

    #[test]
    fn a_fill_routes_to_both_seat_owners() {
        let mut service = EngineService::new(config());
        bind(&mut service, 10, 1, 1, 0);
        bind(&mut service, 11, 2, 2, 0);
        seat_command(
            &mut service,
            10,
            2,
            1,
            1,
            &limit(1, "S1", Side::Sell, 100, 10),
        );
        seat_command(
            &mut service,
            11,
            3,
            2,
            1,
            &limit(2, "B1", Side::Buy, 100, 4),
        );

        let maker_seat = service.seats.get(&1).expect("maker seat");
        let maker_fills: Vec<_> = maker_seat
            .entries()
            .filter_map(|entry| {
                let report = report(entry);
                (report.exec_type == ExecType::Trade).then_some((entry.liquidity, report))
            })
            .collect();
        assert_eq!(maker_fills.len(), 1);
        assert_eq!(maker_fills[0].0, 1, "maker liquidity");
        assert_eq!(maker_fills[0].1.account, 1);
        assert_eq!(maker_fills[0].1.last_qty, 4);
        assert_eq!(maker_fills[0].1.leaves_qty, 6);

        let taker_seat = service.seats.get(&2).expect("taker seat");
        let taker_fills: Vec<_> = taker_seat
            .entries()
            .filter_map(|entry| {
                let report = report(entry);
                (report.exec_type == ExecType::Trade).then_some((entry.liquidity, report))
            })
            .collect();
        assert_eq!(taker_fills.len(), 1);
        assert_eq!(taker_fills[0].0, 2, "taker liquidity");
        assert_eq!(taker_fills[0].1.account, 2);
        assert_eq!(taker_fills[0].1.last_qty, 4);
    }

    #[test]
    fn an_ack_trims_the_seat_outbox() {
        let mut service = EngineService::new(config());
        bind(&mut service, 10, 1, 1, 0);
        seat_command(
            &mut service,
            10,
            2,
            1,
            1,
            &limit(1, "S1", Side::Sell, 100, 10),
        );
        let next = service.seats.get(&1).expect("seat").next_seq();
        service.handle_ingress(
            SessionId(10),
            3,
            Ingress::AckSeat(AckSeat {
                seat_id: 1,
                epoch: 1,
                up_to_seq: next.saturating_sub(1),
            }),
        );
        let seat = service.seats.get(&1).expect("seat");
        assert_eq!(seat.acked_seq(), next.saturating_sub(1));
        assert_eq!(seat.entry_count(), 0);
        assert!(!seat.needs_resync(next.saturating_sub(1)));
    }

    #[test]
    fn an_overflowed_bind_restates_the_resting_orders() {
        let mut service = EngineService::new(config());
        bind(&mut service, 10, 1, 1, 0);
        seat_command(
            &mut service,
            10,
            2,
            1,
            1,
            &limit(1, "S1", Side::Sell, 100, 10),
        );

        service.seat_sessions.remove(&1);
        {
            let seat = service.seats.get_mut(&1).expect("seat");
            seat.overflow();
        }
        let generation = service.seats.get(&1).expect("seat").generation();
        bind(&mut service, 12, 1, 1, 0);

        let seat = service.seats.get(&1).expect("seat");
        assert_eq!(seat.generation(), generation + 1);
        assert_eq!(seat.entry_count(), 1);
        let entry = seat.entries().next().expect("synthetic report");
        assert!(entry.is_resync());
        let report = report(entry);
        assert_eq!(report.cl_ord_id, cl("S1"));
        assert_eq!(report.price, Some(100));
        assert_eq!(report.order_qty, 10);
        assert_eq!(report.leaves_qty, 10);
        assert_eq!(report.cum_qty, 0);
        assert_eq!(report.exec_type, ExecType::New);
        assert_eq!(report.header.event_seq & RESYNC_EVENT_SEQ, RESYNC_EVENT_SEQ);

        let (target, frame) = service.control_egress.back().expect("seat bound");
        assert_eq!(*target, SessionId(12));
        match Egress::decode(frame).expect("egress") {
            Egress::SeatBound(bound) => {
                assert!(bound.resynced);
                assert_eq!(bound.generation, generation + 1);
                assert_eq!(bound.next_seq, seat.next_seq());
            }
            other => panic!("expected SeatBound, got {other:?}"),
        }
    }

    #[test]
    fn a_new_bind_fences_the_previous_session() {
        let mut service = EngineService::new(config());
        bind(&mut service, 10, 1, 1, 0);
        bind(&mut service, 11, 1, 1, 0);

        let seat = service.seats.get(&1).expect("seat");
        assert_eq!(seat.epoch(), 2);
        assert_eq!(service.seat_sessions.get(&1).copied(), Some(SessionId(11)));

        let mut fenced = false;
        let mut bound = false;
        for (target, frame) in &service.control_egress {
            match Egress::decode(frame).expect("egress") {
                Egress::FenceEvicted(message) => {
                    assert_eq!(*target, SessionId(10));
                    assert_eq!(message.new_epoch, 2);
                    fenced = true;
                }
                Egress::SeatBound(message) if message.epoch == 2 => {
                    assert_eq!(*target, SessionId(11));
                    bound = true;
                }
                _ => {}
            }
        }
        assert!(fenced, "the old session must be fenced");
        assert!(bound, "the new session must get SeatBound");
    }

    #[test]
    fn a_malformed_reject_is_a_service_entry() {
        let mut service = EngineService::new(config());
        bind(&mut service, 10, 1, 1, 0);
        service.handle_ingress(
            SessionId(10),
            2,
            Ingress::RejectMalformed(RejectMalformed {
                seat_id: 1,
                epoch: 1,
                raw_cl_ord_id: 0,
                raw_orig_cl_ord_id: 7,
                is_cancel_or_replace: true,
                reason: 101,
            }),
        );
        let seat = service.seats.get(&1).expect("seat");
        let entry = seat.entries().next().expect("reject");
        assert_eq!(entry.payload_kind, crate::envelope::KIND_MALFORMED_REJECT);
        assert_eq!(entry.liquidity, 0);
        assert_eq!(entry.payload.len(), 18);
        assert_eq!(entry.payload[16], 1);
        assert_eq!(entry.payload[17], 101);
    }

    #[test]
    fn unbinding_keeps_the_outbox() {
        let mut service = EngineService::new(config());
        bind(&mut service, 10, 1, 1, 0);
        seat_command(
            &mut service,
            10,
            2,
            1,
            1,
            &limit(1, "S1", Side::Sell, 100, 10),
        );
        let entries = service.seats.get(&1).expect("seat").entry_count();
        service.handle_ingress(
            SessionId(10),
            3,
            Ingress::UnbindSeat(UnbindSeat {
                seat_id: 1,
                epoch: 1,
                reason: 1,
            }),
        );
        assert!(!service.seat_sessions.contains_key(&1));
        assert_eq!(service.seats.get(&1).expect("seat").entry_count(), entries);
    }

    #[test]
    fn a_seat_cannot_send_a_service_command() {
        let mut service = EngineService::new(config());
        bind(&mut service, 10, 1, 1, 0);
        let next = service.seats.get(&1).expect("seat").next_seq();
        seat_command(&mut service, 10, 2, 1, 1, &Command::Snapshot);
        assert_eq!(service.seats.get(&1).expect("seat").next_seq(), next);
    }

    #[test]
    fn conflicting_pins_are_refused() {
        let mut service = EngineService::new(config());
        bind(&mut service, 10, 1, 1, 0);
        bind(&mut service, 11, 2, 1, 0);
        assert_eq!(service.seats.len(), 1);
        match Egress::decode(&service.control_egress.back().expect("reject").1).expect("egress") {
            Egress::BindRejected(message) => {
                assert_eq!(message.seat_id, 2);
                assert_eq!(message.reason, bind_reject_reason::ACCOUNT_CONFLICT);
            }
            other => panic!("expected BindRejected, got {other:?}"),
        }

        bind(&mut service, 11, 1, 2, 0);
        assert_eq!(
            service.seats.get(&1).expect("seat").account_id(),
            1,
            "the first pin is immutable"
        );
    }

    fn seat_config() -> EngineConfig {
        let mut config = config();
        config.risk.max_open_orders_per_account = 4_096;
        config
    }

    fn registry() -> crate::seats::SeatRegistry {
        crate::seats::SeatRegistry::parse(
            r#"
[[seat]]
seat_id = 1
account_id = 11
protocol = "ouch"
token = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
"#,
        )
        .expect("registry")
    }

    #[test]
    fn the_shared_registry_is_enforced_at_bind() {
        let mut service = EngineService::new(seat_config())
            .with_seats(registry())
            .expect("seats");
        bind(&mut service, 10, 1, 99, 0);
        let (_, frame) = service.control_egress.back().expect("reject");
        match Egress::decode(frame).expect("egress") {
            Egress::BindRejected(message) => {
                assert_eq!(message.reason, bind_reject_reason::ACCOUNT_CONFLICT);
            }
            other => panic!("expected BindRejected, got {other:?}"),
        }
        bind(&mut service, 10, 2, 11, 0);
        let (_, frame) = service.control_egress.back().expect("reject");
        match Egress::decode(frame).expect("egress") {
            Egress::BindRejected(message) => {
                assert_eq!(message.reason, bind_reject_reason::UNKNOWN_SEAT);
            }
            other => panic!("expected BindRejected, got {other:?}"),
        }
        bind(&mut service, 10, 1, 11, 0);
        let (_, frame) = service.control_egress.back().expect("bound");
        assert!(matches!(
            Egress::decode(frame).expect("egress"),
            Egress::SeatBound(_)
        ));
    }

    #[test]
    fn the_multi_seat_risk_cap_is_checked_at_startup() {
        let mut config = config();
        config.risk.max_open_orders_per_account = 4_097;
        assert!(matches!(
            EngineService::new(config).with_seats(registry()),
            Err(ServiceError::Configuration(_))
        ));
    }

    #[test]
    fn seats_round_trip_in_the_dbs3_snapshot() {
        let mut service = EngineService::new(config());
        bind(&mut service, 10, 1, 1, 0);
        seat_command(
            &mut service,
            10,
            2,
            1,
            1,
            &limit(1, "S1", Side::Sell, 100, 10),
        );
        let bytes = service.snapshot_bytes().expect("snapshot");
        assert!(bytes.starts_with(&DBS3_MAGIC));

        let restored = EngineService::from_snapshot(config(), &bytes).expect("restore");
        assert_eq!(service.book_hash(), restored.book_hash());
        assert_eq!(restored.seats.len(), 1);
        assert_eq!(restored.seat_accounts.get(&1), Some(&1));
        assert_eq!(
            restored.seats.get(&1).expect("seat").snapshot(),
            service.seats.get(&1).expect("seat").snapshot()
        );
    }
}
