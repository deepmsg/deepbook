//! The deepmsg `ClusteredService` adapter.
//!
//! It decodes the command record the gateway offered, applies it to
//! [`Engine`] with the log position from the callback header, and offers each
//! engine event back to the originating cluster session. `on_take_snapshot`
//! writes the SBE snapshot record (`DBS2`) in as many records as the snapshot
//! publication's maximum payload needs; `on_start` concatenates them and
//! restores whichever format the record carries (`DBS1` or `DBS2`; ADR 0005).

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex};

use deepbook_engine_core::wire::{decode_command, encode_event};
use deepbook_engine_core::{Engine, EngineConfig, EngineSnapshot, Event};
use deepbook_marketdata::{
    DeepmsgConfig, DeepmsgIncrementalPublisher, Level, MarketDataService,
    ServiceConfig as MarketServiceConfig, Snapshot,
};
use deepbook_types::{LogPos, Seq};
use deepmsg_cluster_service::service::{SnapshotReader, SnapshotWriter};
use deepmsg_cluster_service::{
    Cluster, ClusteredService, ContainerError, LogHeader, Role, SessionId,
};
use deepmsg_codec_cluster::close_reason::CloseReason;

use crate::snapshot::{CodecError, decode, encode};

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
}

impl fmt::Display for ServiceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Wire(error) => write!(formatter, "{error}"),
            Self::Codec(error) => write!(formatter, "{error}"),
            Self::Snapshot(error) => write!(formatter, "{error}"),
            Self::MarketData(detail) => write!(formatter, "market data: {detail}"),
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
}

/// The most events queued for one cluster session. A gateway that stays
/// back-pressured for this long is not consuming; dropping the oldest keeps
/// the service bounded and is logged. The bound is the container's default
/// slab capacity, so one end-of-day expiry (one cancel report and one book
/// update per order) fits.
const MAX_PENDING_EGRESS: usize = 65_536;

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
        }
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
        })
    }

    /// Restore from a snapshot record (`DBS1` or SBE `DBS2`).
    ///
    /// # Errors
    ///
    /// The record is corrupt or does not fit the configuration.
    pub fn from_snapshot(config: EngineConfig, bytes: &[u8]) -> Result<Self, ServiceError> {
        let (snapshot, _) = decode_service_snapshot(bytes)?;
        let engine = Engine::restore(config, &snapshot).map_err(ServiceError::Snapshot)?;
        Ok(Self {
            config,
            engine,
            market_data: None,
            restored_log_pos: snapshot.last_log_pos,
            pending_egress: HashMap::new(),
        })
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
        let body = encode(&self.engine.snapshot()).map_err(ServiceError::Codec)?;
        Ok(encode_service_snapshot(
            body,
            self.market_data
                .as_ref()
                .map(|market| market.service.last_md_seq()),
        ))
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
        Ok(events)
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
    fn flush_egress(&mut self, cluster: &mut Cluster<'_>) {
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
}

impl ClusteredService for EngineService {
    fn on_start(
        &mut self,
        cluster: &mut Cluster<'_>,
        snapshot: Option<&mut SnapshotReader<'_>>,
    ) -> Result<(), ContainerError> {
        if let Some(reader) = snapshot {
            let bytes = read_snapshot(cluster, reader)?;
            let (decoded, market_seq) = decode_service_snapshot(&bytes).map_err(container)?;
            self.engine = Engine::restore(self.config, &decoded).map_err(container)?;
            self.restored_log_pos = decoded.last_log_pos;
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
        Ok(())
    }

    fn on_session_open(
        &mut self,
        _cluster: &mut Cluster<'_>,
        _session: SessionId,
        _timestamp: i64,
    ) -> Result<(), ContainerError> {
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
        Ok(())
    }

    fn on_session_message(
        &mut self,
        cluster: &mut Cluster<'_>,
        session: Option<SessionId>,
        _timestamp: i64,
        payload: &[u8],
        header: &LogHeader,
    ) -> Result<(), ContainerError> {
        let events = self.apply(payload, header.position).map_err(container)?;
        self.publish_market_data(&events);
        if let Some(session) = session {
            self.queue_egress(session, &events);
        }
        self.flush_egress(cluster);
        Ok(())
    }

    fn on_timer_event(
        &mut self,
        _cluster: &mut Cluster<'_>,
        _correlation_id: i64,
        _timestamp: i64,
    ) -> Result<(), ContainerError> {
        Ok(())
    }

    fn on_take_snapshot(&mut self, writer: &mut SnapshotWriter<'_>) -> Result<(), ContainerError> {
        let bytes = self.snapshot_bytes().map_err(container)?;
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

    fn on_role_change(&mut self, _new_role: Role) -> Result<(), ContainerError> {
        Ok(())
    }

    fn on_terminate(&mut self, _cluster: &mut Cluster<'_>) -> Result<(), ContainerError> {
        Ok(())
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
/// the prefix (a snapshot written before market data was enabled) still
/// decodes: the prefix is additive.
fn encode_service_snapshot(body: Vec<u8>, market_seq: Option<Seq>) -> Vec<u8> {
    let Some(market_seq) = market_seq else {
        return body;
    };
    let mut out = Vec::with_capacity(MARKET_PREFIX + body.len());
    out.extend_from_slice(&MARKET_MAGIC);
    out.push(MARKET_VERSION);
    out.extend_from_slice(&market_seq.to_le_bytes());
    out.extend_from_slice(&body);
    out
}

fn decode_service_snapshot(bytes: &[u8]) -> Result<(EngineSnapshot, Option<Seq>), ServiceError> {
    if !bytes.starts_with(&MARKET_MAGIC) {
        return Ok((decode(bytes).map_err(ServiceError::Codec)?, None));
    }
    if bytes.len() < MARKET_PREFIX {
        return Err(ServiceError::Codec(CodecError::Truncated));
    }
    if bytes[4] != MARKET_VERSION {
        return Err(ServiceError::Codec(CodecError::BadVersion(bytes[4])));
    }
    let mut seq = [0_u8; 8];
    seq.copy_from_slice(&bytes[5..MARKET_PREFIX]);
    let snapshot = decode(&bytes[MARKET_PREFIX..]).map_err(ServiceError::Codec)?;
    Ok((snapshot, Some(Seq::from_le_bytes(seq))))
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
fn container(error: impl fmt::Display) -> ContainerError {
    ContainerError::Cluster(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use deepbook_engine_core::ClientOrderId;
    use deepbook_engine_core::wire::{encode_command, encode_event};
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
        let wrapped = encode_service_snapshot(body, Some(42));
        let (decoded, market_seq) = decode_service_snapshot(&wrapped).expect("decode");
        assert_eq!(Some(42), market_seq);
        let restored = Engine::restore(config(), &decoded).expect("restore");
        assert_eq!(service.book_hash(), restored.book_hash());

        // A record written before market data was enabled has no prefix.
        let bare = service.snapshot_bytes().expect("bare snapshot");
        let (_, no_market_seq) = decode_service_snapshot(&bare).expect("bare");
        assert_eq!(None, no_market_seq);
    }
}
