//! The M4 cluster client behind the gateway's [`CommandSink`]/[`EventSource`]
//! seam.
//!
//! One [`ClusterEngine`] owns a deepmsg [`Client`], one cluster session and the
//! FIX-side projection it needs to answer cancel/status without a second
//! cluster round trip:
//!
//! ```text
//! FIX ApplicationMessage ──map──▶ engine Command ──offer──▶ cluster ingress
//! FIX ExecutionReport    ◀─map── engine Event   ◀─egress── cluster session
//! ```
//!
//! The engine's own records are the ones `engine-core::wire` defines; this
//! module never invents a second encoding. The cluster session survives a
//! leader change inside `AeronCluster::poll_egress`; a closed session is
//! reconnected with a fresh connect. Commands that cannot be offered yet stay
//! queued in order, which is the design's "M4 queues while the cluster is
//! away" answer.
//!
//! # Limits of the M4 adapter
//!
//! - **One FIX session.** The acceptor hosts one `SenderCompID`/`TargetCompID`
//!   pair, so one cluster session carries its commands. Multiple peers need a
//!   per-peer cluster session and an event-to-session route; the engine's
//!   egress record has no FIX session field yet.
//! - **Numeric accounts.** FIX `1 Account` is a string and the engine's account
//!   is a `u32`. In cluster mode the string must be the decimal id (a missing
//!   account is `0`); a non-numeric account is refused instead of being hashed,
//!   because a hash would not survive a gateway restart with the same meaning.
//! - **Local projection.** A cancel that names no `Account` uses the account
//!   this gateway sent the order with. After a gateway restart that map is
//!   empty, so such a cancel is refused locally; a client that sends `1` on the
//!   cancel does not depend on the map.
//! - **Status is a cash answer.** `OrderStatusRequest` has no engine command;
//!   it is answered from the last ExecutionReport this gateway saw. After a
//!   restart an order the gateway has not seen again answers `unknown order`.

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::{Duration, Instant};

use deepbook_engine_core::{
    CancelOrder, ClientOrderId, Command, Event, ExecType as EngineExecType, ExecutionReportEvent,
    NewOrder, OrdType as EngineOrdType, ReplaceOrder, TimeInForce as EngineTimeInForce,
    decode_event, encode_command,
};
use deepbook_fix::clock::Clock;
use deepbook_fix::{
    ApplicationMessage, CxlRejResponseTo, ExecType, ExecutionReport, NewOrderSingle, OrdStatus,
    OrderCancelReject, OrderCancelReplaceRequest, OrderCancelRequest, OrderStatusRequest,
    SystemClock, format_utc,
};
use deepbook_types::{AccountId, EMPTY_ORDER_ID, OrderId, OrderState, Price, Qty, Side, SymbolId};
use deepmsg_client::client::Client;
use deepmsg_client::fragment_assembler::MessageHeader;
use deepmsg_cluster_client::{AeronCluster, AeronClusterContext, EgressListener};
use deepmsg_core::logbuffer::append::Appended;

use crate::sink::{CommandSink, EventSource, SessionEvent, SessionKey};

/// How many commands may wait for the cluster before new ones are refused.
const MAX_PENDING_COMMANDS: usize = 65_536;
/// How often the session is kept alive; the cluster's session timeout is 10 s.
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(3);
/// How long to wait before a closed session is connected again.
const RECONNECT_DELAY: Duration = Duration::from_secs(1);
/// A cluster-client or mapping failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClusterEngineError {
    /// The media driver could not be reached.
    Driver(String),
    /// The cluster connect failed.
    Connect(String),
    /// The FIX session key cannot be used for egress.
    Configuration(String),
}

impl fmt::Display for ClusterEngineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Driver(message) => write!(formatter, "media driver: {message}"),
            Self::Connect(message) => write!(formatter, "cluster connect: {message}"),
            Self::Configuration(message) => write!(formatter, "configuration: {message}"),
        }
    }
}

impl std::error::Error for ClusterEngineError {}

/// What the gateway's cluster mode needs to know.
#[derive(Clone, Debug)]
pub struct ClusterEngineConfig {
    /// The aeron directory of the driver the cluster node uses.
    pub aeron_dir: PathBuf,
    /// `aeron.cluster.ingress.channel`.
    pub ingress_channel: String,
    /// `<memberId>=<endpoint>,…` for a multi-member cluster whose members own
    /// their drivers; `None` for an IPC or self-naming channel (single node).
    pub ingress_endpoints: Option<String>,
    /// `aeron.cluster.ingress.stream.id`.
    pub ingress_stream_id: i32,
    /// `aeron.cluster.egress.channel`.
    ///
    /// On a UDP channel name the endpoint: the gateway's driver is its own on a
    /// multi-member cluster, and a session that shares an endpoint with an
    /// older one cannot rejoin after a failover (see
    /// [`ClusterEngine::reconnect`]).
    pub egress_channel: String,
    /// `aeron.cluster.egress.stream.id`.
    pub egress_stream_id: i32,
    /// The connect's `aeron.cluster.message.timeout`.
    pub message_timeout_ns: i64,
    /// `newLeaderTimeoutNs`; `None` lets the connect derive it.
    pub new_leader_timeout_ns: Option<i64>,
    /// The engine's symbol id, sent in every command.
    pub symbol: SymbolId,
    /// The FIX `55` the engine's symbol is known by.
    pub symbol_name: String,
    /// The one FIX session this gateway hosts.
    pub session: SessionKey,
}

impl ClusterEngineConfig {
    /// The single-node defaults: IPC both ways, stream 101/102, symbol 1
    /// known as `AAPL`.
    #[must_use]
    pub fn new(aeron_dir: impl Into<PathBuf>, session: SessionKey) -> Self {
        Self {
            aeron_dir: aeron_dir.into(),
            ingress_channel: "aeron:ipc".to_owned(),
            ingress_endpoints: None,
            ingress_stream_id: 101,
            egress_channel: "aeron:ipc".to_owned(),
            egress_stream_id: 102,
            message_timeout_ns: 5_000_000_000,
            new_leader_timeout_ns: None,
            symbol: 1,
            symbol_name: "AAPL".to_owned(),
            session,
        }
    }
}

/// What the egress listener stores; the payload is decoded on the gateway's
/// thread, never in the listener.
struct EgressRecord {
    timestamp: i64,
    payload: Vec<u8>,
}

/// The listener a cluster session calls; it only copies into the shared queue.
struct EgressQueue(Rc<RefCell<VecDeque<EgressRecord>>>);

impl EgressListener for EgressQueue {
    fn on_message(
        &mut self,
        _cluster_session_id: i64,
        timestamp: i64,
        payload: &[u8],
        _header: &MessageHeader,
    ) {
        self.0.borrow_mut().push_back(EgressRecord {
            timestamp,
            payload: payload.to_vec(),
        });
    }
}

/// The FIX-side state a cluster event cannot answer by itself.
#[derive(Clone, Debug)]
struct OrderProjection {
    account: AccountId,
    symbol: String,
    side: Side,
    order_qty: Qty,
    cum_qty: Qty,
    avg_px: Price,
    leaves_qty: Qty,
    status: OrdStatus,
    order_id: String,
}

/// The pure FIX ↔ engine mapping, kept separate so it is testable without a
/// driver or a cluster.
struct Mapping {
    symbol: SymbolId,
    symbol_name: String,
    /// The last state each client order id was reported in.
    orders: HashMap<String, OrderProjection>,
    /// The FIX spelling of each account id, for the reports.
    account_names: HashMap<AccountId, String>,
}

impl Mapping {
    fn new(symbol: SymbolId, symbol_name: &str) -> Self {
        Self {
            symbol,
            symbol_name: symbol_name.to_owned(),
            orders: HashMap::new(),
            account_names: HashMap::new(),
        }
    }

    fn account_name(&self, account: AccountId) -> Option<String> {
        self.account_names
            .get(&account)
            .cloned()
            .or_else(|| Some(account.to_string()))
    }

    fn new_order(&mut self, order: &NewOrderSingle) -> Result<Command, String> {
        let cl_ord_id = client_order_id(&order.cl_ord_id)?;
        let (account, name) = parse_account(order.account.as_deref())?;
        if order.symbol != self.symbol_name {
            return Err(format!("unknown symbol '{}'", order.symbol));
        }
        self.account_names.insert(account, name.clone());
        self.orders.insert(
            order.cl_ord_id.clone(),
            OrderProjection {
                account,
                symbol: order.symbol.clone(),
                side: order.side,
                order_qty: order.order_qty,
                cum_qty: 0,
                avg_px: 0,
                leaves_qty: order.order_qty,
                status: OrdStatus::New,
                order_id: "NONE".to_owned(),
            },
        );
        Ok(Command::NewOrder(NewOrder {
            symbol: self.symbol,
            account,
            cl_ord_id,
            side: order.side,
            price: order.price,
            qty: order.order_qty,
            ord_type: ord_type(order.ord_type),
            time_in_force: time_in_force(order.time_in_force),
            post_only: false,
        }))
    }

    fn cancel(&mut self, request: &OrderCancelRequest) -> Result<Command, String> {
        let cl_ord_id = client_order_id(&request.cl_ord_id)?;
        let orig_cl_ord_id = client_order_id(&request.orig_cl_ord_id)?;
        let account = self
            .orders
            .get(&request.orig_cl_ord_id)
            .map(|order| order.account)
            .ok_or_else(|| format!("unknown order '{}'", request.orig_cl_ord_id))?;
        Ok(Command::CancelOrder(CancelOrder {
            account,
            cl_ord_id,
            orig_cl_ord_id,
        }))
    }

    fn replace(&mut self, request: &OrderCancelReplaceRequest) -> Result<Command, String> {
        let cl_ord_id = client_order_id(&request.cl_ord_id)?;
        let orig_cl_ord_id = client_order_id(&request.orig_cl_ord_id)?;
        let (account, name) = match request.account.as_deref() {
            Some(value) => parse_account(Some(value))?,
            None => {
                let account = self
                    .orders
                    .get(&request.orig_cl_ord_id)
                    .map(|order| order.account)
                    .ok_or_else(|| format!("unknown order '{}'", request.orig_cl_ord_id))?;
                (account, account.to_string())
            }
        };
        if request.symbol != self.symbol_name {
            return Err(format!("unknown symbol '{}'", request.symbol));
        }
        self.account_names.insert(account, name);
        Ok(Command::ReplaceOrder(ReplaceOrder {
            account,
            cl_ord_id,
            orig_cl_ord_id,
            price: request.price,
            qty: request.order_qty,
            ord_type: ord_type(request.ord_type),
            time_in_force: time_in_force(request.time_in_force),
            post_only: false,
        }))
    }

    fn report(
        &mut self,
        event: &ExecutionReportEvent,
        timestamp_millis: Option<i64>,
    ) -> ExecutionReport {
        let order_id = order_id_string(event.order_id);
        let symbol = if event.symbol == self.symbol {
            self.symbol_name.clone()
        } else {
            event.symbol.to_string()
        };
        let status = ord_status(event.state);
        let account = self.account_name(event.account);
        let cl_ord_id = event.cl_ord_id.to_string();
        self.orders.insert(
            cl_ord_id.clone(),
            OrderProjection {
                account: event.account,
                symbol: symbol.clone(),
                side: event.side,
                order_qty: event.order_qty,
                cum_qty: event.cum_qty,
                avg_px: event.avg_px,
                leaves_qty: event.leaves_qty,
                status,
                order_id: order_id.clone(),
            },
        );
        ExecutionReport {
            exec_id: exec_id(event.header.log_pos, event.header.event_seq),
            order_id,
            cl_ord_id: cl_ord_id.clone(),
            exec_type: exec_type(event.exec_type),
            ord_status: status,
            symbol,
            side: event.side,
            order_qty: event.order_qty,
            last_qty: (event.last_qty > 0).then_some(event.last_qty),
            last_px: event.last_px,
            leaves_qty: event.leaves_qty,
            cum_qty: event.cum_qty,
            avg_px: event.avg_px,
            account,
            transact_time: timestamp_millis.map(format_utc),
            text: event.reason.map(|reason| reason.as_str().to_owned()),
        }
    }

    fn cancel_reject(
        &mut self,
        event: &deepbook_engine_core::CancelRejectEvent,
    ) -> OrderCancelReject {
        OrderCancelReject {
            order_id: event
                .order_id
                .map_or_else(|| "NONE".to_owned(), order_id_string),
            cl_ord_id: event.cl_ord_id.to_string(),
            orig_cl_ord_id: event.orig_cl_ord_id.to_string(),
            ord_status: ord_status(event.state),
            cxl_rej_response_to: if event.is_replace {
                CxlRejResponseTo::OrderCancelReplaceRequest
            } else {
                CxlRejResponseTo::OrderCancelRequest
            },
            cxl_rej_reason: Some(cxl_rej_reason(event.reason)),
            text: Some(event.reason.as_str().to_owned()),
        }
    }

    fn status(&self, request: &OrderStatusRequest, exec_id: String, now: i64) -> ExecutionReport {
        let Some(order) = self.orders.get(&request.cl_ord_id) else {
            return ExecutionReport {
                exec_id,
                order_id: "NONE".to_owned(),
                cl_ord_id: request.cl_ord_id.clone(),
                exec_type: ExecType::Rejected,
                ord_status: OrdStatus::Rejected,
                symbol: request.symbol.clone(),
                side: request.side,
                order_qty: 0,
                last_qty: None,
                last_px: None,
                leaves_qty: 0,
                cum_qty: 0,
                avg_px: 0,
                account: None,
                transact_time: Some(format_utc(now)),
                text: Some("unknown order".to_owned()),
            };
        };
        ExecutionReport {
            exec_id,
            order_id: order.order_id.clone(),
            cl_ord_id: request.cl_ord_id.clone(),
            exec_type: ExecType::OrderStatus,
            ord_status: order.status,
            symbol: order.symbol.clone(),
            side: order.side,
            order_qty: order.order_qty,
            last_qty: None,
            last_px: None,
            leaves_qty: order.leaves_qty,
            cum_qty: order.cum_qty,
            avg_px: order.avg_px,
            account: self.account_name(order.account),
            transact_time: Some(format_utc(now)),
            text: None,
        }
    }
}

/// One command waiting for cluster ingress.
struct PendingCommand {
    payload: Vec<u8>,
}

/// The cluster-backed engine seam.
pub struct ClusterEngine {
    config: ClusterEngineConfig,
    client: Client,
    session: Option<AeronCluster>,
    egress: Rc<RefCell<VecDeque<EgressRecord>>>,
    events: VecDeque<SessionEvent>,
    commands: VecDeque<PendingCommand>,
    mapping: Mapping,
    next_local_exec: u64,
    /// Cluster sessions this engine has opened, including the first.
    ///
    /// The multi-member egress endpoint moves by one port per session: a fresh
    /// session that names the endpoint of a session the cluster has not
    /// finished reaping cannot join the new leader's publication.
    sessions_opened: u16,
    last_keep_alive: Instant,
    reconnect_at: Option<Instant>,
}

impl fmt::Debug for ClusterEngine {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClusterEngine")
            .field("cluster_session_id", &self.cluster_session_id())
            .field("pending_commands", &self.commands.len())
            .finish_non_exhaustive()
    }
}

impl ClusterEngine {
    /// Connect the media driver and open a cluster session.
    ///
    /// # Errors
    ///
    /// The driver cannot be reached, or the cluster does not answer a connect
    /// within the configured message timeout.
    pub fn connect(config: ClusterEngineConfig) -> Result<Self, ClusterEngineError> {
        let mut client = Client::connect(&config.aeron_dir)
            .map_err(|error| ClusterEngineError::Driver(error.to_string()))?;
        let egress = Rc::new(RefCell::new(VecDeque::new()));
        let session = open_session(&config, 0, &egress, &mut client)?;
        let mapping = Mapping::new(config.symbol, &config.symbol_name);
        Ok(Self {
            config,
            client,
            session: Some(session),
            egress,
            events: VecDeque::new(),
            commands: VecDeque::new(),
            mapping,
            next_local_exec: 0,
            sessions_opened: 1,
            last_keep_alive: Instant::now(),
            reconnect_at: None,
        })
    }

    /// The cluster-assigned session id, while connected.
    #[must_use]
    pub fn cluster_session_id(&self) -> Option<i64> {
        self.session.as_ref().map(AeronCluster::cluster_session_id)
    }

    /// How many commands are waiting for ingress.
    #[must_use]
    pub fn pending_commands(&self) -> usize {
        self.commands.len()
    }

    /// The FIX session key this engine answers for.
    #[must_use]
    pub fn session_key(&self) -> &SessionKey {
        &self.config.session
    }

    /// Give the client a turn: poll the driver, take egress, offer what is
    /// queued, keep the session alive, and connect again if it closed.
    fn pump(&mut self) {
        let _ = self.client.poll();
        if let Some(session) = self.session.as_mut() {
            if let Err(error) = session.poll_egress(&mut self.client) {
                eprintln!("fix-gateway: cluster egress: {error}");
            }
            if session.is_closed() {
                eprintln!("fix-gateway: cluster session closed");
                self.session = None;
                self.reconnect_at = Some(Instant::now() + RECONNECT_DELAY);
            } else if self.last_keep_alive.elapsed() >= KEEP_ALIVE_INTERVAL {
                session.send_keep_alive(&mut self.client);
                self.last_keep_alive = Instant::now();
            }
        }
        self.drain_egress();

        if self.session.is_none() {
            self.reconnect();
        } else {
            self.flush_commands();
        }
    }

    fn reconnect(&mut self) {
        let now = Instant::now();
        if self.reconnect_at.is_some_and(|at| now < at) {
            return;
        }
        // The offset is the number of sessions already open, so a reconnect
        // never shares an endpoint with its predecessor. It only moves a UDP
        // endpoint; an IPC channel is returned unchanged.
        match open_session(
            &self.config,
            self.sessions_opened,
            &self.egress,
            &mut self.client,
        ) {
            Ok(session) => {
                eprintln!(
                    "fix-gateway: cluster reconnected session={} egress={}",
                    session.cluster_session_id(),
                    egress_channel_for(&self.config.egress_channel, self.sessions_opened)
                );
                self.sessions_opened = self.sessions_opened.saturating_add(1);
                self.session = Some(session);
                self.reconnect_at = None;
                self.last_keep_alive = now;
            }
            Err(error) => {
                eprintln!("fix-gateway: cluster reconnect failed: {error}");
                self.reconnect_at = Some(now + RECONNECT_DELAY);
            }
        }
    }

    fn drain_egress(&mut self) {
        loop {
            let record = self.egress.borrow_mut().pop_front();
            let Some(record) = record else {
                break;
            };
            match decode_event(&record.payload) {
                Ok(event) => self.on_event(event, record.timestamp),
                Err(error) => eprintln!("fix-gateway: cluster event: {error}"),
            }
        }
    }

    fn on_event(&mut self, event: Event, timestamp_ns: i64) {
        // The cluster stamps the session message in nanoseconds; a client
        // that sees none (an old record) still owes FIX a TransactTime.
        let timestamp_millis = Some(if timestamp_ns > 0 {
            timestamp_ns / 1_000_000
        } else {
            SystemClock.now_millis()
        });
        match event {
            Event::ExecutionReport(report) => {
                let message = self.mapping.report(&report, timestamp_millis);
                self.push_application(ApplicationMessage::ExecutionReport(message));
            }
            Event::CancelReject(reject) => {
                let message = self.mapping.cancel_reject(&reject);
                self.push_application(ApplicationMessage::OrderCancelReject(message));
            }
            // Trades already travel as the two ExecutionReports; the book
            // stream is market data and snapshot markers are cluster state.
            Event::Trade(_) | Event::BookUpdate(_) | Event::Snapshot(_) => {}
        }
    }

    fn flush_commands(&mut self) {
        let Some(session) = self.session.as_mut() else {
            return;
        };
        while let Some(command) = self.commands.front() {
            match session.offer(&mut self.client, &command.payload) {
                Some(Appended::Ok { .. }) => {
                    self.commands.pop_front();
                }
                Some(
                    Appended::EndOfLog
                    | Appended::MidRotation
                    | Appended::BackPressured
                    | Appended::NotConnected,
                )
                // `None` is no ingress publication: the session released it
                // while it waits for a new leader. The command stays queued;
                // dropping it here would lose a FIX command the peer believes
                // was accepted.
                | None => break,
                Some(Appended::MaxPositionExceeded)
                | Some(Appended::MessageTooLarge)
                | Some(Appended::Malformed) => {
                    let command = self
                        .commands
                        .pop_front()
                        .expect("the command just observed");
                    eprintln!(
                        "fix-gateway: dropping a {} byte command the cluster refused",
                        command.payload.len()
                    );
                }
            }
        }
    }

    fn offer(&mut self, command: &Command) -> bool {
        if self.commands.len() >= MAX_PENDING_COMMANDS {
            return false;
        }
        self.commands.push_back(PendingCommand {
            payload: encode_command(command),
        });
        self.pump();
        true
    }

    fn push_application(&mut self, message: ApplicationMessage) {
        self.events.push_back(SessionEvent {
            session: self.config.session.clone(),
            message,
        });
    }

    fn push_for(&mut self, session: &SessionKey, message: ApplicationMessage) {
        self.events.push_back(SessionEvent {
            session: session.clone(),
            message,
        });
    }

    fn local_exec_id(&mut self) -> String {
        self.next_local_exec += 1;
        format!("GW-{}", self.next_local_exec)
    }

    fn reject_new(&mut self, session: &SessionKey, order: &NewOrderSingle, text: String) {
        let exec_id = self.local_exec_id();
        let now = format_utc(SystemClock.now_millis());
        let report = ExecutionReport {
            exec_id,
            order_id: "NONE".to_owned(),
            cl_ord_id: order.cl_ord_id.clone(),
            exec_type: ExecType::Rejected,
            ord_status: OrdStatus::Rejected,
            symbol: order.symbol.clone(),
            side: order.side,
            order_qty: order.order_qty,
            last_qty: None,
            last_px: None,
            leaves_qty: 0,
            cum_qty: 0,
            avg_px: 0,
            account: order.account.clone(),
            transact_time: Some(now),
            text: Some(text),
        };
        self.push_for(session, ApplicationMessage::ExecutionReport(report));
    }

    fn reject_cancel(
        &mut self,
        session: &SessionKey,
        cl_ord_id: &str,
        orig_cl_ord_id: &str,
        is_replace: bool,
        text: String,
    ) {
        let reject = OrderCancelReject {
            order_id: "NONE".to_owned(),
            cl_ord_id: cl_ord_id.to_owned(),
            orig_cl_ord_id: orig_cl_ord_id.to_owned(),
            ord_status: OrdStatus::Rejected,
            cxl_rej_response_to: if is_replace {
                CxlRejResponseTo::OrderCancelReplaceRequest
            } else {
                CxlRejResponseTo::OrderCancelRequest
            },
            cxl_rej_reason: Some(99),
            text: Some(text),
        };
        self.push_for(session, ApplicationMessage::OrderCancelReject(reject));
    }
}

impl CommandSink for ClusterEngine {
    fn deliver(&mut self, session: &SessionKey, message: ApplicationMessage) {
        match message {
            ApplicationMessage::NewOrderSingle(order) => match self.mapping.new_order(&order) {
                Ok(command) => {
                    if !self.offer(&command) {
                        self.reject_new(
                            session,
                            &order,
                            "cluster ingress queue is full".to_owned(),
                        );
                    }
                }
                Err(text) => self.reject_new(session, &order, text),
            },
            ApplicationMessage::OrderCancelRequest(request) => {
                match self.mapping.cancel(&request) {
                    Ok(command) => {
                        if !self.offer(&command) {
                            self.reject_cancel(
                                session,
                                &request.cl_ord_id,
                                &request.orig_cl_ord_id,
                                false,
                                "cluster ingress queue is full".to_owned(),
                            );
                        }
                    }
                    Err(text) => self.reject_cancel(
                        session,
                        &request.cl_ord_id,
                        &request.orig_cl_ord_id,
                        false,
                        text,
                    ),
                }
            }
            ApplicationMessage::OrderCancelReplaceRequest(request) => {
                match self.mapping.replace(&request) {
                    Ok(command) => {
                        if !self.offer(&command) {
                            self.reject_cancel(
                                session,
                                &request.cl_ord_id,
                                &request.orig_cl_ord_id,
                                true,
                                "cluster ingress queue is full".to_owned(),
                            );
                        }
                    }
                    Err(text) => self.reject_cancel(
                        session,
                        &request.cl_ord_id,
                        &request.orig_cl_ord_id,
                        true,
                        text,
                    ),
                }
            }
            ApplicationMessage::OrderStatusRequest(request) => {
                let exec_id = self.local_exec_id();
                let now = SystemClock.now_millis();
                let message = self.mapping.status(&request, exec_id, now);
                self.push_for(session, ApplicationMessage::ExecutionReport(message));
            }
            // Reports travel the other way; a peer that sends one is ignored.
            ApplicationMessage::ExecutionReport(_) | ApplicationMessage::OrderCancelReject(_) => {}
        }
    }
}

impl EventSource for ClusterEngine {
    fn poll_event(&mut self) -> Option<SessionEvent> {
        if let Some(event) = self.events.pop_front() {
            return Some(event);
        }
        self.pump();
        self.events.pop_front()
    }
}

fn open_session(
    config: &ClusterEngineConfig,
    egress_offset: u16,
    egress: &Rc<RefCell<VecDeque<EgressRecord>>>,
    client: &mut Client,
) -> Result<AeronCluster, ClusterEngineError> {
    let egress_channel = egress_channel_for(&config.egress_channel, egress_offset);
    let context = AeronClusterContext::new()
        .ingress_channel(Some(&config.ingress_channel))
        .ingress_endpoints(config.ingress_endpoints.as_deref())
        .ingress_stream_id(config.ingress_stream_id)
        .egress_channel(Some(&egress_channel))
        .egress_stream_id(config.egress_stream_id)
        .message_timeout_ns(config.message_timeout_ns)
        .new_leader_timeout_ns(config.new_leader_timeout_ns)
        .client_name("deepbook-fix-gateway")
        .egress_listener(Box::new(EgressQueue(Rc::clone(egress))));
    AeronCluster::connect(context, client)
        .map_err(|error| ClusterEngineError::Connect(error.to_string()))
}

/// The egress channel for the `offset`-th session after the first.
///
/// A UDP egress endpoint names the port the gateway's own driver receives on.
/// The cluster keeps a closed session until its timeout, so a fresh session
/// that reuses the endpoint cannot join the new leader's publication; moving
/// the port by one per session is the process-level equivalent of the
/// reference's `Cluster.Role` reconnect, which lets the driver allocate a
/// unique endpoint. IPC and endpoint-less channels come back unchanged.
fn egress_channel_for(channel: &str, offset: u16) -> String {
    if offset == 0 {
        return channel.to_owned();
    }
    let Some(index) = channel.find("endpoint=") else {
        return channel.to_owned();
    };
    let (head, tail) = channel.split_at(index + "endpoint=".len());
    // The endpoint is the last `host:port` in the string; channel parameters
    // after it (`|term-length=…`) are kept verbatim.
    let Some((host, rest)) = tail.rsplit_once(':') else {
        return channel.to_owned();
    };
    let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
    let Ok(port) = digits.parse::<u16>() else {
        return channel.to_owned();
    };
    let Some(port) = port.checked_add(offset) else {
        return channel.to_owned();
    };
    format!("{head}{host}:{port}{}", &rest[digits.len()..])
}

fn parse_account(value: Option<&str>) -> Result<(AccountId, String), String> {
    let Some(text) = value else {
        return Ok((0, "0".to_owned()));
    };
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(format!("account '{text}' is not a numeric id"));
    }
    let account = text
        .parse::<AccountId>()
        .map_err(|_| format!("account '{text}' is outside u32"))?;
    Ok((account, text.to_owned()))
}

fn client_order_id(value: &str) -> Result<ClientOrderId, String> {
    ClientOrderId::new(value.as_bytes()).map_err(|error| error.to_string())
}

fn ord_type(value: deepbook_fix::OrdType) -> EngineOrdType {
    match value {
        deepbook_fix::OrdType::Limit => EngineOrdType::Limit,
        deepbook_fix::OrdType::Market => EngineOrdType::Market,
    }
}

fn time_in_force(value: deepbook_fix::TimeInForce) -> EngineTimeInForce {
    match value {
        deepbook_fix::TimeInForce::Day => EngineTimeInForce::Day,
        deepbook_fix::TimeInForce::GoodTillCancel => EngineTimeInForce::GoodTillCanceled,
        deepbook_fix::TimeInForce::ImmediateOrCancel => EngineTimeInForce::ImmediateOrCancel,
        deepbook_fix::TimeInForce::FillOrKill => EngineTimeInForce::FillOrKill,
    }
}

fn exec_type(value: EngineExecType) -> ExecType {
    match value {
        EngineExecType::New => ExecType::New,
        EngineExecType::Trade => ExecType::Trade,
        EngineExecType::Canceled => ExecType::Canceled,
        EngineExecType::Replaced => ExecType::Replaced,
        EngineExecType::Rejected => ExecType::Rejected,
    }
}

fn ord_status(value: OrderState) -> OrdStatus {
    match value {
        OrderState::New => OrdStatus::New,
        OrderState::Partial => OrdStatus::PartiallyFilled,
        OrderState::Filled => OrdStatus::Filled,
        OrderState::Canceled => OrdStatus::Canceled,
        OrderState::Rejected => OrdStatus::Rejected,
    }
}

fn cxl_rej_reason(value: deepbook_engine_core::RejectReason) -> u32 {
    use deepbook_engine_core::RejectReason;
    match value {
        RejectReason::UnknownOrder => 1,
        RejectReason::OrderNotOpen => 0,
        RejectReason::DuplicateClOrdId => 6,
        _ => 99,
    }
}

fn order_id_string(value: OrderId) -> String {
    if value == EMPTY_ORDER_ID {
        "NONE".to_owned()
    } else {
        value.to_string()
    }
}

fn exec_id(log_pos: i64, event_seq: deepbook_engine_core::EventSeq) -> String {
    format!("{log_pos}-{event_seq}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use deepbook_engine_core::{EventHeader, ExecutionReportEvent, RejectReason};
    use deepbook_fix::{OrdType as FixOrdType, TimeInForce as FixTimeInForce};

    fn order() -> NewOrderSingle {
        NewOrderSingle {
            cl_ord_id: "C-1".to_owned(),
            account: Some("7".to_owned()),
            symbol: "AAPL".to_owned(),
            side: Side::Buy,
            order_qty: 100,
            ord_type: FixOrdType::Limit,
            price: Some(10_025),
            time_in_force: FixTimeInForce::Day,
            transact_time: "20261025-12:34:56.000".to_owned(),
            handl_inst: None,
        }
    }

    fn header() -> EventHeader {
        EventHeader {
            log_pos: 42,
            event_seq: 3,
        }
    }

    #[test]
    fn a_new_order_becomes_the_engine_command() {
        let mut mapping = Mapping::new(1, "AAPL");
        let command = mapping.new_order(&order()).expect("mapped");
        match command {
            Command::NewOrder(new) => {
                assert_eq!(1, new.symbol);
                assert_eq!(7, new.account);
                assert_eq!("C-1", new.cl_ord_id.to_string());
                assert_eq!(Side::Buy, new.side);
                assert_eq!(Some(10_025), new.price);
                assert_eq!(100, new.qty);
                assert_eq!(EngineOrdType::Limit, new.ord_type);
                assert_eq!(EngineTimeInForce::Day, new.time_in_force);
                assert!(!new.post_only);
            }
            other => panic!("expected NewOrder, got {other:?}"),
        }
    }

    #[test]
    fn a_foreign_symbol_and_a_non_numeric_account_are_refused() {
        let mut mapping = Mapping::new(1, "AAPL");
        let mut foreign = order();
        foreign.symbol = "MSFT".to_owned();
        assert!(mapping.new_order(&foreign).is_err());

        let mut bad_account = order();
        bad_account.cl_ord_id = "C-2".to_owned();
        bad_account.account = Some("A-1".to_owned());
        assert!(mapping.new_order(&bad_account).is_err());
    }

    #[test]
    fn a_report_carries_the_packed_order_id_and_the_event_id() {
        let mut mapping = Mapping::new(1, "AAPL");
        mapping.new_order(&order()).expect("mapped");
        let report = mapping.report(
            &ExecutionReportEvent {
                header: header(),
                order_id: 0x0100_0002,
                cl_ord_id: "C-1".parse().expect("id"),
                account: 7,
                symbol: 1,
                side: Side::Buy,
                exec_type: EngineExecType::New,
                state: OrderState::New,
                price: Some(10_025),
                order_qty: 100,
                last_qty: 0,
                last_px: None,
                leaves_qty: 100,
                cum_qty: 0,
                avg_px: 0,
                reason: None,
            },
            Some(1_790_000_000_000),
        );
        assert_eq!("42-3", report.exec_id);
        assert_eq!("16777218", report.order_id);
        assert_eq!("C-1", report.cl_ord_id);
        assert_eq!(ExecType::New, report.exec_type);
        assert_eq!(OrdStatus::New, report.ord_status);
        assert_eq!("AAPL", report.symbol);
        assert_eq!(Some("7".to_owned()), report.account);
        assert_eq!(None, report.last_qty);
        assert!(report.transact_time.is_some());
    }

    #[test]
    fn a_rejected_unallocated_order_has_no_order_id() {
        let mut mapping = Mapping::new(1, "AAPL");
        let report = mapping.report(
            &ExecutionReportEvent {
                header: header(),
                order_id: EMPTY_ORDER_ID,
                cl_ord_id: "C-9".parse().expect("id"),
                account: 0,
                symbol: 1,
                side: Side::Sell,
                exec_type: EngineExecType::Rejected,
                state: OrderState::Rejected,
                price: None,
                order_qty: 10,
                last_qty: 0,
                last_px: None,
                leaves_qty: 0,
                cum_qty: 0,
                avg_px: 0,
                reason: Some(RejectReason::BookFull),
            },
            None,
        );
        assert_eq!("NONE", report.order_id);
        assert_eq!(ExecType::Rejected, report.exec_type);
        assert_eq!(Some("BookFull".to_owned()), report.text);
    }

    #[test]
    fn a_trade_report_carries_last_qty_and_price() {
        let mut mapping = Mapping::new(1, "AAPL");
        let report = mapping.report(
            &ExecutionReportEvent {
                header: header(),
                order_id: 2,
                cl_ord_id: "C-2".parse().expect("id"),
                account: 7,
                symbol: 1,
                side: Side::Sell,
                exec_type: EngineExecType::Trade,
                state: OrderState::Filled,
                price: Some(10_025),
                order_qty: 40,
                last_qty: 40,
                last_px: Some(10_025),
                leaves_qty: 0,
                cum_qty: 40,
                avg_px: 10_025,
                reason: None,
            },
            None,
        );
        assert_eq!(Some(40), report.last_qty);
        assert_eq!(Some(10_025), report.last_px);
        assert_eq!(OrdStatus::Filled, report.ord_status);
    }

    #[test]
    fn a_cancel_reject_answers_the_request_it_refused() {
        let mut mapping = Mapping::new(1, "AAPL");
        let reject = mapping.cancel_reject(&deepbook_engine_core::CancelRejectEvent {
            header: header(),
            order_id: Some(9),
            cl_ord_id: "C-3".parse().expect("id"),
            orig_cl_ord_id: "C-1".parse().expect("id"),
            account: 7,
            state: OrderState::New,
            reason: RejectReason::OrderNotOpen,
            is_replace: true,
        });
        assert_eq!("9", reject.order_id);
        assert_eq!(
            CxlRejResponseTo::OrderCancelReplaceRequest,
            reject.cxl_rej_response_to
        );
        assert_eq!(Some(0), reject.cxl_rej_reason);
        assert_eq!(Some("OrderNotOpen".to_owned()), reject.text);
    }

    #[test]
    fn a_cancel_uses_the_account_the_order_was_sent_with() {
        let mut mapping = Mapping::new(1, "AAPL");
        mapping.new_order(&order()).expect("mapped");
        let command = mapping
            .cancel(&OrderCancelRequest {
                cl_ord_id: "C-2".to_owned(),
                orig_cl_ord_id: "C-1".to_owned(),
                symbol: "AAPL".to_owned(),
                side: Side::Buy,
                order_qty: Some(100),
                transact_time: "20261025-12:34:57.000".to_owned(),
            })
            .expect("mapped");
        match command {
            Command::CancelOrder(cancel) => {
                assert_eq!(7, cancel.account);
                assert_eq!("C-2", cancel.cl_ord_id.to_string());
                assert_eq!("C-1", cancel.orig_cl_ord_id.to_string());
            }
            other => panic!("expected CancelOrder, got {other:?}"),
        }
    }

    #[test]
    fn a_cancel_for_an_unseen_order_is_refused_locally() {
        let mut mapping = Mapping::new(1, "AAPL");
        assert!(
            mapping
                .cancel(&OrderCancelRequest {
                    cl_ord_id: "C-2".to_owned(),
                    orig_cl_ord_id: "NOPE".to_owned(),
                    symbol: "AAPL".to_owned(),
                    side: Side::Buy,
                    order_qty: None,
                    transact_time: "20261025-12:34:57.000".to_owned(),
                })
                .is_err()
        );
    }

    #[test]
    fn a_status_request_answers_from_the_last_report() {
        let mut mapping = Mapping::new(1, "AAPL");
        mapping.new_order(&order()).expect("mapped");
        let _ = mapping.report(
            &ExecutionReportEvent {
                header: header(),
                order_id: 9,
                cl_ord_id: "C-1".parse().expect("id"),
                account: 7,
                symbol: 1,
                side: Side::Buy,
                exec_type: EngineExecType::New,
                state: OrderState::New,
                price: Some(10_025),
                order_qty: 100,
                last_qty: 0,
                last_px: None,
                leaves_qty: 100,
                cum_qty: 0,
                avg_px: 0,
                reason: None,
            },
            None,
        );
        let report = mapping.status(
            &OrderStatusRequest {
                cl_ord_id: "C-1".to_owned(),
                symbol: "AAPL".to_owned(),
                side: Side::Buy,
            },
            "GW-1".to_owned(),
            1_790_000_000_000,
        );
        assert_eq!("GW-1", report.exec_id);
        assert_eq!("9", report.order_id);
        assert_eq!(ExecType::OrderStatus, report.exec_type);
        assert_eq!(OrdStatus::New, report.ord_status);
        assert_eq!(100, report.leaves_qty);
    }

    #[test]
    fn a_status_request_for_an_unknown_order_is_rejected() {
        let mapping = Mapping::new(1, "AAPL");
        let report = mapping.status(
            &OrderStatusRequest {
                cl_ord_id: "NOPE".to_owned(),
                symbol: "AAPL".to_owned(),
                side: Side::Buy,
            },
            "GW-2".to_owned(),
            1_790_000_000_000,
        );
        assert_eq!(ExecType::Rejected, report.exec_type);
        assert_eq!("NONE", report.order_id);
    }

    #[test]
    fn a_replace_keeps_the_original_orders_account() {
        let mut mapping = Mapping::new(1, "AAPL");
        mapping.new_order(&order()).expect("mapped");
        let command = mapping
            .replace(&OrderCancelReplaceRequest {
                cl_ord_id: "C-2".to_owned(),
                orig_cl_ord_id: "C-1".to_owned(),
                account: None,
                symbol: "AAPL".to_owned(),
                side: Side::Buy,
                order_qty: 150,
                ord_type: FixOrdType::Limit,
                price: Some(10_050),
                time_in_force: FixTimeInForce::Day,
                transact_time: "20261025-12:34:57.000".to_owned(),
            })
            .expect("mapped");
        match command {
            Command::ReplaceOrder(replace) => {
                assert_eq!(7, replace.account);
                assert_eq!(150, replace.qty);
                assert_eq!(Some(10_050), replace.price);
            }
            other => panic!("expected ReplaceOrder, got {other:?}"),
        }
    }

    #[test]
    fn client_order_ids_keep_the_fix_limit() {
        assert!(client_order_id("C-1").is_ok());
        assert!(client_order_id(&"x".repeat(21)).is_err());
    }

    #[test]
    fn a_missing_account_is_zero() {
        assert_eq!((0, "0".to_owned()), parse_account(None).expect("parsed"));
        assert!(parse_account(Some("")).is_err());
        assert!(parse_account(Some("-1")).is_err());
        assert!(parse_account(Some("1.5")).is_err());
    }

    #[test]
    fn the_first_session_keeps_the_configured_egress() {
        let channel = "aeron:udp?term-length=128k|endpoint=localhost:25030";
        assert_eq!(channel, egress_channel_for(channel, 0));
    }

    #[test]
    fn each_session_moves_the_udp_egress_port() {
        assert_eq!(
            "aeron:udp?term-length=128k|endpoint=localhost:25031",
            egress_channel_for("aeron:udp?term-length=128k|endpoint=localhost:25030", 1)
        );
        assert_eq!(
            "aeron:udp?endpoint=localhost:25032|term-length=128k",
            egress_channel_for("aeron:udp?endpoint=localhost:25030|term-length=128k", 2)
        );
        assert_eq!(
            "aeron:udp?endpoint=127.0.0.1:25041|term-length=128k",
            egress_channel_for("aeron:udp?endpoint=127.0.0.1:25030|term-length=128k", 11)
        );
    }

    #[test]
    fn a_channel_that_names_no_udp_endpoint_is_unchanged() {
        for channel in [
            "aeron:ipc",
            "aeron:ipc?term-length=64k",
            "aeron:udp?term-length=128k",
        ] {
            assert_eq!(channel, egress_channel_for(channel, 3));
        }
        // A port that would not fit in the channel is left alone rather than
        // silently wrapped.
        assert_eq!(
            "aeron:udp?endpoint=localhost:65535",
            egress_channel_for("aeron:udp?endpoint=localhost:65535", 1)
        );
    }
}
