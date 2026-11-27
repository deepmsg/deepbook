//! The FIX-seat adapter over the service envelopes (ADR 0012 §6.3).
//!
//! It implements the gateway's existing [`Engine`] seam, so the acceptor does
//! not change: FIX messages arrive in `deliver`, engine events come back from
//! `poll_event`. One `SeatFixEngine` owns every configured FIX seat over one
//! cluster session; the acceptor routes a connection to a seat by its
//! `SenderCompID`/`TargetCompID` pair.
//!
//! A seat is bound lazily, on the first application message. Binding at startup
//! would let a resync report reach a FIX session before its peer logged on.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;

use deepbook_engine_core::{
    CancelOrder, ClientOrderId, Command, NewOrder, OrdType, ReplaceOrder, TimeInForce,
    encode_command,
};
use deepbook_fix::clock::{Clock, SystemClock};
use deepbook_fix::{
    ApplicationMessage, CxlRejResponseTo, ExecType, ExecutionReport, NewOrderSingle, OrdStatus,
    OrderCancelReject, OrderCancelReplaceRequest, OrderCancelRequest, format_utc,
};
use deepbook_types::{AccountId, Side as FixSide};

use crate::cluster::{ClusterEngineConfig, ClusterEngineError, SeatClusterEngine, SeatEgress};
use crate::metrics::GatewayMetrics;
use crate::seat::config::{SeatConfig, SeatProtocol, SeatRegistry};
use crate::seat::fix_session::{FixEventOutcome, FixSeatSession};
use crate::seat::journal::latest;
use crate::sink::{CommandSink, EventSource, SessionEvent, SessionKey};

/// Cluster egress frames handled per `poll_event` call. Without a bound, a
/// busy service keeps the acceptor inside this loop for the whole run and the
/// FIX reports only reach the socket when the burst ends.
const MAX_EGRESS_PER_CALL: usize = 64;

/// One configured FIX seat's runtime.
struct SeatRuntime {
    key: SessionKey,
    session: FixSeatSession,
    bind: (u16, u32, u64),
    bind_pending: bool,
    epoch: Option<u64>,
    pending: VecDeque<Command>,
    /// The last sequence handed to the acceptor; the next poll acknowledges it.
    returned_seq: u64,
}

/// The FIX-seat adapter for every configured FIX seat.
pub struct SeatFixEngine {
    cluster: SeatClusterEngine,
    seats: BTreeMap<u16, SeatRuntime>,
    keys: HashMap<SessionKey, u16>,
    events: VecDeque<(SessionEvent, u64)>,
    local_exec: u64,
}

impl SeatFixEngine {
    /// Connect the cluster and register every configured FIX seat.
    ///
    /// # Errors
    ///
    /// The registry has no FIX seat, two seats share a key, or the cluster
    /// cannot be reached.
    pub fn connect_with(
        config: ClusterEngineConfig,
        registry: &SeatRegistry,
        metrics: Arc<GatewayMetrics>,
    ) -> Result<Self, ClusterEngineError> {
        let mut seats = BTreeMap::new();
        let mut keys = HashMap::new();
        for seat in registry.iter() {
            if seat.protocol != SeatProtocol::Fix {
                continue;
            }
            let key = SessionKey::new(
                seat.sender.clone().unwrap_or_default(),
                seat.target.clone().unwrap_or_default(),
            );
            if keys.insert(key.clone(), seat.seat_id).is_some() {
                return Err(ClusterEngineError::Configuration(format!(
                    "two fix seats share {}->{}",
                    key.sender_comp_id, key.target_comp_id
                )));
            }
            seats.insert(
                seat.seat_id,
                runtime(seat, key, config.symbol, &config.symbol_name),
            );
        }
        if seats.is_empty() {
            return Err(ClusterEngineError::Configuration(
                "the registry has no fix seat".to_owned(),
            ));
        }
        let cluster = SeatClusterEngine::connect_with(config, metrics)?;
        Ok(Self {
            cluster,
            seats,
            keys,
            events: VecDeque::new(),
            local_exec: 0,
        })
    }

    fn runtime(&self, seat_id: u16) -> Option<&SeatRuntime> {
        self.seats.get(&seat_id)
    }

    fn runtime_mut(&mut self, seat_id: u16) -> Option<&mut SeatRuntime> {
        self.seats.get_mut(&seat_id)
    }

    fn ensure_bound(&mut self, seat_id: u16) {
        if !self.cluster.is_connected() {
            return;
        }
        let Some(bind) = self
            .runtime(seat_id)
            .filter(|runtime| runtime.epoch.is_none() && !runtime.bind_pending)
            .map(|runtime| runtime.bind)
        else {
            return;
        };
        let (seat, account, last) = bind;
        self.cluster.bind(seat, account, last);
        if let Some(runtime) = self.runtime_mut(seat_id) {
            runtime.bind_pending = true;
        }
    }

    fn flush_pending(&mut self, seat_id: u16) {
        let Some((epoch, commands)) = self.runtime_mut(seat_id).and_then(|runtime| {
            let epoch = runtime.epoch?;
            Some((epoch, runtime.pending.drain(..).collect::<Vec<Command>>()))
        }) else {
            return;
        };
        for command in commands {
            let bytes = encode_command(&command);
            self.cluster.seat_command(seat_id, epoch, &bytes);
        }
    }

    fn on_bound(&mut self, bound: deepbook_engine_cluster::envelope::SeatBound) {
        let seat_id = bound.seat_id;
        let Some(outcome) = self
            .runtime_mut(seat_id)
            .and_then(|runtime| runtime.session.on_seat_bound(bound).ok())
        else {
            return;
        };
        eprintln!(
            "fix-gateway: seat {seat_id} bound epoch {} next {}",
            outcome.epoch, outcome.next_seq
        );
        if let Some(runtime) = self.runtime_mut(seat_id) {
            runtime.epoch = Some(outcome.epoch);
            runtime.bind_pending = false;
            runtime.returned_seq = 0;
        }
        self.flush_pending(seat_id);
    }

    fn on_event(&mut self, event: crate::cluster::OwnedSeatEvent) {
        let seat_id = event.seat_id;
        let Some((key, outcome)) = self.runtime_mut(seat_id).map(|runtime| {
            let key = runtime.key.clone();
            let outcome = runtime.session.on_event(&event.borrowed());
            (key, outcome)
        }) else {
            return;
        };
        match outcome {
            Ok(FixEventOutcome::Duplicate) => {}
            Ok(FixEventOutcome::Message { message, seat_seq }) => {
                self.events.push_back((
                    SessionEvent {
                        session: key,
                        message: *message,
                    },
                    seat_seq,
                ));
            }
            Err(error) => eprintln!("fix-gateway: seat {seat_id} event: {error}"),
        }
    }

    fn local_exec_id(&mut self) -> String {
        self.local_exec = self.local_exec.saturating_add(1);
        format!("GW-{}", self.local_exec)
    }

    fn reject_new(&mut self, seat_id: u16, order: &NewOrderSingle, text: String) {
        let Some(runtime) = self.runtime(seat_id) else {
            return;
        };
        let session_key = runtime.key.clone();
        let exec_id = self.local_exec_id();
        self.events.push_back((
            SessionEvent {
                session: session_key,
                message: ApplicationMessage::ExecutionReport(ExecutionReport {
                    exec_id,
                    order_id: "NONE".to_owned(),
                    cl_ord_id: order.cl_ord_id.clone(),
                    orig_cl_ord_id: None,
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
                    transact_time: Some(format_utc(SystemClock.now_millis())),
                    text: Some(text),
                }),
            },
            0,
        ));
    }

    fn reject_cancel(
        &mut self,
        seat_id: u16,
        cl_ord_id: &str,
        orig_cl_ord_id: &str,
        is_replace: bool,
        text: String,
    ) {
        let Some(runtime) = self.runtime(seat_id) else {
            return;
        };
        let session_key = runtime.key.clone();
        self.events.push_back((
            SessionEvent {
                session: session_key,
                message: ApplicationMessage::OrderCancelReject(OrderCancelReject {
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
                }),
            },
            0,
        ));
    }

    fn account(&self, seat_id: u16, value: Option<&str>) -> Result<AccountId, String> {
        let seat_account = self
            .runtime(seat_id)
            .map_or(0, |runtime| runtime.session.account_id());
        let Some(value) = value else {
            return Ok(seat_account);
        };
        if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(format!("account '{value}' is not a numeric id"));
        }
        let account: AccountId = value
            .parse()
            .map_err(|_| format!("account '{value}' is outside u32"))?;
        if account != seat_account {
            return Err(format!(
                "account {account} is not this seat's {seat_account}"
            ));
        }
        Ok(account)
    }

    fn symbol(&self, value: &str) -> Result<u16, String> {
        let symbol_id = self
            .seats
            .values()
            .next()
            .map_or(1, |runtime| runtime.session.symbol_id());
        let symbol_name = self
            .seats
            .values()
            .next()
            .map_or("", |runtime| runtime.session.symbol_name());
        if value == symbol_name {
            return Ok(symbol_id);
        }
        value
            .parse::<u16>()
            .ok()
            .filter(|symbol| *symbol == symbol_id)
            .ok_or_else(|| format!("unknown symbol '{value}'"))
    }

    fn client_order_id(value: &str) -> Result<ClientOrderId, String> {
        ClientOrderId::new(value.as_bytes()).map_err(|error| error.to_string())
    }

    fn side(value: FixSide) -> deepbook_types::Side {
        match value {
            FixSide::Buy => deepbook_types::Side::Buy,
            FixSide::Sell => deepbook_types::Side::Sell,
        }
    }

    fn ord_type(value: deepbook_fix::OrdType) -> OrdType {
        match value {
            deepbook_fix::OrdType::Limit => OrdType::Limit,
            deepbook_fix::OrdType::Market => OrdType::Market,
        }
    }

    fn time_in_force(value: deepbook_fix::TimeInForce) -> TimeInForce {
        match value {
            deepbook_fix::TimeInForce::Day => TimeInForce::Day,
            deepbook_fix::TimeInForce::GoodTillCancel => TimeInForce::GoodTillCanceled,
            deepbook_fix::TimeInForce::ImmediateOrCancel => TimeInForce::ImmediateOrCancel,
            deepbook_fix::TimeInForce::FillOrKill => TimeInForce::FillOrKill,
        }
    }

    fn map_new(&self, seat_id: u16, order: &NewOrderSingle) -> Result<Command, String> {
        let account = self.account(seat_id, order.account.as_deref())?;
        let symbol = self.symbol(&order.symbol)?;
        let cl_ord_id = Self::client_order_id(&order.cl_ord_id)?;
        if order.order_qty <= 0 {
            return Err("quantity must be positive".to_owned());
        }
        Ok(Command::NewOrder(NewOrder {
            symbol,
            account,
            cl_ord_id,
            side: Self::side(order.side),
            price: order.price,
            qty: order.order_qty,
            ord_type: Self::ord_type(order.ord_type),
            time_in_force: Self::time_in_force(order.time_in_force),
            post_only: false,
        }))
    }

    fn map_cancel(&self, seat_id: u16, request: &OrderCancelRequest) -> Result<Command, String> {
        let account = self
            .runtime(seat_id)
            .map_or(0, |runtime| runtime.session.account_id());
        let cl_ord_id = Self::client_order_id(&request.cl_ord_id)?;
        let orig_cl_ord_id = Self::client_order_id(&request.orig_cl_ord_id)?;
        Ok(Command::CancelOrder(CancelOrder {
            account,
            cl_ord_id,
            orig_cl_ord_id,
        }))
    }

    fn map_replace(
        &self,
        seat_id: u16,
        request: &OrderCancelReplaceRequest,
    ) -> Result<Command, String> {
        let account = self.account(seat_id, request.account.as_deref())?;
        let symbol = self.symbol(&request.symbol)?;
        let _ = symbol;
        let cl_ord_id = Self::client_order_id(&request.cl_ord_id)?;
        let orig_cl_ord_id = Self::client_order_id(&request.orig_cl_ord_id)?;
        if request.order_qty <= 0 {
            return Err("quantity must be positive".to_owned());
        }
        Ok(Command::ReplaceOrder(ReplaceOrder {
            account,
            cl_ord_id,
            orig_cl_ord_id,
            price: request.price,
            qty: request.order_qty,
            ord_type: Self::ord_type(request.ord_type),
            time_in_force: Self::time_in_force(request.time_in_force),
            post_only: false,
        }))
    }
}

fn runtime(seat: &SeatConfig, key: SessionKey, symbol_id: u16, symbol_name: &str) -> SeatRuntime {
    let last = latest(&seat.journal_dir)
        .ok()
        .flatten()
        .map_or(0, |(_, last)| last);
    SeatRuntime {
        key,
        session: FixSeatSession::new(seat.clone(), symbol_id, symbol_name),
        bind: (seat.seat_id, seat.account_id, last),
        bind_pending: false,
        epoch: None,
        pending: VecDeque::new(),
        returned_seq: 0,
    }
}

impl CommandSink for SeatFixEngine {
    fn deliver(&mut self, session: &SessionKey, message: ApplicationMessage) {
        let Some(&seat_id) = self.keys.get(session) else {
            return;
        };
        if !self.cluster.is_connected() {
            match &message {
                ApplicationMessage::NewOrderSingle(order) => {
                    self.reject_new(seat_id, order, "cluster unavailable".to_owned());
                }
                ApplicationMessage::OrderCancelRequest(request) => {
                    self.reject_cancel(
                        seat_id,
                        &request.cl_ord_id,
                        &request.orig_cl_ord_id,
                        false,
                        "cluster unavailable".to_owned(),
                    );
                }
                ApplicationMessage::OrderCancelReplaceRequest(request) => {
                    self.reject_cancel(
                        seat_id,
                        &request.cl_ord_id,
                        &request.orig_cl_ord_id,
                        true,
                        "cluster unavailable".to_owned(),
                    );
                }
                _ => {}
            }
            return;
        }
        self.ensure_bound(seat_id);
        let command = match message {
            ApplicationMessage::NewOrderSingle(order) => match self.map_new(seat_id, &order) {
                Ok(command) => Some(command),
                Err(text) => {
                    self.reject_new(seat_id, &order, text);
                    None
                }
            },
            ApplicationMessage::OrderCancelRequest(request) => {
                match self.map_cancel(seat_id, &request) {
                    Ok(command) => Some(command),
                    Err(text) => {
                        self.reject_cancel(
                            seat_id,
                            &request.cl_ord_id,
                            &request.orig_cl_ord_id,
                            false,
                            text,
                        );
                        None
                    }
                }
            }
            ApplicationMessage::OrderCancelReplaceRequest(request) => {
                match self.map_replace(seat_id, &request) {
                    Ok(command) => Some(command),
                    Err(text) => {
                        self.reject_cancel(
                            seat_id,
                            &request.cl_ord_id,
                            &request.orig_cl_ord_id,
                            true,
                            text,
                        );
                        None
                    }
                }
            }
            _ => None,
        };
        if let Some(command) = command {
            if let Some(runtime) = self.runtime_mut(seat_id) {
                runtime.pending.push_back(command);
            }
            self.flush_pending(seat_id);
        }
    }

    fn flush(&mut self) {
        let seat_ids: Vec<u16> = self.seats.keys().copied().collect();
        for seat_id in seat_ids {
            self.ensure_bound(seat_id);
            self.flush_pending(seat_id);
        }
    }

    fn pending_commands(&self) -> usize {
        self.seats
            .values()
            .map(|runtime| runtime.pending.len())
            .sum::<usize>()
            .saturating_add(self.cluster.pending_commands())
    }
}

impl EventSource for SeatFixEngine {
    fn poll_event(&mut self) -> Option<SessionEvent> {
        let connected = self.cluster.is_connected();
        let seat_ids: Vec<u16> = self.seats.keys().copied().collect();
        for seat_id in &seat_ids {
            if !connected {
                if let Some(runtime) = self.runtime_mut(*seat_id) {
                    runtime.epoch = None;
                    runtime.bind_pending = false;
                }
            } else {
                let has_pending = self
                    .runtime(*seat_id)
                    .is_some_and(|runtime| !runtime.pending.is_empty());
                if has_pending {
                    self.ensure_bound(*seat_id);
                }
            }
        }
        let mut acks = Vec::new();
        for seat_id in &seat_ids {
            if let Some(runtime) = self.runtime_mut(*seat_id) {
                if let Ok(Some(ack)) = runtime.session.ack_due(runtime.returned_seq) {
                    acks.push(ack);
                }
            }
        }
        for ack in acks {
            self.cluster.ack(ack.seat_id, ack.epoch, ack.up_to_seq);
        }
        if let Some((event, seat_seq)) = self.events.pop_front() {
            if seat_seq > 0 {
                if let Some(seat_id) = self.keys.get(&event.session).copied() {
                    if let Some(runtime) = self.runtime_mut(seat_id) {
                        runtime.returned_seq = seat_seq;
                    }
                }
            }
            return Some(event);
        }
        for _ in 0..MAX_EGRESS_PER_CALL {
            let Some(egress) = self.cluster.poll_egress() else {
                break;
            };
            match egress {
                SeatEgress::Bound(bound) => self.on_bound(bound),
                SeatEgress::Event(event) => self.on_event(event),
                SeatEgress::Rejected(rejected) => {
                    eprintln!("fix-gateway: seat {} bind rejected", rejected.seat_id);
                    if let Some(runtime) = self.runtime_mut(rejected.seat_id) {
                        runtime.bind_pending = false;
                    }
                }
                SeatEgress::Fenced(fenced) => {
                    eprintln!(
                        "fix-gateway: seat {} fenced epoch {} reason {}",
                        fenced.seat_id, fenced.new_epoch, fenced.reason
                    );
                    if let Some(runtime) = self.runtime_mut(fenced.seat_id) {
                        runtime.session.mark_unbound();
                        runtime.epoch = None;
                        runtime.bind_pending = false;
                        runtime.returned_seq = 0;
                    }
                }
            }
        }
        let popped = self.events.pop_front();
        if let Some((event, seat_seq)) = &popped {
            if *seat_seq > 0 {
                if let Some(seat_id) = self.keys.get(&event.session).copied() {
                    if let Some(runtime) = self.runtime_mut(seat_id) {
                        runtime.returned_seq = *seat_seq;
                    }
                }
            }
        }
        popped.map(|(event, _)| event)
    }
}
