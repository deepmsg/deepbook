//! The FIX-seat adapter over the service envelopes (ADR 0012 §6.3).
//!
//! It implements the gateway's existing [`Engine`] seam, so the M1 acceptor
//! does not change: FIX messages arrive in `deliver`, engine events come back
//! from `poll_event`. The difference is underneath — a seat is bound through
//! `SeatClusterEngine`, its events are rendered with the `resync`/replace
//! metadata, and its journal is the FIX seat journal.
//!
//! One `SeatFixEngine` hosts one FIX `SenderCompID`/`TargetCompID` pair, the
//! same limit the M1 acceptor has. The multi-seat FIX listener is a later
//! slice; this one makes the FIX path use the same outbox as OUCH.

use std::collections::VecDeque;
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
use crate::seat::config::{SeatProtocol, SeatRegistry};
use crate::seat::fix_session::{FixEventOutcome, FixSeatSession};
use crate::seat::journal::latest;
use crate::seat::session::BoundOutcome;
use crate::sink::{CommandSink, EventSource, SessionEvent, SessionKey};

/// The FIX-seat adapter.
pub struct SeatFixEngine {
    cluster: SeatClusterEngine,
    session_key: SessionKey,
    session: FixSeatSession,
    symbol_id: u16,
    symbol_name: String,
    /// `(seat_id, account_id, last journaled seq)` for the first bind.
    bind: (u16, u32, u64),
    bind_pending: bool,
    epoch: Option<u64>,
    pending: VecDeque<Command>,
    events: VecDeque<(SessionEvent, u64)>,
    /// The last sequence handed to the acceptor; the next poll acknowledges
    /// it, because by then the FIX session has journaled the message.
    returned_seq: u64,
    local_exec: u64,
}

impl SeatFixEngine {
    /// Connect the cluster and bind one configured FIX seat.
    ///
    /// # Errors
    ///
    /// The registry has no FIX seat for `session_key`, or the cluster cannot be
    /// reached.
    pub fn connect_with(
        config: ClusterEngineConfig,
        registry: &SeatRegistry,
        session_key: SessionKey,
        metrics: Arc<GatewayMetrics>,
    ) -> Result<Self, ClusterEngineError> {
        let seat = registry
            .iter()
            .find(|seat| {
                seat.protocol == SeatProtocol::Fix
                    && seat.sender.as_deref() == Some(session_key.sender_comp_id.as_str())
                    && seat.target.as_deref() == Some(session_key.target_comp_id.as_str())
            })
            .cloned()
            .ok_or_else(|| {
                ClusterEngineError::Configuration(format!(
                    "no fix seat for {}->{}",
                    session_key.sender_comp_id, session_key.target_comp_id
                ))
            })?;
        let last = latest(&seat.journal_dir)
            .ok()
            .flatten()
            .map_or(0, |(_, last)| last);
        let cluster = SeatClusterEngine::connect_with(config.clone(), metrics)?;
        let symbol_name = config.symbol_name.clone();
        Ok(Self {
            cluster,
            session_key,
            session: FixSeatSession::new(seat.clone(), config.symbol, symbol_name.clone()),
            symbol_id: config.symbol,
            symbol_name,
            bind: (seat.seat_id, seat.account_id, last),
            bind_pending: false,
            epoch: None,
            pending: VecDeque::new(),
            events: VecDeque::new(),
            returned_seq: 0,
            local_exec: 0,
        })
    }

    fn ensure_bound(&mut self) {
        if self.epoch.is_some() || self.bind_pending || !self.cluster.is_connected() {
            return;
        }
        let (seat_id, account_id, last) = self.bind;
        self.cluster.bind(seat_id, account_id, last);
        self.bind_pending = true;
    }

    fn on_bound(&mut self, bound: deepbook_engine_cluster::envelope::SeatBound) {
        match self.session.on_seat_bound(bound) {
            Ok(BoundOutcome { epoch, .. }) => {
                self.epoch = Some(epoch);
                self.bind_pending = false;
                self.returned_seq = 0;
                self.flush_pending();
            }
            Err(error) => eprintln!("fix-gateway: seat bound: {error}"),
        }
    }

    fn on_event(&mut self, event: crate::cluster::OwnedSeatEvent) {
        match self.session.on_event(&event.borrowed()) {
            Ok(FixEventOutcome::Duplicate) => {}
            Ok(FixEventOutcome::Message {
                message, seat_seq, ..
            }) => {
                self.events.push_back((
                    SessionEvent {
                        session: self.session_key.clone(),
                        message: *message,
                    },
                    seat_seq,
                ));
            }
            Err(error) => eprintln!("fix-gateway: seat event: {error}"),
        }
    }

    fn flush_pending(&mut self) {
        let Some(epoch) = self.epoch else {
            return;
        };
        while let Some(command) = self.pending.pop_front() {
            let bytes = encode_command(&command);
            self.cluster
                .seat_command(self.session.seat_id(), epoch, &bytes);
        }
    }

    fn local_exec_id(&mut self) -> String {
        self.local_exec = self.local_exec.saturating_add(1);
        format!("GW-{}", self.local_exec)
    }

    fn reject_new(&mut self, order: &NewOrderSingle, text: String) {
        let exec_id = self.local_exec_id();
        self.events.push_back((
            SessionEvent {
                session: self.session_key.clone(),
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
        cl_ord_id: &str,
        orig_cl_ord_id: &str,
        is_replace: bool,
        text: String,
    ) {
        self.events.push_back((
            SessionEvent {
                session: self.session_key.clone(),
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

    fn account(&self, value: Option<&str>) -> Result<AccountId, String> {
        let seat_account = self.session.account_id();
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
        if value == self.symbol_name {
            return Ok(self.symbol_id);
        }
        value
            .parse::<u16>()
            .ok()
            .filter(|symbol| *symbol == self.symbol_id)
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

    fn map_new(&self, order: &NewOrderSingle) -> Result<Command, String> {
        let account = self.account(order.account.as_deref())?;
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

    fn map_cancel(&self, request: &OrderCancelRequest) -> Result<Command, String> {
        let account = self.session.account_id();
        let cl_ord_id = Self::client_order_id(&request.cl_ord_id)?;
        let orig_cl_ord_id = Self::client_order_id(&request.orig_cl_ord_id)?;
        Ok(Command::CancelOrder(CancelOrder {
            account,
            cl_ord_id,
            orig_cl_ord_id,
        }))
    }

    fn map_replace(&self, request: &OrderCancelReplaceRequest) -> Result<Command, String> {
        let account = self.account(request.account.as_deref())?;
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

impl CommandSink for SeatFixEngine {
    fn deliver(&mut self, session: &SessionKey, message: ApplicationMessage) {
        if session != &self.session_key {
            return;
        }
        if !self.cluster.is_connected() {
            match &message {
                ApplicationMessage::NewOrderSingle(order) => {
                    self.reject_new(order, "cluster unavailable".to_owned());
                }
                ApplicationMessage::OrderCancelRequest(request) => {
                    self.reject_cancel(
                        &request.cl_ord_id,
                        &request.orig_cl_ord_id,
                        false,
                        "cluster unavailable".to_owned(),
                    );
                }
                ApplicationMessage::OrderCancelReplaceRequest(request) => {
                    self.reject_cancel(
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
        self.ensure_bound();
        match message {
            ApplicationMessage::NewOrderSingle(order) => match self.map_new(&order) {
                Ok(command) => self.pending.push_back(command),
                Err(text) => {
                    self.reject_new(&order, text);
                    return;
                }
            },
            ApplicationMessage::OrderCancelRequest(request) => match self.map_cancel(&request) {
                Ok(command) => self.pending.push_back(command),
                Err(text) => {
                    self.reject_cancel(&request.cl_ord_id, &request.orig_cl_ord_id, false, text);
                    return;
                }
            },
            ApplicationMessage::OrderCancelReplaceRequest(request) => {
                match self.map_replace(&request) {
                    Ok(command) => self.pending.push_back(command),
                    Err(text) => {
                        self.reject_cancel(&request.cl_ord_id, &request.orig_cl_ord_id, true, text);
                        return;
                    }
                }
            }
            _ => {}
        }
        self.flush_pending();
    }

    fn flush(&mut self) {
        self.flush_pending();
    }

    fn pending_commands(&self) -> usize {
        self.pending
            .len()
            .saturating_add(self.cluster.pending_commands())
    }
}

impl EventSource for SeatFixEngine {
    fn poll_event(&mut self) -> Option<SessionEvent> {
        if !self.cluster.is_connected() {
            self.epoch = None;
            self.bind_pending = false;
        } else if self.epoch.is_none() && !self.pending.is_empty() {
            self.ensure_bound();
        }
        // Ack what the acceptor returned on the previous call: by now the FIX
        // session has journaled (and sent) that message.
        if let Ok(Some(ack)) = self.session.ack_due(self.returned_seq) {
            self.cluster.ack(ack.seat_id, ack.epoch, ack.up_to_seq);
        }
        if let Some((event, seat_seq)) = self.events.pop_front() {
            if seat_seq > 0 {
                self.returned_seq = seat_seq;
            }
            return Some(event);
        }
        while let Some(egress) = self.cluster.poll_egress() {
            match egress {
                SeatEgress::Bound(bound) => self.on_bound(bound),
                SeatEgress::Event(event) => self.on_event(event),
                SeatEgress::Rejected(rejected) => {
                    eprintln!("fix-gateway: seat {} bind rejected", rejected.seat_id);
                    self.bind_pending = false;
                }
                SeatEgress::Fenced(fenced) => {
                    eprintln!("fix-gateway: seat {} fenced", fenced.seat_id);
                    self.session.mark_unbound();
                    self.epoch = None;
                    self.bind_pending = false;
                    self.returned_seq = 0;
                }
            }
        }
        let popped = self.events.pop_front();
        if let Some((_, seat_seq)) = &popped {
            if *seat_seq > 0 {
                self.returned_seq = *seat_seq;
            }
        }
        popped.map(|(event, _)| event)
    }
}
