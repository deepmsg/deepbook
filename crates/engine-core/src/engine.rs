//! The deterministic engine: idempotency, risk, matching and events.
//!
//! The engine is a pure state machine. `on_command` is the only entry point;
//! it takes a log position (the only ordering input) and writes events to a
//! sink. There is no clock, no socket and no serialization here.

use std::collections::{BTreeMap, BTreeSet};

use deepbook_types::{
    AccountId, EMPTY_ORDER_ID, LogPos, OrderId, OrderState, Price, Qty, Seq, Side, SymbolId,
};

use crate::book::{Book, BookView, OrderCold, OrderHot};
use crate::command::{
    CancelOrder, Command, NewOrder, OrdType, RejectReason, ReplaceOrder, TimeInForce,
};
use crate::event::{
    BookUpdateEvent, CancelRejectEvent, Event, EventHeader, EventSeq, EventSink, ExecType,
    ExecutionReportEvent, SnapshotEvent, TradeEvent,
};
use crate::ids::{ClientOrderId, IdMap, MAX_SLOTS, Seen, SeenState};
use crate::risk::{RiskConfig, SelfTradePrevention};
use crate::snapshot::{EngineSnapshot, SnapshotError};

/// Engine configuration.
#[derive(Clone, Copy, Debug)]
pub struct EngineConfig {
    /// The one symbol MVP trades.
    pub symbol: SymbolId,
    /// Slab capacity. A new order beyond it is rejected with `BookFull`.
    pub max_orders: usize,
    /// How many `(account, ClOrdID)` pairs the idempotency window keeps.
    pub recent_order_window: usize,
    /// Pre-trade limits.
    pub risk: RiskConfig,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            symbol: 1,
            max_orders: 1 << 20,
            recent_order_window: 100_000,
            risk: RiskConfig::disabled(),
        }
    }
}

/// The matching engine.
pub struct Engine {
    config: EngineConfig,
    book: Book,
    ids: IdMap,
    open_orders: BTreeMap<AccountId, usize>,
    kill_all: bool,
    killed_accounts: BTreeSet<AccountId>,
    last_log_pos: LogPos,
    event_seq: EventSeq,
}

impl Engine {
    /// Build an engine with `config.max_orders` slab slots.
    #[must_use]
    pub fn new(config: EngineConfig) -> Self {
        let capacity = config.max_orders.clamp(1, MAX_SLOTS);
        Self {
            book: Book::new(config.symbol, capacity),
            ids: IdMap::new(config.recent_order_window),
            open_orders: BTreeMap::new(),
            kill_all: false,
            killed_accounts: BTreeSet::new(),
            last_log_pos: 0,
            event_seq: 0,
            config,
        }
    }

    /// The deterministic book view (test and oracle use).
    #[must_use]
    pub fn view(&self) -> BookView {
        self.book.view()
    }

    /// The deterministic book hash.
    #[must_use]
    pub fn book_hash(&self) -> u64 {
        self.book.hash()
    }

    /// Market-data sequence.
    #[must_use]
    pub const fn md_seq(&self) -> Seq {
        self.book.md_seq
    }

    /// The last command's log position.
    #[must_use]
    pub const fn last_log_pos(&self) -> LogPos {
        self.last_log_pos
    }

    /// The state a cluster snapshot stores.
    #[must_use]
    pub fn snapshot(&self) -> EngineSnapshot {
        EngineSnapshot {
            symbol: self.config.symbol,
            last_log_pos: self.last_log_pos,
            book: self.book.snapshot(),
            seen: self.ids.snapshot(),
            open_orders: self
                .open_orders
                .iter()
                .map(|(account, count)| (*account, *count))
                .collect(),
            kill_all: self.kill_all,
            killed_accounts: self.killed_accounts.iter().copied().collect(),
        }
    }

    /// Rebuild an engine from a snapshot and the deployment configuration.
    ///
    /// The configured `max_orders` must equal the snapshot's slab capacity;
    /// the risk limits and the symbol come from `config`.
    ///
    /// # Errors
    ///
    /// A symbol/capacity mismatch or an inconsistent slab/window.
    pub fn restore(config: EngineConfig, snapshot: &EngineSnapshot) -> Result<Self, SnapshotError> {
        if snapshot.symbol != config.symbol {
            return Err(SnapshotError::WrongSymbol {
                expected: config.symbol,
                actual: snapshot.symbol,
            });
        }
        let capacity = config.max_orders.clamp(1, MAX_SLOTS);
        let book = Book::restore(&snapshot.book, capacity, config.symbol)?;
        let ids = IdMap::restore(&snapshot.seen, config.recent_order_window)?;
        Ok(Self {
            config,
            book,
            ids,
            open_orders: snapshot.open_orders.iter().copied().collect(),
            kill_all: snapshot.kill_all,
            killed_accounts: snapshot.killed_accounts.iter().copied().collect(),
            last_log_pos: snapshot.last_log_pos,
            event_seq: 0,
        })
    }

    /// Apply one command.
    pub fn on_command(&mut self, command: &Command, log_pos: LogPos, sink: &mut impl EventSink) {
        self.last_log_pos = log_pos;
        self.event_seq = 0;
        match command {
            Command::NewOrder(order) => self.new_order(order, sink),
            Command::CancelOrder(cancel) => self.cancel(cancel, sink),
            Command::ReplaceOrder(replace) => self.replace(replace, sink),
            Command::KillSwitch { account } => match account {
                Some(account) => {
                    self.killed_accounts.insert(*account);
                }
                None => self.kill_all = true,
            },
            Command::Snapshot => self.emit_snapshot(sink),
        }
    }

    fn header(&mut self) -> EventHeader {
        let header = EventHeader {
            log_pos: self.last_log_pos,
            event_seq: self.event_seq,
        };
        self.event_seq += 1;
        header
    }

    fn emit_snapshot(&mut self, sink: &mut impl EventSink) {
        let header = self.header();
        let event = SnapshotEvent {
            header,
            symbol: self.config.symbol,
            md_seq: self.book.md_seq,
            book_hash: self.book.hash(),
            last_log_pos: self.last_log_pos,
        };
        sink.emit(&Event::Snapshot(event));
    }

    fn new_order(&mut self, order: &NewOrder, sink: &mut impl EventSink) {
        if let Some(seen) = self.ids.get(order.account, order.cl_ord_id) {
            let header = self.header();
            self.emit_report(
                sink,
                ExecutionReportEvent {
                    header,
                    order_id: seen.order_id,
                    cl_ord_id: order.cl_ord_id,
                    account: order.account,
                    symbol: self.config.symbol,
                    side: order.side,
                    exec_type: ExecType::Rejected,
                    state: OrderState::Rejected,
                    price: order.price,
                    order_qty: order.qty,
                    last_qty: 0,
                    last_px: None,
                    leaves_qty: 0,
                    cum_qty: 0,
                    avg_px: 0,
                    reason: Some(RejectReason::DuplicateClOrdId),
                },
            );
            return;
        }

        if order.symbol != self.config.symbol {
            self.reject_unallocated(order, RejectReason::UnknownSymbol, sink);
            return;
        }

        let Some((order_id, slot)) = self.book.allocate(
            order.account,
            order.side,
            order.price.unwrap_or(0),
            order.qty,
            order.cl_ord_id,
            order.ord_type == OrdType::Market,
        ) else {
            self.reject_unallocated(order, RejectReason::BookFull, sink);
            return;
        };

        // Risk and the order-shape checks. A rejection frees the slot but
        // keeps the client id in the window.
        let killed = self.kill_all || self.killed_accounts.contains(&order.account);
        let open = self.open_orders.get(&order.account).copied().unwrap_or(0);
        let rejection = if order.post_only && order.ord_type != OrdType::Limit {
            Some(RejectReason::InvalidPrice)
        } else if order.post_only && self.would_cross(order.side, order.price) {
            Some(RejectReason::PostOnlyWouldCross)
        } else if let Err(reason) =
            self.config
                .risk
                .check_new_order(order, self.book.last_trade, open, killed)
        {
            Some(reason)
        } else if self.config.risk.self_trade == SelfTradePrevention::RejectTaker
            && self.has_crossing_self_trade(order.account, order.side, order.price)
        {
            Some(RejectReason::SelfTradePrevention)
        } else if order.time_in_force == TimeInForce::FillOrKill
            && self.fillable_qty(order.account, order.side, order.price, order.qty) < order.qty
        {
            Some(RejectReason::FokNotFillable)
        } else {
            None
        };

        if let Some(reason) = rejection {
            self.reject_allocated(slot, order, reason, sink);
            return;
        }

        let seen = Seen {
            order_id,
            state: SeenState::Active,
            price: order.price,
            qty: order.qty,
            filled: 0,
            notional: 0,
            side: order.side,
            symbol: self.config.symbol,
        };
        self.ids.insert(order.account, order.cl_ord_id, seen);

        let cold = self.book.cold[slot as usize];
        let hot = self.book.orders[slot as usize];
        let header = self.header();
        sink.emit(&Event::ExecutionReport(ExecutionReportEvent {
            header,
            order_id,
            cl_ord_id: cold.cl_ord_id,
            account: order.account,
            symbol: self.config.symbol,
            side: order.side,
            exec_type: ExecType::New,
            state: OrderState::New,
            price: order.price,
            order_qty: order.qty,
            last_qty: 0,
            last_px: None,
            leaves_qty: hot.leaves(),
            cum_qty: 0,
            avg_px: 0,
            reason: None,
        }));

        self.match_taker(
            slot,
            order.side,
            order.price,
            order.time_in_force,
            order.ord_type,
            sink,
        );

        let hot = self.book.orders[slot as usize];
        let cold = self.book.cold[slot as usize];
        if hot.leaves() == 0 {
            self.mark(order.account, SeenState::Filled, hot, cold);
            self.book.free(slot);
        } else if order.time_in_force.rests(order.ord_type) {
            self.book.insert_resting(slot);
            self.increment_open(order.account);
            self.emit_book_update(order.side, hot.price, sink);
        } else {
            let header = self.header();
            let mut event = self.report_from_hot(
                header,
                &hot,
                cold.cl_ord_id,
                ExecType::Canceled,
                0,
                None,
                None,
            );
            event.state = OrderState::Canceled;
            event.leaves_qty = 0;
            self.emit_report(sink, event);
            self.mark(order.account, SeenState::Canceled, hot, cold);
            self.book.free(slot);
        }
    }

    fn reject_allocated(
        &mut self,
        slot: u32,
        order: &NewOrder,
        reason: RejectReason,
        sink: &mut impl EventSink,
    ) {
        let hot = self.book.orders[slot as usize];
        let cold = self.book.cold[slot as usize];
        let header = self.header();
        let mut event = self.report_from_hot(
            header,
            &hot,
            cold.cl_ord_id,
            ExecType::Rejected,
            0,
            None,
            Some(reason),
        );
        event.state = OrderState::Rejected;
        event.leaves_qty = 0;
        let seen = self.seen_for(&hot, SeenState::Rejected);
        self.ids.insert(order.account, cold.cl_ord_id, seen);
        self.book.free(slot);
        self.emit_report(sink, event);
    }

    fn reject_unallocated(
        &mut self,
        order: &NewOrder,
        reason: RejectReason,
        sink: &mut impl EventSink,
    ) {
        let header = self.header();
        self.ids.insert(
            order.account,
            order.cl_ord_id,
            Seen {
                order_id: EMPTY_ORDER_ID,
                state: SeenState::Rejected,
                price: order.price,
                qty: order.qty,
                side: order.side,
                symbol: order.symbol,
                ..Seen::default()
            },
        );
        self.emit_report(
            sink,
            ExecutionReportEvent {
                header,
                order_id: EMPTY_ORDER_ID,
                cl_ord_id: order.cl_ord_id,
                account: order.account,
                symbol: order.symbol,
                side: order.side,
                exec_type: ExecType::Rejected,
                state: OrderState::Rejected,
                price: order.price,
                order_qty: order.qty,
                last_qty: 0,
                last_px: None,
                leaves_qty: 0,
                cum_qty: 0,
                avg_px: 0,
                reason: Some(reason),
            },
        );
    }

    fn match_taker(
        &mut self,
        taker_slot: u32,
        side: Side,
        limit: Option<Price>,
        _time_in_force: TimeInForce,
        _ord_type: OrdType,
        sink: &mut impl EventSink,
    ) {
        let opposite = opposite(side);
        loop {
            let taker = self.book.orders[taker_slot as usize];
            if taker.leaves() == 0 {
                break;
            }
            let Some(best) = self.book.best_opposite(side) else {
                break;
            };
            if !crosses(side, limit, best) {
                break;
            }
            let Some(level) = self.book.level(opposite, best) else {
                break;
            };
            let maker_slot = level.head;
            if maker_slot == crate::ids::EMPTY_SLOT {
                break;
            }
            let maker = self.book.orders[maker_slot as usize];

            if maker.account == taker.account
                && self.config.risk.self_trade == SelfTradePrevention::CancelResting
            {
                self.cancel_resting(maker_slot, sink);
                continue;
            }

            let trade_qty = taker.leaves().min(maker.leaves());
            let trade_px = maker.price;

            {
                let taker = &mut self.book.orders[taker_slot as usize];
                taker.filled += trade_qty;
                taker.notional += i128::from(trade_px) * i128::from(trade_qty);
                taker.state = if taker.leaves() == 0 {
                    OrderState::Filled
                } else {
                    OrderState::Partial
                };
            }
            {
                let maker = &mut self.book.orders[maker_slot as usize];
                maker.filled += trade_qty;
                maker.notional += i128::from(trade_px) * i128::from(trade_qty);
                maker.state = if maker.leaves() == 0 {
                    OrderState::Filled
                } else {
                    OrderState::Partial
                };
            }
            self.reduce_level(opposite, trade_px, trade_qty);
            self.book.last_trade = Some(trade_px);

            let maker_cold = self.book.cold[maker_slot as usize];
            let maker_now = self.book.orders[maker_slot as usize];
            let taker_now = self.book.orders[taker_slot as usize];

            let header = self.header();
            sink.emit(&Event::Trade(TradeEvent {
                header,
                maker_order_id: maker_now.id,
                taker_order_id: taker_now.id,
                maker_cl_ord_id: maker_cold.cl_ord_id,
                taker_cl_ord_id: self.book.cold[taker_slot as usize].cl_ord_id,
                maker_account: maker_now.account,
                taker_account: taker_now.account,
                price: trade_px,
                qty: trade_qty,
                aggressor_side: side,
            }));

            let header = self.header();
            let taker_event = self.report_from_hot(
                header,
                &taker_now,
                self.book.cold[taker_slot as usize].cl_ord_id,
                ExecType::Trade,
                trade_qty,
                Some(trade_px),
                None,
            );
            self.emit_report(sink, taker_event);

            let header = self.header();
            let maker_event = self.report_from_hot(
                header,
                &maker_now,
                maker_cold.cl_ord_id,
                ExecType::Trade,
                trade_qty,
                Some(trade_px),
                None,
            );
            self.emit_report(sink, maker_event);

            if maker_now.leaves() == 0 {
                self.book.unlink(maker_slot);
                self.book.free(maker_slot);
                self.decrement_open(maker_now.account);
                self.mark(maker_now.account, SeenState::Filled, maker_now, maker_cold);
                self.emit_book_update(opposite, trade_px, sink);
            } else {
                self.mark(maker_now.account, SeenState::Active, maker_now, maker_cold);
                self.emit_book_update(opposite, trade_px, sink);
            }

            if self.book.orders[taker_slot as usize].leaves() == 0 {
                break;
            }
        }
    }

    fn cancel_resting(&mut self, slot: u32, sink: &mut impl EventSink) {
        let hot = self.book.orders[slot as usize];
        let cold = self.book.cold[slot as usize];
        self.book.unlink(slot);
        self.book.free(slot);
        self.decrement_open(hot.account);
        let header = self.header();
        let mut event = self.report_from_hot(
            header,
            &hot,
            cold.cl_ord_id,
            ExecType::Canceled,
            0,
            None,
            None,
        );
        event.state = OrderState::Canceled;
        event.leaves_qty = 0;
        self.mark(hot.account, SeenState::Canceled, hot, cold);
        self.emit_report(sink, event);
        self.emit_book_update(hot.side, hot.price, sink);
    }

    fn cancel(&mut self, request: &CancelOrder, sink: &mut impl EventSink) {
        let Some(seen) = self.ids.get(request.account, request.orig_cl_ord_id) else {
            self.cancel_reject(
                request,
                None,
                OrderState::Rejected,
                RejectReason::UnknownOrder,
                sink,
            );
            return;
        };
        if seen.state == SeenState::Active {
            let Some(slot) = self.book.find(seen.order_id) else {
                self.cancel_reject(
                    request,
                    Some(seen.order_id),
                    OrderState::Rejected,
                    RejectReason::UnknownOrder,
                    sink,
                );
                return;
            };
            let hot = self.book.orders[slot as usize];
            let cold = self.book.cold[slot as usize];
            self.book.unlink(slot);
            self.book.free(slot);
            self.decrement_open(hot.account);
            let header = self.header();
            let mut event = self.report_from_hot(
                header,
                &hot,
                cold.cl_ord_id,
                ExecType::Canceled,
                0,
                None,
                None,
            );
            event.state = OrderState::Canceled;
            event.leaves_qty = 0;
            self.mark(hot.account, SeenState::Canceled, hot, cold);
            self.emit_report(sink, event);
            self.emit_book_update(hot.side, hot.price, sink);
            return;
        }
        match seen.state {
            SeenState::Canceled => {
                // A repeated cancel answers the terminal state, not an error.
                let header = self.header();
                sink.emit(&Event::ExecutionReport(ExecutionReportEvent {
                    header,
                    order_id: seen.order_id,
                    cl_ord_id: request.orig_cl_ord_id,
                    account: request.account,
                    symbol: seen.symbol,
                    side: seen.side,
                    exec_type: ExecType::Canceled,
                    state: OrderState::Canceled,
                    price: seen.price,
                    order_qty: seen.qty,
                    last_qty: 0,
                    last_px: None,
                    leaves_qty: 0,
                    cum_qty: seen.filled,
                    avg_px: average(seen.notional, seen.filled),
                    reason: None,
                }));
            }
            SeenState::Filled | SeenState::Rejected => {
                let state = if seen.state == SeenState::Filled {
                    OrderState::Filled
                } else {
                    OrderState::Rejected
                };
                self.cancel_reject(
                    request,
                    Some(seen.order_id),
                    state,
                    RejectReason::OrderNotOpen,
                    sink,
                );
            }
            SeenState::Active => unreachable!("handled above"),
        }
    }

    fn replace(&mut self, request: &ReplaceOrder, sink: &mut impl EventSink) {
        let Some(seen) = self.ids.get(request.account, request.orig_cl_ord_id) else {
            self.cancel_reject_replace(
                request,
                None,
                OrderState::Rejected,
                RejectReason::UnknownOrder,
                sink,
            );
            return;
        };
        if seen.state != SeenState::Active {
            let state = match seen.state {
                SeenState::Filled => OrderState::Filled,
                SeenState::Canceled => OrderState::Canceled,
                SeenState::Rejected | SeenState::Active => OrderState::Rejected,
            };
            self.cancel_reject_replace(
                request,
                Some(seen.order_id),
                state,
                RejectReason::OrderNotOpen,
                sink,
            );
            return;
        }
        let Some(slot) = self.book.find(seen.order_id) else {
            self.cancel_reject_replace(
                request,
                Some(seen.order_id),
                OrderState::Rejected,
                RejectReason::UnknownOrder,
                sink,
            );
            return;
        };
        let hot = self.book.orders[slot as usize];
        let cold = self.book.cold[slot as usize];

        // Validate the new shape and risk before touching the book, so a
        // refused replace leaves the original in place.
        let killed = self.kill_all || self.killed_accounts.contains(&request.account);
        let rejection = if request.qty <= hot.filled {
            Some(RejectReason::InvalidQuantity)
        } else if (request.ord_type == OrdType::Limit && request.price.is_none())
            || (request.ord_type == OrdType::Market && request.price.is_some())
            || (request.post_only && request.ord_type != OrdType::Limit)
        {
            Some(RejectReason::InvalidPrice)
        } else if request.post_only && self.would_cross(hot.side, request.price) {
            Some(RejectReason::PostOnlyWouldCross)
        } else if killed {
            Some(RejectReason::KillSwitch)
        } else if self.config.risk.self_trade == SelfTradePrevention::RejectTaker
            && self.has_crossing_self_trade(request.account, hot.side, request.price)
        {
            Some(RejectReason::SelfTradePrevention)
        } else if request.time_in_force == TimeInForce::FillOrKill
            && self.fillable_qty(
                request.account,
                hot.side,
                request.price,
                request.qty - hot.filled,
            ) < request.qty - hot.filled
        {
            Some(RejectReason::FokNotFillable)
        } else if request.qty > self.config.risk.max_order_qty {
            Some(RejectReason::MaxOrderQty)
        } else if let (Some(price), Some(limit)) =
            (request.price, self.config.risk.price_band_ticks)
        {
            let reference = self.book.last_trade.or(self.config.risk.reference_price);
            if let Some(reference) = reference {
                if (i128::from(price) - i128::from(reference)).abs() > i128::from(limit) {
                    Some(RejectReason::PriceBand)
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };
        let rejection = rejection.or_else(|| {
            if let Some(fat) = self.config.risk.fat_finger_ticks {
                if let (Some(price), Some(reference)) = (
                    request.price,
                    self.book.last_trade.or(self.config.risk.reference_price),
                ) {
                    if (i128::from(price) - i128::from(reference)).abs() > i128::from(fat) {
                        return Some(RejectReason::FatFinger);
                    }
                }
            }
            None
        });
        if let Some(reason) = rejection {
            self.cancel_reject_replace(request, Some(seen.order_id), hot.state, reason, sink);
            return;
        }
        let reference = request
            .price
            .or(self.book.last_trade)
            .or(self.config.risk.reference_price);
        if let Some(price) = reference {
            let notional = i128::from(price) * i128::from(request.qty);
            if notional > self.config.risk.max_notional {
                self.cancel_reject_replace(
                    request,
                    Some(seen.order_id),
                    hot.state,
                    RejectReason::MaxNotional,
                    sink,
                );
                return;
            }
        }

        let price_unchanged =
            request.ord_type == OrdType::Limit && request.price == Some(hot.price);
        let qty_reduced = request.qty <= hot.leaves() + hot.filled;
        if price_unchanged && qty_reduced {
            // Same price and no size increase: keep the queue position.
            let old_leaves = hot.leaves();
            let new_leaves = request.qty - hot.filled;
            {
                let order = &mut self.book.orders[slot as usize];
                order.qty = request.qty;
            }
            self.reduce_level(hot.side, hot.price, old_leaves - new_leaves);
            let updated = self.book.orders[slot as usize];
            let header = self.header();
            let event = self.report_from_hot(
                header,
                &updated,
                cold.cl_ord_id,
                ExecType::Replaced,
                0,
                None,
                None,
            );
            self.mark(request.account, SeenState::Active, updated, cold);
            self.emit_report(sink, event);
            self.emit_book_update(hot.side, hot.price, sink);
            return;
        }

        self.book.unlink(slot);
        self.decrement_open(hot.account);
        {
            let order = &mut self.book.orders[slot as usize];
            order.price = request.price.unwrap_or(0);
            order.qty = request.qty;
            order.state = if order.filled > 0 {
                OrderState::Partial
            } else {
                OrderState::New
            };
        }
        let updated = self.book.orders[slot as usize];
        let header = self.header();
        let event = self.report_from_hot(
            header,
            &updated,
            cold.cl_ord_id,
            ExecType::Replaced,
            0,
            None,
            None,
        );
        self.mark(request.account, SeenState::Active, updated, cold);
        self.emit_report(sink, event);
        self.emit_book_update(hot.side, hot.price, sink);

        self.match_taker(
            slot,
            hot.side,
            request.price,
            request.time_in_force,
            request.ord_type,
            sink,
        );
        let after = self.book.orders[slot as usize];
        if after.leaves() == 0 {
            self.mark(request.account, SeenState::Filled, after, cold);
            self.book.free(slot);
        } else if request.time_in_force.rests(request.ord_type) {
            self.book.insert_resting(slot);
            self.increment_open(request.account);
            self.emit_book_update(after.side, after.price, sink);
        } else {
            let header = self.header();
            let mut event = self.report_from_hot(
                header,
                &after,
                cold.cl_ord_id,
                ExecType::Canceled,
                0,
                None,
                None,
            );
            event.state = OrderState::Canceled;
            event.leaves_qty = 0;
            self.mark(request.account, SeenState::Canceled, after, cold);
            self.book.free(slot);
            self.emit_report(sink, event);
        }
    }

    fn cancel_reject(
        &mut self,
        request: &CancelOrder,
        order_id: Option<OrderId>,
        state: OrderState,
        reason: RejectReason,
        sink: &mut impl EventSink,
    ) {
        let header = self.header();
        sink.emit(&Event::CancelReject(CancelRejectEvent {
            header,
            order_id,
            cl_ord_id: request.cl_ord_id,
            orig_cl_ord_id: request.orig_cl_ord_id,
            account: request.account,
            state,
            reason,
        }));
    }

    fn cancel_reject_replace(
        &mut self,
        request: &ReplaceOrder,
        order_id: Option<OrderId>,
        state: OrderState,
        reason: RejectReason,
        sink: &mut impl EventSink,
    ) {
        let header = self.header();
        sink.emit(&Event::CancelReject(CancelRejectEvent {
            header,
            order_id,
            cl_ord_id: request.cl_ord_id,
            orig_cl_ord_id: request.orig_cl_ord_id,
            account: request.account,
            state,
            reason,
        }));
    }

    fn emit_report(&mut self, sink: &mut impl EventSink, event: ExecutionReportEvent) {
        sink.emit(&Event::ExecutionReport(event));
    }

    // The report fields mirror the FIX record; splitting them into a builder
    // would only move the same arguments.
    #[allow(clippy::too_many_arguments)]
    fn report_from_hot(
        &mut self,
        header: EventHeader,
        hot: &OrderHot,
        cl_ord_id: ClientOrderId,
        exec_type: ExecType,
        last_qty: Qty,
        last_px: Option<Price>,
        reason: Option<RejectReason>,
    ) -> ExecutionReportEvent {
        ExecutionReportEvent {
            header,
            order_id: hot.id,
            cl_ord_id,
            account: hot.account,
            symbol: self.config.symbol,
            side: hot.side,
            exec_type,
            state: hot.state,
            price: if hot.is_market { None } else { Some(hot.price) },
            order_qty: hot.qty,
            last_qty,
            last_px,
            leaves_qty: hot.leaves(),
            cum_qty: hot.filled,
            avg_px: hot.average_price(),
            reason,
        }
    }

    fn emit_book_update(&mut self, side: Side, price: Price, sink: &mut impl EventSink) {
        let qty = self.book.level_qty(side, price);
        let md_seq = self.book.next_md_seq();
        let header = self.header();
        sink.emit(&Event::BookUpdate(BookUpdateEvent {
            header,
            symbol: self.config.symbol,
            side,
            price,
            qty,
            md_seq,
        }));
    }

    fn reduce_level(&mut self, side: Side, price: Price, qty: Qty) {
        let level = match side {
            Side::Buy => self.book.bids.get_mut(&price),
            Side::Sell => self.book.asks.get_mut(&price),
        };
        if let Some(level) = level {
            level.total_qty -= qty;
        }
    }

    fn increment_open(&mut self, account: AccountId) {
        *self.open_orders.entry(account).or_insert(0) += 1;
    }

    fn decrement_open(&mut self, account: AccountId) {
        if let Some(count) = self.open_orders.get_mut(&account) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.open_orders.remove(&account);
            }
        }
    }

    fn mark(&mut self, account: AccountId, state: SeenState, hot: OrderHot, cold: OrderCold) {
        let seen = self.seen_for(&hot, state);
        self.ids.mark(account, cold.cl_ord_id, seen);
    }

    fn seen_for(&self, hot: &OrderHot, state: SeenState) -> Seen {
        Seen {
            order_id: hot.id,
            state,
            price: if hot.is_market { None } else { Some(hot.price) },
            qty: hot.qty,
            filled: hot.filled,
            notional: hot.notional,
            side: hot.side,
            symbol: self.config.symbol,
        }
    }

    fn would_cross(&self, side: Side, price: Option<Price>) -> bool {
        let Some(price) = price else {
            return true;
        };
        match self.book.best_opposite(side) {
            Some(best) => crosses(side, Some(price), best),
            None => false,
        }
    }

    fn has_crossing_self_trade(
        &self,
        account: AccountId,
        side: Side,
        limit: Option<Price>,
    ) -> bool {
        match opposite(side) {
            Side::Sell => self
                .book
                .asks
                .iter()
                .take_while(|(price, _)| crosses(side, limit, **price))
                .any(|(_, level)| self.level_has_account(*level, account)),
            Side::Buy => self
                .book
                .bids
                .iter()
                .rev()
                .take_while(|(price, _)| crosses(side, limit, **price))
                .any(|(_, level)| self.level_has_account(*level, account)),
        }
    }

    fn level_has_account(&self, level: crate::book::PriceLevel, account: AccountId) -> bool {
        let mut slot = level.head;
        while slot != crate::ids::EMPTY_SLOT {
            let order = self.book.orders[slot as usize];
            if order.account == account {
                return true;
            }
            slot = order.next;
        }
        false
    }

    fn fillable_qty(&self, account: AccountId, side: Side, limit: Option<Price>, cap: Qty) -> Qty {
        let mut remaining = cap;
        let mut total: Qty = 0;
        match opposite(side) {
            Side::Sell => {
                for (price, level) in &self.book.asks {
                    if !crosses(side, limit, *price) || remaining <= 0 {
                        break;
                    }
                    total += self.level_fillable(*level, account, &mut remaining);
                }
            }
            Side::Buy => {
                for (price, level) in self.book.bids.iter().rev() {
                    if !crosses(side, limit, *price) || remaining <= 0 {
                        break;
                    }
                    total += self.level_fillable(*level, account, &mut remaining);
                }
            }
        }
        total
    }

    fn level_fillable(
        &self,
        level: crate::book::PriceLevel,
        account: AccountId,
        remaining: &mut Qty,
    ) -> Qty {
        let mut total = 0;
        let mut slot = level.head;
        while slot != crate::ids::EMPTY_SLOT && *remaining > 0 {
            let order = self.book.orders[slot as usize];
            let same = order.account == account;
            let skip = same && self.config.risk.self_trade == SelfTradePrevention::CancelResting;
            if !skip {
                let qty = order.leaves().min(*remaining);
                total += qty;
                *remaining -= qty;
            }
            slot = order.next;
        }
        total
    }
}

fn opposite(side: Side) -> Side {
    match side {
        Side::Buy => Side::Sell,
        Side::Sell => Side::Buy,
    }
}

fn crosses(side: Side, limit: Option<Price>, maker_price: Price) -> bool {
    match (side, limit) {
        (Side::Buy, Some(limit)) => maker_price <= limit,
        (Side::Sell, Some(limit)) => maker_price >= limit,
        (_, None) => true,
    }
}

fn average(notional: i128, filled: Qty) -> Price {
    if filled == 0 {
        0
    } else {
        i64::try_from(notional / i128::from(filled)).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::{CancelOrder, NewOrder, OrdType, ReplaceOrder, TimeInForce};

    fn cl(value: &str) -> ClientOrderId {
        value.parse().expect("client order id")
    }

    fn test_config() -> EngineConfig {
        EngineConfig {
            symbol: 1,
            max_orders: 1_024,
            recent_order_window: 1_024,
            risk: RiskConfig::disabled(),
        }
    }

    fn new_engine() -> Engine {
        Engine::new(test_config())
    }

    fn limit(
        account: AccountId,
        id: &str,
        side: Side,
        price: Price,
        qty: Qty,
        tif: TimeInForce,
        post_only: bool,
    ) -> Command {
        Command::NewOrder(NewOrder {
            symbol: 1,
            account,
            cl_ord_id: cl(id),
            side,
            price: Some(price),
            qty,
            ord_type: OrdType::Limit,
            time_in_force: tif,
            post_only,
        })
    }

    fn market(account: AccountId, id: &str, side: Side, qty: Qty) -> Command {
        Command::NewOrder(NewOrder {
            symbol: 1,
            account,
            cl_ord_id: cl(id),
            side,
            price: None,
            qty,
            ord_type: OrdType::Market,
            time_in_force: TimeInForce::ImmediateOrCancel,
            post_only: false,
        })
    }

    fn cancel(account: AccountId, id: &str, orig: &str) -> Command {
        Command::CancelOrder(CancelOrder {
            account,
            cl_ord_id: cl(id),
            orig_cl_ord_id: cl(orig),
        })
    }

    fn replace(account: AccountId, id: &str, orig: &str, price: Price, qty: Qty) -> Command {
        Command::ReplaceOrder(ReplaceOrder {
            account,
            cl_ord_id: cl(id),
            orig_cl_ord_id: cl(orig),
            price: Some(price),
            qty,
            ord_type: OrdType::Limit,
            time_in_force: TimeInForce::GoodTillCanceled,
            post_only: false,
        })
    }

    fn run(engine: &mut Engine, commands: &[Command]) -> Vec<Event> {
        let mut events = Vec::new();
        for (index, command) in commands.iter().enumerate() {
            engine.on_command(command, index as LogPos + 1, &mut events);
        }
        events
    }

    fn reports(events: &[Event]) -> Vec<&ExecutionReportEvent> {
        events
            .iter()
            .filter_map(|event| match event {
                Event::ExecutionReport(report) => Some(report),
                _ => None,
            })
            .collect()
    }

    fn trades(events: &[Event]) -> Vec<&TradeEvent> {
        events
            .iter()
            .filter_map(|event| match event {
                Event::Trade(trade) => Some(trade),
                _ => None,
            })
            .collect()
    }

    fn last_report(events: &[Event]) -> ExecutionReportEvent {
        reports(events)
            .last()
            .map(|report| (*report).clone())
            .expect("report")
    }

    #[test]
    fn a_limit_order_crosses_and_partially_fills_the_maker() {
        let mut engine = new_engine();
        let events = run(
            &mut engine,
            &[
                limit(
                    1,
                    "S1",
                    Side::Sell,
                    100,
                    100,
                    TimeInForce::GoodTillCanceled,
                    false,
                ),
                limit(
                    2,
                    "B1",
                    Side::Buy,
                    100,
                    40,
                    TimeInForce::GoodTillCanceled,
                    false,
                ),
            ],
        );
        let trades = trades(&events);
        assert_eq!(1, trades.len());
        assert_eq!(100, trades[0].price);
        assert_eq!(40, trades[0].qty);
        assert_eq!(cl("S1"), trades[0].maker_cl_ord_id);
        assert_eq!(cl("B1"), trades[0].taker_cl_ord_id);
        let view = engine.view();
        assert_eq!(1, view.asks.len());
        assert_eq!(60, view.asks[0].qty);
        assert_eq!(40, view.asks[0].orders[0].filled);
        assert!(view.bids.is_empty());
    }

    #[test]
    fn price_time_priority_fills_the_oldest_order_first() {
        let mut engine = new_engine();
        let events = run(
            &mut engine,
            &[
                limit(
                    1,
                    "S1",
                    Side::Sell,
                    100,
                    10,
                    TimeInForce::GoodTillCanceled,
                    false,
                ),
                limit(
                    2,
                    "S2",
                    Side::Sell,
                    100,
                    10,
                    TimeInForce::GoodTillCanceled,
                    false,
                ),
                limit(
                    3,
                    "B1",
                    Side::Buy,
                    100,
                    15,
                    TimeInForce::GoodTillCanceled,
                    false,
                ),
            ],
        );
        let trades = trades(&events);
        assert_eq!(2, trades.len());
        assert_eq!(cl("S1"), trades[0].maker_cl_ord_id);
        assert_eq!(10, trades[0].qty);
        assert_eq!(cl("S2"), trades[1].maker_cl_ord_id);
        assert_eq!(5, trades[1].qty);
        let view = engine.view();
        assert_eq!(5, view.asks[0].qty);
        assert_eq!(cl("S2"), view.asks[0].orders[0].cl_ord_id);
    }

    #[test]
    fn a_market_order_sweeps_and_cancels_its_remainder() {
        let mut engine = new_engine();
        let events = run(
            &mut engine,
            &[
                limit(
                    1,
                    "S1",
                    Side::Sell,
                    100,
                    10,
                    TimeInForce::GoodTillCanceled,
                    false,
                ),
                limit(
                    2,
                    "S2",
                    Side::Sell,
                    101,
                    10,
                    TimeInForce::GoodTillCanceled,
                    false,
                ),
                market(3, "B1", Side::Buy, 30),
            ],
        );
        let trades = trades(&events);
        assert_eq!(2, trades.len());
        assert_eq!(100, trades[0].price);
        assert_eq!(101, trades[1].price);
        let last = last_report(&events);
        assert_eq!(ExecType::Canceled, last.exec_type);
        assert_eq!(20, last.cum_qty);
        assert!(engine.view().asks.is_empty());
    }

    #[test]
    fn an_ioc_cancels_the_unfilled_remainder() {
        let mut engine = new_engine();
        let events = run(
            &mut engine,
            &[
                limit(
                    1,
                    "S1",
                    Side::Sell,
                    100,
                    10,
                    TimeInForce::GoodTillCanceled,
                    false,
                ),
                limit(
                    2,
                    "B1",
                    Side::Buy,
                    100,
                    20,
                    TimeInForce::ImmediateOrCancel,
                    false,
                ),
            ],
        );
        let reports = reports(&events);
        let last = reports.last().expect("report");
        assert_eq!(ExecType::Canceled, last.exec_type);
        assert_eq!(10, last.cum_qty);
        assert_eq!(0, last.leaves_qty);
        assert!(engine.view().bids.is_empty());
    }

    #[test]
    fn a_fok_order_is_all_or_nothing() {
        let mut engine = new_engine();
        let events = run(
            &mut engine,
            &[
                limit(
                    1,
                    "S1",
                    Side::Sell,
                    100,
                    10,
                    TimeInForce::GoodTillCanceled,
                    false,
                ),
                limit(2, "B1", Side::Buy, 100, 20, TimeInForce::FillOrKill, false),
            ],
        );
        let last = last_report(&events);
        assert_eq!(ExecType::Rejected, last.exec_type);
        assert_eq!(Some(RejectReason::FokNotFillable), last.reason);
        assert!(trades(&events).is_empty());
        assert_eq!(10, engine.view().asks[0].qty);

        let events = run(
            &mut engine,
            &[
                limit(
                    3,
                    "S2",
                    Side::Sell,
                    100,
                    10,
                    TimeInForce::GoodTillCanceled,
                    false,
                ),
                limit(4, "B2", Side::Buy, 100, 20, TimeInForce::FillOrKill, false),
            ],
        );
        assert_eq!(2, trades(&events).len());
        assert!(engine.view().asks.is_empty());
    }

    #[test]
    fn a_post_only_order_that_would_cross_is_rejected() {
        let mut engine = new_engine();
        let events = run(
            &mut engine,
            &[
                limit(
                    1,
                    "S1",
                    Side::Sell,
                    100,
                    10,
                    TimeInForce::GoodTillCanceled,
                    false,
                ),
                limit(
                    2,
                    "B1",
                    Side::Buy,
                    100,
                    10,
                    TimeInForce::GoodTillCanceled,
                    true,
                ),
            ],
        );
        let last = last_report(&events);
        assert_eq!(Some(RejectReason::PostOnlyWouldCross), last.reason);
        assert_eq!(1, engine.view().asks.len());
        assert!(engine.view().bids.is_empty());
    }

    #[test]
    fn self_trade_can_cancel_the_resting_order() {
        let mut config = EngineConfig {
            max_orders: 1_024,
            recent_order_window: 1_024,
            ..EngineConfig::default()
        };
        config.risk.self_trade = SelfTradePrevention::CancelResting;
        let mut engine = Engine::new(config);
        let events = run(
            &mut engine,
            &[
                limit(
                    1,
                    "S1",
                    Side::Sell,
                    100,
                    10,
                    TimeInForce::GoodTillCanceled,
                    false,
                ),
                limit(
                    1,
                    "B1",
                    Side::Buy,
                    100,
                    10,
                    TimeInForce::GoodTillCanceled,
                    false,
                ),
            ],
        );
        assert!(trades(&events).is_empty());
        assert!(
            reports(&events).iter().any(
                |report| report.exec_type == ExecType::Canceled && report.cl_ord_id == cl("S1")
            )
        );
        let view = engine.view();
        assert!(view.asks.is_empty());
        assert_eq!(1, view.bids.len());
    }

    #[test]
    fn self_trade_can_reject_the_taker() {
        let mut config = EngineConfig {
            max_orders: 1_024,
            recent_order_window: 1_024,
            ..EngineConfig::default()
        };
        config.risk.self_trade = SelfTradePrevention::RejectTaker;
        let mut engine = Engine::new(config);
        let events = run(
            &mut engine,
            &[
                limit(
                    1,
                    "S1",
                    Side::Sell,
                    100,
                    10,
                    TimeInForce::GoodTillCanceled,
                    false,
                ),
                limit(
                    1,
                    "B1",
                    Side::Buy,
                    100,
                    10,
                    TimeInForce::GoodTillCanceled,
                    false,
                ),
            ],
        );
        let last = last_report(&events);
        assert_eq!(Some(RejectReason::SelfTradePrevention), last.reason);
        assert_eq!(1, engine.view().asks.len());
        assert!(engine.view().bids.is_empty());
    }

    #[test]
    fn a_cancel_and_a_repeated_cancel_both_answer_canceled() {
        let mut engine = new_engine();
        let events = run(
            &mut engine,
            &[
                limit(
                    1,
                    "S1",
                    Side::Sell,
                    100,
                    10,
                    TimeInForce::GoodTillCanceled,
                    false,
                ),
                cancel(1, "C1", "S1"),
                cancel(1, "C2", "S1"),
            ],
        );
        let reports = reports(&events);
        let canceled: Vec<&ExecutionReportEvent> = reports
            .iter()
            .filter(|report| report.exec_type == ExecType::Canceled)
            .copied()
            .collect();
        assert_eq!(2, canceled.len());
        assert_eq!(cl("S1"), canceled[0].cl_ord_id);
        assert!(engine.view().asks.is_empty());
    }

    #[test]
    fn an_unknown_cancel_is_rejected() {
        let mut engine = new_engine();
        let events = run(&mut engine, &[cancel(1, "C1", "NOPE")]);
        assert!(events.iter().any(|event| matches!(
            event,
            Event::CancelReject(reject) if reject.reason == RejectReason::UnknownOrder
        )));
    }

    #[test]
    fn a_replace_at_a_new_price_loses_priority() {
        let mut engine = new_engine();
        let events = run(
            &mut engine,
            &[
                limit(
                    1,
                    "S1",
                    Side::Sell,
                    100,
                    10,
                    TimeInForce::GoodTillCanceled,
                    false,
                ),
                limit(
                    2,
                    "S2",
                    Side::Sell,
                    100,
                    10,
                    TimeInForce::GoodTillCanceled,
                    false,
                ),
                replace(1, "R1", "S1", 101, 10),
                limit(
                    3,
                    "B1",
                    Side::Buy,
                    100,
                    10,
                    TimeInForce::GoodTillCanceled,
                    false,
                ),
            ],
        );
        let trades = trades(&events);
        assert_eq!(1, trades.len());
        assert_eq!(cl("S2"), trades[0].maker_cl_ord_id);
        let view = engine.view();
        assert_eq!(101, view.asks[0].price);
        assert_eq!(cl("S1"), view.asks[0].orders[0].cl_ord_id);
    }

    #[test]
    fn a_replace_that_only_reduces_keeps_priority() {
        let mut engine = new_engine();
        let events = run(
            &mut engine,
            &[
                limit(
                    1,
                    "S1",
                    Side::Sell,
                    100,
                    10,
                    TimeInForce::GoodTillCanceled,
                    false,
                ),
                limit(
                    2,
                    "S2",
                    Side::Sell,
                    100,
                    10,
                    TimeInForce::GoodTillCanceled,
                    false,
                ),
                replace(1, "R1", "S1", 100, 5),
                limit(
                    3,
                    "B1",
                    Side::Buy,
                    100,
                    5,
                    TimeInForce::GoodTillCanceled,
                    false,
                ),
            ],
        );
        let trades = trades(&events);
        assert_eq!(1, trades.len());
        assert_eq!(cl("S1"), trades[0].maker_cl_ord_id);
        assert_eq!(5, trades[0].qty);
    }

    #[test]
    fn risk_limits_and_idempotency_reject_orders() {
        let mut config = EngineConfig {
            max_orders: 1_024,
            recent_order_window: 1_024,
            ..EngineConfig::default()
        };
        config.risk.max_order_qty = 5;
        let mut engine = Engine::new(config);
        let events = run(
            &mut engine,
            &[limit(
                1,
                "B1",
                Side::Buy,
                100,
                10,
                TimeInForce::GoodTillCanceled,
                false,
            )],
        );
        let last = last_report(&events);
        assert_eq!(Some(RejectReason::MaxOrderQty), last.reason);

        let mut second = new_engine();
        let events = run(
            &mut second,
            &[
                limit(
                    1,
                    "B1",
                    Side::Buy,
                    100,
                    5,
                    TimeInForce::GoodTillCanceled,
                    false,
                ),
                limit(
                    1,
                    "B1",
                    Side::Buy,
                    100,
                    5,
                    TimeInForce::GoodTillCanceled,
                    false,
                ),
            ],
        );
        let last = last_report(&events);
        assert_eq!(Some(RejectReason::DuplicateClOrdId), last.reason);
    }

    #[test]
    fn a_kill_switch_stops_new_orders_but_not_cancels() {
        let mut engine = new_engine();
        let events = run(
            &mut engine,
            &[
                limit(
                    1,
                    "S1",
                    Side::Sell,
                    100,
                    10,
                    TimeInForce::GoodTillCanceled,
                    false,
                ),
                Command::KillSwitch { account: Some(1) },
                limit(
                    1,
                    "B1",
                    Side::Buy,
                    100,
                    10,
                    TimeInForce::GoodTillCanceled,
                    false,
                ),
                cancel(1, "C1", "S1"),
            ],
        );
        assert!(
            reports(&events)
                .iter()
                .any(|report| report.reason == Some(RejectReason::KillSwitch))
        );
        assert!(
            reports(&events)
                .iter()
                .any(|report| report.exec_type == ExecType::Canceled)
        );
    }

    #[test]
    fn replaying_the_same_stream_produces_the_same_hash_and_snapshot() {
        let commands = [
            limit(
                1,
                "S1",
                Side::Sell,
                100,
                10,
                TimeInForce::GoodTillCanceled,
                false,
            ),
            limit(
                2,
                "B1",
                Side::Buy,
                100,
                4,
                TimeInForce::GoodTillCanceled,
                false,
            ),
            limit(
                3,
                "S2",
                Side::Sell,
                101,
                20,
                TimeInForce::GoodTillCanceled,
                false,
            ),
            market(4, "B2", Side::Buy, 8),
            Command::Snapshot,
        ];
        let mut first = new_engine();
        let first_events = run(&mut first, &commands);
        let mut second = new_engine();
        let second_events = run(&mut second, &commands);
        assert_eq!(first.book_hash(), second.book_hash());
        assert_eq!(first.view(), second.view());
        let first_snapshot = first_events
            .iter()
            .find_map(|event| match event {
                Event::Snapshot(snapshot) => Some(snapshot),
                _ => None,
            })
            .expect("snapshot");
        let second_snapshot = second_events
            .iter()
            .find_map(|event| match event {
                Event::Snapshot(snapshot) => Some(snapshot),
                _ => None,
            })
            .expect("snapshot");
        assert_eq!(first_snapshot.book_hash, second_snapshot.book_hash);
        assert_eq!(first_snapshot.md_seq, second_snapshot.md_seq);
    }
    #[test]
    fn an_unknown_symbol_is_rejected() {
        let mut engine = new_engine();
        let events = run(
            &mut engine,
            &[Command::NewOrder(NewOrder {
                symbol: 2,
                account: 1,
                cl_ord_id: cl("B1"),
                side: Side::Buy,
                price: Some(100),
                qty: 1,
                ord_type: OrdType::Limit,
                time_in_force: TimeInForce::GoodTillCanceled,
                post_only: false,
            })],
        );
        let last = last_report(&events);
        assert_eq!(Some(RejectReason::UnknownSymbol), last.reason);
    }

    #[test]
    fn the_risk_limits_reject_with_their_own_reasons() {
        let mut config = EngineConfig {
            max_orders: 1_024,
            recent_order_window: 1_024,
            ..EngineConfig::default()
        };
        config.risk.max_notional = 1_000;
        let mut engine = Engine::new(config);
        let events = run(
            &mut engine,
            &[limit(
                1,
                "B1",
                Side::Buy,
                100,
                20,
                TimeInForce::GoodTillCanceled,
                false,
            )],
        );
        assert_eq!(Some(RejectReason::MaxNotional), last_report(&events).reason);

        let mut config = EngineConfig {
            max_orders: 1_024,
            recent_order_window: 1_024,
            ..EngineConfig::default()
        };
        config.risk.reference_price = Some(100);
        config.risk.price_band_ticks = Some(5);
        let mut engine = Engine::new(config);
        let events = run(
            &mut engine,
            &[limit(
                1,
                "B1",
                Side::Buy,
                110,
                1,
                TimeInForce::GoodTillCanceled,
                false,
            )],
        );
        assert_eq!(Some(RejectReason::PriceBand), last_report(&events).reason);

        let mut config = EngineConfig {
            max_orders: 1_024,
            recent_order_window: 1_024,
            ..EngineConfig::default()
        };
        config.risk.reference_price = Some(100);
        config.risk.fat_finger_ticks = Some(2);
        let mut engine = Engine::new(config);
        let events = run(
            &mut engine,
            &[limit(
                1,
                "B1",
                Side::Buy,
                110,
                1,
                TimeInForce::GoodTillCanceled,
                false,
            )],
        );
        assert_eq!(Some(RejectReason::FatFinger), last_report(&events).reason);

        let mut config = EngineConfig {
            max_orders: 1_024,
            recent_order_window: 1_024,
            ..EngineConfig::default()
        };
        config.risk.max_open_orders_per_account = 1;
        let mut engine = Engine::new(config);
        let events = run(
            &mut engine,
            &[
                limit(
                    1,
                    "S1",
                    Side::Sell,
                    100,
                    1,
                    TimeInForce::GoodTillCanceled,
                    false,
                ),
                limit(
                    1,
                    "S2",
                    Side::Sell,
                    101,
                    1,
                    TimeInForce::GoodTillCanceled,
                    false,
                ),
            ],
        );
        let last = last_report(&events);
        assert_eq!(Some(RejectReason::MaxOpenOrders), last.reason);
    }
    #[test]
    fn a_snapshot_restores_the_book_window_and_future_events() {
        let mut engine = new_engine();
        let before = [
            limit(
                1,
                "S1",
                Side::Sell,
                100,
                10,
                TimeInForce::GoodTillCanceled,
                false,
            ),
            limit(
                2,
                "B1",
                Side::Buy,
                99,
                5,
                TimeInForce::GoodTillCanceled,
                false,
            ),
            limit(
                3,
                "S2",
                Side::Sell,
                101,
                20,
                TimeInForce::GoodTillCanceled,
                false,
            ),
            cancel(2, "C1", "B1"),
            replace(1, "R1", "S1", 102, 8),
            Command::KillSwitch { account: Some(1) },
        ];
        let mut events = Vec::new();
        let mut log = 0;
        for command in &before {
            log += 1;
            engine.on_command(command, log, &mut events);
        }

        let snapshot = engine.snapshot();
        let mut restored = Engine::restore(test_config(), &snapshot).expect("restore");
        assert_eq!(engine.book_hash(), restored.book_hash());
        assert_eq!(engine.view(), restored.view());
        assert_eq!(snapshot, restored.snapshot());
        assert_eq!(engine.last_log_pos(), restored.last_log_pos());
        assert_eq!(engine.md_seq(), restored.md_seq());

        let continuation = [
            limit(
                3,
                "B2",
                Side::Buy,
                100,
                5,
                TimeInForce::GoodTillCanceled,
                false,
            ),
            limit(
                4,
                "S3",
                Side::Sell,
                99,
                7,
                TimeInForce::GoodTillCanceled,
                false,
            ),
            cancel(2, "C2", "B2"),
            replace(3, "R2", "B2", 103, 4),
        ];
        let mut first = Vec::new();
        let mut second = Vec::new();
        log = 0;
        for command in &continuation {
            log += 1;
            engine.on_command(command, log, &mut first);
            restored.on_command(command, log, &mut second);
        }
        assert_eq!(first, second);
        assert_eq!(engine.book_hash(), restored.book_hash());
        assert_eq!(engine.view(), restored.view());

        // The idempotency window came back: a reused ClOrdID is a duplicate.
        let mut duplicate = Vec::new();
        restored.on_command(
            &limit(
                1,
                "S1",
                Side::Sell,
                110,
                1,
                TimeInForce::GoodTillCanceled,
                false,
            ),
            99,
            &mut duplicate,
        );
        assert_eq!(
            Some(RejectReason::DuplicateClOrdId),
            last_report(&duplicate).reason
        );

        // The per-account kill switch came back too.
        let mut killed = Vec::new();
        restored.on_command(
            &limit(
                1,
                "S9",
                Side::Sell,
                110,
                1,
                TimeInForce::GoodTillCanceled,
                false,
            ),
            100,
            &mut killed,
        );
        assert_eq!(Some(RejectReason::KillSwitch), last_report(&killed).reason);
    }
}
