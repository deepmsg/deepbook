//! The M1 in-process engine.
//!
//! It is deliberately not a matching engine: M2's `engine-core` owns the book,
//! matching and risk. This stub exists so M1 can prove the gateway seam and
//! the FIX round trip end to end:
//!
//! - NewOrderSingle is acknowledged with an `ExecutionReport(ExecType=New)`;
//! - a command's `ClOrdID` is idempotent: a retransmission replays the stored
//!   report, same `OrderID` and `ExecID`;
//! - cancel and replace change the record and report `Canceled` / `Replaced`;
//! - a cancel or replace for an unknown or terminal order is an
//!   `OrderCancelReject`;
//! - OrderStatusRequest answers `ExecType=OrderStatus`.
//!
//! No fills are produced. `leaves_qty` is the order quantity, `cum_qty` and
//! `avg_px` are zero, which is exactly what an accepted-but-unmatched order
//! looks like.

use std::collections::{HashMap, VecDeque};

use deepbook_fix::{
    ApplicationMessage, CxlRejResponseTo, ExecType, ExecutionReport, NewOrderSingle, OrdStatus,
    OrderCancelReject, OrderCancelReplaceRequest, OrderCancelRequest, OrderStatusRequest,
};
use deepbook_types::Side;

use crate::sink::{CommandSink, EventSource, SessionEvent, SessionKey};

/// The in-process M1 engine.
#[derive(Debug, Default)]
pub struct StubEngine {
    next_order: u64,
    next_exec: u64,
    /// The response to each command `ClOrdID`, for a retransmission.
    responses: HashMap<(SessionKey, String), ApplicationMessage>,
    /// The live order records, keyed by the original `ClOrdID`.
    orders: HashMap<(SessionKey, String), OrderRecord>,
    events: VecDeque<SessionEvent>,
}

#[derive(Clone, Debug)]
struct OrderRecord {
    order_id: String,
    symbol: String,
    side: Side,
    qty: i64,
    cum_qty: i64,
    avg_px: i64,
    status: OrdStatus,
}

impl StubEngine {
    /// A fresh engine with no orders.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn next_order_id(&mut self) -> String {
        self.next_order += 1;
        format!("ORDER-{}", self.next_order)
    }

    fn next_exec_id(&mut self) -> String {
        self.next_exec += 1;
        format!("EXEC-{}", self.next_exec)
    }

    fn queue(&mut self, session: &SessionKey, message: ApplicationMessage) {
        self.events.push_back(SessionEvent {
            session: session.clone(),
            message,
        });
    }

    fn replay_or(&mut self, session: &SessionKey, cl_ord_id: &str) -> bool {
        let key = (session.clone(), cl_ord_id.to_owned());
        if let Some(response) = self.responses.get(&key).cloned() {
            self.queue(session, response);
            return true;
        }
        false
    }

    fn remember(&mut self, session: &SessionKey, cl_ord_id: &str, response: ApplicationMessage) {
        self.responses
            .insert((session.clone(), cl_ord_id.to_owned()), response);
    }

    fn on_new_order(&mut self, session: &SessionKey, order: NewOrderSingle) {
        if self.replay_or(session, &order.cl_ord_id) {
            return;
        }
        let order_id = self.next_order_id();
        let exec_id = self.next_exec_id();
        let cl_ord_id = order.cl_ord_id.clone();

        if order.order_qty <= 0 {
            let report = ExecutionReport {
                exec_id,
                order_id,
                cl_ord_id: cl_ord_id.clone(),
                exec_type: ExecType::Rejected,
                ord_status: OrdStatus::Rejected,
                symbol: order.symbol,
                side: order.side,
                order_qty: order.order_qty,
                last_qty: None,
                last_px: None,
                leaves_qty: 0,
                cum_qty: 0,
                avg_px: 0,
                account: order.account,
                transact_time: Some(order.transact_time),
                text: Some("quantity must be positive".to_owned()),
            };
            let message = ApplicationMessage::ExecutionReport(report);
            self.remember(session, &cl_ord_id, message.clone());
            self.queue(session, message);
            return;
        }

        let report = ExecutionReport {
            exec_id,
            order_id: order_id.clone(),
            cl_ord_id: order.cl_ord_id.clone(),
            exec_type: ExecType::New,
            ord_status: OrdStatus::New,
            symbol: order.symbol.clone(),
            side: order.side,
            order_qty: order.order_qty,
            last_qty: None,
            last_px: None,
            leaves_qty: order.order_qty,
            cum_qty: 0,
            avg_px: 0,
            account: order.account,
            transact_time: Some(order.transact_time),
            text: None,
        };
        self.orders.insert(
            (session.clone(), order.cl_ord_id.clone()),
            OrderRecord {
                order_id,
                symbol: order.symbol,
                side: order.side,
                qty: order.order_qty,
                cum_qty: 0,
                avg_px: 0,
                status: OrdStatus::New,
            },
        );
        let message = ApplicationMessage::ExecutionReport(report);
        self.remember(session, &order.cl_ord_id, message.clone());
        self.queue(session, message);
    }

    fn on_cancel(&mut self, session: &SessionKey, request: OrderCancelRequest) {
        if self.replay_or(session, &request.cl_ord_id) {
            return;
        }
        let order_key = (session.clone(), request.orig_cl_ord_id.clone());
        let Some(record) = self.orders.get(&order_key).cloned() else {
            let reject = OrderCancelReject {
                order_id: "NONE".to_owned(),
                cl_ord_id: request.cl_ord_id.clone(),
                orig_cl_ord_id: request.orig_cl_ord_id,
                ord_status: OrdStatus::Rejected,
                cxl_rej_response_to: CxlRejResponseTo::OrderCancelRequest,
                cxl_rej_reason: Some(1),
                text: Some("unknown order".to_owned()),
            };
            let message = ApplicationMessage::OrderCancelReject(reject);
            self.remember(session, &request.cl_ord_id, message.clone());
            self.queue(session, message);
            return;
        };
        if !open(record.status) {
            let reject = OrderCancelReject {
                order_id: record.order_id,
                cl_ord_id: request.cl_ord_id.clone(),
                orig_cl_ord_id: request.orig_cl_ord_id,
                ord_status: record.status,
                cxl_rej_response_to: CxlRejResponseTo::OrderCancelRequest,
                cxl_rej_reason: Some(0),
                text: Some("order is not open".to_owned()),
            };
            let message = ApplicationMessage::OrderCancelReject(reject);
            self.remember(session, &request.cl_ord_id, message.clone());
            self.queue(session, message);
            return;
        }

        let exec_id = self.next_exec_id();
        let report = ExecutionReport {
            exec_id,
            order_id: record.order_id.clone(),
            cl_ord_id: request.cl_ord_id.clone(),
            exec_type: ExecType::Canceled,
            ord_status: OrdStatus::Canceled,
            symbol: record.symbol,
            side: record.side,
            order_qty: record.qty,
            last_qty: None,
            last_px: None,
            leaves_qty: 0,
            cum_qty: record.cum_qty,
            avg_px: record.avg_px,
            account: None,
            transact_time: Some(request.transact_time),
            text: None,
        };
        if let Some(record) = self.orders.get_mut(&order_key) {
            record.status = OrdStatus::Canceled;
        }
        let message = ApplicationMessage::ExecutionReport(report);
        self.remember(session, &request.cl_ord_id, message.clone());
        self.queue(session, message);
    }

    fn on_replace(&mut self, session: &SessionKey, request: OrderCancelReplaceRequest) {
        if self.replay_or(session, &request.cl_ord_id) {
            return;
        }
        let order_key = (session.clone(), request.orig_cl_ord_id.clone());
        let Some(record) = self.orders.get(&order_key).cloned() else {
            let reject = OrderCancelReject {
                order_id: "NONE".to_owned(),
                cl_ord_id: request.cl_ord_id.clone(),
                orig_cl_ord_id: request.orig_cl_ord_id,
                ord_status: OrdStatus::Rejected,
                cxl_rej_response_to: CxlRejResponseTo::OrderCancelReplaceRequest,
                cxl_rej_reason: Some(1),
                text: Some("unknown order".to_owned()),
            };
            let message = ApplicationMessage::OrderCancelReject(reject);
            self.remember(session, &request.cl_ord_id, message.clone());
            self.queue(session, message);
            return;
        };
        if !open(record.status) {
            let reject = OrderCancelReject {
                order_id: record.order_id,
                cl_ord_id: request.cl_ord_id.clone(),
                orig_cl_ord_id: request.orig_cl_ord_id,
                ord_status: record.status,
                cxl_rej_response_to: CxlRejResponseTo::OrderCancelReplaceRequest,
                cxl_rej_reason: Some(0),
                text: Some("order is not open".to_owned()),
            };
            let message = ApplicationMessage::OrderCancelReject(reject);
            self.remember(session, &request.cl_ord_id, message.clone());
            self.queue(session, message);
            return;
        }

        let exec_id = self.next_exec_id();
        let leaves = request.order_qty - record.cum_qty;
        let report = ExecutionReport {
            exec_id,
            order_id: record.order_id.clone(),
            cl_ord_id: request.cl_ord_id.clone(),
            exec_type: ExecType::Replaced,
            ord_status: OrdStatus::New,
            symbol: record.symbol,
            side: record.side,
            order_qty: request.order_qty,
            last_qty: None,
            last_px: None,
            leaves_qty: leaves.max(0),
            cum_qty: record.cum_qty,
            avg_px: record.avg_px,
            account: request.account,
            transact_time: Some(request.transact_time),
            text: None,
        };
        if let Some(record) = self.orders.get_mut(&order_key) {
            record.qty = request.order_qty;
            record.status = OrdStatus::New;
        }
        let message = ApplicationMessage::ExecutionReport(report);
        self.remember(session, &request.cl_ord_id, message.clone());
        self.queue(session, message);
    }

    fn on_status(&mut self, session: &SessionKey, request: OrderStatusRequest) {
        let order_key = (session.clone(), request.cl_ord_id.clone());
        let (record, status) = match self.orders.get(&order_key) {
            Some(record) => (record.clone(), record.status),
            None => {
                let report = ExecutionReport {
                    exec_id: self.next_exec_id(),
                    order_id: "NONE".to_owned(),
                    cl_ord_id: request.cl_ord_id,
                    exec_type: ExecType::Rejected,
                    ord_status: OrdStatus::Rejected,
                    symbol: request.symbol,
                    side: request.side,
                    order_qty: 0,
                    last_qty: None,
                    last_px: None,
                    leaves_qty: 0,
                    cum_qty: 0,
                    avg_px: 0,
                    account: None,
                    transact_time: None,
                    text: Some("unknown order".to_owned()),
                };
                self.queue(session, ApplicationMessage::ExecutionReport(report));
                return;
            }
        };
        let report = ExecutionReport {
            exec_id: self.next_exec_id(),
            order_id: record.order_id,
            cl_ord_id: request.cl_ord_id,
            exec_type: ExecType::OrderStatus,
            ord_status: status,
            symbol: request.symbol,
            side: request.side,
            order_qty: record.qty,
            last_qty: None,
            last_px: None,
            leaves_qty: if open(status) {
                (record.qty - record.cum_qty).max(0)
            } else {
                0
            },
            cum_qty: record.cum_qty,
            avg_px: record.avg_px,
            account: None,
            transact_time: None,
            text: None,
        };
        self.queue(session, ApplicationMessage::ExecutionReport(report));
    }
}

impl CommandSink for StubEngine {
    fn deliver(&mut self, session: &SessionKey, message: ApplicationMessage) {
        match message {
            ApplicationMessage::NewOrderSingle(order) => self.on_new_order(session, order),
            ApplicationMessage::OrderCancelRequest(request) => self.on_cancel(session, request),
            ApplicationMessage::OrderCancelReplaceRequest(request) => {
                self.on_replace(session, request);
            }
            ApplicationMessage::OrderStatusRequest(request) => self.on_status(session, request),
            // Reports travel the other way; a peer that sends one is ignored.
            ApplicationMessage::ExecutionReport(_) | ApplicationMessage::OrderCancelReject(_) => {}
        }
    }
}

impl EventSource for StubEngine {
    fn poll_event(&mut self) -> Option<SessionEvent> {
        self.events.pop_front()
    }
}

fn open(status: OrdStatus) -> bool {
    matches!(status, OrdStatus::New | OrdStatus::PartiallyFilled)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session() -> SessionKey {
        SessionKey::new("DEEPBOOK", "CLIENT")
    }

    fn order(cl_ord_id: &str) -> NewOrderSingle {
        NewOrderSingle {
            cl_ord_id: cl_ord_id.to_owned(),
            account: Some("A-1".to_owned()),
            symbol: "AAPL".to_owned(),
            side: Side::Buy,
            order_qty: 100,
            ord_type: deepbook_fix::OrdType::Limit,
            price: Some(10_025),
            time_in_force: deepbook_fix::TimeInForce::Day,
            transact_time: "20261025-12:34:56.000".to_owned(),
            handl_inst: None,
        }
    }

    fn cancel(orig: &str, cl_ord_id: &str) -> OrderCancelRequest {
        OrderCancelRequest {
            cl_ord_id: cl_ord_id.to_owned(),
            orig_cl_ord_id: orig.to_owned(),
            symbol: "AAPL".to_owned(),
            side: Side::Buy,
            order_qty: Some(100),
            transact_time: "20261025-12:34:57.000".to_owned(),
        }
    }

    fn replace(orig: &str, cl_ord_id: &str, qty: i64) -> OrderCancelReplaceRequest {
        OrderCancelReplaceRequest {
            cl_ord_id: cl_ord_id.to_owned(),
            orig_cl_ord_id: orig.to_owned(),
            account: Some("A-1".to_owned()),
            symbol: "AAPL".to_owned(),
            side: Side::Buy,
            order_qty: qty,
            ord_type: deepbook_fix::OrdType::Limit,
            price: Some(10_050),
            time_in_force: deepbook_fix::TimeInForce::Day,
            transact_time: "20261025-12:34:57.000".to_owned(),
        }
    }

    fn next_report(engine: &mut StubEngine) -> ExecutionReport {
        match engine.poll_event().expect("event").message {
            ApplicationMessage::ExecutionReport(report) => report,
            other => panic!("unexpected report: {other:?}"),
        }
    }

    #[test]
    fn a_new_order_is_acknowledged() {
        let mut engine = StubEngine::new();
        engine.deliver(&session(), ApplicationMessage::NewOrderSingle(order("C-1")));
        let report = next_report(&mut engine);
        assert_eq!(ExecType::New, report.exec_type);
        assert_eq!(OrdStatus::New, report.ord_status);
        assert_eq!("C-1", report.cl_ord_id);
        assert_eq!(100, report.leaves_qty);
        assert_eq!(0, report.cum_qty);
        assert_eq!(0, report.avg_px);
        assert_eq!(Some("A-1".to_owned()), report.account);
        assert!(engine.poll_event().is_none());
    }

    #[test]
    fn a_retransmitted_order_replays_the_same_report() {
        let mut engine = StubEngine::new();
        engine.deliver(&session(), ApplicationMessage::NewOrderSingle(order("C-1")));
        let first = next_report(&mut engine);
        engine.deliver(&session(), ApplicationMessage::NewOrderSingle(order("C-1")));
        let second = next_report(&mut engine);
        assert_eq!(first.order_id, second.order_id);
        assert_eq!(first.exec_id, second.exec_id);
        assert_eq!(1, engine.orders.len());
    }

    #[test]
    fn a_replace_then_cancel_updates_the_record() {
        let mut engine = StubEngine::new();
        engine.deliver(&session(), ApplicationMessage::NewOrderSingle(order("C-1")));
        let _ = next_report(&mut engine);
        engine.deliver(
            &session(),
            ApplicationMessage::OrderCancelReplaceRequest(replace("C-1", "C-2", 150)),
        );
        let replaced = next_report(&mut engine);
        assert_eq!(ExecType::Replaced, replaced.exec_type);
        assert_eq!(OrdStatus::New, replaced.ord_status);
        assert_eq!(150, replaced.leaves_qty);
        assert_eq!(150, replaced.order_qty);

        engine.deliver(
            &session(),
            ApplicationMessage::OrderCancelRequest(cancel("C-1", "C-3")),
        );
        let canceled = next_report(&mut engine);
        assert_eq!(ExecType::Canceled, canceled.exec_type);
        assert_eq!(OrdStatus::Canceled, canceled.ord_status);
        assert_eq!(0, canceled.leaves_qty);
    }

    #[test]
    fn an_unknown_cancel_is_rejected() {
        let mut engine = StubEngine::new();
        engine.deliver(
            &session(),
            ApplicationMessage::OrderCancelRequest(cancel("NOPE", "C-1")),
        );
        match engine.poll_event().expect("event").message {
            ApplicationMessage::OrderCancelReject(reject) => {
                assert_eq!(1, reject.cxl_rej_reason.expect("reason"));
                assert_eq!(
                    CxlRejResponseTo::OrderCancelRequest,
                    reject.cxl_rej_response_to
                );
            }
            other => panic!("unexpected report: {other:?}"),
        }
    }

    #[test]
    fn a_status_request_reports_the_current_state() {
        let mut engine = StubEngine::new();
        engine.deliver(&session(), ApplicationMessage::NewOrderSingle(order("C-1")));
        let _ = next_report(&mut engine);
        engine.deliver(
            &session(),
            ApplicationMessage::OrderCancelRequest(cancel("C-1", "C-2")),
        );
        let _ = next_report(&mut engine);
        engine.deliver(
            &session(),
            ApplicationMessage::OrderStatusRequest(OrderStatusRequest {
                cl_ord_id: "C-1".to_owned(),
                symbol: "AAPL".to_owned(),
                side: Side::Buy,
            }),
        );
        let report = next_report(&mut engine);
        assert_eq!(ExecType::OrderStatus, report.exec_type);
        assert_eq!(OrdStatus::Canceled, report.ord_status);
        assert_eq!("C-1", report.cl_ord_id);
    }

    #[test]
    fn a_non_positive_quantity_is_rejected() {
        let mut engine = StubEngine::new();
        let mut bad = order("C-1");
        bad.order_qty = 0;
        engine.deliver(&session(), ApplicationMessage::NewOrderSingle(bad));
        let report = next_report(&mut engine);
        assert_eq!(ExecType::Rejected, report.exec_type);
        assert_eq!(OrdStatus::Rejected, report.ord_status);
        assert_eq!(0, report.leaves_qty);
    }
}
