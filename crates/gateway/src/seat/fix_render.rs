//! Project engine events onto FIX 4.4 for a seat (ADR 0012 §6.3).
//!
//! The seat outbox is protocol-neutral, so a FIX seat reads the same engine
//! record as an OUCH seat. Two fields come from the envelope rather than the
//! record: `resync` (a synthetic restatement becomes `ExecType=D` with
//! `58=RESYNC`) and `replace_req_cl_ord_id` (the `ClOrdID` of a replace
//! request, with the order's own id in `OrigClOrdID`).

use deepbook_engine_core::{CancelRejectEvent, ExecType, ExecutionReportEvent, RejectReason};
use deepbook_fix::{
    ApplicationMessage, CxlRejResponseTo, ExecType as FixExecType, ExecutionReport, OrdStatus,
    OrderCancelReject,
};
use deepbook_types::OrderState;

/// Render one order-state event.
#[must_use]
pub fn render_report(
    report: &ExecutionReportEvent,
    resync: bool,
    replace_req: Option<&str>,
    symbol_id: u16,
    symbol_name: &str,
) -> ApplicationMessage {
    let exec_type = if resync {
        FixExecType::Restated
    } else {
        match report.exec_type {
            ExecType::New => FixExecType::New,
            ExecType::Trade => FixExecType::Trade,
            ExecType::Canceled => FixExecType::Canceled,
            ExecType::Replaced => FixExecType::Replaced,
            ExecType::Rejected => FixExecType::Rejected,
        }
    };
    let status = match report.state {
        OrderState::New => OrdStatus::New,
        OrderState::Partial => OrdStatus::PartiallyFilled,
        OrderState::Filled => OrdStatus::Filled,
        OrderState::Canceled => OrdStatus::Canceled,
        OrderState::Rejected => OrdStatus::Rejected,
    };
    let text = if resync {
        Some("RESYNC".to_owned())
    } else {
        report.reason.map(|reason| reason.as_str().to_owned())
    };
    ApplicationMessage::ExecutionReport(ExecutionReport {
        exec_id: format!("{}-{}", report.header.log_pos, report.header.event_seq),
        order_id: report.order_id.to_string(),
        cl_ord_id: report.cl_ord_id.to_string(),
        orig_cl_ord_id: if report.exec_type == ExecType::Replaced {
            replace_req.map(str::to_owned)
        } else {
            None
        },
        exec_type,
        ord_status: status,
        symbol: if report.symbol == symbol_id {
            symbol_name.to_owned()
        } else {
            report.symbol.to_string()
        },
        side: report.side,
        order_qty: report.order_qty,
        last_qty: (report.last_qty > 0).then_some(report.last_qty),
        last_px: report.last_px,
        leaves_qty: report.leaves_qty,
        cum_qty: report.cum_qty,
        avg_px: report.avg_px,
        account: Some(report.account.to_string()),
        transact_time: None,
        text,
    })
}

/// Render one cancel/replace refusal.
#[must_use]
pub fn render_cancel_reject(event: &CancelRejectEvent) -> ApplicationMessage {
    ApplicationMessage::OrderCancelReject(OrderCancelReject {
        order_id: event
            .order_id
            .map_or_else(|| "NONE".to_owned(), |order_id| order_id.to_string()),
        cl_ord_id: event.cl_ord_id.to_string(),
        orig_cl_ord_id: event.orig_cl_ord_id.to_string(),
        ord_status: match event.state {
            OrderState::New => OrdStatus::New,
            OrderState::Partial => OrdStatus::PartiallyFilled,
            OrderState::Filled => OrdStatus::Filled,
            OrderState::Canceled => OrdStatus::Canceled,
            OrderState::Rejected => OrdStatus::Rejected,
        },
        cxl_rej_response_to: if event.is_replace {
            CxlRejResponseTo::OrderCancelReplaceRequest
        } else {
            CxlRejResponseTo::OrderCancelRequest
        },
        cxl_rej_reason: Some(cxl_rej_reason(event.reason)),
        text: Some(event.reason.as_str().to_owned()),
    })
}

fn cxl_rej_reason(reason: RejectReason) -> u32 {
    match reason {
        RejectReason::UnknownOrder | RejectReason::OrderNotOpen => 1,
        RejectReason::DuplicateClOrdId => 6,
        _ => 99,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deepbook_engine_core::{ClientOrderId, EventHeader};
    use deepbook_types::Side;

    fn id(value: &str) -> ClientOrderId {
        value.parse().expect("id")
    }

    fn report() -> ExecutionReportEvent {
        ExecutionReportEvent {
            header: EventHeader {
                log_pos: 77,
                event_seq: 3,
            },
            order_id: 9,
            cl_ord_id: id("00000000000000000042"),
            account: 11,
            symbol: 1,
            side: Side::Buy,
            exec_type: ExecType::New,
            state: OrderState::Partial,
            price: Some(100_025),
            order_qty: 7,
            last_qty: 2,
            last_px: Some(100_025),
            leaves_qty: 5,
            cum_qty: 2,
            avg_px: 100_025,
            reason: None,
        }
    }

    fn execution(message: ApplicationMessage) -> ExecutionReport {
        match message {
            ApplicationMessage::ExecutionReport(report) => report,
            other => panic!("expected an execution report, got {other:?}"),
        }
    }

    #[test]
    fn a_report_carries_the_seat_fields() {
        let message = execution(render_report(&report(), false, None, 1, "AAPL"));
        assert_eq!(message.exec_id, "77-3");
        assert_eq!(message.order_id, "9");
        assert_eq!(message.cl_ord_id, "00000000000000000042");
        assert_eq!(message.orig_cl_ord_id, None);
        assert_eq!(message.exec_type, FixExecType::New);
        assert_eq!(message.ord_status, OrdStatus::PartiallyFilled);
        assert_eq!(message.symbol, "AAPL");
        assert_eq!(message.side, Side::Buy);
        assert_eq!(message.last_qty, Some(2));
        assert_eq!(message.last_px, Some(100_025));
        assert_eq!(message.account.as_deref(), Some("11"));
    }

    #[test]
    fn a_replaced_report_names_both_ids() {
        let mut event = report();
        event.exec_type = ExecType::Replaced;
        let message = execution(render_report(
            &event,
            false,
            Some("00000000000000000044"),
            1,
            "AAPL",
        ));
        assert_eq!(message.exec_type, FixExecType::Replaced);
        assert_eq!(
            message.orig_cl_ord_id.as_deref(),
            Some("00000000000000000044")
        );
        assert_eq!(message.cl_ord_id, "00000000000000000042");
    }

    #[test]
    fn a_resync_report_is_restated() {
        let message = execution(render_report(&report(), true, None, 1, "AAPL"));
        assert_eq!(message.exec_type, FixExecType::Restated);
        assert_eq!(message.text.as_deref(), Some("RESYNC"));
        assert_eq!(message.cum_qty, 2);
        assert_eq!(message.avg_px, 100_025);
    }

    #[test]
    fn a_cancel_reject_is_the_fix_answer() {
        let message = render_cancel_reject(&CancelRejectEvent {
            header: EventHeader {
                log_pos: 78,
                event_seq: 1,
            },
            order_id: Some(9),
            cl_ord_id: id("00000000000000000043"),
            orig_cl_ord_id: id("00000000000000000042"),
            account: 11,
            state: OrderState::New,
            reason: RejectReason::UnknownOrder,
            is_replace: true,
        });
        let ApplicationMessage::OrderCancelReject(reject) = message else {
            panic!("expected an order cancel reject");
        };
        assert_eq!(reject.order_id, "9");
        assert_eq!(reject.cl_ord_id, "00000000000000000043");
        assert_eq!(reject.orig_cl_ord_id, "00000000000000000042");
        assert_eq!(reject.cxl_rej_reason, Some(1));
        assert_eq!(
            reject.cxl_rej_response_to,
            CxlRejResponseTo::OrderCancelReplaceRequest
        );
    }
}
