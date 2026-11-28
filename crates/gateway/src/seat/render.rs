//! Project engine events onto DeepOUCH wire messages (ADR 0012 §6.4).
//!
//! The service outbox stores protocol-neutral `engine_core::wire` events. This
//! module is the OUCH half of the projection: it turns an
//! `ExecutionReport`/`CancelReject` plus the outer `seat_seq`, `liquidity`,
//! `resync` and replace-request metadata into the fixed-width frame the client
//! sees. A non-decimal client id is an error, not a silent zero: a static
//! DeepOUCH seat only ever holds decimal ids.

use std::fmt;

use deepbook_engine_core::{ClientOrderId, Event, ExecType, RejectReason, reject_reason_code};
use deepbook_ouch::ids::from_client_order_id;
use deepbook_ouch::wire::{Message, canceled, side as wire_side};
use deepbook_types::Side;

/// Why an event cannot be projected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RenderError {
    /// The engine event carries a non-decimal client order id.
    NonDecimalId(ClientOrderId),
    /// A `Replaced` report had no replace-request id attached.
    MissingReplaceRequest,
    /// A rejection report had no reason.
    MissingRejectReason,
}

impl fmt::Display for RenderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NonDecimalId(id) => write!(formatter, "non-decimal client order id {id}"),
            Self::MissingReplaceRequest => write!(formatter, "a replaced report has no request id"),
            Self::MissingRejectReason => write!(formatter, "a rejection has no reason"),
        }
    }
}

impl std::error::Error for RenderError {}

/// Project one outbox event.
///
/// `replace_req` is the `ReplaceOrder` request's own id, attached by the
/// service only to the `ExecType::Replaced` report. `resync` marks a
/// synthetic restatement; it is always emitted as `OrderAccepted` with the
/// full `cum_qty`/`avg_px`.
///
/// # Errors
///
/// [`RenderError`] for a non-decimal id, a replaced report without a request
/// id, or a rejection without a reason.
pub fn render_event(
    seat_seq: u64,
    liquidity_tag: u8,
    resync: bool,
    replace_req: Option<u64>,
    event: &Event,
) -> Result<Option<Message>, RenderError> {
    match event {
        Event::ExecutionReport(report) => {
            let cl_ord_id = from_client_order_id(report.cl_ord_id)
                .ok_or(RenderError::NonDecimalId(report.cl_ord_id))?;
            let state = report.state as u8;
            let side = match report.side {
                Side::Buy => wire_side::BUY,
                Side::Sell => wire_side::SELL,
            };
            if resync {
                return Ok(Some(Message::OrderAccepted {
                    seq: seat_seq,
                    cl_ord_id,
                    order_id: report.order_id,
                    symbol: report.symbol,
                    side,
                    state,
                    price: report.price.unwrap_or(0),
                    qty: qty(report.order_qty),
                    leaves_qty: qty(report.leaves_qty),
                    log_pos: report.header.log_pos,
                    cum_qty: qty(report.cum_qty),
                    avg_px: report.avg_px,
                }));
            }
            let message = match report.exec_type {
                ExecType::New => Message::OrderAccepted {
                    seq: seat_seq,
                    cl_ord_id,
                    order_id: report.order_id,
                    symbol: report.symbol,
                    side,
                    state,
                    price: report.price.unwrap_or(0),
                    qty: qty(report.order_qty),
                    leaves_qty: qty(report.leaves_qty),
                    log_pos: report.header.log_pos,
                    cum_qty: qty(report.cum_qty),
                    avg_px: report.avg_px,
                },
                ExecType::Replaced => Message::OrderReplaced {
                    seq: seat_seq,
                    req_cl_ord_id: replace_req.ok_or(RenderError::MissingReplaceRequest)?,
                    orig_cl_ord_id: cl_ord_id,
                    order_id: report.order_id,
                    price: report.price.unwrap_or(0),
                    qty: qty(report.order_qty),
                    leaves_qty: qty(report.leaves_qty),
                    state,
                },
                ExecType::Trade => Message::OrderExecuted {
                    seq: seat_seq,
                    cl_ord_id,
                    order_id: report.order_id,
                    fill_price: report.last_px.unwrap_or(0),
                    fill_qty: qty(report.last_qty),
                    leaves_qty: qty(report.leaves_qty),
                    state,
                    liquidity: liquidity_tag,
                },
                ExecType::Canceled => Message::OrderCanceled {
                    seq: seat_seq,
                    cl_ord_id,
                    orig_cl_ord_id: cl_ord_id,
                    order_id: report.order_id,
                    reason: if report.reason == Some(RejectReason::SelfTradePrevention) {
                        canceled::SELF_TRADE_PREVENTION
                    } else {
                        canceled::USER_REQUESTED
                    },
                },
                ExecType::Rejected => Message::OrderRejected {
                    seq: seat_seq,
                    cl_ord_id,
                    orig_cl_ord_id: 0,
                    reason: reject_reason_code(
                        report.reason.ok_or(RenderError::MissingRejectReason)?,
                    ),
                    is_cancel_reject: false,
                },
            };
            Ok(Some(message))
        }
        Event::CancelReject(reject) => {
            let cl_ord_id = from_client_order_id(reject.cl_ord_id)
                .ok_or(RenderError::NonDecimalId(reject.cl_ord_id))?;
            let orig_cl_ord_id = from_client_order_id(reject.orig_cl_ord_id)
                .ok_or(RenderError::NonDecimalId(reject.orig_cl_ord_id))?;
            Ok(Some(Message::OrderRejected {
                seq: seat_seq,
                cl_ord_id,
                orig_cl_ord_id,
                reason: reject_reason_code(reject.reason),
                is_cancel_reject: true,
            }))
        }
        _ => Ok(None),
    }
}

fn qty(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use deepbook_engine_core::ids::ClientOrderId;
    use deepbook_engine_core::{CancelRejectEvent, EventHeader, ExecutionReportEvent};
    use deepbook_ouch::ids::to_client_order_id;
    use deepbook_ouch::wire::{liquidity, msg_type, rejected, state as wire_state};
    use deepbook_types::{OrderState, Side};

    fn cl(value: u64) -> ClientOrderId {
        to_client_order_id(value).expect("id")
    }

    fn report(exec_type: ExecType, state: OrderState) -> ExecutionReportEvent {
        ExecutionReportEvent {
            header: EventHeader {
                log_pos: 77,
                event_seq: 3,
            },
            order_id: 9,
            cl_ord_id: cl(42),
            account: 1,
            symbol: 1,
            side: Side::Buy,
            exec_type,
            state,
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

    #[test]
    fn a_new_report_is_order_accepted() {
        let message = render_event(
            4,
            0,
            false,
            None,
            &Event::ExecutionReport(report(ExecType::New, OrderState::New)),
        )
        .expect("render")
        .expect("message");
        assert_eq!(
            message,
            Message::OrderAccepted {
                seq: 4,
                cl_ord_id: 42,
                order_id: 9,
                symbol: 1,
                side: wire_side::BUY,
                state: wire_state::NEW,
                price: 100_025,
                qty: 7,
                leaves_qty: 5,
                log_pos: 77,
                cum_qty: 2,
                avg_px: 100_025,
            }
        );
        assert_eq!(message.kind(), msg_type::ORDER_ACCEPTED);
    }

    #[test]
    fn a_resync_partial_order_keeps_cum_and_average() {
        let message = render_event(
            9,
            0,
            true,
            None,
            &Event::ExecutionReport(report(ExecType::New, OrderState::Partial)),
        )
        .expect("render")
        .expect("message");
        match message {
            Message::OrderAccepted {
                seq,
                state,
                cum_qty,
                avg_px,
                leaves_qty,
                ..
            } => {
                assert_eq!(seq, 9);
                assert_eq!(state, wire_state::PARTIAL);
                assert_eq!(cum_qty, 2);
                assert_eq!(avg_px, 100_025);
                assert_eq!(leaves_qty, 5);
            }
            other => panic!("expected OrderAccepted, got {other:?}"),
        }
    }

    #[test]
    fn a_replaced_report_uses_the_request_id() {
        let message = render_event(
            5,
            0,
            false,
            Some(44),
            &Event::ExecutionReport(report(ExecType::Replaced, OrderState::New)),
        )
        .expect("render")
        .expect("message");
        assert_eq!(
            message,
            Message::OrderReplaced {
                seq: 5,
                req_cl_ord_id: 44,
                orig_cl_ord_id: 42,
                order_id: 9,
                price: 100_025,
                qty: 7,
                leaves_qty: 5,
                state: wire_state::NEW,
            }
        );

        assert_eq!(
            render_event(
                5,
                0,
                false,
                None,
                &Event::ExecutionReport(report(ExecType::Replaced, OrderState::New))
            ),
            Err(RenderError::MissingReplaceRequest)
        );
    }

    #[test]
    fn a_trade_carries_liquidity() {
        let message = render_event(
            6,
            liquidity::MAKER,
            false,
            None,
            &Event::ExecutionReport(report(ExecType::Trade, OrderState::Partial)),
        )
        .expect("render")
        .expect("message");
        assert_eq!(
            message,
            Message::OrderExecuted {
                seq: 6,
                cl_ord_id: 42,
                order_id: 9,
                fill_price: 100_025,
                fill_qty: 2,
                leaves_qty: 5,
                state: wire_state::PARTIAL,
                liquidity: liquidity::MAKER,
            }
        );
    }

    #[test]
    fn a_rejection_carries_the_reason_code() {
        let mut event = report(ExecType::Rejected, OrderState::Rejected);
        event.reason = Some(RejectReason::InvalidQuantity);
        let message = render_event(7, 0, false, None, &Event::ExecutionReport(event))
            .expect("render")
            .expect("message");
        assert_eq!(
            message,
            Message::OrderRejected {
                seq: 7,
                cl_ord_id: 42,
                orig_cl_ord_id: 0,
                reason: rejected::INVALID_QUANTITY,
                is_cancel_reject: false,
            }
        );

        let mut event = report(ExecType::Rejected, OrderState::Rejected);
        event.reason = None;
        assert_eq!(
            render_event(7, 0, false, None, &Event::ExecutionReport(event)),
            Err(RenderError::MissingRejectReason)
        );
    }

    #[test]
    fn a_cancel_reject_is_a_cancel_reject() {
        let message = render_event(
            8,
            0,
            false,
            None,
            &Event::CancelReject(CancelRejectEvent {
                header: EventHeader {
                    log_pos: 78,
                    event_seq: 1,
                },
                order_id: Some(9),
                cl_ord_id: cl(43),
                orig_cl_ord_id: cl(42),
                account: 1,
                state: OrderState::New,
                reason: RejectReason::UnknownOrder,
                is_replace: false,
            }),
        )
        .expect("render")
        .expect("message");
        assert_eq!(
            message,
            Message::OrderRejected {
                seq: 8,
                cl_ord_id: 43,
                orig_cl_ord_id: 42,
                reason: rejected::UNKNOWN_ORDER,
                is_cancel_reject: true,
            }
        );
    }

    #[test]
    fn a_non_decimal_id_is_refused() {
        let mut event = report(ExecType::New, OrderState::New);
        event.cl_ord_id = "ABC".parse().expect("id");
        assert!(matches!(
            render_event(1, 0, false, None, &Event::ExecutionReport(event)),
            Err(RenderError::NonDecimalId(_))
        ));
    }
}
