//! Typed FIX 4.4 messages and the integer-price bridge.
//!
//! The session layer works on [`SessionMessage`]; application messages are
//! decoded into [`ApplicationMessage`] and handed to the gateway's
//! `CommandSink` (M1c). Prices are integer ticks with two decimal places, so
//! `"100.25"` on the wire is `10_025` here and no floating point is involved.

use std::fmt;

use deepbook_types::{Price, Qty, Side};

use crate::codec::{CodecError, Field, MessageBuilder, MsgType, ParsedMessage, tag};

/// `OrdType(40)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrdType {
    /// `1`.
    Market,
    /// `2`.
    Limit,
}

impl OrdType {
    /// The wire value.
    #[must_use]
    pub const fn as_char(self) -> char {
        match self {
            Self::Market => '1',
            Self::Limit => '2',
        }
    }

    fn parse(value: u8) -> Option<Self> {
        match value {
            b'1' => Some(Self::Market),
            b'2' => Some(Self::Limit),
            _ => None,
        }
    }
}

/// `TimeInForce(59)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeInForce {
    /// `0`.
    Day,
    /// `1`.
    GoodTillCancel,
    /// `3`.
    ImmediateOrCancel,
    /// `4`.
    FillOrKill,
}

impl TimeInForce {
    /// The wire value.
    #[must_use]
    pub const fn as_char(self) -> char {
        match self {
            Self::Day => '0',
            Self::GoodTillCancel => '1',
            Self::ImmediateOrCancel => '3',
            Self::FillOrKill => '4',
        }
    }

    fn parse(value: u8) -> Option<Self> {
        match value {
            b'0' => Some(Self::Day),
            b'1' => Some(Self::GoodTillCancel),
            b'3' => Some(Self::ImmediateOrCancel),
            b'4' => Some(Self::FillOrKill),
            _ => None,
        }
    }
}

/// `ExecType(150)` in the FIX 4.4 sense: fills are `F`, never `1`/`2`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecType {
    /// `0`.
    New,
    /// `4`.
    Canceled,
    /// `5`.
    Replaced,
    /// `8`.
    Rejected,
    /// `F`.
    Trade,
}

impl ExecType {
    /// The wire value.
    #[must_use]
    pub const fn as_char(self) -> char {
        match self {
            Self::New => '0',
            Self::Canceled => '4',
            Self::Replaced => '5',
            Self::Rejected => '8',
            Self::Trade => 'F',
        }
    }

    fn parse(value: u8) -> Option<Self> {
        match value {
            b'0' => Some(Self::New),
            b'4' => Some(Self::Canceled),
            b'5' => Some(Self::Replaced),
            b'8' => Some(Self::Rejected),
            b'F' => Some(Self::Trade),
            _ => None,
        }
    }
}

/// `OrdStatus(39)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrdStatus {
    /// `0`.
    New,
    /// `1`.
    PartiallyFilled,
    /// `2`.
    Filled,
    /// `4`.
    Canceled,
    /// `8`.
    Rejected,
}

impl OrdStatus {
    /// The wire value.
    #[must_use]
    pub const fn as_char(self) -> char {
        match self {
            Self::New => '0',
            Self::PartiallyFilled => '1',
            Self::Filled => '2',
            Self::Canceled => '4',
            Self::Rejected => '8',
        }
    }

    fn parse(value: u8) -> Option<Self> {
        match value {
            b'0' => Some(Self::New),
            b'1' => Some(Self::PartiallyFilled),
            b'2' => Some(Self::Filled),
            b'4' => Some(Self::Canceled),
            b'8' => Some(Self::Rejected),
            _ => None,
        }
    }
}

/// `CxlRejResponseTo(434)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CxlRejResponseTo {
    /// `1`.
    OrderCancelRequest,
    /// `2`.
    OrderCancelReplaceRequest,
}

impl CxlRejResponseTo {
    /// The wire value.
    #[must_use]
    pub const fn as_char(self) -> char {
        match self {
            Self::OrderCancelRequest => '1',
            Self::OrderCancelReplaceRequest => '2',
        }
    }

    fn parse(value: u8) -> Option<Self> {
        match value {
            b'1' => Some(Self::OrderCancelRequest),
            b'2' => Some(Self::OrderCancelReplaceRequest),
            _ => None,
        }
    }
}

/// A session-level message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionMessage {
    /// `A`.
    Logon {
        /// `98`; the subset only supports `0` (none).
        encrypt_method: u8,
        /// `108`, seconds.
        heart_bt_int: u32,
        /// `141=Y`: both sequence numbers reset.
        reset_seq_num: bool,
        /// `553`.
        username: Option<String>,
        /// `554`.
        password: Option<String>,
    },
    /// `5`.
    Logout {
        /// `58`.
        text: Option<String>,
    },
    /// `0`.
    Heartbeat {
        /// `112`, when it answers a `TestRequest`.
        test_req_id: Option<String>,
    },
    /// `1`.
    TestRequest {
        /// `112`.
        test_req_id: String,
    },
    /// `2`.
    ResendRequest {
        /// `7`.
        begin_seq_no: u64,
        /// `16`; `0` means "to the end of the stream".
        end_seq_no: u64,
    },
    /// `4`.
    SequenceReset {
        /// `123=Y`: gap fill; otherwise a reset.
        gap_fill: bool,
        /// `36`.
        new_seq_no: u64,
    },
    /// `3`.
    Reject {
        /// `45`.
        ref_seq_num: u64,
        /// `371`.
        ref_tag_id: Option<u32>,
        /// `372`.
        ref_msg_type: Option<String>,
        /// `373`.
        session_reject_reason: Option<u32>,
        /// `58`.
        text: Option<String>,
    },
}

/// `D NewOrderSingle`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewOrderSingle {
    /// `11`.
    pub cl_ord_id: String,
    /// `1`.
    pub account: Option<String>,
    /// `55`.
    pub symbol: String,
    /// `54`.
    pub side: Side,
    /// `38`.
    pub order_qty: Qty,
    /// `40`.
    pub ord_type: OrdType,
    /// `44`, required for a limit order.
    pub price: Option<Price>,
    /// `59`.
    pub time_in_force: TimeInForce,
    /// `60`.
    pub transact_time: String,
    /// `21`.
    pub handl_inst: Option<char>,
}

/// `F OrderCancelRequest`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrderCancelRequest {
    /// `11`.
    pub cl_ord_id: String,
    /// `41`.
    pub orig_cl_ord_id: String,
    /// `55`.
    pub symbol: String,
    /// `54`.
    pub side: Side,
    /// `38`.
    pub order_qty: Option<Qty>,
    /// `60`.
    pub transact_time: String,
}

/// `G OrderCancelReplaceRequest`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrderCancelReplaceRequest {
    /// `11`.
    pub cl_ord_id: String,
    /// `41`.
    pub orig_cl_ord_id: String,
    /// `1`.
    pub account: Option<String>,
    /// `55`.
    pub symbol: String,
    /// `54`.
    pub side: Side,
    /// `38`.
    pub order_qty: Qty,
    /// `40`.
    pub ord_type: OrdType,
    /// `44`, required for a limit order.
    pub price: Option<Price>,
    /// `59`.
    pub time_in_force: TimeInForce,
    /// `60`.
    pub transact_time: String,
}

/// `8 ExecutionReport`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionReport {
    /// `17`.
    pub exec_id: String,
    /// `37`.
    pub order_id: String,
    /// `11`.
    pub cl_ord_id: String,
    /// `150`.
    pub exec_type: ExecType,
    /// `39`.
    pub ord_status: OrdStatus,
    /// `55`.
    pub symbol: String,
    /// `54`.
    pub side: Side,
    /// `38`.
    pub order_qty: Qty,
    /// `32`; absent on an acknowledgement.
    pub last_qty: Option<Qty>,
    /// `31`; absent on an acknowledgement.
    pub last_px: Option<Price>,
    /// `151`.
    pub leaves_qty: Qty,
    /// `14`.
    pub cum_qty: Qty,
    /// `6`.
    pub avg_px: Price,
    /// `1`.
    pub account: Option<String>,
    /// `60`.
    pub transact_time: Option<String>,
    /// `58`.
    pub text: Option<String>,
}

/// `9 OrderCancelReject`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrderCancelReject {
    /// `37`.
    pub order_id: String,
    /// `11`.
    pub cl_ord_id: String,
    /// `41`.
    pub orig_cl_ord_id: String,
    /// `39`.
    pub ord_status: OrdStatus,
    /// `434`.
    pub cxl_rej_response_to: CxlRejResponseTo,
    /// `102`.
    pub cxl_rej_reason: Option<u32>,
    /// `58`.
    pub text: Option<String>,
}

/// `H OrderStatusRequest`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrderStatusRequest {
    /// `11`.
    pub cl_ord_id: String,
    /// `55`.
    pub symbol: String,
    /// `54`.
    pub side: Side,
}

/// An application message the gateway hands to the engine seam.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ApplicationMessage {
    /// `D`.
    NewOrderSingle(NewOrderSingle),
    /// `F`.
    OrderCancelRequest(OrderCancelRequest),
    /// `G`.
    OrderCancelReplaceRequest(OrderCancelReplaceRequest),
    /// `8`.
    ExecutionReport(ExecutionReport),
    /// `9`.
    OrderCancelReject(OrderCancelReject),
    /// `H`.
    OrderStatusRequest(OrderStatusRequest),
}

/// A decoded message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    /// Session-level.
    Session(SessionMessage),
    /// Application-level.
    Application(ApplicationMessage),
}

/// Decode a session-level message.
///
/// # Errors
///
/// A required field is missing or invalid, or the type is not session-level.
pub fn decode_session(message: &ParsedMessage) -> Result<SessionMessage, CodecError> {
    let msg_type = message.msg_type()?;
    match msg_type {
        MsgType::Logon => {
            let encrypt_method = u8::try_from(required_u64(message, tag::ENCRYPT_METHOD)?)
                .map_err(|_| invalid(tag::ENCRYPT_METHOD, message))?;
            if encrypt_method != 0 {
                return Err(invalid(tag::ENCRYPT_METHOD, message));
            }
            Ok(SessionMessage::Logon {
                encrypt_method,
                heart_bt_int: u32::try_from(required_u64(message, tag::HEART_BT_INT)?)
                    .map_err(|_| invalid(tag::HEART_BT_INT, message))?,
                reset_seq_num: message.get(tag::RESET_SEQ_NUM_FLAG) == Some(b"Y"),
                username: optional_string(message, tag::USERNAME),
                password: optional_string(message, tag::PASSWORD),
            })
        }
        MsgType::Logout => Ok(SessionMessage::Logout {
            text: optional_string(message, tag::TEXT),
        }),
        MsgType::Heartbeat => Ok(SessionMessage::Heartbeat {
            test_req_id: optional_string(message, tag::TEST_REQ_ID),
        }),
        MsgType::TestRequest => Ok(SessionMessage::TestRequest {
            test_req_id: required_string(message, tag::TEST_REQ_ID)?,
        }),
        MsgType::ResendRequest => Ok(SessionMessage::ResendRequest {
            begin_seq_no: required_u64(message, tag::BEGIN_SEQ_NO)?,
            end_seq_no: required_u64(message, tag::END_SEQ_NO)?,
        }),
        MsgType::SequenceReset => Ok(SessionMessage::SequenceReset {
            gap_fill: message.get(tag::GAP_FILL_FLAG) == Some(b"Y"),
            new_seq_no: required_u64(message, tag::NEW_SEQ_NO)?,
        }),
        MsgType::Reject => Ok(SessionMessage::Reject {
            ref_seq_num: required_u64(message, tag::REF_SEQ_NUM)?,
            ref_tag_id: optional_u64(message, tag::REF_TAG_ID)
                .map(u32::try_from)
                .transpose()
                .map_err(|_| invalid(tag::REF_TAG_ID, message))?,
            ref_msg_type: optional_string(message, tag::REF_MSG_TYPE),
            session_reject_reason: optional_u64(message, tag::SESSION_REJECT_REASON)
                .map(u32::try_from)
                .transpose()
                .map_err(|_| invalid(tag::SESSION_REJECT_REASON, message))?,
            text: optional_string(message, tag::TEXT),
        }),
        other => Err(CodecError::UnsupportedMsgType(format!(
            "{} is not a session message",
            other.as_str()
        ))),
    }
}

/// Decode an application-level message.
///
/// # Errors
///
/// A required field is missing or invalid, or the type is session-level.
pub fn decode_application(message: &ParsedMessage) -> Result<ApplicationMessage, CodecError> {
    let msg_type = message.msg_type()?;
    match msg_type {
        MsgType::NewOrderSingle => {
            let ord_type = required_ord_type(message)?;
            Ok(ApplicationMessage::NewOrderSingle(NewOrderSingle {
                cl_ord_id: required_string(message, tag::CL_ORD_ID)?,
                account: optional_string(message, tag::ACCOUNT),
                symbol: required_string(message, tag::SYMBOL)?,
                side: required_side(message)?,
                order_qty: required_i64(message, tag::ORDER_QTY)?,
                ord_type,
                price: required_price_for(ord_type, message)?,
                time_in_force: required_time_in_force(message)?,
                transact_time: required_string(message, tag::TRANSACT_TIME)?,
                handl_inst: optional_char(message, tag::HANDL_INST),
            }))
        }
        MsgType::OrderCancelRequest => {
            Ok(ApplicationMessage::OrderCancelRequest(OrderCancelRequest {
                cl_ord_id: required_string(message, tag::CL_ORD_ID)?,
                orig_cl_ord_id: required_string(message, tag::ORIG_CL_ORD_ID)?,
                symbol: required_string(message, tag::SYMBOL)?,
                side: required_side(message)?,
                order_qty: optional_i64(message, tag::ORDER_QTY),
                transact_time: required_string(message, tag::TRANSACT_TIME)?,
            }))
        }
        MsgType::OrderCancelReplaceRequest => {
            let ord_type = required_ord_type(message)?;
            Ok(ApplicationMessage::OrderCancelReplaceRequest(
                OrderCancelReplaceRequest {
                    cl_ord_id: required_string(message, tag::CL_ORD_ID)?,
                    orig_cl_ord_id: required_string(message, tag::ORIG_CL_ORD_ID)?,
                    account: optional_string(message, tag::ACCOUNT),
                    symbol: required_string(message, tag::SYMBOL)?,
                    side: required_side(message)?,
                    order_qty: required_i64(message, tag::ORDER_QTY)?,
                    ord_type,
                    price: required_price_for(ord_type, message)?,
                    time_in_force: required_time_in_force(message)?,
                    transact_time: required_string(message, tag::TRANSACT_TIME)?,
                },
            ))
        }
        MsgType::ExecutionReport => Ok(ApplicationMessage::ExecutionReport(ExecutionReport {
            exec_id: required_string(message, tag::EXEC_ID)?,
            order_id: required_string(message, tag::ORDER_ID)?,
            cl_ord_id: required_string(message, tag::CL_ORD_ID)?,
            exec_type: required_exec_type(message)?,
            ord_status: required_ord_status(message)?,
            symbol: required_string(message, tag::SYMBOL)?,
            side: required_side(message)?,
            order_qty: required_i64(message, tag::ORDER_QTY)?,
            last_qty: optional_i64(message, tag::LAST_QTY),
            last_px: optional_price(message, tag::LAST_PX)?,
            leaves_qty: required_i64(message, tag::LEAVES_QTY)?,
            cum_qty: required_i64(message, tag::CUM_QTY)?,
            avg_px: required_price(message, tag::AVG_PX)?,
            account: optional_string(message, tag::ACCOUNT),
            transact_time: optional_string(message, tag::TRANSACT_TIME),
            text: optional_string(message, tag::TEXT),
        })),
        MsgType::OrderCancelReject => {
            Ok(ApplicationMessage::OrderCancelReject(OrderCancelReject {
                order_id: required_string(message, tag::ORDER_ID)?,
                cl_ord_id: required_string(message, tag::CL_ORD_ID)?,
                orig_cl_ord_id: required_string(message, tag::ORIG_CL_ORD_ID)?,
                ord_status: required_ord_status(message)?,
                cxl_rej_response_to: required_cxl_rej_response_to(message)?,
                cxl_rej_reason: optional_u64(message, tag::CXL_REJ_REASON)
                    .map(u32::try_from)
                    .transpose()
                    .map_err(|_| invalid(tag::CXL_REJ_REASON, message))?,
                text: optional_string(message, tag::TEXT),
            }))
        }
        MsgType::OrderStatusRequest => {
            Ok(ApplicationMessage::OrderStatusRequest(OrderStatusRequest {
                cl_ord_id: required_string(message, tag::CL_ORD_ID)?,
                symbol: required_string(message, tag::SYMBOL)?,
                side: required_side(message)?,
            }))
        }
        other => Err(CodecError::UnsupportedMsgType(format!(
            "{} is not an application message",
            other.as_str()
        ))),
    }
}

/// Decode either category.
///
/// # Errors
///
/// As [`decode_session`] and [`decode_application`].
pub fn decode(message: &ParsedMessage) -> Result<Message, CodecError> {
    if message.msg_type()?.is_admin() {
        decode_session(message).map(Message::Session)
    } else {
        decode_application(message).map(Message::Application)
    }
}

/// Append an application message to `builder`.
#[must_use]
pub fn encode_application(builder: MessageBuilder, message: &ApplicationMessage) -> MessageBuilder {
    match message {
        ApplicationMessage::NewOrderSingle(order) => {
            let builder = builder
                .body_str(tag::CL_ORD_ID, &order.cl_ord_id)
                .body_str(tag::SYMBOL, &order.symbol)
                .body(tag::SIDE, side_byte(order.side).to_vec())
                .body_int(tag::ORDER_QTY, order.order_qty)
                .body(tag::ORD_TYPE, order.ord_type.as_char().to_string())
                .body(
                    tag::TIME_IN_FORCE,
                    order.time_in_force.as_char().to_string(),
                )
                .body_str(tag::TRANSACT_TIME, &order.transact_time);
            let builder = optional_account(builder, order.account.as_deref());
            let builder = match order.price {
                Some(price) => builder.body_str(tag::PRICE, &format_price(price)),
                None => builder,
            };
            match order.handl_inst {
                Some(value) => builder.body(tag::HANDL_INST, value.to_string()),
                None => builder,
            }
        }
        ApplicationMessage::OrderCancelRequest(order) => {
            let builder = builder
                .body_str(tag::CL_ORD_ID, &order.cl_ord_id)
                .body_str(tag::ORIG_CL_ORD_ID, &order.orig_cl_ord_id)
                .body_str(tag::SYMBOL, &order.symbol)
                .body(tag::SIDE, side_byte(order.side).to_vec())
                .body_str(tag::TRANSACT_TIME, &order.transact_time);
            match order.order_qty {
                Some(qty) => builder.body_int(tag::ORDER_QTY, qty),
                None => builder,
            }
        }
        ApplicationMessage::OrderCancelReplaceRequest(order) => {
            let builder = builder
                .body_str(tag::CL_ORD_ID, &order.cl_ord_id)
                .body_str(tag::ORIG_CL_ORD_ID, &order.orig_cl_ord_id)
                .body_str(tag::SYMBOL, &order.symbol)
                .body(tag::SIDE, side_byte(order.side).to_vec())
                .body_int(tag::ORDER_QTY, order.order_qty)
                .body(tag::ORD_TYPE, order.ord_type.as_char().to_string())
                .body(
                    tag::TIME_IN_FORCE,
                    order.time_in_force.as_char().to_string(),
                )
                .body_str(tag::TRANSACT_TIME, &order.transact_time);
            let builder = optional_account(builder, order.account.as_deref());
            match order.price {
                Some(price) => builder.body_str(tag::PRICE, &format_price(price)),
                None => builder,
            }
        }
        ApplicationMessage::ExecutionReport(report) => {
            let builder = builder
                .body_str(tag::EXEC_ID, &report.exec_id)
                .body_str(tag::ORDER_ID, &report.order_id)
                .body_str(tag::CL_ORD_ID, &report.cl_ord_id)
                .body(tag::EXEC_TYPE, report.exec_type.as_char().to_string())
                .body(tag::ORD_STATUS, report.ord_status.as_char().to_string())
                .body_str(tag::SYMBOL, &report.symbol)
                .body(tag::SIDE, side_byte(report.side).to_vec())
                .body_int(tag::ORDER_QTY, report.order_qty);
            let builder = match report.last_px {
                Some(price) => builder.body_str(tag::LAST_PX, &format_price(price)),
                None => builder,
            };
            let builder = match report.last_qty {
                Some(qty) => builder.body_int(tag::LAST_QTY, qty),
                None => builder,
            };
            let builder = builder
                .body_int(tag::LEAVES_QTY, report.leaves_qty)
                .body_int(tag::CUM_QTY, report.cum_qty)
                .body_str(tag::AVG_PX, &format_price(report.avg_px));
            let builder = optional_account(builder, report.account.as_deref());
            let builder = match &report.transact_time {
                Some(value) => builder.body_str(tag::TRANSACT_TIME, value),
                None => builder,
            };
            match &report.text {
                Some(value) => builder.body_str(tag::TEXT, value),
                None => builder,
            }
        }
        ApplicationMessage::OrderCancelReject(reject) => {
            let builder = builder
                .body_str(tag::ORDER_ID, &reject.order_id)
                .body_str(tag::CL_ORD_ID, &reject.cl_ord_id)
                .body_str(tag::ORIG_CL_ORD_ID, &reject.orig_cl_ord_id)
                .body(tag::ORD_STATUS, reject.ord_status.as_char().to_string())
                .body(
                    tag::CXL_REJ_RESPONSE_TO,
                    reject.cxl_rej_response_to.as_char().to_string(),
                );
            let builder = match reject.cxl_rej_reason {
                Some(reason) => builder.body_int(tag::CXL_REJ_REASON, reason),
                None => builder,
            };
            match &reject.text {
                Some(value) => builder.body_str(tag::TEXT, value),
                None => builder,
            }
        }
        ApplicationMessage::OrderStatusRequest(request) => builder
            .body_str(tag::CL_ORD_ID, &request.cl_ord_id)
            .body_str(tag::SYMBOL, &request.symbol)
            .body(tag::SIDE, side_byte(request.side).to_vec()),
    }
}

fn optional_account(builder: MessageBuilder, account: Option<&str>) -> MessageBuilder {
    match account {
        Some(value) => builder.body_str(tag::ACCOUNT, value),
        None => builder,
    }
}

fn side_byte(side: Side) -> &'static [u8] {
    match side {
        Side::Buy => b"1",
        Side::Sell => b"2",
    }
}

/// Parse a FIX decimal price into hundredths.
///
/// `"100"`, `"100.2"` and `"100.25"` are `10_000`, `10_020` and `10_025`.
/// More than two decimal places is accepted only when the extra digits are
/// zero (`"100.2500"`): the engine's price is an integer tick and silently
/// rounding would change an order.
///
/// # Errors
///
/// The value is empty, negative, has more than one dot, more than two
/// fractional digits, a non-digit, or overflows `i64`.
pub fn parse_price(value: &str) -> Result<Price, CodecError> {
    let invalid = || CodecError::Invalid {
        tag: tag::PRICE,
        value: value.to_owned(),
    };
    if value.is_empty() || value.starts_with('-') {
        return Err(invalid());
    }

    let (whole, fraction) = match value.split_once('.') {
        Some((whole, fraction)) => (whole, fraction),
        None => (value, ""),
    };
    if whole.is_empty() || fraction.contains('.') {
        return Err(invalid());
    }
    if !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(invalid());
    }
    // QuickFIX/J may print more than two decimal places depending on its
    // configuration. Extra digits are accepted only when they are zero; a
    // non-zero third digit would have to be rounded, and this build never
    // rounds a price.
    if fraction.len() > 2 && fraction.as_bytes()[2..].iter().any(|byte| *byte != b'0') {
        return Err(invalid());
    }

    let whole: i64 = whole.parse().map_err(|_| invalid())?;
    let fraction = fraction.get(..fraction.len().min(2)).unwrap_or(fraction);
    let fraction: i64 = match fraction.len() {
        0 => 0,
        1 => fraction.parse::<i64>().map_err(|_| invalid())? * 10,
        2 => fraction.parse().map_err(|_| invalid())?,
        _ => return Err(invalid()),
    };
    whole
        .checked_mul(100)
        .and_then(|ticks| ticks.checked_add(fraction))
        .ok_or_else(invalid)
}

/// Format hundredths as a FIX decimal price.
#[must_use]
pub fn format_price(ticks: Price) -> String {
    let sign = if ticks < 0 { "-" } else { "" };
    let absolute = ticks.unsigned_abs();
    format!("{sign}{}.{:02}", absolute / 100, absolute % 100)
}

fn required_price_for(
    ord_type: OrdType,
    message: &ParsedMessage,
) -> Result<Option<Price>, CodecError> {
    match ord_type {
        OrdType::Limit => required_price(message, tag::PRICE).map(Some),
        OrdType::Market => optional_price(message, tag::PRICE),
    }
}

fn required_price(message: &ParsedMessage, tag: u32) -> Result<Price, CodecError> {
    parse_price(message.get_str(tag)?).map_err(|_| invalid(tag, message))
}

fn optional_price(message: &ParsedMessage, tag: u32) -> Result<Option<Price>, CodecError> {
    match message.get(tag) {
        Some(value) => Ok(Some(parse_price(
            std::str::from_utf8(value).map_err(|_| invalid(tag, message))?,
        )?)),
        None => Ok(None),
    }
}

fn required_string(message: &ParsedMessage, tag: u32) -> Result<String, CodecError> {
    Ok(message.get_str(tag)?.to_owned())
}

fn optional_string(message: &ParsedMessage, tag: u32) -> Option<String> {
    message
        .get(tag)
        .and_then(|value| std::str::from_utf8(value).ok())
        .map(str::to_owned)
}

fn required_u64(message: &ParsedMessage, tag: u32) -> Result<u64, CodecError> {
    message.get_u64(tag)
}

fn optional_u64(message: &ParsedMessage, tag: u32) -> Option<u64> {
    message.get(tag).and_then(|value| {
        std::str::from_utf8(value)
            .ok()
            .and_then(|value| value.parse().ok())
    })
}

fn required_i64(message: &ParsedMessage, tag: u32) -> Result<i64, CodecError> {
    message.get_i64(tag)
}

fn optional_i64(message: &ParsedMessage, tag: u32) -> Option<i64> {
    message.get(tag).and_then(|value| {
        std::str::from_utf8(value)
            .ok()
            .and_then(|value| value.parse().ok())
    })
}

fn optional_char(message: &ParsedMessage, tag: u32) -> Option<char> {
    message
        .get(tag)
        .and_then(|value| std::str::from_utf8(value).ok())
        .and_then(|value| value.chars().next())
}

fn required_side(message: &ParsedMessage) -> Result<Side, CodecError> {
    match message.get(tag::SIDE) {
        Some(b"1") => Ok(Side::Buy),
        Some(b"2") => Ok(Side::Sell),
        _ => Err(invalid(tag::SIDE, message)),
    }
}

fn required_ord_type(message: &ParsedMessage) -> Result<OrdType, CodecError> {
    message
        .get(tag::ORD_TYPE)
        .and_then(|value| value.first().copied())
        .and_then(OrdType::parse)
        .ok_or_else(|| invalid(tag::ORD_TYPE, message))
}

fn required_time_in_force(message: &ParsedMessage) -> Result<TimeInForce, CodecError> {
    message
        .get(tag::TIME_IN_FORCE)
        .and_then(|value| value.first().copied())
        .and_then(TimeInForce::parse)
        .ok_or_else(|| invalid(tag::TIME_IN_FORCE, message))
}

fn required_exec_type(message: &ParsedMessage) -> Result<ExecType, CodecError> {
    message
        .get(tag::EXEC_TYPE)
        .and_then(|value| value.first().copied())
        .and_then(ExecType::parse)
        .ok_or_else(|| invalid(tag::EXEC_TYPE, message))
}

fn required_ord_status(message: &ParsedMessage) -> Result<OrdStatus, CodecError> {
    message
        .get(tag::ORD_STATUS)
        .and_then(|value| value.first().copied())
        .and_then(OrdStatus::parse)
        .ok_or_else(|| invalid(tag::ORD_STATUS, message))
}

fn required_cxl_rej_response_to(message: &ParsedMessage) -> Result<CxlRejResponseTo, CodecError> {
    message
        .get(tag::CXL_REJ_RESPONSE_TO)
        .and_then(|value| value.first().copied())
        .and_then(CxlRejResponseTo::parse)
        .ok_or_else(|| invalid(tag::CXL_REJ_RESPONSE_TO, message))
}

fn invalid(tag: u32, message: &ParsedMessage) -> CodecError {
    CodecError::Invalid {
        tag,
        value: message
            .get(tag)
            .map(|value| String::from_utf8_lossy(value).into_owned())
            .unwrap_or_default(),
    }
}

/// The body fields of a message as `(tag, value)` pairs, for a caller that
/// needs to inspect what was not decoded.
#[must_use]
pub fn raw_fields(message: &ParsedMessage) -> Vec<(u32, &[u8])> {
    message
        .fields()
        .iter()
        .map(|field: &Field| (field.tag, field.value.as_slice()))
        .collect()
}

impl fmt::Display for OrdType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.as_char())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::parse;

    fn new_order() -> Vec<u8> {
        let mut builder = MessageBuilder::new(
            MsgType::NewOrderSingle,
            "PEER",
            "DEEPBOOK",
            2,
            "20261025-12:00:00.000",
        )
        .body_str(tag::CL_ORD_ID, "C-1")
        .body_str(tag::SYMBOL, "AAPL")
        .body(tag::SIDE, b"1".to_vec())
        .body_int(tag::ORDER_QTY, 100)
        .body(tag::ORD_TYPE, "2".to_string())
        .body(tag::TIME_IN_FORCE, "0".to_string())
        .body_str(tag::TRANSACT_TIME, "20261025-12:00:00.000")
        .body_str(tag::PRICE, "100.25");
        builder = builder.body_str(tag::ACCOUNT, "A-1");
        builder.build()
    }

    #[test]
    fn a_new_order_single_decodes() {
        let parsed = parse(&new_order()).expect("parses");
        let message = decode_application(&parsed).expect("decodes");
        let ApplicationMessage::NewOrderSingle(order) = message else {
            panic!("wrong variant");
        };
        assert_eq!("C-1", order.cl_ord_id);
        assert_eq!("A-1", order.account.expect("account"));
        assert_eq!("AAPL", order.symbol);
        assert_eq!(Side::Buy, order.side);
        assert_eq!(100, order.order_qty);
        assert_eq!(OrdType::Limit, order.ord_type);
        assert_eq!(Some(10_025), order.price);
        assert_eq!(TimeInForce::Day, order.time_in_force);
    }

    #[test]
    fn a_missing_required_field_names_the_tag() {
        let raw = MessageBuilder::new(
            MsgType::NewOrderSingle,
            "PEER",
            "DEEPBOOK",
            2,
            "20261025-12:00:00.000",
        )
        .body_str(tag::SYMBOL, "AAPL")
        .body(tag::SIDE, b"1".to_vec())
        .body_int(tag::ORDER_QTY, 100)
        .body(tag::ORD_TYPE, "2".to_string())
        .body_str(tag::PRICE, "100.25")
        .body(tag::TIME_IN_FORCE, "0".to_string())
        .body_str(tag::TRANSACT_TIME, "20261025-12:00:00.000")
        .build();
        let parsed = parse(&raw).expect("parses");
        assert_eq!(
            Err(CodecError::Missing {
                tag: tag::CL_ORD_ID
            }),
            decode_application(&parsed)
        );
    }

    #[test]
    fn a_limit_order_needs_a_price() {
        let raw = MessageBuilder::new(
            MsgType::NewOrderSingle,
            "PEER",
            "DEEPBOOK",
            2,
            "20261025-12:00:00.000",
        )
        .body_str(tag::CL_ORD_ID, "C-1")
        .body_str(tag::SYMBOL, "AAPL")
        .body(tag::SIDE, b"1".to_vec())
        .body_int(tag::ORDER_QTY, 100)
        .body(tag::ORD_TYPE, "2".to_string())
        .body(tag::TIME_IN_FORCE, "0".to_string())
        .body_str(tag::TRANSACT_TIME, "20261025-12:00:00.000")
        .build();
        let parsed = parse(&raw).expect("parses");
        assert!(matches!(
            decode_application(&parsed),
            Err(CodecError::Missing { tag: tag::PRICE })
        ));
    }

    #[test]
    fn an_execution_report_round_trips() {
        let report = ExecutionReport {
            exec_id: "E-1".to_owned(),
            order_id: "O-1".to_owned(),
            cl_ord_id: "C-1".to_owned(),
            exec_type: ExecType::Trade,
            ord_status: OrdStatus::PartiallyFilled,
            symbol: "AAPL".to_owned(),
            side: Side::Sell,
            order_qty: 100,
            last_qty: Some(30),
            last_px: Some(10_025),
            leaves_qty: 70,
            cum_qty: 30,
            avg_px: 10_025,
            account: Some("A-1".to_owned()),
            transact_time: Some("20261025-12:00:00.000".to_owned()),
            text: None,
        };
        let builder = encode_application(
            MessageBuilder::new(
                MsgType::ExecutionReport,
                "DEEPBOOK",
                "PEER",
                3,
                "20261025-12:00:00.001",
            ),
            &ApplicationMessage::ExecutionReport(report.clone()),
        );
        let parsed = parse(&builder.build()).expect("parses");
        let decoded = decode_application(&parsed).expect("decodes");
        assert_eq!(ApplicationMessage::ExecutionReport(report), decoded);
    }

    #[test]
    fn prices_are_integer_hundredths() {
        assert_eq!(10_000, parse_price("100").expect("whole"));
        assert_eq!(10_020, parse_price("100.2").expect("one decimal"));
        assert_eq!(10_025, parse_price("100.25").expect("two decimals"));
        assert_eq!(0, parse_price("0.00").expect("zero"));
        assert!(parse_price("100.255").is_err());
        assert!(parse_price("100.2501").is_err());
        assert_eq!(10_025, parse_price("100.2500").expect("zero padding"));
        assert!(parse_price("-1").is_err());
        assert!(parse_price("1.2.3").is_err());
        assert!(parse_price("").is_err());
        assert_eq!("100.25", format_price(10_025));
        assert_eq!("0.05", format_price(5));
    }

    #[test]
    fn session_and_application_categories_do_not_mix() {
        let parsed = parse(&new_order()).expect("parses");
        assert!(matches!(
            decode_session(&parsed),
            Err(CodecError::UnsupportedMsgType(_))
        ));
    }
}
