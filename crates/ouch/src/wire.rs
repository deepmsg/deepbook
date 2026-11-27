//! DeepOUCH/1.0 fixed-width binary order-entry wire (ADR 0012 §5).
//!
//! Every frame is a 4-byte header followed by a fixed payload:
//!
//! ```text
//! payload_len: u16 LE | msg_type: u8 | version: u8 | payload
//! ```
//!
//! All multi-byte integers are little-endian. The codec is structural: it
//! validates the header, lengths and reserved bytes, and returns the raw enum
//! bytes for the gateway to turn into a `RejectMalformed` when they are not
//! one this build knows. No `unsafe`, no alignment requirement.
#![forbid(unsafe_code)]

use std::fmt;

/// The wire version.
pub const VERSION: u8 = 1;
/// The header length.
pub const HEADER_LEN: usize = 4;
/// The largest payload the protocol accepts.
pub const MAX_PAYLOAD: usize = 80;

/// Message type bytes.
pub mod msg_type {
    pub const LOGIN_REQUEST: u8 = b'L';
    pub const LOGIN_ACCEPTED: u8 = b'A';
    pub const LOGIN_REJECTED: u8 = b'J';
    pub const CLIENT_HEARTBEAT: u8 = b'R';
    pub const SERVER_HEARTBEAT: u8 = b'H';
    pub const LOGOUT: u8 = b'G';
    pub const ENTER_ORDER: u8 = b'N';
    pub const CANCEL_ORDER: u8 = b'X';
    pub const REPLACE_ORDER: u8 = b'M';
    pub const ORDER_REPLACED: u8 = b'U';
    pub const ORDER_ACCEPTED: u8 = b'E';
    pub const ORDER_EXECUTED: u8 = b'F';
    pub const ORDER_CANCELED: u8 = b'C';
    pub const ORDER_REJECTED: u8 = b'D';
}

/// `side` wire values.
pub mod side {
    pub const BUY: u8 = 1;
    pub const SELL: u8 = 2;
}

/// `ord_type` wire values.
pub mod ord_type {
    pub const LIMIT: u8 = 1;
    pub const MARKET: u8 = 2;
}

/// `tif` wire values.
pub mod tif {
    pub const DAY: u8 = 0;
    pub const GTC: u8 = 1;
    pub const IOC: u8 = 2;
    pub const FOK: u8 = 3;
}

/// Outbound `state` values.
pub mod state {
    pub const NEW: u8 = 0;
    pub const PARTIAL: u8 = 1;
    pub const FILLED: u8 = 2;
    pub const CANCELED: u8 = 3;
    pub const REJECTED: u8 = 4;
}

/// Outbound `liquidity` values.
pub mod liquidity {
    pub const NONE: u8 = 0;
    pub const MAKER: u8 = 1;
    pub const TAKER: u8 = 2;
}

/// `LoginRejected.reason` values.
pub mod login_rejected {
    pub const INVALID_TOKEN: u8 = 1;
    pub const UNKNOWN_SEAT: u8 = 2;
    pub const SEAT_IN_USE: u8 = 3;
    pub const SEQ_OUT_OF_RANGE: u8 = 4;
    pub const CLUSTER_DOWN: u8 = 5;
    pub const ACCOUNT_CONFLICT: u8 = 6;
}

/// `Logout.reason` values.
pub mod logout {
    pub const CLIENT_REQUESTED: u8 = 0;
    pub const HEARTBEAT_TIMEOUT: u8 = 1;
    pub const PROTOCOL_VIOLATION: u8 = 2;
    pub const RESYNC_REQUIRED: u8 = 3;
    pub const FENCED: u8 = 4;
    pub const SLOW_CONSUMER: u8 = 5;
    pub const SERVER_SHUTDOWN: u8 = 6;
}

/// `OrderCanceled.reason` values.
pub mod canceled {
    pub const USER_REQUESTED: u8 = 0;
    pub const IMMEDIATE_OR_CANCEL: u8 = 1;
    pub const SELF_TRADE_PREVENTION: u8 = 2;
    pub const DAY_EXPIRED: u8 = 3;
}

/// `OrderRejected.reason` values. Engine reasons `1..=17` are the
/// `engine_core::wire` encoding; `101..=104` are service-level malformed
/// inputs.
pub mod rejected {
    pub const DUPLICATE_CL_ORD_ID: u8 = 1;
    pub const UNKNOWN_ORDER: u8 = 2;
    pub const ORDER_NOT_OPEN: u8 = 3;
    pub const INVALID_QUANTITY: u8 = 4;
    pub const INVALID_PRICE: u8 = 5;
    pub const POST_ONLY_WOULD_CROSS: u8 = 6;
    pub const FOK_NOT_FILLABLE: u8 = 7;
    pub const SELF_TRADE_PREVENTION: u8 = 8;
    pub const KILL_SWITCH: u8 = 9;
    pub const MAX_ORDER_QTY: u8 = 10;
    pub const MAX_NOTIONAL: u8 = 11;
    pub const PRICE_BAND: u8 = 12;
    pub const FAT_FINGER: u8 = 13;
    pub const MAX_OPEN_ORDERS: u8 = 14;
    pub const BOOK_FULL: u8 = 15;
    pub const UNKNOWN_SYMBOL: u8 = 16;
    pub const ID_WINDOW_FULL: u8 = 17;
    pub const ZERO_OR_INVALID_CL_ORD_ID: u8 = 101;
    pub const QTY_OVERFLOW_OR_ZERO: u8 = 102;
    pub const INVALID_WIRE_PRICE: u8 = 103;
    pub const INVALID_WIRE_ENUM: u8 = 104;
}

/// A frame decode failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameError {
    /// The buffer ends before the header or payload it promised.
    Truncated,
    /// The version byte is not [`VERSION`].
    BadVersion(u8),
    /// The type byte is not one this build knows.
    UnknownType(u8),
    /// The declared payload is larger than [`MAX_PAYLOAD`].
    PayloadTooLarge(usize),
    /// The payload length is not the one this type defines.
    BadLength { expected: usize, found: usize },
    /// A reserved byte is not zero.
    NonZeroReserved,
    /// A boolean-shaped byte is not `0` or `1`.
    BadEnum(u8),
}

impl fmt::Display for FrameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated => write!(formatter, "truncated DeepOUCH frame"),
            Self::BadVersion(version) => write!(formatter, "unknown DeepOUCH version {version}"),
            Self::UnknownType(kind) => write!(formatter, "unknown DeepOUCH type {kind:#04x}"),
            Self::PayloadTooLarge(length) => {
                write!(formatter, "DeepOUCH payload length {length} is too large")
            }
            Self::BadLength { expected, found } => {
                write!(
                    formatter,
                    "DeepOUCH payload length {found}, expected {expected}"
                )
            }
            Self::NonZeroReserved => write!(formatter, "DeepOUCH reserved byte is not zero"),
            Self::BadEnum(value) => write!(formatter, "unknown DeepOUCH enum value {value}"),
        }
    }
}

impl std::error::Error for FrameError {}

/// One DeepOUCH/1.0 message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Message {
    /// Client login.
    LoginRequest {
        /// The seat.
        seat_id: u16,
        /// The 32-byte pre-shared token.
        token: [u8; 32],
        /// `0` = replay from the oldest of the current generation,
        /// `u64::MAX` = live only, otherwise a sequence.
        requested_seq: u64,
    },
    /// Login accepted.
    LoginAccepted {
        /// The seat.
        seat_id: u16,
        /// The journal generation.
        generation: u64,
        /// The first outbound business sequence.
        next_seq: u64,
    },
    /// Login refused.
    LoginRejected {
        /// A [`login_rejected`] reason.
        reason: u8,
    },
    /// Client heartbeat.
    ClientHeartbeat,
    /// Server heartbeat.
    ServerHeartbeat,
    /// Session logout.
    Logout {
        /// A [`logout`] reason.
        reason: u8,
    },
    /// New order.
    EnterOrder {
        /// Client order id.
        cl_ord_id: u64,
        /// Price in ticks; `0` for a market order.
        price: i64,
        /// Quantity.
        qty: u64,
        /// Symbol.
        symbol: u16,
        /// A [`side`] value.
        side: u8,
        /// An [`ord_type`] value.
        ord_type: u8,
        /// A [`tif`] value.
        tif: u8,
        /// Bit 0: post-only; bits 1..7 reserved.
        flags: u8,
    },
    /// Cancel.
    CancelOrder {
        /// The cancel request id.
        cl_ord_id: u64,
        /// The order to cancel.
        orig_cl_ord_id: u64,
        /// Symbol.
        symbol: u16,
    },
    /// Replace.
    ReplaceOrder {
        /// The replace request id.
        cl_ord_id: u64,
        /// The order to replace.
        orig_cl_ord_id: u64,
        /// New price.
        new_price: i64,
        /// New quantity.
        new_qty: u64,
        /// Symbol.
        symbol: u16,
        /// An [`ord_type`] value.
        ord_type: u8,
        /// A [`tif`] value.
        tif: u8,
        /// Bit 0: post-only; bits 1..7 reserved.
        flags: u8,
    },
    /// Replacement acknowledged.
    OrderReplaced {
        /// Seat sequence.
        seq: u64,
        /// The replace request id.
        req_cl_ord_id: u64,
        /// The order that was replaced.
        orig_cl_ord_id: u64,
        /// The engine order id.
        order_id: u32,
        /// Price.
        price: i64,
        /// Quantity.
        qty: u64,
        /// Remaining quantity.
        leaves_qty: u64,
        /// A [`state`] value.
        state: u8,
    },
    /// New order acknowledged (also the resync restatement).
    OrderAccepted {
        /// Seat sequence.
        seq: u64,
        /// Client order id.
        cl_ord_id: u64,
        /// Engine order id.
        order_id: u32,
        /// Symbol.
        symbol: u16,
        /// A [`side`] value.
        side: u8,
        /// A [`state`] value.
        state: u8,
        /// Price.
        price: i64,
        /// Quantity.
        qty: u64,
        /// Remaining quantity.
        leaves_qty: u64,
        /// The engine log position.
        log_pos: i64,
        /// Cumulative filled quantity.
        cum_qty: u64,
        /// Average fill price.
        avg_px: i64,
    },
    /// A fill.
    OrderExecuted {
        /// Seat sequence.
        seq: u64,
        /// Client order id.
        cl_ord_id: u64,
        /// Engine order id.
        order_id: u32,
        /// Fill price.
        fill_price: i64,
        /// Fill quantity.
        fill_qty: u64,
        /// Remaining quantity.
        leaves_qty: u64,
        /// A [`state`] value.
        state: u8,
        /// A [`liquidity`] value.
        liquidity: u8,
    },
    /// Canceled.
    OrderCanceled {
        /// Seat sequence.
        seq: u64,
        /// The cancel request id.
        cl_ord_id: u64,
        /// The canceled order.
        orig_cl_ord_id: u64,
        /// Engine order id.
        order_id: u32,
        /// A [`canceled`] reason.
        reason: u8,
    },
    /// Refused.
    OrderRejected {
        /// Seat sequence.
        seq: u64,
        /// Client order id.
        cl_ord_id: u64,
        /// Original id, or `0`.
        orig_cl_ord_id: u64,
        /// A [`rejected`] reason.
        reason: u8,
        /// Whether this answers a cancel/replace.
        is_cancel_reject: bool,
    },
}

impl Message {
    /// The type byte.
    #[must_use]
    pub const fn kind(&self) -> u8 {
        match self {
            Self::LoginRequest { .. } => msg_type::LOGIN_REQUEST,
            Self::LoginAccepted { .. } => msg_type::LOGIN_ACCEPTED,
            Self::LoginRejected { .. } => msg_type::LOGIN_REJECTED,
            Self::ClientHeartbeat => msg_type::CLIENT_HEARTBEAT,
            Self::ServerHeartbeat => msg_type::SERVER_HEARTBEAT,
            Self::Logout { .. } => msg_type::LOGOUT,
            Self::EnterOrder { .. } => msg_type::ENTER_ORDER,
            Self::CancelOrder { .. } => msg_type::CANCEL_ORDER,
            Self::ReplaceOrder { .. } => msg_type::REPLACE_ORDER,
            Self::OrderReplaced { .. } => msg_type::ORDER_REPLACED,
            Self::OrderAccepted { .. } => msg_type::ORDER_ACCEPTED,
            Self::OrderExecuted { .. } => msg_type::ORDER_EXECUTED,
            Self::OrderCanceled { .. } => msg_type::ORDER_CANCELED,
            Self::OrderRejected { .. } => msg_type::ORDER_REJECTED,
        }
    }
}

/// Encode one frame.
///
/// # Errors
///
/// [`FrameError::PayloadTooLarge`] if the payload does not fit the header.
pub fn encode(message: &Message) -> Result<Vec<u8>, FrameError> {
    let mut payload = Vec::with_capacity(72);
    match message {
        Message::LoginRequest {
            seat_id,
            token,
            requested_seq,
        } => {
            put_u16(&mut payload, *seat_id);
            put_u16(&mut payload, 0);
            payload.extend_from_slice(token);
            put_u64(&mut payload, *requested_seq);
        }
        Message::LoginAccepted {
            seat_id,
            generation,
            next_seq,
        } => {
            put_u16(&mut payload, *seat_id);
            put_u16(&mut payload, 0);
            put_u64(&mut payload, *generation);
            put_u64(&mut payload, *next_seq);
        }
        Message::LoginRejected { reason } => {
            payload.push(*reason);
            payload.extend_from_slice(&[0; 7]);
        }
        Message::ClientHeartbeat | Message::ServerHeartbeat => {}
        Message::Logout { reason } => {
            payload.push(*reason);
            payload.extend_from_slice(&[0; 7]);
        }
        Message::EnterOrder {
            cl_ord_id,
            price,
            qty,
            symbol,
            side,
            ord_type,
            tif,
            flags,
        } => {
            put_u64(&mut payload, *cl_ord_id);
            put_i64(&mut payload, *price);
            put_u64(&mut payload, *qty);
            put_u16(&mut payload, *symbol);
            payload.push(*side);
            payload.push(*ord_type);
            payload.push(*tif);
            payload.push(*flags);
            payload.extend_from_slice(&[0; 2]);
        }
        Message::CancelOrder {
            cl_ord_id,
            orig_cl_ord_id,
            symbol,
        } => {
            put_u64(&mut payload, *cl_ord_id);
            put_u64(&mut payload, *orig_cl_ord_id);
            put_u16(&mut payload, *symbol);
            payload.extend_from_slice(&[0; 6]);
        }
        Message::ReplaceOrder {
            cl_ord_id,
            orig_cl_ord_id,
            new_price,
            new_qty,
            symbol,
            ord_type,
            tif,
            flags,
        } => {
            put_u64(&mut payload, *cl_ord_id);
            put_u64(&mut payload, *orig_cl_ord_id);
            put_i64(&mut payload, *new_price);
            put_u64(&mut payload, *new_qty);
            put_u16(&mut payload, *symbol);
            payload.push(*ord_type);
            payload.push(*tif);
            payload.push(*flags);
            payload.extend_from_slice(&[0; 3]);
        }
        Message::OrderReplaced {
            seq,
            req_cl_ord_id,
            orig_cl_ord_id,
            order_id,
            price,
            qty,
            leaves_qty,
            state,
        } => {
            put_u64(&mut payload, *seq);
            put_u64(&mut payload, *req_cl_ord_id);
            put_u64(&mut payload, *orig_cl_ord_id);
            put_u32(&mut payload, *order_id);
            put_i64(&mut payload, *price);
            put_u64(&mut payload, *qty);
            put_u64(&mut payload, *leaves_qty);
            payload.push(*state);
            payload.extend_from_slice(&[0; 3]);
        }
        Message::OrderAccepted {
            seq,
            cl_ord_id,
            order_id,
            symbol,
            side,
            state,
            price,
            qty,
            leaves_qty,
            log_pos,
            cum_qty,
            avg_px,
        } => {
            put_u64(&mut payload, *seq);
            put_u64(&mut payload, *cl_ord_id);
            put_u32(&mut payload, *order_id);
            put_u16(&mut payload, *symbol);
            payload.push(*side);
            payload.push(*state);
            put_i64(&mut payload, *price);
            put_u64(&mut payload, *qty);
            put_u64(&mut payload, *leaves_qty);
            put_i64(&mut payload, *log_pos);
            put_u64(&mut payload, *cum_qty);
            put_i64(&mut payload, *avg_px);
        }
        Message::OrderExecuted {
            seq,
            cl_ord_id,
            order_id,
            fill_price,
            fill_qty,
            leaves_qty,
            state,
            liquidity,
        } => {
            put_u64(&mut payload, *seq);
            put_u64(&mut payload, *cl_ord_id);
            put_u32(&mut payload, *order_id);
            put_i64(&mut payload, *fill_price);
            put_u64(&mut payload, *fill_qty);
            put_u64(&mut payload, *leaves_qty);
            payload.push(*state);
            payload.push(*liquidity);
            payload.extend_from_slice(&[0; 2]);
        }
        Message::OrderCanceled {
            seq,
            cl_ord_id,
            orig_cl_ord_id,
            order_id,
            reason,
        } => {
            put_u64(&mut payload, *seq);
            put_u64(&mut payload, *cl_ord_id);
            put_u64(&mut payload, *orig_cl_ord_id);
            put_u32(&mut payload, *order_id);
            payload.push(*reason);
            payload.extend_from_slice(&[0; 3]);
        }
        Message::OrderRejected {
            seq,
            cl_ord_id,
            orig_cl_ord_id,
            reason,
            is_cancel_reject,
        } => {
            put_u64(&mut payload, *seq);
            put_u64(&mut payload, *cl_ord_id);
            put_u64(&mut payload, *orig_cl_ord_id);
            payload.push(*reason);
            payload.push(u8::from(*is_cancel_reject));
            payload.extend_from_slice(&[0; 6]);
        }
    }
    frame(message.kind(), payload)
}

/// The total length of the frame at the start of `bytes`.
///
/// # Errors
///
/// [`FrameError`] for a short header or a payload over [`MAX_PAYLOAD`].
pub fn frame_len(bytes: &[u8]) -> Result<usize, FrameError> {
    let header = bytes.get(..HEADER_LEN).ok_or(FrameError::Truncated)?;
    let payload_len = u16::from_le_bytes(header[..2].try_into().expect("two")) as usize;
    if payload_len > MAX_PAYLOAD {
        return Err(FrameError::PayloadTooLarge(payload_len));
    }
    Ok(HEADER_LEN + payload_len)
}

/// Decode the first frame in `bytes`, returning it and the bytes consumed.
///
/// # Errors
///
/// [`FrameError`] for a short, unknown or structurally invalid frame.
pub fn decode(bytes: &[u8]) -> Result<(Message, usize), FrameError> {
    let total = frame_len(bytes)?;
    let header = bytes.get(..HEADER_LEN).ok_or(FrameError::Truncated)?;
    let version = header[3];
    if version != VERSION {
        return Err(FrameError::BadVersion(version));
    }
    let kind = header[2];
    let payload = bytes.get(HEADER_LEN..total).ok_or(FrameError::Truncated)?;
    let message = decode_payload(kind, payload)?;
    Ok((message, total))
}

/// Decode exactly one frame and reject trailing bytes.
///
/// # Errors
///
/// [`FrameError`] for a short frame, trailing bytes, or any [`decode`] error.
pub fn decode_exact(bytes: &[u8]) -> Result<Message, FrameError> {
    let (message, used) = decode(bytes)?;
    if used != bytes.len() {
        return Err(FrameError::BadLength {
            expected: used,
            found: bytes.len(),
        });
    }
    Ok(message)
}

fn decode_payload(kind: u8, payload: &[u8]) -> Result<Message, FrameError> {
    match kind {
        msg_type::LOGIN_REQUEST => {
            let mut reader = Reader::new(exact(payload, 44)?);
            let seat_id = reader.u16()?;
            reader.reserved(2)?;
            let mut token = [0_u8; 32];
            token.copy_from_slice(reader.take(32)?);
            Ok(Message::LoginRequest {
                seat_id,
                token,
                requested_seq: reader.u64()?,
            })
        }
        msg_type::LOGIN_ACCEPTED => {
            let mut reader = Reader::new(exact(payload, 20)?);
            let seat_id = reader.u16()?;
            reader.reserved(2)?;
            Ok(Message::LoginAccepted {
                seat_id,
                generation: reader.u64()?,
                next_seq: reader.u64()?,
            })
        }
        msg_type::LOGIN_REJECTED => {
            let mut reader = Reader::new(exact(payload, 8)?);
            let reason = reader.u8()?;
            reader.reserved(7)?;
            Ok(Message::LoginRejected { reason })
        }
        msg_type::CLIENT_HEARTBEAT => {
            exact(payload, 0)?;
            Ok(Message::ClientHeartbeat)
        }
        msg_type::SERVER_HEARTBEAT => {
            exact(payload, 0)?;
            Ok(Message::ServerHeartbeat)
        }
        msg_type::LOGOUT => {
            let mut reader = Reader::new(exact(payload, 8)?);
            let reason = reader.u8()?;
            reader.reserved(7)?;
            Ok(Message::Logout { reason })
        }
        msg_type::ENTER_ORDER => {
            let mut reader = Reader::new(exact(payload, 32)?);
            let cl_ord_id = reader.u64()?;
            let price = reader.i64()?;
            let qty = reader.u64()?;
            let symbol = reader.u16()?;
            let side = reader.u8()?;
            let ord_type = reader.u8()?;
            let tif = reader.u8()?;
            let flags = reader.u8()?;
            reader.reserved(2)?;
            Ok(Message::EnterOrder {
                cl_ord_id,
                price,
                qty,
                symbol,
                side,
                ord_type,
                tif,
                flags,
            })
        }
        msg_type::CANCEL_ORDER => {
            let mut reader = Reader::new(exact(payload, 24)?);
            let cl_ord_id = reader.u64()?;
            let orig_cl_ord_id = reader.u64()?;
            let symbol = reader.u16()?;
            reader.reserved(6)?;
            Ok(Message::CancelOrder {
                cl_ord_id,
                orig_cl_ord_id,
                symbol,
            })
        }
        msg_type::REPLACE_ORDER => {
            let mut reader = Reader::new(exact(payload, 40)?);
            let cl_ord_id = reader.u64()?;
            let orig_cl_ord_id = reader.u64()?;
            let new_price = reader.i64()?;
            let new_qty = reader.u64()?;
            let symbol = reader.u16()?;
            let ord_type = reader.u8()?;
            let tif = reader.u8()?;
            let flags = reader.u8()?;
            reader.reserved(3)?;
            Ok(Message::ReplaceOrder {
                cl_ord_id,
                orig_cl_ord_id,
                new_price,
                new_qty,
                symbol,
                ord_type,
                tif,
                flags,
            })
        }
        msg_type::ORDER_REPLACED => {
            let mut reader = Reader::new(exact(payload, 56)?);
            let seq = reader.u64()?;
            let req_cl_ord_id = reader.u64()?;
            let orig_cl_ord_id = reader.u64()?;
            let order_id = reader.u32()?;
            let price = reader.i64()?;
            let qty = reader.u64()?;
            let leaves_qty = reader.u64()?;
            let state = reader.u8()?;
            reader.reserved(3)?;
            Ok(Message::OrderReplaced {
                seq,
                req_cl_ord_id,
                orig_cl_ord_id,
                order_id,
                price,
                qty,
                leaves_qty,
                state,
            })
        }
        msg_type::ORDER_ACCEPTED => {
            let mut reader = Reader::new(exact(payload, 72)?);
            let seq = reader.u64()?;
            let cl_ord_id = reader.u64()?;
            let order_id = reader.u32()?;
            let symbol = reader.u16()?;
            let side = reader.u8()?;
            let state = reader.u8()?;
            let price = reader.i64()?;
            let qty = reader.u64()?;
            let leaves_qty = reader.u64()?;
            let log_pos = reader.i64()?;
            let cum_qty = reader.u64()?;
            let avg_px = reader.i64()?;
            Ok(Message::OrderAccepted {
                seq,
                cl_ord_id,
                order_id,
                symbol,
                side,
                state,
                price,
                qty,
                leaves_qty,
                log_pos,
                cum_qty,
                avg_px,
            })
        }
        msg_type::ORDER_EXECUTED => {
            let mut reader = Reader::new(exact(payload, 48)?);
            let seq = reader.u64()?;
            let cl_ord_id = reader.u64()?;
            let order_id = reader.u32()?;
            let fill_price = reader.i64()?;
            let fill_qty = reader.u64()?;
            let leaves_qty = reader.u64()?;
            let state = reader.u8()?;
            let liquidity = reader.u8()?;
            reader.reserved(2)?;
            Ok(Message::OrderExecuted {
                seq,
                cl_ord_id,
                order_id,
                fill_price,
                fill_qty,
                leaves_qty,
                state,
                liquidity,
            })
        }
        msg_type::ORDER_CANCELED => {
            let mut reader = Reader::new(exact(payload, 32)?);
            let seq = reader.u64()?;
            let cl_ord_id = reader.u64()?;
            let orig_cl_ord_id = reader.u64()?;
            let order_id = reader.u32()?;
            let reason = reader.u8()?;
            reader.reserved(3)?;
            Ok(Message::OrderCanceled {
                seq,
                cl_ord_id,
                orig_cl_ord_id,
                order_id,
                reason,
            })
        }
        msg_type::ORDER_REJECTED => {
            let mut reader = Reader::new(exact(payload, 32)?);
            let seq = reader.u64()?;
            let cl_ord_id = reader.u64()?;
            let orig_cl_ord_id = reader.u64()?;
            let reason = reader.u8()?;
            let is_cancel_reject = match reader.u8()? {
                0 => false,
                1 => true,
                other => return Err(FrameError::BadEnum(other)),
            };
            reader.reserved(6)?;
            Ok(Message::OrderRejected {
                seq,
                cl_ord_id,
                orig_cl_ord_id,
                reason,
                is_cancel_reject,
            })
        }
        other => Err(FrameError::UnknownType(other)),
    }
}

fn frame(kind: u8, payload: Vec<u8>) -> Result<Vec<u8>, FrameError> {
    if payload.len() > MAX_PAYLOAD {
        return Err(FrameError::PayloadTooLarge(payload.len()));
    }
    let length =
        u16::try_from(payload.len()).map_err(|_| FrameError::PayloadTooLarge(payload.len()))?;
    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.extend_from_slice(&length.to_le_bytes());
    out.push(kind);
    out.push(VERSION);
    out.extend_from_slice(&payload);
    Ok(out)
}

fn exact(payload: &[u8], expected: usize) -> Result<&[u8], FrameError> {
    if payload.len() == expected {
        Ok(payload)
    } else {
        Err(FrameError::BadLength {
            expected,
            found: payload.len(),
        })
    }
}

fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_i64(out: &mut Vec<u8>, value: i64) {
    out.extend_from_slice(&value.to_le_bytes());
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], FrameError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(FrameError::Truncated)?;
        let slice = self
            .bytes
            .get(self.offset..end)
            .ok_or(FrameError::Truncated)?;
        self.offset = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8, FrameError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, FrameError> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().expect("two")))
    }

    fn u32(&mut self) -> Result<u32, FrameError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().expect("four")))
    }

    fn u64(&mut self) -> Result<u64, FrameError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().expect("eight")))
    }

    fn i64(&mut self) -> Result<i64, FrameError> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().expect("eight")))
    }

    fn reserved(&mut self, length: usize) -> Result<(), FrameError> {
        if self.take(length)?.iter().any(|byte| *byte != 0) {
            Err(FrameError::NonZeroReserved)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token() -> [u8; 32] {
        [7_u8; 32]
    }

    fn messages() -> Vec<Message> {
        vec![
            Message::LoginRequest {
                seat_id: 3,
                token: token(),
                requested_seq: u64::MAX,
            },
            Message::LoginAccepted {
                seat_id: 3,
                generation: 2,
                next_seq: 42,
            },
            Message::LoginRejected {
                reason: login_rejected::INVALID_TOKEN,
            },
            Message::ClientHeartbeat,
            Message::ServerHeartbeat,
            Message::Logout {
                reason: logout::RESYNC_REQUIRED,
            },
            Message::EnterOrder {
                cl_ord_id: 42,
                price: 100_025,
                qty: 7,
                symbol: 1,
                side: side::BUY,
                ord_type: ord_type::LIMIT,
                tif: tif::GTC,
                flags: 1,
            },
            Message::CancelOrder {
                cl_ord_id: 43,
                orig_cl_ord_id: 42,
                symbol: 1,
            },
            Message::ReplaceOrder {
                cl_ord_id: 44,
                orig_cl_ord_id: 42,
                new_price: 100_050,
                new_qty: 8,
                symbol: 1,
                ord_type: ord_type::LIMIT,
                tif: tif::GTC,
                flags: 0,
            },
            Message::OrderReplaced {
                seq: 1,
                req_cl_ord_id: 44,
                orig_cl_ord_id: 42,
                order_id: 9,
                price: 100_050,
                qty: 8,
                leaves_qty: 8,
                state: state::NEW,
            },
            Message::OrderAccepted {
                seq: 2,
                cl_ord_id: 42,
                order_id: 9,
                symbol: 1,
                side: side::BUY,
                state: state::PARTIAL,
                price: 100_025,
                qty: 7,
                leaves_qty: 5,
                log_pos: 77,
                cum_qty: 2,
                avg_px: 100_025,
            },
            Message::OrderExecuted {
                seq: 2,
                cl_ord_id: 42,
                order_id: 9,
                fill_price: 100_025,
                fill_qty: 2,
                leaves_qty: 5,
                state: state::PARTIAL,
                liquidity: liquidity::TAKER,
            },
            Message::OrderCanceled {
                seq: 3,
                cl_ord_id: 43,
                orig_cl_ord_id: 42,
                order_id: 9,
                reason: canceled::USER_REQUESTED,
            },
            Message::OrderRejected {
                seq: 4,
                cl_ord_id: 45,
                orig_cl_ord_id: 0,
                reason: rejected::INVALID_WIRE_ENUM,
                is_cancel_reject: false,
            },
        ]
    }

    #[test]
    fn every_message_round_trips() {
        for message in messages() {
            let bytes = encode(&message).expect("encode");
            assert_eq!(decode_exact(&bytes), Ok(message));
        }
    }

    #[test]
    fn totals_match_the_specification() {
        let expected = [
            (msg_type::LOGIN_REQUEST, 48),
            (msg_type::LOGIN_ACCEPTED, 24),
            (msg_type::LOGIN_REJECTED, 12),
            (msg_type::CLIENT_HEARTBEAT, 4),
            (msg_type::SERVER_HEARTBEAT, 4),
            (msg_type::LOGOUT, 12),
            (msg_type::ENTER_ORDER, 36),
            (msg_type::CANCEL_ORDER, 28),
            (msg_type::REPLACE_ORDER, 44),
            (msg_type::ORDER_REPLACED, 60),
            (msg_type::ORDER_ACCEPTED, 76),
            (msg_type::ORDER_EXECUTED, 52),
            (msg_type::ORDER_CANCELED, 36),
            (msg_type::ORDER_REJECTED, 36),
        ];
        for (message, (kind, length)) in messages().into_iter().zip(expected) {
            let bytes = encode(&message).expect("encode");
            assert_eq!(message.kind(), kind);
            assert_eq!(bytes.len(), length, "length for type {}", kind as char);
            assert_eq!(&bytes[2..3], &[kind]);
            assert_eq!(bytes[3], VERSION);
            assert_eq!(
                u16::from_le_bytes(bytes[..2].try_into().unwrap()) as usize,
                length - HEADER_LEN
            );
        }
    }

    #[test]
    fn order_accepted_layout_is_stable() {
        let message = Message::OrderAccepted {
            seq: 0x0102_0304_0506_0708,
            cl_ord_id: 42,
            order_id: 9,
            symbol: 1,
            side: side::SELL,
            state: state::PARTIAL,
            price: 100_025,
            qty: 7,
            leaves_qty: 5,
            log_pos: 77,
            cum_qty: 2,
            avg_px: 100_025,
        };
        let bytes = encode(&message).expect("encode");
        let body = &bytes[HEADER_LEN..];
        assert_eq!(&body[..8], &0x0102_0304_0506_0708_u64.to_le_bytes());
        assert_eq!(&body[8..16], &42_u64.to_le_bytes());
        assert_eq!(&body[16..20], &9_u32.to_le_bytes());
        assert_eq!(&body[20..22], &1_u16.to_le_bytes());
        assert_eq!(body[22], side::SELL);
        assert_eq!(body[23], state::PARTIAL);
        assert_eq!(&body[24..32], &100_025_i64.to_le_bytes());
        assert_eq!(&body[32..40], &7_u64.to_le_bytes());
        assert_eq!(&body[40..48], &5_u64.to_le_bytes());
        assert_eq!(&body[48..56], &77_i64.to_le_bytes());
        assert_eq!(&body[56..64], &2_u64.to_le_bytes());
        assert_eq!(&body[64..72], &100_025_i64.to_le_bytes());
    }

    #[test]
    fn a_stream_yields_one_frame_at_a_time() {
        let first = encode(&Message::ClientHeartbeat).expect("first");
        let second = encode(&Message::EnterOrder {
            cl_ord_id: 1,
            price: 2,
            qty: 3,
            symbol: 4,
            side: side::BUY,
            ord_type: ord_type::LIMIT,
            tif: tif::DAY,
            flags: 0,
        })
        .expect("second");
        let mut stream = first.clone();
        stream.extend_from_slice(&second);
        let (message, used) = decode(&stream).expect("first frame");
        assert_eq!(message, Message::ClientHeartbeat);
        assert_eq!(used, first.len());
        let (message, used) = decode(&stream[used..]).expect("second frame");
        assert!(matches!(message, Message::EnterOrder { cl_ord_id: 1, .. }));
        assert_eq!(used, second.len());
        assert_eq!(used, stream.len() - first.len());
    }

    #[test]
    fn rejects_bad_headers() {
        assert_eq!(decode(&[]), Err(FrameError::Truncated));
        assert_eq!(decode(&[0, 0, b'L']), Err(FrameError::Truncated));

        let header = [81, 0, msg_type::ENTER_ORDER, VERSION];
        assert_eq!(decode(&header), Err(FrameError::PayloadTooLarge(81)));

        let mut bytes = encode(&Message::ClientHeartbeat).expect("encode");
        bytes[3] = 2;
        assert_eq!(decode(&bytes), Err(FrameError::BadVersion(2)));

        let mut bytes = encode(&Message::ClientHeartbeat).expect("encode");
        bytes[2] = b'Z';
        assert_eq!(decode(&bytes), Err(FrameError::UnknownType(b'Z')));

        let bytes = [10, 0, msg_type::ENTER_ORDER, VERSION];
        assert_eq!(decode(&bytes), Err(FrameError::Truncated));
    }

    #[test]
    fn rejects_bad_lengths_and_reserved_bytes() {
        let mut bytes = encode(&Message::EnterOrder {
            cl_ord_id: 1,
            price: 2,
            qty: 3,
            symbol: 4,
            side: side::BUY,
            ord_type: ord_type::LIMIT,
            tif: tif::DAY,
            flags: 0,
        })
        .expect("encode");
        bytes[0] = 31;
        assert_eq!(
            decode(&bytes),
            Err(FrameError::BadLength {
                expected: 32,
                found: 31
            })
        );

        let mut bytes = encode(&Message::EnterOrder {
            cl_ord_id: 1,
            price: 2,
            qty: 3,
            symbol: 4,
            side: side::BUY,
            ord_type: ord_type::LIMIT,
            tif: tif::DAY,
            flags: 0,
        })
        .expect("encode");
        bytes[HEADER_LEN + 30] = 1;
        assert_eq!(decode(&bytes), Err(FrameError::NonZeroReserved));

        let mut bytes = encode(&Message::LoginRequest {
            seat_id: 1,
            token: token(),
            requested_seq: 0,
        })
        .expect("encode");
        bytes[HEADER_LEN + 2] = 1;
        assert_eq!(decode(&bytes), Err(FrameError::NonZeroReserved));
    }

    #[test]
    fn rejects_a_bad_boolean_and_trailing_bytes() {
        let mut bytes = encode(&Message::OrderRejected {
            seq: 1,
            cl_ord_id: 2,
            orig_cl_ord_id: 3,
            reason: rejected::INVALID_WIRE_ENUM,
            is_cancel_reject: false,
        })
        .expect("encode");
        bytes[HEADER_LEN + 25] = 2;
        assert_eq!(decode(&bytes), Err(FrameError::BadEnum(2)));

        let mut bytes = encode(&Message::ClientHeartbeat).expect("encode");
        bytes.push(0);
        assert_eq!(
            decode_exact(&bytes),
            Err(FrameError::BadLength {
                expected: 4,
                found: 5
            })
        );
    }
}
