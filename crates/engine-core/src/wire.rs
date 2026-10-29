//! The command and event records carried in the cluster log and egress.
//!
//! The gateway offers a [`Command`] as the session-message payload; the
//! `ClusteredService` decodes it, applies it to the engine and offers each
//! [`Event`] back on the session. Fixed little-endian fields, one version byte
//! and one kind byte. SBE may replace this record without changing the
//! in-memory types.

use deepbook_types::{OrderState, Price, Side};

use crate::command::{
    CancelOrder, Command, NewOrder, OrdType, RejectReason, ReplaceOrder, TimeInForce,
};
use crate::event::{
    BookUpdateEvent, CancelRejectEvent, Event, EventHeader, ExecType, ExecutionReportEvent,
    SnapshotEvent, TradeEvent,
};
use crate::ids::ClientOrderId;

const VERSION: u8 = 1;

/// A wire encode/decode failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WireError {
    /// The version byte is not one this build knows.
    BadVersion(u8),
    /// The kind byte is not one this build knows.
    UnknownKind(u8),
    /// The record ends before the field it promised.
    Truncated,
    /// An enum byte is not one this build knows.
    BadEnum(u8),
}

impl std::fmt::Display for WireError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadVersion(version) => write!(formatter, "unknown wire version {version}"),
            Self::UnknownKind(kind) => write!(formatter, "unknown record kind {kind}"),
            Self::Truncated => write!(formatter, "truncated command/event record"),
            Self::BadEnum(byte) => write!(formatter, "unknown enum value {byte}"),
        }
    }
}

impl std::error::Error for WireError {}

/// Encode a command.
#[must_use]
pub fn encode_command(command: &Command) -> Vec<u8> {
    let mut out = vec![VERSION];
    match command {
        Command::NewOrder(order) => {
            out.push(1);
            put_new_order(&mut out, order);
        }
        Command::CancelOrder(cancel) => {
            out.push(2);
            out.extend_from_slice(&cancel.account.to_le_bytes());
            out.extend_from_slice(cancel.cl_ord_id.as_bytes());
            out.extend_from_slice(cancel.orig_cl_ord_id.as_bytes());
        }
        Command::ReplaceOrder(replace) => {
            out.push(3);
            out.extend_from_slice(&replace.account.to_le_bytes());
            out.extend_from_slice(replace.cl_ord_id.as_bytes());
            out.extend_from_slice(replace.orig_cl_ord_id.as_bytes());
            put_price(&mut out, replace.price);
            out.extend_from_slice(&replace.qty.to_le_bytes());
            out.push(ord_type_byte(replace.ord_type));
            out.push(tif_byte(replace.time_in_force));
            out.push(u8::from(replace.post_only));
        }
        Command::KillSwitch { account } => {
            out.push(4);
            match account {
                Some(account) => {
                    out.push(1);
                    out.extend_from_slice(&account.to_le_bytes());
                }
                None => out.push(0),
            }
        }
        Command::Snapshot => out.push(5),
        Command::ExpireDayOrders => out.push(6),
    }
    out
}

/// Decode a command.
///
/// # Errors
///
/// [`WireError`] for a short or unknown record.
pub fn decode_command(bytes: &[u8]) -> Result<Command, WireError> {
    let mut reader = Reader::new(bytes);
    let version = reader.u8()?;
    if version != VERSION {
        return Err(WireError::BadVersion(version));
    }
    match reader.u8()? {
        1 => Ok(Command::NewOrder(NewOrder {
            symbol: reader.u16()?,
            account: reader.u32()?,
            cl_ord_id: reader.client_order_id()?,
            side: side_from(reader.u8()?)?,
            price: reader.price()?,
            qty: reader.i64()?,
            ord_type: ord_type_from(reader.u8()?)?,
            time_in_force: tif_from(reader.u8()?)?,
            post_only: reader.u8()? != 0,
        })),
        2 => Ok(Command::CancelOrder(CancelOrder {
            account: reader.u32()?,
            cl_ord_id: reader.client_order_id()?,
            orig_cl_ord_id: reader.client_order_id()?,
        })),
        3 => Ok(Command::ReplaceOrder(ReplaceOrder {
            account: reader.u32()?,
            cl_ord_id: reader.client_order_id()?,
            orig_cl_ord_id: reader.client_order_id()?,
            price: reader.price()?,
            qty: reader.i64()?,
            ord_type: ord_type_from(reader.u8()?)?,
            time_in_force: tif_from(reader.u8()?)?,
            post_only: reader.u8()? != 0,
        })),
        4 => {
            let account = match reader.u8()? {
                0 => None,
                1 => Some(reader.u32()?),
                other => return Err(WireError::BadEnum(other)),
            };
            Ok(Command::KillSwitch { account })
        }
        5 => Ok(Command::Snapshot),
        6 => Ok(Command::ExpireDayOrders),
        other => Err(WireError::UnknownKind(other)),
    }
}

/// Encode an event.
#[must_use]
pub fn encode_event(event: &Event) -> Vec<u8> {
    let mut out = Vec::with_capacity(96);
    out.push(VERSION);
    match event {
        Event::ExecutionReport(report) => {
            out.push(10);
            put_header(&mut out, report.header);
            out.extend_from_slice(&report.order_id.to_le_bytes());
            out.extend_from_slice(report.cl_ord_id.as_bytes());
            out.extend_from_slice(&report.account.to_le_bytes());
            out.extend_from_slice(&report.symbol.to_le_bytes());
            out.push(side_byte(report.side));
            out.push(exec_type_byte(report.exec_type));
            out.push(state_byte(report.state));
            put_price(&mut out, report.price);
            out.extend_from_slice(&report.order_qty.to_le_bytes());
            out.extend_from_slice(&report.last_qty.to_le_bytes());
            put_price(&mut out, report.last_px);
            out.extend_from_slice(&report.leaves_qty.to_le_bytes());
            out.extend_from_slice(&report.cum_qty.to_le_bytes());
            out.extend_from_slice(&report.avg_px.to_le_bytes());
            out.push(reason_byte(report.reason));
        }
        Event::CancelReject(reject) => {
            out.push(11);
            put_header(&mut out, reject.header);
            match reject.order_id {
                Some(order_id) => {
                    out.push(1);
                    out.extend_from_slice(&order_id.to_le_bytes());
                }
                None => out.push(0),
            }
            out.extend_from_slice(reject.cl_ord_id.as_bytes());
            out.extend_from_slice(reject.orig_cl_ord_id.as_bytes());
            out.extend_from_slice(&reject.account.to_le_bytes());
            out.push(state_byte(reject.state));
            out.push(reason_byte(Some(reject.reason)));
            out.push(u8::from(reject.is_replace));
        }
        Event::Trade(trade) => {
            out.push(12);
            put_header(&mut out, trade.header);
            out.extend_from_slice(&trade.maker_order_id.to_le_bytes());
            out.extend_from_slice(&trade.taker_order_id.to_le_bytes());
            out.extend_from_slice(trade.maker_cl_ord_id.as_bytes());
            out.extend_from_slice(trade.taker_cl_ord_id.as_bytes());
            out.extend_from_slice(&trade.maker_account.to_le_bytes());
            out.extend_from_slice(&trade.taker_account.to_le_bytes());
            out.extend_from_slice(&trade.price.to_le_bytes());
            out.extend_from_slice(&trade.qty.to_le_bytes());
            out.push(side_byte(trade.aggressor_side));
        }
        Event::BookUpdate(update) => {
            out.push(13);
            put_header(&mut out, update.header);
            out.extend_from_slice(&update.symbol.to_le_bytes());
            out.push(side_byte(update.side));
            out.extend_from_slice(&update.price.to_le_bytes());
            out.extend_from_slice(&update.qty.to_le_bytes());
            out.extend_from_slice(&update.md_seq.to_le_bytes());
        }
        Event::Snapshot(snapshot) => {
            out.push(14);
            put_header(&mut out, snapshot.header);
            out.extend_from_slice(&snapshot.symbol.to_le_bytes());
            out.extend_from_slice(&snapshot.md_seq.to_le_bytes());
            out.extend_from_slice(&snapshot.book_hash.to_le_bytes());
            out.extend_from_slice(&snapshot.last_log_pos.to_le_bytes());
        }
    }
    out
}

/// Decode an event.
///
/// # Errors
///
/// [`WireError`] for a short or unknown record.
pub fn decode_event(bytes: &[u8]) -> Result<Event, WireError> {
    let mut reader = Reader::new(bytes);
    let version = reader.u8()?;
    if version != VERSION {
        return Err(WireError::BadVersion(version));
    }
    let kind = reader.u8()?;
    let header = reader.header()?;
    match kind {
        10 => Ok(Event::ExecutionReport(ExecutionReportEvent {
            header,
            order_id: reader.u32()?,
            cl_ord_id: reader.client_order_id()?,
            account: reader.u32()?,
            symbol: reader.u16()?,
            side: side_from(reader.u8()?)?,
            exec_type: exec_type_from(reader.u8()?)?,
            state: state_from(reader.u8()?)?,
            price: reader.price()?,
            order_qty: reader.i64()?,
            last_qty: reader.i64()?,
            last_px: reader.price()?,
            leaves_qty: reader.i64()?,
            cum_qty: reader.i64()?,
            avg_px: reader.i64()?,
            reason: reason_from(reader.u8()?)?,
        })),
        11 => {
            let order_id = match reader.u8()? {
                0 => None,
                1 => Some(reader.u32()?),
                other => return Err(WireError::BadEnum(other)),
            };
            Ok(Event::CancelReject(CancelRejectEvent {
                header,
                order_id,
                cl_ord_id: reader.client_order_id()?,
                orig_cl_ord_id: reader.client_order_id()?,
                account: reader.u32()?,
                state: state_from(reader.u8()?)?,
                reason: reason_from(reader.u8()?)?.ok_or(WireError::BadEnum(0))?,
                is_replace: reader.u8()? != 0,
            }))
        }
        12 => Ok(Event::Trade(TradeEvent {
            header,
            maker_order_id: reader.u32()?,
            taker_order_id: reader.u32()?,
            maker_cl_ord_id: reader.client_order_id()?,
            taker_cl_ord_id: reader.client_order_id()?,
            maker_account: reader.u32()?,
            taker_account: reader.u32()?,
            price: reader.i64()?,
            qty: reader.i64()?,
            aggressor_side: side_from(reader.u8()?)?,
        })),
        13 => Ok(Event::BookUpdate(BookUpdateEvent {
            header,
            symbol: reader.u16()?,
            side: side_from(reader.u8()?)?,
            price: reader.i64()?,
            qty: reader.i64()?,
            md_seq: reader.u64()?,
        })),
        14 => Ok(Event::Snapshot(SnapshotEvent {
            header,
            symbol: reader.u16()?,
            md_seq: reader.u64()?,
            book_hash: reader.u64()?,
            last_log_pos: reader.i64()?,
        })),
        other => Err(WireError::UnknownKind(other)),
    }
}

fn put_new_order(out: &mut Vec<u8>, order: &NewOrder) {
    out.extend_from_slice(&order.symbol.to_le_bytes());
    out.extend_from_slice(&order.account.to_le_bytes());
    out.extend_from_slice(order.cl_ord_id.as_bytes());
    out.push(side_byte(order.side));
    put_price(out, order.price);
    out.extend_from_slice(&order.qty.to_le_bytes());
    out.push(ord_type_byte(order.ord_type));
    out.push(tif_byte(order.time_in_force));
    out.push(u8::from(order.post_only));
}

fn put_header(out: &mut Vec<u8>, header: EventHeader) {
    out.extend_from_slice(&header.log_pos.to_le_bytes());
    out.extend_from_slice(&header.event_seq.to_le_bytes());
}

fn put_price(out: &mut Vec<u8>, price: Option<Price>) {
    match price {
        Some(price) => {
            out.push(1);
            out.extend_from_slice(&price.to_le_bytes());
        }
        None => out.push(0),
    }
}

fn side_byte(side: Side) -> u8 {
    match side {
        Side::Buy => 0,
        Side::Sell => 1,
    }
}

fn side_from(byte: u8) -> Result<Side, WireError> {
    match byte {
        0 => Ok(Side::Buy),
        1 => Ok(Side::Sell),
        other => Err(WireError::BadEnum(other)),
    }
}

fn ord_type_byte(ord_type: OrdType) -> u8 {
    match ord_type {
        OrdType::Limit => 0,
        OrdType::Market => 1,
    }
}

fn ord_type_from(byte: u8) -> Result<OrdType, WireError> {
    match byte {
        0 => Ok(OrdType::Limit),
        1 => Ok(OrdType::Market),
        other => Err(WireError::BadEnum(other)),
    }
}

fn tif_byte(tif: TimeInForce) -> u8 {
    match tif {
        TimeInForce::Day => 0,
        TimeInForce::GoodTillCanceled => 1,
        TimeInForce::ImmediateOrCancel => 2,
        TimeInForce::FillOrKill => 3,
    }
}

fn tif_from(byte: u8) -> Result<TimeInForce, WireError> {
    match byte {
        0 => Ok(TimeInForce::Day),
        1 => Ok(TimeInForce::GoodTillCanceled),
        2 => Ok(TimeInForce::ImmediateOrCancel),
        3 => Ok(TimeInForce::FillOrKill),
        other => Err(WireError::BadEnum(other)),
    }
}

fn exec_type_byte(exec_type: ExecType) -> u8 {
    match exec_type {
        ExecType::New => 0,
        ExecType::Trade => 1,
        ExecType::Canceled => 2,
        ExecType::Replaced => 3,
        ExecType::Rejected => 4,
    }
}

fn exec_type_from(byte: u8) -> Result<ExecType, WireError> {
    match byte {
        0 => Ok(ExecType::New),
        1 => Ok(ExecType::Trade),
        2 => Ok(ExecType::Canceled),
        3 => Ok(ExecType::Replaced),
        4 => Ok(ExecType::Rejected),
        other => Err(WireError::BadEnum(other)),
    }
}

fn state_byte(state: OrderState) -> u8 {
    match state {
        OrderState::New => 0,
        OrderState::Partial => 1,
        OrderState::Filled => 2,
        OrderState::Canceled => 3,
        OrderState::Rejected => 4,
    }
}

fn state_from(byte: u8) -> Result<OrderState, WireError> {
    match byte {
        0 => Ok(OrderState::New),
        1 => Ok(OrderState::Partial),
        2 => Ok(OrderState::Filled),
        3 => Ok(OrderState::Canceled),
        4 => Ok(OrderState::Rejected),
        other => Err(WireError::BadEnum(other)),
    }
}

fn reason_byte(reason: Option<RejectReason>) -> u8 {
    match reason {
        None => 0,
        Some(reason) => reason_from_value(reason),
    }
}

fn reason_from_value(reason: RejectReason) -> u8 {
    match reason {
        RejectReason::DuplicateClOrdId => 1,
        RejectReason::UnknownOrder => 2,
        RejectReason::OrderNotOpen => 3,
        RejectReason::InvalidQuantity => 4,
        RejectReason::InvalidPrice => 5,
        RejectReason::PostOnlyWouldCross => 6,
        RejectReason::FokNotFillable => 7,
        RejectReason::SelfTradePrevention => 8,
        RejectReason::KillSwitch => 9,
        RejectReason::MaxOrderQty => 10,
        RejectReason::MaxNotional => 11,
        RejectReason::PriceBand => 12,
        RejectReason::FatFinger => 13,
        RejectReason::MaxOpenOrders => 14,
        RejectReason::BookFull => 15,
        RejectReason::UnknownSymbol => 16,
        RejectReason::IdWindowFull => 17,
    }
}

fn reason_from(byte: u8) -> Result<Option<RejectReason>, WireError> {
    Ok(match byte {
        0 => None,
        1 => Some(RejectReason::DuplicateClOrdId),
        2 => Some(RejectReason::UnknownOrder),
        3 => Some(RejectReason::OrderNotOpen),
        4 => Some(RejectReason::InvalidQuantity),
        5 => Some(RejectReason::InvalidPrice),
        6 => Some(RejectReason::PostOnlyWouldCross),
        7 => Some(RejectReason::FokNotFillable),
        8 => Some(RejectReason::SelfTradePrevention),
        9 => Some(RejectReason::KillSwitch),
        10 => Some(RejectReason::MaxOrderQty),
        11 => Some(RejectReason::MaxNotional),
        12 => Some(RejectReason::PriceBand),
        13 => Some(RejectReason::FatFinger),
        14 => Some(RejectReason::MaxOpenOrders),
        15 => Some(RejectReason::BookFull),
        16 => Some(RejectReason::UnknownSymbol),
        17 => Some(RejectReason::IdWindowFull),
        other => return Err(WireError::BadEnum(other)),
    })
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], WireError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(WireError::Truncated)?;
        let slice = self
            .bytes
            .get(self.offset..end)
            .ok_or(WireError::Truncated)?;
        self.offset = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8, WireError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, WireError> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().expect("two")))
    }

    fn u32(&mut self) -> Result<u32, WireError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().expect("four")))
    }

    fn u64(&mut self) -> Result<u64, WireError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().expect("eight")))
    }

    fn i64(&mut self) -> Result<i64, WireError> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().expect("eight")))
    }

    fn price(&mut self) -> Result<Option<Price>, WireError> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.i64()?)),
            other => Err(WireError::BadEnum(other)),
        }
    }

    fn client_order_id(&mut self) -> Result<ClientOrderId, WireError> {
        let bytes: [u8; ClientOrderId::LENGTH] = self
            .take(ClientOrderId::LENGTH)?
            .try_into()
            .map_err(|_| WireError::Truncated)?;
        Ok(ClientOrderId::from_padded_bytes(bytes))
    }

    fn header(&mut self) -> Result<EventHeader, WireError> {
        Ok(EventHeader {
            log_pos: self.i64()?,
            event_seq: self.u32()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deepbook_types::{AccountId, LogPos};

    fn cl(value: &str) -> ClientOrderId {
        value.parse().expect("client order id")
    }

    #[test]
    fn every_command_round_trips() {
        let commands = [
            Command::NewOrder(NewOrder {
                symbol: 1,
                account: 2,
                cl_ord_id: cl("C-1"),
                side: Side::Buy,
                price: Some(10_025),
                qty: 100,
                ord_type: OrdType::Limit,
                time_in_force: TimeInForce::GoodTillCanceled,
                post_only: false,
            }),
            Command::NewOrder(NewOrder {
                symbol: 1,
                account: 2,
                cl_ord_id: cl("C-2"),
                side: Side::Sell,
                price: None,
                qty: 5,
                ord_type: OrdType::Market,
                time_in_force: TimeInForce::ImmediateOrCancel,
                post_only: false,
            }),
            Command::CancelOrder(CancelOrder {
                account: 2,
                cl_ord_id: cl("C-3"),
                orig_cl_ord_id: cl("C-1"),
            }),
            Command::ReplaceOrder(ReplaceOrder {
                account: 2,
                cl_ord_id: cl("C-4"),
                orig_cl_ord_id: cl("C-1"),
                price: Some(10_050),
                qty: 150,
                ord_type: OrdType::Limit,
                time_in_force: TimeInForce::Day,
                post_only: true,
            }),
            Command::KillSwitch { account: None },
            Command::KillSwitch { account: Some(7) },
            Command::Snapshot,
            Command::ExpireDayOrders,
        ];
        for command in commands {
            let bytes = encode_command(&command);
            assert_eq!(Ok(command), decode_command(&bytes));
        }
    }

    #[test]
    fn every_event_round_trips() {
        let header = EventHeader {
            log_pos: 9,
            event_seq: 3,
        };
        let events = [
            Event::ExecutionReport(ExecutionReportEvent {
                header,
                order_id: 11,
                cl_ord_id: cl("C-1"),
                account: 2,
                symbol: 1,
                side: Side::Buy,
                exec_type: ExecType::Trade,
                state: OrderState::Partial,
                price: Some(10_025),
                order_qty: 100,
                last_qty: 30,
                last_px: Some(10_025),
                leaves_qty: 70,
                cum_qty: 30,
                avg_px: 10_025,
                reason: None,
            }),
            Event::CancelReject(CancelRejectEvent {
                header,
                order_id: Some(11),
                cl_ord_id: cl("C-2"),
                orig_cl_ord_id: cl("C-1"),
                account: 2,
                state: OrderState::Filled,
                reason: RejectReason::OrderNotOpen,
                is_replace: false,
            }),
            Event::Trade(TradeEvent {
                header,
                maker_order_id: 1,
                taker_order_id: 11,
                maker_cl_ord_id: cl("S-1"),
                taker_cl_ord_id: cl("C-1"),
                maker_account: 1,
                taker_account: 2,
                price: 10_025,
                qty: 30,
                aggressor_side: Side::Buy,
            }),
            Event::BookUpdate(BookUpdateEvent {
                header,
                symbol: 1,
                side: Side::Sell,
                price: 10_025,
                qty: 70,
                md_seq: 4,
            }),
            Event::Snapshot(SnapshotEvent {
                header,
                symbol: 1,
                md_seq: 4,
                book_hash: 0x1234,
                last_log_pos: 9,
            }),
        ];
        for event in events {
            let bytes = encode_event(&event);
            assert_eq!(Ok(event), decode_event(&bytes));
        }
    }

    #[test]
    fn a_short_or_unknown_record_is_refused() {
        let bytes = encode_command(&Command::Snapshot);
        assert!(decode_command(&bytes[..0]).is_err());
        assert_eq!(
            Err(WireError::UnknownKind(99)),
            decode_command(&[VERSION, 99])
        );
    }

    #[test]
    fn account_ids_and_log_positions_round_trip() {
        let account: AccountId = 42;
        let log_pos: LogPos = -7;
        let command = Command::KillSwitch {
            account: Some(account),
        };
        assert_eq!(Ok(command), decode_command(&encode_command(&command)));
        let event = Event::Snapshot(SnapshotEvent {
            header: EventHeader {
                log_pos,
                event_seq: 0,
            },
            symbol: 1,
            md_seq: 0,
            book_hash: 0,
            last_log_pos: log_pos,
        });
        let bytes = encode_event(&event);
        assert_eq!(Ok(event), decode_event(&bytes));
    }
}
