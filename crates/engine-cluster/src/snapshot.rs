//! The engine snapshot record.
//!
//! Two formats are readable: the legacy hand-written `DBS1` record and the SBE
//! message from schema 200. During the migration's first phase (ADR 0005)
//! [`encode`] still writes `DBS1`: every node must be able to read the SBE
//! record before any node writes it. [`decode`] dispatches on the four-byte
//! magic, so a cluster can recover from either format.

mod dbs1;
mod sbe;

pub use dbs1::CodecError;

use deepbook_engine_core::EngineSnapshot;

/// Encode in the format the deployment writes.
///
/// Phase 1 writes `DBS1`; flipping this to [`encode_sbe`] is Phase 2 and must
/// happen only after every node can read `DBS2`.
#[must_use]
pub fn encode(snapshot: &EngineSnapshot) -> Vec<u8> {
    dbs1::encode(snapshot)
}

/// Encode as `DBS2` + SBE schema 200.
///
/// # Errors
///
/// A count or an encoded length does not fit the format.
pub fn encode_sbe(snapshot: &EngineSnapshot) -> Result<Vec<u8>, CodecError> {
    sbe::encode(snapshot)
}

/// Decode either format.
///
/// # Errors
///
/// The record is not a snapshot, is truncated, or does not belong to a version
/// this build knows.
pub fn decode(bytes: &[u8]) -> Result<EngineSnapshot, CodecError> {
    if bytes.starts_with(&dbs1::MAGIC) {
        dbs1::decode(bytes)
    } else if bytes.starts_with(&sbe::MAGIC) {
        sbe::decode(&bytes[sbe::MAGIC.len()..])
    } else {
        Err(CodecError::BadMagic)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deepbook_engine_core::ids::ClientOrderId;
    use deepbook_engine_core::{
        BookSnapshot, Engine, EngineConfig, LevelSnapshot, NewOrder, OrdType, OrderSlotSnapshot,
        RiskConfig, Seen, SeenEntry, SeenState, TimeInForce,
    };
    use deepbook_engine_core::{CancelOrder, Command, ReplaceOrder};
    use deepbook_types::{AccountId, LogPos, OrderState, Price, Qty, Side};

    fn config() -> EngineConfig {
        EngineConfig {
            symbol: 1,
            max_orders: 64,
            recent_order_window: 64,
            risk: RiskConfig::disabled(),
        }
    }

    fn cl(value: &str) -> ClientOrderId {
        value.parse().expect("client order id")
    }

    fn limit(account: AccountId, id: &str, side: Side, price: Price, qty: Qty) -> Command {
        Command::NewOrder(NewOrder {
            symbol: 1,
            account,
            cl_ord_id: cl(id),
            side,
            price: Some(price),
            qty,
            ord_type: OrdType::Limit,
            time_in_force: TimeInForce::GoodTillCanceled,
            post_only: false,
        })
    }

    /// An engine state with resting orders, a cancel, a replace and a trade.
    fn busy_engine() -> Engine {
        let mut engine = Engine::new(config());
        let commands = [
            limit(1, "S1", Side::Sell, 100, 10),
            limit(2, "B1", Side::Buy, 99, 5),
            limit(3, "S2", Side::Sell, 101, 20),
            Command::CancelOrder(CancelOrder {
                account: 2,
                cl_ord_id: cl("C1"),
                orig_cl_ord_id: cl("B1"),
            }),
            Command::ReplaceOrder(ReplaceOrder {
                account: 1,
                cl_ord_id: cl("R1"),
                orig_cl_ord_id: cl("S1"),
                price: Some(102),
                qty: 8,
                ord_type: OrdType::Limit,
                time_in_force: TimeInForce::GoodTillCanceled,
                post_only: false,
            }),
            limit(4, "B2", Side::Buy, 101, 5),
        ];
        let mut events = Vec::new();
        for (index, command) in commands.iter().enumerate() {
            engine.on_command(command, index as LogPos + 1, &mut events);
        }
        engine
    }

    #[test]
    fn the_default_encoding_is_still_dbs1() {
        let snapshot = busy_engine().snapshot();
        assert!(encode(&snapshot).starts_with(b"DBS1"));
    }

    #[test]
    fn both_formats_decode_to_the_same_snapshot() {
        let engine = busy_engine();
        let snapshot = engine.snapshot();
        let dbs1 = encode(&snapshot);
        let sbe = encode_sbe(&snapshot).expect("sbe encode");

        assert!(sbe.starts_with(b"DBS2"));
        assert_eq!(decode(&dbs1).expect("dbs1 decode"), snapshot);
        assert_eq!(decode(&sbe).expect("sbe decode"), snapshot);
    }

    #[test]
    fn an_sbe_snapshot_restores_the_same_engine() {
        let mut engine = busy_engine();
        let snapshot = engine.snapshot();
        let decoded = decode(&encode_sbe(&snapshot).expect("sbe encode")).expect("sbe decode");
        let mut restored = Engine::restore(config(), &decoded).expect("restore");

        assert_eq!(engine.book_hash(), restored.book_hash());
        assert_eq!(engine.view(), restored.view());

        let continuation = [
            limit(5, "B3", Side::Buy, 102, 3),
            limit(6, "S3", Side::Sell, 100, 4),
        ];
        let mut first = Vec::new();
        let mut second = Vec::new();
        for (index, command) in continuation.iter().enumerate() {
            let log = index as LogPos + 1;
            engine.on_command(command, log, &mut first);
            restored.on_command(command, log, &mut second);
        }
        assert_eq!(first, second);
    }

    #[test]
    fn an_sbe_record_truncated_anywhere_is_refused() {
        let snapshot = busy_engine().snapshot();
        let bytes = encode_sbe(&snapshot).expect("sbe encode");
        for length in 0..bytes.len() {
            assert!(decode(&bytes[..length]).is_err(), "length {length}");
        }
    }

    #[test]
    fn a_foreign_schema_id_or_version_is_refused() {
        let snapshot = Engine::new(config()).snapshot();
        let bytes = encode_sbe(&snapshot).expect("sbe encode");

        let mut foreign_schema = bytes.clone();
        foreign_schema[8..10].copy_from_slice(&999_u16.to_le_bytes());
        assert_eq!(decode(&foreign_schema), Err(CodecError::BadMagic));

        let mut foreign_version = bytes;
        foreign_version[10..12].copy_from_slice(&2_u16.to_le_bytes());
        assert_eq!(decode(&foreign_version), Err(CodecError::BadVersion(2)));
    }

    #[test]
    fn the_sbe_frame_has_the_expected_header_and_length() {
        let snapshot = Engine::new(config()).snapshot();
        let bytes = encode_sbe(&snapshot).expect("sbe encode");

        assert_eq!(&bytes[..4], b"DBS2");
        assert_eq!(u16::from_le_bytes([bytes[4], bytes[5]]), 32); // block length
        assert_eq!(u16::from_le_bytes([bytes[6], bytes[7]]), 1); // template id
        assert_eq!(u16::from_le_bytes([bytes[8], bytes[9]]), 200); // schema id
        assert_eq!(u16::from_le_bytes([bytes[10], bytes[11]]), 1); // version
        // header + fixed block + seven group headers and entries + var-data
        let expected = 4
            + 8
            + 32
            + 6
            + 4 * snapshot.killed_accounts.len()
            + 6
            + 8 * snapshot.open_orders.len()
            + 6
            + 73 * snapshot.seen.len()
            + 6
            + 4 * snapshot.book.free.len()
            + 6
            + 83 * snapshot.book.slots.len()
            + 6
            + 24 * snapshot.book.bids.len()
            + 6
            + 24 * snapshot.book.asks.len()
            + 4
            + snapshot.book.generations.len();
        assert_eq!(bytes.len(), expected);
    }

    #[test]
    fn a_group_count_beyond_u16_round_trips() {
        let mut snapshot = Engine::new(config()).snapshot();
        snapshot.killed_accounts = (0..70_000).collect();

        let bytes = encode_sbe(&snapshot).expect("sbe encode");
        assert_eq!(decode(&bytes).expect("sbe decode"), snapshot);
    }

    #[test]
    fn notional_extremes_round_trip() {
        for notional in [i128::MIN, -1, 0, 1, i128::MAX] {
            let snapshot = synthetic_snapshot(notional);
            let bytes = encode_sbe(&snapshot).expect("sbe encode");
            assert_eq!(decode(&bytes).expect("sbe decode"), snapshot, "{notional}");
        }
    }

    fn synthetic_snapshot(notional: i128) -> EngineSnapshot {
        EngineSnapshot {
            symbol: 1,
            last_log_pos: 5,
            book: BookSnapshot {
                slots: vec![OrderSlotSnapshot {
                    slot: 0,
                    id: 7,
                    price: 100,
                    qty: 3,
                    filled: 1,
                    notional,
                    account: 2,
                    prev: u32::MAX,
                    next: u32::MAX,
                    side: Side::Buy,
                    state: OrderState::Partial,
                    is_market: false,
                    cl_ord_id: cl("N1"),
                }],
                generations: vec![0, 1, 2],
                free: vec![1, 2],
                free_head: 1,
                bids: vec![LevelSnapshot {
                    price: 100,
                    total_qty: 3,
                    head: 0,
                    tail: 0,
                }],
                asks: Vec::new(),
                last_trade: Some(100),
                md_seq: 9,
            },
            seen: vec![SeenEntry {
                account: 2,
                cl_ord_id: cl("N1"),
                seen: Seen {
                    order_id: 7,
                    state: SeenState::Active,
                    price: Some(100),
                    qty: 3,
                    filled: 1,
                    notional,
                    side: Side::Buy,
                    symbol: 1,
                },
            }],
            open_orders: vec![(2, 1)],
            kill_all: false,
            killed_accounts: vec![9, 10],
        }
    }
}
