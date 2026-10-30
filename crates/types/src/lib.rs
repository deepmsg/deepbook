//! Basic value types shared by every deepbook crate.
//!
//! v0, frozen at M0. Prices are integer ticks and quantities are integer
//! lots; no floating point reaches the matching engine.
#![forbid(unsafe_code)]

/// Price in ticks.
pub type Price = i64;

/// Quantity in lots.
pub type Qty = i64;

/// Slab slot index. [`EMPTY_ORDER_ID`] is the sentinel.
///
/// A slab slot is reused after an order reaches a terminal state. The FIX
/// `OrderID` must not be the bare slot index; see ADR 0003
/// (`docs/adr/0003-order-identity-and-matching.md`) about generation counters.
pub type OrderId = u32;

/// Symbol id. The MVP has one symbol.
pub type SymbolId = u16;

/// Account id.
pub type AccountId = u32;

/// Cluster log position; monotonic across restarts and leadership terms.
pub type LogPos = i64;

/// Market-data sequence number.
pub type Seq = u64;

/// The empty-slot sentinel for [`OrderId`].
pub const EMPTY_ORDER_ID: OrderId = u32::MAX;

/// Order side.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    /// Buy side.
    Buy,
    /// Sell side.
    Sell,
}

/// Order state as seen by the engine and the FIX layer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderState {
    /// Accepted, no fill.
    New,
    /// Partially filled.
    Partial,
    /// Fully filled.
    Filled,
    /// Canceled.
    Canceled,
    /// Rejected before it joined the book.
    Rejected,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_empty_order_id_is_the_sentinel() {
        assert_eq!(EMPTY_ORDER_ID, u32::MAX);
    }

    #[test]
    fn prices_and_quantities_are_integer_ticks_and_lots() {
        let price: Price = 10_025; // 100.25 in hundredths
        let qty: Qty = 10;
        assert_eq!(price * qty, 100_250);
    }
}
