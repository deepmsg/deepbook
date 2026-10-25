//! The commands the engine consumes and the reasons it can refuse one.
//!
//! A command is a value, not bytes: M4's cluster adapter will decode the SBE
//! command and call [`Engine::on_command`](crate::Engine::on_command). The
//! engine sees no clock and no socket.

use deepbook_types::{AccountId, Price, Qty, Side, SymbolId};

use crate::ids::ClientOrderId;

/// `OrdType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrdType {
    /// Limit: a price is required and the remainder may rest.
    Limit,
    /// Market: no price; the remainder is canceled.
    Market,
}

/// `TimeInForce`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeInForce {
    /// Day (the engine treats it like GTC; expiry is a timer, M4).
    Day,
    /// Good till canceled.
    GoodTillCanceled,
    /// Immediate or cancel: the remainder is canceled.
    ImmediateOrCancel,
    /// Fill or kill: all or nothing.
    FillOrKill,
}

impl TimeInForce {
    /// Whether an unfilled remainder rests.
    #[must_use]
    pub const fn rests(self, ord_type: OrdType) -> bool {
        matches!(ord_type, OrdType::Limit) && matches!(self, Self::Day | Self::GoodTillCanceled)
    }
}

/// A new order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NewOrder {
    /// Symbol.
    pub symbol: SymbolId,
    /// Account.
    pub account: AccountId,
    /// Client order id; unique per account within the window.
    pub cl_ord_id: ClientOrderId,
    /// Buy or sell.
    pub side: Side,
    /// Limit price; `None` only for a market order.
    pub price: Option<Price>,
    /// Quantity in lots.
    pub qty: Qty,
    /// Limit or market.
    pub ord_type: OrdType,
    /// Time in force.
    pub time_in_force: TimeInForce,
    /// Must not take liquidity.
    pub post_only: bool,
}

/// A cancel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CancelOrder {
    /// Account.
    pub account: AccountId,
    /// The cancel request's own client order id.
    pub cl_ord_id: ClientOrderId,
    /// The order to cancel.
    pub orig_cl_ord_id: ClientOrderId,
}

/// A replace.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplaceOrder {
    /// Account.
    pub account: AccountId,
    /// The replace request's own client order id.
    pub cl_ord_id: ClientOrderId,
    /// The order to replace.
    pub orig_cl_ord_id: ClientOrderId,
    /// New limit price.
    pub price: Option<Price>,
    /// New quantity (must be greater than the already filled quantity).
    pub qty: Qty,
    /// New order type.
    pub ord_type: OrdType,
    /// New time in force.
    pub time_in_force: TimeInForce,
    /// New post-only flag.
    pub post_only: bool,
}

/// One engine input.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Command {
    /// New order.
    NewOrder(NewOrder),
    /// Cancel an order.
    CancelOrder(CancelOrder),
    /// Replace an order.
    ReplaceOrder(ReplaceOrder),
    /// Stop new orders globally (`None`) or for one account.
    KillSwitch {
        /// The account, or the whole market.
        account: Option<AccountId>,
    },
    /// Take an in-memory snapshot marker and hash.
    Snapshot,
}

/// Why the engine refused a command.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RejectReason {
    /// The `(account, ClOrdID)` is in the window.
    DuplicateClOrdId,
    /// The `OrigClOrdID` is not in the window.
    UnknownOrder,
    /// The order is not open (filled, canceled or rejected).
    OrderNotOpen,
    /// Quantity must be positive.
    InvalidQuantity,
    /// A limit order needs a price; a market order must not have one.
    InvalidPrice,
    /// A post-only order would take liquidity.
    PostOnlyWouldCross,
    /// A fill-or-kill order cannot be filled completely.
    FokNotFillable,
    /// Self-trade prevention refused the taker.
    SelfTradePrevention,
    /// A kill switch is active.
    KillSwitch,
    /// Above the per-order quantity limit.
    MaxOrderQty,
    /// Above the per-order notional limit.
    MaxNotional,
    /// Outside the configured price band.
    PriceBand,
    /// Further from the reference than the fat-finger limit.
    FatFinger,
    /// The account has too many open orders.
    MaxOpenOrders,
    /// The order slab has no free slot.
    BookFull,
    /// The symbol is not configured.
    UnknownSymbol,
    /// The idempotency window is full (should be evicted, kept for safety).
    IdWindowFull,
}

impl RejectReason {
    /// A short stable name for logs and tests.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DuplicateClOrdId => "DuplicateClOrdId",
            Self::UnknownOrder => "UnknownOrder",
            Self::OrderNotOpen => "OrderNotOpen",
            Self::InvalidQuantity => "InvalidQuantity",
            Self::InvalidPrice => "InvalidPrice",
            Self::PostOnlyWouldCross => "PostOnlyWouldCross",
            Self::FokNotFillable => "FokNotFillable",
            Self::SelfTradePrevention => "SelfTradePrevention",
            Self::KillSwitch => "KillSwitch",
            Self::MaxOrderQty => "MaxOrderQty",
            Self::MaxNotional => "MaxNotional",
            Self::PriceBand => "PriceBand",
            Self::FatFinger => "FatFinger",
            Self::MaxOpenOrders => "MaxOpenOrders",
            Self::BookFull => "BookFull",
            Self::UnknownSymbol => "UnknownSymbol",
            Self::IdWindowFull => "IdWindowFull",
        }
    }
}

impl std::fmt::Display for RejectReason {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}
