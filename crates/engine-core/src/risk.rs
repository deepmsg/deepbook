//! Pre-trade risk, as pure functions of the command and current state.
//!
//! Every check here is deterministic: the same command against the same state
//! gives the same decision on every replay. Market-data and FIX timestamps are
//! not inputs.

use deepbook_types::{Price, Qty};

use crate::command::{NewOrder, OrdType, RejectReason};

/// What the matching path does when the resting order belongs to the same
/// account as the taker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SelfTradePrevention {
    /// Allow the trade.
    None,
    /// Cancel the resting order and keep matching.
    CancelResting,
    /// Refuse the incoming order before any fill.
    RejectTaker,
}

/// Per-engine risk limits. The defaults are disabled, which is what the
/// differential oracle uses.
#[derive(Clone, Copy, Debug)]
pub struct RiskConfig {
    /// Maximum quantity of one order.
    pub max_order_qty: Qty,
    /// Maximum `price * qty` of one order.
    pub max_notional: i128,
    /// Maximum distance from the reference price, in ticks.
    pub price_band_ticks: Option<Price>,
    /// Maximum distance from the reference price for a limit order, in ticks.
    pub fat_finger_ticks: Option<Price>,
    /// Reference price when the book has no last trade.
    pub reference_price: Option<Price>,
    /// Maximum open (resting) orders per account.
    pub max_open_orders_per_account: usize,
    /// Self-trade policy.
    pub self_trade: SelfTradePrevention,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            max_order_qty: Qty::MAX,
            max_notional: i128::MAX,
            price_band_ticks: None,
            fat_finger_ticks: None,
            reference_price: None,
            max_open_orders_per_account: usize::MAX,
            self_trade: SelfTradePrevention::None,
        }
    }
}

impl RiskConfig {
    /// All checks disabled.
    #[must_use]
    pub fn disabled() -> Self {
        Self::default()
    }

    /// Check a new order. `open_orders` is the account's resting order count
    /// before this command.
    ///
    /// # Errors
    ///
    /// The first limit the order breaks, in the fixed order documented by the
    /// engine (kill switch, quantity, notional, band, fat finger, open count).
    pub fn check_new_order(
        &self,
        order: &NewOrder,
        last_trade: Option<Price>,
        open_orders: usize,
        killed: bool,
    ) -> Result<(), RejectReason> {
        if killed {
            return Err(RejectReason::KillSwitch);
        }
        if order.qty <= 0 {
            return Err(RejectReason::InvalidQuantity);
        }
        if order.ord_type == OrdType::Limit && order.price.is_none() {
            return Err(RejectReason::InvalidPrice);
        }
        if order.ord_type == OrdType::Market && order.price.is_some() {
            return Err(RejectReason::InvalidPrice);
        }
        if order.qty > self.max_order_qty {
            return Err(RejectReason::MaxOrderQty);
        }

        let reference = last_trade.or(self.reference_price);
        let notional_price = order.price.or(reference);
        if let Some(price) = notional_price {
            let notional = i128::from(price) * i128::from(order.qty);
            if notional > self.max_notional {
                return Err(RejectReason::MaxNotional);
            }
        }

        if let (Some(price), Some(reference)) = (order.price, reference) {
            let distance = (i128::from(price) - i128::from(reference)).abs();
            if let Some(band) = self.price_band_ticks {
                if distance > i128::from(band) {
                    return Err(RejectReason::PriceBand);
                }
            }
            if let Some(fat_finger) = self.fat_finger_ticks {
                if distance > i128::from(fat_finger) {
                    return Err(RejectReason::FatFinger);
                }
            }
        }

        if self.max_open_orders_per_account != usize::MAX
            && order.time_in_force.rests(order.ord_type)
            && open_orders >= self.max_open_orders_per_account
        {
            return Err(RejectReason::MaxOpenOrders);
        }
        Ok(())
    }
}
