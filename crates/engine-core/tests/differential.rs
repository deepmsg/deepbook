//! Differential test: the slab engine against a naive `Vec` oracle.
//!
//! The oracle is deliberately O(n): it sorts the whole opposite side on every
//! fill. It shares no matching code with the engine. Both consume the same
//! generated command stream; the test compares the trade list and the final
//! book view (FIFO order, quantities and fills).
//!
//! Risk and self-trade prevention are disabled here: those are checked by the
//! engine's unit tests and would double the oracle's semantics.

use deepbook_engine_core::{
    BookView, CancelOrder, ClientOrderId, Command, Engine, EngineConfig, Event, LevelView,
    NewOrder, OrdType, OrderView, ReplaceOrder, RiskConfig, TimeInForce,
};
use deepbook_types::{AccountId, Price, Qty, Side};
use proptest::prelude::*;

const SYMBOL: u16 = 1;

fn cl_id(pool: u8) -> ClientOrderId {
    format!("C{}", pool % 16).parse().expect("client order id")
}

#[derive(Clone, Debug)]
enum Op {
    New {
        account: u8,
        cl: u8,
        side: bool,
        price: Price,
        qty: Qty,
        market: bool,
        tif: u8,
        post_only: bool,
    },
    Cancel {
        account: u8,
        cl: u8,
        orig: u8,
    },
    Replace {
        account: u8,
        cl: u8,
        orig: u8,
        price: Price,
        qty: Qty,
    },
}

impl Op {
    fn account(&self) -> AccountId {
        let value = match self {
            Self::New { account, .. }
            | Self::Cancel { account, .. }
            | Self::Replace { account, .. } => *account,
        };
        AccountId::from(value % 3)
    }

    fn command(&self) -> Command {
        match self {
            Self::New {
                cl,
                side,
                price,
                qty,
                market,
                tif,
                post_only,
                ..
            } => {
                let ord_type = if *market {
                    OrdType::Market
                } else {
                    OrdType::Limit
                };
                let time_in_force = match tif % 4 {
                    0 => TimeInForce::Day,
                    1 => TimeInForce::GoodTillCanceled,
                    2 => TimeInForce::ImmediateOrCancel,
                    _ => TimeInForce::FillOrKill,
                };
                Command::NewOrder(NewOrder {
                    symbol: SYMBOL,
                    account: self.account(),
                    cl_ord_id: cl_id(*cl),
                    side: if *side { Side::Buy } else { Side::Sell },
                    price: if *market { None } else { Some(*price) },
                    qty: *qty,
                    ord_type,
                    time_in_force,
                    post_only: *post_only,
                })
            }
            Self::Cancel { cl, orig, .. } => Command::CancelOrder(CancelOrder {
                account: self.account(),
                cl_ord_id: cl_id(*cl),
                orig_cl_ord_id: cl_id(*orig),
            }),
            Self::Replace {
                cl,
                orig,
                price,
                qty,
                ..
            } => Command::ReplaceOrder(ReplaceOrder {
                account: self.account(),
                cl_ord_id: cl_id(*cl),
                orig_cl_ord_id: cl_id(*orig),
                price: Some(*price),
                qty: *qty,
                ord_type: OrdType::Limit,
                time_in_force: TimeInForce::GoodTillCanceled,
                post_only: false,
            }),
        }
    }
}

#[derive(Clone, Debug)]
struct OracleOrder {
    seq: u64,
    account: AccountId,
    cl: ClientOrderId,
    side: Side,
    price: Option<Price>,
    qty: Qty,
    filled: Qty,
    ord_type: OrdType,
    tif: TimeInForce,
    active: bool,
}

impl OracleOrder {
    fn leaves(&self) -> Qty {
        self.qty - self.filled
    }
}

struct Oracle {
    orders: Vec<OracleOrder>,
    next_seq: u64,
    trades: Vec<(ClientOrderId, ClientOrderId, Price, Qty)>,
    last_trade: Option<Price>,
}

impl Oracle {
    fn new() -> Self {
        Self {
            orders: Vec::new(),
            next_seq: 1,
            trades: Vec::new(),
            last_trade: None,
        }
    }

    fn apply(&mut self, op: &Op) {
        match op {
            Op::New { .. } => self.new_order(op),
            Op::Cancel { orig, .. } => self.cancel(op.account(), cl_id(*orig)),
            Op::Replace {
                orig, price, qty, ..
            } => self.replace(op.account(), cl_id(*orig), *price, *qty),
        }
    }

    fn new_order(&mut self, op: &Op) {
        let Op::New {
            side,
            price,
            qty,
            market,
            tif,
            post_only,
            ..
        } = op
        else {
            return;
        };
        let account = op.account();
        let cl = match op {
            Op::New { cl, .. } => cl_id(*cl),
            _ => unreachable!(),
        };
        // A client id already used for this account is a duplicate.
        if self
            .orders
            .iter()
            .any(|order| order.account == account && order.cl == cl)
        {
            return;
        }
        let side = if *side { Side::Buy } else { Side::Sell };
        let ord_type = if *market {
            OrdType::Market
        } else {
            OrdType::Limit
        };
        let limit = if *market { None } else { Some(*price) };
        let time_in_force = match tif % 4 {
            0 => TimeInForce::Day,
            1 => TimeInForce::GoodTillCanceled,
            2 => TimeInForce::ImmediateOrCancel,
            _ => TimeInForce::FillOrKill,
        };

        let order = OracleOrder {
            seq: self.next_seq,
            account,
            cl,
            side,
            price: limit,
            qty: *qty,
            filled: 0,
            ord_type,
            tif: time_in_force,
            active: false,
        };
        self.next_seq += 1;

        let reject = *qty <= 0
            || (*post_only && (ord_type != OrdType::Limit || self.would_cross(side, limit)))
            || (time_in_force == TimeInForce::FillOrKill && self.fillable(side, limit) < *qty);
        if reject {
            self.orders.push(order);
            return;
        }

        self.orders.push(order);
        let index = self.orders.len() - 1;
        self.match_taker(index);
        let order = &mut self.orders[index];
        if order.filled == order.qty {
            order.active = false;
        } else if order.tif.rests(order.ord_type) {
            order.active = true;
        }
    }

    fn cancel(&mut self, account: AccountId, orig: ClientOrderId) {
        let Some(index) = self
            .orders
            .iter()
            .position(|order| order.account == account && order.cl == orig)
        else {
            return;
        };
        let order = &mut self.orders[index];
        if order.active {
            order.active = false;
        }
    }

    fn replace(&mut self, account: AccountId, orig: ClientOrderId, price: Price, qty: Qty) {
        let Some(index) = self
            .orders
            .iter()
            .position(|order| order.account == account && order.cl == orig)
        else {
            return;
        };
        if !self.orders[index].active || qty <= self.orders[index].filled {
            return;
        }
        let same_price = self.orders[index].price == Some(price);
        let reduces = qty <= self.orders[index].qty;
        self.orders[index].price = Some(price);
        self.orders[index].qty = qty;
        if same_price && reduces {
            return;
        }
        self.orders[index].active = false;
        self.orders[index].seq = self.next_seq;
        self.next_seq += 1;
        self.match_taker(index);
        let order = &mut self.orders[index];
        order.active = order.filled != order.qty;
    }

    fn match_taker(&mut self, taker_index: usize) {
        loop {
            let taker = &self.orders[taker_index];
            let leaves = taker.leaves();
            if leaves <= 0 {
                break;
            }
            let side = taker.side;
            let limit = taker.price;
            let mut best: Option<usize> = None;
            for (index, maker) in self.orders.iter().enumerate() {
                if index == taker_index
                    || !maker.active
                    || maker.side == side
                    || !crosses(side, limit, maker.price)
                {
                    continue;
                }
                best = match best {
                    None => Some(index),
                    Some(current) => {
                        if better(side, maker, &self.orders[current]) {
                            Some(index)
                        } else {
                            Some(current)
                        }
                    }
                };
            }
            let Some(maker_index) = best else {
                break;
            };
            let maker_price = self.orders[maker_index]
                .price
                .expect("resting order has price");
            let trade_qty = leaves.min(self.orders[maker_index].leaves());
            if trade_qty <= 0 {
                break;
            }
            let maker_cl = self.orders[maker_index].cl;
            let taker_cl = self.orders[taker_index].cl;
            self.orders[taker_index].filled += trade_qty;
            self.orders[maker_index].filled += trade_qty;
            self.trades
                .push((maker_cl, taker_cl, maker_price, trade_qty));
            self.last_trade = Some(maker_price);
            if self.orders[maker_index].leaves() == 0 {
                self.orders[maker_index].active = false;
            }
            if self.orders[taker_index].leaves() == 0 {
                break;
            }
        }
    }

    fn would_cross(&self, side: Side, limit: Option<Price>) -> bool {
        self.orders
            .iter()
            .any(|order| order.active && order.side != side && crosses(side, limit, order.price))
    }

    fn fillable(&self, side: Side, limit: Option<Price>) -> Qty {
        self.orders
            .iter()
            .filter(|order| order.active && order.side != side && crosses(side, limit, order.price))
            .map(OracleOrder::leaves)
            .sum()
    }

    fn view(&self) -> BookView {
        BookView {
            symbol: SYMBOL,
            bids: self.side_view(Side::Buy),
            asks: self.side_view(Side::Sell),
            last_trade: self.last_trade,
            md_seq: 0,
        }
    }

    fn side_view(&self, side: Side) -> Vec<LevelView> {
        let mut active: Vec<&OracleOrder> = self
            .orders
            .iter()
            .filter(|order| order.active && order.side == side)
            .collect();
        active.sort_by_key(|order| {
            let price = order.price.unwrap_or(0);
            let key = if side == Side::Buy { -price } else { price };
            (key, order.seq)
        });
        let mut levels: Vec<LevelView> = Vec::new();
        for order in active {
            let price = order.price.expect("resting order has price");
            if levels.last().is_none_or(|level| level.price != price) {
                levels.push(LevelView {
                    price,
                    qty: 0,
                    orders: Vec::new(),
                });
            }
            let level = levels.last_mut().expect("just pushed");
            level.qty += order.leaves();
            level.orders.push(OrderView {
                cl_ord_id: order.cl,
                account: order.account,
                side: order.side,
                price,
                qty: order.qty,
                filled: order.filled,
            });
        }
        levels
    }
}

fn crosses(side: Side, limit: Option<Price>, maker: Option<Price>) -> bool {
    let Some(maker) = maker else {
        return false;
    };
    match (side, limit) {
        (Side::Buy, Some(limit)) => maker <= limit,
        (Side::Sell, Some(limit)) => maker >= limit,
        (_, None) => true,
    }
}

fn better(side: Side, candidate: &OracleOrder, current: &OracleOrder) -> bool {
    let candidate_price = candidate.price.unwrap_or(0);
    let current_price = current.price.unwrap_or(0);
    match side {
        Side::Buy => (candidate_price, candidate.seq) < (current_price, current.seq),
        Side::Sell => (-candidate_price, candidate.seq) < (-current_price, current.seq),
    }
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        (
            0_u8..3,
            0_u8..16,
            any::<bool>(),
            90_i64..=110,
            1_i64..=20,
            any::<bool>(),
            0_u8..4,
            any::<bool>(),
        )
            .prop_map(
                |(account, cl, side, price, qty, market, tif, post_only)| Op::New {
                    account,
                    cl,
                    side,
                    price,
                    qty,
                    market,
                    tif,
                    post_only,
                }
            ),
        (0_u8..3, 0_u8..16, 0_u8..16).prop_map(|(account, cl, orig)| Op::Cancel {
            account,
            cl,
            orig,
        }),
        (0_u8..3, 0_u8..16, 0_u8..16, 90_i64..=110, 1_i64..=20).prop_map(
            |(account, cl, orig, price, qty)| Op::Replace {
                account,
                cl,
                orig,
                price,
                qty,
            }
        ),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn the_engine_matches_the_vec_oracle(ops in prop::collection::vec(op_strategy(), 0..64)) {
        let config = EngineConfig {
            symbol: SYMBOL,
            max_orders: 4_096,
            recent_order_window: 4_096,
            risk: RiskConfig::disabled(),
        };
        let mut engine = Engine::new(config);
        let mut oracle = Oracle::new();
        let mut engine_trades: Vec<(ClientOrderId, ClientOrderId, Price, Qty)> = Vec::new();

        for (index, op) in ops.iter().enumerate() {
            let mut events = Vec::new();
            engine.on_command(&op.command(), index as i64 + 1, &mut events);
            for event in &events {
                if let Event::Trade(trade) = event {
                    engine_trades.push((
                        trade.maker_cl_ord_id,
                        trade.taker_cl_ord_id,
                        trade.price,
                        trade.qty,
                    ));
                }
            }
            oracle.apply(op);
        }

        let mut engine_view = engine.view();
        let engine_last_trade = engine_view.last_trade;
        engine_view.md_seq = 0;
        prop_assert_eq!(oracle.trades.clone(), engine_trades);
        prop_assert_eq!(oracle.last_trade, engine_last_trade);
        prop_assert_eq!(oracle.view(), engine_view);
    }
}
