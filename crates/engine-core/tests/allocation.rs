//! The counted-allocation check from M2's acceptance.
//!
//! After warmup, matching against an existing price level and canceling a
//! resting order must not allocate. The allocator counts only the test
//! thread; the test harness runs on that thread, but the measured loops touch
//! no `Vec` or map that grows.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use deepbook_engine_core::{
    CancelOrder, ClientOrderId, Command, Engine, EngineConfig, Event, EventSink, NewOrder, OrdType,
    RiskConfig, TimeInForce,
};
use deepbook_types::{AccountId, Price, Qty, Side};

thread_local! {
    static TRACKING: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

struct CountingAllocator;

// SAFETY: every method forwards its arguments unchanged to `System`; the
// counting wrapper adds no invariants and never changes the allocation.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count();
        // SAFETY: the layout is forwarded unchanged to the system allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count();
        // SAFETY: the layout is forwarded unchanged to the system allocator.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        count();
        // SAFETY: the pointer and layouts are forwarded unchanged.
        unsafe { System.realloc(pointer, layout, new_size) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: the pointer and layout are forwarded unchanged.
        unsafe { System.dealloc(pointer, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn count() {
    let _ = TRACKING.try_with(|tracking| {
        if tracking.get() {
            let _ = ALLOCATIONS.try_with(|allocations| {
                allocations.set(allocations.get() + 1);
            });
        }
    });
}

#[derive(Default)]
struct NullSink;

impl EventSink for NullSink {
    fn emit(&mut self, _event: &Event) {}
}

fn client_order_id(index: u64) -> ClientOrderId {
    // Base-26 digits, no allocation.
    let mut bytes = [b'X'; ClientOrderId::LENGTH];
    let mut value = index;
    for slot in (1..17).rev() {
        bytes[slot] = b'a' + u8::try_from(value % 26).expect("digit");
        value /= 26;
    }
    ClientOrderId::new(&bytes).expect("client order id")
}

fn order(account: AccountId, cl: ClientOrderId, side: Side, price: Price, qty: Qty) -> Command {
    Command::NewOrder(NewOrder {
        symbol: 1,
        account,
        cl_ord_id: cl,
        side,
        price: Some(price),
        qty,
        ord_type: OrdType::Limit,
        time_in_force: TimeInForce::GoodTillCanceled,
        post_only: false,
    })
}

#[test]
fn matching_and_canceling_do_not_allocate_after_warmup() {
    const RESTING: u64 = 5_000;
    const MATCHES: u64 = 5_000;

    let mut engine = Engine::new(EngineConfig {
        symbol: 1,
        max_orders: 20_000,
        recent_order_window: 20_000,
        risk: RiskConfig::disabled(),
    });
    let mut sink = NullSink;

    // Warmup: a deep resting sell at 100 and many resting buys at 90. Both
    // price levels, the slab and the id window are populated before counting.
    engine.on_command(
        &order(1, client_order_id(0), Side::Sell, 100, 1_000_000),
        1,
        &mut sink,
    );
    for index in 0..RESTING {
        engine.on_command(
            &order(2, client_order_id(index + 2), Side::Buy, 90, 1),
            index as i64 + 2,
            &mut sink,
        );
    }

    TRACKING.with(|tracking| tracking.set(true));
    ALLOCATIONS.with(|allocations| allocations.set(0));

    // Matching at the existing ask level: each buy takes one lot and leaves.
    for index in 0..MATCHES {
        engine.on_command(
            &order(3, client_order_id(20_000 + index), Side::Buy, 100, 1),
            index as i64 + 10_000,
            &mut sink,
        );
    }

    // Canceling the resting bids: existing level, existing slab slots.
    for index in 0..RESTING {
        engine.on_command(
            &Command::CancelOrder(CancelOrder {
                account: 2,
                cl_ord_id: client_order_id(40_000 + index),
                orig_cl_ord_id: client_order_id(index + 2),
            }),
            index as i64 + 20_000,
            &mut sink,
        );
    }

    let allocations = ALLOCATIONS.with(std::cell::Cell::get);
    TRACKING.with(|tracking| tracking.set(false));
    assert_eq!(0, allocations, "the warm paths allocated");
}
