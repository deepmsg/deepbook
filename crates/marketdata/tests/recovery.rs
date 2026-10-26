//! End-to-end recovery over a simulated lossy incremental link.
//!
//! Frames are encoded with [`deepbook_marketdata::wire`] before delivery, so
//! the test exercises the real frame on every hop. A dropped frame creates the
//! gap; the subscriber asks the service for the missing incrementals (or a
//! snapshot when the hole is older than the service's ring).

use deepbook_marketdata::{
    ApplyOutcome, Incremental, MarketDataService, RecoveryRequest, RecoveryResponse, ServiceConfig,
    Subscriber, WireMessage, decode, encode,
};
use deepbook_types::Side;
use proptest::prelude::*;

fn frame(incremental: &Incremental) -> Vec<u8> {
    encode(&WireMessage::Incremental(*incremental))
}

fn response_frame(response: &RecoveryResponse) -> Vec<u8> {
    encode(&WireMessage::RecoveryResponse(response.clone()))
}

fn decoded_incremental(frame: &[u8]) -> Incremental {
    match decode(frame).expect("decode incremental") {
        WireMessage::Incremental(incremental) => incremental,
        other => panic!("expected an incremental, got {other:?}"),
    }
}

fn decoded_response(frame: &[u8]) -> RecoveryResponse {
    match decode(frame).expect("decode response") {
        WireMessage::RecoveryResponse(response) => response,
        other => panic!("expected a recovery response, got {other:?}"),
    }
}

#[derive(Clone, Copy, Debug)]
enum Event {
    Update {
        side: Side,
        price: i64,
        qty: i64,
    },
    Trade {
        price: i64,
        qty: i64,
        aggressor: Side,
    },
}

fn publish(service: &mut MarketDataService, seq: u64, event: Event) -> Incremental {
    match event {
        Event::Update { side, price, qty } => service
            .on_book_update(seq, side, price, qty)
            .expect("book update"),
        Event::Trade {
            price,
            qty,
            aggressor,
        } => service.on_trade(seq, price, qty, aggressor).expect("trade"),
    }
}

fn run_scenario(events: &[Event], base: usize, drop: impl Fn(usize) -> bool) -> (u64, u64) {
    let mut service = MarketDataService::new(ServiceConfig {
        symbol: 1,
        recent_capacity: 8,
    });
    let mut subscriber = Subscriber::new(1);
    let mut last = 0_u64;

    for (index, event) in events.iter().copied().enumerate() {
        let seq = index as u64 + 1;
        let incremental = publish(&mut service, seq, event);
        if seq as usize == base {
            let snapshot = service.snapshot();
            let encoded = encode(&WireMessage::Snapshot(snapshot.clone()));
            let decoded = match decode(&encoded).expect("snapshot") {
                WireMessage::Snapshot(snapshot) => snapshot,
                other => panic!("expected a snapshot, got {other:?}"),
            };
            assert_eq!(ApplyOutcome::Synced, subscriber.on_snapshot(&decoded));
            last = seq;
            continue;
        }
        if seq as usize <= base {
            continue;
        }
        if drop(seq as usize) {
            continue;
        }
        let delivered = decoded_incremental(&frame(&incremental));
        let outcome = subscriber.on_incremental(&delivered);
        if matches!(
            outcome,
            ApplyOutcome::Gap { .. } | ApplyOutcome::NeedsSnapshot
        ) {
            let request = subscriber.recovery_request().expect("a request");
            let response = service.recover(&request);
            let decoded = decoded_response(&response_frame(&response));
            let _ = subscriber.on_recovery(&decoded);
        }
        assert!(subscriber.last_md_seq().expect("base") >= last);
        last = subscriber.last_md_seq().expect("base");
    }

    // The tail may have been dropped. A running subscriber would learn that
    // from the next frame or the periodic snapshot; the test asks directly.
    if subscriber.last_md_seq() != Some(service.last_md_seq()) {
        let request = RecoveryRequest {
            symbol: 1,
            last_md_seq: subscriber.last_md_seq().unwrap_or(0),
        };
        let response = service.recover(&request);
        let decoded = decoded_response(&response_frame(&response));
        let _ = subscriber.on_recovery(&decoded);
    }

    assert_eq!(service.book(), subscriber.book());
    assert_eq!(Some(service.last_md_seq()), subscriber.last_md_seq());
    (
        service.last_md_seq(),
        subscriber.last_md_seq().expect("synced"),
    )
}

fn sample_events() -> Vec<Event> {
    let mut events = Vec::new();
    for index in 0..40_i64 {
        if index % 3 == 0 {
            events.push(Event::Trade {
                price: 100 + index % 5,
                qty: 1 + index % 4,
                aggressor: if index % 2 == 0 {
                    Side::Buy
                } else {
                    Side::Sell
                },
            });
        } else {
            events.push(Event::Update {
                side: if index % 2 == 0 {
                    Side::Sell
                } else {
                    Side::Buy
                },
                price: 100 + index % 7,
                qty: 1 + index % 9,
            });
        }
    }
    events
}

#[test]
fn a_gap_within_the_ring_is_repaired_with_incrementals() {
    let events = sample_events();
    let (service_last, subscriber_last) = run_scenario(&events, 5, |seq| seq % 4 == 0);
    assert_eq!(service_last, subscriber_last);
}

#[test]
fn a_long_blackout_is_repaired_with_a_snapshot() {
    let events = sample_events();
    // Drop every sequence from 10 through 25; the ring holds eight, so the
    // recovery must return a snapshot.
    let (service_last, subscriber_last) = run_scenario(&events, 5, |seq| (10..=25).contains(&seq));
    assert_eq!(service_last, subscriber_last);
}

#[test]
fn a_subscriber_without_a_base_gets_a_snapshot() {
    let mut service = MarketDataService::new(ServiceConfig {
        symbol: 1,
        recent_capacity: 8,
    });
    for seq in 1..=3 {
        service
            .on_book_update(seq, Side::Sell, 100 + seq as i64, 1)
            .expect("update");
    }
    let mut subscriber = Subscriber::new(1);
    let request = RecoveryRequest {
        symbol: 1,
        last_md_seq: 0,
    };
    let response = service.recover(&request);
    assert!(matches!(response, RecoveryResponse::Snapshot(_)));
    let decoded = decoded_response(&response_frame(&response));
    assert_eq!(ApplyOutcome::Synced, subscriber.on_recovery(&decoded));
    assert_eq!(service.book(), subscriber.book());
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn random_streams_and_drops_recover_to_the_same_book(
        base in 1_usize..=10,
        drop_every in 2_usize..=9,
        events in prop::collection::vec(
            (any::<bool>(), any::<bool>(), 90_i64..=110, 1_i64..=20),
            12..=48,
        ),
    ) {
        let events: Vec<Event> = events
            .into_iter()
            .map(|(is_trade, side, price, qty)| {
                let side = if side { Side::Buy } else { Side::Sell };
                if is_trade {
                    Event::Trade { price, qty, aggressor: side }
                } else {
                    Event::Update { side, price, qty }
                }
            })
            .collect();
        let base = base.min(events.len());
        let (service_last, subscriber_last) =
            run_scenario(&events, base, |seq| seq % drop_every == 0);
        prop_assert_eq!(service_last, subscriber_last);
    }
}
