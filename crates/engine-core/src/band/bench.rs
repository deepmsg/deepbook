//! The ABBA microbenchmark for the price band against the tree it replaced.
//!
//! Ignored by default: it measures, it does not gate CI. Run it pinned to a
//! physical core and read the verdict it prints:
//!
//! ```text
//! taskset -c 3 cargo test -p deepbook-engine-core --lib band::bench \
//!     -- --ignored --nocapture
//! ```
//!
//! Each round replays the same deterministic stream of 500,000 operations
//! (45% new limits, 45% cancels, 10% sweeps of 5-20 levels) over 4,000 price
//! levels, once against `BTreeMap<Price, u32>` and once against
//! `PriceBandSide`. The stream is built before any timing, so both arms do
//! identical work in identical order.
//!
//! Per-operation `Instant::now` would cost more than the operation itself
//! (a vDSO clock read is about half an operation), so a sample is one batch of
//! 64 operations; the reported `E[X | X < p99]` and `p99` are over batch
//! means. Each arm replays the stream once before the measured pass so the
//! book is at steady-state density (the measured pass starts from the same
//! populated state for both arms), and the design's kill criteria are:
//!
//! 1. the band's truncated mean beats the tree's by `max(10%, 2 x floor)`,
//!    where `floor` is the relative drift between the two tree rounds, and
//! 2. the band's p99 does not regress.
//!
//! The last measurement is one recenter of the production geometry with 4,000
//! levels in the band: the cold-path stall the design budgets at 150-350 us.

use std::collections::BTreeMap;
use std::hint::black_box;
use std::time::Instant;

use deepbook_types::Price;

use super::{PRODUCTION_L0_BITS, PriceBandSide, RECENTER_STEP_TICKS, STEP_TICKS};
use crate::ids::EMPTY_SLOT;

const OPERATIONS: usize = 500_000;
const PRICE_LEVELS: usize = 4_000;
const BATCH: usize = 64;

/// xorshift64*: deterministic and dependency-free.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed | 1)
    }

    fn next(&mut self) -> u64 {
        let mut value = self.0;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.0 = value;
        value.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }
}

#[derive(Clone, Copy)]
enum Operation {
    Limit(Price),
    Cancel(Price),
    Sweep(u32),
}

/// The same stream for every arm: the active-set bookkeeping happens here, so
/// neither arm decides what to cancel.
fn build_stream() -> Vec<Operation> {
    let mut rng = Rng::new(0x9e37_79b9_7f4a_7c15);
    let prices: Vec<Price> = (0..PRICE_LEVELS).map(|index| index as Price * 16).collect();
    let mut counts = vec![0_u32; prices.len()];
    let mut active: Vec<usize> = Vec::new();
    let mut stream = Vec::with_capacity(OPERATIONS);

    while stream.len() < OPERATIONS {
        match rng.next() % 100 {
            0..=44 => {
                let slot = (rng.next() % prices.len() as u64) as usize;
                if counts[slot] == 0 {
                    active.push(slot);
                }
                counts[slot] += 1;
                stream.push(Operation::Limit(prices[slot]));
            }
            45..=89 => {
                if active.is_empty() {
                    continue;
                }
                let pick = (rng.next() % active.len() as u64) as usize;
                let slot = active[pick];
                counts[slot] -= 1;
                if counts[slot] == 0 {
                    active.swap_remove(pick);
                }
                stream.push(Operation::Cancel(prices[slot]));
            }
            _ => {
                let levels = 5 + (rng.next() % 16) as u32;
                stream.push(Operation::Sweep(levels));
            }
        }
    }
    stream
}

trait Levels {
    fn limit(&mut self, price: Price);
    fn cancel(&mut self, price: Price);
    fn sweep(&mut self, levels: u32) -> u64;
}

struct TreeArm {
    map: BTreeMap<Price, u32>,
}

impl Levels for TreeArm {
    fn limit(&mut self, price: Price) {
        *self.map.entry(price).or_insert(0) += 1;
    }

    fn cancel(&mut self, price: Price) {
        if let Some(count) = self.map.get_mut(&price) {
            *count -= 1;
            if *count == 0 {
                self.map.remove(&price);
            }
        }
    }

    fn sweep(&mut self, levels: u32) -> u64 {
        self.map
            .iter()
            .take(levels as usize)
            .map(|(_, count)| u64::from(*count))
            .sum()
    }
}

struct BandArm {
    side: PriceBandSide,
}

impl BandArm {
    /// The production book allocates at start-up and its pages fault in as
    /// levels arrive; the benchmark faults them here so the tail it measures
    /// is the operation, not the first touch of a fresh 4 MiB mapping.
    fn new() -> Self {
        let arm = Self {
            side: PriceBandSide::new(PRODUCTION_L0_BITS),
        };
        for level in arm.side.levels.iter() {
            black_box(*level);
        }
        for word in arm.side.l2.iter() {
            black_box(*word);
        }
        for word in arm.side.l1.iter() {
            black_box(*word);
        }
        arm
    }
}

impl Levels for BandArm {
    fn limit(&mut self, price: Price) {
        let level = self.side.level_or_insert(price, 0);
        level.total_qty += 1;
        if level.head == EMPTY_SLOT {
            level.head = 0;
            level.tail = 0;
        }
    }

    fn cancel(&mut self, price: Price) {
        if let Some(level) = self.side.level_mut(price, 0) {
            level.total_qty -= 1;
            if level.total_qty == 0 {
                level.head = EMPTY_SLOT;
                level.tail = EMPTY_SLOT;
            }
        }
        self.side.release_if_empty(price, 0);
    }

    fn sweep(&mut self, levels: u32) -> u64 {
        if self.side.best(false, 0).is_none() {
            return 0;
        }
        self.side
            .in_band_ascending(0)
            .take(levels as usize)
            .map(|(_, level)| level.total_qty as u64)
            .sum()
    }
}

/// Nanoseconds per operation, one sample per batch.
fn run(arm: &mut impl Levels, stream: &[Operation]) -> Vec<f64> {
    let mut samples = Vec::with_capacity(stream.len() / BATCH + 1);
    let mut index = 0;
    while index < stream.len() {
        let end = (index + BATCH).min(stream.len());
        let started = Instant::now();
        for operation in &stream[index..end] {
            match operation {
                Operation::Limit(price) => arm.limit(*price),
                Operation::Cancel(price) => arm.cancel(*price),
                Operation::Sweep(levels) => {
                    black_box(arm.sweep(*levels));
                }
            }
        }
        samples.push(started.elapsed().as_nanos() as f64 / (end - index) as f64);
        index = end;
    }
    samples
}

/// `(E[X | X < p99], p99)` over the samples.
fn summarize(samples: Vec<f64>) -> (f64, f64, Vec<(usize, f64)>) {
    assert!(samples.len() > 10, "too few samples to say anything");
    let mut sorted = samples.clone();
    sorted.sort_by(|left, right| left.total_cmp(right));
    let p99 = sorted[(sorted.len() * 99 / 100).min(sorted.len() - 1)];
    let below: Vec<f64> = sorted
        .iter()
        .copied()
        .filter(|sample| *sample < p99)
        .collect();
    let mean = if below.is_empty() {
        sorted[0]
    } else {
        below.iter().sum::<f64>() / below.len() as f64
    };
    let mut slowest: Vec<(usize, f64)> = samples.iter().copied().enumerate().collect();
    slowest.sort_by(|left, right| right.1.total_cmp(&left.1));
    slowest.truncate(5);
    (mean, p99, slowest)
}

#[test]
#[ignore = "a microbenchmark; run it pinned with --ignored --nocapture"]
fn the_band_against_the_tree() {
    let stream = build_stream();
    assert_eq!(stream.len(), OPERATIONS);

    // ABBA twice: tree, band, band, tree, tree, band, band, tree.
    let mut tree_rounds = Vec::new();
    let mut band_rounds = Vec::new();
    for round in 1..=8 {
        let tree = matches!(round, 1 | 4 | 5 | 8);
        if tree {
            let mut arm = TreeArm {
                map: BTreeMap::new(),
            };
            // Warm the state: a measured pass from an empty book spends its
            // early sweeps scanning empty bitmap words, which is ramp-up, not
            // steady state. The same stream replayed keeps the counts valid.
            let _ = run(&mut arm, &stream);
            let (mean, p99, slowest) = summarize(run(&mut arm, &stream));
            println!(
                "round {round} tree: E[X|X<p99] {mean:.2} ns   p99 {p99:.2} ns   slowest {}",
                slowest.len()
            );
            tree_rounds.push((mean, p99));
        } else {
            let mut arm = BandArm::new();
            let _ = run(&mut arm, &stream);
            let (mean, p99, slowest) = summarize(run(&mut arm, &stream));
            println!(
                "round {round} band: E[X|X<p99] {mean:.2} ns   p99 {p99:.2} ns   slowest {}",
                slowest.len()
            );
            band_rounds.push((mean, p99));
        }
    }

    let average = |rounds: &[(f64, f64)]| {
        let count = rounds.len() as f64;
        (
            rounds.iter().map(|(mean, _)| mean).sum::<f64>() / count,
            rounds.iter().map(|(_, p99)| p99).sum::<f64>() / count,
        )
    };
    let (tree, tree_p99) = average(&tree_rounds);
    let (band, band_p99) = average(&band_rounds);
    let drift = tree_rounds[3].0 - tree_rounds[0].0;
    let floor = drift.abs() / tree_rounds[0].0;
    let improvement = (tree - band) / tree;
    let threshold = 0.10_f64.max(2.0 * floor);

    println!();
    println!("tree {tree:.2} ns (p99 {tree_p99:.2})   band {band:.2} ns (p99 {band_p99:.2})");
    println!(
        "improvement {:.1}%   noise floor {:.1}%   required {:.1}%",
        improvement * 100.0,
        floor * 100.0,
        threshold * 100.0
    );
    let mean_pass = improvement >= threshold;
    let tail_pass = band_p99 <= tree_p99;
    if mean_pass && tail_pass {
        println!("VERDICT: PASS - the band wins on the mean and does not regress p99");
    } else {
        println!("VERDICT: FAIL - mean_pass={mean_pass} tail_pass={tail_pass}");
    }
    assert!(
        mean_pass,
        "the band did not beat max(10%, 2 x floor) on the mean"
    );

    // Diagnostic: time operations one by one to see which kind owns the tail.
    // A vDSO clock read biases each sample by tens of nanoseconds, so only the
    // outliers are informative.
    {
        let mut arm = BandArm::new();
        let mut worst: Vec<(u128, &'static str)> = Vec::new();
        for operation in &stream {
            let started = Instant::now();
            let kind = match operation {
                Operation::Limit(price) => {
                    arm.limit(*price);
                    "limit"
                }
                Operation::Cancel(price) => {
                    arm.cancel(*price);
                    "cancel"
                }
                Operation::Sweep(levels) => {
                    black_box(arm.sweep(*levels));
                    "sweep"
                }
            };
            let elapsed = started.elapsed().as_nanos();
            if elapsed > 500 {
                worst.push((elapsed, kind));
                if worst.len() == 10 {
                    break;
                }
            }
        }
        worst.sort_by_key(|entry| std::cmp::Reverse(entry.0));
        println!("per-op outliers (>500 ns): {worst:?}");
    }

    // The cold path: one recenter of the production geometry with 4,000
    // levels in the band.
    let mut side = PriceBandSide::new(PRODUCTION_L0_BITS);
    for index in 0..PRICE_LEVELS {
        let level = side.level_or_insert(index as Price * 16, 0);
        level.total_qty = 1;
        level.head = 0;
        level.tail = 0;
    }
    let shift = RECENTER_STEP_TICKS / STEP_TICKS;
    let started = Instant::now();
    side.recenter(0, RECENTER_STEP_TICKS);
    let elapsed = started.elapsed();
    println!(
        "recenter by {shift} units ({RECENTER_STEP_TICKS} ticks): {} us",
        elapsed.as_micros()
    );
}
