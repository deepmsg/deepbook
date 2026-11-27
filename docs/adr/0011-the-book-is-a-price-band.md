# ADR 0011: the order book is a price band and a radix-64 bitmap

- Status: **Accepted** — 2026-11-27, implemented by M6
- Date: 2026-11-27
- Deciders: project owner

## Context

M0-M5 kept the two sides of the book as `BTreeMap<Price, PriceLevel>`. That
has two costs the M6 gate is about:

1. an order that opens a price level that did not exist allocates a B-tree
   node, so "construction is the only allocation" is false on the busiest
   path a market maker uses;
2. the best price and the next level are pointer chases through tree nodes,
   and the iteration order that `Book::hash`, `Book::view` and the snapshot
   depend on is the tree's.

`OrderHot` also measured 64 bytes with 16-byte alignment (ADR 0003's
2026-11-20 note): the slab was not guaranteed to keep an order inside one
cache line.

## Decision

1. **One side is a contiguous band.** `PriceBandSide` holds the levels of one
   side in a `Box<[PriceLevel]>` indexed by `price - base`, plus a three-level
   bitmap: `l0: u64` over 64 `l1` words, each over 64 `l2` words, each over 64
   levels. The production geometry is 64 `l1` words: a 262,144-tick band and
   4 MiB of levels per side. The geometry is a runtime parameter
   (`new(l0_bits)`), not a const generic: production and the small-geometry
   tests run the same code, and `Engine`/`EngineConfig` do not grow a type
   parameter.
2. **Best price is three `tzcnt`/`lzcnt`s; a new level is an array write and
   a bit set.** Levels are addressed by offset, so no node allocation and no
   pointer chase. Matching, cancel, replace, best-price and iteration never
   allocate.
3. **Out-of-band levels live in a `BTreeMap` on the cold path.** An order
   whose price is outside the band still rests (`outliers`), so a violent
   price move never rejects an order. Inserting there is the one allocating
   path; `outlier_inserts` counts it and the allocation test asserts it stays
   zero for an in-band workload.
4. **One recenter, derived from the last trade.** Both sides share `base`.
   When the last trade comes within `RECENTER_MARGIN` (16,384 ticks) of an
   edge, or leaves the band, the base moves to the trade price centred in the
   band and aligned to `RECENTER_STEP_TICKS` (65,536). A move is a multiple of
   one `l1` word (4,096 ticks = 64 `l2` words = one `l0` bit), so it is a
   `copy_within` of the levels and the two word arrays, a bit shift of `l0`, an
   explicit zero of the vacated region, and admission of the levels the new
   window covers. The recenter runs once per command, after matching.
5. **The base is persisted.** The snapshot schema goes to version 3 with
   `hasBasePrice` and `basePrice`. A version-2 record carries no base; the
   reader derives one from the levels' mid, which is only sound when the whole
   cluster cold-restarts from that record — a live member's band may have moved
   since. Recovery from a v3 record is byte-for-byte reproducible.
6. **Iteration is the merged price order.** `Book::hash`, `Book::view`,
   `Book::snapshot` and the snapshot groups read outliers below the band, the
   bitmap ascending, and outliers above — or the reverse for bids. The hash is
   unchanged.
7. **`OrderHot` is one cache line.** `repr(C, align(64))` with compile-time
   `offset_of!` assertions; `Side` and `OrderState` are `repr(u8)` so the
   offsets are guaranteed (ADR 0003 records this as the aligned-slab option).

## Alternatives considered

- **Keep the `BTreeMap` and accept the level-creation allocation.** Rejected:
  it is the one allocation left on the market-maker path, and the bitmap's
  best-price cost is smaller than the tree's.
- **Const-generic geometry.** Rejected: it buys a compile-time constant at the
  price of a type parameter through `Engine`, `EngineConfig` and the service.
- **Reject out-of-band prices.** Rejected: a risk configuration already has a
  price band (`RiskConfig::price_band_ticks`); the book must hold what risk
  admits.
- **A prefixed or per-symbol family of bands.** Deferred: the engine is one
  symbol in the MVP, and the memory accounting is per symbol (about 8 MiB).

## Consequences

- **Memory.** About 8 MiB per symbol (two 4 MiB level arrays plus about 65 KiB
  of bitmap), allocated at construction. A test book with `l0_bits = 4` is a
  few hundred KiB.
- **Recenter is cold but not free.** With 4,000 levels in the band a measured
  recenter costs 415-686 us (production geometry, one 65,536-tick move),
  against the 150-350 us the design estimated. It is deterministic and rare
  (a trade has to come within $163.84 of an edge of a $2,621.44 band), but it
  happens inside one command, so it is a tail event M6 owns. The benchmark
  below prints it on every run.
- **Snapshot v2 is only safe for a whole-cluster cold restart.** The upgrade
  order is: every node reads v3, then every node writes v3.
- **`crates/engine-cluster`'s DBS1 codec cannot carry the base**; a DBS1
  round-trip restores with a derived base and its test says so.

## Measurement

`taskset -c 3 cargo test --release -p deepbook-engine-core --lib band::bench
-- --ignored --nocapture`

One deterministic stream (500,000 operations over 4,000 price levels: 45% new
limits, 45% cancels, 10% sweeps of 5-20 levels), eight ABBA rounds, batch
means of 64 operations, each arm replayed once before the measured pass so the
book is at steady-state density. Over three runs:

| arm | E[X \| X < p99] | p99 |
| --- | --- | --- |
| `BTreeMap<Price, u32>` | 56.4-57.0 ns | 66.1-67.3 ns |
| `PriceBandSide` | 17.0-17.3 ns | 23.0-23.5 ns |

69.7-69.8% better on the truncated mean, and the p99 improves rather than
regresses (the design's two kill criteria); the noise floor between the first
and last tree rounds is 0.2-0.3%.

The first measurement of this benchmark, from an empty book, showed a p99 of
195-285 ns for the band: the early sweeps scan nearly empty bitmap words.
That is ramp-up, not steady state, and warming the measured arm removed it.
The benchmark keeps a cold-start diagnostic for the same reason.
