//! One side of the book as a contiguous price band plus a three-level bitmap.
//!
//! The band covers `[base, base + band_ticks)`. A level inside it is addressed
//! by `idx = price - base` and lives in `levels`; a level outside it lives in
//! `outliers`, a `BTreeMap` on the cold path. A radix-64 bitmap finds the best
//! in-band level in three `tzcnt`/`lzcnt`s.
//!
//! Geometry: `l1` has `l0_bits` words, `l2` has `l0_bits * 64` words and the
//! band covers `l0_bits * STEP_TICKS` ticks. The production geometry is
//! `l0_bits = 64` (262,144 ticks, 4 MiB of levels per side); a test may build
//! `l0_bits = 4` so that a recenter moves whole `l1` words and `l0` bits while
//! every array is small.
//!
//! # Allocation contract
//!
//! `new` allocates the three arrays; nothing else does. In-band insert, match,
//! cancel, replace, best-price and iteration are allocation-free. The cold
//! paths are a level that falls outside the band (`outliers`) and a recenter
//! that pushes a non-empty edge level out of it; `outlier_inserts` counts the
//! former so a test can assert it stayed zero.

use std::collections::BTreeMap;

use deepbook_types::Price;
#[cfg(test)]
use deepbook_types::Qty;

use crate::book::PriceLevel;
use crate::ids::EMPTY_SLOT;

/// Ticks covered by one `l1` word: the unit a recenter moves.
pub(crate) const STEP_TICKS: i64 = 4_096;

/// The production geometry: 64 `l1` words, 4,096 `l2` words, 262,144 ticks.
pub(crate) const PRODUCTION_L0_BITS: usize = 64;

/// The production band width in ticks.
pub(crate) const PRODUCTION_BAND_TICKS: i64 = PRODUCTION_L0_BITS as i64 * STEP_TICKS;

/// A recenter moves `base` by a multiple of this many ticks.
pub(crate) const RECENTER_STEP_TICKS: i64 = 16 * STEP_TICKS;

/// A trade this close to an edge triggers a recenter.
pub(crate) const RECENTER_MARGIN: i64 = 16_384;

/// One side of the book: bids or asks.
pub(crate) struct PriceBandSide {
    /// Bit `g` set iff `l1[g] != 0`.
    l0: u64,
    /// Bit `b` of `l1[g]` set iff `l2[g * 64 + b] != 0`.
    l1: Box<[u64]>,
    /// Bit `b` of `l2[w]` set iff `levels[w * 64 + b].head != EMPTY_SLOT`.
    l2: Box<[u64]>,
    /// The levels inside the band; an empty level has `head == EMPTY_SLOT`.
    levels: Box<[PriceLevel]>,
    /// Non-empty levels outside the band, keyed by absolute price.
    outliers: BTreeMap<Price, PriceLevel>,
    /// Non-empty levels strictly below the band.
    outliers_below: u32,
    /// Non-empty levels at or above the band's top.
    outliers_above: u32,
    /// How many levels have ever been inserted into `outliers` (a test gauge).
    pub(crate) outlier_inserts: u64,
    /// Ticks covered by the band; `l1.len() * STEP_TICKS`.
    band_ticks: i64,
}

impl PriceBandSide {
    /// A band with `l0_bits` `l1` words. 1..=64.
    pub(crate) fn new(l0_bits: usize) -> Self {
        assert!((1..=64).contains(&l0_bits), "l0_bits must be 1..=64");
        let band_ticks = l0_bits as i64 * STEP_TICKS;
        Self {
            l0: 0,
            l1: vec![0; l0_bits].into_boxed_slice(),
            l2: vec![0; l0_bits * 64].into_boxed_slice(),
            levels: vec![PriceLevel::EMPTY; band_ticks as usize].into_boxed_slice(),
            outliers: BTreeMap::new(),
            outliers_below: 0,
            outliers_above: 0,
            outlier_inserts: 0,
            band_ticks,
        }
    }

    /// Ticks covered by the band.
    pub(crate) const fn band_ticks(&self) -> i64 {
        self.band_ticks
    }

    /// The `l1` word count, which is the shift-unit count of a recenter.
    fn l1_words(&self) -> usize {
        self.l1.len()
    }

    /// The index of `price` when it is inside the band.
    fn band_index(&self, price: Price, base: Price) -> Option<usize> {
        let offset = price.checked_sub(base)?;
        if (0..self.band_ticks).contains(&offset) {
            Some(offset as usize)
        } else {
            None
        }
    }

    /// The level at `price`, if it is non-empty.
    pub(crate) fn level(&self, price: Price, base: Price) -> Option<PriceLevel> {
        match self.band_index(price, base) {
            Some(index) => {
                let level = self.levels[index];
                (level.head != EMPTY_SLOT).then_some(level)
            }
            None => self.outliers.get(&price).copied(),
        }
    }

    /// The level at `price` for mutation, if it is non-empty.
    pub(crate) fn level_mut(&mut self, price: Price, base: Price) -> Option<&mut PriceLevel> {
        if let Some(index) = self.band_index(price, base) {
            (self.levels[index].head != EMPTY_SLOT).then(|| &mut self.levels[index])
        } else {
            self.outliers.get_mut(&price)
        }
    }

    /// The level at `price`, creating it (and its bit) when absent.
    ///
    /// A price outside the band creates an `outliers` entry, which is the one
    /// allocation the cold path is allowed.
    pub(crate) fn level_or_insert(&mut self, price: Price, base: Price) -> &mut PriceLevel {
        if let Some(index) = self.band_index(price, base) {
            if self.levels[index].head == EMPTY_SLOT {
                self.set_bit(index);
            }
            return &mut self.levels[index];
        }
        if !self.outliers.contains_key(&price) {
            if price < base {
                self.outliers_below += 1;
            } else {
                self.outliers_above += 1;
            }
            self.outlier_inserts += 1;
        }
        self.outliers.entry(price).or_insert(PriceLevel::EMPTY)
    }

    /// Forget an empty level after its last order leaves.
    pub(crate) fn release_if_empty(&mut self, price: Price, base: Price) {
        if let Some(index) = self.band_index(price, base) {
            if self.levels[index].head == EMPTY_SLOT {
                self.clear_bit(index);
                self.levels[index] = PriceLevel::EMPTY;
            }
            return;
        }
        if self
            .outliers
            .get(&price)
            .is_some_and(|level| level.head == EMPTY_SLOT)
        {
            self.outliers.remove(&price);
            if price < base {
                self.outliers_below -= 1;
            } else {
                self.outliers_above -= 1;
            }
        }
    }

    /// The best price: the lowest when `highest` is false, the highest when it
    /// is true.
    pub(crate) fn best(&self, highest: bool, base: Price) -> Option<Price> {
        if highest {
            if self.outliers_above > 0 {
                return self.outliers.last_key_value().map(|(price, _)| *price);
            }
            if self.l0 != 0 {
                let g1 = 63 - self.l0.leading_zeros() as usize;
                let w1 = self.l1[g1];
                let g2 = (g1 << 6) + (63 - w1.leading_zeros() as usize);
                let w2 = self.l2[g2];
                let index = (g2 << 6) + (63 - w2.leading_zeros() as usize);
                return Some(base + index as i64);
            }
            if self.outliers_below > 0 {
                return self.outliers.last_key_value().map(|(price, _)| *price);
            }
            None
        } else {
            if self.outliers_below > 0 {
                return self.outliers.first_key_value().map(|(price, _)| *price);
            }
            if self.l0 != 0 {
                let g1 = self.l0.trailing_zeros() as usize;
                let w1 = self.l1[g1];
                let g2 = (g1 << 6) + w1.trailing_zeros() as usize;
                let w2 = self.l2[g2];
                let index = (g2 << 6) + w2.trailing_zeros() as usize;
                return Some(base + index as i64);
            }
            if self.outliers_above > 0 {
                return self.outliers.first_key_value().map(|(price, _)| *price);
            }
            None
        }
    }

    /// The non-empty levels outside the band.
    pub(crate) const fn outliers(&self) -> &BTreeMap<Price, PriceLevel> {
        &self.outliers
    }

    /// Non-empty levels strictly below the band.
    #[cfg(test)]
    pub(crate) const fn outliers_below(&self) -> u32 {
        self.outliers_below
    }

    /// Non-empty levels at or above the band's top.
    #[cfg(test)]
    pub(crate) const fn outliers_above(&self) -> u32 {
        self.outliers_above
    }

    /// The in-band levels ascending by index, relative to `base`.
    pub(crate) fn in_band_ascending(&self, base: Price) -> InBandAscending<'_> {
        InBandAscending {
            levels: &self.levels,
            words: &self.l2,
            base,
            next_word: 0,
            current_word: 0,
            bits: 0,
        }
    }

    /// The in-band levels descending by index, relative to `base`.
    pub(crate) fn in_band_descending(&self, base: Price) -> InBandDescending<'_> {
        InBandDescending {
            levels: &self.levels,
            words: &self.l2,
            base,
            next_word: self.l2.len().checked_sub(1),
            current_word: 0,
            bits: 0,
        }
    }

    /// Insert a restored level, which must be non-empty.
    pub(crate) fn restore_level(&mut self, price: Price, level: PriceLevel, base: Price) {
        debug_assert!(level.head != EMPTY_SLOT);
        let target = self.level_or_insert(price, base);
        *target = level;
    }

    /// Move the band from `old_base` to `new_base`.
    ///
    /// The difference must be a multiple of [`STEP_TICKS`]. This is the cold
    /// path: it may move levels into `outliers` (allocating) and back.
    pub(crate) fn recenter(&mut self, old_base: Price, new_base: Price) {
        let units =
            (i128::from(new_base) - i128::from(old_base)).div_euclid(i128::from(STEP_TICKS));
        let Ok(units) = i64::try_from(units) else {
            self.rebuild(old_base, new_base);
            return;
        };
        if units == 0 {
            return;
        }
        if units.unsigned_abs() as usize >= self.l1_words() {
            self.rebuild(old_base, new_base);
        } else if units > 0 {
            let shift = units as usize;
            self.shift_up(old_base, new_base, shift);
        } else {
            let shift = (-units) as usize;
            self.shift_down(old_base, new_base, shift);
        }
    }

    /// Every in-band level moves to `outliers`, then every level the new band
    /// covers comes back. The fallback when the windows do not overlap.
    fn rebuild(&mut self, old_base: Price, new_base: Price) {
        let prices: Vec<Price> = self
            .in_band_ascending(old_base)
            .map(|(price, _)| price)
            .collect();
        for price in prices {
            let index = self.band_index(price, old_base).expect("in-band");
            let level = self.levels[index];
            self.levels[index] = PriceLevel::EMPTY;
            if !self.outliers.contains_key(&price) {
                self.outlier_inserts += 1;
            }
            self.outliers.insert(price, level);
        }
        self.l0 = 0;
        self.l1.fill(0);
        self.l2.fill(0);
        debug_assert!(
            self.levels.iter().all(|level| level.head == EMPTY_SLOT),
            "the band still holds a level after the sweep"
        );

        let top = new_base.saturating_add(self.band_ticks);
        let prices: Vec<Price> = self
            .outliers
            .range(new_base..top)
            .map(|(price, _)| *price)
            .collect();
        for price in prices {
            let level = self
                .outliers
                .remove(&price)
                .expect("the range yielded the key");
            let index = self
                .band_index(price, new_base)
                .expect("the range is the band");
            self.set_bit(index);
            self.levels[index] = level;
        }
        self.recount_outliers(new_base);
    }

    /// Move the window up by `shift` `l1` words.
    fn shift_up(&mut self, old_base: Price, new_base: Price, shift: usize) {
        let shift_l2 = shift * 64;
        let shift_ticks = shift as i64 * STEP_TICKS;
        let top = old_base.saturating_add(shift_ticks);

        let prices: Vec<Price> = self
            .in_band_ascending(old_base)
            .take_while(|(price, _)| *price < top)
            .map(|(price, _)| price)
            .collect();
        for price in prices {
            let index = self.band_index(price, old_base).expect("in-band");
            let level = self.levels[index];
            if !self.outliers.contains_key(&price) {
                self.outlier_inserts += 1;
            }
            self.outliers.insert(price, level);
        }

        let band = self.levels.len();
        let l2_words = self.l2.len();
        self.levels.copy_within(shift_ticks as usize..band, 0);
        self.levels[band - shift_ticks as usize..].fill(PriceLevel::EMPTY);
        self.l2.copy_within(shift_l2..l2_words, 0);
        self.l2[l2_words - shift_l2..].fill(0);
        let l1_words = self.l1.len();
        self.l1.copy_within(shift..l1_words, 0);
        self.l1[l1_words - shift..].fill(0);
        self.l0 >>= shift;

        let entry = new_base
            .saturating_add(self.band_ticks)
            .saturating_sub(shift_ticks);
        let top = new_base.saturating_add(self.band_ticks);
        self.admit_range(entry..top, new_base);
        self.recount_outliers(new_base);
    }

    /// Move the window down by `shift` `l1` words.
    fn shift_down(&mut self, old_base: Price, new_base: Price, shift: usize) {
        let shift_l2 = shift * 64;
        let shift_ticks = shift as i64 * STEP_TICKS;
        let entry = old_base
            .saturating_add(self.band_ticks)
            .saturating_sub(shift_ticks);

        let prices: Vec<Price> = self
            .in_band_ascending(old_base)
            .skip_while(|(price, _)| *price < entry)
            .map(|(price, _)| price)
            .collect();
        for price in prices {
            let index = self.band_index(price, old_base).expect("in-band");
            let level = self.levels[index];
            if !self.outliers.contains_key(&price) {
                self.outlier_inserts += 1;
            }
            self.outliers.insert(price, level);
        }

        let band = self.levels.len();
        let l2_words = self.l2.len();
        self.levels
            .copy_within(0..band - shift_ticks as usize, shift_ticks as usize);
        self.levels[..shift_ticks as usize].fill(PriceLevel::EMPTY);
        self.l2.copy_within(0..l2_words - shift_l2, shift_l2);
        self.l2[..shift_l2].fill(0);
        self.l1.copy_within(0..self.l1.len() - shift, shift);
        self.l1[..shift].fill(0);
        self.l0 <<= shift;

        let top = new_base.saturating_add(shift_ticks);
        self.admit_range(new_base..top, new_base);
        self.recount_outliers(new_base);
    }

    /// Move every outlier in `range` into the band.
    fn admit_range(&mut self, range: std::ops::Range<Price>, base: Price) {
        let prices: Vec<Price> = self
            .outliers
            .range(range)
            .map(|(price, _)| *price)
            .collect();
        for price in prices {
            let level = self
                .outliers
                .remove(&price)
                .expect("the range yielded the key");
            let index = self
                .band_index(price, base)
                .expect("the range is inside the band");
            debug_assert!(
                self.levels[index].head == EMPTY_SLOT,
                "admitted onto a level"
            );
            self.set_bit(index);
            self.levels[index] = level;
        }
    }

    /// The two outlier counts, recomputed from the map.
    fn recount_outliers(&mut self, base: Price) {
        let below = self.outliers.range(..base).count();
        self.outliers_below = u32::try_from(below).unwrap_or(u32::MAX);
        let above = self.outliers.len() - below;
        self.outliers_above = u32::try_from(above).unwrap_or(u32::MAX);
    }

    /// Set bit `index` in `l2` and cascade into `l1` and `l0`.
    fn set_bit(&mut self, index: usize) {
        let l2_index = index >> 6;
        let previous = self.l2[l2_index];
        self.l2[l2_index] = previous | (1_u64 << (index & 63));
        if previous == 0 {
            let l1_index = l2_index >> 6;
            let previous = self.l1[l1_index];
            self.l1[l1_index] = previous | (1_u64 << (l2_index & 63));
            if previous == 0 {
                self.l0 |= 1_u64 << l1_index;
            }
        }
    }

    /// Clear bit `index` in `l2` and cascade into `l1` and `l0`.
    fn clear_bit(&mut self, index: usize) {
        let l2_index = index >> 6;
        let next = self.l2[l2_index] & !(1_u64 << (index & 63));
        self.l2[l2_index] = next;
        if next == 0 {
            let l1_index = l2_index >> 6;
            let next = self.l1[l1_index] & !(1_u64 << (l2_index & 63));
            self.l1[l1_index] = next;
            if next == 0 {
                self.l0 &= !(1_u64 << l1_index);
            }
        }
    }

    /// Whether the bitmap is consistent with `levels`, for tests.
    pub(crate) fn bitmap_is_consistent(&self) -> bool {
        let mut l1 = vec![0_u64; self.l1.len()];
        let mut l0 = 0_u64;
        for (word, bits) in self.l2.iter().enumerate() {
            let rebuilt = (0..64)
                .filter(|bit| self.levels[(word << 6) + bit].head != EMPTY_SLOT)
                .fold(0_u64, |acc, bit| acc | (1_u64 << bit));
            if rebuilt != *bits {
                return false;
            }
            if rebuilt != 0 {
                l1[word >> 6] |= 1_u64 << (word & 63);
            }
        }
        for (word, bits) in l1.iter().enumerate() {
            if *bits != self.l1[word] {
                return false;
            }
            if *bits != 0 {
                l0 |= 1_u64 << word;
            }
        }
        l0 == self.l0
    }

    /// The level map as tests compare it, without the empty placeholders.
    #[cfg(test)]
    pub(crate) fn to_map(&self, base: Price) -> BTreeMap<Price, (Qty, u32, u32)> {
        let mut map = BTreeMap::new();
        for (price, level) in self.in_band_ascending(base) {
            map.insert(price, (level.total_qty, level.head, level.tail));
        }
        for (price, level) in &self.outliers {
            map.insert(*price, (level.total_qty, level.head, level.tail));
        }
        map
    }
}

/// Ascending in-band levels.
pub(crate) struct InBandAscending<'a> {
    levels: &'a [PriceLevel],
    words: &'a [u64],
    base: Price,
    next_word: usize,
    current_word: usize,
    bits: u64,
}

impl Iterator for InBandAscending<'_> {
    type Item = (Price, PriceLevel);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.bits != 0 {
                let bit = self.bits.trailing_zeros() as usize;
                self.bits &= self.bits - 1;
                let index = (self.current_word << 6) + bit;
                return Some((self.base + index as i64, self.levels[index]));
            }
            if self.next_word >= self.words.len() {
                return None;
            }
            self.current_word = self.next_word;
            self.next_word += 1;
            self.bits = self.words[self.current_word];
        }
    }
}

/// Descending in-band levels.
pub(crate) struct InBandDescending<'a> {
    levels: &'a [PriceLevel],
    words: &'a [u64],
    base: Price,
    next_word: Option<usize>,
    current_word: usize,
    bits: u64,
}

impl Iterator for InBandDescending<'_> {
    type Item = (Price, PriceLevel);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.bits != 0 {
                let bit = 63 - self.bits.leading_zeros() as usize;
                self.bits &= !(1_u64 << bit);
                let index = (self.current_word << 6) + bit;
                return Some((self.base + index as i64, self.levels[index]));
            }
            let word = self.next_word.take()?;
            self.current_word = word;
            self.next_word = word.checked_sub(1);
            self.bits = self.words[word];
        }
    }
}

#[cfg(test)]
mod bench;

#[cfg(test)]
mod tests {
    use deepbook_types::Qty;

    use super::*;

    /// A level with `qty` and a live head (any slot but the sentinel).
    fn level(qty: Qty) -> PriceLevel {
        PriceLevel {
            total_qty: qty,
            head: 0,
            tail: 0,
        }
    }

    fn insert(side: &mut PriceBandSide, base: Price, price: Price, qty: Qty) {
        let target = side.level_or_insert(price, base);
        target.total_qty += qty;
        if target.head == EMPTY_SLOT {
            *target = level(target.total_qty);
        }
    }

    fn remove(side: &mut PriceBandSide, base: Price, price: Price) {
        let target = side.level_mut(price, base).expect("level to remove");
        *target = PriceLevel::EMPTY;
        side.release_if_empty(price, base);
    }

    fn assert_invariants(side: &PriceBandSide, base: Price) {
        assert!(side.bitmap_is_consistent(), "bitmap and levels disagree");
        assert_eq!(
            side.outliers().len(),
            side.outliers_below() as usize + side.outliers_above() as usize,
            "outlier counters disagree with the map"
        );
        let top = base.saturating_add(side.band_ticks());
        for price in side.outliers().keys() {
            assert!(
                *price < base || *price >= top,
                "outlier {price} is inside the band [{base}, {top})"
            );
        }
    }

    #[test]
    fn bits_cascade_across_the_three_levels() {
        let mut side = PriceBandSide::new(4);
        // One level per l1 word, at the first tick of each 4,096-tick unit.
        for unit in 0..4 {
            insert(&mut side, 0, unit * STEP_TICKS, 1);
        }
        assert_ne!(side.l0, 0);
        for unit in 0..4 {
            assert_ne!(side.l1[unit], 0);
        }
        assert_invariants(&side, 0);

        for unit in 0..4 {
            remove(&mut side, 0, unit * STEP_TICKS);
        }
        assert_eq!(side.l0, 0);
        assert!(side.l1.iter().all(|word| *word == 0));
        assert!(side.l2.iter().all(|word| *word == 0));
        assert_eq!(side.best(false, 0), None);
        assert_invariants(&side, 0);
    }

    #[test]
    fn best_reads_outliers_on_both_sides() {
        let mut side = PriceBandSide::new(4);
        insert(&mut side, 0, -10, 1);
        insert(&mut side, 0, 5, 1);
        insert(&mut side, 0, 20_000, 1);
        assert_invariants(&side, 0);
        assert_eq!(side.best(false, 0), Some(-10));
        assert_eq!(side.best(true, 0), Some(20_000));

        remove(&mut side, 0, -10);
        assert_eq!(side.best(false, 0), Some(5));
        remove(&mut side, 0, 20_000);
        assert_eq!(side.best(true, 0), Some(5));
    }

    #[test]
    fn shift_up_keeps_every_absolute_price() {
        let mut side = PriceBandSide::new(4);
        let prices = [0, 4_095, 4_096, 8_191, 10_009, 16_383];
        for price in prices {
            insert(&mut side, 0, price, 1);
        }
        let before = side.to_map(0);
        side.recenter(0, 4_096);
        assert_eq!(before, side.to_map(4_096));
        assert_invariants(&side, 4_096);
        // 0 and 4,095 fell below the new band; 16,383 is still inside it.
        assert_eq!(side.outliers_below(), 2);
        assert_eq!(side.outliers_above(), 0);
        assert_eq!(side.best(false, 4_096), Some(0));
        assert_eq!(side.best(true, 4_096), Some(16_383));
    }

    #[test]
    fn shift_down_keeps_every_absolute_price() {
        let mut side = PriceBandSide::new(4);
        for price in [0, 4_095, 8_192, 12_287, 16_383] {
            insert(&mut side, 8_192, price, 1);
        }
        let before = side.to_map(8_192);
        side.recenter(8_192, 4_096);
        assert_eq!(before, side.to_map(4_096));
        assert_invariants(&side, 4_096);
        // The new band covers [4,096, 20,480): 0 and 4,095 are below it and
        // 16,383 is back inside, so nothing is above.
        assert_eq!(side.outliers_above(), 0);
        assert_eq!(side.outliers_below(), 2);
        assert_eq!(side.best(false, 4_096), Some(0));
        assert_eq!(side.best(true, 4_096), Some(16_383));
    }

    #[test]
    fn a_shift_past_the_band_rebuilds() {
        let mut side = PriceBandSide::new(4);
        for price in [0, 4_096, 8_192, 12_287] {
            insert(&mut side, 0, price, 1);
        }
        let before = side.to_map(0);
        // Four l1 words: a unit shift of four no longer overlaps the window.
        side.recenter(0, 16_384);
        assert_eq!(before, side.to_map(16_384));
        assert_invariants(&side, 16_384);
        assert_eq!(side.best(false, 16_384), Some(0));
        assert_eq!(side.best(true, 16_384), Some(12_287));
        assert_eq!(side.outliers_above(), 0);
        assert_eq!(side.outliers_below(), 4);
    }

    #[test]
    fn every_recenter_preserves_the_levels() {
        let mut side = PriceBandSide::new(4);
        let prices = [
            -10_000, -4_097, -1, 0, 63, 64, 4_095, 4_096, 8_192, 16_383, 16_384, 30_000,
        ];
        for price in prices {
            insert(
                &mut side,
                0,
                price,
                price.unsigned_abs().min(1_000) as i64 + 1,
            );
        }
        let before = side.to_map(0);
        let mut base = 0;
        for target in [4_096, 8_192, 65_536, -4_096, -65_536, 0] {
            side.recenter(base, target);
            base = target;
            assert_eq!(before, side.to_map(base), "levels changed moving to {base}");
            assert_invariants(&side, base);
        }
        // Seven prices stay inside the production-sized band, five are out of it.
        assert_eq!(side.outliers().len(), 5);
    }

    #[test]
    fn release_clears_the_bit_and_the_counter() {
        let mut side = PriceBandSide::new(4);
        insert(&mut side, 0, 1_000, 5);
        insert(&mut side, 0, -1, 5);
        assert_eq!(side.best(false, 0), Some(-1));
        remove(&mut side, 0, -1);
        assert_eq!(side.outliers_below(), 0);
        assert_eq!(side.best(false, 0), Some(1_000));
        remove(&mut side, 0, 1_000);
        assert_eq!(side.best(false, 0), None);
        assert_invariants(&side, 0);
    }
}
