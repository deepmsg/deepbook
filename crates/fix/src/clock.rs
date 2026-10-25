//! FIX `UTCTimestamp` formatting and the clocks the session reads.
//!
//! The session never calls the OS clock itself: every entry point takes
//! `now_millis`, so a test can hand it a fixed time. The gateway uses
//! [`SystemClock`]; tests use [`ManualClock`].

use std::time::{SystemTime, UNIX_EPOCH};

/// Milliseconds since the Unix epoch, UTC.
pub type Millis = i64;

/// A source of wall-clock milliseconds.
pub trait Clock {
    /// Now, in milliseconds since the Unix epoch.
    fn now_millis(&self) -> Millis;
}

/// The OS clock.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_millis(&self) -> Millis {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(duration) => i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
            Err(_) => 0,
        }
    }
}

/// A clock a test moves by hand.
#[derive(Clone, Copy, Debug, Default)]
pub struct ManualClock {
    millis: Millis,
}

impl ManualClock {
    /// A clock stopped at `millis`.
    #[must_use]
    pub const fn new(millis: Millis) -> Self {
        Self { millis }
    }

    /// Move the clock to `millis`.
    pub const fn set(&mut self, millis: Millis) {
        self.millis = millis;
    }

    /// Move the clock forward by `delta`.
    pub const fn advance(&mut self, delta: Millis) {
        self.millis += delta;
    }
}

impl Clock for ManualClock {
    fn now_millis(&self) -> Millis {
        self.millis
    }
}

/// `YYYYMMDD-HH:MM:SS.sss`, the FIX 4.4 `UTCTimestamp` (`SendingTime`, 52).
#[must_use]
pub fn format_utc(millis: Millis) -> String {
    let seconds = millis.div_euclid(1_000);
    let millis_part = millis.rem_euclid(1_000);
    let days = seconds.div_euclid(86_400);
    let second_of_day = seconds.rem_euclid(86_400);
    let hour = second_of_day / 3_600;
    let minute = (second_of_day % 3_600) / 60;
    let second = second_of_day % 60;
    let (year, month, day) = civil_from_days(days);

    format!("{year:04}{month:02}{day:02}-{hour:02}:{minute:02}:{second:02}.{millis_part:03}")
}

/// Days since 1970-01-01 to a proleptic Gregorian `(year, month, day)`.
///
/// Howard Hinnant's `civil_from_days`, which is exact for every `i64` day
/// count this program can reach.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    let year = year + i64::from(month <= 2);

    (
        year,
        u32::try_from(month).expect("1..=12"),
        u32::try_from(day).expect("1..=31"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_epoch_is_the_fix_epoch() {
        assert_eq!("19700101-00:00:00.000", format_utc(0));
    }

    #[test]
    fn a_known_instant_formats_as_utc() {
        // 2026-10-25 12:34:56.789 UTC.
        assert_eq!("20261025-12:34:56.789", format_utc(1_792_931_696_789));
    }

    #[test]
    fn millis_below_a_second_keep_their_leading_zeroes() {
        assert_eq!("19700101-00:00:00.007", format_utc(7));
    }

    #[test]
    fn a_leap_day_is_a_day() {
        // 2024-02-29 00:00:00 UTC.
        assert_eq!("20240229-00:00:00.000", format_utc(1_709_164_800_000));
    }

    #[test]
    fn the_manual_clock_moves_when_told() {
        let mut clock = ManualClock::new(1_000);
        assert_eq!(1_000, clock.now_millis());
        clock.advance(500);
        assert_eq!(1_500, clock.now_millis());
        clock.set(42);
        assert_eq!(42, clock.now_millis());
    }
}
