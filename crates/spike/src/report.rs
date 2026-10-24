//! Percentiles and the machine line that has to travel with them.
//!
//! A latency number without its machine is not evidence. The spike prints both
//! on one run; the ADR records them together.

use std::time::Duration;

/// The percentile block for one measurement.
#[derive(Clone, Copy, Debug)]
pub struct Stats {
    /// Median.
    pub p50: Duration,
    /// 99th percentile.
    pub p99: Duration,
    /// 99.9th percentile.
    pub p999: Duration,
    /// Worst sample.
    pub max: Duration,
    /// Arithmetic mean.
    pub mean: Duration,
}

/// Sort the samples and answer the percentile block.
///
/// # Panics
///
/// An empty sample set: a measurement with no samples is a bug, not a zero.
#[must_use]
pub fn stats(mut samples: Vec<Duration>) -> Stats {
    assert!(!samples.is_empty(), "a measurement needs a sample");
    samples.sort_unstable();
    let mean_nanos = samples.iter().map(Duration::as_nanos).sum::<u128>() / samples.len() as u128;

    Stats {
        p50: percentile(&samples, 0.50),
        p99: percentile(&samples, 0.99),
        p999: percentile(&samples, 0.999),
        max: samples[samples.len() - 1],
        mean: Duration::from_nanos(u64::try_from(mean_nanos).unwrap_or(u64::MAX)),
    }
}

fn percentile(sorted: &[Duration], quantile: f64) -> Duration {
    let index = ((sorted.len() as f64 * quantile) as usize).min(sorted.len() - 1);
    sorted[index]
}

/// Kernel, CPU and architecture, as one line.
#[must_use]
pub fn machine() -> String {
    let kernel = std::fs::read_to_string("/proc/sys/kernel/osrelease")
        .map_or_else(|_| "unknown".to_owned(), |value| value.trim().to_owned());
    let cpu = std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|text| {
            text.lines()
                .find_map(|line| line.strip_prefix("model name").map(str::trim))
                .map(|line| line.trim_start_matches(':').trim().to_owned())
        })
        .unwrap_or_else(|| "unknown".to_owned());
    format!("{cpu}, kernel {kernel}, {}", std::env::consts::ARCH)
}

/// Format a duration in microseconds with one decimal.
#[must_use]
pub fn micros(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1e6
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_land_on_the_sorted_sample() {
        let samples = (1..=100).map(Duration::from_micros).collect();
        let stats = stats(samples);
        // The index is floor(n * q): 100 samples put p50 at 51 us and p99 at
        // the last sample.
        assert_eq!(Duration::from_micros(51), stats.p50);
        assert_eq!(Duration::from_micros(100), stats.p99);
        assert_eq!(Duration::from_micros(100), stats.max);
    }
}
