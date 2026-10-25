//! The latency samples and their summary.
//!
//! Percentiles use the nearest-rank definition on the sorted samples. Every
//! value is in nanoseconds; the table prints microseconds because a FIX RTT
//! on loopback is measured there, not in seconds.

use std::time::Duration;

/// One run's samples and outcome.
#[derive(Clone, Debug)]
pub struct Report {
    /// Orders handed to the socket (warmup included).
    pub sent: u64,
    /// First reports matched to an order (warmup included).
    pub completed: u64,
    /// Reports whose `ExecType` was `Rejected`.
    pub rejected: u64,
    /// Orders that never produced a report before the deadline.
    pub unanswered: u64,
    /// Wall time from the first send to the last sample or the deadline.
    pub elapsed: Duration,
    /// RTT samples in nanoseconds, warmup excluded.
    pub samples_ns: Vec<u64>,
}

/// The numbers a table or a JSON line carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Summary {
    /// The sample count.
    pub count: u64,
    /// Fastest sample.
    pub min_ns: u64,
    /// Arithmetic mean, rounded down.
    pub mean_ns: u64,
    /// 50th percentile.
    pub p50_ns: u64,
    /// 90th percentile.
    pub p90_ns: u64,
    /// 99th percentile.
    pub p99_ns: u64,
    /// 99.9th percentile.
    pub p999_ns: u64,
    /// Slowest sample.
    pub max_ns: u64,
}

impl Report {
    /// Summarise the (warmup-excluded) samples.
    #[must_use]
    pub fn summary(&self) -> Summary {
        if self.samples_ns.is_empty() {
            return Summary {
                count: 0,
                min_ns: 0,
                mean_ns: 0,
                p50_ns: 0,
                p90_ns: 0,
                p99_ns: 0,
                p999_ns: 0,
                max_ns: 0,
            };
        }
        let mut sorted = self.samples_ns.clone();
        sorted.sort_unstable();
        let total: u128 = sorted.iter().map(|value| u128::from(*value)).sum();
        #[allow(clippy::cast_possible_truncation)] // a mean of u64 nanoseconds
        let mean = (total / sorted.len() as u128) as u64;
        Summary {
            count: sorted.len() as u64,
            min_ns: sorted[0],
            mean_ns: mean,
            p50_ns: percentile(&sorted, 0.50),
            p90_ns: percentile(&sorted, 0.90),
            p99_ns: percentile(&sorted, 0.99),
            p999_ns: percentile(&sorted, 0.999),
            max_ns: sorted[sorted.len() - 1],
        }
    }

    /// Orders per second over the measured wall time.
    #[must_use]
    pub fn rate_per_second(&self) -> f64 {
        let seconds = self.elapsed.as_secs_f64();
        if seconds <= 0.0 {
            return 0.0;
        }
        #[allow(clippy::cast_precision_loss)] // a counter, not an exact integer
        let sent = self.sent as f64;
        sent / seconds
    }

    /// A human table row set (the bench binary prints this unless `--json`).
    #[must_use]
    pub fn to_table(&self) -> String {
        let summary = self.summary();
        let mut out = String::new();
        out.push_str(&format!(
            "sent={} completed={} rejected={} unanswered={} elapsed={:.3}s rate={:.0}/s\n",
            self.sent,
            self.completed,
            self.rejected,
            self.unanswered,
            self.elapsed.as_secs_f64(),
            self.rate_per_second()
        ));
        if summary.count == 0 {
            out.push_str("no RTT samples\n");
            return out;
        }
        out.push_str(&format!(
            "rtt n={} min={} p50={} p90={} p99={} p99.9={} max={} mean={}\n",
            summary.count,
            micros(summary.min_ns),
            micros(summary.p50_ns),
            micros(summary.p90_ns),
            micros(summary.p99_ns),
            micros(summary.p999_ns),
            micros(summary.max_ns),
            micros(summary.mean_ns),
        ));
        out
    }

    /// One JSON object, so `deploy/bench.sh` can build a comparison table
    /// without parsing the table text.
    #[must_use]
    pub fn to_json(&self) -> String {
        let summary = self.summary();
        format!(
            "{{\"sent\":{},\"completed\":{},\"rejected\":{},\"unanswered\":{},\
             \"elapsed_ms\":{},\"rate_per_s\":{:.3},\
             \"rtt_ns\":{{\"count\":{},\"min\":{},\"mean\":{},\"p50\":{},\
             \"p90\":{},\"p99\":{},\"p999\":{},\"max\":{}}}}}",
            self.sent,
            self.completed,
            self.rejected,
            self.unanswered,
            self.elapsed.as_millis(),
            self.rate_per_second(),
            summary.count,
            summary.min_ns,
            summary.mean_ns,
            summary.p50_ns,
            summary.p90_ns,
            summary.p99_ns,
            summary.p999_ns,
            summary.max_ns,
        )
    }
}

/// Nearest-rank percentile on a sorted, non-empty slice.
fn percentile(sorted: &[u64], fraction: f64) -> u64 {
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let rank = ((sorted.len() - 1) as f64 * fraction).round() as usize;
    sorted[rank.min(sorted.len() - 1)]
}

fn micros(nanos: u64) -> String {
    format!("{:.1}us", nanos as f64 / 1_000.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report(samples: &[u64]) -> Report {
        Report {
            sent: samples.len() as u64,
            completed: samples.len() as u64,
            rejected: 0,
            unanswered: 0,
            elapsed: Duration::from_secs(1),
            samples_ns: samples.to_vec(),
        }
    }

    #[test]
    fn percentiles_are_nearest_rank() {
        let summary = report(&[100, 200, 300, 400, 500]).summary();
        assert_eq!(5, summary.count);
        assert_eq!(100, summary.min_ns);
        assert_eq!(500, summary.max_ns);
        assert_eq!(300, summary.mean_ns);
        assert_eq!(300, summary.p50_ns);
        assert_eq!(500, summary.p90_ns);
        assert_eq!(500, summary.p99_ns);
    }

    #[test]
    fn an_empty_report_does_not_divide_by_zero() {
        let summary = report(&[]).summary();
        assert_eq!(0, summary.count);
        assert_eq!(0.0, report(&[]).rate_per_second());
    }

    #[test]
    fn json_names_the_same_numbers_as_the_summary() {
        let json = report(&[1_000, 2_000, 3_000]).to_json();
        assert!(json.contains("\"p50\":2000"));
        assert!(json.contains("\"unanswered\":0"));
        assert!(json.contains("\"count\":3"));
    }
}
