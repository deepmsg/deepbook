//! The load client's names in the application log.
//!
//! The component is `tool`, not `bench`: deepbook's benchmark client is a
//! harness process, and the deployment's component list has one tool entry.
//! It runs on the client host with node id `client`, so its file is
//! `tool-client.json`.

use deepmsg_log::{Level, Name, event_named};

use crate::report::{Report, Summary};

/// The component names `DEEPMSG_LOG_LEVEL=tool=debug` may use.
pub static COMPONENTS: [&str; 1] = ["tool"];

/// The load client process.
#[derive(Clone, Copy, Debug)]
pub struct Tool;

impl Name for Tool {
    fn as_str(&self) -> &'static str {
        "tool"
    }
}

/// What the load client writes about itself: one pair per run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ToolEvent {
    BenchmarkStart,
    BenchmarkEnd,
}

impl ToolEvent {
    /// Every name; the vocabulary test keeps them unique and prefixed.
    pub const ALL: [Self; 2] = [Self::BenchmarkStart, Self::BenchmarkEnd];
}

impl Name for ToolEvent {
    fn as_str(&self) -> &'static str {
        match self {
            Self::BenchmarkStart => "benchmark.start",
            Self::BenchmarkEnd => "benchmark.end",
        }
    }
}

/// The run's parameters, before the first order.
pub fn benchmark_start(rate: u64, samples: u64, warmup: u64) {
    event_named(Level::Info, Tool, ToolEvent::BenchmarkStart)
        .message("benchmark started")
        .field_i64("rate", saturating(rate))
        .field_i64("samples", saturating(samples))
        .field_i64("warmup", saturating(warmup))
        .emit();
}

/// The run's outcome. Microseconds, not nanoseconds: the schema has no
/// floating point and a microsecond is the unit a report is read in.
pub fn benchmark_end(report: &Report, summary: &Summary) {
    event_named(Level::Info, Tool, ToolEvent::BenchmarkEnd)
        .message("benchmark ended")
        .field_i64("sent", saturating(report.sent))
        .field_i64("completed", saturating(report.completed))
        .field_i64("rejected", saturating(report.rejected))
        .field_i64("unanswered", saturating(report.unanswered))
        .field_i64(
            "elapsed_ms",
            saturating(u64::try_from(report.elapsed.as_millis()).unwrap_or(u64::MAX)),
        )
        .field_i64("samples", saturating(summary.count))
        .field_i64("p50_us", saturating(summary.p50_ns / 1_000))
        .field_i64("p99_us", saturating(summary.p99_ns / 1_000))
        .emit();
}

fn saturating(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::ToolEvent;
    use deepmsg_log::Name;

    #[test]
    fn the_vocabulary_has_no_duplicates() {
        let mut names: Vec<&str> = ToolEvent::ALL.iter().map(Name::as_str).collect();
        names.sort_unstable();
        let before = names.len();
        names.dedup();
        assert_eq!(before, names.len(), "an event name is repeated");
    }

    #[test]
    fn every_name_is_in_the_benchmark_namespace() {
        for event in ToolEvent::ALL {
            let name = event.as_str();
            assert!(
                name.starts_with("benchmark."),
                "{name} is not in the benchmark namespace"
            );
        }
    }

    #[test]
    fn no_field_is_a_reserved_name() {
        // The encoder drops a reserved name silently; `target` is one of them.
        for name in [
            "rate",
            "samples",
            "warmup",
            "sent",
            "completed",
            "rejected",
            "unanswered",
            "elapsed_ms",
            "p50_us",
            "p99_us",
        ] {
            assert!(
                !deepmsg_log::record::is_reserved(name),
                "{name} is reserved"
            );
        }
    }
}
