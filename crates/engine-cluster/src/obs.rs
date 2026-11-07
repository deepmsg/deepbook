//! The engine's own names in the application log.
//!
//! deepmsg's `Component`/`Event` are the infrastructure's vocabulary; the
//! engine's are its own (`deepmsg-log`'s [`Name`]). The component is what the
//! writer puts in the file name (`engine-<node>.json`), so it is `engine` here
//! and nowhere else in the deployment.

use deepmsg_log::{Level, Name, event_named};

use crate::metrics::EngineMetrics;

/// The component names `DEEPMSG_LOG_LEVEL=engine=debug` may use.
pub static COMPONENTS: [&str; 1] = ["engine"];

/// The engine process (one per member).
#[derive(Clone, Copy, Debug)]
pub struct Engine;

impl Name for Engine {
    fn as_str(&self) -> &'static str {
        "engine"
    }
}

/// What the engine writes about itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EngineEvent {
    Start,
    Stop,
    Recover,
    Snapshot,
    Role,
    Stats,
}

impl EngineEvent {
    /// Every name; the vocabulary test keeps them unique and prefixed.
    pub const ALL: [Self; 6] = [
        Self::Start,
        Self::Stop,
        Self::Recover,
        Self::Snapshot,
        Self::Role,
        Self::Stats,
    ];
}

impl Name for EngineEvent {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Start => "engine.start",
            Self::Stop => "engine.stop",
            Self::Recover => "engine.recover",
            Self::Snapshot => "engine.snapshot",
            Self::Role => "engine.role",
            Self::Stats => "engine.stats",
        }
    }
}

/// The container is up and the service has been asked to start.
pub fn start(symbol: u16) {
    event_named(Level::Info, Engine, EngineEvent::Start)
        .message("engine started")
        .field_i64("symbol", i64::from(symbol))
        .emit();
}

/// The container is going down (the cluster asked it to terminate).
pub fn stop() {
    event_named(Level::Info, Engine, EngineEvent::Stop)
        .message("engine stopped")
        .emit();
}

/// How the engine came back: from a snapshot, or from an empty book and the
/// log.
pub fn recover(from_snapshot: bool, log_pos: i64, bytes: usize) {
    event_named(Level::Info, Engine, EngineEvent::Recover)
        .message("engine recovery")
        .field_bool("from_snapshot", from_snapshot)
        .field_i64("log_pos", log_pos)
        .field_i64("bytes", i64::try_from(bytes).unwrap_or(i64::MAX))
        .emit();
}

/// A snapshot was taken. Three members that applied the same log must report
/// the same `hash`; `state_hash` is the digest with `last_log_pos` zeroed, so
/// it is comparable across a leadership change too.
pub fn snapshot(len: usize, hash: u64, state_hash: Option<u64>) {
    let mut event = event_named(Level::Info, Engine, EngineEvent::Snapshot)
        .message("snapshot taken")
        .field_i64("len", i64::try_from(len).unwrap_or(i64::MAX))
        .field_i64("hash", hash as i64);
    if let Some(state_hash) = state_hash {
        event = event.field_i64("state_hash", state_hash as i64);
    }
    event.emit();
}

/// The cluster told the service its role changed.
pub fn role(role: &str, leader: bool) {
    event_named(Level::Info, Engine, EngineEvent::Role)
        .message("role changed")
        .field_str("role", role)
        .field_bool("leader", leader)
        .emit();
}

/// The cumulative numbers, every five seconds. Scalars only: a per-order event
/// would put the whole order flow into the log store.
pub fn stats(metrics: &EngineMetrics) {
    event_named(Level::Info, Engine, EngineEvent::Stats)
        .message("engine stats")
        .field_i64("applied_total", saturating(metrics.applied()))
        .field_i64("rejected_total", saturating(metrics.rejected()))
        .field_i64("filled_total", saturating(metrics.filled()))
        .field_i64("snapshot_count", saturating(metrics.snapshots()))
        .field_i64("commit_position", metrics.commit_position())
        .field_bool("leader", metrics.is_leader())
        .emit();
}

fn saturating(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::EngineEvent;
    use deepmsg_log::Name;

    #[test]
    fn the_vocabulary_has_no_duplicates() {
        let mut names: Vec<&str> = EngineEvent::ALL.iter().map(Name::as_str).collect();
        names.sort_unstable();
        let before = names.len();
        names.dedup();
        assert_eq!(before, names.len(), "an event name is repeated");
    }

    #[test]
    fn every_name_is_in_the_engine_namespace() {
        for event in EngineEvent::ALL {
            let name = event.as_str();
            assert!(
                name.starts_with("engine."),
                "{name} is not in the engine namespace"
            );
        }
    }

    #[test]
    fn no_field_is_a_reserved_name() {
        // The encoder drops a reserved name silently; `target` is one of them.
        for name in [
            "symbol",
            "from_snapshot",
            "log_pos",
            "bytes",
            "len",
            "hash",
            "state_hash",
            "role",
            "leader",
            "applied_total",
            "rejected_total",
            "filled_total",
            "snapshot_count",
            "commit_position",
        ] {
            assert!(
                !deepmsg_log::record::is_reserved(name),
                "{name} is reserved"
            );
        }
    }
}
