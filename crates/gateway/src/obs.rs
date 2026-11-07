//! The gateway's own names in the application log.
//!
//! `deepmsg-log`'s `Component` and `Event` are the infrastructure's vocabulary;
//! an application implements [`Name`] for its own (see `crates/log/README.md`
//! in deepmsg). One component per process: `gateway` here, `engine` in
//! `deepbook-engine-cluster`, `tool` for the load client. The component is the
//! first half of the file name, so two processes sharing one would interleave
//! their lines.
//!
//! With no logger installed every call is a no-op (the facade is off unless
//! `DEEPMSG_LOG_DIR` says otherwise), so library code emits unconditionally
//! and the binary's `Guard` decides whether anything is written.

use std::net::SocketAddr;

use deepmsg_log::{Level, Name, event_named};

/// The component names `DEEPMSG_LOG_LEVEL=gateway=debug` may use.
pub static COMPONENTS: [&str; 1] = ["gateway"];

/// The gateway process.
#[derive(Clone, Copy, Debug)]
pub struct Gateway;

impl Name for Gateway {
    fn as_str(&self) -> &'static str {
        "gateway"
    }
}

/// What the gateway writes about itself: the FIX session's life and the
/// cluster session's, and the commands it refuses before the cluster sees them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GatewayEvent {
    SessionOpen,
    SessionClose,
    ClusterLost,
    ClusterRecovered,
    CommandRejected,
}

impl GatewayEvent {
    /// Every name; the vocabulary test keeps them unique and prefixed.
    pub const ALL: [Self; 5] = [
        Self::SessionOpen,
        Self::SessionClose,
        Self::ClusterLost,
        Self::ClusterRecovered,
        Self::CommandRejected,
    ];
}

impl Name for GatewayEvent {
    fn as_str(&self) -> &'static str {
        match self {
            Self::SessionOpen => "gateway.session.open",
            Self::SessionClose => "gateway.session.close",
            Self::ClusterLost => "gateway.cluster.lost",
            Self::ClusterRecovered => "gateway.cluster.recovered",
            Self::CommandRejected => "gateway.command.rejected",
        }
    }
}

/// A peer logged on. Cold path: the peer is formatted here, not on the FIX
/// command path.
pub fn session_open(peer: SocketAddr, sender: &str, target: &str) {
    event_named(Level::Info, Gateway, GatewayEvent::SessionOpen)
        .message("FIX session logged on")
        .field_str("peer", &peer.to_string())
        .field_str("sender", sender)
        .field_str("target_comp", target)
        .emit();
}

/// A peer's session ended, however it ended.
pub fn session_close(peer: SocketAddr, sender: &str, target: &str) {
    event_named(Level::Info, Gateway, GatewayEvent::SessionClose)
        .message("FIX session closed")
        .field_str("peer", &peer.to_string())
        .field_str("sender", sender)
        .field_str("target_comp", target)
        .emit();
}

/// The cluster session the gateway was using is gone.
pub fn cluster_lost(cluster_session_id: i64, sessions_opened: u16) {
    event_named(Level::Warn, Gateway, GatewayEvent::ClusterLost)
        .message("cluster session closed")
        .field_i64("cluster_session_id", cluster_session_id)
        .field_i64("sessions_opened", i64::from(sessions_opened))
        .emit();
}

/// A new cluster session is up.
pub fn cluster_recovered(cluster_session_id: i64, sessions_opened: u16) {
    event_named(Level::Info, Gateway, GatewayEvent::ClusterRecovered)
        .message("cluster session reconnected")
        .field_i64("cluster_session_id", cluster_session_id)
        .field_i64("sessions_opened", i64::from(sessions_opened))
        .emit();
}

/// A command the gateway refused before it reached the cluster.
pub fn command_rejected(cl_ord_id: &str, reason: &str) {
    event_named(Level::Warn, Gateway, GatewayEvent::CommandRejected)
        .message("command rejected at the gateway")
        .field_str("cl_ord_id", cl_ord_id)
        .field_str("reason", reason)
        .emit();
}

#[cfg(test)]
mod tests {
    use super::GatewayEvent;
    use deepmsg_log::Name;

    #[test]
    fn the_vocabulary_has_no_duplicates() {
        let mut names: Vec<&str> = GatewayEvent::ALL.iter().map(Name::as_str).collect();
        names.sort_unstable();
        let before = names.len();
        names.dedup();
        assert_eq!(before, names.len(), "an event name is repeated");
    }

    #[test]
    fn every_name_is_in_the_gateway_namespace() {
        for event in GatewayEvent::ALL {
            let name = event.as_str();
            assert!(
                name.starts_with("gateway."),
                "{name} is not in the gateway namespace"
            );
        }
    }

    #[test]
    fn no_field_is_a_reserved_name() {
        // The encoder drops a reserved name silently; `target` was the first
        // draft of the peer's CompID and never reached a line.
        for name in [
            "peer",
            "sender",
            "target_comp",
            "cluster_session_id",
            "sessions_opened",
            "cl_ord_id",
            "reason",
        ] {
            assert!(
                !deepmsg_log::record::is_reserved(name),
                "{name} is reserved"
            );
        }
    }
}
