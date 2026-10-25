//! The gateway-to-engine seam.
//!
//! The FIX layer never calls the engine directly: the session emits
//! `Action::Deliver(ApplicationMessage)` and the gateway hands it to
//! [`CommandSink::deliver`]. The engine answers through
//! [`EventSource::poll_event`], and the gateway routes each event back to the
//! session named by its [`SessionKey`]. That is the same shape M4's cluster
//! egress has: a command goes out, a report comes back later.

use deepbook_fix::ApplicationMessage;

/// Where an accepted FIX command goes.
pub trait CommandSink {
    /// Take one application message for `session`.
    ///
    /// The sink owns the decision of what an accepted order becomes. It
    /// publishes reports through [`EventSource`], not by returning them.
    fn deliver(&mut self, session: &SessionKey, message: ApplicationMessage);
}

/// One report on its way back to a FIX session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionEvent {
    /// The session the report belongs to.
    pub session: SessionKey,
    /// The FIX message to send.
    pub message: ApplicationMessage,
}

/// Where the engine's reports come from.
pub trait EventSource {
    /// The next report, or `None`.
    fn poll_event(&mut self) -> Option<SessionEvent>;
}

/// The engine seam: commands in, reports out.
///
/// Blanket-implemented for anything that is both halves, so a test engine is
/// one type and a cluster adapter can be one type too.
pub trait Engine: CommandSink + EventSource {}

impl<T: CommandSink + EventSource> Engine for T {}

/// The identity of one FIX session: our `SenderCompID` and the peer's
/// `TargetCompID`.
///
/// This is the key the journal directory and the session registry use. It is
/// independent of the TCP connection so a reconnect resumes the same session.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SessionKey {
    /// Our `49`.
    pub sender_comp_id: String,
    /// The peer's `56`.
    pub target_comp_id: String,
}

impl SessionKey {
    /// A key for one pair.
    #[must_use]
    pub fn new(sender_comp_id: impl Into<String>, target_comp_id: impl Into<String>) -> Self {
        Self {
            sender_comp_id: sender_comp_id.into(),
            target_comp_id: target_comp_id.into(),
        }
    }

    /// A filesystem-safe directory name for the pair.
    #[must_use]
    pub fn directory_name(&self) -> String {
        format!(
            "{}-{}",
            sanitise(&self.sender_comp_id),
            sanitise(&self.target_comp_id)
        )
    }
}

fn sanitise(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' || character == '_' {
                character
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_is_a_safe_directory_name() {
        let key = SessionKey::new("DEEPBOOK", "CLIENT/1");
        assert_eq!("DEEPBOOK-CLIENT_1", key.directory_name());
    }
}
