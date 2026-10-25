//! The gateway-to-engine seam.
//!
//! M1c implements the sink with an in-process engine; M4 swaps in the cluster
//! client. The FIX layer never calls the engine directly: the session emits
//! `Action::Deliver(ApplicationMessage)` and the gateway hands it here.

use deepbook_fix::ApplicationMessage;

/// Where an accepted FIX command goes.
pub trait CommandSink {
    /// Take one application message.
    ///
    /// The sink owns the decision of what an accepted order becomes: M1c will
    /// turn it into an `ExecutionReport` (or a reject) and hand that back to
    /// the session.
    fn deliver(&mut self, message: ApplicationMessage);
}

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
