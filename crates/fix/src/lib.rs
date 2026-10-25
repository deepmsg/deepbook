//! FIX 4.4 session layer, codec and outbound journal.
//!
//! M1a is the pure part: the wire codec, the typed message subset, the
//! acceptor session state machine and the journal it resends from. The epoll
//! event loop and the gateway's `CommandSink` are M1b/M1c; nothing here opens
//! a socket or reads the OS clock unless the caller asks it to.
//!
//! Scope: Logon/Logout/Heartbeat/TestRequest/ResendRequest/SequenceReset/
//! Reject, NewOrderSingle, OrderCancelRequest, OrderCancelReplaceRequest,
//! ExecutionReport, OrderCancelReject, OrderStatusRequest. Application
//! messages are never gap-filled; a resend outside the journal window ends
//! the session.
//!
//! ```no_run
//! use deepbook_fix::clock::SystemClock;
//! use deepbook_fix::journal::{FileJournal, FileSequenceStore};
//! use deepbook_fix::session::{Session, SessionConfig};
//! use deepbook_fix::Clock;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let session = Session::new(
//!     SessionConfig::new("DEEPBOOK", "CLIENT", 30),
//!     FileJournal::open("sessions/client/journal.bin", 4096)?,
//!     FileSequenceStore::new("sessions/client/sequences"),
//!     SystemClock.now_millis(),
//! )?;
//! let _ = session;
//! # Ok(())
//! # }
//! ```
#![forbid(unsafe_code)]

pub mod clock;
pub mod codec;
pub mod journal;
pub mod message;
pub mod session;

pub use clock::{Clock, ManualClock, SystemClock, format_utc};
pub use codec::{CodecError, MessageBuilder, MsgType, ParsedMessage, parse};
pub use journal::{
    FileJournal, FileSequenceStore, Journal, JournalEntry, SequenceStore, Sequences,
};
pub use message::{
    ApplicationMessage, CxlRejResponseTo, ExecType, ExecutionReport, Message, NewOrderSingle,
    OrdStatus, OrdType, OrderCancelReject, OrderCancelReplaceRequest, OrderCancelRequest,
    OrderStatusRequest, SessionMessage, TimeInForce, decode, decode_application, decode_session,
    encode_application, format_price, parse_price,
};
pub use session::{Action, Session, SessionConfig, SessionError, SessionState};
