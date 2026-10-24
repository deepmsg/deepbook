//! FIX 4.4 session layer, codec and outbound journal (M1).
//!
//! Scope: Logon/Logout/Heartbeat/TestRequest/ResendRequest/SequenceReset/
//! Reject, NewOrderSingle, OrderCancelRequest, OrderCancelReplaceRequest,
//! ExecutionReport, OrderCancelReject, OrderStatusRequest. Application
//! messages are never gap-filled; a resend outside the journal window ends
//! the session.
#![forbid(unsafe_code)]
