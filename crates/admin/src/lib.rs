//! HTTP/WS control plane, kill switch, queries and the UI host (M5).
//!
//! The admin process is off the hot path. It serves the static dashboard,
//! exposes queries, and publishes market data to the browser with conflation
//! applied only at the WebSocket layer.
#![forbid(unsafe_code)]
