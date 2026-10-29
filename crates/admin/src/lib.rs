//! HTTP/WS control plane, metrics and the UI host (M5).
//!
//! The admin is off the matching hot path. It subscribes to the engine's
//! market-data stream (the same deepmsg driver as the cluster), mirrors the
//! book and the trade tape, and serves:
//!
//! - `/` — the static dashboard (`crates/ui`);
//! - `/ws` — the conflated view as JSON, pushed at the browser's cadence;
//! - `/api/book`, `/api/trades`, `/api/latency` — the same data on demand;
//! - `POST /api/kill`, `/api/control` — the kill switch ([`control`]);
//! - `/metrics` — Prometheus text, including the cluster's commit position and
//!   the last benchmark's RTT gauges.
//!
//! The feed owns the deepmsg client on the main thread; HTTP and WebSocket
//! connections are threads over one `Arc<Mutex<View>>`. The control path has
//! its own cluster session on its own thread.
#![forbid(unsafe_code)]

pub mod control;
pub mod feed;
pub mod http;
pub mod metrics;
pub mod state;
pub mod web;
pub mod ws;

pub use control::{Control, ControlConfig, ControlStatus, KillOutcome, KillRequest};
pub use feed::Feed;
pub use state::{SharedView, View, shared_view};
