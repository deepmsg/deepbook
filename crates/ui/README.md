# UI

Static dashboard (order book depth, recent trades, latency, kill switch) served
by `admin`. Lightweight Charts. This directory is **not** a Cargo workspace
member; it holds assets, not a Rust crate.

The kill-switch panel posts to `POST /api/kill` and polls `GET /api/control`.
The admin may refuse the post (loopback-only by default; `--control-token` for
other peers), and the status line says what the admin submitted, not what the
engine applied: the engine sends no acknowledgement for a kill.
