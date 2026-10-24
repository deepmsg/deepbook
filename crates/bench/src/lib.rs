//! Open-loop FIX client and latency harness (M5).
//!
//! The client sends at a fixed rate so the measurement does not suffer from
//! coordinated omission. t0 is before `sendto`; t1 is after `recv` returns the
//! complete report. Server-side TSC spans are for attribution only and are
//! never reported as "end-to-end".
#![forbid(unsafe_code)]
