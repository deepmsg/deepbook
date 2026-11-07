//! The gateway's numbers for its loopback `/metrics` page.
//!
//! These are the facts only this process has: how many FIX sessions are open,
//! whether its cluster session is up, and how many commands and reports went
//! through. The cluster's own counters keep answering after the engine dies
//! (a CnC file is a memory-mapped record, not a heartbeat), so a scrape of
//! this page is the liveness signal the deployment alerts on.
//!
//! `new_leader_timeouts_total` is deliberately absent: the pinned deepmsg
//! client closes its session when its `newLeaderTimeoutNs` expires, but does
//! not expose *why* it closed, and counting every close as a leader timeout
//! would be a lie. It arrives when the client exposes a close reason.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// The gateway's counters and gauges, shared by the acceptor and the cluster
/// engine.
#[derive(Debug, Default)]
pub struct GatewayMetrics {
    sessions: AtomicU64,
    cluster_connected: AtomicBool,
    orders_total: AtomicU64,
    reports_total: AtomicU64,
    rejects_total: AtomicU64,
}

impl GatewayMetrics {
    /// Logged-on FIX sessions.
    #[must_use]
    pub fn sessions(&self) -> u64 {
        self.sessions.load(Ordering::Relaxed)
    }

    /// Whether the cluster session is up.
    #[must_use]
    pub fn cluster_connected(&self) -> bool {
        self.cluster_connected.load(Ordering::Relaxed)
    }

    /// Commands received from peers.
    #[must_use]
    pub fn orders_total(&self) -> u64 {
        self.orders_total.load(Ordering::Relaxed)
    }

    /// Reports handed back to the FIX side.
    #[must_use]
    pub fn reports_total(&self) -> u64 {
        self.reports_total.load(Ordering::Relaxed)
    }

    /// Commands the gateway refused before the cluster saw them.
    #[must_use]
    pub fn rejects_total(&self) -> u64 {
        self.rejects_total.load(Ordering::Relaxed)
    }

    pub(crate) fn note_session_open(&self) {
        self.sessions.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn note_session_close(&self) {
        // Saturating: a close is only counted for a session that counted an
        // open, and a race must not wrap the gauge.
        let _ = self
            .sessions
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |open| {
                Some(open.saturating_sub(1))
            });
    }

    pub(crate) fn set_cluster_connected(&self, connected: bool) {
        self.cluster_connected.store(connected, Ordering::Relaxed);
    }

    pub(crate) fn note_order(&self) {
        self.orders_total.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn note_report(&self) {
        self.reports_total.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn note_reject(&self) {
        self.rejects_total.fetch_add(1, Ordering::Relaxed);
    }
}

/// The `/metrics` body (Prometheus text format 0.0.4).
#[must_use]
pub fn prometheus(metrics: &GatewayMetrics) -> String {
    let mut out = String::with_capacity(512);
    push_gauge(&mut out, "deepbook_gateway_sessions", metrics.sessions());
    push_gauge(
        &mut out,
        "deepbook_gateway_cluster_connected",
        u64::from(metrics.cluster_connected()),
    );
    push_counter(
        &mut out,
        "deepbook_gateway_orders_total",
        metrics.orders_total(),
    );
    push_counter(
        &mut out,
        "deepbook_gateway_reports_total",
        metrics.reports_total(),
    );
    push_counter(
        &mut out,
        "deepbook_gateway_rejects_total",
        metrics.rejects_total(),
    );
    out
}

fn push_gauge(out: &mut String, name: &str, value: u64) {
    out.push_str("# TYPE ");
    out.push_str(name);
    out.push_str(" gauge\n");
    out.push_str(name);
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

fn push_counter(out: &mut String, name: &str, value: u64) {
    out.push_str("# TYPE ");
    out.push_str(name);
    out.push_str(" counter\n");
    out.push_str(name);
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

#[cfg(test)]
mod tests {
    use super::{GatewayMetrics, prometheus};

    #[test]
    fn the_page_is_prometheus_text_with_the_five_series() {
        let metrics = GatewayMetrics::default();
        metrics.note_session_open();
        metrics.set_cluster_connected(true);
        metrics.note_order();
        metrics.note_report();
        metrics.note_reject();

        let page = prometheus(&metrics);
        assert!(page.contains("deepbook_gateway_sessions 1"), "{page}");
        assert!(
            page.contains("deepbook_gateway_cluster_connected 1"),
            "{page}"
        );
        assert!(page.contains("deepbook_gateway_orders_total 1"), "{page}");
        assert!(page.contains("deepbook_gateway_reports_total 1"), "{page}");
        assert!(page.contains("deepbook_gateway_rejects_total 1"), "{page}");
    }

    #[test]
    fn a_closed_session_does_not_take_the_gauge_below_zero() {
        let metrics = GatewayMetrics::default();
        metrics.note_session_close();
        assert_eq!(0, metrics.sessions());
    }
}
