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
    /// Bytes the FIX side handed to each connection's decoder.
    fix_bytes_read_total: AtomicU64,
    /// Complete application frames the decoder produced, before delivery.
    fix_orders_decoded_total: AtomicU64,
    /// Bytes queued for FIX peers or submitted but not yet written.
    outbound_bytes: AtomicU64,
    /// The highest `outbound_bytes` seen since the gateway started.
    outbound_high_water_bytes: AtomicU64,
    /// Bytes actually handed to the sockets.
    fix_bytes_written_total: AtomicU64,
    /// Poll cycles that stopped reading because the outbound queue hit its
    /// high-water mark.
    outbound_blocked_total: AtomicU64,
    /// Poll cycles that found queued reports behind an in-flight write.
    write_backpressure_total: AtomicU64,
    /// Commands waiting for the cluster.
    pending_commands: AtomicU64,
    /// Entries in the gateway's local order projection.
    cluster_orders_len: AtomicU64,
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

    /// Bytes the FIX side handed to the decoders.
    #[must_use]
    pub fn fix_bytes_read_total(&self) -> u64 {
        self.fix_bytes_read_total.load(Ordering::Relaxed)
    }

    /// Complete application frames the decoders produced.
    #[must_use]
    pub fn fix_orders_decoded_total(&self) -> u64 {
        self.fix_orders_decoded_total.load(Ordering::Relaxed)
    }

    /// Bytes queued for FIX peers, including the chunk in flight.
    #[must_use]
    pub fn outbound_bytes(&self) -> u64 {
        self.outbound_bytes.load(Ordering::Relaxed)
    }

    /// The highest `outbound_bytes` since the gateway started.
    #[must_use]
    pub fn outbound_high_water_bytes(&self) -> u64 {
        self.outbound_high_water_bytes.load(Ordering::Relaxed)
    }

    /// Bytes actually handed to the sockets.
    #[must_use]
    pub fn fix_bytes_written_total(&self) -> u64 {
        self.fix_bytes_written_total.load(Ordering::Relaxed)
    }

    /// Poll cycles that stopped reading on the outbound high-water mark.
    #[must_use]
    pub fn outbound_blocked_total(&self) -> u64 {
        self.outbound_blocked_total.load(Ordering::Relaxed)
    }

    /// Poll cycles that found queued reports behind an in-flight write.
    #[must_use]
    pub fn write_backpressure_total(&self) -> u64 {
        self.write_backpressure_total.load(Ordering::Relaxed)
    }

    /// Commands waiting for the cluster.
    #[must_use]
    pub fn pending_commands(&self) -> u64 {
        self.pending_commands.load(Ordering::Relaxed)
    }

    /// Entries in the gateway's local order projection.
    #[must_use]
    pub fn cluster_orders_len(&self) -> u64 {
        self.cluster_orders_len.load(Ordering::Relaxed)
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

    pub(crate) fn note_fix_bytes(&self, bytes: usize) {
        self.fix_bytes_read_total
            .fetch_add(u64::try_from(bytes).unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    pub(crate) fn note_fix_order_decoded(&self) {
        self.fix_orders_decoded_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn set_outbound_bytes(&self, bytes: u64) {
        self.outbound_bytes.store(bytes, Ordering::Relaxed);
        self.outbound_high_water_bytes
            .fetch_max(bytes, Ordering::Relaxed);
    }

    pub(crate) fn note_fix_bytes_written(&self, bytes: u64) {
        self.fix_bytes_written_total
            .fetch_add(bytes, Ordering::Relaxed);
    }

    pub(crate) fn note_outbound_blocked(&self) {
        self.outbound_blocked_total.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn note_write_backpressure(&self) {
        self.write_backpressure_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn set_pending_commands(&self, commands: usize) {
        self.pending_commands.store(
            u64::try_from(commands).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    pub(crate) fn set_cluster_orders_len(&self, orders: usize) {
        self.cluster_orders_len
            .store(u64::try_from(orders).unwrap_or(u64::MAX), Ordering::Relaxed);
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
    push_counter(
        &mut out,
        "deepbook_gateway_fix_bytes_read_total",
        metrics.fix_bytes_read_total(),
    );
    push_counter(
        &mut out,
        "deepbook_gateway_fix_orders_decoded_total",
        metrics.fix_orders_decoded_total(),
    );
    push_gauge(
        &mut out,
        "deepbook_gateway_outbound_bytes",
        metrics.outbound_bytes(),
    );
    push_gauge(
        &mut out,
        "deepbook_gateway_outbound_high_water_bytes",
        metrics.outbound_high_water_bytes(),
    );
    push_counter(
        &mut out,
        "deepbook_gateway_fix_bytes_written_total",
        metrics.fix_bytes_written_total(),
    );
    push_counter(
        &mut out,
        "deepbook_gateway_outbound_blocked_total",
        metrics.outbound_blocked_total(),
    );
    push_counter(
        &mut out,
        "deepbook_gateway_write_backpressure_total",
        metrics.write_backpressure_total(),
    );
    push_gauge(
        &mut out,
        "deepbook_gateway_pending_commands",
        metrics.pending_commands(),
    );
    push_gauge(
        &mut out,
        "deepbook_gateway_cluster_orders_len",
        metrics.cluster_orders_len(),
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
    fn the_page_is_prometheus_text_with_every_series() {
        let metrics = GatewayMetrics::default();
        metrics.note_session_open();
        metrics.set_cluster_connected(true);
        metrics.note_order();
        metrics.note_report();
        metrics.note_reject();
        metrics.note_fix_bytes(1234);
        metrics.note_fix_order_decoded();
        metrics.set_outbound_bytes(567);
        metrics.set_outbound_bytes(89);
        metrics.note_fix_bytes_written(456);
        metrics.note_outbound_blocked();
        metrics.note_write_backpressure();
        metrics.set_pending_commands(7);
        metrics.set_cluster_orders_len(9);

        let page = prometheus(&metrics);
        assert!(page.contains("deepbook_gateway_sessions 1"), "{page}");
        assert!(
            page.contains("deepbook_gateway_cluster_connected 1"),
            "{page}"
        );
        assert!(page.contains("deepbook_gateway_orders_total 1"), "{page}");
        assert!(page.contains("deepbook_gateway_reports_total 1"), "{page}");
        assert!(page.contains("deepbook_gateway_rejects_total 1"), "{page}");
        assert!(
            page.contains("deepbook_gateway_fix_bytes_read_total 1234"),
            "{page}"
        );
        assert!(
            page.contains("deepbook_gateway_fix_orders_decoded_total 1"),
            "{page}"
        );
        assert!(
            page.contains("deepbook_gateway_outbound_bytes 89"),
            "{page}"
        );
        assert!(
            page.contains("deepbook_gateway_outbound_high_water_bytes 567"),
            "{page}"
        );
        assert!(
            page.contains("deepbook_gateway_fix_bytes_written_total 456"),
            "{page}"
        );
        assert!(
            page.contains("deepbook_gateway_outbound_blocked_total 1"),
            "{page}"
        );
        assert!(
            page.contains("deepbook_gateway_write_backpressure_total 1"),
            "{page}"
        );
        assert!(
            page.contains("deepbook_gateway_pending_commands 7"),
            "{page}"
        );
        assert!(
            page.contains("deepbook_gateway_cluster_orders_len 9"),
            "{page}"
        );
    }

    #[test]
    fn a_closed_session_does_not_take_the_gauge_below_zero() {
        let metrics = GatewayMetrics::default();
        metrics.note_session_close();
        assert_eq!(0, metrics.sessions());
    }
}
