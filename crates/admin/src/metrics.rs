//! Prometheus text and the latency summary the UI polls.
//!
//! The admin is the only process that knows both the market-data view and the
//! cluster counters, so it exports one `/metrics` page. The benchmark's
//! gauges come from a text file written by `fix-bench --prometheus`; the admin
//! does not need to parse the benchmark's JSON.

use std::path::Path;
use std::time::Duration;

use deepmsg_cluster_common::counters::{
    CLUSTER_COMMIT_POSITION_TYPE_ID, CLUSTER_NODE_ROLE_TYPE_ID, CLUSTER_SNAPSHOT_COUNTER_TYPE_ID,
    find_cluster_counter,
};
use deepmsg_cnc::file::CncFile;

use crate::state::View;

/// The cluster counters one scrape read.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ClusterCounters {
    /// `aeron.cluster.commit.position`.
    pub commit_position: Option<i64>,
    /// The snapshot counter.
    pub snapshot_count: Option<i64>,
    /// `Cluster.Role` (`0` follower, `2` leader).
    pub role: Option<i64>,
}

/// Read the three counters from the media driver's CnC file.
#[must_use]
pub fn read_cluster(aeron_dir: &Path, cluster_id: i32) -> ClusterCounters {
    let Ok(cnc) = CncFile::open(aeron_dir, Duration::from_millis(200)) else {
        return ClusterCounters::default();
    };
    let Some(counters) = cnc.counters() else {
        return ClusterCounters::default();
    };
    let read = |type_id: i32| {
        find_cluster_counter(&counters, type_id, cluster_id)
            .and_then(|counter_id| counters.value(counter_id))
    };
    ClusterCounters {
        commit_position: read(CLUSTER_COMMIT_POSITION_TYPE_ID),
        snapshot_count: read(CLUSTER_SNAPSHOT_COUNTER_TYPE_ID),
        role: read(CLUSTER_NODE_ROLE_TYPE_ID),
    }
}

/// The `/metrics` body.
#[must_use]
pub fn prometheus(view: &View, cluster: &ClusterCounters, bench: Option<&str>) -> String {
    let mut out = String::with_capacity(1024);
    out.push_str("# TYPE deepbook_md_seq gauge\ndeepbook_md_seq ");
    out.push_str(&view.md_seq.to_string());
    out.push('\n');
    out.push_str("# TYPE deepbook_synced gauge\ndeepbook_synced ");
    out.push_str(if view.synced { "1" } else { "0" });
    out.push('\n');
    out.push_str("# TYPE deepbook_book_levels gauge\n");
    out.push_str(&format!(
        "deepbook_book_levels{{side=\"bid\"}} {}\n",
        view.bids.len()
    ));
    out.push_str(&format!(
        "deepbook_book_levels{{side=\"ask\"}} {}\n",
        view.asks.len()
    ));
    out.push_str("# TYPE deepbook_trades_total counter\ndeepbook_trades_total ");
    out.push_str(&view.trades_total.to_string());
    out.push('\n');
    out.push_str("# TYPE deepbook_book_qty gauge\n");
    out.push_str(&format!(
        "deepbook_book_qty{{side=\"bid\"}} {}\n",
        view.bids.iter().map(|level| level.qty).sum::<i64>()
    ));
    out.push_str(&format!(
        "deepbook_book_qty{{side=\"ask\"}} {}\n",
        view.asks.iter().map(|level| level.qty).sum::<i64>()
    ));
    if let Some(value) = cluster.commit_position {
        out.push_str("# TYPE deepbook_cluster_commit_position gauge\n");
        out.push_str(&format!("deepbook_cluster_commit_position {value}\n"));
    }
    if let Some(value) = cluster.snapshot_count {
        out.push_str("# TYPE deepbook_cluster_snapshot_count gauge\n");
        out.push_str(&format!("deepbook_cluster_snapshot_count {value}\n"));
    }
    if let Some(value) = cluster.role {
        out.push_str("# TYPE deepbook_cluster_role gauge\n");
        out.push_str(&format!("deepbook_cluster_role {value}\n"));
    }
    if let Some(bench) = bench {
        for line in bench.lines() {
            if line.starts_with("deepbook_bench_") {
                out.push_str(line);
                out.push('\n');
            }
        }
    }
    out
}

/// The `/api/latency` body: the benchmark's headline numbers as JSON.
#[must_use]
pub fn latency_json(bench: Option<&str>) -> String {
    let mut p50 = 0.0;
    let mut p90 = 0.0;
    let mut p99 = 0.0;
    let mut rate = 0.0;
    let mut completed = 0_u64;
    let mut available = false;
    if let Some(bench) = bench {
        for line in bench.lines() {
            let Some((name, value)) = line.split_once(' ') else {
                continue;
            };
            match name {
                "deepbook_bench_rtt_p50_us" => {
                    p50 = value.trim().parse().unwrap_or(0.0);
                    available = true;
                }
                "deepbook_bench_rtt_p90_us" => p90 = value.trim().parse().unwrap_or(0.0),
                "deepbook_bench_rtt_p99_us" => p99 = value.trim().parse().unwrap_or(0.0),
                "deepbook_bench_rate_per_s" => rate = value.trim().parse().unwrap_or(0.0),
                "deepbook_bench_completed" => {
                    completed = value.trim().parse().unwrap_or(0);
                }
                _ => {}
            }
        }
    }
    format!(
        "{{\"available\":{},\"p50_us\":{p50:.1},\"p90_us\":{p90:.1},\"p99_us\":{p99:.1},\
         \"rate_per_s\":{rate:.0},\"completed\":{completed}}}",
        available
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::LevelView;

    #[test]
    fn metrics_name_the_view_and_pass_the_bench_through() {
        let view = View {
            md_seq: 12,
            synced: true,
            bids: vec![LevelView { price: 100, qty: 5 }],
            asks: vec![],
            ..View::default()
        };
        let cluster = ClusterCounters {
            commit_position: Some(99),
            snapshot_count: Some(1),
            role: Some(2),
        };
        let text = prometheus(
            &view,
            &cluster,
            Some("deepbook_bench_rtt_p50_us 544.2\ndeepbook_bench_rate_per_s 9808\nother 1\n"),
        );
        assert!(text.contains("deepbook_md_seq 12"));
        assert!(text.contains("deepbook_book_levels{side=\"bid\"} 1"));
        assert!(text.contains("deepbook_cluster_commit_position 99"));
        assert!(text.contains("deepbook_bench_rtt_p50_us 544.2"));
        assert!(!text.contains("other 1"));
    }

    #[test]
    fn latency_json_is_zeroed_without_a_file() {
        let json = latency_json(None);
        assert!(json.contains("\"available\":false"));
        assert!(json.contains("\"p50_us\":0.0"));
    }

    #[test]
    fn latency_json_reads_the_gauges() {
        let json = latency_json(Some(
            "deepbook_bench_rtt_p50_us 544.2\n\
             deepbook_bench_rtt_p99_us 1038.0\n\
             deepbook_bench_rate_per_s 9808.4\n\
             deepbook_bench_completed 20000\n",
        ));
        assert!(json.contains("\"available\":true"));
        assert!(json.contains("\"p50_us\":544.2"));
        assert!(json.contains("\"completed\":20000"));
    }
}
