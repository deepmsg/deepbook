//! The bench client against the in-process M1 gateway.
//!
//! This is the harness smoke test, not a performance gate: it proves the
//! Logon, the open-loop send path, report matching and the percentile
//! report work over a real loopback socket.

use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

use deepbook_bench::{BenchConfig, run};
use deepbook_gateway::{Gateway, GatewayConfig, StubEngine};

fn temporary_directory(name: &str) -> PathBuf {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let path = std::env::temp_dir().join(format!(
        "deepbook-bench-{}-{name}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir_all(&path).expect("temporary directory");
    path
}

#[test]
fn the_bench_measures_reports_over_a_real_socket() {
    let directory = temporary_directory("smoke");
    let mut config = GatewayConfig::new(
        "127.0.0.1:0".parse().expect("address"),
        &directory,
        "DEEPBOOK",
        "CLIENT",
    );
    config.tick = Duration::from_millis(1);
    let mut gateway = Gateway::bind(config, StubEngine::new()).expect("bind");
    let address = gateway.local_addr().expect("local address");
    let handle = thread::spawn(move || {
        gateway
            .run_until(Instant::now() + Duration::from_secs(3))
            .expect("gateway");
    });

    let bench = BenchConfig {
        address,
        samples: 200,
        warmup: 20,
        rate_per_second: 5_000,
        timeout: Duration::from_secs(3),
        ..BenchConfig::default()
    };
    let report = run(&bench).expect("bench run");

    assert_eq!(220, report.sent);
    assert_eq!(200, report.completed, "every kept order has a first report");
    assert_eq!(200, report.samples_ns.len());
    assert_eq!(0, report.rejected);
    assert_eq!(0, report.unanswered);
    let summary = report.summary();
    assert!(summary.p50_ns > 0);
    assert!(summary.max_ns >= summary.p99_ns);
    assert!(report.rate_per_second() > 0.0);

    handle.join().expect("gateway thread");
    std::fs::remove_dir_all(directory).expect("cleanup");
}
