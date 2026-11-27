//! `ouch-gateway`: the DeepOUCH multi-seat gateway (ADR 0012).
//!
//! ```text
//! ouch-gateway --driver-dir /dev/shm/aeron --bind 127.0.0.1:9880 \
//!              --seats seats.toml
//! ```
//!
//! FIX seats remain on `fix-gateway`; this binary hosts the OUCH seats. Both
//! load the same `seats.toml` and share the engine's service envelopes.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use deepbook_gateway::metrics::GatewayMetrics;
use deepbook_gateway::seat::{SeatGateway, SeatGatewayConfig, SeatRegistry};
use deepbook_gateway::{ClusterEngineConfig, SessionKey};
use deepbook_io::BackendKind;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("ouch-gateway: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let options = Options::parse()?;
    let text = std::fs::read_to_string(&options.seats)
        .map_err(|error| format!("{}: {error}", options.seats.display()))?;
    let registry = SeatRegistry::parse(&text).map_err(|error| error.to_string())?;
    if registry.is_empty() {
        return Err(format!("{} has no seats", options.seats.display()));
    }

    let driver_dir = options
        .driver_dir
        .ok_or_else(|| "--driver-dir is required".to_owned())?;
    let driver_display = driver_dir.display().to_string();
    let mut cluster = ClusterEngineConfig::new(driver_dir, SessionKey::new("DEEPBOOK", "SEATS"));
    if let Some(channel) = options.ingress_channel {
        cluster.ingress_channel = channel;
    }
    if let Some(endpoints) = options.ingress_endpoints {
        cluster.ingress_endpoints = Some(endpoints);
    }
    if let Some(stream) = options.ingress_stream_id {
        cluster.ingress_stream_id = stream;
    }
    if let Some(channel) = options.egress_channel {
        cluster.egress_channel = channel;
    }
    if let Some(stream) = options.egress_stream_id {
        cluster.egress_stream_id = stream;
    }
    if let Some(timeout) = options.message_timeout_ms {
        cluster.message_timeout_ns = timeout.saturating_mul(1_000_000);
    }
    if let Some(attempts) = options.connect_attempts {
        cluster.connect_attempts = attempts;
    }
    if let Some(timeout) = options.new_leader_timeout_ms {
        cluster.new_leader_timeout_ns = (timeout > 0).then(|| timeout.saturating_mul(1_000_000));
    }

    let mut config = SeatGatewayConfig::new(options.bind, registry, cluster);
    config.tick = Duration::from_micros(options.tick_us);
    config.backend = options.backend;
    let metrics = Arc::new(GatewayMetrics::default());
    let mut gateway = SeatGateway::new(config, metrics)
        .map_err(|error| format!("cannot start the seat gateway: {error}"))?;
    println!(
        "ouch-gateway bind={} seats={} driver={}",
        gateway.local_addr().map_err(|error| error.to_string())?,
        options.seats.display(),
        driver_display
    );
    gateway
        .run()
        .map_err(|error| format!("event loop: {error}"))
}

struct Options {
    bind: SocketAddr,
    seats: PathBuf,
    driver_dir: Option<PathBuf>,
    ingress_channel: Option<String>,
    ingress_endpoints: Option<String>,
    ingress_stream_id: Option<i32>,
    egress_channel: Option<String>,
    egress_stream_id: Option<i32>,
    message_timeout_ms: Option<i64>,
    connect_attempts: Option<u32>,
    new_leader_timeout_ms: Option<i64>,
    tick_us: u64,
    backend: BackendKind,
}

impl Options {
    fn parse() -> Result<Self, String> {
        let mut options = Self {
            bind: "127.0.0.1:9880".parse().expect("valid default"),
            seats: PathBuf::from("seats.toml"),
            driver_dir: None,
            ingress_channel: None,
            ingress_endpoints: None,
            ingress_stream_id: None,
            egress_channel: None,
            egress_stream_id: None,
            message_timeout_ms: None,
            connect_attempts: None,
            new_leader_timeout_ms: None,
            tick_us: 100_000,
            backend: BackendKind::Epoll,
        };
        let mut arguments = std::env::args().skip(1);
        while let Some(argument) = arguments.next() {
            let mut value = || {
                arguments
                    .next()
                    .ok_or_else(|| format!("{argument} needs a value"))
            };
            match argument.as_str() {
                "--bind" => options.bind = value()?.parse().map_err(|_| "bad --bind")?,
                "--seats" => options.seats = PathBuf::from(value()?),
                "--driver-dir" => options.driver_dir = Some(PathBuf::from(value()?)),
                "--ingress-channel" => options.ingress_channel = Some(value()?),
                "--ingress-endpoints" => options.ingress_endpoints = Some(value()?),
                "--ingress-stream-id" => {
                    options.ingress_stream_id =
                        Some(value()?.parse().map_err(|_| "bad --ingress-stream-id")?);
                }
                "--egress-channel" => options.egress_channel = Some(value()?),
                "--egress-stream-id" => {
                    options.egress_stream_id =
                        Some(value()?.parse().map_err(|_| "bad --egress-stream-id")?);
                }
                "--message-timeout-ms" => {
                    options.message_timeout_ms =
                        Some(value()?.parse().map_err(|_| "bad --message-timeout-ms")?);
                }
                "--connect-attempts" => {
                    options.connect_attempts =
                        Some(value()?.parse().map_err(|_| "bad --connect-attempts")?);
                }
                "--new-leader-timeout-ms" => {
                    options.new_leader_timeout_ms = Some(
                        value()?
                            .parse()
                            .map_err(|_| "bad --new-leader-timeout-ms")?,
                    );
                }
                "--tick-us" => {
                    options.tick_us = value()?.parse().map_err(|_| "bad --tick-us")?;
                }
                "--backend" => {
                    options.backend = match value()?.as_str() {
                        "epoll" => BackendKind::Epoll,
                        "uring" => BackendKind::Uring,
                        other => return Err(format!("unknown backend {other}")),
                    };
                }
                "--help" | "-h" => {
                    println!(
                        "ouch-gateway --driver-dir DIR [--bind ADDR] [--seats FILE] \
                         [--ingress-channel CH] [--egress-channel CH] [--backend epoll|uring]"
                    );
                    std::process::exit(0);
                }
                other => return Err(format!("unknown argument {other}")),
            }
        }
        Ok(options)
    }
}
