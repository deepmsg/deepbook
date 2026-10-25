//! The deepbook engine's cluster service container.
//!
//! The counterpart of `deepmsg-clustered-service-container`, with
//! [`EngineService`] in place of the stock registry. It reads the usual
//! `aeron.cluster.*` properties and three engine settings:
//!
//! | Property | Default |
//! |---|---|
//! | `deepbook.engine.symbol` | 1 |
//! | `deepbook.engine.max-orders` | 65536 |
//! | `deepbook.engine.recent-window` | 100000 |
//!
//! Market data is published on the same driver by default
//! (`deepbook.marketdata.enabled=true`). The channels default to IPC and the
//! stream ids to 2001/2002; the admin process subscribes to the same names.
//!
//! The media driver and the archive are separate processes in the same
//! `aeron.dir`.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use deepbook_engine_cluster::{EngineService, MarketDataConfig};
use deepbook_engine_core::{EngineConfig, RiskConfig};
use deepbook_marketdata::{DeepmsgConfig, DeepmsgRecoveryResponder};
use deepmsg_cluster_common::config::Properties;
use deepmsg_cluster_service::{ClusteredServiceAgent, ServiceContainerConfig};
use deepmsg_driver::idle::Strategy;
use deepmsg_driver::sys;

const NAME: &str = "engine-container";

const USAGE: &str = "\
Usage: engine-container [options] [properties file ...]

  -Dname=value   set one cluster or driver property (a file overrides it)
  -h, --help     print this message

The engine settings are deepbook.engine.symbol, deepbook.engine.max-orders and
deepbook.engine.recent-window. The media driver and the archive are separate
processes in the same -Daeron.dir.";

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("{NAME}: {message}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|arg| arg == "-h" || arg == "--help") {
        println!("{USAGE}");
        return Ok(());
    }

    let mut properties = Properties::new();
    properties
        .apply_args(args.iter().filter(|arg| arg.starts_with("-D")).cloned())
        .map_err(|error| error.to_string())?;
    for arg in &args {
        if arg.starts_with("-D") {
            continue;
        }
        if arg.starts_with('-') {
            return Err(format!("unknown option '{arg}'\n\n{USAGE}"));
        }
        properties
            .load_file(Path::new(arg))
            .map_err(|error| error.to_string())?;
    }

    let config = ServiceContainerConfig::from_properties(&properties).map_err(|error| {
        format!("the configuration is refused: {error} (see `aeron.cluster.*` in docs/compat.md)")
    })?;
    let engine_config = EngineConfig {
        symbol: property(&properties, "deepbook.engine.symbol", 1),
        max_orders: property(&properties, "deepbook.engine.max-orders", 65_536),
        recent_order_window: property(&properties, "deepbook.engine.recent-window", 100_000),
        risk: RiskConfig::disabled(),
    };
    let service = if property(&properties, "deepbook.marketdata.enabled", true) {
        let deepmsg = market_data_config(&properties, &config.aeron_dir);
        let market = MarketDataConfig {
            deepmsg: deepmsg.clone(),
            recent_capacity: property(&properties, "deepbook.marketdata.recent-capacity", 10_000),
        };
        let service = EngineService::with_market_data(engine_config, market)
            .map_err(|error| error.to_string())?;
        // Recovery requests must be answered even when no command is arriving,
        // so a dashboard that starts on an idle cluster still gets a snapshot.
        // The responder owns the recovery channels; the service's publisher
        // only publishes incrementals.
        if let Some(cell) = service.market_snapshot_handle() {
            std::thread::Builder::new()
                .name("deepbook-md-recovery".to_owned())
                .spawn(move || {
                    let mut responder = match DeepmsgRecoveryResponder::connect(&deepmsg) {
                        Ok(responder) => responder,
                        Err(error) => {
                            eprintln!("engine: market data recovery: {error}");
                            return;
                        }
                    };
                    loop {
                        let snapshot = match cell.lock() {
                            Ok(snapshot) => snapshot.clone(),
                            Err(_) => break,
                        };
                        if let Err(error) = responder.poll_snapshot(&snapshot) {
                            eprintln!("engine: market data recovery: {error}");
                        }
                        std::thread::sleep(std::time::Duration::from_millis(5));
                    }
                })
                .map_err(|error| format!("cannot start the market-data responder: {error}"))?;
        }
        service
    } else {
        EngineService::new(engine_config)
    };

    sys::set_current_thread_name("deepbook-engine")
        .map_err(|error| format!("the thread cannot be named: {error}"))?;
    let mut idle = Strategy::by_agrona_name(&config.idle_strategy).ok_or_else(|| {
        format!(
            "aeron.cluster.idle.strategy={} names no strategy this build has",
            config.idle_strategy
        )
    })?;

    let mut agent = ClusteredServiceAgent::launch(config, service).map_err(|e| e.to_string())?;
    if let Err(error) = sys::install_stop_handler() {
        let _ = agent.close();
        return Err(format!("could not install the signal handler: {error}"));
    }

    println!("STARTED service=deepbook-engine");
    while sys::stop_signal().is_none() {
        match agent.do_work() {
            Ok(work) => idle.idle(work),
            Err(error) if error.is_termination() => {
                return agent.close().map_err(|error| error.to_string());
            }
            Err(error) => {
                let close = agent.close();
                return Err(match close {
                    Ok(()) => error.to_string(),
                    Err(close_error) => format!("{error}; closing also failed: {close_error}"),
                });
            }
        }
    }
    agent.close().map_err(|error| error.to_string())
}

fn property<T: std::str::FromStr>(properties: &Properties, name: &str, default: T) -> T {
    properties
        .get(name)
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn property_string(properties: &Properties, name: &str, default: &str) -> String {
    properties.get(name).unwrap_or(default).to_owned()
}

fn market_data_config(properties: &Properties, aeron_dir: &Path) -> DeepmsgConfig {
    let incremental = property_string(
        properties,
        "deepbook.marketdata.incremental.channel",
        "aeron:ipc",
    );
    let incremental_subscription = property_string(
        properties,
        "deepbook.marketdata.incremental.subscription.channel",
        &incremental,
    );
    let request = property_string(
        properties,
        "deepbook.marketdata.recovery.request.channel",
        "aeron:ipc",
    );
    let request_subscription = property_string(
        properties,
        "deepbook.marketdata.recovery.request.subscription.channel",
        &request,
    );
    let response = property_string(
        properties,
        "deepbook.marketdata.recovery.response.channel",
        "aeron:ipc",
    );
    let response_subscription = property_string(
        properties,
        "deepbook.marketdata.recovery.response.subscription.channel",
        &response,
    );
    let aeron = properties
        .get("deepbook.marketdata.aeron.dir")
        .map_or_else(|| aeron_dir.to_path_buf(), PathBuf::from);
    let mut config = DeepmsgConfig::new(
        aeron,
        incremental,
        incremental_subscription,
        request,
        request_subscription,
        response,
        response_subscription,
    );
    config.incremental_stream_id = property(
        properties,
        "deepbook.marketdata.incremental.stream.id",
        config.incremental_stream_id,
    );
    config.recovery_stream_id = property(
        properties,
        "deepbook.marketdata.recovery.stream.id",
        config.recovery_stream_id,
    );
    config
}
