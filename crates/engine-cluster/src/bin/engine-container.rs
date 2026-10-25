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
//! The media driver and the archive are separate processes in the same
//! `aeron.dir`.

use std::path::Path;
use std::process::ExitCode;

use deepbook_engine_cluster::EngineService;
use deepbook_engine_core::{EngineConfig, RiskConfig};
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
    let service = EngineService::new(engine_config);

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
