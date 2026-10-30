//! The deepbook engine's cluster service container.
//!
//! The counterpart of `deepmsg-clustered-service-container`, with
//! [`EngineService`] in place of the stock registry. It reads the usual
//! `aeron.cluster.*` properties and the engine and risk settings:
//!
//! | Property | Default |
//! |---|---|
//! | `deepbook.engine.symbol` | 1 |
//! | `deepbook.engine.max-orders` | 65536 |
//! | `deepbook.engine.recent-window` | 100000 |
//! | `deepbook.risk.max-order-qty` | unlimited |
//! | `deepbook.risk.max-notional` | unlimited |
//! | `deepbook.risk.price-band-ticks` | off |
//! | `deepbook.risk.fat-finger-ticks` | off |
//! | `deepbook.risk.reference-price` | off |
//! | `deepbook.risk.max-open-orders` | unlimited |
//! | `deepbook.risk.self-trade` | `none` |
//!
//! Every `deepbook.*` value is parsed strictly: a malformed limit stops the
//! container instead of silently falling back to "unlimited". The limits are
//! deployment configuration, not cluster state (ADR 0005), so every node must
//! start with the same values and changing one restarts the whole cluster.
//! The price band and the fat-finger check apply once the book has a last
//! trade, or from `deepbook.risk.reference-price` when one is set.
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
use deepbook_engine_core::{EngineConfig, RiskConfig, SelfTradePrevention};
use deepbook_marketdata::{DeepmsgConfig, DeepmsgRecoveryResponder};
use deepbook_types::{Price, Qty};
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
deepbook.engine.recent-window. The pre-trade limits are deepbook.risk.max-order-qty,
deepbook.risk.max-notional, deepbook.risk.price-band-ticks,
deepbook.risk.fat-finger-ticks, deepbook.risk.reference-price,
deepbook.risk.max-open-orders and deepbook.risk.self-trade (none, cancel-resting
or reject-taker). A malformed value is refused at startup. The media driver and
the archive are separate processes in the same -Daeron.dir.";

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
        format!(
            "the configuration is refused: {error} \
             (see `aeron.cluster.*` in deepmsg's `docs/compat.md`)"
        )
    })?;
    let engine_config = engine_config(&properties)?;
    println!("{NAME}: {}", describe_risk(&engine_config.risk));
    let service = if property(&properties, "deepbook.marketdata.enabled", true)? {
        let deepmsg = market_data_config(&properties, &config.aeron_dir)?;
        let market = MarketDataConfig {
            deepmsg: deepmsg.clone(),
            recent_capacity: property(&properties, "deepbook.marketdata.recent-capacity", 10_000)?,
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

/// Parse the `deepbook.engine.*` and `deepbook.risk.*` settings.
///
/// # Errors
///
/// A malformed value, or a limit that would halt trading by accident (a
/// zero or negative quantity/notional limit, a negative band or a
/// non-positive reference price).
fn engine_config(properties: &Properties) -> Result<EngineConfig, String> {
    Ok(EngineConfig {
        symbol: property(properties, "deepbook.engine.symbol", 1)?,
        max_orders: property(properties, "deepbook.engine.max-orders", 65_536)?,
        recent_order_window: property(properties, "deepbook.engine.recent-window", 100_000)?,
        risk: risk_config(properties)?,
    })
}

fn risk_config(properties: &Properties) -> Result<RiskConfig, String> {
    let defaults = RiskConfig::disabled();
    let config = RiskConfig {
        max_order_qty: property(
            properties,
            "deepbook.risk.max-order-qty",
            defaults.max_order_qty,
        )?,
        max_notional: property(
            properties,
            "deepbook.risk.max-notional",
            defaults.max_notional,
        )?,
        price_band_ticks: optional(properties, "deepbook.risk.price-band-ticks")?,
        fat_finger_ticks: optional(properties, "deepbook.risk.fat-finger-ticks")?,
        reference_price: optional(properties, "deepbook.risk.reference-price")?,
        max_open_orders_per_account: property(
            properties,
            "deepbook.risk.max-open-orders",
            defaults.max_open_orders_per_account,
        )?,
        self_trade: match properties.get("deepbook.risk.self-trade") {
            None => SelfTradePrevention::None,
            Some(value) => parse_self_trade(value)?,
        },
    };
    // A limit that is valid but would reject every order is almost always a
    // typo; the kill switch is the deliberate way to stop the market.
    if config.max_order_qty <= 0 {
        return Err("deepbook.risk.max-order-qty must be positive".to_owned());
    }
    if config.max_notional <= 0 {
        return Err("deepbook.risk.max-notional must be positive".to_owned());
    }
    if config.price_band_ticks.is_some_and(|band| band < 0) {
        return Err("deepbook.risk.price-band-ticks must not be negative".to_owned());
    }
    if config.fat_finger_ticks.is_some_and(|ticks| ticks < 0) {
        return Err("deepbook.risk.fat-finger-ticks must not be negative".to_owned());
    }
    if config.reference_price.is_some_and(|price| price <= 0) {
        return Err("deepbook.risk.reference-price must be positive".to_owned());
    }
    Ok(config)
}

fn parse_self_trade(value: &str) -> Result<SelfTradePrevention, String> {
    match value.to_ascii_lowercase().as_str() {
        "none" => Ok(SelfTradePrevention::None),
        "cancel-resting" => Ok(SelfTradePrevention::CancelResting),
        "reject-taker" => Ok(SelfTradePrevention::RejectTaker),
        _ => Err(format!(
            "deepbook.risk.self-trade={value} is not one of none, cancel-resting, reject-taker"
        )),
    }
}

/// One line describing the effective limits, printed at startup.
fn describe_risk(risk: &RiskConfig) -> String {
    format!(
        "risk max-qty={} max-notional={} price-band={} fat-finger={} reference-price={} \
         max-open-orders={} self-trade={}",
        limit(risk.max_order_qty == Qty::MAX, risk.max_order_qty),
        limit(risk.max_notional == i128::MAX, risk.max_notional),
        ticks(risk.price_band_ticks),
        ticks(risk.fat_finger_ticks),
        ticks(risk.reference_price),
        limit(
            risk.max_open_orders_per_account == usize::MAX,
            risk.max_open_orders_per_account
        ),
        self_trade_name(risk.self_trade),
    )
}

fn limit<T: std::fmt::Display>(unlimited: bool, value: T) -> String {
    if unlimited {
        "unlimited".to_owned()
    } else {
        value.to_string()
    }
}

fn ticks(value: Option<Price>) -> String {
    value.map_or_else(|| "off".to_owned(), |ticks| ticks.to_string())
}

const fn self_trade_name(value: SelfTradePrevention) -> &'static str {
    match value {
        SelfTradePrevention::None => "none",
        SelfTradePrevention::CancelResting => "cancel-resting",
        SelfTradePrevention::RejectTaker => "reject-taker",
    }
}

/// A `deepbook.*` setting, or its default when it is absent.
///
/// # Errors
///
/// The value is present but does not parse as `T`.
fn property<T: std::str::FromStr>(
    properties: &Properties,
    name: &str,
    default: T,
) -> Result<T, String> {
    match properties.get(name) {
        None => Ok(default),
        Some(value) => value
            .parse()
            .map_err(|_| format!("{name}={value} is not a valid value")),
    }
}

/// An optional `deepbook.*` setting: absent means the check is off.
///
/// # Errors
///
/// The value is present but does not parse as `T`.
fn optional<T: std::str::FromStr>(
    properties: &Properties,
    name: &str,
) -> Result<Option<T>, String> {
    match properties.get(name) {
        None => Ok(None),
        Some(value) => value
            .parse()
            .map(Some)
            .map_err(|_| format!("{name}={value} is not a valid value")),
    }
}

fn property_string(properties: &Properties, name: &str, default: &str) -> String {
    properties.get(name).unwrap_or(default).to_owned()
}

fn market_data_config(properties: &Properties, aeron_dir: &Path) -> Result<DeepmsgConfig, String> {
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
    )?;
    config.recovery_stream_id = property(
        properties,
        "deepbook.marketdata.recovery.stream.id",
        config.recovery_stream_id,
    )?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn properties(pairs: &[(&str, &str)]) -> Properties {
        let mut properties = Properties::new();
        let args: Vec<String> = pairs
            .iter()
            .map(|(name, value)| format!("-D{name}={value}"))
            .collect();
        properties.apply_args(args).expect("applied");
        properties
    }

    #[test]
    fn absent_settings_leave_risk_disabled() {
        let config = engine_config(&properties(&[])).expect("parsed");
        assert_eq!(1, config.symbol);
        assert_eq!(65_536, config.max_orders);
        assert_eq!(100_000, config.recent_order_window);
        assert_eq!(Qty::MAX, config.risk.max_order_qty);
        assert_eq!(i128::MAX, config.risk.max_notional);
        assert_eq!(None, config.risk.price_band_ticks);
        assert_eq!(None, config.risk.fat_finger_ticks);
        assert_eq!(None, config.risk.reference_price);
        assert_eq!(usize::MAX, config.risk.max_open_orders_per_account);
        assert_eq!(SelfTradePrevention::None, config.risk.self_trade);
    }

    #[test]
    fn every_risk_setting_is_read() {
        let config = engine_config(&properties(&[
            ("deepbook.engine.symbol", "3"),
            ("deepbook.engine.max-orders", "128"),
            ("deepbook.engine.recent-window", "256"),
            ("deepbook.risk.max-order-qty", "500"),
            ("deepbook.risk.max-notional", "1000000"),
            ("deepbook.risk.price-band-ticks", "25"),
            ("deepbook.risk.fat-finger-ticks", "100"),
            ("deepbook.risk.reference-price", "10025"),
            ("deepbook.risk.max-open-orders", "10"),
            ("deepbook.risk.self-trade", "cancel-resting"),
        ]))
        .expect("parsed");
        assert_eq!(3, config.symbol);
        assert_eq!(128, config.max_orders);
        assert_eq!(256, config.recent_order_window);
        assert_eq!(500, config.risk.max_order_qty);
        assert_eq!(1_000_000, config.risk.max_notional);
        assert_eq!(Some(25), config.risk.price_band_ticks);
        assert_eq!(Some(100), config.risk.fat_finger_ticks);
        assert_eq!(Some(10_025), config.risk.reference_price);
        assert_eq!(10, config.risk.max_open_orders_per_account);
        assert_eq!(SelfTradePrevention::CancelResting, config.risk.self_trade);
    }

    #[test]
    fn the_self_trade_policies_all_parse() {
        assert_eq!(
            SelfTradePrevention::None,
            parse_self_trade("none").expect("parsed")
        );
        assert_eq!(
            SelfTradePrevention::RejectTaker,
            parse_self_trade("Reject-Taker").expect("parsed")
        );
        assert!(parse_self_trade("sometimes").is_err());
    }

    #[test]
    fn a_malformed_value_is_refused() {
        let error = engine_config(&properties(&[("deepbook.risk.max-order-qty", "ten")]))
            .expect_err("refused");
        assert!(error.contains("deepbook.risk.max-order-qty=ten"), "{error}");

        let error = engine_config(&properties(&[("deepbook.engine.max-orders", "lots")]))
            .expect_err("refused");
        assert!(error.contains("deepbook.engine.max-orders=lots"), "{error}");
    }

    #[test]
    fn a_limit_that_would_halt_trading_is_refused() {
        for (name, value) in [
            ("deepbook.risk.max-order-qty", "0"),
            ("deepbook.risk.max-order-qty", "-5"),
            ("deepbook.risk.max-notional", "0"),
            ("deepbook.risk.max-notional", "-1"),
            ("deepbook.risk.price-band-ticks", "-1"),
            ("deepbook.risk.fat-finger-ticks", "-1"),
            ("deepbook.risk.reference-price", "0"),
            ("deepbook.risk.reference-price", "-100"),
        ] {
            let result = engine_config(&properties(&[(name, value)]));
            assert!(result.is_err(), "{name}={value} was accepted");
        }
    }

    #[test]
    fn the_summary_says_what_is_off() {
        let summary = describe_risk(&RiskConfig::disabled());
        assert!(summary.contains("max-qty=unlimited"), "{summary}");
        assert!(summary.contains("price-band=off"), "{summary}");
        assert!(summary.contains("self-trade=none"), "{summary}");

        let risk = RiskConfig {
            max_order_qty: 500,
            max_notional: 1_000_000,
            price_band_ticks: Some(25),
            fat_finger_ticks: None,
            reference_price: Some(10_025),
            max_open_orders_per_account: 10,
            self_trade: SelfTradePrevention::RejectTaker,
        };
        let summary = describe_risk(&risk);
        assert!(summary.contains("max-qty=500"), "{summary}");
        assert!(summary.contains("price-band=25"), "{summary}");
        assert!(summary.contains("reference-price=10025"), "{summary}");
        assert!(summary.contains("max-open-orders=10"), "{summary}");
        assert!(summary.contains("self-trade=reject-taker"), "{summary}");
    }
}
