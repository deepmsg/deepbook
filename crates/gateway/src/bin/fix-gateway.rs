//! `fix-gateway`: the M1 acceptor with the in-process stub engine, or the M4
//! acceptor over the deepmsg cluster client.
//!
//! ```text
//! fix-gateway [--bind 127.0.0.1:9876] [--session-dir fix-sessions]
//!             [--sender DEEPBOOK] [--target CLIENT]
//!             [--heartbeat 30] [--journal 4096]
//!
//! fix-gateway --cluster --aeron-dir /dev/shm/aeron ...
//! ```
//!
//! Without `--cluster` the in-process [`StubEngine`] answers (M1). With it,
//! [`ClusterEngine`] offers each command to the cluster and turns the
//! cluster's events back into FIX reports (M4). The command line, the session
//! directories and the FIX session behaviour are the same in both modes.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;

use deepbook_fix::journal::SyncPolicy;
use deepbook_gateway::{
    ClusterEngine, ClusterEngineConfig, Engine, Gateway, GatewayConfig, SessionKey, StubEngine,
};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("fix-gateway: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let options = Options::parse()?;
    let mut config = GatewayConfig::new(
        options.bind,
        options.session_dir.clone(),
        options.sender.clone(),
        options.target.clone(),
    );
    config.heart_bt_int = options.heartbeat;
    config.journal_capacity = options.journal;
    config.tick = std::time::Duration::from_micros(options.tick_us);
    config.journal_sync = if options.journal_sync_batch {
        SyncPolicy::Every(options.journal_batch)
    } else {
        SyncPolicy::Each
    };

    if options.cluster {
        let aeron_dir = options
            .aeron_dir
            .clone()
            .ok_or_else(|| "--cluster needs --aeron-dir".to_owned())?;
        let mut cluster_config =
            ClusterEngineConfig::new(aeron_dir, SessionKey::new(&options.sender, &options.target));
        cluster_config.ingress_channel = options.ingress_channel.clone();
        cluster_config.ingress_stream_id = options.ingress_stream_id;
        cluster_config.egress_channel = options.egress_channel.clone();
        cluster_config.egress_stream_id = options.egress_stream_id;
        cluster_config.message_timeout_ns = options.message_timeout_ms.saturating_mul(1_000_000);
        cluster_config.new_leader_timeout_ns = (options.new_leader_timeout_ms > 0)
            .then(|| options.new_leader_timeout_ms.saturating_mul(1_000_000));
        cluster_config.symbol = options.symbol_id;
        cluster_config.symbol_name = options.symbol.clone();

        let engine = ClusterEngine::connect(cluster_config)
            .map_err(|error| format!("cannot connect the cluster: {error}"))?;
        println!(
            "fix-gateway cluster session={} aeron={}",
            engine
                .cluster_session_id()
                .map_or_else(|| "none".to_owned(), |id| id.to_string()),
            options
                .aeron_dir
                .as_ref()
                .map_or_else(String::new, |path| path.display().to_string())
        );
        serve(config, engine, &options)
    } else {
        serve(config, StubEngine::new(), &options)
    }
}

fn serve<E: Engine>(config: GatewayConfig, engine: E, options: &Options) -> Result<(), String> {
    let mut gateway = Gateway::bind(config, engine)
        .map_err(|error| format!("cannot bind {}: {error}", options.bind))?;
    let address = gateway
        .local_addr()
        .map_err(|error| format!("cannot read the bound address: {error}"))?;
    println!(
        "fix-gateway listening on {address} sessions={} sender={} target={}",
        options.session_dir.display(),
        options.sender,
        options.target
    );
    gateway.run().map_err(|error| error.to_string())
}

struct Options {
    bind: SocketAddr,
    session_dir: PathBuf,
    sender: String,
    target: String,
    heartbeat: u32,
    journal: usize,
    tick_us: u64,
    journal_sync_batch: bool,
    journal_batch: u32,
    cluster: bool,
    aeron_dir: Option<PathBuf>,
    ingress_channel: String,
    ingress_stream_id: i32,
    egress_channel: String,
    egress_stream_id: i32,
    message_timeout_ms: i64,
    new_leader_timeout_ms: i64,
    symbol_id: u16,
    symbol: String,
}

impl Options {
    fn parse() -> Result<Self, String> {
        let mut options = Self {
            bind: "127.0.0.1:9876"
                .parse()
                .expect("the default address parses"),
            session_dir: "fix-sessions".into(),
            sender: "DEEPBOOK".to_owned(),
            target: "CLIENT".to_owned(),
            heartbeat: 30,
            journal: 4096,
            tick_us: 100_000,
            journal_sync_batch: false,
            journal_batch: 64,
            cluster: false,
            aeron_dir: None,
            ingress_channel: "aeron:ipc".to_owned(),
            ingress_stream_id: 101,
            egress_channel: "aeron:ipc".to_owned(),
            egress_stream_id: 102,
            message_timeout_ms: 5_000,
            new_leader_timeout_ms: 0,
            symbol_id: 1,
            symbol: "AAPL".to_owned(),
        };

        let arguments: Vec<String> = std::env::args().skip(1).collect();
        let mut index = 0;
        while index < arguments.len() {
            let option = arguments[index].as_str();
            index += 1;
            let mut value = |name: &str| -> Result<String, String> {
                let value = arguments
                    .get(index)
                    .ok_or_else(|| format!("{name} needs a value"))?;
                index += 1;
                Ok(value.clone())
            };
            match option {
                "--bind" => {
                    options.bind = value("--bind")?
                        .parse()
                        .map_err(|_| "--bind must be IP:port".to_owned())?;
                }
                "--session-dir" => options.session_dir = value("--session-dir")?.into(),
                "--sender" => options.sender = value("--sender")?,
                "--target" => options.target = value("--target")?,
                "--heartbeat" => {
                    options.heartbeat = value("--heartbeat")?
                        .parse()
                        .map_err(|_| "--heartbeat must be a number of seconds".to_owned())?;
                }
                "--journal" => {
                    options.journal = value("--journal")?
                        .parse()
                        .map_err(|_| "--journal must be a number".to_owned())?;
                }
                "--tick-us" => {
                    options.tick_us = value("--tick-us")?
                        .parse()
                        .map_err(|_| "--tick-us must be a number".to_owned())?;
                }
                "--journal-sync" => {
                    options.journal_sync_batch = match value("--journal-sync")?.as_str() {
                        "each" => false,
                        "batch" => true,
                        other => {
                            return Err(format!("--journal-sync {other}: expected each|batch"));
                        }
                    };
                }
                "--journal-batch" => {
                    options.journal_batch = value("--journal-batch")?
                        .parse()
                        .map_err(|_| "--journal-batch must be a number".to_owned())?;
                }
                "--cluster" => options.cluster = true,
                "--aeron-dir" => options.aeron_dir = Some(value("--aeron-dir")?.into()),
                "--ingress-channel" => options.ingress_channel = value("--ingress-channel")?,
                "--ingress-stream-id" => {
                    options.ingress_stream_id = value("--ingress-stream-id")?
                        .parse()
                        .map_err(|_| "--ingress-stream-id must be a number".to_owned())?;
                }
                "--egress-channel" => options.egress_channel = value("--egress-channel")?,
                "--egress-stream-id" => {
                    options.egress_stream_id = value("--egress-stream-id")?
                        .parse()
                        .map_err(|_| "--egress-stream-id must be a number".to_owned())?;
                }
                "--message-timeout-ms" => {
                    options.message_timeout_ms = value("--message-timeout-ms")?
                        .parse()
                        .map_err(|_| "--message-timeout-ms must be a number".to_owned())?;
                }
                "--new-leader-timeout-ms" => {
                    options.new_leader_timeout_ms = value("--new-leader-timeout-ms")?
                        .parse()
                        .map_err(|_| "--new-leader-timeout-ms must be a number".to_owned())?;
                }
                "--symbol-id" => {
                    options.symbol_id = value("--symbol-id")?
                        .parse()
                        .map_err(|_| "--symbol-id must be a number".to_owned())?;
                }
                "--symbol" => options.symbol = value("--symbol")?,
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                other => return Err(format!("unknown option {other}")),
            }
        }
        Ok(options)
    }
}

fn print_usage() {
    println!(
        "usage: fix-gateway [--bind IP:PORT] [--session-dir PATH]\n\
         \x20                  [--sender COMPID] [--target COMPID]\n\
         \x20                  [--heartbeat SECONDS] [--journal ENTRIES]\n\
         \x20                  [--tick-us MICROSECONDS]\n\
         \x20                  [--journal-sync each|batch] [--journal-batch N]\n\
         \x20                  [--cluster --aeron-dir PATH]\n\
         \x20                  [--ingress-channel URI] [--ingress-stream-id N]\n\
         \x20                  [--egress-channel URI] [--egress-stream-id N]\n\
         \x20                  [--message-timeout-ms N]\n\
         \x20                  [--new-leader-timeout-ms N]\n\
         \x20                  [--symbol-id N] [--symbol S]"
    );
}
