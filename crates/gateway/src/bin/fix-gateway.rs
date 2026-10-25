//! `fix-gateway`: the M1 acceptor with the in-process stub engine.
//!
//! ```text
//! fix-gateway [--bind 127.0.0.1:9876] [--session-dir fix-sessions]
//!             [--sender DEEPBOOK] [--target CLIENT]
//!             [--heartbeat 30] [--journal 4096]
//! ```
//!
//! It exists so a FIX initiator (M1d's QuickFIX/J harness) has a process to
//! connect to. M4 swaps `StubEngine` for the cluster adapter; the command line
//! and the session directories stay.

use std::net::SocketAddr;
use std::process::ExitCode;

use deepbook_gateway::{Gateway, GatewayConfig, StubEngine};

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
    let config = GatewayConfig::new(
        options.bind,
        options.session_dir.clone(),
        options.sender.clone(),
        options.target.clone(),
    );
    let mut config = config;
    config.heart_bt_int = options.heartbeat;
    config.journal_capacity = options.journal;

    let mut gateway = Gateway::bind(config, StubEngine::new())
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
    session_dir: std::path::PathBuf,
    sender: String,
    target: String,
    heartbeat: u32,
    journal: usize,
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
         \x20                  [--heartbeat SECONDS] [--journal ENTRIES]"
    );
}
