//! `fix-bench`: the M5 open-loop FIX client.
//!
//! ```text
//! fix-bench [--connect 127.0.0.1:9876] [--rate 10000] [--samples 100000]
//!           [--warmup 5000] [--account 1] [--price 1.00] [--qty 1]
//!           [--symbol AAPL] [--sender CLIENT] [--target DEEPBOOK]
//!           [--timeout-ms 60000] [--json]
//! ```
//!
//! It prints the client-side RTT table (`--json` for one JSON object). The
//! numbers are send-queue to first-report; see `deepbook_bench::client`.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use deepbook_bench::{BenchConfig, run};

fn main() -> ExitCode {
    let (config, json, prometheus) = match parse() {
        Ok(options) => options,
        Err(error) => {
            eprintln!("fix-bench: {error}");
            return ExitCode::FAILURE;
        }
    };
    match run(&config) {
        Ok(report) => {
            if json {
                println!("{}", report.to_json());
            } else {
                print!("{}", report.to_table());
            }
            if let Some(path) = &prometheus {
                if let Err(error) = write_prometheus(path, &report) {
                    eprintln!("fix-bench: {error}");
                    return ExitCode::FAILURE;
                }
            }
            if report.completed == 0 {
                eprintln!("fix-bench: no report was received");
                ExitCode::FAILURE
            } else {
                ExitCode::SUCCESS
            }
        }
        Err(error) => {
            eprintln!("fix-bench: {error}");
            ExitCode::FAILURE
        }
    }
}

fn write_prometheus(path: &PathBuf, report: &deepbook_bench::Report) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs());
    std::fs::write(path, report.to_prometheus(timestamp))
        .map_err(|error| format!("{}: {error}", path.display()))
}

fn parse() -> Result<(BenchConfig, bool, Option<PathBuf>), String> {
    let mut config = BenchConfig::default();
    let mut json = false;
    let mut prometheus = None;
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
            "--connect" => {
                config.address = value("--connect")?
                    .parse()
                    .map_err(|_| "--connect must be IP:PORT".to_owned())?;
            }
            "--sender" => config.sender = value("--sender")?,
            "--target" => config.target = value("--target")?,
            "--account" => config.account = value("--account")?,
            "--symbol" => config.symbol = value("--symbol")?,
            "--price" => config.price = value("--price")?,
            "--qty" => {
                config.quantity = value("--qty")?
                    .parse()
                    .map_err(|_| "--qty must be a number".to_owned())?;
            }
            "--rate" => {
                config.rate_per_second = value("--rate")?
                    .parse()
                    .map_err(|_| "--rate must be a number".to_owned())?;
            }
            "--samples" => {
                config.samples = value("--samples")?
                    .parse()
                    .map_err(|_| "--samples must be a number".to_owned())?;
            }
            "--warmup" => {
                config.warmup = value("--warmup")?
                    .parse()
                    .map_err(|_| "--warmup must be a number".to_owned())?;
            }
            "--timeout-ms" => {
                let millis: u64 = value("--timeout-ms")?
                    .parse()
                    .map_err(|_| "--timeout-ms must be a number".to_owned())?;
                config.timeout = Duration::from_millis(millis);
            }
            "--json" => json = true,
            "--seed-book" => config.seed_book = true,
            "--prometheus" => prometheus = Some(value("--prometheus")?.into()),
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            other => return Err(format!("unknown option {other}")),
        }
    }
    if config.samples == 0 {
        return Err("--samples must be positive".to_owned());
    }
    Ok((config, json, prometheus))
}

fn print_usage() {
    println!(
        "usage: fix-bench [--connect IP:PORT] [--rate N] [--samples N]\n\
         \x20                [--warmup N] [--account ID] [--symbol S]\n\
         \x20                [--price FIX_DECIMAL] [--qty N]\n\
         \x20                [--sender COMPID] [--target COMPID]\n\
         \x20                [--timeout-ms N] [--json] [--seed-book]\n\
         \x20                [--prometheus PATH]\n\
         \n\
         Orders are IOC buys at --price; only the first report is an RTT sample.\n\
         --seed-book sends four resting levels and two trades before the run."
    );
}
