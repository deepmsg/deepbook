//! `cluster-spike`: the M0 cluster checks.
//!
//! ```text
//! cluster-spike smoke
//! cluster-spike p99 [--rate N] [--samples N] [--warmup N]
//! cluster-spike gate3 [--bids N]
//! cluster-spike gate4 [--bids N]
//! cluster-spike failover
//! ```
//!
//! Common options: `--deepmsg-dir PATH`, `--profile release|debug`,
//! `--base-port N`, `--keep`.

use std::process::ExitCode;

use deepbook_spike::config::Config;
use deepbook_spike::scenarios;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(command) = args.first().map(String::as_str) else {
        usage();
        return ExitCode::FAILURE;
    };

    let mut rate = 50_000_u64;
    let mut samples = 20_000_usize;
    let mut warmup = 1_000_usize;
    let mut bids = 1_000_usize;
    let mut deepmsg_dir: Option<String> = None;
    let mut profile: Option<String> = None;
    let mut base_port: Option<u16> = None;
    let mut keep = false;

    let mut index = 1;
    while index < args.len() {
        match args[index].as_str() {
            "--rate" => rate = take(&args, &mut index, "rate"),
            "--samples" => samples = take(&args, &mut index, "samples"),
            "--warmup" => warmup = take(&args, &mut index, "warmup"),
            "--bids" => bids = take(&args, &mut index, "bids"),
            "--deepmsg-dir" => deepmsg_dir = Some(take(&args, &mut index, "deepmsg-dir")),
            "--profile" => profile = Some(take(&args, &mut index, "profile")),
            "--base-port" => base_port = Some(take(&args, &mut index, "base-port")),
            "--keep" => {
                keep = true;
                index += 1;
            }
            "--help" | "-h" => {
                usage();
                return ExitCode::SUCCESS;
            }
            other => {
                eprintln!("unknown option: {other}");
                usage();
                return ExitCode::FAILURE;
            }
        }
    }

    let mut config = Config::new(command == "gate3");
    config.base_port = base_port.unwrap_or(config.base_port);
    config.keep |= keep;
    if let Some(dir) = deepmsg_dir {
        config.deepmsg_dir = dir.into();
    }
    if let Some(profile) = profile {
        config.profile = profile;
    }

    let result = match command {
        "smoke" => scenarios::smoke::run(config),
        "p99" => scenarios::p99::run(config, rate, samples, warmup),
        "gate3" => scenarios::gate3::run(config, bids),
        "gate4" => scenarios::gate4::run(config, bids),
        "failover" => scenarios::failover::run(config),
        other => {
            eprintln!("unknown command: {other}");
            usage();
            return ExitCode::FAILURE;
        }
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

fn take<T: std::str::FromStr>(args: &[String], index: &mut usize, name: &str) -> T {
    *index += 1;
    let Some(value) = args.get(*index) else {
        eprintln!("--{name} needs a value");
        std::process::exit(2);
    };
    *index += 1;
    value.parse().unwrap_or_else(|_| {
        eprintln!("--{name}: {value:?} is not a value");
        std::process::exit(2);
    })
}

fn usage() {
    eprintln!(
        "usage:\n  cluster-spike smoke\n  cluster-spike p99 [--rate N] [--samples N] [--warmup N]\n  cluster-spike gate3 [--bids N]\n  cluster-spike gate4 [--bids N]\n  cluster-spike failover\n\ncommon: --deepmsg-dir PATH --profile release|debug --base-port N --keep"
    );
}
