//! Open-loop `offer -> egress` latency against `basic-auction`.

use std::time::{Duration, Instant};

use crate::client::{self, BidEgress};
use crate::config::Config;
use crate::process::{Processes, connect_timeout};
use crate::report::{self, micros};

/// Run the latency check at `rate` bids per second.
///
/// # Errors
///
/// The cluster did not start, a bid was not offered, or an egress did not
/// arrive.
pub fn run(config: Config, rate: u64, samples: usize, warmup: usize) -> Result<(), String> {
    if rate == 0 || samples == 0 {
        return Err("rate and samples must be non-zero".to_owned());
    }

    let processes = Processes::start(config)?;
    let aeron = processes.aeron_dir();
    let mut connected = client::connect(&aeron, connect_timeout())?;

    let total = warmup
        .checked_add(samples)
        .ok_or_else(|| "warmup + samples overflowed".to_owned())?;
    let interval = Duration::from_nanos(1_000_000_000 / rate);
    let start = Instant::now();
    let mut offered = Vec::with_capacity(total);

    for i in 0..total {
        let due = start
            + interval
                * u32::try_from(i).map_err(|_| format!("sample index {i} does not fit u32"))?;
        while Instant::now() < due {
            connected.poll();
            std::hint::spin_loop();
        }

        let correlation = u64::try_from(i).map_err(|_| "index".to_owned())?;
        let price = 1_000 + i64::try_from(i).map_err(|_| "price".to_owned())?;
        let at = connected.bid(correlation, correlation, price)?;
        offered.push(at);
        connected.poll();
    }

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        connected.poll();
        let complete = {
            let arrivals = connected.arrivals.borrow();
            (warmup..total).all(|i| arrivals.by_correlation.contains_key(&(i as u64)))
        };
        if complete || Instant::now() >= deadline {
            break;
        }
        std::hint::spin_loop();
    }

    let arrivals = connected.arrivals.borrow();
    let mut latencies = Vec::with_capacity(samples);
    for (i, at) in offered.iter().enumerate().take(total).skip(warmup) {
        let correlation = i as u64;
        let egress = arrivals.by_correlation.get(&correlation).ok_or_else(|| {
            format!(
                "no egress for correlation {correlation} ({} arrived)",
                arrivals.messages
            )
        })?;
        // Decode as a sanity check: the egress must be the auction's record.
        BidEgress::decode(&egress.payload)
            .ok_or_else(|| format!("egress {correlation} is not a bid record"))?;
        latencies.push(egress.at.duration_since(*at));
    }
    let stats = report::stats(latencies);

    println!(
        "p99: rate={rate}/s samples={samples} p50={:.1}us p99={:.1}us p999={:.1}us max={:.1}us mean={:.1}us",
        micros(stats.p50),
        micros(stats.p99),
        micros(stats.p999),
        micros(stats.max),
        micros(stats.mean),
    );
    println!("machine: {}", report::machine());
    Ok(())
}
