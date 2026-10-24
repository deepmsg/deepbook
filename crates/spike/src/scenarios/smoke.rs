//! Start the single-node cluster, send one bid, read the egress, stop.

use std::time::Duration;

use crate::client::{self, BidEgress};
use crate::config::Config;
use crate::process::{Processes, connect_timeout};

/// Run the smoke check.
///
/// # Errors
///
/// The cluster did not start, the bid did not arrive, or the egress did not
/// decode.
pub fn run(config: Config) -> Result<(), String> {
    let processes = Processes::start(config)?;
    let aeron = processes.aeron_dir();
    let mut connected = client::connect(&aeron, connect_timeout())?;

    connected.bid(1, 7, 100)?;
    let egress = connected.wait_for(1, Duration::from_secs(10))?;
    let bid = BidEgress::decode(&egress.payload)
        .ok_or_else(|| format!("a {} byte egress is not a bid record", egress.payload.len()))?;

    println!(
        "smoke: ok correlation={} winner={} best={} succeeded={} run_dir={}",
        bid.correlation_id,
        bid.winner,
        bid.best_price,
        bid.succeeded,
        processes.run_dir().display()
    );
    Ok(())
}
