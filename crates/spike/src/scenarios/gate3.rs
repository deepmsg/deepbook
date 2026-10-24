//! M0 gate 3: snapshot, `kill -9`, restart, state restored.
//!
//! `basic-auction` is the state machine: it keeps the best price and the
//! winning customer, writes them to its snapshot, and reports them in every
//! egress. The check is therefore observable from the client:
//!
//! 1. raise the best price with a run of bids;
//! 2. ask the operator tool for a snapshot and wait for counter `205`;
//! 3. `SIGKILL` every process;
//! 4. restart on the same archive and cluster directories;
//! 5. send a **lower** bid: it must lose, and its egress must report the
//!    pre-crash best price and winner.

use std::time::Duration;

use crate::client::{self, BidEgress};
use crate::config::Config;
use crate::process::{Processes, connect_timeout};

/// Run the recovery check with `bids` increasing bids before the snapshot.
///
/// # Errors
///
/// The cluster did not start, a bid or its egress failed, the snapshot was not
/// taken, or the recovered state differs from the pre-crash state.
pub fn run(config: Config, bids: usize) -> Result<(), String> {
    if bids == 0 {
        return Err("bids must be non-zero".to_owned());
    }
    let mut config = config;
    // The snapshot has to survive a SIGKILL; without a sync level the archive
    // is allowed to lose it and the test would be measuring the log replay.
    config.durable = true;

    let mut processes = Processes::start(config)?;
    let aeron = processes.aeron_dir();
    let mut connected = client::connect(&aeron, connect_timeout())?;

    let mut before = None;
    for i in 0..bids {
        let correlation = u64::try_from(i).map_err(|_| "bid index".to_owned())?;
        let price = 100 + i64::try_from(i).map_err(|_| "price".to_owned())?;
        connected.bid(correlation, correlation, price)?;
        let egress = connected.wait_for(correlation, Duration::from_secs(5))?;
        before = Some(
            BidEgress::decode(&egress.payload)
                .ok_or_else(|| format!("egress {correlation} is not a bid record"))?,
        );
    }
    let before = before.ok_or_else(|| "no bid produced an egress".to_owned())?;
    if !before.succeeded {
        return Err(format!("the last pre-snapshot bid did not win: {before:?}"));
    }
    println!(
        "gate3: before snapshot winner={} best={}",
        before.winner, before.best_price
    );

    processes.snapshot()?;
    let count = processes.wait_for_snapshot(Duration::from_secs(20))?;
    println!("gate3: snapshot count={count}");
    // The archive is at sync level 2; give the recording a moment to finish
    // before the process disappears.
    std::thread::sleep(Duration::from_millis(200));

    // Drop the old session before its driver is killed; it cannot be reused.
    drop(connected);
    processes.restart()?;

    let aeron = processes.aeron_dir();
    let mut connected = client::connect(&aeron, connect_timeout())?;

    let correlation = u64::try_from(bids).map_err(|_| "bids".to_owned())? + 1_000_000;
    connected.bid(correlation, 9_999, 1)?;
    let egress = connected.wait_for(correlation, Duration::from_secs(10))?;
    let after = BidEgress::decode(&egress.payload)
        .ok_or_else(|| "the post-recovery egress is not a bid record".to_owned())?;

    println!(
        "gate3: after restart winner={} best={} succeeded={}",
        after.winner, after.best_price, after.succeeded
    );

    if after.succeeded {
        return Err(format!(
            "a bid at price 1 won after recovery; the state was not restored: {after:?}"
        ));
    }
    if after.best_price != before.best_price || after.winner != before.winner {
        return Err(format!(
            "state mismatch: before=(winner={}, best={}), after=(winner={}, best={})",
            before.winner, before.best_price, after.winner, after.best_price
        ));
    }

    println!(
        "gate3: ok state restored (winner={}, best={})",
        after.winner, after.best_price
    );
    Ok(())
}
