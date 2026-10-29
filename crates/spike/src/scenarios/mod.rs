//! The M0 checks (smoke, latency, snapshot recovery, leader failover) and
//! the deepbook failover run (P1-3).

pub mod failover;
pub mod gate3;
pub mod gate4;
pub mod p99;
pub mod smoke;
