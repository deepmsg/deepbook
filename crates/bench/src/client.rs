//! The open-loop FIX client.
//!
//! One thread owns the socket: a non-blocking `TcpStream`, a partial-frame
//! decoder and an outbound byte buffer. Each turn drains every complete
//! response, then appends every order whose absolute send deadline has
//! passed, then flushes. The schedule is absolute, so falling behind shows up
//! as queueing time in the next sample rather than as a lower send rate.
//!
//! A `TimeInForce=IOC` buy at a price below the book is acknowledged and then
//! canceled by the engine: it never rests, so a long run does not exhaust the
//! slab. The *first* report for an order is the RTT sample; the second (the
//! IOC cancellation) is ignored.

use std::collections::HashMap;
use std::fmt;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use deepbook_fix::codec::{MessageBuilder, MsgType, ParsedMessage, tag};
use deepbook_fix::frame::{FrameDecoder, FrameError};
use deepbook_fix::message::{ApplicationMessage, decode_application, decode_session};

use crate::report::Report;

/// The default SendingTime/TransactTime. The bench does not measure the
/// gateway's clock validation; a fixed value keeps the message builder out of
/// the timed path and is legal FIX.
const SENDING_TIME: &str = "20261025-12:00:00.000";
/// Stop appending new orders while this much unsent data is queued.
const MAX_OUTBOUND_BYTES: usize = 64 << 20;
/// Keep the sample vector's initial allocation small; it grows as needed.
const SAMPLE_CAPACITY: usize = 1 << 20;
/// Do not spend a whole turn queueing at an unlimited rate.
const MAX_SENDS_PER_TURN: u64 = 4_096;
/// How long to keep reading after the last order was written.
const DRAIN_GRACE: Duration = Duration::from_secs(5);

/// What one run should do.
#[derive(Clone, Debug)]
pub struct BenchConfig {
    /// The gateway's address.
    pub address: SocketAddr,
    /// Our `SenderCompID` (`49`).
    pub sender: String,
    /// The gateway's `TargetCompID` (`56`).
    pub target: String,
    /// `Account(1)`; the cluster gateway requires a decimal id.
    pub account: String,
    /// `Symbol(55)`.
    pub symbol: String,
    /// `Price(44)` as FIX decimal text.
    pub price: String,
    /// `OrderQty(38)`.
    pub quantity: i64,
    /// Ordinal orders per second; `0` means as fast as the socket takes them.
    pub rate_per_second: u64,
    /// Samples to keep (all orders are sent; warmup extra).
    pub samples: u64,
    /// Orders sent before the first kept sample.
    pub warmup: u64,
    /// The whole run's deadline.
    pub timeout: Duration,
}

impl Default for BenchConfig {
    fn default() -> Self {
        Self {
            address: "127.0.0.1:9876"
                .parse()
                .expect("the default address parses"),
            sender: "CLIENT".to_owned(),
            target: "DEEPBOOK".to_owned(),
            account: "1".to_owned(),
            symbol: "AAPL".to_owned(),
            price: "1.00".to_owned(),
            quantity: 1,
            rate_per_second: 10_000,
            samples: 100_000,
            warmup: 5_000,
            timeout: Duration::from_secs(60),
        }
    }
}

/// Why a run stopped early.
#[derive(Debug)]
pub enum ClientError {
    /// A socket operation failed.
    Io(io::Error),
    /// A FIX message could not be built, parsed or answered.
    Fix(String),
    /// The gateway's Logon reply did not arrive.
    LogonTimeout,
    /// The peer closed the socket.
    Closed,
    /// The configuration cannot produce a run.
    Config(String),
}

impl fmt::Display for ClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "{error}"),
            Self::Fix(detail) => write!(formatter, "FIX: {detail}"),
            Self::LogonTimeout => write!(formatter, "no Logon reply before the deadline"),
            Self::Closed => write!(formatter, "the gateway closed the connection"),
            Self::Config(detail) => write!(formatter, "configuration: {detail}"),
        }
    }
}

impl std::error::Error for ClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for ClientError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// The state one connection carries between turns.
struct RunState {
    sequence: u64,
    outbound: Vec<u8>,
    outbound_offset: usize,
    inflight: HashMap<u64, Instant>,
    samples_ns: Vec<u64>,
    completed: u64,
    rejected: u64,
}

/// Connect, log on, drive `samples` orders and return the client-side RTT.
///
/// # Errors
///
/// [`ClientError`] when the socket, the Logon or a FIX frame fails.
pub fn run(config: &BenchConfig) -> Result<Report, ClientError> {
    if config.samples == 0 {
        return Err(ClientError::Config("samples must be positive".to_owned()));
    }
    let total = config
        .samples
        .checked_add(config.warmup)
        .ok_or_else(|| ClientError::Config("samples + warmup overflows".to_owned()))?;

    let start_connect = Instant::now();
    let mut stream = TcpStream::connect_timeout(&config.address, Duration::from_secs(5))?;
    stream.set_nodelay(true)?;
    let mut state = RunState {
        sequence: 1,
        outbound: Vec::with_capacity(1 << 20),
        outbound_offset: 0,
        inflight: HashMap::new(),
        samples_ns: Vec::with_capacity(
            usize::try_from(config.samples)
                .unwrap_or(usize::MAX)
                .min(SAMPLE_CAPACITY),
        ),
        completed: 0,
        rejected: 0,
    };
    logon(&mut stream, config, &mut state.sequence, start_connect)?;
    stream.set_nonblocking(true)?;

    let interval = (config.rate_per_second > 0)
        .then(|| Duration::from_nanos(1_000_000_000_u64.saturating_div(config.rate_per_second)));
    let start = Instant::now();
    let deadline = start + config.timeout;
    let mut next_due = start;
    let mut decoder = FrameDecoder::new();
    let mut read_buffer = [0_u8; 64 * 1024];
    let mut sent = 0_u64;
    let mut drain_until: Option<Instant> = None;

    loop {
        // Every complete report the socket has.
        loop {
            match stream.read(&mut read_buffer) {
                Ok(0) => return Err(ClientError::Closed),
                Ok(count) => decoder.push(&read_buffer[..count]),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(ClientError::Io(error)),
            }
            while let Some(frame) = decoder.next_frame() {
                let frame = frame.map_err(frame_error)?;
                on_frame(&frame, &mut state, config)?;
            }
        }

        // Every order whose absolute deadline has passed.
        if sent < total {
            let now = Instant::now();
            let mut sends = 0_u64;
            if let Some(interval) = interval {
                while sent < total && now >= next_due && sends < MAX_SENDS_PER_TURN {
                    queue_order(sent, &mut state, config);
                    sent += 1;
                    sends += 1;
                    next_due += interval;
                }
            } else {
                while sent < total
                    && sends < MAX_SENDS_PER_TURN
                    && state.outbound.len() - state.outbound_offset < MAX_OUTBOUND_BYTES
                {
                    queue_order(sent, &mut state, config);
                    sent += 1;
                    sends += 1;
                }
            }
        }

        // Write what the socket takes.
        while state.outbound_offset < state.outbound.len() {
            match stream.write(&state.outbound[state.outbound_offset..]) {
                Ok(0) => return Err(ClientError::Closed),
                Ok(count) => state.outbound_offset += count,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(ClientError::Io(error)),
            }
        }
        if state.outbound_offset == state.outbound.len() {
            state.outbound.clear();
            state.outbound_offset = 0;
        }

        let now = Instant::now();
        if sent == total {
            if state.inflight.is_empty() {
                break;
            }
            let until_drain = *drain_until.get_or_insert(now + DRAIN_GRACE);
            if now >= until_drain {
                break;
            }
        }
        if now >= deadline {
            break;
        }

        if interval.is_some() {
            let now = Instant::now();
            if next_due > now {
                let wait = (next_due - now).min(Duration::from_micros(50));
                std::thread::sleep(wait);
            }
        }
    }

    Ok(Report {
        sent,
        completed: state.completed,
        rejected: state.rejected,
        unanswered: u64::try_from(state.inflight.len()).unwrap_or(u64::MAX),
        elapsed: start.elapsed(),
        samples_ns: state.samples_ns,
    })
}

fn frame_error(error: FrameError) -> ClientError {
    ClientError::Fix(error.to_string())
}

/// Build one IOC buy and append it, timing the order at the append.
fn queue_order(index: u64, state: &mut RunState, config: &BenchConfig) {
    let cl_ord_id = format!("B{index}");
    let message = new_order(state.sequence, &cl_ord_id, config);
    state.sequence += 1;
    let queued_at = Instant::now();
    if index >= config.warmup {
        state.inflight.insert(index, queued_at);
    }
    state.outbound.extend_from_slice(&message);
}

/// Take one report, a TestRequest or an admin message.
fn on_frame(
    frame: &ParsedMessage,
    state: &mut RunState,
    config: &BenchConfig,
) -> Result<(), ClientError> {
    let msg_type = frame
        .msg_type()
        .map_err(|error| ClientError::Fix(error.to_string()))?;
    match msg_type {
        MsgType::TestRequest => {
            let test_req_id = frame
                .get_str(tag::TEST_REQ_ID)
                .map_err(|error| ClientError::Fix(error.to_string()))?;
            let reply = heartbeat(state.sequence, test_req_id, config);
            state.sequence += 1;
            state.outbound.extend_from_slice(&reply);
            return Ok(());
        }
        MsgType::Logout => return Err(ClientError::Closed),
        _ if msg_type.is_admin() => return Ok(()),
        _ => {}
    }
    let ApplicationMessage::ExecutionReport(report) =
        decode_application(frame).map_err(|error| ClientError::Fix(error.to_string()))?
    else {
        return Ok(());
    };
    let Some(index) = order_index(&report.cl_ord_id) else {
        return Ok(());
    };
    let Some(queued_at) = state.inflight.remove(&index) else {
        // The IOC cancellation, or an order from the warmup.
        return Ok(());
    };
    state.completed += 1;
    if report.exec_type == deepbook_fix::ExecType::Rejected {
        state.rejected += 1;
    }
    if index >= config.warmup {
        let nanos = queued_at.elapsed().as_nanos();
        state
            .samples_ns
            .push(u64::try_from(nanos).unwrap_or(u64::MAX));
    }
    Ok(())
}

fn order_index(cl_ord_id: &str) -> Option<u64> {
    cl_ord_id.strip_prefix('B')?.parse().ok()
}

fn logon(
    stream: &mut TcpStream,
    config: &BenchConfig,
    sequence: &mut u64,
    started: Instant,
) -> Result<(), ClientError> {
    let message = MessageBuilder::new(
        MsgType::Logon,
        &config.sender,
        &config.target,
        *sequence,
        SENDING_TIME,
    )
    .body(tag::ENCRYPT_METHOD, b"0".to_vec())
    .body_int(tag::HEART_BT_INT, 30_u32)
    .build();
    *sequence += 1;
    stream.write_all(&message)?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;

    let mut decoder = FrameDecoder::new();
    let mut buffer = [0_u8; 4_096];
    let deadline = started + Duration::from_secs(5);
    loop {
        while let Some(frame) = decoder.next_frame() {
            let frame = frame.map_err(frame_error)?;
            let message_type = frame
                .msg_type()
                .map_err(|error| ClientError::Fix(error.to_string()))?;
            if message_type == MsgType::Logon {
                return Ok(());
            }
            if message_type == MsgType::TestRequest {
                let test_req_id = frame
                    .get_str(tag::TEST_REQ_ID)
                    .map_err(|error| ClientError::Fix(error.to_string()))?;
                let reply = heartbeat(*sequence, test_req_id, config);
                *sequence += 1;
                stream.write_all(&reply)?;
            }
            if message_type == MsgType::Logout || message_type == MsgType::Reject {
                let detail = decode_session(&frame)
                    .map_or_else(|error| error.to_string(), |message| format!("{message:?}"));
                return Err(ClientError::Fix(format!(
                    "the gateway refused the Logon: {detail}"
                )));
            }
        }
        if Instant::now() >= deadline {
            return Err(ClientError::LogonTimeout);
        }
        match stream.read(&mut buffer) {
            Ok(0) => return Err(ClientError::Closed),
            Ok(count) => decoder.push(&buffer[..count]),
            Err(error)
                if error.kind() == io::ErrorKind::WouldBlock
                    || error.kind() == io::ErrorKind::TimedOut => {}
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(ClientError::Io(error)),
        }
    }
}

fn new_order(sequence: u64, cl_ord_id: &str, config: &BenchConfig) -> Vec<u8> {
    MessageBuilder::new(
        MsgType::NewOrderSingle,
        &config.sender,
        &config.target,
        sequence,
        SENDING_TIME,
    )
    .body_str(tag::CL_ORD_ID, cl_ord_id)
    .body_str(tag::ACCOUNT, &config.account)
    .body_str(tag::SYMBOL, &config.symbol)
    .body(tag::SIDE, b"1".to_vec())
    .body_int(tag::ORDER_QTY, config.quantity)
    .body(tag::ORD_TYPE, b"2".to_vec())
    .body_str(tag::PRICE, &config.price)
    // IOC: the engine cancels an unfilled remainder instead of resting it.
    .body(tag::TIME_IN_FORCE, b"3".to_vec())
    .body_str(tag::TRANSACT_TIME, SENDING_TIME)
    .build()
}

fn heartbeat(sequence: u64, test_req_id: &str, config: &BenchConfig) -> Vec<u8> {
    MessageBuilder::new(
        MsgType::Heartbeat,
        &config.sender,
        &config.target,
        sequence,
        SENDING_TIME,
    )
    .body_str(tag::TEST_REQ_ID, test_req_id)
    .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> BenchConfig {
        BenchConfig {
            samples: 4,
            warmup: 1,
            rate_per_second: 1_000_000,
            timeout: Duration::from_secs(5),
            ..BenchConfig::default()
        }
    }

    #[test]
    fn an_order_carries_the_bench_identifiers() {
        let message = new_order(7, "B42", &config());
        let parsed = deepbook_fix::codec::parse(&message).expect("parses");
        assert_eq!(MsgType::NewOrderSingle, parsed.msg_type().expect("type"));
        assert_eq!(Some("B42".as_bytes()), parsed.get(tag::CL_ORD_ID));
        assert_eq!(Some(b"3".as_slice()), parsed.get(tag::TIME_IN_FORCE));
        assert_eq!(Some(b"1".as_slice()), parsed.get(tag::SIDE));
    }

    #[test]
    fn a_client_order_id_maps_back_to_its_ordinal() {
        assert_eq!(Some(42), order_index("B42"));
        assert_eq!(Some(0), order_index("B0"));
        assert_eq!(None, order_index("C42"));
        assert_eq!(None, order_index("B"));
        assert_eq!(None, order_index("B-1"));
    }
}
