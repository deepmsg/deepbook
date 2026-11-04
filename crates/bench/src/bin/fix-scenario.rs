//! `fix-scenario`: replay a deterministic order script and check every fill.
//!
//! `fix-bench` answers "how fast". It sends IOC buys at a price the book does
//! not cross, so it produces **no trades at all** -- its own module comment
//! says so. Every performance number this project has is therefore a number
//! about the pipe, never about the matcher.
//!
//! This binary answers the other question: *did the matching come out right,
//! and does a leader change change it?* It reads a script in which every line
//! carries the outcome the engine must produce, replays it through the
//! gateway, and fails on the first line that disagrees.
//!
//! ```text
//! fix-scenario [--connect 127.0.0.1:9876] [--script FILE] [--account 1]
//!              [--symbol AAPL] [--sender CLIENT] [--target DEEPBOOK]
//!              [--order-timeout-ms 10000] [--pace-ms 0]
//! ```
//!
//! # The script
//!
//! Whitespace separated, `#` starts a comment:
//!
//! ```text
//! <cl_ord_id> <side> <qty> <price> <tif> <expect> <cum> <px|-
//! ```
//!
//! | field | meaning |
//! |---|---|
//! | `side` | `1` buy, `2` sell |
//! | `tif` | `0` Day, `3` IOC |
//! | `expect` | `NEW` (rests), `FILLED`, `CANCELED`, `REJECTED` |
//! | `cum` | cumulative filled quantity when `expect` is reached |
//! | `px` | the fill prices this order must produce, `+`-separated, or `-` |
//!
//! `NEW` is a terminal expectation as well: a resting order is acknowledged
//! once and nothing else follows it.
//!
//! The script is produced by a model of the book (`gen-scenario.py`), so a
//! disagreement is either the engine or the model -- which is the point: the
//! four-host run then adds the one thing the model cannot check, that the
//! answer is the same after a leader dies.

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::process::ExitCode;
use std::time::{Duration, Instant};

use deepbook_fix::codec::{MessageBuilder, MsgType, ParsedMessage, tag};
use deepbook_fix::frame::{FrameDecoder, FrameError};
use deepbook_fix::message::{
    ApplicationMessage, OrdStatus, decode_application, decode_session, parse_price,
};

/// A fixed SendingTime/TransactTime. The gateway validates presence, not the
/// clock, and a constant keeps the message builder out of anything timed.
const SENDING_TIME: &str = "20261025-12:00:00.000";

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Expect {
    New,
    Filled,
    Canceled,
    Rejected,
}

impl Expect {
    fn parse(word: &str) -> Result<Self, String> {
        match word {
            "NEW" => Ok(Self::New),
            "FILLED" => Ok(Self::Filled),
            "CANCELED" => Ok(Self::Canceled),
            "REJECTED" => Ok(Self::Rejected),
            other => Err(format!("unknown expectation '{other}'")),
        }
    }

    fn reached(self, status: OrdStatus) -> bool {
        matches!(
            (self, status),
            (Self::New, OrdStatus::New)
                | (Self::Filled, OrdStatus::Filled)
                | (Self::Canceled, OrdStatus::Canceled)
                | (Self::Rejected, OrdStatus::Rejected)
        )
    }

    fn name(self) -> &'static str {
        match self {
            Self::New => "NEW",
            Self::Filled => "FILLED",
            Self::Canceled => "CANCELED",
            Self::Rejected => "REJECTED",
        }
    }
}

struct Step {
    id: String,
    side: &'static str,
    qty: i64,
    price: String,
    tif: &'static str,
    expect: Expect,
    expect_cum: i64,
    expect_px: Vec<i64>,
    line_number: usize,
}

struct Seen {
    status: OrdStatus,
    cum: i64,
    fills: Vec<i64>,
    reports: usize,
    late: usize,
    done: bool,
}

impl Default for Seen {
    fn default() -> Self {
        Self {
            status: OrdStatus::New,
            cum: 0,
            fills: Vec::new(),
            reports: 0,
            late: 0,
            done: false,
        }
    }
}

struct Options {
    address: SocketAddr,
    script: String,
    account: String,
    symbol: String,
    sender: String,
    target: String,
    order_timeout: Duration,
    pace: Duration,
    trace: bool,
}

fn main() -> ExitCode {
    let options = match parse() {
        Ok(options) => options,
        Err(error) => {
            eprintln!("fix-scenario: {error}");
            return ExitCode::FAILURE;
        }
    };
    let steps = match read_script(&options.script) {
        Ok(steps) => steps,
        Err(error) => {
            eprintln!("fix-scenario: {error}");
            return ExitCode::FAILURE;
        }
    };
    match run(&options, &steps) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("fix-scenario: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run(options: &Options, steps: &[Step]) -> Result<(), String> {
    let mut stream = TcpStream::connect_timeout(&options.address, Duration::from_secs(5))
        .map_err(|error| format!("connect {}: {error}", options.address))?;
    stream
        .set_nodelay(true)
        .map_err(|error| format!("nodelay: {error}"))?;
    let mut sequence = 1_u64;
    logon(&mut stream, options, &mut sequence)?;

    let mut decoder = FrameDecoder::new();
    let mut buffer = vec![0_u8; 64 * 1024];
    let started = Instant::now();
    let mut fills_total = 0_u64;
    let mut late_total = 0_u64;

    for step in steps {
        let message = new_order(sequence, step, options);
        sequence += 1;
        stream
            .write_all(&message)
            .map_err(|error| format!("write {}: {error}", step.id))?;

        let mut seen = Seen::default();
        let deadline = Instant::now() + options.order_timeout;
        pump_until(
            &mut stream,
            &mut decoder,
            &mut buffer,
            &mut sequence,
            options,
            &step.id,
            step.expect,
            deadline,
            &mut seen,
        )?;
        if !seen.done {
            return Err(format!(
                "line {}: order {} never reached {} (last status {:?}, {} reports)",
                step.line_number,
                step.id,
                step.expect.name(),
                seen.status,
                seen.reports
            ));
        }
        if seen.cum != step.expect_cum {
            return Err(format!(
                "line {}: order {} expected cum {} got {}",
                step.line_number, step.id, step.expect_cum, seen.cum
            ));
        }
        if seen.fills != step.expect_px {
            return Err(format!(
                "line {}: order {} expected fills {:?} got {:?}",
                step.line_number, step.id, step.expect_px, seen.fills
            ));
        }
        fills_total += u64::try_from(seen.fills.len()).unwrap_or(0);
        late_total += u64::try_from(seen.late).unwrap_or(0);
        if !options.pace.is_zero() {
            std::thread::sleep(options.pace);
        }
    }

    // Every trade has two sides. The taker's report is the one this order
    // waited for; the maker's arrives afterwards, addressed to a resting order
    // this client has already finished with -- those are the "late" reports.
    // They must be one for one: if the engine stopped reporting the maker, the
    // fills would still look right from the taker's side and the book would
    // silently disagree with what the script asked for.
    if late_total != fills_total {
        return Err(format!(
            "{fills_total} fills but {late_total} maker reports: the two sides of a trade disagree"
        ));
    }
    let elapsed = started.elapsed();
    println!(
        "scenario ok: {} orders, {} fills, {} maker reports, {:.3}s",
        steps.len(),
        fills_total,
        late_total,
        elapsed.as_secs_f64()
    );
    // A Logout is polite and keeps the gateway's session bookkeeping clean for
    // the next run.
    let logout = MessageBuilder::new(
        MsgType::Logout,
        &options.sender,
        &options.target,
        sequence,
        SENDING_TIME,
    )
    .build();
    let _ = stream.write_all(&logout);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn pump_until(
    stream: &mut TcpStream,
    decoder: &mut FrameDecoder,
    buffer: &mut [u8],
    sequence: &mut u64,
    options: &Options,
    id: &str,
    expect: Expect,
    deadline: Instant,
    seen: &mut Seen,
) -> Result<(), String> {
    stream
        .set_read_timeout(Some(Duration::from_millis(50)))
        .map_err(|error| format!("read timeout: {error}"))?;
    loop {
        if seen.done {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Ok(());
        }
        match stream.read(buffer) {
            Ok(0) => return Err("the gateway closed the session".to_owned()),
            Ok(count) => decoder.push(&buffer[..count]),
            Err(error)
                if error.kind() == io::ErrorKind::WouldBlock
                    || error.kind() == io::ErrorKind::TimedOut => {}
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(format!("read: {error}")),
        }
        while let Some(frame) = decoder.next_frame() {
            let frame = frame.map_err(frame_error)?;
            handle(&frame, stream, sequence, options, id, expect, seen)?;
        }
    }
}

fn handle(
    frame: &ParsedMessage,
    stream: &mut TcpStream,
    sequence: &mut u64,
    options: &Options,
    id: &str,
    expect: Expect,
    seen: &mut Seen,
) -> Result<(), String> {
    let message_type = frame
        .msg_type()
        .map_err(|error| format!("msg_type: {error}"))?;
    match message_type {
        MsgType::TestRequest => {
            let test_req_id = frame
                .get_str(tag::TEST_REQ_ID)
                .map_err(|error| format!("test req id: {error}"))?;
            let reply = heartbeat(*sequence, test_req_id, options);
            *sequence += 1;
            stream
                .write_all(&reply)
                .map_err(|error| format!("heartbeat write: {error}"))?;
            return Ok(());
        }
        MsgType::Logout => return Err("the gateway logged the session out".to_owned()),
        _ if message_type.is_admin() => return Ok(()),
        _ => {}
    }
    let ApplicationMessage::ExecutionReport(report) =
        decode_application(frame).map_err(|error| format!("decode: {error}"))?
    else {
        return Ok(());
    };
    if report.cl_ord_id != id {
        // A report for a previous order arriving after this client had already
        // seen that order's terminal report. Ignored rather than fatal: an
        // order's outcome is decided by the report that made it terminal, and
        // the run reports how many of these there were so a burst is visible.
        seen.late += 1;
        return Ok(());
    }
    if options.trace {
        eprintln!(
            "trace: {} exec={:?} status={:?} last_qty={:?} last_px={:?} cum={} leaves={}",
            report.cl_ord_id,
            report.exec_type,
            report.ord_status,
            report.last_qty,
            report.last_px,
            report.cum_qty,
            report.leaves_qty
        );
    }
    seen.reports += 1;
    seen.status = report.ord_status;
    seen.cum = report.cum_qty;
    if let Some(px) = report.last_px {
        seen.fills.push(px);
    }
    // A resting order stops at its acknowledgement; a crossing one runs on
    // to its fill. Which one is terminal is the script's statement, not this
    // client's guess.
    seen.done = expect.reached(seen.status);
    Ok(())
}

fn logon(stream: &mut TcpStream, options: &Options, sequence: &mut u64) -> Result<(), String> {
    let message = MessageBuilder::new(
        MsgType::Logon,
        &options.sender,
        &options.target,
        *sequence,
        SENDING_TIME,
    )
    .body(tag::ENCRYPT_METHOD, b"0".to_vec())
    .body_int(tag::HEART_BT_INT, 30_u32)
    .body(tag::RESET_SEQ_NUM_FLAG, b"Y".to_vec())
    .build();
    *sequence += 1;
    stream
        .write_all(&message)
        .map_err(|error| format!("logon write: {error}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .map_err(|error| format!("read timeout: {error}"))?;
    let mut decoder = FrameDecoder::new();
    let mut buffer = [0_u8; 4_096];
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        while let Some(frame) = decoder.next_frame() {
            let frame = frame.map_err(frame_error)?;
            let message_type = frame
                .msg_type()
                .map_err(|error| format!("msg_type: {error}"))?;
            if message_type == MsgType::Logon {
                return Ok(());
            }
            if message_type == MsgType::Logout || message_type == MsgType::Reject {
                let detail = decode_session(&frame)
                    .map_or_else(|error| error.to_string(), |message| format!("{message:?}"));
                return Err(format!("the gateway refused the Logon: {detail}"));
            }
        }
        if Instant::now() >= deadline {
            return Err("the gateway never answered the Logon".to_owned());
        }
        match stream.read(&mut buffer) {
            Ok(0) => return Err("the gateway closed the session during the Logon".to_owned()),
            Ok(count) => decoder.push(&buffer[..count]),
            Err(error)
                if error.kind() == io::ErrorKind::WouldBlock
                    || error.kind() == io::ErrorKind::TimedOut => {}
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(format!("logon read: {error}")),
        }
    }
}

fn new_order(sequence: u64, step: &Step, options: &Options) -> Vec<u8> {
    MessageBuilder::new(
        MsgType::NewOrderSingle,
        &options.sender,
        &options.target,
        sequence,
        SENDING_TIME,
    )
    .body_str(tag::CL_ORD_ID, &step.id)
    .body_str(tag::ACCOUNT, &options.account)
    .body_str(tag::SYMBOL, &options.symbol)
    .body(tag::SIDE, step.side.as_bytes().to_vec())
    .body_int(tag::ORDER_QTY, step.qty)
    .body(tag::ORD_TYPE, b"2".to_vec())
    .body_str(tag::PRICE, &step.price)
    .body(tag::TIME_IN_FORCE, step.tif.as_bytes().to_vec())
    .body_str(tag::TRANSACT_TIME, SENDING_TIME)
    .build()
}

fn heartbeat(sequence: u64, test_req_id: &str, options: &Options) -> Vec<u8> {
    MessageBuilder::new(
        MsgType::Heartbeat,
        &options.sender,
        &options.target,
        sequence,
        SENDING_TIME,
    )
    .body_str(tag::TEST_REQ_ID, test_req_id)
    .build()
}

fn frame_error(error: FrameError) -> String {
    format!("frame: {error}")
}

fn read_script(path: &str) -> Result<Vec<Step>, String> {
    let text = std::fs::read_to_string(path).map_err(|error| format!("{path}: {error}"))?;
    let mut steps = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let line_number = index + 1;
        let line = line.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() != 8 {
            return Err(format!(
                "line {line_number}: expected 8 fields, found {}",
                fields.len()
            ));
        }
        let side = match fields[1] {
            "1" => "1",
            "2" => "2",
            other => return Err(format!("line {line_number}: side '{other}' is not 1 or 2")),
        };
        let qty: i64 = fields[2]
            .parse()
            .map_err(|_| format!("line {line_number}: qty '{}'", fields[2]))?;
        let tif = match fields[4] {
            "0" => "0",
            "3" => "3",
            other => return Err(format!("line {line_number}: tif '{other}' is not 0 or 3")),
        };
        let expect = Expect::parse(fields[5]).map_err(|e| format!("line {line_number}: {e}"))?;
        let expect_cum: i64 = fields[6]
            .parse()
            .map_err(|_| format!("line {line_number}: cum '{}'", fields[6]))?;
        let expect_px = if fields[7] == "-" {
            Vec::new()
        } else {
            fields[7]
                .split('+')
                .map(|price| {
                    parse_price(price).map_err(|error| {
                        format!("line {line_number}: price '{}': {error}", fields[7])
                    })
                })
                .collect::<Result<Vec<_>, _>>()?
        };
        steps.push(Step {
            id: fields[0].to_owned(),
            side,
            qty,
            price: fields[3].to_owned(),
            tif,
            expect,
            expect_cum,
            expect_px,
            line_number,
        });
    }
    if steps.is_empty() {
        return Err(format!("{path}: no steps"));
    }
    Ok(steps)
}

fn parse() -> Result<Options, String> {
    let mut options = Options {
        address: "127.0.0.1:9876"
            .parse()
            .map_err(|_| "the default address parses".to_owned())?,
        script: String::new(),
        account: "1".to_owned(),
        symbol: "AAPL".to_owned(),
        sender: "CLIENT".to_owned(),
        target: "DEEPBOOK".to_owned(),
        order_timeout: Duration::from_secs(10),
        pace: Duration::ZERO,
        trace: false,
    };
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut index = 0;
    while index < args.len() {
        let name = args[index].clone();
        let mut next = |name: &str| -> Result<String, String> {
            index += 1;
            args.get(index)
                .cloned()
                .ok_or_else(|| format!("{name} needs a value"))
        };
        match name.as_str() {
            "--connect" => {
                let address = next("--connect")?;
                options.address = address
                    .to_socket_addrs()
                    .map_err(|error| format!("{address}: {error}"))?
                    .next()
                    .ok_or_else(|| format!("{address}: no address"))?;
            }
            "--script" => options.script = next("--script")?,
            "--account" => options.account = next("--account")?,
            "--symbol" => options.symbol = next("--symbol")?,
            "--sender" => options.sender = next("--sender")?,
            "--target" => options.target = next("--target")?,
            "--order-timeout-ms" => {
                let value = next("--order-timeout-ms")?;
                let ms: u64 = value
                    .parse()
                    .map_err(|_| "--order-timeout-ms must be a number".to_owned())?;
                options.order_timeout = Duration::from_millis(ms);
            }
            "--pace-ms" => {
                let value = next("--pace-ms")?;
                let ms: u64 = value
                    .parse()
                    .map_err(|_| "--pace-ms must be a number".to_owned())?;
                options.pace = Duration::from_millis(ms);
            }
            "--trace" => options.trace = true,
            "--help" | "-h" => return Err(usage()),
            other => return Err(format!("unknown option '{other}'\n{}", usage())),
        }
        index += 1;
    }
    if options.script.is_empty() {
        return Err(format!("--script is required\n{}", usage()));
    }
    Ok(options)
}

fn usage() -> String {
    "usage: fix-scenario --script FILE [--connect IP:PORT] [--account N] [--symbol S]\n\
     \x20                [--sender CLIENT] [--target DEEPBOOK]\n\
     \x20                [--order-timeout-ms 10000] [--pace-ms 0]\n\
     \x20\n\
     \x20 Script lines: <id> <side> <qty> <price> <tif> <expect> <cum> <px|->\n"
        .to_owned()
}
