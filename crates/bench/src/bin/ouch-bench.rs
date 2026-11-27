//! `ouch-bench`: the DeepOUCH open-loop client (ADR 0012 §7.2).
//!
//! ```text
//! ouch-bench --connect 127.0.0.1:9880 --seat 1 --token <64 hex> \
//!            [--rate 10000] [--samples 100000] [--warmup 5000] \
//!            [--price 10025] [--qty 1] [--symbol 1] [--json]
//! ```
//!
//! t0 is taken when the `EnterOrder` is queued for the socket; t1 is the first
//! report for that `cl_ord_id`. The report table is the same histogram
//! `fix-bench` prints, so the ABBA run compares like with like.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::ExitCode;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use deepbook_bench::obs::{self, COMPONENTS, Tool};
use deepbook_bench::report::Report;
use deepbook_ouch::wire::{self, Message, ord_type, side, tif};
use deepmsg_log::Guard;

fn main() -> ExitCode {
    let _log = match Guard::from_env_with(Tool, &COMPONENTS, None) {
        Ok(guard) => guard,
        Err(error) => {
            eprintln!("ouch-bench: the log configuration is refused: {error}");
            return ExitCode::FAILURE;
        }
    };
    let options = match Options::parse() {
        Ok(options) => options,
        Err(error) => {
            eprintln!("ouch-bench: {error}");
            return ExitCode::FAILURE;
        }
    };
    match run(&options) {
        Ok(report) => {
            let summary = report.summary();
            obs::benchmark_end(&report, &summary);
            if options.json {
                println!("{}", report.to_json());
            } else {
                print!("{}", report.to_table());
            }
            if report.completed == 0 {
                eprintln!("ouch-bench: no report was received");
                ExitCode::FAILURE
            } else {
                ExitCode::SUCCESS
            }
        }
        Err(error) => {
            eprintln!("ouch-bench: {error}");
            ExitCode::FAILURE
        }
    }
}

struct Options {
    address: std::net::SocketAddr,
    seat_id: u16,
    token: [u8; 32],
    price: i64,
    quantity: u64,
    symbol: u16,
    rate_per_second: u64,
    samples: u64,
    warmup: u64,
    timeout: Duration,
    json: bool,
}

impl Options {
    fn parse() -> Result<Self, String> {
        let mut options = Self {
            address: "127.0.0.1:9880".parse().expect("default address"),
            seat_id: 1,
            token: [0; 32],
            price: 10_025,
            quantity: 1,
            symbol: 1,
            rate_per_second: 10_000,
            samples: 100_000,
            warmup: 5_000,
            timeout: Duration::from_secs(60),
            json: false,
        };
        let mut token = None;
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
                    options.address = value("--connect")?
                        .parse()
                        .map_err(|_| "--connect must be IP:PORT".to_owned())?;
                }
                "--seat" => {
                    options.seat_id = value("--seat")?
                        .parse()
                        .map_err(|_| "--seat must be a number".to_owned())?;
                }
                "--token" => token = Some(parse_token(&value("--token")?)?),
                "--price" => {
                    options.price = value("--price")?
                        .parse()
                        .map_err(|_| "--price must be an integer".to_owned())?;
                }
                "--qty" => {
                    options.quantity = value("--qty")?
                        .parse()
                        .map_err(|_| "--qty must be a number".to_owned())?;
                }
                "--symbol" => {
                    options.symbol = value("--symbol")?
                        .parse()
                        .map_err(|_| "--symbol must be a number".to_owned())?;
                }
                "--rate" => {
                    options.rate_per_second = value("--rate")?
                        .parse()
                        .map_err(|_| "--rate must be a number".to_owned())?;
                }
                "--samples" => {
                    options.samples = value("--samples")?
                        .parse()
                        .map_err(|_| "--samples must be a number".to_owned())?;
                }
                "--warmup" => {
                    options.warmup = value("--warmup")?
                        .parse()
                        .map_err(|_| "--warmup must be a number".to_owned())?;
                }
                "--timeout-ms" => {
                    options.timeout = Duration::from_millis(
                        value("--timeout-ms")?
                            .parse()
                            .map_err(|_| "--timeout-ms must be a number".to_owned())?,
                    );
                }
                "--json" => options.json = true,
                "--help" | "-h" => {
                    println!(
                        "ouch-bench --connect ADDR --seat ID --token HEX [--rate N] \
                         [--samples N] [--warmup N] [--price TICKS] [--qty N] [--json]"
                    );
                    std::process::exit(0);
                }
                other => return Err(format!("unknown argument {other}")),
            }
        }
        options.token = token.ok_or_else(|| "--token is required".to_owned())?;
        if options.samples == 0 {
            return Err("--samples must be positive".to_owned());
        }
        Ok(options)
    }
}

fn parse_token(text: &str) -> Result<[u8; 32], String> {
    if text.len() != 64 {
        return Err("--token must be 64 hex digits".to_owned());
    }
    let mut token = [0_u8; 32];
    for (index, byte) in token.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[index * 2..index * 2 + 2], 16)
            .map_err(|_| "--token must be hexadecimal".to_owned())?;
    }
    Ok(token)
}

fn run(options: &Options) -> Result<Report, String> {
    let mut stream = TcpStream::connect(options.address)
        .map_err(|error| format!("connect {}: {error}", options.address))?;
    stream
        .set_nodelay(true)
        .map_err(|error| format!("nodelay: {error}"))?;
    stream
        .set_read_timeout(Some(Duration::from_millis(100)))
        .map_err(|error| format!("read timeout: {error}"))?;
    let login = Message::LoginRequest {
        seat_id: options.seat_id,
        token: options.token,
        requested_seq: u64::MAX,
    };
    write_message(&mut stream, &login)?;

    let mut decoder = Vec::new();
    let mut buffer = [0_u8; 4096];
    let accepted = loop {
        let (message, used) = read_message(&mut stream, &mut decoder, &mut buffer)?;
        if used > 0 {
            // read_message drains one frame when it can.
        }
        match message {
            Message::LoginAccepted { .. } => break true,
            Message::LoginRejected { reason } => {
                return Err(format!("login rejected (reason {reason})"));
            }
            _ => {}
        }
    };
    let _ = accepted;

    let started = Arc::new(Mutex::new(HashMap::<u64, Instant>::new()));
    let (sender, receiver) = mpsc::channel::<(u64, Instant, bool)>();
    let reader_stream = stream
        .try_clone()
        .map_err(|error| format!("clone socket: {error}"))?;
    let reader = std::thread::spawn(move || reader_loop(reader_stream, sender));

    let total = options.warmup.saturating_add(options.samples);
    let began = Instant::now();
    let mut sent = 0_u64;
    let mut last_heartbeat = Instant::now();
    let mut completed = 0_u64;
    let mut rejected = 0_u64;
    let mut samples = Vec::with_capacity(usize::try_from(options.samples).unwrap_or(0));
    let mut first_send = None;
    let mut last_completion = began;

    for ordinal in 0..total {
        if options.rate_per_second > 0 {
            let target = Duration::from_secs_f64(ordinal as f64 / options.rate_per_second as f64);
            let deadline = began + target;
            let now = Instant::now();
            if deadline > now {
                std::thread::sleep(deadline - now);
            }
        }
        if last_heartbeat.elapsed() >= Duration::from_secs(1) {
            let _ = write_message(&mut stream, &Message::ClientHeartbeat);
            last_heartbeat = Instant::now();
        }
        let cl_ord_id = ordinal + 1;
        let order = Message::EnterOrder {
            cl_ord_id,
            price: options.price,
            qty: options.quantity,
            symbol: options.symbol,
            side: side::BUY,
            ord_type: ord_type::LIMIT,
            tif: tif::IOC,
            flags: 0,
        };
        let before = Instant::now();
        if let Err(error) = write_message(&mut stream, &order) {
            eprintln!("ouch-bench: send: {error}");
            break;
        }
        if first_send.is_none() {
            first_send = Some(before);
        }
        started.lock().expect("sent map").insert(cl_ord_id, before);
        sent += 1;

        while let Ok((id, completed_at, was_rejected)) = receiver.try_recv() {
            if let Some(t0) = started.lock().expect("sent map").remove(&id) {
                completed += 1;
                if was_rejected {
                    rejected += 1;
                }
                if id > options.warmup {
                    samples.push(
                        completed_at
                            .saturating_duration_since(t0)
                            .as_nanos()
                            .min(u128::from(u64::MAX)) as u64,
                    );
                }
                last_completion = completed_at;
            }
        }
    }

    let deadline = started_time(first_send.unwrap_or(began)) + options.timeout;
    while completed < sent && Instant::now() < deadline {
        match receiver.recv_timeout(Duration::from_millis(50)) {
            Ok((id, completed_at, was_rejected)) => {
                if let Some(t0) = started.lock().expect("sent map").remove(&id) {
                    completed += 1;
                    if was_rejected {
                        rejected += 1;
                    }
                    if id > options.warmup {
                        samples.push(
                            completed_at
                                .saturating_duration_since(t0)
                                .as_nanos()
                                .min(u128::from(u64::MAX)) as u64,
                        );
                    }
                    last_completion = completed_at;
                }
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
    let unanswered = sent.saturating_sub(completed);
    let elapsed = first_send.map_or(Duration::ZERO, |first| {
        last_completion.max(first).saturating_duration_since(first)
    });
    let _ = reader.join();
    Ok(Report {
        sent,
        completed,
        rejected,
        unanswered,
        elapsed,
        samples_ns: samples,
    })
}

fn started_time(value: Instant) -> Instant {
    value
}

fn write_message(stream: &mut TcpStream, message: &Message) -> Result<(), String> {
    let frame = wire::encode(message).map_err(|error| error.to_string())?;
    stream.write_all(&frame).map_err(|error| error.to_string())
}

fn read_message(
    stream: &mut TcpStream,
    decoder: &mut Vec<u8>,
    buffer: &mut [u8],
) -> Result<(Message, usize), String> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match wire::decode(decoder) {
            Ok((message, used)) => {
                decoder.drain(..used);
                return Ok((message, used));
            }
            Err(wire::FrameError::Truncated) => {}
            Err(error) => return Err(error.to_string()),
        }
        match stream.read(buffer) {
            Ok(0) => return Err("connection closed during login".to_owned()),
            Ok(count) => decoder.extend_from_slice(&buffer[..count]),
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock
                    || error.kind() == std::io::ErrorKind::TimedOut =>
            {
                if Instant::now() >= deadline {
                    return Err("login timed out".to_owned());
                }
            }
            Err(error) => return Err(error.to_string()),
        }
    }
}

fn reader_loop(mut stream: TcpStream, sender: mpsc::Sender<(u64, Instant, bool)>) {
    let mut decoder = Vec::new();
    let mut buffer = [0_u8; 8192];
    loop {
        match wire::decode(&decoder) {
            Ok((message, used)) => {
                decoder.drain(..used);
                let (id, was_rejected) = match message {
                    Message::OrderAccepted { cl_ord_id, .. }
                    | Message::OrderExecuted { cl_ord_id, .. }
                    | Message::OrderCanceled { cl_ord_id, .. } => (cl_ord_id, false),
                    Message::OrderReplaced { req_cl_ord_id, .. } => (req_cl_ord_id, false),
                    Message::OrderRejected { cl_ord_id, .. } => (cl_ord_id, true),
                    Message::Logout { .. } => return,
                    _ => continue,
                };
                if sender.send((id, Instant::now(), was_rejected)).is_err() {
                    return;
                }
                continue;
            }
            Err(wire::FrameError::Truncated) => {}
            Err(_) => return,
        }
        match stream.read(&mut buffer) {
            Ok(0) => return,
            Ok(count) => decoder.extend_from_slice(&buffer[..count]),
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock
                    || error.kind() == std::io::ErrorKind::TimedOut => {}
            Err(_) => return,
        }
    }
}
