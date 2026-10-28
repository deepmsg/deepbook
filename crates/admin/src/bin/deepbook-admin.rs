//! `deepbook-admin`: the M5 dashboard host.
//!
//! ```text
//! deepbook-admin --aeron-dir /dev/shm/aeron [--listen 127.0.0.1:8080]
//!                [--symbol 1] [--ui-dir crates/ui]
//!                [--cluster-id 7] [--bench-metrics target/bench/last.prom]
//!                [--incremental-channel URI] [--incremental-stream-id N]
//!                [--recovery-channel URI] [--recovery-stream-id N]
//! ```
//!
//! It connects to the same driver as the cluster and the `fix-gateway`. The
//! channels default to the engine container's defaults (`aeron:ipc`, streams
//! 2001/2002).

use std::io::Read;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use deepbook_admin::{Feed, View, http, metrics, shared_view, web, ws};
use deepbook_marketdata::DeepmsgConfig;
use deepbook_types::SymbolId;

/// How often a WebSocket client receives the conflated view.
const PUSH_INTERVAL: Duration = Duration::from_millis(100);
/// How often the feed polls the driver.
const FEED_INTERVAL: Duration = Duration::from_millis(2);
/// How long the request head may take.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("deepbook-admin: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let options = Options::parse()?;
    let view = shared_view();
    let mut feed = Feed::connect(&options.market_data, options.symbol, Arc::clone(&view))
        .map_err(|error| format!("market data: {error}"))?;
    let listener = TcpListener::bind(options.listen)
        .map_err(|error| format!("cannot bind {}: {error}", options.listen))?;
    listener
        .set_nonblocking(true)
        .map_err(|error| error.to_string())?;
    let address = listener.local_addr().map_err(|error| error.to_string())?;
    println!(
        "deepbook-admin listening on http://{address} ui={} aeron={}",
        options.ui_dir.display(),
        options.aeron_dir.display()
    );

    let context = Arc::new(Context {
        view,
        ui_dir: options.ui_dir,
        aeron_dir: options.aeron_dir,
        cluster_id: options.cluster_id,
        bench_metrics: options.bench_metrics,
    });

    let mut feed_error: Option<Instant> = None;
    loop {
        match feed.poll() {
            Ok(_) => feed_error = None,
            Err(error) => {
                // A driver that restarted or a lost image is retried, but the
                // log is throttled to one line a second.
                if feed_error.is_none_or(|at| at.elapsed() >= Duration::from_secs(1)) {
                    eprintln!("deepbook-admin: market data: {error}");
                    feed_error = Some(Instant::now());
                }
            }
        }
        loop {
            match listener.accept() {
                Ok((stream, _peer)) => {
                    let context = Arc::clone(&context);
                    std::thread::spawn(move || handle(stream, &context));
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(error) => {
                    eprintln!("deepbook-admin: accept: {error}");
                    break;
                }
            }
        }
        std::thread::sleep(FEED_INTERVAL);
    }
}

struct Context {
    view: deepbook_admin::SharedView,
    ui_dir: PathBuf,
    aeron_dir: PathBuf,
    cluster_id: i32,
    bench_metrics: Option<PathBuf>,
}

fn handle(mut stream: TcpStream, context: &Context) {
    let Ok(Some(request)) = http::read_request(&mut stream, REQUEST_TIMEOUT) else {
        return;
    };
    if request.method != "GET" {
        let _ = http::bad_request(&mut stream);
        return;
    }
    match request.path.as_str() {
        "/ws" => websocket(stream, &request, context),
        "/metrics" => {
            let bench = read_bench(context);
            let cluster = metrics::read_cluster(&context.aeron_dir, context.cluster_id);
            let body = context
                .view
                .lock()
                .map(|view| metrics::prometheus(&view, &cluster, bench.as_deref()))
                .unwrap_or_default();
            let _ = http::respond(
                &mut stream,
                200,
                "OK",
                "text/plain; version=0.0.4; charset=utf-8",
                body.as_bytes(),
            );
        }
        "/api/book" => {
            let body = context
                .view
                .lock()
                .map(|view: std::sync::MutexGuard<'_, View>| view.book_json())
                .unwrap_or_else(|_| "{}".to_owned());
            let _ = http::json(&mut stream, &body);
        }
        "/api/trades" => {
            let body = context
                .view
                .lock()
                .map(|view: std::sync::MutexGuard<'_, View>| view.trades_json())
                .unwrap_or_else(|_| "{}".to_owned());
            let _ = http::json(&mut stream, &body);
        }
        "/api/latency" => {
            let bench = read_bench(context);
            let _ = http::json(&mut stream, &metrics::latency_json(bench.as_deref()));
        }
        path => {
            if let Some(asset) = web::asset(&context.ui_dir, path) {
                let _ = http::ok(&mut stream, asset.content_type, &asset.body);
            } else {
                let _ = http::not_found(&mut stream);
            }
        }
    }
}

fn read_bench(context: &Context) -> Option<String> {
    context
        .bench_metrics
        .as_ref()
        .and_then(|path| std::fs::read_to_string(path).ok())
}

fn websocket(mut stream: TcpStream, request: &http::Request, context: &Context) {
    if ws::upgrade(&mut stream, request).is_err() {
        return;
    }
    if stream.set_nonblocking(true).is_err() {
        return;
    }
    let mut reader = ws::FrameReader::new();
    let mut buffer = [0_u8; 4_096];
    let mut last_sent: Option<Instant> = None;
    loop {
        if last_sent.is_none_or(|at| at.elapsed() >= PUSH_INTERVAL) {
            let body = context
                .view
                .lock()
                .map(|view: std::sync::MutexGuard<'_, View>| view.to_json())
                .unwrap_or_else(|_| "{}".to_owned());
            if ws::write_text(&mut stream, &body).is_err() {
                break;
            }
            last_sent = Some(Instant::now());
        }

        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(count) => reader.push(&buffer[..count]),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(_) => break,
        }
        let mut close = false;
        while let Some(frame) = reader.next_frame() {
            match frame {
                ws::ClientFrame::Close => {
                    let _ = ws::write_control(&mut stream, 0x8, &[]);
                    close = true;
                    break;
                }
                ws::ClientFrame::Ping(payload) => {
                    let _ = ws::write_control(&mut stream, 0xa, &payload);
                }
                ws::ClientFrame::Text(_) | ws::ClientFrame::Pong | ws::ClientFrame::Other => {}
            }
        }
        if close {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

struct Options {
    listen: SocketAddr,
    aeron_dir: PathBuf,
    symbol: SymbolId,
    ui_dir: PathBuf,
    cluster_id: i32,
    bench_metrics: Option<PathBuf>,
    market_data: DeepmsgConfig,
}

impl Options {
    fn parse() -> Result<Self, String> {
        let mut listen: SocketAddr = "127.0.0.1:8080"
            .parse()
            .expect("the default address parses");
        let mut aeron_dir: Option<PathBuf> = None;
        let mut symbol: SymbolId = 1;
        let mut ui_dir = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../ui"));
        let mut cluster_id = 7;
        let mut bench_metrics: Option<PathBuf> = None;
        let mut incremental = "aeron:ipc".to_owned();
        let mut incremental_stream_id = 2001;
        let mut recovery = "aeron:ipc".to_owned();
        let mut recovery_stream_id = 2002;

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
                "--listen" => {
                    listen = value("--listen")?
                        .parse()
                        .map_err(|_| "--listen must be IP:PORT".to_owned())?;
                }
                "--aeron-dir" => aeron_dir = Some(value("--aeron-dir")?.into()),
                "--symbol" => {
                    symbol = value("--symbol")?
                        .parse()
                        .map_err(|_| "--symbol must be a number".to_owned())?;
                }
                "--ui-dir" => ui_dir = value("--ui-dir")?.into(),
                "--cluster-id" => {
                    cluster_id = value("--cluster-id")?
                        .parse()
                        .map_err(|_| "--cluster-id must be a number".to_owned())?;
                }
                "--bench-metrics" => bench_metrics = Some(value("--bench-metrics")?.into()),
                "--incremental-channel" => incremental = value("--incremental-channel")?,
                "--incremental-stream-id" => {
                    incremental_stream_id = value("--incremental-stream-id")?
                        .parse()
                        .map_err(|_| "--incremental-stream-id must be a number".to_owned())?;
                }
                "--recovery-channel" => recovery = value("--recovery-channel")?,
                "--recovery-stream-id" => {
                    recovery_stream_id = value("--recovery-stream-id")?
                        .parse()
                        .map_err(|_| "--recovery-stream-id must be a number".to_owned())?;
                }
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                other => return Err(format!("unknown option {other}")),
            }
        }
        let Some(aeron_dir) = aeron_dir else {
            return Err("--aeron-dir is required".to_owned());
        };
        let mut market_data = DeepmsgConfig::new(
            &aeron_dir,
            incremental.clone(),
            &incremental,
            recovery.clone(),
            &recovery,
            recovery.clone(),
            &recovery,
        );
        market_data.incremental_stream_id = incremental_stream_id;
        market_data.recovery_stream_id = recovery_stream_id;
        Ok(Self {
            listen,
            aeron_dir,
            symbol,
            ui_dir,
            cluster_id,
            bench_metrics,
            market_data,
        })
    }
}

fn print_usage() {
    println!(
        "usage: deepbook-admin --aeron-dir PATH [--listen IP:PORT]\n\
         \x20                    [--symbol N] [--ui-dir PATH] [--cluster-id N]\n\
         \x20                    [--bench-metrics PATH]\n\
         \x20                    [--incremental-channel URI] [--incremental-stream-id N]\n\
         \x20                    [--recovery-channel URI] [--recovery-stream-id N]"
    );
}
