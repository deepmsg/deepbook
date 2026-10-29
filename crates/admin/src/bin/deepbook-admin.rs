//! `deepbook-admin`: the M5 dashboard host.
//!
//! ```text
//! deepbook-admin --aeron-dir /dev/shm/aeron [--listen 127.0.0.1:8080]
//!                [--symbol 1] [--ui-dir crates/ui]
//!                [--cluster-id 7] [--bench-metrics target/bench/last.prom]
//!                [--incremental-channel URI] [--incremental-stream-id N]
//!                [--recovery-channel URI] [--recovery-stream-id N]
//!                [--ingress-channel URI] [--ingress-stream-id N]
//!                [--egress-channel URI] [--egress-stream-id N]
//!                [--control-token TOKEN]
//! ```
//!
//! It connects to the same driver as the cluster and the `fix-gateway`. The
//! channels default to the engine container's defaults (`aeron:ipc`, streams
//! 2001/2002 for market data and 101/102 for the cluster, as the gateway uses).
//!
//! `POST /api/kill` offers a `KillSwitch` command to the cluster log; see
//! `deepbook_admin::control` for what the endpoint can and cannot confirm, and
//! [`kill`] for the authorization rule.

use std::io::Read;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use deepbook_admin::control::{ControlConfig, KillOutcome, KillRequest};
use deepbook_admin::{Control, Feed, View, http, metrics, shared_view, web, ws};
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
    let control = Control::spawn(options.control).map_err(|error| format!("control: {error}"))?;
    println!(
        "deepbook-admin listening on http://{address} ui={} aeron={} control={}",
        options.ui_dir.display(),
        options.aeron_dir.display(),
        if options.control_token.is_some() {
            "token"
        } else {
            "loopback-only"
        }
    );

    let context = Arc::new(Context {
        view,
        ui_dir: options.ui_dir,
        aeron_dir: options.aeron_dir,
        cluster_id: options.cluster_id,
        bench_metrics: options.bench_metrics,
        control,
        control_token: options.control_token,
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
    control: Control,
    control_token: Option<String>,
}

fn handle(mut stream: TcpStream, context: &Context) {
    let Ok(Some(request)) = http::read_request(&mut stream, REQUEST_TIMEOUT) else {
        return;
    };
    if request.method != "GET" {
        if request.method == "POST" && request.path == "/api/kill" {
            kill(&mut stream, &request, context);
        } else {
            let _ = http::method_not_allowed(&mut stream);
        }
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
        "/api/control" => {
            let _ = http::json(&mut stream, &context.control.status().to_json());
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

/// `POST /api/kill`: offer one `KillSwitch` to the cluster log.
///
/// The body is `{}`/empty for the whole market or `{"account": N}` for one
/// account. Nothing the engine does is immediate — the command waits in the
/// log like any other — and there is no acknowledgement event, so a `202`
/// means "queued for the cluster", not "applied".
///
/// # Authorization
///
/// With `--control-token` the request must carry `Authorization: Bearer
/// <token>`. Without one, only a loopback peer is accepted: a dashboard bound
/// to `0.0.0.0` for the monitoring profile cannot be used to kill the market
/// from another host by accident. The comparison is not constant-time; the
/// token is a demo-grade guard for a trusted network, not an authentication
/// system (the admin serves plain HTTP).
fn kill(stream: &mut TcpStream, request: &http::Request, context: &Context) {
    if !authorized(stream, request, context) {
        let _ = http::respond_json(
            stream,
            403,
            "Forbidden",
            &json_error("the kill switch needs a loopback peer or the --control-token"),
        );
        return;
    }
    let Ok(body) = std::str::from_utf8(&request.body) else {
        let _ = http::respond_json(
            stream,
            400,
            "Bad Request",
            &json_error("the body is not UTF-8"),
        );
        return;
    };
    let kill = match KillRequest::from_body(body) {
        Ok(kill) => kill,
        Err(error) => {
            let _ = http::respond_json(stream, 400, "Bad Request", &json_error(&error));
            return;
        }
    };
    let (status, reason, body) = match context.control.kill(kill) {
        KillOutcome::Queued => (
            202,
            "Accepted",
            format!(
                "{{\"status\":\"queued\",{}}}",
                match kill {
                    KillRequest::All => "\"scope\":\"all\"".to_owned(),
                    KillRequest::Account(account) => format!("\"account\":{account}"),
                }
            ),
        ),
        KillOutcome::Full => (
            503,
            "Service Unavailable",
            json_error("the control queue is full"),
        ),
        KillOutcome::Closed => (
            503,
            "Service Unavailable",
            json_error("the control thread is gone"),
        ),
    };
    let _ = http::respond_json(stream, status, reason, &body);
}

fn authorized(stream: &TcpStream, request: &http::Request, context: &Context) -> bool {
    let loopback = stream.peer_addr().is_ok_and(|peer| peer.ip().is_loopback());
    allowed(
        loopback,
        request.header("authorization"),
        context.control_token.as_deref(),
    )
}

/// Whether a kill request may proceed: the configured token when there is
/// one, otherwise a loopback peer.
fn allowed(loopback: bool, presented: Option<&str>, token: Option<&str>) -> bool {
    match token {
        Some(token) => presented
            .and_then(|value| value.strip_prefix("Bearer "))
            .is_some_and(|value| value == token),
        None => loopback,
    }
}

/// A JSON error object whose message is safe inside a string.
fn json_error(message: &str) -> String {
    let cleaned: String = message.chars().filter(|char| !char.is_control()).collect();
    format!(
        "{{\"error\":\"{}\"}}",
        cleaned.replace('\\', "\\\\").replace('"', "\\\"")
    )
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
    control: ControlConfig,
    control_token: Option<String>,
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
        let mut ingress_channel = "aeron:ipc".to_owned();
        let mut ingress_stream_id = 101;
        let mut egress_channel = "aeron:ipc".to_owned();
        let mut egress_stream_id = 102;
        let mut control_token: Option<String> = None;

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
                "--ingress-channel" => ingress_channel = value("--ingress-channel")?,
                "--ingress-stream-id" => {
                    ingress_stream_id = value("--ingress-stream-id")?
                        .parse()
                        .map_err(|_| "--ingress-stream-id must be a number".to_owned())?;
                }
                "--egress-channel" => egress_channel = value("--egress-channel")?,
                "--egress-stream-id" => {
                    egress_stream_id = value("--egress-stream-id")?
                        .parse()
                        .map_err(|_| "--egress-stream-id must be a number".to_owned())?;
                }
                "--control-token" => control_token = Some(value("--control-token")?),
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
        let control = ControlConfig {
            aeron_dir: aeron_dir.clone(),
            ingress_channel,
            ingress_stream_id,
            egress_channel,
            egress_stream_id,
            ..ControlConfig::new(&aeron_dir)
        };
        Ok(Self {
            listen,
            aeron_dir,
            symbol,
            ui_dir,
            cluster_id,
            bench_metrics,
            market_data,
            control,
            control_token,
        })
    }
}

fn print_usage() {
    println!(
        "usage: deepbook-admin --aeron-dir PATH [--listen IP:PORT]\n\
         \x20                    [--symbol N] [--ui-dir PATH] [--cluster-id N]\n\
         \x20                    [--bench-metrics PATH]\n\
         \x20                    [--incremental-channel URI] [--incremental-stream-id N]\n\
         \x20                    [--recovery-channel URI] [--recovery-stream-id N]\n\
         \x20                    [--ingress-channel URI] [--ingress-stream-id N]\n\
         \x20                    [--egress-channel URI] [--egress-stream-id N]\n\
         \x20                    [--control-token TOKEN]"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_loopback_peer_may_kill_without_a_token() {
        assert!(allowed(true, None, None));
        assert!(!allowed(false, None, None));
    }

    #[test]
    fn a_configured_token_is_required_from_every_peer() {
        assert!(allowed(false, Some("Bearer s3cret"), Some("s3cret")));
        assert!(allowed(true, Some("Bearer s3cret"), Some("s3cret")));
        assert!(!allowed(true, None, Some("s3cret")));
        assert!(!allowed(true, Some("Bearer wrong"), Some("s3cret")));
        assert!(!allowed(true, Some("s3cret"), Some("s3cret")));
        assert!(!allowed(true, Some("Basic s3cret"), Some("s3cret")));
    }

    #[test]
    fn an_error_message_is_a_json_string() {
        assert_eq!(
            "{\"error\":\"a \\\"quoted\\\" word\"}",
            json_error("a \"quoted\" word")
        );
        // Control characters are dropped, not escaped.
        assert_eq!("{\"error\":\"ab\"}", json_error("a\nb"));
        assert_eq!("{\"error\":\"c:\\\\tmp\"}", json_error("c:\\tmp"));
    }
}
