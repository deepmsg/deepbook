//! M1c over a real loopback socket: a command reaches the engine and its
//! ExecutionReport comes back on the same session.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use deepbook_fix::codec::{MessageBuilder, MsgType, ParsedMessage, tag};
use deepbook_fix::message::{ApplicationMessage, ExecType, OrdStatus, decode_application};
use deepbook_gateway::frame::FrameDecoder;
use deepbook_gateway::{Gateway, GatewayConfig, StubEngine};
use deepbook_io::BackendKind;

fn temporary_directory(name: &str) -> PathBuf {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let path = std::env::temp_dir().join(format!(
        "deepbook-gateway-engine-{}-{name}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir_all(&path).expect("temporary directory");
    path
}

fn message(msg_type: MsgType, seq: u64, body: &[(u32, Vec<u8>)]) -> Vec<u8> {
    let mut builder =
        MessageBuilder::new(msg_type, "CLIENT", "DEEPBOOK", seq, "20261025-12:34:56.000");
    for (tag, value) in body {
        builder = builder.body(*tag, value.clone());
    }
    builder.build()
}

fn logon(seq: u64) -> Vec<u8> {
    message(
        MsgType::Logon,
        seq,
        &[
            (tag::ENCRYPT_METHOD, b"0".to_vec()),
            (tag::HEART_BT_INT, b"30".to_vec()),
        ],
    )
}

fn new_order(seq: u64, cl_ord_id: &str) -> Vec<u8> {
    message(
        MsgType::NewOrderSingle,
        seq,
        &[
            (tag::CL_ORD_ID, cl_ord_id.as_bytes().to_vec()),
            (tag::ACCOUNT, b"A-1".to_vec()),
            (tag::SYMBOL, b"AAPL".to_vec()),
            (tag::SIDE, b"1".to_vec()),
            (tag::ORDER_QTY, b"100".to_vec()),
            (tag::ORD_TYPE, b"2".to_vec()),
            (tag::PRICE, b"100.25".to_vec()),
            (tag::TIME_IN_FORCE, b"0".to_vec()),
            (tag::TRANSACT_TIME, b"20261025-12:34:56.000".to_vec()),
        ],
    )
}

fn replace(seq: u64, cl_ord_id: &str, orig: &str) -> Vec<u8> {
    message(
        MsgType::OrderCancelReplaceRequest,
        seq,
        &[
            (tag::CL_ORD_ID, cl_ord_id.as_bytes().to_vec()),
            (tag::ORIG_CL_ORD_ID, orig.as_bytes().to_vec()),
            (tag::ACCOUNT, b"A-1".to_vec()),
            (tag::SYMBOL, b"AAPL".to_vec()),
            (tag::SIDE, b"1".to_vec()),
            (tag::ORDER_QTY, b"150".to_vec()),
            (tag::ORD_TYPE, b"2".to_vec()),
            (tag::PRICE, b"100.50".to_vec()),
            (tag::TIME_IN_FORCE, b"0".to_vec()),
            (tag::TRANSACT_TIME, b"20261025-12:34:57.000".to_vec()),
        ],
    )
}

fn cancel(seq: u64, cl_ord_id: &str, orig: &str) -> Vec<u8> {
    message(
        MsgType::OrderCancelRequest,
        seq,
        &[
            (tag::CL_ORD_ID, cl_ord_id.as_bytes().to_vec()),
            (tag::ORIG_CL_ORD_ID, orig.as_bytes().to_vec()),
            (tag::SYMBOL, b"AAPL".to_vec()),
            (tag::SIDE, b"1".to_vec()),
            (tag::ORDER_QTY, b"100".to_vec()),
            (tag::TRANSACT_TIME, b"20261025-12:34:58.000".to_vec()),
        ],
    )
}

fn status(seq: u64, cl_ord_id: &str) -> Vec<u8> {
    message(
        MsgType::OrderStatusRequest,
        seq,
        &[
            (tag::CL_ORD_ID, cl_ord_id.as_bytes().to_vec()),
            (tag::SYMBOL, b"AAPL".to_vec()),
            (tag::SIDE, b"1".to_vec()),
        ],
    )
}

fn logout(seq: u64) -> Vec<u8> {
    message(MsgType::Logout, seq, &[(tag::TEXT, b"bye".to_vec())])
}

fn read_frame(stream: &mut TcpStream) -> ParsedMessage {
    let mut decoder = FrameDecoder::new();
    let mut buffer = [0_u8; 1024];
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(frame) = decoder.next_frame() {
            return frame.expect("a complete frame");
        }
        if Instant::now() >= deadline {
            panic!("no frame from the gateway before the deadline");
        }
        match stream.read(&mut buffer) {
            Ok(0) => panic!("the gateway closed before sending a frame"),
            Ok(count) => decoder.push(&buffer[..count]),
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock
                    || error.kind() == std::io::ErrorKind::TimedOut => {}
            Err(error) => panic!("read: {error}"),
        }
    }
}

fn execution_report(frame: &ParsedMessage) -> deepbook_fix::ExecutionReport {
    match decode_application(frame).expect("application message") {
        ApplicationMessage::ExecutionReport(report) => report,
        other => panic!("expected an ExecutionReport, got {other:?}"),
    }
}

fn round_trip(backend: BackendKind) {
    let name = format!("orders-{backend:?}");
    let directory = temporary_directory(&name);
    let mut config = GatewayConfig::new(
        "127.0.0.1:0".parse().expect("address"),
        &directory,
        "DEEPBOOK",
        "CLIENT",
    );
    config.backend = backend;
    let mut gateway = match Gateway::bind(config, StubEngine::new()) {
        Ok(gateway) => gateway,
        Err(error) => {
            eprintln!("SKIPPED {backend:?}: {error}");
            std::fs::remove_dir_all(directory).expect("cleanup");
            return;
        }
    };
    let address = gateway.local_addr().expect("local address");
    let handle = std::thread::spawn(move || {
        gateway
            .run_until(Instant::now() + Duration::from_secs(3))
            .expect("gateway");
    });

    let mut stream = TcpStream::connect(address).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_millis(100)))
        .expect("read timeout");

    stream.write_all(&logon(1)).expect("logon");
    let reply = read_frame(&mut stream);
    assert_eq!(MsgType::Logon, reply.msg_type().expect("type"));

    stream.write_all(&new_order(2, "C-1")).expect("order");
    let accepted = execution_report(&read_frame(&mut stream));
    assert_eq!(ExecType::New, accepted.exec_type);
    assert_eq!(OrdStatus::New, accepted.ord_status);
    assert_eq!("C-1", accepted.cl_ord_id);
    assert_eq!("ORDER-1", accepted.order_id);
    assert_eq!(100, accepted.leaves_qty);
    assert_eq!(0, accepted.cum_qty);
    assert_eq!(0, accepted.avg_px);

    // The same ClOrdID is a retransmission, not a second order.
    stream
        .write_all(&new_order(3, "C-1"))
        .expect("retransmitted order");
    let duplicate = execution_report(&read_frame(&mut stream));
    assert_eq!(accepted.order_id, duplicate.order_id);
    assert_eq!(accepted.exec_id, duplicate.exec_id);

    stream
        .write_all(&replace(4, "C-2", "C-1"))
        .expect("replace");
    let replaced = execution_report(&read_frame(&mut stream));
    assert_eq!(ExecType::Replaced, replaced.exec_type);
    assert_eq!(OrdStatus::New, replaced.ord_status);
    assert_eq!("ORDER-1", replaced.order_id);
    assert_eq!(150, replaced.leaves_qty);

    stream.write_all(&cancel(5, "C-3", "C-1")).expect("cancel");
    let canceled = execution_report(&read_frame(&mut stream));
    assert_eq!(ExecType::Canceled, canceled.exec_type);
    assert_eq!(OrdStatus::Canceled, canceled.ord_status);
    assert_eq!(0, canceled.leaves_qty);

    stream
        .write_all(&cancel(6, "C-4", "NOPE"))
        .expect("unknown cancel");
    match decode_application(&read_frame(&mut stream)).expect("application") {
        ApplicationMessage::OrderCancelReject(reject) => {
            assert_eq!("C-4", reject.cl_ord_id);
            assert_eq!("NOPE", reject.orig_cl_ord_id);
        }
        other => panic!("expected an OrderCancelReject, got {other:?}"),
    }

    stream.write_all(&status(7, "C-1")).expect("status");
    let reported = execution_report(&read_frame(&mut stream));
    assert_eq!(ExecType::OrderStatus, reported.exec_type);
    assert_eq!(OrdStatus::Canceled, reported.ord_status);

    stream.write_all(&logout(8)).expect("logout");
    let reply = read_frame(&mut stream);
    assert_eq!(MsgType::Logout, reply.msg_type().expect("type"));

    drop(stream);
    handle.join().expect("gateway thread");
    std::fs::remove_dir_all(directory).expect("cleanup");
}

#[test]
fn a_command_reaches_the_engine_over_epoll() {
    round_trip(BackendKind::Epoll);
}

#[test]
fn a_command_reaches_the_engine_over_uring() {
    round_trip(BackendKind::Uring);
}
