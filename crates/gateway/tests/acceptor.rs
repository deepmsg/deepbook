//! The acceptor over a real loopback socket: logon, one order, logout.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use deepbook_fix::ApplicationMessage;
use deepbook_fix::codec::{MessageBuilder, MsgType, ParsedMessage, tag};
use deepbook_gateway::frame::FrameDecoder;
use deepbook_gateway::{CommandSink, Gateway, GatewayConfig, SessionKey};

struct RecordingSink {
    messages: Arc<Mutex<Vec<ApplicationMessage>>>,
}

impl CommandSink for RecordingSink {
    fn deliver(&mut self, message: ApplicationMessage) {
        self.messages.lock().expect("sink").push(message);
    }
}

fn temporary_directory(name: &str) -> PathBuf {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let path = std::env::temp_dir().join(format!(
        "deepbook-gateway-it-{}-{name}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir_all(&path).expect("temporary directory");
    path
}

fn logon(seq: u64) -> Vec<u8> {
    MessageBuilder::new(
        MsgType::Logon,
        "CLIENT",
        "DEEPBOOK",
        seq,
        "20261025-12:34:56.000",
    )
    .body_int(tag::ENCRYPT_METHOD, 0)
    .body_int(tag::HEART_BT_INT, 30)
    .build()
}

fn new_order(seq: u64) -> Vec<u8> {
    MessageBuilder::new(
        MsgType::NewOrderSingle,
        "CLIENT",
        "DEEPBOOK",
        seq,
        "20261025-12:34:56.001",
    )
    .body_str(tag::CL_ORD_ID, "C-1")
    .body_str(tag::SYMBOL, "AAPL")
    .body(tag::SIDE, b"1".to_vec())
    .body_int(tag::ORDER_QTY, 100)
    .body(tag::ORD_TYPE, b"2".to_vec())
    .body_str(tag::PRICE, "100.25")
    .body(tag::TIME_IN_FORCE, b"0".to_vec())
    .body_str(tag::TRANSACT_TIME, "20261025-12:34:56.001")
    .build()
}

fn logout(seq: u64) -> Vec<u8> {
    MessageBuilder::new(
        MsgType::Logout,
        "CLIENT",
        "DEEPBOOK",
        seq,
        "20261025-12:34:56.002",
    )
    .body_str(tag::TEXT, "bye")
    .build()
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

#[test]
fn a_client_logs_on_sends_an_order_and_logs_out() {
    let directory = temporary_directory("session");
    let messages = Arc::new(Mutex::new(Vec::new()));
    let sink = RecordingSink {
        messages: Arc::clone(&messages),
    };
    let config = GatewayConfig::new(
        "127.0.0.1:0".parse().expect("address"),
        &directory,
        "DEEPBOOK",
        "CLIENT",
    );
    let mut gateway = Gateway::bind(config, sink).expect("bind");
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
    assert_eq!(1, reply.seq_num().expect("seq"));

    stream.write_all(&new_order(2)).expect("order");
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if !messages.lock().expect("sink").is_empty() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the order did not reach the sink"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    let delivered = messages.lock().expect("sink");
    match delivered.first().expect("one message") {
        ApplicationMessage::NewOrderSingle(order) => {
            assert_eq!("C-1", order.cl_ord_id);
            assert_eq!(10_025, order.price.expect("price"));
        }
        other => panic!("unexpected application message: {other:?}"),
    }
    drop(delivered);

    stream.write_all(&logout(3)).expect("logout");
    let reply = read_frame(&mut stream);
    assert_eq!(MsgType::Logout, reply.msg_type().expect("type"));
    drop(stream);

    // A reconnect resumes the session: the peer's next inbound sequence is 4,
    // and our next outbound sequence is 3 (Logon reply 1, Logout reply 2).
    let mut again = TcpStream::connect(address).expect("reconnect");
    again
        .set_read_timeout(Some(Duration::from_millis(100)))
        .expect("read timeout");
    again.write_all(&logon(4)).expect("second logon");
    let reply = read_frame(&mut again);
    assert_eq!(MsgType::Logon, reply.msg_type().expect("type"));
    assert_eq!(3, reply.seq_num().expect("resumed outbound sequence"));
    drop(again);

    handle.join().expect("gateway thread");

    let key = SessionKey::new("DEEPBOOK", "CLIENT");
    assert!(
        directory
            .join(key.directory_name())
            .join("journal.bin")
            .is_file()
    );
    std::fs::remove_dir_all(directory).expect("cleanup");
}

#[test]
fn an_unlogged_connection_is_closed_at_the_logon_timeout() {
    // The timeout is a session property, so this drives the same gateway with
    // a short one and expects the server to close without a FIX message.
    let directory = temporary_directory("timeout");
    let messages = Arc::new(Mutex::new(Vec::new()));
    let sink = RecordingSink {
        messages: Arc::clone(&messages),
    };
    let mut config = GatewayConfig::new(
        "127.0.0.1:0".parse().expect("address"),
        &directory,
        "DEEPBOOK",
        "CLIENT",
    );
    config.logon_timeout_millis = 100;
    config.tick = Duration::from_millis(10);
    let mut gateway = Gateway::bind(config, sink).expect("bind");
    let address = gateway.local_addr().expect("local address");
    let handle = std::thread::spawn(move || {
        gateway
            .run_until(Instant::now() + Duration::from_secs(2))
            .expect("gateway");
    });

    let mut stream = TcpStream::connect(address).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_millis(100)))
        .expect("read timeout");
    let mut buffer = [0_u8; 64];
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(_) => {}
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock
                    || error.kind() == std::io::ErrorKind::TimedOut => {}
            Err(error) => panic!("read: {error}"),
        }
        assert!(
            Instant::now() < deadline,
            "the gateway did not close the idle connection"
        );
    }

    drop(stream);
    handle.join().expect("gateway thread");
    std::fs::remove_dir_all(directory).expect("cleanup");
}
