//! A small FIX 4.4 client for the failover run.
//!
//! It is not a FIX engine: one connection, a logon, one order at a time and
//! the reports the gateway sends back. It answers `TestRequest` the way a real
//! engine does, so a long wait for the cluster to finish an election does not
//! end the session, and it resends an order under the same `ClOrdID` when the
//! caller asks: the engine's idempotency window makes that a safe retry after
//! a failover (ADR 0003).

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use deepbook_fix::codec::{MessageBuilder, MsgType, ParsedMessage, tag};
use deepbook_fix::frame::FrameDecoder;
use deepbook_fix::message::{ApplicationMessage, decode_application};
use deepbook_fix::{ExecutionReport, SessionMessage, decode_session};

/// The FIX sender this client logs on as; the gateway's acceptor default.
pub const SENDER: &str = "CLIENT";
/// The FIX target this client logs on to.
pub const TARGET: &str = "DEEPBOOK";

/// One FIX connection to the gateway.
pub struct FixClient {
    stream: TcpStream,
    decoder: FrameDecoder,
    next_seq: u64,
}

impl FixClient {
    /// Connect and log on.
    ///
    /// # Errors
    ///
    /// The connection failed, or the gateway did not answer the `Logon`.
    pub fn connect(address: SocketAddr) -> Result<Self, String> {
        let stream = TcpStream::connect_timeout(&address, Duration::from_secs(5))
            .map_err(|error| format!("connect {address}: {error}"))?;
        stream
            .set_read_timeout(Some(Duration::from_millis(50)))
            .map_err(|error| format!("read timeout: {error}"))?;
        let mut fix = Self {
            stream,
            decoder: FrameDecoder::new(),
            next_seq: 1,
        };
        fix.send(
            MsgType::Logon,
            &[
                (tag::ENCRYPT_METHOD, b"0".to_vec()),
                (tag::HEART_BT_INT, b"30".to_vec()),
            ],
        )?;
        fix.read_until(Duration::from_secs(10), |message_type| {
            message_type == MsgType::Logon
        })?;
        Ok(fix)
    }

    /// Send a limit order. `price` is in engine ticks (hundredths).
    ///
    /// # Errors
    ///
    /// The connection failed.
    pub fn new_order(
        &mut self,
        cl_ord_id: &str,
        account: u32,
        sell: bool,
        qty: i64,
        price: i64,
    ) -> Result<(), String> {
        self.send(
            MsgType::NewOrderSingle,
            &[
                (tag::CL_ORD_ID, cl_ord_id.as_bytes().to_vec()),
                (tag::ACCOUNT, account.to_string().into_bytes()),
                (tag::SYMBOL, b"AAPL".to_vec()),
                (tag::SIDE, if sell { b"2".to_vec() } else { b"1".to_vec() }),
                (tag::ORDER_QTY, qty.to_string().into_bytes()),
                (tag::ORD_TYPE, b"2".to_vec()),
                (tag::PRICE, format_price(price).into_bytes()),
                (tag::TIME_IN_FORCE, b"0".to_vec()),
                (tag::TRANSACT_TIME, b"20261029-12:00:00.000".to_vec()),
            ],
        )
    }

    /// The next application report within `timeout`, or `None` when the
    /// timeout passed with no report.
    ///
    /// Heartbeats are skipped and `TestRequest` answered, so a caller may poll
    /// this in a loop for as long as it likes.
    ///
    /// # Errors
    ///
    /// The gateway closed the session, or it sent something that is not an
    /// `ExecutionReport`.
    pub fn read_report(&mut self, timeout: Duration) -> Result<Option<ExecutionReport>, String> {
        let deadline = Instant::now() + timeout;
        loop {
            while let Some(frame) = self.decoder.next_frame() {
                let frame = frame.map_err(|error| format!("frame: {error}"))?;
                let message_type = frame.msg_type().map_err(|error| format!("{error}"))?;
                match message_type {
                    MsgType::Heartbeat => {}
                    MsgType::TestRequest => self.answer_test_request(&frame)?,
                    MsgType::Logout => {
                        return Err(format!(
                            "the gateway logged the session out: {}",
                            frame_text(&frame)
                        ));
                    }
                    other if other.is_admin() => {
                        return Err(format!("unexpected session message {other:?}"));
                    }
                    _ => match decode_application(&frame) {
                        Ok(ApplicationMessage::ExecutionReport(report)) => return Ok(Some(report)),
                        Ok(other) => return Err(format!("expected a report, got {other:?}")),
                        Err(error) => return Err(format!("decode: {error}")),
                    },
                }
            }
            if Instant::now() >= deadline {
                return Ok(None);
            }
            let mut buffer = [0_u8; 4_096];
            match self.stream.read(&mut buffer) {
                Ok(0) => return Err("the gateway closed the connection".to_owned()),
                Ok(count) => self.decoder.push(&buffer[..count]),
                Err(error)
                    if error.kind() == std::io::ErrorKind::WouldBlock
                        || error.kind() == std::io::ErrorKind::TimedOut =>
                {
                    std::thread::sleep(Duration::from_millis(2));
                }
                Err(error) => return Err(format!("read: {error}")),
            }
        }
    }

    /// Send a `Logout`; the session ends when the gateway answers.
    pub fn logout(&mut self) {
        let _ = self.send(MsgType::Logout, &[(tag::TEXT, b"bye".to_vec())]);
    }

    fn send(&mut self, msg_type: MsgType, body: &[(u32, Vec<u8>)]) -> Result<(), String> {
        let mut builder = MessageBuilder::new(
            msg_type,
            SENDER,
            TARGET,
            self.next_seq,
            "20261029-12:00:00.000",
        );
        for (tag, value) in body {
            builder = builder.body(*tag, value.clone());
        }
        self.next_seq += 1;
        self.stream
            .write_all(&builder.build())
            .map_err(|error| format!("send {msg_type:?}: {error}"))
    }

    fn answer_test_request(&mut self, frame: &ParsedMessage) -> Result<(), String> {
        let Ok(SessionMessage::TestRequest { test_req_id }) = decode_session(frame) else {
            return Ok(());
        };
        self.send(
            MsgType::Heartbeat,
            &[(tag::TEST_REQ_ID, test_req_id.into_bytes())],
        )
    }

    fn read_until(
        &mut self,
        timeout: Duration,
        wanted: impl Fn(MsgType) -> bool,
    ) -> Result<ParsedMessage, String> {
        let deadline = Instant::now() + timeout;
        loop {
            while let Some(frame) = self.decoder.next_frame() {
                let frame = frame.map_err(|error| format!("frame: {error}"))?;
                let message_type = frame.msg_type().map_err(|error| format!("{error}"))?;
                if wanted(message_type) {
                    return Ok(frame);
                }
                if message_type == MsgType::TestRequest {
                    self.answer_test_request(&frame)?;
                }
            }
            if Instant::now() >= deadline {
                return Err("no session message before the deadline".to_owned());
            }
            let mut buffer = [0_u8; 4_096];
            match self.stream.read(&mut buffer) {
                Ok(0) => return Err("the gateway closed the connection".to_owned()),
                Ok(count) => self.decoder.push(&buffer[..count]),
                Err(error)
                    if error.kind() == std::io::ErrorKind::WouldBlock
                        || error.kind() == std::io::ErrorKind::TimedOut =>
                {
                    std::thread::sleep(Duration::from_millis(2));
                }
                Err(error) => return Err(format!("read: {error}")),
            }
        }
    }
}

/// `100.25` from `10025`.
#[must_use]
pub fn format_price(ticks: i64) -> String {
    format!("{}.{:02}", ticks / 100, (ticks % 100).abs())
}

fn frame_text(frame: &ParsedMessage) -> String {
    match decode_session(frame) {
        Ok(SessionMessage::Logout { text }) => text.unwrap_or_default(),
        _ => String::new(),
    }
}

/// The engine's tick scale, for callers that read a report's price.
pub const TICKS: i64 = 100;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prices_are_printed_in_ticks() {
        assert_eq!("100.25", format_price(10_025));
        assert_eq!("100.00", format_price(10_000));
        assert_eq!("0.05", format_price(5));
        assert_eq!("1.50", format_price(150));
    }

    #[test]
    fn a_report_decodes() {
        let mut builder = MessageBuilder::new(
            MsgType::ExecutionReport,
            TARGET,
            SENDER,
            7,
            "20261029-12:00:00.000",
        );
        for (tag, value) in [
            (tag::ORDER_ID, b"1".to_vec()),
            (tag::CL_ORD_ID, b"C-1".to_vec()),
            (tag::EXEC_ID, b"1-0".to_vec()),
            (tag::EXEC_TYPE, b"0".to_vec()),
            (tag::ORD_STATUS, b"0".to_vec()),
            (tag::SYMBOL, b"AAPL".to_vec()),
            (tag::SIDE, b"1".to_vec()),
            (tag::ORDER_QTY, b"10".to_vec()),
            (tag::LEAVES_QTY, b"10".to_vec()),
            (tag::CUM_QTY, b"0".to_vec()),
            (tag::AVG_PX, b"0".to_vec()),
            (tag::ACCOUNT, b"3".to_vec()),
            (tag::TRANSACT_TIME, b"20261029-12:00:00.000".to_vec()),
        ] {
            builder = builder.body(tag, value);
        }
        let bytes = builder.build();
        let mut decoder = FrameDecoder::new();
        decoder.push(&bytes);
        let frame = decoder.next_frame().expect("a frame").expect("parsed");
        let ApplicationMessage::ExecutionReport(report) =
            decode_application(&frame).expect("decoded")
        else {
            panic!("not a report");
        };
        assert_eq!("C-1", report.cl_ord_id);
        assert_eq!(10, report.leaves_qty);
    }
}
