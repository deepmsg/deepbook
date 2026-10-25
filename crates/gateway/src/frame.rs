//! Extract complete FIX messages from a byte stream.
//!
//! FIX has no length prefix; `BodyLength(9)` is the length. The decoder keeps
//! a buffer, finds the start of a message (`8=FIX.4.4` + SOH), computes the
//! end from `9`, and hands the complete frame to the codec. Bytes before the
//! first `8=` are discarded, which is what a reconnect after a half-written
//! message looks like.

use deepbook_fix::codec::{CodecError, ParsedMessage, parse};

const START_MARKER: &[u8] = b"8=FIX.4.4\x01";
const CHECKSUM_LENGTH: usize = 7; // "10=ddd" + SOH
/// A message larger than this is a protocol error, not a buffer to grow.
pub const MAX_MESSAGE_LENGTH: usize = 1 << 20;

/// Why a frame could not be accepted.
#[derive(Debug)]
pub enum FrameError {
    /// The declared length is impossible or too large.
    Length(String),
    /// The codec refused the frame.
    Codec(CodecError),
}

impl std::fmt::Display for FrameError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Length(detail) => write!(formatter, "bad FIX frame length: {detail}"),
            Self::Codec(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for FrameError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Codec(error) => Some(error),
            Self::Length(_) => None,
        }
    }
}

/// A stream decoder.
#[derive(Debug, Default)]
pub struct FrameDecoder {
    buffer: Vec<u8>,
}

impl FrameDecoder {
    /// An empty decoder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Append bytes read from the socket.
    pub fn push(&mut self, bytes: &[u8]) {
        self.buffer.extend_from_slice(bytes);
    }

    /// How many bytes are buffered.
    #[must_use]
    pub fn buffered(&self) -> usize {
        self.buffer.len()
    }

    /// The next complete message, or `None` when more bytes are needed.
    ///
    /// # Errors
    ///
    /// The stream is not FIX, a length is impossible, or the codec refused the
    /// complete frame.
    pub fn next_frame(&mut self) -> Option<Result<ParsedMessage, FrameError>> {
        if self.buffer.is_empty() {
            return None;
        }

        let start = match find_start(&self.buffer) {
            Some(start) => start,
            None => {
                // Keep a possible partial marker; drop everything else.
                let keep = START_MARKER.len().saturating_sub(1);
                let excess = self.buffer.len().saturating_sub(keep);
                if excess > 0 {
                    self.buffer.drain(..excess);
                }
                return None;
            }
        };
        if start > 0 {
            self.buffer.drain(..start);
        }

        let first_soh = self.buffer.iter().position(|byte| *byte == 0x01)?;
        let second_soh = self.buffer[first_soh + 1..]
            .iter()
            .position(|byte| *byte == 0x01)
            .map(|position| position + first_soh + 1)?;
        if !self.buffer[first_soh + 1..].starts_with(b"9=") {
            return Some(Err(FrameError::Length(
                "the second field is not BodyLength".to_owned(),
            )));
        }
        let declared = match parse_u32(&self.buffer[first_soh + 3..second_soh]) {
            Ok(value) => value,
            Err(detail) => return Some(Err(FrameError::Length(detail))),
        };
        let body_start = second_soh + 1;
        let total = match usize::try_from(declared)
            .ok()
            .and_then(|length| length.checked_add(body_start))
            .and_then(|length| length.checked_add(CHECKSUM_LENGTH))
        {
            Some(total) => total,
            None => {
                return Some(Err(FrameError::Length(
                    "BodyLength overflows the buffer".to_owned(),
                )));
            }
        };
        if total > MAX_MESSAGE_LENGTH {
            return Some(Err(FrameError::Length(format!(
                "BodyLength={declared} exceeds {MAX_MESSAGE_LENGTH}"
            ))));
        }
        if self.buffer.len() < total {
            return None;
        }

        let frame = self.buffer[..total].to_vec();
        match parse(&frame) {
            Ok(message) => {
                self.buffer.drain(..total);
                Some(Ok(message))
            }
            Err(error) => Some(Err(FrameError::Codec(error))),
        }
    }
}

fn find_start(buffer: &[u8]) -> Option<usize> {
    buffer
        .windows(START_MARKER.len())
        .position(|window| window == START_MARKER)
}

fn parse_u32(bytes: &[u8]) -> Result<u32, String> {
    if bytes.is_empty() || !bytes.iter().all(u8::is_ascii_digit) {
        return Err(format!(
            "BodyLength {:?} is not a number",
            String::from_utf8_lossy(bytes)
        ));
    }
    let mut value: u32 = 0;
    for byte in bytes {
        value = value
            .checked_mul(10)
            .and_then(|value| value.checked_add(u32::from(byte - b'0')))
            .ok_or_else(|| "BodyLength overflows u32".to_owned())?;
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use deepbook_fix::codec::{MessageBuilder, MsgType, tag};

    fn logon() -> Vec<u8> {
        MessageBuilder::new(
            MsgType::Logon,
            "CLIENT",
            "DEEPBOOK",
            1,
            "20261025-12:34:56.000",
        )
        .body_int(tag::ENCRYPT_METHOD, 0)
        .body_int(tag::HEART_BT_INT, 30)
        .build()
    }

    #[test]
    fn a_message_split_at_every_boundary_decodes() {
        let raw = logon();
        for split in 1..raw.len() {
            let mut decoder = FrameDecoder::new();
            decoder.push(&raw[..split]);
            assert!(decoder.next_frame().is_none(), "split {split} needs more");
            decoder.push(&raw[split..]);
            let frame = decoder
                .next_frame()
                .expect("frame")
                .expect("complete frame");
            assert_eq!(MsgType::Logon, frame.msg_type().expect("type"));
            assert_eq!(1, frame.seq_num().expect("seq"));
            assert!(decoder.next_frame().is_none());
        }
    }

    #[test]
    fn two_messages_in_one_read_decode_in_order() {
        let mut decoder = FrameDecoder::new();
        let first = logon();
        let second = MessageBuilder::new(
            MsgType::Heartbeat,
            "CLIENT",
            "DEEPBOOK",
            2,
            "20261025-12:34:57.000",
        )
        .build();
        let mut bytes = first.clone();
        bytes.extend_from_slice(&second);
        decoder.push(&bytes);
        assert_eq!(
            1,
            decoder
                .next_frame()
                .expect("first")
                .expect("ok")
                .seq_num()
                .expect("seq")
        );
        assert_eq!(
            2,
            decoder
                .next_frame()
                .expect("second")
                .expect("ok")
                .seq_num()
                .expect("seq")
        );
        assert!(decoder.next_frame().is_none());
    }

    #[test]
    fn garbage_before_the_start_marker_is_dropped() {
        let mut decoder = FrameDecoder::new();
        let mut bytes = b"not a fix message".to_vec();
        bytes.extend_from_slice(&logon());
        decoder.push(&bytes);
        assert!(decoder.next_frame().expect("frame").is_ok());
    }

    #[test]
    fn a_bad_checksum_is_a_frame_error() {
        let mut raw = logon();
        let start = raw
            .windows(3)
            .rposition(|window| window == b"10=")
            .expect("trailer");
        raw[start + 3] = if raw[start + 3] == b'0' { b'1' } else { b'0' };
        let mut decoder = FrameDecoder::new();
        decoder.push(&raw);
        assert!(matches!(
            decoder.next_frame().expect("frame"),
            Err(FrameError::Codec(CodecError::BadChecksum { .. }))
        ));
    }

    #[test]
    fn a_second_field_that_is_not_body_length_is_refused() {
        let mut decoder = FrameDecoder::new();
        decoder.push(b"8=FIX.4.4\x01XX=5\x0110=000\x01");
        assert!(matches!(
            decoder.next_frame().expect("frame"),
            Err(FrameError::Length(_))
        ));
    }
}
