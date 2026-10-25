//! A minimal WebSocket server side: the RFC 6455 handshake and text frames.
//!
//! The admin only needs server-to-client text (the whole conflated view) and
//! the client's control frames (close/ping). There is no extension, no
//! fragmentation and no compression; a message larger than one frame is not
//! sent. SHA-1 and base64 are here rather than in a dependency because they
//! are the only two primitives the handshake needs.

use std::io::{self, Write};
use std::net::TcpStream;

use crate::http::Request;

/// The RFC 6455 GUID.
pub const GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

/// The `Sec-WebSocket-Accept` value for a client key.
#[must_use]
pub fn accept_key(key: &str) -> String {
    base64(&sha1(format!("{key}{GUID}").as_bytes()))
}

/// Write the `101 Switching Protocols` answer.
///
/// # Errors
///
/// The request has no `Sec-WebSocket-Key`, or the socket failed.
pub fn upgrade(stream: &mut TcpStream, request: &Request) -> io::Result<()> {
    let key = request
        .header("sec-websocket-key")
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "no Sec-WebSocket-Key"))?;
    let accept = accept_key(key);
    write!(
        stream,
        "HTTP/1.1 101 Switching Protocols\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Accept: {accept}\r\n\r\n"
    )?;
    stream.flush()
}

/// Write one unfragmented text frame.
///
/// # Errors
///
/// The socket failed.
pub fn write_text(stream: &mut TcpStream, text: &str) -> io::Result<()> {
    write_frame(stream, 0x1, text.as_bytes())
}

/// Write one control frame (close/ping/pong).
///
/// # Errors
///
/// The socket failed.
pub fn write_control(stream: &mut TcpStream, opcode: u8, payload: &[u8]) -> io::Result<()> {
    write_frame(stream, opcode, payload)
}

fn write_frame(stream: &mut TcpStream, opcode: u8, payload: &[u8]) -> io::Result<()> {
    let mut frame = Vec::with_capacity(payload.len() + 10);
    frame.push(0x80 | opcode);
    match payload.len() {
        0..=125 => {
            #[allow(clippy::cast_possible_truncation)] // the arm bounds it to 125
            frame.push(payload.len() as u8);
        }
        126..=65_535 => {
            frame.push(126);
            #[allow(clippy::cast_possible_truncation)] // the arm bounds it to u16
            frame.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        }
        _ => {
            frame.push(127);
            #[allow(clippy::cast_possible_truncation)] // a message this large is not sent
            frame.extend_from_slice(&(payload.len() as u64).to_be_bytes());
        }
    }
    frame.extend_from_slice(payload);
    stream.write_all(&frame)
}

/// One complete client frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientFrame {
    /// A text message (the admin ignores its content).
    Text(Vec<u8>),
    /// A close; the server answers with close and stops.
    Close,
    /// A ping; the server answers with a pong carrying the same payload.
    Ping(Vec<u8>),
    /// A pong; nothing to do.
    Pong,
    /// A binary or reserved opcode; ignored.
    Other,
}

/// A stream of client frames, tolerant of reads that split a frame.
#[derive(Debug, Default)]
pub struct FrameReader {
    buffer: Vec<u8>,
}

impl FrameReader {
    /// A reader with no buffered bytes.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add bytes read from the socket.
    pub fn push(&mut self, bytes: &[u8]) {
        self.buffer.extend_from_slice(bytes);
    }

    /// The next complete frame, or `None` while more bytes are needed.
    #[must_use]
    pub fn next_frame(&mut self) -> Option<ClientFrame> {
        if self.buffer.len() < 2 {
            return None;
        }
        let first = self.buffer[0];
        let second = self.buffer[1];
        let opcode = first & 0x0f;
        let masked = second & 0x80 != 0;
        if !masked {
            // A client frame must be masked; drop the connection's byte
            // stream rather than guess.
            self.buffer.clear();
            return Some(ClientFrame::Other);
        }
        let length = match second & 0x7f {
            126 => {
                if self.buffer.len() < 4 {
                    return None;
                }
                usize::from(u16::from_be_bytes([self.buffer[2], self.buffer[3]]))
            }
            127 => {
                if self.buffer.len() < 10 {
                    return None;
                }
                let mut bytes = [0_u8; 8];
                bytes.copy_from_slice(&self.buffer[2..10]);
                usize::try_from(u64::from_be_bytes(bytes)).unwrap_or(usize::MAX)
            }
            short => usize::from(short),
        };
        let header = if second & 0x7f == 126 {
            4
        } else if second & 0x7f == 127 {
            10
        } else {
            2
        };
        let total = header + 4 + length;
        if self.buffer.len() < total {
            return None;
        }
        let mask = [
            self.buffer[header],
            self.buffer[header + 1],
            self.buffer[header + 2],
            self.buffer[header + 3],
        ];
        let mut payload = self.buffer[header + 4..total].to_vec();
        for (index, byte) in payload.iter_mut().enumerate() {
            *byte ^= mask[index % 4];
        }
        self.buffer.drain(..total);
        Some(match opcode {
            0x1 => ClientFrame::Text(payload),
            0x8 => ClientFrame::Close,
            0x9 => ClientFrame::Ping(payload),
            0xa => ClientFrame::Pong,
            _ => ClientFrame::Other,
        })
    }
}

/// SHA-1 (`FIPS 180-4`).
#[must_use]
pub fn sha1(data: &[u8]) -> [u8; 20] {
    let mut state = [
        0x6745_2301_u32,
        0xefcd_ab89,
        0x98ba_dcfe,
        0x1032_5476,
        0xc3d2_e1f0,
    ];
    let bit_length = (data.len() as u64).wrapping_mul(8);
    let mut message = Vec::with_capacity(data.len() + 72);
    message.extend_from_slice(data);
    message.push(0x80);
    while message.len() % 64 != 56 {
        message.push(0);
    }
    message.extend_from_slice(&bit_length.to_be_bytes());

    for chunk in message.chunks_exact(64) {
        let mut words = [0_u32; 80];
        for (index, word) in words.iter_mut().take(16).enumerate() {
            let start = index * 4;
            *word = u32::from_be_bytes([
                chunk[start],
                chunk[start + 1],
                chunk[start + 2],
                chunk[start + 3],
            ]);
        }
        for index in 16..80 {
            words[index] =
                (words[index - 3] ^ words[index - 8] ^ words[index - 14] ^ words[index - 16])
                    .rotate_left(1);
        }
        let [mut a, mut b, mut c, mut d, mut e] = state;
        for (index, word) in words.iter().enumerate() {
            let (function, constant) = match index {
                0..=19 => ((b & c) | (!b & d), 0x5a82_7999),
                20..=39 => (b ^ c ^ d, 0x6ed9_eba1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8f1b_bcdc),
                _ => (b ^ c ^ d, 0xca62_c1d6),
            };
            let temp = a
                .rotate_left(5)
                .wrapping_add(function)
                .wrapping_add(e)
                .wrapping_add(constant)
                .wrapping_add(*word);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = temp;
        }
        state[0] = state[0].wrapping_add(a);
        state[1] = state[1].wrapping_add(b);
        state[2] = state[2].wrapping_add(c);
        state[3] = state[3].wrapping_add(d);
        state[4] = state[4].wrapping_add(e);
    }

    let mut digest = [0_u8; 20];
    for (index, word) in state.iter().enumerate() {
        digest[index * 4..index * 4 + 4].copy_from_slice(&word.to_be_bytes());
    }
    digest
}

/// Standard base64 with padding.
#[must_use]
pub fn base64(data: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let first = u32::from(chunk[0]);
        let second = u32::from(*chunk.get(1).unwrap_or(&0));
        let third = u32::from(*chunk.get(2).unwrap_or(&0));
        let combined = (first << 16) | (second << 8) | third;
        out.push(char::from(TABLE[((combined >> 18) & 63) as usize]));
        out.push(char::from(TABLE[((combined >> 12) & 63) as usize]));
        if chunk.len() > 1 {
            out.push(char::from(TABLE[((combined >> 6) & 63) as usize]));
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(char::from(TABLE[(combined & 63) as usize]));
        } else {
            out.push('=');
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha1_matches_the_fips_vectors() {
        assert_eq!(
            "a9993e364706816aba3e25717850c26c9cd0d89d",
            hex(&sha1(b"abc"))
        );
        assert_eq!("da39a3ee5e6b4b0d3255bfef95601890afd80709", hex(&sha1(b"")));
        assert_eq!(
            "84983e441c3bd26ebaae4aa1f95129e5e54670f1",
            hex(&sha1(
                b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"
            ))
        );
    }

    #[test]
    fn base64_matches_the_rfc_vectors() {
        assert_eq!("", base64(b""));
        assert_eq!("Zg==", base64(b"f"));
        assert_eq!("Zm8=", base64(b"fo"));
        assert_eq!("Zm9v", base64(b"foo"));
        assert_eq!("Zm9vYmFy", base64(b"foobar"));
    }

    #[test]
    fn the_rfc_handshake_key_matches() {
        assert_eq!(
            "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=",
            accept_key("dGhlIHNhbXBsZSBub25jZQ==")
        );
    }

    #[test]
    fn a_masked_client_frame_is_unmasked() {
        // "Hello" masked with 37 fa 21 3d.
        let frame = [
            0x81,
            0x85,
            37,
            0xfa,
            0x21,
            0x3d,
            b'H' ^ 37,
            b'e' ^ 0xfa,
            b'l' ^ 0x21,
            b'l' ^ 0x3d,
            b'o' ^ 37,
        ];
        let mut reader = FrameReader::new();
        reader.push(&frame);
        assert_eq!(
            Some(ClientFrame::Text(b"Hello".to_vec())),
            reader.next_frame()
        );
        assert_eq!(None, reader.next_frame());
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}
