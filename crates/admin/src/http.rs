//! A very small HTTP/1.1 server side: one request per connection.
//!
//! The dashboard serves static files, three JSON snapshots, the Prometheus
//! text endpoint and the kill switch. A request that does not fit the header
//! budget is refused rather than buffered without bound; a body is read only
//! up to [`MAX_BODY`] and only when `Content-Length` declares it (the kill
//! endpoint's one-line JSON), so chunked bodies are not supported.

use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

/// The largest request head accepted.
pub const MAX_HEAD: usize = 16 * 1024;
/// The largest request body accepted.
pub const MAX_BODY: usize = 4 * 1024;

/// One parsed request.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Request {
    /// `GET`, `POST`, …
    pub method: String,
    /// The path without the query string.
    pub path: String,
    /// Lower-cased header names.
    pub headers: HashMap<String, String>,
    /// The body, when `Content-Length` declared one.
    pub body: Vec<u8>,
}

impl Request {
    /// One header, by lower-cased name.
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).map(String::as_str)
    }
}

/// Parse a complete request head (through the blank line).
#[must_use]
pub fn parse_request(bytes: &[u8]) -> Option<Request> {
    let text = std::str::from_utf8(bytes).ok()?;
    let mut lines = text.split("\r\n");
    let request_line = lines.next()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?.to_owned();
    let target = parts.next()?;
    let _version = parts.next()?;
    let path = target.split('?').next().unwrap_or(target).to_owned();
    let mut headers = HashMap::new();
    for line in lines {
        if line.is_empty() {
            break;
        }
        let (name, value) = line.split_once(':')?;
        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_owned());
    }
    Some(Request {
        method,
        path,
        headers,
        body: Vec::new(),
    })
}

/// Read one request head and, when one is declared, its body.
///
/// `Ok(None)` means the peer closed before sending a complete head.
///
/// # Errors
///
/// The socket failed, the head exceeded [`MAX_HEAD`], or the body exceeded
/// [`MAX_BODY`].
pub fn read_request(stream: &mut TcpStream, timeout: Duration) -> io::Result<Option<Request>> {
    stream.set_read_timeout(Some(timeout))?;
    let mut buffer = Vec::with_capacity(1024);
    let mut chunk = [0_u8; 1024];
    let end = loop {
        if let Some(end) = find_head_end(&buffer) {
            break end;
        }
        if buffer.len() >= MAX_HEAD {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "request head too large",
            ));
        }
        match stream.read(&mut chunk) {
            Ok(0) => return Ok(None),
            Ok(count) => buffer.extend_from_slice(&chunk[..count]),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    };
    let Some(mut request) = parse_request(&buffer[..end]) else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "the request head does not parse",
        ));
    };

    let length = match request.header("content-length") {
        None => 0,
        Some(value) => value
            .parse::<usize>()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad Content-Length"))?,
    };
    if length > MAX_BODY {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "request body too large",
        ));
    }
    // Anything the head read already carried past the blank line is the start
    // of the body; the rest arrives in further reads.
    request.body.extend_from_slice(&buffer[end..]);
    while request.body.len() < length {
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(count) => request.body.extend_from_slice(&chunk[..count]),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    request.body.truncate(length);
    Ok(Some(request))
}

fn find_head_end(buffer: &[u8]) -> Option<usize> {
    buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|position| position + 4)
}

/// Write a complete response and close the connection.
///
/// # Errors
///
/// The socket failed.
pub fn respond(
    stream: &mut TcpStream,
    status: u16,
    reason: &str,
    content_type: &str,
    body: &[u8],
) -> io::Result<()> {
    write!(
        stream,
        "HTTP/1.1 {status} {reason}\r\n\
         Content-Type: {content_type}\r\n\
         Content-Length: {}\r\n\
         Cache-Control: no-store\r\n\
         Connection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(body)?;
    stream.flush()
}

/// A `200` with a content type.
///
/// # Errors
///
/// The socket failed.
pub fn ok(stream: &mut TcpStream, content_type: &str, body: &[u8]) -> io::Result<()> {
    respond(stream, 200, "OK", content_type, body)
}

/// A `200 application/json`.
///
/// # Errors
///
/// The socket failed.
pub fn json(stream: &mut TcpStream, body: &str) -> io::Result<()> {
    ok(stream, "application/json", body.as_bytes())
}

/// A `404`.
///
/// # Errors
///
/// The socket failed.
pub fn not_found(stream: &mut TcpStream) -> io::Result<()> {
    respond(
        stream,
        404,
        "Not Found",
        "text/plain; charset=utf-8",
        b"not found\n",
    )
}

/// A `400`.
///
/// # Errors
///
/// The socket failed.
pub fn bad_request(stream: &mut TcpStream) -> io::Result<()> {
    respond(
        stream,
        400,
        "Bad Request",
        "text/plain; charset=utf-8",
        b"bad request\n",
    )
}

/// A `405`.
///
/// # Errors
///
/// The socket failed.
pub fn method_not_allowed(stream: &mut TcpStream) -> io::Result<()> {
    respond(
        stream,
        405,
        "Method Not Allowed",
        "text/plain; charset=utf-8",
        b"method not allowed\n",
    )
}

/// A JSON response with an arbitrary status.
///
/// # Errors
///
/// The socket failed.
pub fn respond_json(
    stream: &mut TcpStream,
    status: u16,
    reason: &str,
    body: &str,
) -> io::Result<()> {
    respond(stream, status, reason, "application/json", body.as_bytes())
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;

    use super::*;

    #[test]
    fn a_get_with_headers_parses() {
        let request = parse_request(
            b"GET /api/book?x=1 HTTP/1.1\r\nHost: localhost\r\n\
              Sec-WebSocket-Key: abc\r\n\r\n",
        )
        .expect("parsed");
        assert_eq!("GET", request.method);
        assert_eq!("/api/book", request.path);
        assert_eq!(Some("abc"), request.header("sec-websocket-key"));
        assert!(request.body.is_empty());
    }

    #[test]
    fn a_head_split_across_reads_is_found() {
        assert_eq!(Some(8), find_head_end(b"GET \r\n\r\n"));
        assert_eq!(None, find_head_end(b"GET /\r\n"));
    }

    /// One connection whose writes arrive in the given pieces.
    fn read_pieces(pieces: &[&[u8]]) -> io::Result<Request> {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let address = listener.local_addr().expect("address");
        let mut writer = std::net::TcpStream::connect(address).expect("connect");
        let (mut reader, _) = listener.accept().expect("accept");
        for piece in pieces {
            writer.write_all(piece).expect("write");
        }
        let request = read_request(&mut reader, Duration::from_secs(1))?;
        request.ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "closed"))
    }

    #[test]
    fn a_post_body_is_read_after_the_head() {
        let request = read_pieces(&[
            b"POST /api/kill HTTP/1.1\r\nContent-Length: 13\r\n\r\n",
            b"{\"account\":7}",
        ])
        .expect("read");
        assert_eq!("POST", request.method);
        assert_eq!("/api/kill", request.path);
        assert_eq!(b"{\"account\":7}".to_vec(), request.body);
    }

    #[test]
    fn a_body_sent_with_the_head_is_kept() {
        let request = read_pieces(&[b"POST /api/kill HTTP/1.1\r\nContent-Length: 2\r\n\r\n{}"])
            .expect("read");
        assert_eq!(b"{}".to_vec(), request.body);
    }

    #[test]
    fn a_body_larger_than_the_budget_is_refused() {
        let head = format!(
            "POST /api/kill HTTP/1.1\r\nContent-Length: {}\r\n\r\n",
            MAX_BODY + 1
        );
        let error = read_pieces(&[head.as_bytes()]).expect_err("refused");
        assert_eq!(io::ErrorKind::InvalidData, error.kind());
    }
}
