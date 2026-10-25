//! Completion event loop: epoll and io_uring behind one trait (M5).
//!
//! The gateway drives both backends the same way: it arms an accept, arms one
//! read per connection, submits a write, and consumes completions. The epoll
//! backend is readiness-based underneath and turns readiness into the same
//! completions by performing the non-blocking read/write when `wait` returns;
//! the io_uring backend submits real operations and reports their results.
//!
//! # The unsafe boundary
//!
//! This crate is the only place that needs `unsafe`: wrapping an accepted raw
//! file descriptor in an [`OwnedFd`] and zeroing the `sockaddr_storage` the
//! kernel fills in. Both are marked and justified inline. Everything the
//! gateway touches is safe.

use std::collections::{HashMap, VecDeque};
use std::io;
use std::net::{SocketAddr, TcpListener};
use std::time::Duration;

use mio::net::TcpListener as MioListener;
use mio::net::TcpStream as MioStream;
use mio::{Events, Interest, Poll, Token};

mod uring;

pub use uring::UringLoop;

/// One read may take this many bytes; the FIX decoder buffers the rest.
pub const READ_BUFFER: usize = 64 * 1024;

/// Which backend [`open`] builds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum BackendKind {
    /// `mio` over epoll: readiness turned into completions.
    #[default]
    Epoll,
    /// `io_uring`: real completion operations.
    Uring,
}

/// One completion the loop produced.
#[derive(Debug)]
pub enum Event {
    /// A connection was accepted; its id is the handle for every other call.
    Accepted {
        /// The backend's connection id.
        id: u64,
        /// The peer's address.
        peer: SocketAddr,
    },
    /// An accept failed (the acceptor re-arms and logs).
    AcceptFailed(io::Error),
    /// A read finished; the bytes are in [`EventLoop::read_data`].
    Read {
        /// Connection id.
        id: u64,
        /// Bytes read, `Ok(0)` for end of stream, or the error.
        result: io::Result<usize>,
    },
    /// A write finished.
    Written {
        /// Connection id.
        id: u64,
        /// Bytes written, or the error.
        result: io::Result<usize>,
    },
}

/// A completion event loop.
pub trait EventLoop: Send {
    /// The bound address.
    ///
    /// # Errors
    ///
    /// The listener has no address.
    fn local_addr(&self) -> io::Result<SocketAddr>;

    /// Arm the next accept.
    ///
    /// # Errors
    ///
    /// The accept operation could not be submitted.
    fn start_accept(&mut self) -> io::Result<()>;

    /// Arm one read for `id`; its bytes arrive in [`Self::read_data`].
    ///
    /// # Errors
    ///
    /// The read could not be armed.
    fn start_read(&mut self, id: u64) -> io::Result<()>;

    /// Submit `bytes` for `id`. The bytes are copied.
    ///
    /// # Errors
    ///
    /// The write could not be submitted.
    fn submit_write(&mut self, id: u64, bytes: &[u8]) -> io::Result<()>;

    /// The bytes of the last completed read for `id`.
    fn read_data(&self, id: u64) -> &[u8];

    /// Close `id`. An io_uring close waits for outstanding operations.
    fn close(&mut self, id: u64);

    /// Wait for completions, appending them to `events`.
    ///
    /// # Errors
    ///
    /// The poller failed.
    fn wait(&mut self, timeout: Duration, events: &mut Vec<Event>) -> io::Result<()>;
}

/// Bind `addr` with the chosen backend.
///
/// # Errors
///
/// The address cannot be bound, or io_uring is unavailable on this kernel.
pub fn open(kind: BackendKind, addr: SocketAddr) -> io::Result<Box<dyn EventLoop>> {
    match kind {
        BackendKind::Epoll => Ok(Box::new(EpollLoop::bind(addr)?)),
        BackendKind::Uring => Ok(Box::new(UringLoop::bind(addr)?)),
    }
}

const LISTENER_TOKEN: Token = Token(0);
const EVENT_CAPACITY: usize = 256;

// ---------------------------------------------------------------------------
// epoll

struct EpollConnection {
    stream: MioStream,
    read_buffer: Vec<u8>,
    write_buffer: Vec<u8>,
    write_offset: usize,
    reading: bool,
    writing: bool,
}

/// `mio`/epoll: readiness converted to completions.
pub struct EpollLoop {
    poll: Poll,
    listener: MioListener,
    events: Events,
    connections: HashMap<u64, EpollConnection>,
    next_id: u64,
    accepting: bool,
    /// Completions produced while submitting, drained by the next `wait`.
    pending: VecDeque<Event>,
}

impl EpollLoop {
    /// Bind `addr`.
    ///
    /// # Errors
    ///
    /// The address cannot be bound or the poller cannot be created.
    pub fn bind(addr: SocketAddr) -> io::Result<Self> {
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        let mut listener = MioListener::from_std(listener);
        let poll = Poll::new()?;
        poll.registry()
            .register(&mut listener, LISTENER_TOKEN, Interest::READABLE)?;
        Ok(Self {
            poll,
            listener,
            events: Events::with_capacity(EVENT_CAPACITY),
            connections: HashMap::new(),
            next_id: 1,
            accepting: false,
            pending: VecDeque::new(),
        })
    }

    fn update_interest(&mut self, id: u64) -> io::Result<()> {
        let Some((reading, writing)) = self
            .connections
            .get(&id)
            .map(|connection| (connection.reading, connection.writing))
        else {
            return Ok(());
        };
        if !reading && !writing {
            return Ok(());
        }
        let interest = match (reading, writing) {
            (true, true) => Interest::READABLE | Interest::WRITABLE,
            (true, false) => Interest::READABLE,
            (false, true) => Interest::WRITABLE,
            (false, false) => return Ok(()),
        };
        let token = Token(usize::try_from(id).unwrap_or(usize::MAX));
        if let Some(connection) = self.connections.get_mut(&id) {
            self.poll
                .registry()
                .reregister(&mut connection.stream, token, interest)?;
        }
        Ok(())
    }
}

impl EventLoop for EpollLoop {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    fn start_accept(&mut self) -> io::Result<()> {
        self.accepting = true;
        Ok(())
    }

    fn start_read(&mut self, id: u64) -> io::Result<()> {
        if let Some(connection) = self.connections.get_mut(&id) {
            connection.reading = true;
        }
        self.update_interest(id)
    }

    fn submit_write(&mut self, id: u64, bytes: &[u8]) -> io::Result<()> {
        let Some(connection) = self.connections.get_mut(&id) else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "no such connection",
            ));
        };
        connection.write_buffer.clear();
        connection.write_buffer.extend_from_slice(bytes);
        connection.write_offset = 0;
        connection.writing = true;
        self.update_interest(id)
    }

    fn read_data(&self, id: u64) -> &[u8] {
        self.connections
            .get(&id)
            .map_or(&[], |connection| connection.read_buffer.as_slice())
    }

    fn close(&mut self, id: u64) {
        self.connections.remove(&id);
    }

    fn wait(&mut self, timeout: Duration, events: &mut Vec<Event>) -> io::Result<()> {
        events.extend(self.pending.drain(..));
        if !events.is_empty() {
            return Ok(());
        }
        self.poll.poll(&mut self.events, Some(timeout))?;
        let tokens: Vec<(Token, bool, bool, bool, bool)> = self
            .events
            .iter()
            .map(|event| {
                (
                    event.token(),
                    event.is_readable(),
                    event.is_writable(),
                    event.is_read_closed() || event.is_write_closed(),
                    event.is_error(),
                )
            })
            .collect();
        for (token, readable, writable, closed, error) in tokens {
            if token == LISTENER_TOKEN {
                if self.accepting {
                    self.accepting = false;
                    self.accept(events)?;
                }
                continue;
            }
            let id = u64::try_from(usize::from(token)).unwrap_or(u64::MAX);
            if error || closed {
                let _ = self.update_interest(id);
                events.push(Event::Read {
                    id,
                    result: Err(io::Error::new(
                        io::ErrorKind::ConnectionReset,
                        "connection closed",
                    )),
                });
                continue;
            }
            if readable {
                self.read_ready(id, events)?;
            }
            if writable {
                self.write_ready(id, events)?;
            }
        }
        Ok(())
    }
}

impl EpollLoop {
    fn accept(&mut self, events: &mut Vec<Event>) -> io::Result<()> {
        loop {
            match self.listener.accept() {
                Ok((stream, peer)) => {
                    let id = self.next_id;
                    self.next_id += 1;
                    let token = Token(usize::try_from(id).unwrap_or(usize::MAX));
                    let mut stream = stream;
                    stream.set_nodelay(true).ok();
                    if let Err(error) =
                        self.poll
                            .registry()
                            .register(&mut stream, token, Interest::READABLE)
                    {
                        events.push(Event::AcceptFailed(error));
                        continue;
                    }
                    self.connections.insert(
                        id,
                        EpollConnection {
                            stream,
                            read_buffer: vec![0_u8; READ_BUFFER],
                            write_buffer: Vec::new(),
                            write_offset: 0,
                            reading: true,
                            writing: false,
                        },
                    );
                    events.push(Event::Accepted { id, peer });
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => {
                    events.push(Event::AcceptFailed(error));
                    break;
                }
            }
        }
        Ok(())
    }

    fn read_ready(&mut self, id: u64, events: &mut Vec<Event>) -> io::Result<()> {
        let Some(connection) = self.connections.get_mut(&id) else {
            return Ok(());
        };
        if !connection.reading {
            // A readiness event queued before the last read was consumed.
            return Ok(());
        }
        connection.reading = false;
        let result = {
            use std::io::Read;
            connection.stream.read(&mut connection.read_buffer)
        };
        if matches!(&result, Err(error) if error.kind() == io::ErrorKind::WouldBlock) {
            // Spurious readiness: re-arm instead of reporting a broken
            // connection.
            if let Some(connection) = self.connections.get_mut(&id) {
                connection.reading = true;
            }
            let _ = self.update_interest(id);
            return Ok(());
        }
        let _ = self.update_interest(id);
        events.push(Event::Read { id, result });
        Ok(())
    }

    fn write_ready(&mut self, id: u64, events: &mut Vec<Event>) -> io::Result<()> {
        let Some(connection) = self.connections.get_mut(&id) else {
            return Ok(());
        };
        if !connection.writing {
            // A readiness event queued before the write completed.
            return Ok(());
        }
        let result = {
            use std::io::Write;
            if connection.write_offset >= connection.write_buffer.len() {
                Ok(0)
            } else {
                connection
                    .stream
                    .write(&connection.write_buffer[connection.write_offset..])
            }
        };
        let written = match &result {
            Ok(count) => *count,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                return Ok(());
            }
            Err(_) => 0,
        };
        connection.write_offset += written;
        if connection.write_offset >= connection.write_buffer.len() {
            connection.writing = false;
        }
        let _ = self.update_interest(id);
        events.push(Event::Written { id, result });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpStream;

    fn round_trip(kind: BackendKind) {
        let mut io = match open(kind, "127.0.0.1:0".parse().expect("address")) {
            Ok(io) => io,
            Err(error) => {
                eprintln!("SKIPPED {kind:?}: {error}");
                return;
            }
        };
        io.start_accept().expect("accept");
        let address = io.local_addr().expect("address");
        let mut client = TcpStream::connect(address).expect("connect");
        client.set_nodelay(true).expect("nodelay");

        let mut events = Vec::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut id = None;
        while std::time::Instant::now() < deadline && id.is_none() {
            events.clear();
            io.wait(Duration::from_millis(100), &mut events)
                .expect("wait");
            for event in &events {
                if let Event::Accepted { id: accepted, .. } = event {
                    id = Some(*accepted);
                }
            }
        }
        let id = id.expect("accepted");
        io.start_read(id).expect("read");

        client.write_all(b"hello").expect("write");
        let mut received = Vec::new();
        while std::time::Instant::now() < deadline {
            events.clear();
            io.wait(Duration::from_millis(100), &mut events)
                .expect("wait");
            for event in &events {
                if let Event::Read {
                    id: event_id,
                    result,
                } = event
                {
                    assert_eq!(id, *event_id);
                    let count = result.as_ref().expect("read");
                    received.extend_from_slice(&io.read_data(id)[..*count]);
                }
            }
            if !received.is_empty() {
                break;
            }
        }
        assert_eq!(b"hello", received.as_slice());

        io.submit_write(id, &received).expect("write");
        let mut written = false;
        while std::time::Instant::now() < deadline && !written {
            events.clear();
            io.wait(Duration::from_millis(100), &mut events)
                .expect("wait");
            for event in &events {
                if let Event::Written {
                    id: event_id,
                    result,
                } = event
                {
                    assert_eq!(id, *event_id);
                    assert_eq!(&5, result.as_ref().expect("written"));
                    written = true;
                }
            }
        }
        assert!(written, "no write completion");

        let mut echoed = [0_u8; 5];
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("timeout");
        client.read_exact(&mut echoed).expect("echo");
        assert_eq!(b"hello", &echoed);
        io.close(id);
    }

    #[test]
    fn epoll_round_trips() {
        round_trip(BackendKind::Epoll);
    }

    #[test]
    fn uring_round_trips_or_is_unavailable() {
        round_trip(BackendKind::Uring);
    }
}
