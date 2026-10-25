//! The single-threaded FIX acceptor.
//!
//! One thread owns every connection. `mio`'s Linux backend is epoll; the loop
//! is deliberately the only place that blocks. Each connection has its own
//! [`FrameDecoder`] and its own [`Session`], and every session is backed by a
//! journal directory keyed by the CompID pair, so a reconnect resumes the same
//! session rather than starting a new one.
//!
//! The acceptor does not decide what an accepted order becomes.
//! `Action::Deliver` goes to the [`Engine`]; the engine publishes reports
//! through its [`crate::EventSource`], and `drain_engine_events` sends each
//! one back to the session that owns it. M1c's [`crate::StubEngine`] is the
//! in-process implementation; M2 replaces it with `engine-core` and M4 with
//! the cluster client behind the same seam.
//!
//! M1b accepts the **one** CompID pair named by [`GatewayConfig`]. The
//! registry key and journal directory are already per pair; letting a gateway
//! host several peers is a later slice, not a rewrite.

use std::collections::{HashMap, VecDeque};
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use deepbook_fix::clock::{Clock, SystemClock};
use deepbook_fix::journal::{FileJournal, FileSequenceStore};
use deepbook_fix::session::{Action, Session, SessionConfig};
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};

use crate::frame::FrameDecoder;
use crate::sink::{Engine, SessionKey};

const LISTENER_TOKEN: Token = Token(0);
const EVENT_CAPACITY: usize = 256;
const READ_BUFFER: usize = 8_192;

/// The acceptor's configuration.
#[derive(Clone, Debug)]
pub struct GatewayConfig {
    /// The address to listen on.
    pub bind: SocketAddr,
    /// Where the per-session journals and sequence files live.
    pub session_dir: PathBuf,
    /// Our `SenderCompID`.
    pub sender_comp_id: String,
    /// The expected peer `TargetCompID`.
    pub target_comp_id: String,
    /// Heartbeat interval before the peer's `108` arrives.
    pub heart_bt_int: u32,
    /// Outbound journal entries kept per session.
    pub journal_capacity: usize,
    /// The longest `epoll_wait`; also the heartbeat timer granularity.
    pub tick: Duration,
    /// How long a connection may sit without a Logon.
    pub logon_timeout_millis: i64,
}

impl GatewayConfig {
    /// A configuration with the design's defaults: heartbeat 30 s, journal
    /// 4096, a 100 ms tick and a 10 s logon timeout.
    #[must_use]
    pub fn new(
        bind: SocketAddr,
        session_dir: impl Into<PathBuf>,
        sender_comp_id: impl Into<String>,
        target_comp_id: impl Into<String>,
    ) -> Self {
        Self {
            bind,
            session_dir: session_dir.into(),
            sender_comp_id: sender_comp_id.into(),
            target_comp_id: target_comp_id.into(),
            heart_bt_int: 30,
            journal_capacity: 4_096,
            tick: Duration::from_millis(100),
            logon_timeout_millis: 10_000,
        }
    }
}

struct Connection {
    stream: TcpStream,
    key: SessionKey,
    decoder: FrameDecoder,
    session: Session<FileJournal, FileSequenceStore>,
    outbound: VecDeque<Vec<u8>>,
    interest: Interest,
    close_after_flush: bool,
}

impl Connection {
    /// Write everything the socket will take. `true` means the connection is
    /// broken and must be dropped.
    fn flush(&mut self) -> bool {
        let Self {
            stream, outbound, ..
        } = self;
        while let Some(front) = outbound.front_mut() {
            match stream.write(front) {
                Ok(0) => return true,
                Ok(written) => {
                    front.drain(..written);
                    if front.is_empty() {
                        outbound.pop_front();
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return false,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(_) => return true,
            }
        }
        false
    }

    fn desired_interest(&self) -> Interest {
        if self.outbound.is_empty() {
            Interest::READABLE
        } else {
            Interest::READABLE | Interest::WRITABLE
        }
    }

    fn finished(&self) -> bool {
        self.close_after_flush && self.outbound.is_empty()
    }
}

/// One acceptor thread's state.
pub struct Gateway<E: Engine> {
    poll: Poll,
    listener: TcpListener,
    config: GatewayConfig,
    key: SessionKey,
    engine: E,
    events: Events,
    connections: HashMap<u64, Connection>,
    next_token: u64,
}

impl<E: Engine> Gateway<E> {
    /// Bind and register the listener.
    ///
    /// # Errors
    ///
    /// The address cannot be bound or the poller cannot be created.
    pub fn bind(config: GatewayConfig, engine: E) -> io::Result<Self> {
        let mut listener = TcpListener::bind(config.bind)?;
        let poll = Poll::new()?;
        poll.registry()
            .register(&mut listener, LISTENER_TOKEN, Interest::READABLE)?;
        let key = SessionKey::new(&config.sender_comp_id, &config.target_comp_id);
        Ok(Self {
            poll,
            listener,
            config,
            key,
            engine,
            events: Events::with_capacity(EVENT_CAPACITY),
            connections: HashMap::new(),
            next_token: 1,
        })
    }

    /// The address the listener is bound to, with port 0 resolved.
    ///
    /// # Errors
    ///
    /// The socket has no address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// How many connections are open.
    #[must_use]
    pub fn connection_count(&self) -> usize {
        self.connections.len()
    }

    /// Run one poll cycle: accept, read, write, then tick every session.
    ///
    /// # Errors
    ///
    /// The poller or the listener failed. A broken connection is closed, not
    /// returned.
    pub fn poll_once(&mut self, timeout: Duration) -> io::Result<usize> {
        let timeout = timeout.min(self.config.tick);
        self.poll.poll(&mut self.events, Some(timeout))?;

        let events: Vec<(Token, bool, bool, bool, bool)> = self
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

        let mut work = 0;
        for (token, readable, writable, closed, error) in events {
            if token == LISTENER_TOKEN {
                work += self.accept_ready()?;
                continue;
            }
            let id = u64::try_from(usize::from(token)).unwrap_or(u64::MAX);
            work += self.process_connection(id, readable, writable, closed, error)?;
        }

        work += self.tick_sessions()?;
        work += self.drain_engine_events()?;
        self.connections
            .retain(|_, connection| !connection.finished());
        Ok(work)
    }

    /// Poll until `deadline`; a test or a supervisor uses this.
    ///
    /// # Errors
    ///
    /// As [`Self::poll_once`].
    pub fn run_until(&mut self, deadline: Instant) -> io::Result<()> {
        let tick = self.config.tick;
        while Instant::now() < deadline {
            self.poll_once(tick)?;
        }
        Ok(())
    }

    /// Poll forever.
    ///
    /// # Errors
    ///
    /// As [`Self::poll_once`].
    pub fn run(&mut self) -> io::Result<()> {
        loop {
            self.poll_once(self.config.tick)?;
        }
    }

    /// Close every connection and stop listening.
    pub fn shutdown(&mut self) {
        self.connections.clear();
        let _ = self.poll.registry().deregister(&mut self.listener);
    }

    fn accept_ready(&mut self) -> io::Result<usize> {
        let mut accepted = 0;
        loop {
            match self.listener.accept() {
                Ok((stream, peer)) => {
                    self.add_connection(stream, peer);
                    accepted += 1;
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
        Ok(accepted)
    }

    fn add_connection(&mut self, mut stream: TcpStream, peer: SocketAddr) {
        // A new TCP connection for the same CompID pair replaces the old
        // session. The old file handles must be closed before the new session
        // opens the same journal.
        let replaced: Vec<u64> = self
            .connections
            .iter()
            .filter(|(_, connection)| connection.key == self.key)
            .map(|(id, _)| *id)
            .collect();
        for id in replaced {
            self.connections.remove(&id);
        }

        let now = SystemClock.now_millis();
        let session = match self.open_session(now) {
            Ok(session) => session,
            Err(error) => {
                eprintln!("gateway: cannot open session for {peer}: {error}");
                return;
            }
        };

        let id = self.next_token;
        self.next_token += 1;
        let token = Token(usize::try_from(id).unwrap_or(usize::MAX));
        if let Err(error) = self
            .poll
            .registry()
            .register(&mut stream, token, Interest::READABLE)
        {
            eprintln!("gateway: cannot register {peer}: {error}");
            return;
        }

        self.connections.insert(
            id,
            Connection {
                stream,
                key: self.key.clone(),
                decoder: FrameDecoder::new(),
                session,
                outbound: VecDeque::new(),
                interest: Interest::READABLE,
                close_after_flush: false,
            },
        );
    }

    fn open_session(
        &self,
        now_millis: i64,
    ) -> Result<Session<FileJournal, FileSequenceStore>, io::Error> {
        let directory = self.config.session_dir.join(self.key.directory_name());
        let journal =
            FileJournal::open(directory.join("journal.bin"), self.config.journal_capacity)
                .map_err(io::Error::other)?;
        let sequences = FileSequenceStore::new(directory.join("sequences"));
        let mut config = SessionConfig::new(
            &self.config.sender_comp_id,
            &self.config.target_comp_id,
            self.config.heart_bt_int,
        );
        config.logon_timeout_millis = self.config.logon_timeout_millis;
        Session::new(config, journal, sequences, now_millis).map_err(io::Error::other)
    }

    fn process_connection(
        &mut self,
        id: u64,
        readable: bool,
        writable: bool,
        closed: bool,
        error: bool,
    ) -> io::Result<usize> {
        if !self.connections.contains_key(&id) {
            return Ok(0);
        }
        if error {
            self.connections.remove(&id);
            return Ok(1);
        }

        let mut work = 0;
        if readable {
            work += self.read_connection(id)?;
        }
        if writable {
            work += 1;
        }
        // A half-close may arrive with the last message. The read above has
        // consumed it; let the reply drain before the descriptor goes.
        if closed {
            if let Some(connection) = self.connections.get_mut(&id) {
                connection.close_after_flush = true;
            }
            work += 1;
        }
        self.flush_connection(id);
        self.update_interest(id)?;
        Ok(work)
    }

    fn read_connection(&mut self, id: u64) -> io::Result<usize> {
        let now = SystemClock.now_millis();
        let mut bytes_read = 0;
        let mut eof = false;
        {
            let Some(connection) = self.connections.get_mut(&id) else {
                return Ok(0);
            };
            let mut buffer = [0_u8; READ_BUFFER];
            loop {
                match connection.stream.read(&mut buffer) {
                    Ok(0) => {
                        eof = true;
                        break;
                    }
                    Ok(count) => {
                        connection.decoder.push(&buffer[..count]);
                        bytes_read += count;
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                    Err(_) => {
                        eof = true;
                        break;
                    }
                }
            }
        }

        let mut work = usize::from(bytes_read > 0);
        loop {
            let frame = self
                .connections
                .get_mut(&id)
                .and_then(|connection| connection.decoder.next_frame());
            let Some(frame) = frame else {
                break;
            };
            let result = {
                let Some(connection) = self.connections.get_mut(&id) else {
                    return Ok(work);
                };
                match frame {
                    Ok(message) => Some(connection.session.on_inbound(message.raw(), now)),
                    Err(_) => None,
                }
            };
            let Some(result) = result else {
                self.connections.remove(&id);
                return Ok(work + 1);
            };
            match result {
                Ok(actions) => work += self.apply_actions(id, actions),
                Err(_) => {
                    self.connections.remove(&id);
                    return Ok(work + 1);
                }
            }
            if self
                .connections
                .get(&id)
                .is_none_or(|connection| connection.close_after_flush)
            {
                break;
            }
        }
        if eof {
            if let Some(connection) = self.connections.get_mut(&id) {
                connection.close_after_flush = true;
            }
            work += 1;
        }
        Ok(work)
    }

    fn tick_sessions(&mut self) -> io::Result<usize> {
        let now = SystemClock.now_millis();
        let ids: Vec<u64> = self.connections.keys().copied().collect();
        let mut work = 0;
        for id in ids {
            let actions = self
                .connections
                .get_mut(&id)
                .map(|connection| connection.session.on_tick(now));
            match actions {
                Some(Ok(actions)) => work += self.apply_actions(id, actions),
                Some(Err(_)) => {
                    self.connections.remove(&id);
                    work += 1;
                }
                None => {}
            }
            self.flush_connection(id);
            self.update_interest(id)?;
        }
        Ok(work)
    }

    fn apply_actions(&mut self, id: u64, actions: Vec<Action>) -> usize {
        let key = self
            .connections
            .get(&id)
            .map(|connection| connection.key.clone());
        let mut work = 0;
        for action in actions {
            work += 1;
            match action {
                Action::Send { raw, .. } => {
                    if let Some(connection) = self.connections.get_mut(&id) {
                        connection.outbound.push_back(raw);
                    }
                }
                Action::Deliver(message) => {
                    if let Some(key) = &key {
                        self.engine.deliver(key, message);
                    }
                }
                Action::Disconnect { .. } => {
                    if let Some(connection) = self.connections.get_mut(&id) {
                        connection.close_after_flush = true;
                    }
                    break;
                }
            }
        }
        work
    }

    /// Give the engine's reports back to the session that owns them.
    fn drain_engine_events(&mut self) -> io::Result<usize> {
        let now = SystemClock.now_millis();
        let mut work = 0;
        while let Some(event) = self.engine.poll_event() {
            let Some(id) = self
                .connections
                .iter()
                .find(|(_, connection)| connection.key == event.session)
                .map(|(id, _)| *id)
            else {
                // The session disconnected before its report came back.
                continue;
            };
            let action = self
                .connections
                .get_mut(&id)
                .map(|connection| connection.session.send_application(&event.message, now));
            if let Some(Ok(action)) = action {
                work += self.apply_actions(id, vec![action]);
            }
            self.flush_connection(id);
            self.update_interest(id)?;
        }
        Ok(work)
    }

    fn flush_connection(&mut self, id: u64) {
        let Some(connection) = self.connections.get_mut(&id) else {
            return;
        };
        if connection.flush() {
            self.connections.remove(&id);
        }
    }

    fn update_interest(&mut self, id: u64) -> io::Result<()> {
        let Self {
            poll, connections, ..
        } = self;
        let Some(connection) = connections.get_mut(&id) else {
            return Ok(());
        };
        let desired = connection.desired_interest();
        if desired != connection.interest {
            let token = Token(usize::try_from(id).unwrap_or(usize::MAX));
            poll.registry()
                .reregister(&mut connection.stream, token, desired)?;
            connection.interest = desired;
        }
        Ok(())
    }

    /// The journal directory of a session.
    #[must_use]
    pub fn session_directory(&self) -> PathBuf {
        self.config.session_dir.join(self.key.directory_name())
    }
}
