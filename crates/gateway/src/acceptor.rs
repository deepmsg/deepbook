//! The single-threaded FIX acceptor over a completion event loop.
//!
//! One thread owns every connection. The loop is [`deepbook_io::EventLoop`]:
//! the default backend is `mio`/epoll, and `--backend uring` selects
//! io_uring. Both report the same completions (accept, read, written), so the
//! FIX logic, the session state machine and the journals do not know which
//! one is underneath.
//!
//! Each connection has its own [`FrameDecoder`] and its own [`Session`], and
//! every session is backed by a journal directory keyed by the CompID pair, so
//! a reconnect resumes the same session rather than starting a new one.
//!
//! The acceptor does not decide what an accepted order becomes.
//! `Action::Deliver` goes to the [`Engine`]; the engine publishes reports
//! through its [`crate::EventSource`], and `drain_engine_events` queues each
//! one on the session that owns it.
//!
//! M1b accepts the **one** CompID pair named by [`GatewayConfig`]. The
//! registry key and journal directory are already per pair; letting a gateway
//! host several peers is a later slice, not a rewrite.

use std::collections::{HashMap, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use deepbook_fix::clock::{Clock, SystemClock};
use deepbook_fix::frame::FrameDecoder;
use deepbook_fix::journal::{FileJournal, FileSequenceStore, SyncPolicy};
use deepbook_fix::session::{Action, Session, SessionConfig};
use deepbook_io::{BackendKind, Event, EventLoop};

use crate::sink::{Engine, SessionKey};

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
    /// The longest poll; also the heartbeat timer granularity.
    pub tick: Duration,
    /// How long a connection may sit without a Logon.
    pub logon_timeout_millis: i64,
    /// The journal and sequence-file durability policy. M1's promise is
    /// [`SyncPolicy::Each`]; M5 measures the batching policies.
    pub journal_sync: SyncPolicy,
    /// The I/O backend.
    pub backend: BackendKind,
}

impl GatewayConfig {
    /// A configuration with the design's defaults: heartbeat 30 s, journal
    /// 4096, a 100 ms tick, a 10 s logon timeout and epoll.
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
            journal_sync: SyncPolicy::Each,
            backend: BackendKind::Epoll,
        }
    }
}

struct Connection {
    key: SessionKey,
    decoder: FrameDecoder,
    session: Session<FileJournal, FileSequenceStore>,
    outbound: VecDeque<Vec<u8>>,
    writing: bool,
    close_after_flush: bool,
}

impl Connection {
    fn finished(&self) -> bool {
        self.close_after_flush && self.outbound.is_empty() && !self.writing
    }
}

/// One acceptor thread's state.
pub struct Gateway<E: Engine> {
    io: Box<dyn EventLoop>,
    config: GatewayConfig,
    key: SessionKey,
    engine: E,
    connections: HashMap<u64, Connection>,
}

impl<E: Engine> Gateway<E> {
    /// Bind and arm the first accept.
    ///
    /// # Errors
    ///
    /// The address cannot be bound, the backend cannot be created, or the
    /// accept cannot be armed.
    pub fn bind(config: GatewayConfig, engine: E) -> io::Result<Self> {
        let mut io = deepbook_io::open(config.backend, config.bind)?;
        io.start_accept()?;
        let key = SessionKey::new(&config.sender_comp_id, &config.target_comp_id);
        Ok(Self {
            io,
            config,
            key,
            engine,
            connections: HashMap::new(),
        })
    }

    /// The address the listener is bound to, with port 0 resolved.
    ///
    /// # Errors
    ///
    /// The socket has no address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.io.local_addr()
    }

    /// How many connections are open.
    #[must_use]
    pub fn connection_count(&self) -> usize {
        self.connections.len()
    }

    /// The backend this gateway runs.
    #[must_use]
    pub const fn backend(&self) -> BackendKind {
        self.config.backend
    }

    /// Run one poll cycle: completions, timers, then engine reports.
    ///
    /// # Errors
    ///
    /// The event loop failed. A broken connection is closed, not returned.
    pub fn poll_once(&mut self, timeout: Duration) -> io::Result<usize> {
        let mut events = Vec::new();
        self.io.wait(timeout.min(self.config.tick), &mut events)?;
        let mut work = 0;
        for event in events {
            match event {
                Event::Accepted { id, peer } => {
                    work += self.add_connection(id, peer);
                }
                Event::AcceptFailed(error) => {
                    eprintln!("gateway: accept: {error}");
                    work += 1;
                }
                Event::Read { id, result } => {
                    work += self.read_completion(id, result);
                }
                Event::Written { id, result } => {
                    work += self.write_completion(id, result);
                }
            }
        }
        // The accept is one-shot on io_uring; arming it here also restarts it
        // after an accept error. epoll only sets a flag.
        self.io.start_accept()?;

        work += self.tick_sessions();
        work += self.drain_engine_events()?;
        self.close_finished();
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

    /// Close every connection and stop accepting.
    pub fn shutdown(&mut self) {
        for id in self.connections.keys().copied().collect::<Vec<_>>() {
            self.io.close(id);
        }
        self.connections.clear();
    }

    /// The journal directory of a session.
    #[must_use]
    pub fn session_directory(&self) -> PathBuf {
        self.config.session_dir.join(self.key.directory_name())
    }

    fn add_connection(&mut self, id: u64, peer: SocketAddr) -> usize {
        // A new TCP connection for the same CompID pair replaces the old
        // session. The old file handles must be closed before the new session
        // opens the same journal.
        let replaced: Vec<u64> = self
            .connections
            .iter()
            .filter(|(_, connection)| connection.key == self.key)
            .map(|(id, _)| *id)
            .collect();
        for replaced in replaced {
            self.drop_connection(replaced);
        }

        let now = SystemClock.now_millis();
        let session = match self.open_session(now) {
            Ok(session) => session,
            Err(error) => {
                eprintln!("gateway: cannot open session for {peer}: {error}");
                self.io.close(id);
                return 1;
            }
        };
        self.connections.insert(
            id,
            Connection {
                key: self.key.clone(),
                decoder: FrameDecoder::new(),
                session,
                outbound: VecDeque::new(),
                writing: false,
                close_after_flush: false,
            },
        );
        if self.io.start_read(id).is_err() {
            self.drop_connection(id);
        }
        1
    }

    fn open_session(
        &self,
        now_millis: i64,
    ) -> Result<Session<FileJournal, FileSequenceStore>, io::Error> {
        let directory = self.config.session_dir.join(self.key.directory_name());
        let journal = FileJournal::open_with_sync(
            directory.join("journal.bin"),
            self.config.journal_capacity,
            self.config.journal_sync,
        )
        .map_err(io::Error::other)?;
        let sequences =
            FileSequenceStore::with_sync(directory.join("sequences"), self.config.journal_sync);
        let mut config = SessionConfig::new(
            &self.config.sender_comp_id,
            &self.config.target_comp_id,
            self.config.heart_bt_int,
        );
        config.logon_timeout_millis = self.config.logon_timeout_millis;
        Session::new(config, journal, sequences, now_millis).map_err(io::Error::other)
    }

    fn drop_connection(&mut self, id: u64) {
        self.connections.remove(&id);
        self.io.close(id);
    }

    fn close_finished(&mut self) {
        let finished: Vec<u64> = self
            .connections
            .iter()
            .filter(|(_, connection)| connection.finished())
            .map(|(id, _)| *id)
            .collect();
        for id in finished {
            self.drop_connection(id);
        }
    }

    fn read_completion(&mut self, id: u64, result: io::Result<usize>) -> usize {
        let count = match result {
            Ok(0) => {
                if let Some(connection) = self.connections.get_mut(&id) {
                    connection.close_after_flush = true;
                }
                return 1;
            }
            Ok(count) => count,
            Err(_) => {
                self.drop_connection(id);
                return 1;
            }
        };
        let bytes = self.io.read_data(id).get(..count).unwrap_or(&[]).to_vec();
        let Some(connection) = self.connections.get_mut(&id) else {
            return 1;
        };
        connection.decoder.push(&bytes);

        let now = SystemClock.now_millis();
        let mut work = 1;
        loop {
            let frame = self
                .connections
                .get_mut(&id)
                .and_then(|connection| connection.decoder.next_frame());
            let Some(frame) = frame else {
                break;
            };
            let Ok(message) = frame else {
                self.drop_connection(id);
                return work;
            };
            let result = self
                .connections
                .get_mut(&id)
                .map(|connection| connection.session.on_inbound(message.raw(), now));
            let Some(result) = result else {
                break;
            };
            match result {
                Ok(actions) => work += self.apply_actions(id, actions),
                Err(_) => {
                    self.drop_connection(id);
                    return work;
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
        if self
            .connections
            .get(&id)
            .is_some_and(|connection| !connection.close_after_flush)
            && self.io.start_read(id).is_err()
        {
            self.drop_connection(id);
            work += 1;
        }
        self.submit_next_write(id);
        work
    }

    fn write_completion(&mut self, id: u64, result: io::Result<usize>) -> usize {
        match result {
            Ok(0) => {
                self.drop_connection(id);
            }
            Ok(count) => {
                if let Some(connection) = self.connections.get_mut(&id) {
                    if let Some(front) = connection.outbound.front_mut() {
                        let count = count.min(front.len());
                        front.drain(..count);
                        if front.is_empty() {
                            connection.outbound.pop_front();
                        }
                    }
                    connection.writing = false;
                }
                self.submit_next_write(id);
            }
            Err(_) => {
                self.drop_connection(id);
            }
        }
        1
    }

    /// Submit the front of `outbound` if no write is in flight.
    fn submit_next_write(&mut self, id: u64) {
        let front = match self.connections.get(&id) {
            Some(connection) if !connection.writing && !connection.outbound.is_empty() => {
                connection.outbound.front().cloned()
            }
            _ => return,
        };
        let Some(front) = front else {
            return;
        };
        match self.io.submit_write(id, &front) {
            Ok(()) => {
                if let Some(connection) = self.connections.get_mut(&id) {
                    connection.writing = true;
                }
            }
            Err(_) => self.drop_connection(id),
        }
    }

    fn tick_sessions(&mut self) -> usize {
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
                    self.drop_connection(id);
                    work += 1;
                }
                None => {}
            }
        }
        work
    }

    fn apply_actions(&mut self, id: u64, actions: Vec<Action>) -> usize {
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
                    let key = self
                        .connections
                        .get(&id)
                        .map(|connection| connection.key.clone());
                    if let Some(key) = key {
                        self.engine.deliver(&key, message);
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
        self.submit_next_write(id);
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
            match action {
                Some(Ok(action)) => work += self.apply_actions(id, vec![action]),
                Some(Err(error)) => eprintln!("gateway: send_application failed: {error}"),
                None => {}
            }
        }
        Ok(work)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_default_config_uses_epoll() {
        let config = GatewayConfig::new(
            "127.0.0.1:0".parse().expect("address"),
            "sessions",
            "DEEPBOOK",
            "CLIENT",
        );
        assert_eq!(BackendKind::Epoll, config.backend);
        assert_eq!(SyncPolicy::Each, config.journal_sync);
    }
}
