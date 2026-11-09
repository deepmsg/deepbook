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
use std::sync::Arc;
use std::time::{Duration, Instant};

use deepbook_fix::ApplicationMessage;
use deepbook_fix::clock::{Clock, SystemClock};
use deepbook_fix::frame::FrameDecoder;
use deepbook_fix::journal::{FileJournal, FileSequenceStore, SyncPolicy};
use deepbook_fix::session::{Action, Session, SessionConfig};
use deepbook_io::{BackendKind, Event, EventLoop};

use crate::metrics::GatewayMetrics;
use crate::obs;
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
    /// Stop reading a peer's orders when its outbound report queue reaches
    /// this many bytes; resume below half. Reports are never dropped.
    pub max_outbound_bytes: usize,
    /// The I/O backend.
    pub backend: BackendKind,
}

impl GatewayConfig {
    /// A configuration with the design's defaults: heartbeat 30 s, journal
    /// 4096, a 100 ms tick, a 10 s logon timeout, a 16 MiB outbound
    /// high-water mark and epoll.
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
            max_outbound_bytes: MAX_OUTBOUND_BYTES,
            backend: BackendKind::Epoll,
        }
    }
}

struct Connection {
    key: SessionKey,
    /// The peer that opened the TCP connection; carried for the log only.
    peer: SocketAddr,
    decoder: FrameDecoder,
    session: Session<FileJournal, FileSequenceStore>,
    outbound: Outbound,
    writing: bool,
    close_after_flush: bool,
    /// Whether the outbound high-water mark stopped the read side.
    read_paused: bool,
    /// Whether the facade was told this session logged on, so the close is
    /// announced exactly once and never without an open.
    announced_open: bool,
}

/// The reports waiting for one connection, in order.
///
/// One contiguous buffer, not one `Vec` per report. `start` is the first byte
/// not yet submitted; `chunk` is the range currently handed to the backend
/// (it may be partially written). The bytes stay in `bytes` until the chunk
/// completes, so a partial write can resubmit its remainder.
#[derive(Debug, Default)]
struct Outbound {
    bytes: Vec<u8>,
    start: usize,
    chunk: Option<(usize, usize)>,
}

impl Outbound {
    fn append(&mut self, raw: &[u8]) {
        self.bytes.extend_from_slice(raw);
    }

    /// Bytes queued, including the part of the in-flight chunk not yet written.
    fn queued(&self) -> usize {
        let unsent = self.bytes.len().saturating_sub(self.start);
        let in_flight = self
            .chunk
            .map_or(0, |(start, end)| end.saturating_sub(start));
        unsent.saturating_add(in_flight)
    }

    fn has_unsent(&self) -> bool {
        self.start < self.bytes.len()
    }

    fn chunk(&self) -> Option<(usize, usize)> {
        self.chunk
    }

    /// Start the next chunk of at most `max` bytes; returns its range.
    ///
    /// Only called when no chunk is in flight (or its completion cleared it).
    fn begin_chunk(&mut self, max: usize) -> Option<(usize, usize)> {
        self.chunk = None;
        self.compact();
        if !self.has_unsent() {
            return None;
        }
        let start = self.start;
        let end = start.saturating_add(max).min(self.bytes.len());
        if end <= start {
            return None;
        }
        self.start = end;
        self.chunk = Some((start, end));
        Some((start, end))
    }

    /// Account for `count` written bytes; true when the chunk is complete.
    fn written(&mut self, count: usize) -> bool {
        let Some((start, end)) = &mut self.chunk else {
            return true;
        };
        let advance = count.min(end.saturating_sub(*start));
        *start += advance;
        if *start >= *end {
            self.chunk = None;
            true
        } else {
            false
        }
    }

    fn finished(&self) -> bool {
        self.chunk.is_none() && !self.has_unsent()
    }

    fn compact(&mut self) {
        if self.chunk.is_some() {
            return;
        }
        if self.start >= self.bytes.len() {
            self.bytes.clear();
            self.start = 0;
        } else if self.start >= 64 * 1024 {
            self.bytes.drain(..self.start);
            self.start = 0;
        }
    }
}

impl Connection {
    fn finished(&self) -> bool {
        self.close_after_flush && self.outbound.finished() && !self.writing
    }

    fn queued_bytes(&self) -> usize {
        self.outbound.queued()
    }
}

/// One acceptor thread's state.
pub struct Gateway<E: Engine> {
    io: Box<dyn EventLoop>,
    config: GatewayConfig,
    key: SessionKey,
    engine: E,
    connections: HashMap<u64, Connection>,
    /// Reports that arrived while their FIX session had no connection.
    ///
    /// An execution report is an engine outcome; it cannot be reconstructed
    /// from the FIX journal, which only holds messages that reached a live
    /// session. A short disconnect therefore queues the report and a reconnect
    /// sends it, rather than losing it.
    pending_reports: HashMap<SessionKey, VecDeque<ApplicationMessage>>,
    /// What the loopback `/metrics` page reads.
    metrics: Arc<GatewayMetrics>,
}

/// The most reports queued for one disconnected session. A peer that never
/// returns must not grow the gateway without bound; the oldest report is
/// dropped (and logged) when the bound is reached.
const MAX_PENDING_REPORTS: usize = 4_096;

/// The most bytes coalesced into one backend write. Reports are ~150–200 B, so
/// this is a few hundred reports per submission; the epoll/uring backends copy
/// the bytes, and one 64 KiB copy replaces hundreds of report-sized ones.
const MAX_WRITE_BYTES: usize = 64 * 1024;

/// The default outbound high-water mark per connection: 16 MiB.
const MAX_OUTBOUND_BYTES: usize = 16 * 1024 * 1024;

impl<E: Engine> Gateway<E> {
    /// Bind and arm the first accept.
    ///
    /// # Errors
    ///
    /// The address cannot be bound, the backend cannot be created, or the
    /// accept cannot be armed.
    pub fn bind(config: GatewayConfig, engine: E) -> io::Result<Self> {
        Self::bind_with_metrics(config, engine, Arc::new(GatewayMetrics::default()))
    }

    /// The same, with the counters the binary's `/metrics` page reads.
    ///
    /// # Errors
    ///
    /// The address cannot be bound, the backend cannot be created, or the
    /// accept cannot be armed.
    pub fn bind_with_metrics(
        config: GatewayConfig,
        engine: E,
        metrics: Arc<GatewayMetrics>,
    ) -> io::Result<Self> {
        let mut io = deepbook_io::open(config.backend, config.bind)?;
        io.start_accept()?;
        let key = SessionKey::new(&config.sender_comp_id, &config.target_comp_id);
        Ok(Self {
            io,
            config,
            key,
            engine,
            connections: HashMap::new(),
            pending_reports: HashMap::new(),
            metrics,
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
        work += self.flush_pending_reports();
        work += self.drain_engine_events()?;
        self.flush_writes();
        self.update_all_read_pauses();
        self.metrics.set_outbound_bytes(self.outbound_bytes());
        self.metrics
            .set_pending_commands(self.engine.pending_commands());
        self.metrics
            .set_cluster_orders_len(self.engine.orders_len());
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
                peer,
                decoder: FrameDecoder::new(),
                session,
                outbound: Outbound::default(),
                writing: false,
                close_after_flush: false,
                read_paused: false,
                announced_open: false,
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
        if let Some(connection) = self.connections.remove(&id) {
            if connection.announced_open {
                self.metrics.note_session_close();
                obs::session_close(
                    connection.peer,
                    &self.config.sender_comp_id,
                    &self.config.target_comp_id,
                );
            }
        }
        self.io.close(id);
    }

    /// Announce a session that just logged on.
    ///
    /// The FIX session becomes active on the peer's `Logon` (or its `Logout`'s
    /// acknowledge), which is inside `on_inbound`; the connection itself is
    /// older than that. Emitting here keeps `session.open` meaning "this peer
    /// can trade", not "a socket was accepted".
    fn note_logon(&mut self, id: u64) -> bool {
        let Some(connection) = self.connections.get_mut(&id) else {
            return false;
        };
        if connection.announced_open || !connection.session.is_active() {
            return false;
        }
        connection.announced_open = true;
        self.metrics.note_session_open();
        obs::session_open(
            connection.peer,
            &self.config.sender_comp_id,
            &self.config.target_comp_id,
        );
        true
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
        self.metrics.note_fix_bytes(count);
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
            if matches!(message.msg_type(), Ok(kind) if !kind.is_admin()) {
                self.metrics.note_fix_order_decoded();
            }
            let result = self
                .connections
                .get_mut(&id)
                .map(|connection| connection.session.on_inbound(message.raw(), now));
            let Some(result) = result else {
                break;
            };
            match result {
                Ok(actions) => {
                    work += self.apply_actions(id, actions);
                    self.note_logon(id);
                }
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
        self.update_read_pause(id);
        if self
            .connections
            .get(&id)
            .is_some_and(|connection| !connection.close_after_flush && !connection.read_paused)
            && self.io.start_read(id).is_err()
        {
            self.drop_connection(id);
            work += 1;
        }
        work
    }

    fn write_completion(&mut self, id: u64, result: io::Result<usize>) -> usize {
        match result {
            Ok(0) => {
                self.drop_connection(id);
            }
            Ok(count) => {
                self.metrics
                    .note_fix_bytes_written(u64::try_from(count).unwrap_or(u64::MAX));
                if let Some(connection) = self.connections.get_mut(&id) {
                    connection.outbound.written(count);
                    connection.writing = false;
                }
                self.submit_next_write(id);
                self.update_read_pause(id);
            }
            Err(_) => {
                self.drop_connection(id);
            }
        }
        1
    }

    /// Submit the next chunk for `id` if no write is in flight.
    ///
    /// A partially written chunk continues where the backend stopped — the
    /// backend copies what it is given, so the remainder is submitted again.
    /// Otherwise the next [`MAX_WRITE_BYTES`]-sized slice of the contiguous
    /// outbound buffer is submitted. Submissions happen at the end of a poll
    /// cycle, not per report, so the reports a cycle produced travel together.
    fn submit_next_write(&mut self, id: u64) {
        let Some(connection) = self.connections.get(&id) else {
            return;
        };
        if connection.writing {
            if connection.outbound.has_unsent() {
                self.metrics.note_write_backpressure();
            }
            return;
        }

        if let Some((start, end)) = connection.outbound.chunk() {
            if start < end {
                let bytes = &connection.outbound.bytes[start..end];
                match self.io.submit_write(id, bytes) {
                    Ok(()) => {
                        if let Some(connection) = self.connections.get_mut(&id) {
                            connection.writing = true;
                        }
                    }
                    Err(_) => self.drop_connection(id),
                }
                return;
            }
        }

        let Some((start, end)) = self
            .connections
            .get_mut(&id)
            .and_then(|connection| connection.outbound.begin_chunk(MAX_WRITE_BYTES))
        else {
            return;
        };
        let bytes = &self
            .connections
            .get(&id)
            .expect("the connection was just present")
            .outbound
            .bytes[start..end];
        match self.io.submit_write(id, bytes) {
            Ok(()) => {
                if let Some(connection) = self.connections.get_mut(&id) {
                    connection.writing = true;
                }
            }
            Err(_) => self.drop_connection(id),
        }
    }

    /// Submit whatever the cycle queued. Called once per poll cycle, after the
    /// engine events and the timers, so those reports share one chunk.
    fn flush_writes(&mut self) {
        let ids: Vec<u64> = self.connections.keys().copied().collect();
        for id in ids {
            self.submit_next_write(id);
        }
    }

    /// Stop arming reads on a connection whose report queue is at its
    /// high-water mark, and resume below half. Reports are never dropped.
    fn update_read_pause(&mut self, id: u64) {
        let Some(connection) = self.connections.get(&id) else {
            return;
        };
        let high = self.config.max_outbound_bytes;
        if high == 0 {
            return;
        }
        let queued = connection.queued_bytes();
        if !connection.read_paused && queued >= high {
            if let Some(connection) = self.connections.get_mut(&id) {
                connection.read_paused = true;
            }
            let _ = self.io.stop_read(id);
            self.metrics.note_outbound_blocked();
        } else if connection.read_paused && queued <= high / 2 {
            if let Some(connection) = self.connections.get_mut(&id) {
                connection.read_paused = false;
            }
            let _ = self.io.start_read(id);
        }
    }

    fn update_all_read_pauses(&mut self) {
        let ids: Vec<u64> = self.connections.keys().copied().collect();
        for id in ids {
            self.update_read_pause(id);
        }
    }

    /// Bytes queued for FIX peers, including the chunk in flight.
    fn outbound_bytes(&self) -> u64 {
        self.connections
            .values()
            .map(|connection| u64::try_from(connection.queued_bytes()).unwrap_or(u64::MAX))
            .sum()
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
                        connection.outbound.append(&raw);
                    }
                }
                Action::Deliver(message) => {
                    let key = self
                        .connections
                        .get(&id)
                        .map(|connection| connection.key.clone());
                    if let Some(key) = key {
                        self.metrics.note_order();
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
        work
    }

    /// Give the engine's reports back to the session that owns them.
    fn drain_engine_events(&mut self) -> io::Result<usize> {
        let now = SystemClock.now_millis();
        let mut work = 0;
        while let Some(event) = self.engine.poll_event() {
            self.metrics.note_report();
            let Some(id) = self
                .connections
                .iter()
                .find(|(_, connection)| connection.key == event.session)
                .map(|(id, _)| *id)
            else {
                // No live connection for the session: keep the report until
                // the peer reconnects instead of dropping an engine outcome.
                let key = event.session.clone();
                let queue = self.pending_reports.entry(key.clone()).or_default();
                if queue.len() >= MAX_PENDING_REPORTS {
                    queue.pop_front();
                    eprintln!(
                        "gateway: pending reports for {} are full; dropping the oldest",
                        key.directory_name()
                    );
                }
                queue.push_back(event.message);
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

    /// Send reports queued while their session was disconnected.
    ///
    /// Only an active (logged-on) session can take application messages; the
    /// order within a session is preserved because the queue is FIFO and old
    /// reports are sent before `drain_engine_events` pulls new ones.
    fn flush_pending_reports(&mut self) -> usize {
        if self.pending_reports.is_empty() {
            return 0;
        }
        let now = SystemClock.now_millis();
        let active: Vec<(u64, SessionKey)> = self
            .connections
            .iter()
            .filter(|(_, connection)| connection.session.is_active())
            .map(|(id, connection)| (*id, connection.key.clone()))
            .collect();
        let mut work = 0;
        for (id, key) in active {
            while let Some(message) = self
                .pending_reports
                .get_mut(&key)
                .and_then(std::collections::VecDeque::pop_front)
            {
                let action = self
                    .connections
                    .get_mut(&id)
                    .map(|connection| connection.session.send_application(&message, now));
                match action {
                    Some(Ok(action)) => work += self.apply_actions(id, vec![action]),
                    Some(Err(error)) => {
                        // Put it back: the session is not ready after all.
                        eprintln!("gateway: pending report: {error}");
                        if let Some(queue) = self.pending_reports.get_mut(&key) {
                            queue.push_front(message);
                        }
                        break;
                    }
                    None => break,
                }
            }
        }
        self.pending_reports.retain(|_, queue| !queue.is_empty());
        work
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
        assert_eq!(16 * 1024 * 1024, config.max_outbound_bytes);
    }

    #[test]
    fn the_outbound_buffer_chunks_without_per_report_vectors() {
        let mut outbound = Outbound::default();
        outbound.append(&[1; 3]);
        outbound.append(&[2; 5]);
        assert_eq!(8, outbound.queued());

        let (start, end) = outbound.begin_chunk(6).expect("a chunk");
        assert_eq!((0, 6), (start, end));
        assert_eq!(8, outbound.queued(), "6 in flight + 2 unsent");

        assert!(!outbound.written(2));
        assert_eq!((2, 6), outbound.chunk().expect("still in flight"));
        assert_eq!(6, outbound.queued(), "2 unsent + 4 in flight");

        assert!(outbound.written(4));
        assert_eq!(2, outbound.queued(), "2 unsent");

        let (start, end) = outbound.begin_chunk(6).expect("the rest");
        assert_eq!((6, 8), (start, end));
        assert!(outbound.written(2));
        assert!(outbound.finished());
        assert_eq!(0, outbound.queued());
    }

    #[test]
    fn a_partial_chunk_keeps_the_remainder() {
        let mut outbound = Outbound::default();
        outbound.append(&[7; 10]);
        assert_eq!((0, 4), outbound.begin_chunk(4).expect("chunk"));

        // A one-byte write leaves the same chunk, starting at the next byte.
        assert!(!outbound.written(1));
        assert_eq!((1, 4), outbound.chunk().expect("remainder"));
        assert_eq!(9, outbound.queued());

        assert!(outbound.written(3));
        assert_eq!(6, outbound.queued());
    }
}
