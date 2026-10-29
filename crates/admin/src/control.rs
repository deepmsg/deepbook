//! The admin's write path: one cluster session that offers operator commands.
//!
//! The dashboard is a subscriber on the market-data side (ADR 0008) and, from
//! this module, a *client* on the cluster side. `POST /api/kill` queues a
//! [`Command::KillSwitch`]; a dedicated thread owns the deepmsg client (which
//! is not `Send`), keeps the cluster session alive, offers the queue in order
//! and discards the egress it receives.
//!
//! ```text
//! POST /api/kill ──queue──▶ control thread ──offer──▶ cluster ingress
//! ```
//!
//! The command travels the same log as a FIX order, so every replica applies
//! it at the same position and replay after a restart reaches the same state.
//!
//! # Limits of this path
//!
//! - **No acknowledgement.** The engine emits no event for a kill switch, so
//!   the admin can report that a command was *offered* to the cluster, not
//!   that the engine applied it. [`ControlStatus`] is the admin's memory of
//!   what it submitted.
//! - **No un-kill.** `Command::KillSwitch` has no counterpart, and the kill
//!   state is in the snapshot, so resuming means restarting the cluster with
//!   a fresh log. This is the "stop the market" switch, not a pause.
//! - **At least once.** A command stays queued until ingress accepts it; a
//!   reconnect re-offers the queue. A kill is idempotent, so a duplicate is
//!   harmless.
//! - **One session.** Like the gateway (ADR 0006), the admin opens one cluster
//!   session; the engine's service-originated reports are broadcast to it and
//!   thrown away.

use std::cell::RefCell;
use std::collections::{BTreeSet, VecDeque};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use deepbook_engine_core::{Command, encode_command};
use deepbook_types::AccountId;
use deepmsg_client::client::Client;
use deepmsg_client::fragment_assembler::MessageHeader;
use deepmsg_cluster_client::{AeronCluster, AeronClusterContext, EgressListener};
use deepmsg_core::logbuffer::append::Appended;

/// How many commands may wait for the worker before the endpoint refuses one.
pub const MAX_QUEUED_COMMANDS: usize = 64;
/// How often the session is kept alive; the cluster's session timeout is 10 s.
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(3);
/// How long to wait before a failed connect or a closed session is retried.
const RETRY_DELAY: Duration = Duration::from_secs(1);
/// How long the idle loop waits for a request before polling the transport.
const POLL_INTERVAL: Duration = Duration::from_millis(1);

/// Where the control session connects.
#[derive(Clone, Debug)]
pub struct ControlConfig {
    /// The aeron directory of the driver the cluster node uses.
    pub aeron_dir: PathBuf,
    /// `aeron.cluster.ingress.channel`.
    pub ingress_channel: String,
    /// `aeron.cluster.ingress.stream.id`.
    pub ingress_stream_id: i32,
    /// `aeron.cluster.egress.channel`.
    pub egress_channel: String,
    /// `aeron.cluster.egress.stream.id`.
    pub egress_stream_id: i32,
    /// The connect's `aeron.cluster.message.timeout`.
    pub message_timeout_ns: i64,
}

impl ControlConfig {
    /// The single-node defaults: IPC both ways, streams 101/102 (the gateway's
    /// defaults, so one cluster serves both).
    #[must_use]
    pub fn new(aeron_dir: impl Into<PathBuf>) -> Self {
        Self {
            aeron_dir: aeron_dir.into(),
            ingress_channel: "aeron:ipc".to_owned(),
            ingress_stream_id: 101,
            egress_channel: "aeron:ipc".to_owned(),
            egress_stream_id: 102,
            message_timeout_ns: 5_000_000_000,
        }
    }
}

/// What the operator asked for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KillRequest {
    /// Stop every account's new orders.
    All,
    /// Stop one account's new orders.
    Account(AccountId),
}

impl KillRequest {
    /// Parse a request body: empty, `{}` or `{"account": null}` kill the whole
    /// market; `{"account": N}` kills one account.
    ///
    /// # Errors
    ///
    /// The text is not one of those objects. Unknown fields are refused rather
    /// than ignored, so a misspelt scope cannot kill more than was asked.
    pub fn from_body(body: &str) -> Result<Self, String> {
        let body = body.trim();
        if body.is_empty() || body == "{}" {
            return Ok(Self::All);
        }
        let rest = body
            .strip_prefix('{')
            .and_then(|rest| rest.strip_suffix('}'))
            .ok_or_else(|| "the body must be a JSON object".to_owned())?;
        let (name, value) = rest
            .split_once(':')
            .ok_or_else(|| "the body must be {\"account\": <id|null>}".to_owned())?;
        if name.trim() != "\"account\"" {
            return Err(format!("unknown field {}", name.trim()));
        }
        let value = value.trim();
        if value == "null" {
            return Ok(Self::All);
        }
        let account = value
            .parse::<AccountId>()
            .map_err(|_| format!("\"account\" must be a number or null, got {value}"))?;
        Ok(Self::Account(account))
    }

    /// The engine command this request becomes.
    #[must_use]
    pub const fn command(self) -> Command {
        Command::KillSwitch {
            account: match self {
                Self::All => None,
                Self::Account(account) => Some(account),
            },
        }
    }
}

/// Why the endpoint could not accept a request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KillOutcome {
    /// The command is queued for the cluster.
    Queued,
    /// The worker is not consuming; the endpoint should refuse rather than grow.
    Full,
    /// The worker thread is gone (the process is shutting down).
    Closed,
}

/// What the admin remembers about the control path.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ControlStatus {
    /// Whether the cluster session is open.
    pub connected: bool,
    /// Commands waiting for ingress.
    pub pending: usize,
    /// A whole-market kill was offered.
    pub kill_all: bool,
    /// Accounts whose kill was offered, in ascending order.
    pub killed_accounts: Vec<AccountId>,
    /// How long ago the last accepted kill was offered.
    pub last_submit_age_ms: Option<u64>,
}

impl ControlStatus {
    /// One JSON object for `/api/control` and the dashboard's footer.
    #[must_use]
    pub fn to_json(&self) -> String {
        format!(
            "{{\"connected\":{},\"pending\":{},\"kill_all\":{},\"killed_accounts\":[{}],\
             \"last_submit_age_ms\":{}}}",
            self.connected,
            self.pending,
            self.kill_all,
            self.killed_accounts
                .iter()
                .map(AccountId::to_string)
                .collect::<Vec<_>>()
                .join(","),
            self.last_submit_age_ms
                .map_or_else(|| "null".to_owned(), |age| age.to_string()),
        )
    }
}

/// The worker's state, shared with the HTTP threads.
#[derive(Debug, Default)]
struct Shared {
    connected: bool,
    pending: usize,
    kill_all: bool,
    killed_accounts: BTreeSet<AccountId>,
    last_submit: Option<Instant>,
}

impl Shared {
    fn lock(shared: &Arc<Mutex<Self>>) -> MutexGuard<'_, Self> {
        shared.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn status(&self) -> ControlStatus {
        ControlStatus {
            connected: self.connected,
            pending: self.pending,
            kill_all: self.kill_all,
            killed_accounts: self.killed_accounts.iter().copied().collect(),
            last_submit_age_ms: self
                .last_submit
                .map(|at| u64::try_from(at.elapsed().as_millis()).unwrap_or(u64::MAX)),
        }
    }
}

/// The HTTP side's handle: queue a kill, read what was submitted.
pub struct Control {
    requests: SyncSender<Command>,
    shared: Arc<Mutex<Shared>>,
}

impl Control {
    /// Start the control thread. It connects in the background and retries a
    /// driver or cluster that is not up yet.
    ///
    /// # Errors
    ///
    /// The thread cannot be started.
    pub fn spawn(config: ControlConfig) -> Result<Self, String> {
        let (requests, receiver) = mpsc::sync_channel(MAX_QUEUED_COMMANDS);
        let shared = Arc::new(Mutex::new(Shared::default()));
        // The client and the session are not `Send`, so they are built on
        // the worker thread rather than handed to it.
        let state = Arc::clone(&shared);
        std::thread::Builder::new()
            .name("deepbook-control".to_owned())
            .spawn(move || Worker::new(config, receiver, state).run())
            .map_err(|error| format!("cannot start the control thread: {error}"))?;
        Ok(Self { requests, shared })
    }

    /// Queue one kill.
    #[must_use]
    pub fn kill(&self, request: KillRequest) -> KillOutcome {
        match self.requests.try_send(request.command()) {
            Ok(()) => KillOutcome::Queued,
            Err(TrySendError::Full(_)) => KillOutcome::Full,
            Err(TrySendError::Disconnected(_)) => KillOutcome::Closed,
        }
    }

    /// What was offered so far; see the module note on acknowledgement.
    #[must_use]
    pub fn status(&self) -> ControlStatus {
        Shared::lock(&self.shared).status()
    }
}

impl std::fmt::Debug for Control {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Control")
            .field("status", &self.status())
            .finish()
    }
}

/// Egress the admin is not interested in: it only keeps the session drained so
/// the cluster's egress publication never back-pressures the service.
struct DiscardEgress(Rc<RefCell<u64>>);

impl EgressListener for DiscardEgress {
    fn on_message(
        &mut self,
        _cluster_session_id: i64,
        _timestamp: i64,
        _payload: &[u8],
        _header: &MessageHeader,
    ) {
        *self.0.borrow_mut() += 1;
    }
}

struct Worker {
    config: ControlConfig,
    requests: Receiver<Command>,
    shared: Arc<Mutex<Shared>>,
    client: Option<Client>,
    session: Option<AeronCluster>,
    egress: Rc<RefCell<u64>>,
    queue: VecDeque<Command>,
    last_keep_alive: Instant,
    retry_at: Instant,
}

impl Worker {
    fn new(config: ControlConfig, requests: Receiver<Command>, shared: Arc<Mutex<Shared>>) -> Self {
        Self {
            config,
            requests,
            shared,
            client: None,
            session: None,
            egress: Rc::new(RefCell::new(0)),
            queue: VecDeque::new(),
            last_keep_alive: Instant::now(),
            retry_at: Instant::now(),
        }
    }

    fn run(mut self) {
        loop {
            self.turn();
            // Waiting on the queue keeps the idle CPU low and wakes the turn
            // as soon as an operator asks for a kill.
            match self.requests.recv_timeout(POLL_INTERVAL) {
                Ok(command) => self.queue.push_back(command),
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => return,
            }
        }
    }

    fn turn(&mut self) {
        self.connect_driver();
        self.connect_session();
        self.poll();
        self.flush();
        self.publish();
    }

    fn connect_driver(&mut self) {
        if self.client.is_some() || Instant::now() < self.retry_at {
            return;
        }
        match Client::connect(&self.config.aeron_dir) {
            Ok(client) => self.client = Some(client),
            Err(error) => {
                eprintln!("deepbook-admin: control: media driver: {error}");
                self.retry_at = Instant::now() + RETRY_DELAY;
            }
        }
    }

    fn connect_session(&mut self) {
        let Some(client) = self.client.as_mut() else {
            return;
        };
        if self.session.is_some() || Instant::now() < self.retry_at {
            return;
        }
        let context = AeronClusterContext::new()
            .ingress_channel(Some(&self.config.ingress_channel))
            .ingress_stream_id(self.config.ingress_stream_id)
            .egress_channel(Some(&self.config.egress_channel))
            .egress_stream_id(self.config.egress_stream_id)
            .message_timeout_ns(self.config.message_timeout_ns)
            .client_name("deepbook-admin")
            .egress_listener(Box::new(DiscardEgress(Rc::clone(&self.egress))));
        match AeronCluster::connect(context, client) {
            Ok(session) => {
                eprintln!(
                    "deepbook-admin: control: cluster session {}",
                    session.cluster_session_id()
                );
                self.session = Some(session);
                self.last_keep_alive = Instant::now();
            }
            Err(error) => {
                eprintln!("deepbook-admin: control: cluster connect: {error}");
                self.retry_at = Instant::now() + RETRY_DELAY;
            }
        }
    }

    fn poll(&mut self) {
        let Some(client) = self.client.as_mut() else {
            return;
        };
        let _ = client.poll();
        let Some(session) = self.session.as_mut() else {
            return;
        };
        if let Err(error) = session.poll_egress(client) {
            eprintln!("deepbook-admin: control: cluster egress: {error}");
        }
        if session.is_closed() {
            eprintln!("deepbook-admin: control: cluster session closed");
            self.session = None;
            self.retry_at = Instant::now() + RETRY_DELAY;
        } else if self.last_keep_alive.elapsed() >= KEEP_ALIVE_INTERVAL {
            session.send_keep_alive(client);
            self.last_keep_alive = Instant::now();
        }
    }

    fn flush(&mut self) {
        let Some(client) = self.client.as_mut() else {
            return;
        };
        let Some(session) = self.session.as_mut() else {
            return;
        };
        let mut accepted: Vec<Command> = Vec::new();
        while let Some(&command) = self.queue.front() {
            match session.offer(client, &encode_command(&command)) {
                Some(Appended::Ok { .. }) => {
                    // At least once: the queue is popped only after ingress
                    // accepted the command.
                    self.queue.pop_front();
                    accepted.push(command);
                }
                Some(
                    Appended::EndOfLog
                    | Appended::MidRotation
                    | Appended::BackPressured
                    | Appended::NotConnected,
                )
                | None => break,
                Some(
                    Appended::MaxPositionExceeded | Appended::MessageTooLarge | Appended::Malformed,
                ) => {
                    self.queue.pop_front();
                    eprintln!("deepbook-admin: control: the cluster refused a kill command");
                }
            }
        }
        for command in &accepted {
            self.record(command);
        }
    }

    fn record(&self, command: &Command) {
        let mut shared = Shared::lock(&self.shared);
        if let Command::KillSwitch { account } = command {
            match account {
                Some(account) => {
                    shared.killed_accounts.insert(*account);
                }
                None => shared.kill_all = true,
            }
            shared.last_submit = Some(Instant::now());
        }
    }

    fn publish(&self) {
        let mut shared = Shared::lock(&self.shared);
        shared.connected = self.session.is_some();
        shared.pending = self.queue.len();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_body_kills_the_market() {
        for body in [
            "",
            "  ",
            "{}",
            " {\"account\": null} ",
            "{\"account\":null}",
        ] {
            assert_eq!(
                Ok(KillRequest::All),
                KillRequest::from_body(body),
                "{body:?}"
            );
        }
    }

    #[test]
    fn an_account_body_kills_that_account() {
        assert_eq!(
            Ok(KillRequest::Account(7)),
            KillRequest::from_body("{\"account\": 7}")
        );
        assert_eq!(
            Ok(KillRequest::Account(0)),
            KillRequest::from_body("{\"account\":0}")
        );
    }

    #[test]
    fn a_body_that_is_not_one_known_object_is_refused() {
        for body in [
            "{\"account\": -1}",
            "{\"account\": \"7\"}",
            "{\"account\": 7, \"symbol\": 1}",
            "{\"symbol\": 1}",
            "[7]",
            "account=7",
            "{\"account\"}",
            "{",
        ] {
            assert!(
                KillRequest::from_body(body).is_err(),
                "{body:?} was accepted"
            );
        }
    }

    #[test]
    fn a_request_becomes_the_engine_command() {
        assert_eq!(
            Command::KillSwitch { account: None },
            KillRequest::All.command()
        );
        assert_eq!(
            Command::KillSwitch { account: Some(9) },
            KillRequest::Account(9).command()
        );
    }

    #[test]
    fn a_status_serializes_the_submitted_kills() {
        let status = ControlStatus {
            connected: true,
            pending: 2,
            kill_all: false,
            killed_accounts: vec![3, 9],
            last_submit_age_ms: Some(1500),
        };
        assert_eq!(
            "{\"connected\":true,\"pending\":2,\"kill_all\":false,\
             \"killed_accounts\":[3,9],\"last_submit_age_ms\":1500}",
            status.to_json()
        );

        let empty = ControlStatus::default();
        assert_eq!(
            "{\"connected\":false,\"pending\":0,\"kill_all\":false,\
             \"killed_accounts\":[],\"last_submit_age_ms\":null}",
            empty.to_json()
        );
    }
}
