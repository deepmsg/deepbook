//! The FIX 4.4 session state machine.
//!
//! The acceptor drives this one message at a time. Every method takes
//! `now_millis`, so a test owns the clock, and every outbound message is
//! journaled **before** it is returned as an [`Action::Send`]: the caller
//! writes the bytes and cannot get ahead of the journal.
//!
//! # Sequence semantics (fixed here, not left to the caller)
//!
//! - Inbound and outbound sequences are independent.
//! - Inbound `seq > next_in`: send one `ResendRequest(next_in, 0)`, queue the
//!   message, and process it only when the gap closes.
//! - Inbound `seq < next_in` with `43=Y`: a duplicate, ignored.
//! - Inbound `seq < next_in` without `43=Y`: `Logout` and disconnect.
//! - `ResendRequest` with `16=0` means "to the end of the stream".
//! - An application message is never gap-filled. An admin run in the window
//!   becomes one `SequenceReset-GapFill`; an application message is resent
//!   with `43=Y` and `122=OrigSendingTime`.
//! - A resend that starts before the journal window sends `Logout` and
//!   disconnects; it never silently drops a command.
//! - A `Logon` with `141=Y` resets **both** sequences to 1 and clears the
//!   journal.
//! - A `Logon` whose `seq > next_in` is accepted and answered with a
//!   `ResendRequest`; a lower one without `43=Y` is refused.

use std::collections::BTreeMap;
use std::fmt;

use crate::clock::format_utc;
use crate::codec::{
    CodecError, MessageBuilder, MsgType, ParsedMessage, parse, rewrite_for_resend, tag,
};
use crate::journal::{Journal, JournalEntry, SequenceStore, Sequences};
use crate::message::{
    ApplicationMessage, SessionMessage, decode_application, decode_session, encode_application,
};

/// The acceptor's configuration.
#[derive(Clone, Debug)]
pub struct SessionConfig {
    /// Our `SenderCompID`.
    pub sender_comp_id: String,
    /// The peer's `TargetCompID`.
    pub target_comp_id: String,
    /// Our heartbeat interval, used until the peer's `108` arrives.
    pub heart_bt_int: u32,
    /// How long a connection may sit without a `Logon` before it is closed.
    pub logon_timeout_millis: i64,
}

impl SessionConfig {
    /// A configuration with a ten-second logon timeout.
    #[must_use]
    pub fn new(
        sender_comp_id: impl Into<String>,
        target_comp_id: impl Into<String>,
        heart_bt_int: u32,
    ) -> Self {
        Self {
            sender_comp_id: sender_comp_id.into(),
            target_comp_id: target_comp_id.into(),
            heart_bt_int: heart_bt_int.max(1),
            logon_timeout_millis: 10_000,
        }
    }
}

/// Where the session is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionState {
    /// Waiting for the peer's `Logon`.
    AwaitingLogon,
    /// Logged on and exchanging messages.
    Active,
    /// The session ended; no further messages are accepted.
    LoggedOut,
}

/// What the caller must do after one entry point.
///
/// The enum is carried one message per call, never in a long-lived
/// collection: boxing `Deliver` would add an allocation to the order path for
/// no stack it needs back.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    /// Write `raw` to the socket. It is already in the journal.
    Send {
        /// The sequence number the message carries.
        seq: u64,
        /// The complete message.
        raw: Vec<u8>,
    },
    /// Hand an application message to the engine seam.
    Deliver(ApplicationMessage),
    /// Close the connection. Any `Send` before this one must be written first.
    Disconnect {
        /// Why the session ended.
        reason: String,
    },
}

/// A session-layer failure.
#[derive(Debug)]
pub enum SessionError {
    /// The bytes are not a valid FIX message.
    Codec(CodecError),
    /// The journal or sequence store failed.
    Journal(crate::journal::JournalError),
    /// The caller used the session out of order (e.g. sent before `Logon`).
    Protocol(String),
}

impl fmt::Display for SessionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Codec(error) => write!(formatter, "{error}"),
            Self::Journal(error) => write!(formatter, "{error}"),
            Self::Protocol(detail) => write!(formatter, "{detail}"),
        }
    }
}

impl std::error::Error for SessionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Codec(error) => Some(error),
            Self::Journal(error) => Some(error),
            Self::Protocol(_) => None,
        }
    }
}

impl From<CodecError> for SessionError {
    fn from(error: CodecError) -> Self {
        Self::Codec(error)
    }
}

impl From<crate::journal::JournalError> for SessionError {
    fn from(error: crate::journal::JournalError) -> Self {
        Self::Journal(error)
    }
}

/// One FIX 4.4 acceptor session.
pub struct Session<J: Journal, S: SequenceStore> {
    config: SessionConfig,
    journal: J,
    sequences: S,
    next_in: u64,
    next_out: u64,
    state: SessionState,
    heartbeat_secs: u32,
    last_received_ms: i64,
    last_sent_ms: i64,
    connected_at_ms: i64,
    test_request: Option<(String, i64)>,
    resend_pending: Option<u64>,
    queued: BTreeMap<u64, ParsedMessage>,
}

impl<J: Journal, S: SequenceStore> Session<J, S> {
    /// Build a session from its journal and persisted sequences.
    ///
    /// The outbound sequence is the larger of the journal's next sequence and
    /// the stored one, so a journal that outlived its state file is not
    /// overwritten.
    ///
    /// # Errors
    ///
    /// The sequence store cannot be read.
    pub fn new(
        config: SessionConfig,
        journal: J,
        sequences: S,
        now_millis: i64,
    ) -> Result<Self, SessionError> {
        let stored = sequences.load()?;
        Ok(Self {
            heartbeat_secs: config.heart_bt_int,
            next_in: stored.next_in.max(1),
            next_out: stored.next_out.max(journal.next_seq()).max(1),
            config,
            journal,
            sequences,
            state: SessionState::AwaitingLogon,
            last_received_ms: now_millis,
            last_sent_ms: now_millis,
            connected_at_ms: now_millis,
            test_request: None,
            resend_pending: None,
            queued: BTreeMap::new(),
        })
    }

    /// The next inbound sequence number expected.
    #[must_use]
    pub const fn next_in(&self) -> u64 {
        self.next_in
    }

    /// The next outbound sequence number to be assigned.
    #[must_use]
    pub const fn next_out(&self) -> u64 {
        self.next_out
    }

    /// The session state.
    #[must_use]
    pub const fn state(&self) -> SessionState {
        self.state
    }

    /// Whether the session is logged on.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.state == SessionState::Active
    }

    /// The journal, for a test or an operator.
    #[must_use]
    pub const fn journal(&self) -> &J {
        &self.journal
    }

    /// Handle one inbound message.
    ///
    /// # Errors
    ///
    /// The bytes do not parse, the session is in a state that cannot accept
    /// them, or the journal/state write failed.
    pub fn on_inbound(&mut self, raw: &[u8], now_millis: i64) -> Result<Vec<Action>, SessionError> {
        let message = parse(raw)?;
        let sender = message.get_str(tag::SENDER_COMP_ID)?;
        let target = message.get_str(tag::TARGET_COMP_ID)?;
        if sender != self.config.target_comp_id || target != self.config.sender_comp_id {
            return self.logout(
                format!(
                    "CompID problem: expected {}/{} but received {sender}/{target}",
                    self.config.target_comp_id, self.config.sender_comp_id
                ),
                now_millis,
            );
        }

        let msg_type = message.msg_type()?;
        let seq = message.seq_num()?;
        match self.state {
            SessionState::AwaitingLogon => self.handle_logon(message, msg_type, seq, now_millis),
            SessionState::Active => self.handle_active(message, msg_type, seq, now_millis),
            SessionState::LoggedOut => Ok(vec![Action::Disconnect {
                reason: "session is logged out".to_owned(),
            }]),
        }
    }

    /// Drive the timers: logon timeout, heartbeat, `TestRequest` timeout.
    ///
    /// # Errors
    ///
    /// A journal or state write failed.
    pub fn on_tick(&mut self, now_millis: i64) -> Result<Vec<Action>, SessionError> {
        match self.state {
            SessionState::AwaitingLogon => {
                if now_millis - self.connected_at_ms >= self.config.logon_timeout_millis {
                    self.state = SessionState::LoggedOut;
                    return Ok(vec![Action::Disconnect {
                        reason: "no Logon before the logon timeout".to_owned(),
                    }]);
                }
                Ok(Vec::new())
            }
            SessionState::LoggedOut => Ok(Vec::new()),
            SessionState::Active => {
                let heartbeat_ms = i64::from(self.heartbeat_secs) * 1_000;
                if let Some((_, sent_at)) = &self.test_request {
                    if now_millis - sent_at >= heartbeat_ms {
                        self.test_request = None;
                        return self.logout("TestRequest timeout".to_owned(), now_millis);
                    }
                } else if now_millis - self.last_received_ms >= heartbeat_ms {
                    let id = format!("TEST-{now_millis}");
                    self.test_request = Some((id.clone(), now_millis));
                    let action = self.send_new(
                        MsgType::TestRequest,
                        vec![(tag::TEST_REQ_ID, id.into_bytes())],
                        now_millis,
                    )?;
                    return Ok(vec![action]);
                }

                if now_millis - self.last_sent_ms >= heartbeat_ms {
                    return Ok(vec![self.send_new(
                        MsgType::Heartbeat,
                        Vec::new(),
                        now_millis,
                    )?]);
                }
                Ok(Vec::new())
            }
        }
    }

    /// Send an application message (an execution report, usually).
    ///
    /// # Errors
    ///
    /// The session is not active, or the journal write failed.
    pub fn send_application(
        &mut self,
        message: &ApplicationMessage,
        now_millis: i64,
    ) -> Result<Action, SessionError> {
        if self.state != SessionState::Active {
            return Err(SessionError::Protocol(
                "cannot send an application message before Logon".to_owned(),
            ));
        }
        let msg_type = application_msg_type(message);
        let sending_time = format_utc(now_millis);
        let builder = MessageBuilder::new(
            msg_type,
            &self.config.sender_comp_id,
            &self.config.target_comp_id,
            self.next_out,
            sending_time,
        );
        self.send_builder(msg_type, encode_application(builder, message), now_millis)
    }

    fn handle_logon(
        &mut self,
        message: ParsedMessage,
        msg_type: MsgType,
        seq: u64,
        now_millis: i64,
    ) -> Result<Vec<Action>, SessionError> {
        if msg_type != MsgType::Logon {
            return self.logout("Logon expected".to_owned(), now_millis);
        }
        let SessionMessage::Logon {
            heart_bt_int,
            reset_seq_num,
            ..
        } = decode_session(&message)?
        else {
            unreachable!("msg_type checked");
        };

        if reset_seq_num {
            if seq != 1 {
                return self.logout(
                    format!("141=Y requires MsgSeqNum=1, received {seq}"),
                    now_millis,
                );
            }
            self.next_in = 1;
            self.next_out = 1;
            self.journal.clear()?;
            self.test_request = None;
            self.resend_pending = None;
            self.queued.clear();
        }

        if seq < self.next_in {
            if message.poss_dup() {
                return Ok(Vec::new());
            }
            return self.logout(
                format!(
                    "MsgSeqNum too low, expecting {} but received {seq}",
                    self.next_in
                ),
                now_millis,
            );
        }

        let mut actions = Vec::new();
        if seq > self.next_in {
            actions.push(self.send_new(
                MsgType::ResendRequest,
                vec![
                    (tag::BEGIN_SEQ_NO, self.next_in.to_string().into_bytes()),
                    (tag::END_SEQ_NO, b"0".to_vec()),
                ],
                now_millis,
            )?);
            self.resend_pending = Some(self.next_in);
        }

        self.next_in = seq + 1;
        self.heartbeat_secs = heart_bt_int.max(1);
        self.last_received_ms = now_millis;
        self.state = SessionState::Active;

        let mut body = vec![
            (tag::ENCRYPT_METHOD, b"0".to_vec()),
            (
                tag::HEART_BT_INT,
                self.heartbeat_secs.to_string().into_bytes(),
            ),
        ];
        if reset_seq_num {
            body.push((tag::RESET_SEQ_NUM_FLAG, b"Y".to_vec()));
        }
        actions.push(self.send_new(MsgType::Logon, body, now_millis)?);
        self.persist()?;
        Ok(actions)
    }

    fn handle_active(
        &mut self,
        message: ParsedMessage,
        msg_type: MsgType,
        seq: u64,
        now_millis: i64,
    ) -> Result<Vec<Action>, SessionError> {
        if msg_type == MsgType::Logon {
            // A reset Logon is valid mid-session: `141=Y` says both sides
            // restart at 1 and the journal belongs to the old stream.
            if matches!(
                decode_session(&message)?,
                SessionMessage::Logon {
                    reset_seq_num: true,
                    ..
                }
            ) {
                return self.handle_logon(message, msg_type, seq, now_millis);
            }
            return self.logout("Logon already received".to_owned(), now_millis);
        }

        if seq < self.next_in {
            if message.poss_dup() {
                return Ok(Vec::new());
            }
            return self.logout(
                format!(
                    "MsgSeqNum too low, expecting {} but received {seq}",
                    self.next_in
                ),
                now_millis,
            );
        }

        if seq > self.next_in {
            let begin = self.next_in;
            self.queued.insert(seq, message);
            if self.resend_pending != Some(begin) {
                self.resend_pending = Some(begin);
                return Ok(vec![self.send_new(
                    MsgType::ResendRequest,
                    vec![
                        (tag::BEGIN_SEQ_NO, begin.to_string().into_bytes()),
                        (tag::END_SEQ_NO, b"0".to_vec()),
                    ],
                    now_millis,
                )?]);
            }
            return Ok(Vec::new());
        }

        self.next_in += 1;
        self.last_received_ms = now_millis;
        if self
            .resend_pending
            .is_some_and(|begin| self.next_in > begin)
        {
            self.resend_pending = None;
        }

        let mut actions = self.process_message(&message, seq, now_millis)?;
        actions.extend(self.drain_queued(now_millis)?);
        self.persist()?;
        Ok(actions)
    }

    fn process_message(
        &mut self,
        message: &ParsedMessage,
        seq: u64,
        now_millis: i64,
    ) -> Result<Vec<Action>, SessionError> {
        let msg_type = message.msg_type()?;
        if !msg_type.is_admin() {
            return Ok(vec![Action::Deliver(decode_application(message)?)]);
        }

        match decode_session(message)? {
            SessionMessage::Logon { .. } => {
                self.logout("Logon already received".to_owned(), now_millis)
            }
            SessionMessage::Logout { text } => {
                let reason = text.unwrap_or_else(|| "peer logged out".to_owned());
                let reply = self.send_new(
                    MsgType::Logout,
                    vec![(tag::TEXT, b"closing".to_vec())],
                    now_millis,
                )?;
                self.state = SessionState::LoggedOut;
                Ok(vec![reply, Action::Disconnect { reason }])
            }
            SessionMessage::Heartbeat { test_req_id } => {
                if let Some((expected, _)) = &self.test_request {
                    if test_req_id.as_deref() == Some(expected.as_str()) {
                        self.test_request = None;
                    }
                }
                Ok(Vec::new())
            }
            SessionMessage::TestRequest { test_req_id } => Ok(vec![self.send_new(
                MsgType::Heartbeat,
                vec![(tag::TEST_REQ_ID, test_req_id.into_bytes())],
                now_millis,
            )?]),
            SessionMessage::ResendRequest {
                begin_seq_no,
                end_seq_no,
            } => self.handle_resend_request(begin_seq_no, end_seq_no, now_millis),
            SessionMessage::SequenceReset {
                gap_fill,
                new_seq_no,
            } => {
                if gap_fill {
                    if new_seq_no < self.next_in {
                        return Ok(vec![self.send_reject(
                            seq,
                            "GapFill NewSeqNo is not ahead of the expected sequence",
                            now_millis,
                        )?]);
                    }
                } else if new_seq_no == 0 {
                    return Ok(vec![self.send_reject(
                        seq,
                        "SequenceReset NewSeqNo is zero",
                        now_millis,
                    )?]);
                }
                self.next_in = new_seq_no;
                self.persist()?;
                Ok(Vec::new())
            }
            SessionMessage::Reject { .. } => Ok(Vec::new()),
        }
    }

    fn handle_resend_request(
        &mut self,
        begin_seq_no: u64,
        end_seq_no: u64,
        now_millis: i64,
    ) -> Result<Vec<Action>, SessionError> {
        let last_out = self.next_out.saturating_sub(1);
        let begin = begin_seq_no.max(1);
        let end = if end_seq_no == 0 {
            last_out
        } else {
            end_seq_no.min(last_out)
        };
        if begin > end {
            return Ok(Vec::new());
        }
        if begin < self.journal.oldest_seq().unwrap_or(last_out + 1) {
            return self.logout(
                format!("ResendRequest {begin} is outside the journal window"),
                now_millis,
            );
        }

        let mut actions = Vec::new();
        let mut admin_run: Option<(u64, u64)> = None;
        for seq in begin..=end {
            let Some(entry) = self.journal.get(seq).cloned() else {
                return self.logout(
                    format!("ResendRequest {seq} is outside the journal window"),
                    now_millis,
                );
            };
            if entry.msg_type.is_admin() {
                admin_run = match admin_run {
                    Some((start, _)) => Some((start, seq)),
                    None => Some((seq, seq)),
                };
                continue;
            }
            if let Some((start, finish)) = admin_run.take() {
                actions.push(self.gap_fill(start, finish + 1, now_millis)?);
            }
            actions.push(self.resend(&entry, now_millis)?);
        }
        if let Some((start, finish)) = admin_run {
            actions.push(self.gap_fill(start, finish + 1, now_millis)?);
        }
        Ok(actions)
    }

    fn drain_queued(&mut self, now_millis: i64) -> Result<Vec<Action>, SessionError> {
        let mut actions = Vec::new();
        // A queued Logout ends the session; nothing after it is processed.
        while self.state == SessionState::Active {
            let Some(message) = self.queued.remove(&self.next_in) else {
                break;
            };
            let seq = self.next_in;
            self.next_in += 1;
            self.last_received_ms = now_millis;
            if self
                .resend_pending
                .is_some_and(|begin| self.next_in > begin)
            {
                self.resend_pending = None;
            }
            actions.extend(self.process_message(&message, seq, now_millis)?);
        }
        Ok(actions)
    }

    fn logout(&mut self, reason: String, now_millis: i64) -> Result<Vec<Action>, SessionError> {
        let mut actions = Vec::new();
        if self.state != SessionState::LoggedOut {
            actions.push(self.send_new(
                MsgType::Logout,
                vec![(tag::TEXT, reason.as_bytes().to_vec())],
                now_millis,
            )?);
        }
        self.state = SessionState::LoggedOut;
        actions.push(Action::Disconnect { reason });
        Ok(actions)
    }

    fn send_reject(
        &mut self,
        ref_seq_num: u64,
        text: &str,
        now_millis: i64,
    ) -> Result<Action, SessionError> {
        self.send_new(
            MsgType::Reject,
            vec![
                (tag::REF_SEQ_NUM, ref_seq_num.to_string().into_bytes()),
                (tag::SESSION_REJECT_REASON, b"5".to_vec()),
                (tag::TEXT, text.as_bytes().to_vec()),
            ],
            now_millis,
        )
    }

    fn send_new(
        &mut self,
        msg_type: MsgType,
        body: Vec<(u32, Vec<u8>)>,
        now_millis: i64,
    ) -> Result<Action, SessionError> {
        let sending_time = format_utc(now_millis);
        let mut builder = MessageBuilder::new(
            msg_type,
            &self.config.sender_comp_id,
            &self.config.target_comp_id,
            self.next_out,
            sending_time,
        );
        for (tag, value) in body {
            builder = builder.body(tag, value);
        }
        self.send_builder(msg_type, builder, now_millis)
    }

    fn send_builder(
        &mut self,
        msg_type: MsgType,
        builder: MessageBuilder,
        now_millis: i64,
    ) -> Result<Action, SessionError> {
        let raw = builder.build();
        let seq = self.next_out;
        self.journal.append(JournalEntry {
            seq,
            msg_type,
            raw: raw.clone(),
            sending_time: format_utc(now_millis),
        })?;
        self.next_out += 1;
        self.persist()?;
        self.last_sent_ms = now_millis;
        Ok(Action::Send { seq, raw })
    }

    fn resend(&mut self, entry: &JournalEntry, now_millis: i64) -> Result<Action, SessionError> {
        let raw = rewrite_for_resend(&entry.raw, &format_utc(now_millis))?;
        self.last_sent_ms = now_millis;
        Ok(Action::Send {
            seq: entry.seq,
            raw,
        })
    }

    fn gap_fill(
        &mut self,
        seq: u64,
        new_seq_no: u64,
        now_millis: i64,
    ) -> Result<Action, SessionError> {
        let sending_time = format_utc(now_millis);
        let raw = MessageBuilder::new(
            MsgType::SequenceReset,
            &self.config.sender_comp_id,
            &self.config.target_comp_id,
            seq,
            sending_time,
        )
        .body(tag::GAP_FILL_FLAG, b"Y".to_vec())
        .body_int(tag::NEW_SEQ_NO, new_seq_no)
        .build();
        self.last_sent_ms = now_millis;
        Ok(Action::Send { seq, raw })
    }

    fn persist(&mut self) -> Result<(), SessionError> {
        self.sequences.store(Sequences {
            next_in: self.next_in,
            next_out: self.next_out,
        })?;
        Ok(())
    }
}

fn application_msg_type(message: &ApplicationMessage) -> MsgType {
    match message {
        ApplicationMessage::NewOrderSingle(_) => MsgType::NewOrderSingle,
        ApplicationMessage::OrderCancelRequest(_) => MsgType::OrderCancelRequest,
        ApplicationMessage::OrderCancelReplaceRequest(_) => MsgType::OrderCancelReplaceRequest,
        ApplicationMessage::ExecutionReport(_) => MsgType::ExecutionReport,
        ApplicationMessage::OrderCancelReject(_) => MsgType::OrderCancelReject,
        ApplicationMessage::OrderStatusRequest(_) => MsgType::OrderStatusRequest,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::parse;
    use crate::journal::{MemoryJournal, MemorySequenceStore};
    use crate::message::{ExecType, OrdStatus};
    use deepbook_types::Side;

    const NOW: i64 = 1_792_931_696_000;

    fn config() -> SessionConfig {
        SessionConfig::new("DEEPBOOK", "PEER", 30)
    }

    fn session() -> Session<MemoryJournal, MemorySequenceStore> {
        Session::new(
            config(),
            MemoryJournal::new(64),
            MemorySequenceStore::default(),
            NOW,
        )
        .expect("session")
    }

    fn inbound(msg_type: MsgType, seq: u64, body: &[(u32, Vec<u8>)]) -> Vec<u8> {
        let mut builder =
            MessageBuilder::new(msg_type, "PEER", "DEEPBOOK", seq, "20261025-12:34:56.000");
        for (tag, value) in body {
            builder = builder.body(*tag, value.clone());
        }
        builder.build()
    }

    fn logon(seq: u64, reset: bool) -> Vec<u8> {
        let mut body = vec![
            (tag::ENCRYPT_METHOD, b"0".to_vec()),
            (tag::HEART_BT_INT, b"30".to_vec()),
        ];
        if reset {
            body.push((tag::RESET_SEQ_NUM_FLAG, b"Y".to_vec()));
        }
        inbound(MsgType::Logon, seq, &body)
    }

    fn new_order(seq: u64, cl_ord_id: &str) -> Vec<u8> {
        inbound(
            MsgType::NewOrderSingle,
            seq,
            &[
                (tag::CL_ORD_ID, cl_ord_id.as_bytes().to_vec()),
                (tag::SYMBOL, b"AAPL".to_vec()),
                (tag::SIDE, b"1".to_vec()),
                (tag::ORDER_QTY, b"100".to_vec()),
                (tag::ORD_TYPE, b"2".to_vec()),
                (tag::PRICE, b"100.25".to_vec()),
                (tag::TIME_IN_FORCE, b"0".to_vec()),
                (tag::TRANSACT_TIME, b"20261025-12:34:56.000".to_vec()),
            ],
        )
    }

    fn execution_report() -> ApplicationMessage {
        ApplicationMessage::ExecutionReport(crate::message::ExecutionReport {
            exec_id: "E-1".to_owned(),
            order_id: "O-1".to_owned(),
            cl_ord_id: "C-1".to_owned(),
            exec_type: ExecType::New,
            ord_status: OrdStatus::New,
            symbol: "AAPL".to_owned(),
            side: Side::Buy,
            order_qty: 100,
            last_qty: None,
            last_px: None,
            leaves_qty: 100,
            cum_qty: 0,
            avg_px: 0,
            account: None,
            transact_time: None,
            text: None,
        })
    }

    fn sends(actions: &[Action]) -> Vec<(u64, ParsedMessage)> {
        actions
            .iter()
            .filter_map(|action| match action {
                Action::Send { raw, .. } => Some(parse(raw).expect("parse sent")),
                _ => None,
            })
            .map(|message| (message.seq_num().expect("seq"), message))
            .collect()
    }

    #[test]
    fn a_logon_is_answered_and_both_sequences_advance() {
        let mut session = session();
        let actions = session.on_inbound(&logon(1, false), NOW).expect("logon");
        let sent = sends(&actions);
        assert_eq!(1, sent.len());
        assert_eq!(1, sent[0].0);
        assert_eq!(MsgType::Logon, sent[0].1.msg_type().expect("type"));
        assert_eq!(b"30", sent[0].1.get(tag::HEART_BT_INT).expect("108"));
        assert!(session.is_active());
        assert_eq!(2, session.next_in());
        assert_eq!(2, session.next_out());
        assert!(session.journal().get(1).is_some());
    }

    #[test]
    fn an_application_message_is_delivered_and_journaled_on_the_way_out() {
        let mut session = session();
        session.on_inbound(&logon(1, false), NOW).expect("logon");
        let actions = session
            .on_inbound(&new_order(2, "C-1"), NOW + 1)
            .expect("order");
        assert!(matches!(
            actions.as_slice(),
            [Action::Deliver(ApplicationMessage::NewOrderSingle(_))]
        ));
        assert_eq!(3, session.next_in());

        let action = session
            .send_application(&execution_report(), NOW + 2)
            .expect("report");
        let Action::Send { seq, raw } = action else {
            panic!("not a send");
        };
        assert_eq!(2, seq);
        let sent = parse(&raw).expect("parse");
        assert_eq!(MsgType::ExecutionReport, sent.msg_type().expect("type"));
        assert_eq!(3, session.next_out());
        assert!(session.journal().get(2).is_some());
    }

    #[test]
    fn a_gap_requests_a_resend_and_queues_until_it_closes() {
        let mut session = session();
        session.on_inbound(&logon(1, false), NOW).expect("logon");
        let actions = session
            .on_inbound(&new_order(4, "C-4"), NOW + 1)
            .expect("gap");
        let sent = sends(&actions);
        assert_eq!(1, sent.len());
        assert_eq!(MsgType::ResendRequest, sent[0].1.msg_type().expect("type"));
        assert_eq!(b"2", sent[0].1.get(tag::BEGIN_SEQ_NO).expect("begin seq"));
        assert_eq!(b"0", sent[0].1.get(tag::END_SEQ_NO).expect("end seq"));
        assert_eq!(2, session.next_in());

        let delivered = session
            .on_inbound(&new_order(2, "C-2"), NOW + 2)
            .expect("2");
        assert!(matches!(
            delivered.as_slice(),
            [Action::Deliver(ApplicationMessage::NewOrderSingle(_))]
        ));
        assert_eq!(3, session.next_in());
        let delivered = session
            .on_inbound(&new_order(3, "C-3"), NOW + 3)
            .expect("3");
        let mut variants: Vec<&str> = Vec::new();
        for action in &delivered {
            if let Action::Deliver(ApplicationMessage::NewOrderSingle(order)) = action {
                variants.push(order.cl_ord_id.as_str());
            }
        }
        assert_eq!(vec!["C-3", "C-4"], variants);
        assert_eq!(5, session.next_in());
    }

    #[test]
    fn a_duplicate_is_ignored_and_a_low_sequence_ends_the_session() {
        let mut session = session();
        session.on_inbound(&logon(1, false), NOW).expect("logon");
        let duplicate = inbound(
            MsgType::Heartbeat,
            1,
            &[(tag::POSS_DUP_FLAG, b"Y".to_vec())],
        );
        assert!(
            session
                .on_inbound(&duplicate, NOW + 1)
                .expect("duplicate")
                .is_empty()
        );
        let low = inbound(MsgType::Heartbeat, 1, &[]);
        let actions = session.on_inbound(&low, NOW + 2).expect("low");
        assert!(actions.iter().any(|action| matches!(
            action,
            Action::Disconnect { reason } if reason.contains("too low")
        )));
        assert_eq!(SessionState::LoggedOut, session.state());
    }

    #[test]
    fn a_resend_replays_application_with_poss_dup_and_gap_fills_admin() {
        let mut session = session();
        session.on_inbound(&logon(1, false), NOW).expect("logon");
        session
            .on_inbound(&new_order(2, "C-1"), NOW + 1)
            .expect("order");
        session
            .send_application(&execution_report(), NOW + 2)
            .expect("report 1");
        session
            .send_application(&execution_report(), NOW + 3)
            .expect("report 2");
        session.on_tick(NOW + 40_000).expect("test request").len();
        // A TestRequest is journaled at seq 4; request 2..=4.
        let request = inbound(
            MsgType::ResendRequest,
            3,
            &[
                (tag::BEGIN_SEQ_NO, b"2".to_vec()),
                (tag::END_SEQ_NO, b"4".to_vec()),
            ],
        );
        let actions = session.on_inbound(&request, NOW + 4).expect("resend");
        let sent = sends(&actions);
        assert_eq!(3, sent.len());
        assert_eq!(2, sent[0].0);
        assert!(sent[0].1.poss_dup());
        assert_eq!(3, sent[1].0);
        assert!(sent[1].1.poss_dup());
        assert_eq!(4, sent[2].0);
        assert_eq!(MsgType::SequenceReset, sent[2].1.msg_type().expect("type"));
        assert_eq!(b"Y", sent[2].1.get(tag::GAP_FILL_FLAG).expect("flag"));
        assert_eq!(b"5", sent[2].1.get(tag::NEW_SEQ_NO).expect("new seq"));
    }

    #[test]
    fn a_resend_before_the_window_ends_the_session() {
        let mut session = Session::new(
            config(),
            MemoryJournal::new(1),
            MemorySequenceStore::default(),
            NOW,
        )
        .expect("session");
        session.on_inbound(&logon(1, false), NOW).expect("logon");
        session
            .send_application(&execution_report(), NOW + 1)
            .expect("1");
        session
            .send_application(&execution_report(), NOW + 2)
            .expect("2");
        let request = inbound(
            MsgType::ResendRequest,
            2,
            &[
                (tag::BEGIN_SEQ_NO, b"2".to_vec()),
                (tag::END_SEQ_NO, b"0".to_vec()),
            ],
        );
        let actions = session.on_inbound(&request, NOW + 3).expect("resend");
        assert!(actions.iter().any(|action| matches!(
            action,
            Action::Disconnect { reason } if reason.contains("outside the journal window")
        )));
    }

    #[test]
    fn a_reset_logon_clears_the_journal_and_both_sequences() {
        let mut session = session();
        session.on_inbound(&logon(1, false), NOW).expect("logon");
        session
            .send_application(&execution_report(), NOW + 1)
            .expect("report");
        assert_eq!(3, session.next_out());
        assert_eq!(2, session.journal().len());

        let actions = session
            .on_inbound(&logon(1, true), NOW + 2)
            .expect("reset logon");
        let sent = sends(&actions);
        assert_eq!(1, sent[0].0);
        assert_eq!(b"Y", sent[0].1.get(tag::RESET_SEQ_NUM_FLAG).expect("141"));
        assert_eq!(2, session.next_in());
        assert_eq!(2, session.next_out());
        assert_eq!(1, session.journal().len());
        assert!(session.journal().get(1).is_some());
    }

    #[test]
    fn a_logon_with_a_high_sequence_asks_for_a_resend() {
        let mut session = session();
        let actions = session.on_inbound(&logon(5, false), NOW).expect("logon");
        let sent = sends(&actions);
        assert_eq!(MsgType::ResendRequest, sent[0].1.msg_type().expect("type"));
        assert_eq!(MsgType::Logon, sent[1].1.msg_type().expect("type"));
        assert_eq!(6, session.next_in());
        assert!(session.is_active());
    }

    #[test]
    fn a_sequence_reset_gap_fill_moves_the_expected_sequence() {
        let mut session = session();
        session.on_inbound(&logon(1, false), NOW).expect("logon");
        let reset = inbound(
            MsgType::SequenceReset,
            2,
            &[
                (tag::GAP_FILL_FLAG, b"Y".to_vec()),
                (tag::NEW_SEQ_NO, b"5".to_vec()),
            ],
        );
        session.on_inbound(&reset, NOW + 1).expect("reset");
        assert_eq!(5, session.next_in());
    }

    #[test]
    fn a_missing_test_request_answer_ends_the_session() {
        let mut session = session();
        session.on_inbound(&logon(1, false), NOW).expect("logon");
        let actions = session.on_tick(NOW + 30_000).expect("tick");
        let sent = sends(&actions);
        assert_eq!(1, sent.len());
        assert_eq!(MsgType::TestRequest, sent[0].1.msg_type().expect("type"));
        let actions = session.on_tick(NOW + 60_000).expect("tick 2");
        assert!(actions.iter().any(|action| matches!(
            action,
            Action::Disconnect { reason } if reason.contains("TestRequest timeout")
        )));
    }

    #[test]
    fn a_heartbeat_answers_the_test_request() {
        let mut session = session();
        session.on_inbound(&logon(1, false), NOW).expect("logon");
        let actions = session.on_tick(NOW + 30_000).expect("tick");
        let test_request = sends(&actions)[0].1.clone();
        let id = test_request
            .get(tag::TEST_REQ_ID)
            .expect("test req id")
            .to_vec();
        let heartbeat = inbound(MsgType::Heartbeat, 2, &[(tag::TEST_REQ_ID, id.clone())]);
        session.on_inbound(&heartbeat, NOW + 31_000).expect("hb");
        assert!(session.test_request.is_none());
        assert_ne!(id, Vec::<u8>::new());
    }

    #[test]
    fn an_active_session_still_sends_heartbeats() {
        let mut session = session();
        session.on_inbound(&logon(1, false), NOW).expect("logon");
        let actions = session.on_tick(NOW + 30_000).expect("tick");
        let sent = sends(&actions);
        assert_eq!(1, sent.len());
        assert_eq!(MsgType::TestRequest, sent[0].1.msg_type().expect("type"));
        let id = sent[0]
            .1
            .get(tag::TEST_REQ_ID)
            .expect("test req id")
            .to_vec();
        // A peer message keeps the TestRequest from timing out.
        let heartbeat = inbound(MsgType::Heartbeat, 2, &[(tag::TEST_REQ_ID, id)]);
        session.on_inbound(&heartbeat, NOW + 31_000).expect("hb");
        let actions = session.on_tick(NOW + 62_000).expect("tick 2");
        let sent = sends(&actions);
        assert_eq!(1, sent.len());
        assert_eq!(MsgType::TestRequest, sent[0].1.msg_type().expect("type"));
    }

    #[test]
    fn a_bad_checksum_is_a_codec_error() {
        let mut session = session();
        let mut raw = logon(1, false);
        let checksum_start = raw
            .windows(3)
            .rposition(|window| window == b"10=")
            .expect("trailer");
        let digit = checksum_start + 3;
        raw[digit] = if raw[digit] == b'0' { b'1' } else { b'0' };
        assert!(matches!(
            session.on_inbound(&raw, NOW),
            Err(SessionError::Codec(CodecError::BadChecksum { .. }))
        ));
    }

    #[test]
    fn a_wrong_comp_id_is_logged_out() {
        let mut session = session();
        let wrong = MessageBuilder::new(
            MsgType::Logon,
            "SOMEONE",
            "DEEPBOOK",
            1,
            "20261025-12:34:56.000",
        )
        .body_int(tag::ENCRYPT_METHOD, 0)
        .body_int(tag::HEART_BT_INT, 30)
        .build();
        let actions = session.on_inbound(&wrong, NOW).expect("wrong");
        assert!(actions.iter().any(|action| matches!(
            action,
            Action::Disconnect { reason } if reason.contains("CompID")
        )));
    }

    #[test]
    fn the_peer_logout_is_answered_and_closes() {
        let mut session = session();
        session.on_inbound(&logon(1, false), NOW).expect("logon");
        let logout = inbound(MsgType::Logout, 2, &[(tag::TEXT, b"bye".to_vec())]);
        let actions = session.on_inbound(&logout, NOW + 1).expect("logout");
        assert!(matches!(actions[0], Action::Send { .. }));
        assert!(matches!(actions[1], Action::Disconnect { .. }));
        assert_eq!(SessionState::LoggedOut, session.state());
    }
}
