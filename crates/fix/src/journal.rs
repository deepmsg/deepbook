//! The outbound journal and the persisted sequence numbers.
//!
//! A FIX session writes every outbound message to its journal **before** the
//! socket sees it. A `ResendRequest` is answered from this journal: an
//! application message is replayed byte for byte with `43=Y`, an admin range
//! is gap-filled. The journal keeps the last [`capacity`](MemoryJournal)
//! messages; a resend that starts before the oldest entry ends the session
//! rather than silently losing a command.
//!
//! M1's durability policy is one `fdatasync` per append
//! ([`File::sync_data`]); that is the O_DSYNC promise the design asks for
//! without adding a platform dependency. Batching is an M5 measurement.

use std::collections::VecDeque;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::codec::MsgType;

/// One message the session sent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalEntry {
    /// The outbound `MsgSeqNum`.
    pub seq: u64,
    /// The message type.
    pub msg_type: MsgType,
    /// The complete message, exactly as it went on the wire.
    pub raw: Vec<u8>,
    /// `SendingTime(52)` of the original send.
    pub sending_time: String,
}

/// A journal problem.
#[derive(Debug)]
pub enum JournalError {
    /// A filesystem operation failed.
    Io {
        /// The path involved.
        path: PathBuf,
        /// The underlying error.
        source: io::Error,
    },
    /// A record or state file is not what this build wrote.
    Corrupt(String),
    /// The caller asked for a sequence the journal does not hold.
    OutOfOrder {
        /// The sequence it tried to append.
        seq: u64,
        /// The sequence the journal expected.
        expected: u64,
    },
}

impl fmt::Display for JournalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { path, source } => write!(formatter, "{}: {source}", path.display()),
            Self::Corrupt(detail) => write!(formatter, "journal is corrupt: {detail}"),
            Self::OutOfOrder { seq, expected } => {
                write!(formatter, "journal append seq {seq}, expected {expected}")
            }
        }
    }
}

impl std::error::Error for JournalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

fn io_error(path: &Path, source: io::Error) -> JournalError {
    JournalError::Io {
        path: path.to_path_buf(),
        source,
    }
}

/// The journal a session writes to.
pub trait Journal {
    /// Append one message. The session calls this before the send.
    ///
    /// # Errors
    ///
    /// A storage error, or a sequence that is not after the last one.
    fn append(&mut self, entry: JournalEntry) -> Result<(), JournalError>;

    /// The entry for `seq`, if the window still holds it.
    fn get(&self, seq: u64) -> Option<&JournalEntry>;

    /// The oldest sequence the window holds.
    fn oldest_seq(&self) -> Option<u64>;

    /// The next sequence to append (the highest ever appended plus one).
    fn next_seq(&self) -> u64;

    /// How many entries the window holds.
    fn len(&self) -> usize;

    /// Whether the window is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Empty the journal, for a `141=Y` logon that resets the stream.
    ///
    /// # Errors
    ///
    /// A storage error.
    fn clear(&mut self) -> Result<(), JournalError>;
}

/// The last `capacity` messages, in memory.
#[derive(Debug)]
pub struct MemoryJournal {
    capacity: usize,
    entries: VecDeque<JournalEntry>,
    last_seq: u64,
}

impl MemoryJournal {
    /// A journal that keeps the last `capacity` messages (at least one).
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            entries: VecDeque::new(),
            last_seq: 0,
        }
    }

    fn from_parts(capacity: usize, entries: Vec<JournalEntry>, last_seq: u64) -> Self {
        let capacity = capacity.max(1);
        let mut journal = Self {
            capacity,
            entries: VecDeque::new(),
            last_seq,
        };
        for entry in entries {
            journal.push(entry);
        }
        journal
    }

    fn push(&mut self, entry: JournalEntry) {
        self.last_seq = entry.seq;
        self.entries.push_back(entry);
        while self.entries.len() > self.capacity {
            self.entries.pop_front();
        }
    }
}

impl Journal for MemoryJournal {
    fn append(&mut self, entry: JournalEntry) -> Result<(), JournalError> {
        let expected = self.last_seq + 1;
        if entry.seq < expected {
            return Err(JournalError::OutOfOrder {
                seq: entry.seq,
                expected,
            });
        }
        self.push(entry);
        Ok(())
    }

    fn get(&self, seq: u64) -> Option<&JournalEntry> {
        self.entries.iter().find(|entry| entry.seq == seq)
    }

    fn oldest_seq(&self) -> Option<u64> {
        self.entries.front().map(|entry| entry.seq)
    }

    fn next_seq(&self) -> u64 {
        self.last_seq + 1
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn clear(&mut self) -> Result<(), JournalError> {
        self.entries.clear();
        self.last_seq = 0;
        Ok(())
    }
}

const MAGIC: [u8; 4] = *b"DBJF";
const VERSION: u8 = 1;
const HEADER_LENGTH: usize = 4 + 1 + 8 + 2 + 1 + 4;

/// A journal backed by one append-only file.
///
/// Every append is `write` followed by `sync_data`; a crash can lose the
/// message being written, never a message the peer already saw. On open, a
/// truncated tail is discarded so the next append does not continue after a
/// half record.
#[derive(Debug)]
pub struct FileJournal {
    path: PathBuf,
    file: File,
    memory: MemoryJournal,
}

impl FileJournal {
    /// Open (or create) `path`, recovering the last `capacity` entries.
    ///
    /// # Errors
    ///
    /// A filesystem error.
    pub fn open(path: impl Into<PathBuf>, capacity: usize) -> Result<Self, JournalError> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| io_error(parent, error))?;
        }

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|error| io_error(&path, error))?;

        let mut contents = Vec::new();
        file.read_to_end(&mut contents)
            .map_err(|error| io_error(&path, error))?;

        let mut entries = Vec::new();
        let mut last_seq = 0;
        let mut offset = 0;
        while offset < contents.len() {
            match decode_record(&contents[offset..]) {
                Ok((entry, used)) => {
                    last_seq = entry.seq;
                    entries.push(entry);
                    offset += used;
                }
                Err(_) => break,
            }
        }
        if offset < contents.len() {
            file.set_len(u64::try_from(offset).unwrap_or(0))
                .map_err(|error| io_error(&path, error))?;
            file.sync_data().map_err(|error| io_error(&path, error))?;
        }
        file.seek(SeekFrom::Start(offset as u64))
            .map_err(|error| io_error(&path, error))?;

        Ok(Self {
            path,
            file,
            memory: MemoryJournal::from_parts(capacity, entries, last_seq),
        })
    }

    /// The file this journal appends to.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Journal for FileJournal {
    fn append(&mut self, entry: JournalEntry) -> Result<(), JournalError> {
        let expected = self.memory.next_seq();
        if entry.seq < expected {
            return Err(JournalError::OutOfOrder {
                seq: entry.seq,
                expected,
            });
        }
        let record = encode_record(&entry);
        self.file
            .write_all(&record)
            .map_err(|error| io_error(&self.path, error))?;
        self.file
            .sync_data()
            .map_err(|error| io_error(&self.path, error))?;
        self.memory.append(entry)
    }

    fn get(&self, seq: u64) -> Option<&JournalEntry> {
        self.memory.get(seq)
    }

    fn oldest_seq(&self) -> Option<u64> {
        self.memory.oldest_seq()
    }

    fn next_seq(&self) -> u64 {
        self.memory.next_seq()
    }

    fn len(&self) -> usize {
        self.memory.len()
    }

    fn clear(&mut self) -> Result<(), JournalError> {
        self.file
            .set_len(0)
            .map_err(|error| io_error(&self.path, error))?;
        self.file
            .seek(SeekFrom::Start(0))
            .map_err(|error| io_error(&self.path, error))?;
        self.file
            .sync_data()
            .map_err(|error| io_error(&self.path, error))?;
        self.memory.clear()
    }
}

fn encode_record(entry: &JournalEntry) -> Vec<u8> {
    let sending_time = entry.sending_time.as_bytes();
    let msg_type = entry.msg_type.as_str().as_bytes();
    let mut record = Vec::with_capacity(HEADER_LENGTH + sending_time.len() + entry.raw.len());
    record.extend_from_slice(&MAGIC);
    record.push(VERSION);
    record.extend_from_slice(&entry.seq.to_le_bytes());
    record.extend_from_slice(
        &u16::try_from(sending_time.len())
            .unwrap_or(u16::MAX)
            .to_le_bytes(),
    );
    record.push(msg_type[0]);
    record.extend_from_slice(
        &u32::try_from(entry.raw.len())
            .unwrap_or(u32::MAX)
            .to_le_bytes(),
    );
    record.extend_from_slice(sending_time);
    record.extend_from_slice(&entry.raw);
    record
}

fn decode_record(bytes: &[u8]) -> Result<(JournalEntry, usize), JournalError> {
    if bytes.len() < HEADER_LENGTH {
        return Err(JournalError::Corrupt("short header".to_owned()));
    }
    if bytes[..4] != MAGIC {
        return Err(JournalError::Corrupt("magic".to_owned()));
    }
    if bytes[4] != VERSION {
        return Err(JournalError::Corrupt("version".to_owned()));
    }
    let seq = u64::from_le_bytes(bytes[5..13].try_into().expect("eight bytes"));
    let sending_time_length = usize::from(u16::from_le_bytes(
        bytes[13..15].try_into().expect("two bytes"),
    ));
    let msg_type = MsgType::parse(&bytes[15..16])
        .ok_or_else(|| JournalError::Corrupt("message type".to_owned()))?;
    let raw_length = usize::try_from(u32::from_le_bytes(
        bytes[16..20].try_into().expect("four bytes"),
    ))
    .map_err(|_| JournalError::Corrupt("raw length".to_owned()))?;
    let total = HEADER_LENGTH + sending_time_length + raw_length;
    if bytes.len() < total {
        return Err(JournalError::Corrupt("short record".to_owned()));
    }
    let sending_time =
        std::str::from_utf8(&bytes[HEADER_LENGTH..HEADER_LENGTH + sending_time_length])
            .map_err(|_| JournalError::Corrupt("sending time is not UTF-8".to_owned()))?
            .to_owned();
    let raw = bytes[HEADER_LENGTH + sending_time_length..total].to_vec();
    Ok((
        JournalEntry {
            seq,
            msg_type,
            raw,
            sending_time,
        },
        total,
    ))
}

/// The two sequence numbers a session persists.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Sequences {
    /// The next inbound message expected.
    pub next_in: u64,
    /// The next outbound message to send.
    pub next_out: u64,
}

impl Default for Sequences {
    fn default() -> Self {
        Self {
            next_in: 1,
            next_out: 1,
        }
    }
}

/// Where the session keeps its sequence numbers.
pub trait SequenceStore {
    /// Read the stored numbers, or the defaults for a new session.
    ///
    /// # Errors
    ///
    /// A storage or parse error.
    fn load(&self) -> Result<Sequences, JournalError>;

    /// Persist the numbers.
    ///
    /// # Errors
    ///
    /// A storage error.
    fn store(&mut self, sequences: Sequences) -> Result<(), JournalError>;
}

/// Sequence numbers held in memory.
#[derive(Clone, Copy, Debug, Default)]
pub struct MemorySequenceStore {
    sequences: Sequences,
}

impl MemorySequenceStore {
    /// A store that starts at `sequences`.
    #[must_use]
    pub const fn new(sequences: Sequences) -> Self {
        Self { sequences }
    }
}

impl SequenceStore for MemorySequenceStore {
    fn load(&self) -> Result<Sequences, JournalError> {
        Ok(self.sequences)
    }

    fn store(&mut self, sequences: Sequences) -> Result<(), JournalError> {
        self.sequences = sequences;
        Ok(())
    }
}

/// Sequence numbers in a one-line file, replaced atomically.
#[derive(Clone, Debug)]
pub struct FileSequenceStore {
    path: PathBuf,
}

impl FileSequenceStore {
    /// A store at `path`.
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// The file this store writes.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl SequenceStore for FileSequenceStore {
    fn load(&self) -> Result<Sequences, JournalError> {
        let text = match std::fs::read_to_string(&self.path) {
            Ok(text) => text,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(Sequences::default());
            }
            Err(error) => return Err(io_error(&self.path, error)),
        };
        let mut parts = text.split_whitespace();
        let next_in = parts
            .next()
            .ok_or_else(|| JournalError::Corrupt("empty state file".to_owned()))?
            .parse()
            .map_err(|_| JournalError::Corrupt("next_in".to_owned()))?;
        let next_out = parts
            .next()
            .ok_or_else(|| JournalError::Corrupt("state file has one number".to_owned()))?
            .parse()
            .map_err(|_| JournalError::Corrupt("next_out".to_owned()))?;
        if parts.next().is_some() {
            return Err(JournalError::Corrupt(
                "state file has more than two numbers".to_owned(),
            ));
        }
        Ok(Sequences { next_in, next_out })
    }

    fn store(&mut self, sequences: Sequences) -> Result<(), JournalError> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| io_error(parent, error))?;
        }
        let temporary = self.path.with_extension("tmp");
        {
            let mut file = File::create(&temporary).map_err(|error| io_error(&temporary, error))?;
            writeln!(file, "{} {}", sequences.next_in, sequences.next_out)
                .map_err(|error| io_error(&temporary, error))?;
            file.sync_all()
                .map_err(|error| io_error(&temporary, error))?;
        }
        std::fs::rename(&temporary, &self.path).map_err(|error| io_error(&self.path, error))?;
        if let Some(parent) = self.path.parent() {
            // Make the rename durable. Opening a directory read-only is enough
            // for fsync on Linux.
            if let Ok(directory) = File::open(parent) {
                let _ = directory.sync_all();
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(seq: u64, msg_type: MsgType) -> JournalEntry {
        JournalEntry {
            seq,
            msg_type,
            raw: format!("raw-{seq}").into_bytes(),
            sending_time: format!("20261025-12:00:{seq:02}.000"),
        }
    }

    fn temporary_directory(name: &str) -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |duration| duration.as_nanos());
        let path = std::env::temp_dir().join(format!(
            "deepbook-fix-{}-{name}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).expect("temporary directory");
        path
    }

    #[test]
    fn the_memory_journal_evicts_the_oldest_entry() {
        let mut journal = MemoryJournal::new(2);
        journal
            .append(entry(1, MsgType::NewOrderSingle))
            .expect("1");
        journal
            .append(entry(2, MsgType::ExecutionReport))
            .expect("2");
        journal.append(entry(3, MsgType::Heartbeat)).expect("3");
        assert_eq!(2, journal.len());
        assert_eq!(Some(2), journal.oldest_seq());
        assert_eq!(4, journal.next_seq());
        assert!(journal.get(1).is_none());
        assert_eq!(3, journal.get(3).expect("3").seq);
    }

    #[test]
    fn the_memory_journal_refuses_a_reused_sequence() {
        let mut journal = MemoryJournal::new(4);
        journal.append(entry(1, MsgType::Logon)).expect("1");
        assert!(matches!(
            journal.append(entry(1, MsgType::Heartbeat)),
            Err(JournalError::OutOfOrder {
                seq: 1,
                expected: 2
            })
        ));
    }

    #[test]
    fn a_file_journal_recovers_its_entries() {
        let directory = temporary_directory("journal");
        let path = directory.join("journal.bin");
        {
            let mut journal = FileJournal::open(&path, 16).expect("open");
            journal.append(entry(1, MsgType::Logon)).expect("1");
            journal
                .append(entry(2, MsgType::NewOrderSingle))
                .expect("2");
            journal
                .append(entry(3, MsgType::ExecutionReport))
                .expect("3");
        }
        let journal = FileJournal::open(&path, 16).expect("reopen");
        assert_eq!(3, journal.len());
        assert_eq!(Some(1), journal.oldest_seq());
        assert_eq!(4, journal.next_seq());
        assert_eq!(b"raw-2", journal.get(2).expect("2").raw.as_slice());
        std::fs::remove_dir_all(directory).expect("cleanup");
    }

    #[test]
    fn a_truncated_tail_is_discarded_on_open() {
        let directory = temporary_directory("truncated");
        let path = directory.join("journal.bin");
        {
            let mut journal = FileJournal::open(&path, 16).expect("open");
            journal.append(entry(1, MsgType::Logon)).expect("1");
            journal.append(entry(2, MsgType::Heartbeat)).expect("2");
        }
        let length = std::fs::metadata(&path).expect("metadata").len();
        let file = OpenOptions::new().write(true).open(&path).expect("open");
        file.set_len(length - 3).expect("truncate");
        drop(file);

        let mut journal = FileJournal::open(&path, 16).expect("reopen");
        assert_eq!(1, journal.len());
        assert_eq!(2, journal.next_seq());
        journal
            .append(entry(2, MsgType::Heartbeat))
            .expect("append");
        assert_eq!(3, journal.next_seq());
        std::fs::remove_dir_all(directory).expect("cleanup");
    }

    #[test]
    fn clearing_a_file_journal_empties_the_file() {
        let directory = temporary_directory("clear");
        let path = directory.join("journal.bin");
        let mut journal = FileJournal::open(&path, 16).expect("open");
        journal.append(entry(1, MsgType::Logon)).expect("1");
        journal.clear().expect("clear");
        assert!(journal.is_empty());
        assert_eq!(1, journal.next_seq());
        assert_eq!(0, std::fs::metadata(&path).expect("metadata").len());
        std::fs::remove_dir_all(directory).expect("cleanup");
    }

    #[test]
    fn sequence_numbers_round_trip_through_a_file() {
        let directory = temporary_directory("sequences");
        let path = directory.join("sequences");
        let mut store = FileSequenceStore::new(&path);
        assert_eq!(Sequences::default(), store.load().expect("default"));
        store
            .store(Sequences {
                next_in: 7,
                next_out: 9,
            })
            .expect("store");
        assert_eq!(
            Sequences {
                next_in: 7,
                next_out: 9
            },
            store.load().expect("load")
        );
        std::fs::remove_dir_all(directory).expect("cleanup");
    }
}
