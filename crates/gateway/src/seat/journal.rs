//! The per-seat DeepOUCH journal (ADR 0012 §6.4).
//!
//! The gateway writes every outbound wire frame here before the socket sees
//! it, indexed by the service's `seat_seq`. A resync rotates to a new
//! generation file (`gen-<n>.journal`); old generations are never truncated in
//! place. A record is `seat_seq: u64 LE | length: u32 LE | frame bytes`; a
//! torn tail is truncated on open, so a crash cannot make the journal
//! unreadable.

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const RECORD_HEADER: usize = 12;

/// A journal failure.
#[derive(Debug)]
pub enum SeatJournalError {
    /// A filesystem operation failed.
    Io {
        /// The path.
        path: PathBuf,
        /// The cause.
        source: io::Error,
    },
    /// The file is not a sequence of contiguous records.
    Corrupt(String),
    /// An append skipped or repeated a sequence.
    OutOfOrder {
        /// The sequence offered.
        seq: u64,
        /// The sequence the journal expected.
        expected: u64,
    },
    /// A replay asked for a sequence the generation does not hold.
    OutOfRange {
        /// The sequence asked for.
        from: u64,
        /// The generation's oldest sequence, when it has one.
        oldest: Option<u64>,
        /// The generation's next sequence.
        next: u64,
    },
}

impl fmt::Display for SeatJournalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { path, source } => write!(formatter, "{}: {source}", path.display()),
            Self::Corrupt(detail) => write!(formatter, "seat journal is corrupt: {detail}"),
            Self::OutOfOrder { seq, expected } => {
                write!(
                    formatter,
                    "seat journal append seq {seq}, expected {expected}"
                )
            }
            Self::OutOfRange { from, oldest, next } => write!(
                formatter,
                "seat journal replay from {from}, oldest {oldest:?}, next {next}"
            ),
        }
    }
}

impl std::error::Error for SeatJournalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

/// One generation of a seat's outbound journal.
#[derive(Debug)]
pub struct SeatJournal {
    path: PathBuf,
    file: File,
    oldest: Option<u64>,
    next: u64,
    sync_every: u32,
    unsynced: u32,
    synced: u64,
}

impl SeatJournal {
    /// Open (or create) `gen-<generation>.journal` under `directory`.
    ///
    /// # Errors
    ///
    /// [`SeatJournalError`] for an I/O failure or a corrupt record sequence.
    pub fn open(directory: impl AsRef<Path>, generation: u64) -> Result<Self, SeatJournalError> {
        Self::open_with_sync(directory, generation, 1)
    }

    /// The same, syncing every `sync_every` appends; the tail is flushed by
    /// [`Self::sync`] (an acknowledgement forces it).
    ///
    /// # Errors
    ///
    /// As [`Self::open`].
    pub fn open_with_sync(
        directory: impl AsRef<Path>,
        generation: u64,
        sync_every: u32,
    ) -> Result<Self, SeatJournalError> {
        let directory = directory.as_ref();
        fs::create_dir_all(directory).map_err(|source| SeatJournalError::Io {
            path: directory.to_path_buf(),
            source,
        })?;
        let path = directory.join(format!("gen-{generation}.journal"));
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|source| SeatJournalError::Io {
                path: path.clone(),
                source,
            })?;
        let (oldest, next, valid) = scan(&mut file, &path)?;
        let length = file
            .metadata()
            .map_err(|source| SeatJournalError::Io {
                path: path.clone(),
                source,
            })?
            .len();
        if length != valid {
            file.set_len(valid).map_err(|source| SeatJournalError::Io {
                path: path.clone(),
                source,
            })?;
        }
        file.seek(SeekFrom::End(0))
            .map_err(|source| SeatJournalError::Io {
                path: path.clone(),
                source,
            })?;
        Ok(Self {
            path,
            file,
            oldest,
            next,
            sync_every: sync_every.max(1),
            unsynced: 0,
            synced: next.saturating_sub(1),
        })
    }

    /// The file path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The oldest sequence still held, if any frame was written.
    #[must_use]
    pub const fn oldest_seq(&self) -> Option<u64> {
        self.oldest
    }

    /// The next sequence this generation will accept.
    #[must_use]
    pub const fn next_seq(&self) -> u64 {
        self.next
    }

    /// Append one frame and make it durable.
    ///
    /// # Errors
    ///
    /// [`SeatJournalError`] for an out-of-order sequence or an I/O failure.
    pub fn append(&mut self, seat_seq: u64, frame: &[u8]) -> Result<(), SeatJournalError> {
        if seat_seq == 0 {
            return Err(SeatJournalError::OutOfOrder {
                seq: seat_seq,
                expected: self.next,
            });
        }
        if self.oldest.is_none() {
            self.oldest = Some(seat_seq);
            self.next = seat_seq;
        }
        if seat_seq != self.next {
            return Err(SeatJournalError::OutOfOrder {
                seq: seat_seq,
                expected: self.next,
            });
        }
        let length = u32::try_from(frame.len())
            .map_err(|_| SeatJournalError::Corrupt("frame is larger than u32".to_owned()))?;
        self.file
            .write_all(&seat_seq.to_le_bytes())
            .and_then(|()| self.file.write_all(&length.to_le_bytes()))
            .and_then(|()| self.file.write_all(frame))
            .map_err(|source| SeatJournalError::Io {
                path: self.path.clone(),
                source,
            })?;
        self.next = seat_seq.saturating_add(1);
        self.unsynced = self.unsynced.saturating_add(1);
        if self.sync_every <= 1 || self.unsynced >= self.sync_every {
            self.sync()?;
        }
        Ok(())
    }

    /// The last sequence known durable.
    #[must_use]
    pub const fn synced_seq(&self) -> u64 {
        self.synced
    }

    /// Flush the unsynced tail.
    ///
    /// # Errors
    ///
    /// [`SeatJournalError`] for an I/O failure.
    pub fn sync(&mut self) -> Result<(), SeatJournalError> {
        if self.unsynced == 0 {
            self.synced = self.synced.max(self.next.saturating_sub(1));
            return Ok(());
        }
        self.file
            .sync_data()
            .map_err(|source| SeatJournalError::Io {
                path: self.path.clone(),
                source,
            })?;
        self.unsynced = 0;
        self.synced = self.next.saturating_sub(1);
        Ok(())
    }

    /// The raw frames from `from` to the end of this generation.
    ///
    /// # Errors
    ///
    /// [`SeatJournalError::OutOfRange`] when `from` is before the oldest
    /// retained frame or beyond the next sequence.
    pub fn replay(&self, from: u64) -> Result<Vec<u8>, SeatJournalError> {
        if from == self.next {
            return Ok(Vec::new());
        }
        if self.oldest.is_none() || from < self.oldest.unwrap_or(self.next) || from >= self.next {
            return Err(SeatJournalError::OutOfRange {
                from,
                oldest: self.oldest,
                next: self.next,
            });
        }
        let bytes = fs::read(&self.path).map_err(|source| SeatJournalError::Io {
            path: self.path.clone(),
            source,
        })?;
        let mut offset = 0_usize;
        let mut out = Vec::new();
        while offset + RECORD_HEADER <= bytes.len() {
            let seq =
                u64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("eight bytes"));
            let length = u32::from_le_bytes(
                bytes[offset + 8..offset + RECORD_HEADER]
                    .try_into()
                    .expect("four bytes"),
            ) as usize;
            let start = offset + RECORD_HEADER;
            let end = start.saturating_add(length);
            if end > bytes.len() {
                break;
            }
            if seq >= from {
                out.extend_from_slice(&bytes[start..end]);
            }
            offset = end;
        }
        Ok(out)
    }
}

/// The newest generation under `directory` and its last journaled sequence.
///
/// `None` means the seat has no journal yet (a fresh gateway). A torn tail is
/// repaired by [`SeatJournal::open`] on the way.
///
/// # Errors
///
/// [`SeatJournalError`] for a directory that cannot be read or a latest file
/// that cannot be opened.
pub fn latest(directory: impl AsRef<Path>) -> Result<Option<(u64, u64)>, SeatJournalError> {
    let directory = directory.as_ref();
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(SeatJournalError::Io {
                path: directory.to_path_buf(),
                source,
            });
        }
    };
    let mut newest: Option<u64> = None;
    for entry in entries {
        let entry = entry.map_err(|source| SeatJournalError::Io {
            path: directory.to_path_buf(),
            source,
        })?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(generation) = name
            .strip_prefix("gen-")
            .and_then(|rest| rest.strip_suffix(".journal"))
            .and_then(|digits| digits.parse::<u64>().ok())
        else {
            continue;
        };
        newest = Some(newest.map_or(generation, |current| current.max(generation)));
    }
    let Some(generation) = newest else {
        return Ok(None);
    };
    let journal = SeatJournal::open(directory, generation)?;
    let last = journal
        .oldest_seq()
        .map_or(0, |_| journal.next_seq().saturating_sub(1));
    Ok(Some((generation, last)))
}

fn scan(file: &mut File, path: &Path) -> Result<(Option<u64>, u64, u64), SeatJournalError> {
    file.seek(SeekFrom::Start(0))
        .map_err(|source| SeatJournalError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|source| SeatJournalError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    let mut oldest = None;
    let mut next = 1_u64;
    let mut offset = 0_usize;
    while offset + RECORD_HEADER <= bytes.len() {
        let seq = u64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("eight bytes"));
        let length = u32::from_le_bytes(
            bytes[offset + 8..offset + RECORD_HEADER]
                .try_into()
                .expect("four bytes"),
        ) as usize;
        let end = (offset + RECORD_HEADER).saturating_add(length);
        if end > bytes.len() {
            break;
        }
        if oldest.is_none() {
            oldest = Some(seq);
            next = seq;
        }
        if seq != next {
            return Err(SeatJournalError::Corrupt(format!(
                "sequence {seq}, expected {next}"
            )));
        }
        next = seq.saturating_add(1);
        offset = end;
    }
    Ok((oldest, next, offset as u64))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(0);

    fn directory() -> PathBuf {
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("deepbook-seat-journal-{}-{id}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        path
    }

    #[test]
    fn append_replay_and_reopen() {
        let directory = directory();
        {
            let mut journal = SeatJournal::open(&directory, 1).expect("open");
            assert_eq!(journal.oldest_seq(), None);
            assert_eq!(journal.next_seq(), 1);
            journal.append(5, b"aaaa").expect("append");
            journal.append(6, b"bb").expect("append");
            assert_eq!(journal.oldest_seq(), Some(5));
            assert_eq!(journal.next_seq(), 7);
            assert_eq!(journal.replay(5).expect("replay"), b"aaaabb");
            assert_eq!(journal.replay(6).expect("replay"), b"bb");
            assert_eq!(journal.replay(7).expect("replay"), b"");
            assert!(matches!(
                journal.replay(4),
                Err(SeatJournalError::OutOfRange { .. })
            ));
            assert!(matches!(
                journal.replay(8),
                Err(SeatJournalError::OutOfRange { .. })
            ));
        }
        let journal = SeatJournal::open(&directory, 1).expect("reopen");
        assert_eq!(journal.oldest_seq(), Some(5));
        assert_eq!(journal.next_seq(), 7);
        assert_eq!(journal.replay(5).expect("replay"), b"aaaabb");
        let _ = fs::remove_dir_all(&directory);
    }

    #[test]
    fn a_torn_tail_is_truncated_on_open() {
        let directory = directory();
        {
            let mut journal = SeatJournal::open(&directory, 1).expect("open");
            journal.append(1, b"good").expect("append");
        }
        let path = directory.join("gen-1.journal");
        let mut file = OpenOptions::new().append(true).open(&path).expect("append");
        file.write_all(&2_u64.to_le_bytes()).expect("seq");
        file.write_all(&9_u32.to_le_bytes()).expect("length");
        file.write_all(b"bad").expect("short frame");
        drop(file);

        let mut journal = SeatJournal::open(&directory, 1).expect("reopen");
        assert_eq!(journal.next_seq(), 2);
        journal.append(2, b"ok").expect("append after repair");
        assert_eq!(journal.replay(1).expect("replay"), b"goodok");
        let _ = fs::remove_dir_all(&directory);
    }

    #[test]
    fn out_of_order_appends_are_refused() {
        let directory = directory();
        let mut journal = SeatJournal::open(&directory, 1).expect("open");
        journal.append(1, b"a").expect("append");
        assert!(matches!(
            journal.append(3, b"b"),
            Err(SeatJournalError::OutOfOrder { .. })
        ));
        assert!(matches!(
            journal.append(0, b"b"),
            Err(SeatJournalError::OutOfOrder { .. })
        ));
        let _ = fs::remove_dir_all(&directory);
    }
}
