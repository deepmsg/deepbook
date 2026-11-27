//! The static seat registry (`seats.toml`, ADR 0012 §6.1).
//!
//! The cluster and every gateway load the same file. A seat is either a FIX
//! seat (one `SenderCompID`/`TargetCompID` pair) or a DeepOUCH seat (one
//! 32-byte pre-shared token). `seat_id` and `account_id` are both unique; the
//! service pins them on the first bind, so a mismatch here is a configuration
//! error, not a runtime surprise.
//!
//! The parser accepts a deliberately small TOML subset: `[[seat]]` blocks,
//! `key = value` lines, integers, quoted strings, and `#` comments. That is
//! enough for the registry and keeps the gateway free of a TOML dependency.

use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;

/// The protocol a seat speaks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SeatProtocol {
    /// FIX 4.4.
    Fix,
    /// DeepOUCH/1.0.
    Ouch,
}

/// One configured seat.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SeatConfig {
    /// The seat id.
    pub seat_id: u16,
    /// The pinned account.
    pub account_id: u32,
    /// The protocol.
    pub protocol: SeatProtocol,
    /// The DeepOUCH pre-shared token.
    pub token: Option<[u8; 32]>,
    /// The FIX sender.
    pub sender: Option<String>,
    /// The FIX target.
    pub target: Option<String>,
    /// The per-seat journal directory.
    pub journal_dir: PathBuf,
}

/// A registry parse or validation failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SeatConfigError {
    /// The 1-based line, or `0` for a whole-registry error.
    pub line: usize,
    /// What is wrong.
    pub detail: String,
}

impl fmt::Display for SeatConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.line == 0 {
            write!(formatter, "seats: {}", self.detail)
        } else {
            write!(formatter, "seats line {}: {}", self.line, self.detail)
        }
    }
}

impl std::error::Error for SeatConfigError {}

impl SeatConfigError {
    fn at(line: usize, detail: impl Into<String>) -> Self {
        Self {
            line,
            detail: detail.into(),
        }
    }

    fn whole(detail: impl Into<String>) -> Self {
        Self {
            line: 0,
            detail: detail.into(),
        }
    }
}

/// The parsed seat registry.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SeatRegistry {
    seats: BTreeMap<u16, SeatConfig>,
    accounts: BTreeMap<u32, u16>,
}

impl SeatRegistry {
    /// Parse `seats.toml`.
    ///
    /// # Errors
    ///
    /// [`SeatConfigError`] for a syntax error, a missing required field, a
    /// duplicate seat/account, or a token that is not 64 hex digits.
    pub fn parse(text: &str) -> Result<Self, SeatConfigError> {
        let mut registry = Self::default();
        let mut builder: Option<SeatBuilder> = None;
        for (index, raw) in text.lines().enumerate() {
            let line_number = index + 1;
            let line = raw.split('#').next().unwrap_or("").trim();
            if line.is_empty() {
                continue;
            }
            if line == "[[seat]]" {
                if let Some(builder) = builder.take() {
                    registry.insert(builder.finish()?)?;
                }
                builder = Some(SeatBuilder::new(line_number));
                continue;
            }
            if line.starts_with('[') {
                return Err(SeatConfigError::at(
                    line_number,
                    "only [[seat]] tables are supported",
                ));
            }
            let Some(builder) = builder.as_mut() else {
                return Err(SeatConfigError::at(
                    line_number,
                    "a key appears before the first [[seat]] table",
                ));
            };
            let Some((key, value)) = line.split_once('=') else {
                return Err(SeatConfigError::at(line_number, "expected key = value"));
            };
            builder.set(key.trim(), value.trim(), line_number)?;
        }
        if let Some(builder) = builder {
            registry.insert(builder.finish()?)?;
        }
        Ok(registry)
    }

    fn insert(&mut self, seat: SeatConfig) -> Result<(), SeatConfigError> {
        if self.seats.contains_key(&seat.seat_id) {
            return Err(SeatConfigError::whole(format!(
                "seat {} is defined twice",
                seat.seat_id
            )));
        }
        if self.accounts.contains_key(&seat.account_id) {
            return Err(SeatConfigError::whole(format!(
                "account {} is claimed by more than one seat",
                seat.account_id
            )));
        }
        self.accounts.insert(seat.account_id, seat.seat_id);
        self.seats.insert(seat.seat_id, seat);
        Ok(())
    }

    /// The seat with this id.
    #[must_use]
    pub fn get(&self, seat_id: u16) -> Option<&SeatConfig> {
        self.seats.get(&seat_id)
    }

    /// The seat pinned to this account.
    #[must_use]
    pub fn seat_for_account(&self, account_id: u32) -> Option<u16> {
        self.accounts.get(&account_id).copied()
    }

    /// Every seat, id order.
    pub fn iter(&self) -> impl Iterator<Item = &SeatConfig> {
        self.seats.values()
    }

    /// How many seats are configured.
    #[must_use]
    pub fn len(&self) -> usize {
        self.seats.len()
    }

    /// Whether the registry is empty (the legacy single-peer mode).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.seats.is_empty()
    }
}

struct SeatBuilder {
    line: usize,
    seat_id: Option<u16>,
    account_id: Option<u32>,
    protocol: Option<SeatProtocol>,
    token: Option<[u8; 32]>,
    sender: Option<String>,
    target: Option<String>,
    journal: Option<String>,
}

impl SeatBuilder {
    fn new(line: usize) -> Self {
        Self {
            line,
            seat_id: None,
            account_id: None,
            protocol: None,
            token: None,
            sender: None,
            target: None,
            journal: None,
        }
    }

    fn set(&mut self, key: &str, value: &str, line: usize) -> Result<(), SeatConfigError> {
        match key {
            "seat_id" => self.seat_id = Some(parse_u16(value, line, key)?),
            "account_id" => self.account_id = Some(parse_u32(value, line, key)?),
            "protocol" => {
                self.protocol = Some(match quoted(value, line, key)? {
                    "fix" => SeatProtocol::Fix,
                    "ouch" => SeatProtocol::Ouch,
                    other => {
                        return Err(SeatConfigError::at(
                            line,
                            format!("unknown protocol {other:?}"),
                        ));
                    }
                });
            }
            "token" => self.token = Some(parse_token(quoted(value, line, key)?, line)?),
            "sender" => self.sender = Some(quoted(value, line, key)?.to_owned()),
            "target" => self.target = Some(quoted(value, line, key)?.to_owned()),
            "journal" => self.journal = Some(quoted(value, line, key)?.to_owned()),
            other => {
                return Err(SeatConfigError::at(line, format!("unknown key {other:?}")));
            }
        }
        Ok(())
    }

    fn finish(self) -> Result<SeatConfig, SeatConfigError> {
        let seat_id = self
            .seat_id
            .ok_or_else(|| SeatConfigError::at(self.line, "seat_id is required"))?;
        let account_id = self
            .account_id
            .ok_or_else(|| SeatConfigError::at(self.line, "account_id is required"))?;
        let protocol = self
            .protocol
            .ok_or_else(|| SeatConfigError::at(self.line, "protocol is required"))?;
        match protocol {
            SeatProtocol::Ouch => {
                if self.token.is_none() {
                    return Err(SeatConfigError::at(
                        self.line,
                        "an ouch seat needs a 64-digit token",
                    ));
                }
                if self.sender.is_some() || self.target.is_some() {
                    return Err(SeatConfigError::at(
                        self.line,
                        "an ouch seat must not set sender/target",
                    ));
                }
            }
            SeatProtocol::Fix => {
                if self.sender.is_none() || self.target.is_none() {
                    return Err(SeatConfigError::at(
                        self.line,
                        "a fix seat needs sender and target",
                    ));
                }
                if self.token.is_some() {
                    return Err(SeatConfigError::at(
                        self.line,
                        "a fix seat must not set a token",
                    ));
                }
            }
        }
        let journal_dir = self
            .journal
            .map_or_else(|| PathBuf::from(format!("seat_{seat_id}")), PathBuf::from);
        Ok(SeatConfig {
            seat_id,
            account_id,
            protocol,
            token: self.token,
            sender: self.sender,
            target: self.target,
            journal_dir,
        })
    }
}

fn quoted<'a>(value: &'a str, line: usize, key: &str) -> Result<&'a str, SeatConfigError> {
    let bytes = value.as_bytes();
    if bytes.len() >= 2 && bytes[0] == b'"' && bytes[bytes.len() - 1] == b'"' {
        Ok(&value[1..value.len() - 1])
    } else {
        Err(SeatConfigError::at(
            line,
            format!("{key} needs a quoted string"),
        ))
    }
}

fn parse_u16(value: &str, line: usize, key: &str) -> Result<u16, SeatConfigError> {
    value
        .parse()
        .map_err(|_| SeatConfigError::at(line, format!("{key} is not a u16")))
}

fn parse_u32(value: &str, line: usize, key: &str) -> Result<u32, SeatConfigError> {
    value
        .parse()
        .map_err(|_| SeatConfigError::at(line, format!("{key} is not a u32")))
}

fn parse_token(value: &str, line: usize) -> Result<[u8; 32], SeatConfigError> {
    if value.len() != 64 {
        return Err(SeatConfigError::at(
            line,
            "token must be exactly 64 hex digits",
        ));
    }
    let mut token = [0_u8; 32];
    for (index, byte) in token.iter_mut().enumerate() {
        let pair = &value[index * 2..index * 2 + 2];
        *byte = u8::from_str_radix(pair, 16)
            .map_err(|_| SeatConfigError::at(line, "token is not hexadecimal"))?;
    }
    Ok(token)
}

#[cfg(test)]
mod tests {
    use super::*;

    const EMPTY: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

    fn registry() -> &'static str {
        r#"
# the maker
[[seat]]
seat_id = 1
account_id = 11
protocol = "ouch"
token = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
journal = "seats/one"

[[seat]]
seat_id = 2
account_id = 12
protocol = "fix"
sender = "DEEPBOOK"
target = "CLIENT"
"#
    }

    #[test]
    fn parses_both_protocols() {
        let registry = SeatRegistry::parse(registry()).expect("parse");
        assert_eq!(registry.len(), 2);
        let ouch = registry.get(1).expect("ouch");
        assert_eq!(ouch.account_id, 11);
        assert_eq!(ouch.protocol, SeatProtocol::Ouch);
        assert_eq!(ouch.token, Some(parse_token(EMPTY, 1).expect("token")));
        assert_eq!(ouch.journal_dir, PathBuf::from("seats/one"));
        let fix = registry.get(2).expect("fix");
        assert_eq!(fix.sender.as_deref(), Some("DEEPBOOK"));
        assert_eq!(fix.target.as_deref(), Some("CLIENT"));
        assert_eq!(fix.journal_dir, PathBuf::from("seat_2"));
        assert_eq!(registry.seat_for_account(11), Some(1));
        assert_eq!(registry.seat_for_account(99), None);
    }

    #[test]
    fn rejects_duplicates_and_bad_values() {
        let duplicate = r#"
[[seat]]
seat_id = 1
account_id = 11
protocol = "ouch"
token = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
[[seat]]
seat_id = 1
account_id = 12
protocol = "ouch"
token = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
"#;
        assert!(SeatRegistry::parse(duplicate).is_err());

        let bad_token = r#"
[[seat]]
seat_id = 1
account_id = 11
protocol = "ouch"
token = "00"
"#;
        assert!(SeatRegistry::parse(bad_token).is_err());

        let missing_sender = r#"
[[seat]]
seat_id = 1
account_id = 11
protocol = "fix"
target = "CLIENT"
"#;
        assert!(SeatRegistry::parse(missing_sender).is_err());

        let stray_key = "seat_id = 1\n";
        assert!(SeatRegistry::parse(stray_key).is_err());

        let unknown = r#"
[[seat]]
seat_id = 1
account_id = 11
protocol = "ouch"
token = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
color = "blue"
"#;
        assert!(SeatRegistry::parse(unknown).is_err());
    }

    #[test]
    fn an_empty_registry_is_legacy_mode() {
        let registry = SeatRegistry::parse("# nothing\n").expect("parse");
        assert!(registry.is_empty());
    }
}
