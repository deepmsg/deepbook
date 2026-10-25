//! The FIX 4.4 wire format: parse, validate and build.
//!
//! Every message is tag=value pairs separated by SOH (`0x01`). The codec
//! checks the three framing rules before anybody interprets a field:
//!
//! 1. `8=BeginString` is `FIX.4.4` and is the first field;
//! 2. `9=BodyLength` counts every byte from the start of `35=MsgType` to the
//!    SOH immediately before `10=CheckSum`;
//! 3. `10=CheckSum` is the sum of every byte up to that SOH, modulo 256,
//!    printed as three digits.
//!
//! Unknown fields are preserved: the resend path rewrites only the header
//! and leaves the body in its original order and size.

use std::fmt;

/// The field separator.
pub const SOH: u8 = 0x01;

/// The only BeginString this build understands.
pub const BEGIN_STRING: &str = "FIX.4.4";

/// The FIX tags the subset uses. `10` is the trailer.
pub mod tag {
    /// `BeginString`.
    pub const BEGIN_STRING: u32 = 8;
    /// `BodyLength`.
    pub const BODY_LENGTH: u32 = 9;
    /// `MsgType`.
    pub const MSG_TYPE: u32 = 35;
    /// `SenderCompID`.
    pub const SENDER_COMP_ID: u32 = 49;
    /// `TargetCompID`.
    pub const TARGET_COMP_ID: u32 = 56;
    /// `MsgSeqNum`.
    pub const MSG_SEQ_NUM: u32 = 34;
    /// `SendingTime`.
    pub const SENDING_TIME: u32 = 52;
    /// `PossDupFlag`.
    pub const POSS_DUP_FLAG: u32 = 43;
    /// `OrigSendingTime`.
    pub const ORIG_SENDING_TIME: u32 = 122;
    /// `CheckSum`.
    pub const CHECK_SUM: u32 = 10;

    /// `BeginSeqNo`.
    pub const BEGIN_SEQ_NO: u32 = 7;
    /// `EndSeqNo`.
    pub const END_SEQ_NO: u32 = 16;
    /// `NewSeqNo`.
    pub const NEW_SEQ_NO: u32 = 36;
    /// `GapFillFlag`.
    pub const GAP_FILL_FLAG: u32 = 123;
    /// `RefSeqNum`.
    pub const REF_SEQ_NUM: u32 = 45;
    /// `Text`.
    pub const TEXT: u32 = 58;
    /// `HeartBtInt`, seconds.
    pub const HEART_BT_INT: u32 = 108;
    /// `TestReqID`.
    pub const TEST_REQ_ID: u32 = 112;
    /// `EncryptMethod`.
    pub const ENCRYPT_METHOD: u32 = 98;
    /// `ResetSeqNumFlag`.
    pub const RESET_SEQ_NUM_FLAG: u32 = 141;
    /// `Username`.
    pub const USERNAME: u32 = 553;
    /// `Password`.
    pub const PASSWORD: u32 = 554;
    /// `RefTagID`.
    pub const REF_TAG_ID: u32 = 371;
    /// `RefMsgType`.
    pub const REF_MSG_TYPE: u32 = 372;
    /// `SessionRejectReason`.
    pub const SESSION_REJECT_REASON: u32 = 373;

    /// `ClOrdID`.
    pub const CL_ORD_ID: u32 = 11;
    /// `OrigClOrdID`.
    pub const ORIG_CL_ORD_ID: u32 = 41;
    /// `Account`.
    pub const ACCOUNT: u32 = 1;
    /// `ExecID`.
    pub const EXEC_ID: u32 = 17;
    /// `OrderID`.
    pub const ORDER_ID: u32 = 37;
    /// `ExecType`.
    pub const EXEC_TYPE: u32 = 150;
    /// `OrdStatus`.
    pub const ORD_STATUS: u32 = 39;
    /// `LastQty`.
    pub const LAST_QTY: u32 = 32;
    /// `LastPx`.
    pub const LAST_PX: u32 = 31;
    /// `LeavesQty`.
    pub const LEAVES_QTY: u32 = 151;
    /// `CumQty`.
    pub const CUM_QTY: u32 = 14;
    /// `AvgPx`.
    pub const AVG_PX: u32 = 6;
    /// `Symbol`.
    pub const SYMBOL: u32 = 55;
    /// `Side`.
    pub const SIDE: u32 = 54;
    /// `OrderQty`.
    pub const ORDER_QTY: u32 = 38;
    /// `OrdType`.
    pub const ORD_TYPE: u32 = 40;
    /// `Price`.
    pub const PRICE: u32 = 44;
    /// `TimeInForce`.
    pub const TIME_IN_FORCE: u32 = 59;
    /// `TransactTime`.
    pub const TRANSACT_TIME: u32 = 60;
    /// `HandlInst`.
    pub const HANDL_INST: u32 = 21;
    /// `CxlRejResponseTo`.
    pub const CXL_REJ_RESPONSE_TO: u32 = 434;
    /// `CxlRejReason`.
    pub const CXL_REJ_REASON: u32 = 102;
    /// `OrdRejReason`.
    pub const ORD_REJ_REASON: u32 = 103;
}

/// A message type this build knows.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MsgType {
    /// `A`.
    Logon,
    /// `5`.
    Logout,
    /// `0`.
    Heartbeat,
    /// `1`.
    TestRequest,
    /// `2`.
    ResendRequest,
    /// `4`.
    SequenceReset,
    /// `3`.
    Reject,
    /// `D`.
    NewOrderSingle,
    /// `F`.
    OrderCancelRequest,
    /// `G`.
    OrderCancelReplaceRequest,
    /// `8`.
    ExecutionReport,
    /// `9`.
    OrderCancelReject,
    /// `H`.
    OrderStatusRequest,
}

impl MsgType {
    /// Parse the two-byte `35` value.
    #[must_use]
    pub fn parse(value: &[u8]) -> Option<Self> {
        match value {
            b"A" => Some(Self::Logon),
            b"5" => Some(Self::Logout),
            b"0" => Some(Self::Heartbeat),
            b"1" => Some(Self::TestRequest),
            b"2" => Some(Self::ResendRequest),
            b"4" => Some(Self::SequenceReset),
            b"3" => Some(Self::Reject),
            b"D" => Some(Self::NewOrderSingle),
            b"F" => Some(Self::OrderCancelRequest),
            b"G" => Some(Self::OrderCancelReplaceRequest),
            b"8" => Some(Self::ExecutionReport),
            b"9" => Some(Self::OrderCancelReject),
            b"H" => Some(Self::OrderStatusRequest),
            _ => None,
        }
    }

    /// The wire value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Logon => "A",
            Self::Logout => "5",
            Self::Heartbeat => "0",
            Self::TestRequest => "1",
            Self::ResendRequest => "2",
            Self::SequenceReset => "4",
            Self::Reject => "3",
            Self::NewOrderSingle => "D",
            Self::OrderCancelRequest => "F",
            Self::OrderCancelReplaceRequest => "G",
            Self::ExecutionReport => "8",
            Self::OrderCancelReject => "9",
            Self::OrderStatusRequest => "H",
        }
    }

    /// Whether the type is session-level (admin). A resend may gap-fill these
    /// and must never gap-fill an application message.
    #[must_use]
    pub const fn is_admin(self) -> bool {
        matches!(
            self,
            Self::Logon
                | Self::Logout
                | Self::Heartbeat
                | Self::TestRequest
                | Self::ResendRequest
                | Self::SequenceReset
                | Self::Reject
        )
    }
}

/// A framing or field problem.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CodecError {
    /// The bytes do not end in SOH or contain an empty field.
    Malformed(String),
    /// A required field is absent.
    Missing {
        /// The tag.
        tag: u32,
    },
    /// A field is present but cannot be read as its type.
    Invalid {
        /// The tag.
        tag: u32,
        /// The value, lossily decoded.
        value: String,
    },
    /// `8` is not `FIX.4.4` or is not the first field.
    NotFix44,
    /// `35` names a message type this build does not know.
    UnsupportedMsgType(String),
    /// `9` does not match the bytes between it and `10`.
    BadBodyLength {
        /// The value of `9`.
        declared: u32,
        /// The bytes actually between `9` and `10`.
        actual: u32,
    },
    /// `10` does not match the bytes before it.
    BadChecksum {
        /// The value of `10`.
        declared: u8,
        /// The sum of the bytes before `10`.
        actual: u8,
    },
}

impl fmt::Display for CodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed(detail) => write!(formatter, "malformed FIX message: {detail}"),
            Self::Missing { tag } => write!(formatter, "missing required tag {tag}"),
            Self::Invalid { tag, value } => {
                write!(formatter, "tag {tag} has invalid value {value:?}")
            }
            Self::NotFix44 => write!(formatter, "BeginString is not FIX.4.4"),
            Self::UnsupportedMsgType(value) => write!(formatter, "unsupported MsgType {value:?}"),
            Self::BadBodyLength { declared, actual } => {
                write!(formatter, "BodyLength={declared}, actual={actual}")
            }
            Self::BadChecksum { declared, actual } => {
                write!(formatter, "CheckSum={declared:03}, actual={actual:03}")
            }
        }
    }
}

impl std::error::Error for CodecError {}

/// One tag=value pair.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Field {
    /// The tag.
    pub tag: u32,
    /// The value bytes, exactly as they were on the wire.
    pub value: Vec<u8>,
}

/// A parsed message, with its framing already validated.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedMessage {
    fields: Vec<Field>,
    raw: Vec<u8>,
}

impl ParsedMessage {
    /// The value of the first field with `tag`.
    #[must_use]
    pub fn get(&self, tag: u32) -> Option<&[u8]> {
        self.fields
            .iter()
            .find(|field| field.tag == tag)
            .map(|field| field.value.as_slice())
    }

    /// A UTF-8 value.
    ///
    /// # Errors
    ///
    /// The field is absent or is not UTF-8.
    pub fn get_str(&self, tag: u32) -> Result<&str, CodecError> {
        let value = self.get(tag).ok_or(CodecError::Missing { tag })?;
        std::str::from_utf8(value).map_err(|_| CodecError::Invalid {
            tag,
            value: String::from_utf8_lossy(value).into_owned(),
        })
    }

    /// An unsigned integer value.
    ///
    /// # Errors
    ///
    /// The field is absent or is not an unsigned integer.
    pub fn get_u64(&self, tag: u32) -> Result<u64, CodecError> {
        let value = self.get_str(tag)?;
        value.parse().map_err(|_| CodecError::Invalid {
            tag,
            value: value.to_owned(),
        })
    }

    /// A signed integer value.
    ///
    /// # Errors
    ///
    /// The field is absent or is not an integer.
    pub fn get_i64(&self, tag: u32) -> Result<i64, CodecError> {
        let value = self.get_str(tag)?;
        value.parse().map_err(|_| CodecError::Invalid {
            tag,
            value: value.to_owned(),
        })
    }

    /// The message type.
    ///
    /// # Errors
    ///
    /// `35` is absent or names a type this build does not know.
    pub fn msg_type(&self) -> Result<MsgType, CodecError> {
        let value = self
            .get(tag::MSG_TYPE)
            .ok_or(CodecError::Missing { tag: tag::MSG_TYPE })?;
        MsgType::parse(value).ok_or_else(|| {
            CodecError::UnsupportedMsgType(String::from_utf8_lossy(value).into_owned())
        })
    }

    /// `34`.
    ///
    /// # Errors
    ///
    /// The field is absent or invalid.
    pub fn seq_num(&self) -> Result<u64, CodecError> {
        self.get_u64(tag::MSG_SEQ_NUM)
    }

    /// `52`.
    ///
    /// # Errors
    ///
    /// The field is absent or is not UTF-8.
    pub fn sending_time(&self) -> Result<&str, CodecError> {
        self.get_str(tag::SENDING_TIME)
    }

    /// `43=Y`.
    #[must_use]
    pub fn poss_dup(&self) -> bool {
        self.get(tag::POSS_DUP_FLAG) == Some(b"Y")
    }

    /// `122`.
    #[must_use]
    pub fn orig_sending_time(&self) -> Option<&[u8]> {
        self.get(tag::ORIG_SENDING_TIME)
    }

    /// The fields in wire order.
    #[must_use]
    pub fn fields(&self) -> &[Field] {
        &self.fields
    }

    /// The body fields in wire order, excluding the standard header and
    /// trailer.
    pub fn body_fields(&self) -> impl Iterator<Item = &Field> {
        self.fields.iter().filter(|field| !is_header_tag(field.tag))
    }

    /// Header fields beyond the seven this build writes itself. A resend
    /// carries them through unchanged (e.g. `50`, `142`).
    pub fn extra_header_fields(&self) -> impl Iterator<Item = &Field> {
        self.fields.iter().filter(|field| {
            is_header_tag(field.tag)
                && !matches!(
                    field.tag,
                    tag::BEGIN_STRING
                        | tag::BODY_LENGTH
                        | tag::MSG_TYPE
                        | tag::SENDER_COMP_ID
                        | tag::TARGET_COMP_ID
                        | tag::MSG_SEQ_NUM
                        | tag::SENDING_TIME
                        | tag::POSS_DUP_FLAG
                        | tag::ORIG_SENDING_TIME
                        | tag::CHECK_SUM
                )
        })
    }

    /// The bytes this message was parsed from.
    #[must_use]
    pub fn raw(&self) -> &[u8] {
        &self.raw
    }
}

/// Whether `tag` belongs to the FIX 4.4 session header.
///
/// The list is the standard header block, not just the fields this build
/// writes: a peer may send `50`, `142`, `115` and friends, and a resend must
/// keep them.
#[must_use]
pub fn is_header_tag(tag: u32) -> bool {
    matches!(
        tag,
        tag::BEGIN_STRING
            | tag::BODY_LENGTH
            | tag::MSG_TYPE
            | tag::SENDER_COMP_ID
            | tag::TARGET_COMP_ID
            | 115
            | 128
            | 90
            | 91
            | tag::MSG_SEQ_NUM
            | 50
            | 142
            | 57
            | 143
            | 116
            | 144
            | 129
            | 145
            | tag::POSS_DUP_FLAG
            | 97
            | tag::SENDING_TIME
            | tag::ORIG_SENDING_TIME
            | 212
            | 213
            | 347
            | 369
            | 370
            | tag::CHECK_SUM
    )
}

/// Parse and validate one message.
///
/// # Errors
///
/// The framing rules or the field syntax were broken.
pub fn parse(raw: &[u8]) -> Result<ParsedMessage, CodecError> {
    let fields = split(raw)?;
    if fields.len() < 4 {
        return Err(CodecError::Malformed(format!(
            "{} fields, fewer than 8/9/35/10",
            fields.len()
        )));
    }

    let first = &fields[0];
    if first.tag != tag::BEGIN_STRING || first.value != BEGIN_STRING.as_bytes() {
        return Err(CodecError::NotFix44);
    }
    if fields[1].tag != tag::BODY_LENGTH {
        return Err(CodecError::Malformed(
            "9 is not the second field".to_owned(),
        ));
    }
    if fields[2].tag != tag::MSG_TYPE {
        return Err(CodecError::Malformed(
            "35 is not the third field".to_owned(),
        ));
    }
    let last = fields.last().expect("checked above");
    if last.tag != tag::CHECK_SUM || last.value.len() != 3 {
        return Err(CodecError::Malformed(
            "10 is not the last field or is not three digits".to_owned(),
        ));
    }
    for field in &fields[3..fields.len() - 1] {
        if matches!(
            field.tag,
            tag::BEGIN_STRING | tag::BODY_LENGTH | tag::MSG_TYPE | tag::CHECK_SUM
        ) {
            return Err(CodecError::Malformed(format!(
                "framing tag {} appears in the body",
                field.tag
            )));
        }
    }

    let body_start = fields[2].start;
    let checksum_start = last.start;
    let actual_body_length = u32::try_from(checksum_start - body_start)
        .map_err(|_| CodecError::Malformed("body length overflows u32".to_owned()))?;
    let declared_body_length =
        parse_decimal(&fields[1].value).ok_or_else(|| CodecError::Invalid {
            tag: tag::BODY_LENGTH,
            value: String::from_utf8_lossy(&fields[1].value).into_owned(),
        })?;
    if declared_body_length != actual_body_length {
        return Err(CodecError::BadBodyLength {
            declared: declared_body_length,
            actual: actual_body_length,
        });
    }

    let checksum: u8 = raw[..checksum_start]
        .iter()
        .fold(0_u8, |accumulator, byte| accumulator.wrapping_add(*byte));
    let declared_checksum = parse_decimal(&last.value)
        .and_then(|value| u8::try_from(value).ok())
        .ok_or_else(|| CodecError::Invalid {
            tag: tag::CHECK_SUM,
            value: String::from_utf8_lossy(&last.value).into_owned(),
        })?;
    if declared_checksum != checksum {
        return Err(CodecError::BadChecksum {
            declared: declared_checksum,
            actual: checksum,
        });
    }

    Ok(ParsedMessage {
        fields: fields
            .into_iter()
            .map(|field| Field {
                tag: field.tag,
                value: field.value,
            })
            .collect(),
        raw: raw.to_vec(),
    })
}

struct RawField {
    tag: u32,
    value: Vec<u8>,
    start: usize,
}

fn split(raw: &[u8]) -> Result<Vec<RawField>, CodecError> {
    if raw.is_empty() {
        return Err(CodecError::Malformed("empty".to_owned()));
    }
    if raw.last() != Some(&SOH) {
        return Err(CodecError::Malformed("does not end with SOH".to_owned()));
    }

    let mut fields = Vec::new();
    let mut start = 0;
    while start < raw.len() {
        let relative_end = raw[start..]
            .iter()
            .position(|byte| *byte == SOH)
            .ok_or_else(|| CodecError::Malformed("field has no SOH".to_owned()))?;
        let end = start + relative_end;
        if end == start {
            return Err(CodecError::Malformed("empty field".to_owned()));
        }
        let field = &raw[start..end];
        let separator = field
            .iter()
            .position(|byte| *byte == b'=')
            .ok_or_else(|| CodecError::Malformed("field has no '='".to_owned()))?;
        if separator == 0 {
            return Err(CodecError::Malformed("field has no tag".to_owned()));
        }
        let tag = parse_decimal(&field[..separator]).ok_or_else(|| {
            CodecError::Malformed(format!(
                "tag {:?} is not a number",
                String::from_utf8_lossy(&field[..separator])
            ))
        })?;
        fields.push(RawField {
            tag,
            value: field[separator + 1..].to_vec(),
            start,
        });
        start = end + 1;
    }

    Ok(fields)
}

fn parse_decimal(value: &[u8]) -> Option<u32> {
    if value.is_empty() || !value.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let mut number: u32 = 0;
    for byte in value {
        number = number
            .checked_mul(10)?
            .checked_add(u32::from(byte - b'0'))?;
    }
    Some(number)
}

/// Build one message and compute its framing.
///
/// The header is written as `8, 9, 35, 49, 56, 34, 52`, then any extra header
/// fields in insertion order, then the body, then `10`.
#[derive(Clone, Debug)]
pub struct MessageBuilder {
    msg_type: MsgType,
    sender: String,
    target: String,
    seq: u64,
    sending_time: String,
    header: Vec<Field>,
    body: Vec<Field>,
}

impl MessageBuilder {
    /// A message with the seven header fields this build writes itself.
    #[must_use]
    pub fn new(
        msg_type: MsgType,
        sender: impl Into<String>,
        target: impl Into<String>,
        seq: u64,
        sending_time: impl Into<String>,
    ) -> Self {
        Self {
            msg_type,
            sender: sender.into(),
            target: target.into(),
            seq,
            sending_time: sending_time.into(),
            header: Vec::new(),
            body: Vec::new(),
        }
    }

    /// Append a header field (after `52`).
    #[must_use]
    pub fn header(mut self, tag: u32, value: impl Into<Vec<u8>>) -> Self {
        self.header.push(Field {
            tag,
            value: value.into(),
        });
        self
    }

    /// Append a body field.
    #[must_use]
    pub fn body(mut self, tag: u32, value: impl Into<Vec<u8>>) -> Self {
        self.body.push(Field {
            tag,
            value: value.into(),
        });
        self
    }

    /// Append a body field from a number.
    #[must_use]
    pub fn body_int(self, tag: u32, value: impl fmt::Display) -> Self {
        self.body(tag, value.to_string())
    }

    /// Append a body field from a string.
    #[must_use]
    pub fn body_str(self, tag: u32, value: &str) -> Self {
        self.body(tag, value.as_bytes().to_vec())
    }

    /// Serialise, computing `9` and `10`.
    #[must_use]
    pub fn build(self) -> Vec<u8> {
        let mut body = Vec::new();
        push_field(&mut body, tag::MSG_TYPE, self.msg_type.as_str().as_bytes());
        push_field(&mut body, tag::SENDER_COMP_ID, self.sender.as_bytes());
        push_field(&mut body, tag::TARGET_COMP_ID, self.target.as_bytes());
        push_field(&mut body, tag::MSG_SEQ_NUM, self.seq.to_string().as_bytes());
        push_field(&mut body, tag::SENDING_TIME, self.sending_time.as_bytes());
        for field in &self.header {
            push_field(&mut body, field.tag, &field.value);
        }
        for field in &self.body {
            push_field(&mut body, field.tag, &field.value);
        }

        let mut raw = Vec::with_capacity(body.len() + 32);
        raw.extend_from_slice(b"8=");
        raw.extend_from_slice(BEGIN_STRING.as_bytes());
        raw.push(SOH);
        raw.extend_from_slice(b"9=");
        raw.extend_from_slice(body.len().to_string().as_bytes());
        raw.push(SOH);
        raw.extend_from_slice(&body);
        let checksum = raw
            .iter()
            .fold(0_u8, |accumulator, byte| accumulator.wrapping_add(*byte));
        raw.extend_from_slice(format!("10={checksum:03}").as_bytes());
        raw.push(SOH);
        raw
    }
}

fn push_field(out: &mut Vec<u8>, tag: u32, value: &[u8]) {
    out.extend_from_slice(tag.to_string().as_bytes());
    out.push(b'=');
    out.extend_from_slice(value);
    out.push(SOH);
}

/// Rewrite a message for a resend: `43=Y`, `122` = its original `SendingTime`,
/// and a fresh `52`.
///
/// The sequence number and every body field stay as they were; unknown header
/// fields are carried through. The caller sends the result under the original
/// sequence number.
///
/// # Errors
///
/// The original does not parse or has no message type.
pub fn rewrite_for_resend(original: &[u8], sending_time: &str) -> Result<Vec<u8>, CodecError> {
    let parsed = parse(original)?;
    let msg_type = parsed.msg_type()?;
    let sender = parsed.get_str(tag::SENDER_COMP_ID)?;
    let target = parsed.get_str(tag::TARGET_COMP_ID)?;
    let seq = parsed.seq_num()?;
    let original_sending_time = parsed.sending_time()?;

    let mut builder = MessageBuilder::new(msg_type, sender, target, seq, sending_time)
        .header(tag::POSS_DUP_FLAG, b"Y".to_vec())
        .header(
            tag::ORIG_SENDING_TIME,
            original_sending_time.as_bytes().to_vec(),
        );
    for field in parsed.extra_header_fields() {
        builder = builder.header(field.tag, field.value.clone());
    }
    for field in parsed.body_fields() {
        builder = builder.body(field.tag, field.value.clone());
    }
    Ok(builder.build())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn logon() -> Vec<u8> {
        MessageBuilder::new(
            MsgType::Logon,
            "PEER",
            "DEEPBOOK",
            1,
            "20261025-12:00:00.000",
        )
        .body_int(tag::ENCRYPT_METHOD, 0)
        .body_int(tag::HEART_BT_INT, 30)
        .build()
    }

    #[test]
    fn a_built_message_parses() {
        let raw = logon();
        let parsed = parse(&raw).expect("parses");
        assert_eq!(MsgType::Logon, parsed.msg_type().expect("type"));
        assert_eq!(1, parsed.seq_num().expect("seq"));
        assert_eq!("PEER", parsed.get_str(tag::SENDER_COMP_ID).expect("sender"));
        assert_eq!(
            "DEEPBOOK",
            parsed.get_str(tag::TARGET_COMP_ID).expect("target")
        );
        assert_eq!(30, parsed.get_u64(tag::HEART_BT_INT).expect("heartbeat"));
    }

    #[test]
    fn a_bad_checksum_is_refused() {
        let mut raw = logon();
        let checksum_start = raw
            .windows(3)
            .rposition(|window| window == b"10=")
            .expect("10 field");
        let digit = checksum_start + 3;
        raw[digit] = if raw[digit] == b'0' { b'1' } else { b'0' };
        assert!(matches!(parse(&raw), Err(CodecError::BadChecksum { .. })));
    }

    #[test]
    fn a_bad_body_length_is_refused() {
        let mut raw = logon();
        let nine = raw
            .windows(2)
            .position(|window| window == b"9=")
            .expect("9 field")
            + 2;
        let end = nine
            + raw[nine..]
                .iter()
                .position(|byte| *byte == SOH)
                .expect("SOH");
        let declared: u32 = std::str::from_utf8(&raw[nine..end])
            .expect("digits")
            .parse()
            .expect("number");
        let replacement = (declared + 1).to_string();
        assert_eq!(replacement.len(), end - nine);
        raw[nine..end].copy_from_slice(replacement.as_bytes());
        // The checksum is now wrong too; fix it so BodyLength is the failure.
        let checksum_start = raw
            .windows(3)
            .rposition(|window| window == b"10=")
            .expect("10 field");
        let checksum: u8 = raw[..checksum_start]
            .iter()
            .fold(0_u8, |accumulator, byte| accumulator.wrapping_add(*byte));
        let trailer = format!("10={checksum:03}");
        raw[checksum_start..checksum_start + trailer.len()].copy_from_slice(trailer.as_bytes());
        assert!(matches!(parse(&raw), Err(CodecError::BadBodyLength { .. })));
    }

    #[test]
    fn a_message_that_does_not_end_in_soh_is_refused() {
        let mut raw = logon();
        raw.pop();
        assert!(matches!(parse(&raw), Err(CodecError::Malformed(_))));
    }

    #[test]
    fn a_non_fix_begin_string_is_refused() {
        let raw = b"8=FIX.4.2\x019=5\x0135=0\x0110=000\x01";
        assert_eq!(Err(CodecError::NotFix44), parse(raw));
    }

    #[test]
    fn a_resend_sets_poss_dup_and_keeps_the_sequence() {
        let original = MessageBuilder::new(
            MsgType::NewOrderSingle,
            "PEER",
            "DEEPBOOK",
            7,
            "20261025-12:00:00.000",
        )
        .body_str(tag::CL_ORD_ID, "C-1")
        .body_str(tag::SYMBOL, "AAPL")
        .body(tag::SIDE, b"1".to_vec())
        .body_int(tag::ORDER_QTY, 100)
        .build();
        let resent = rewrite_for_resend(&original, "20261025-12:00:01.000").expect("rewrites");
        let parsed = parse(&resent).expect("parses");
        assert_eq!(7, parsed.seq_num().expect("seq"));
        assert!(parsed.poss_dup());
        assert_eq!(
            Some(b"20261025-12:00:00.000".as_slice()),
            parsed.orig_sending_time()
        );
        assert_eq!(
            "20261025-12:00:01.000",
            parsed.sending_time().expect("sending time")
        );
        assert_eq!("C-1", parsed.get_str(tag::CL_ORD_ID).expect("cl ord"));
        assert_eq!(100, parsed.get_u64(tag::ORDER_QTY).expect("qty"));
    }

    #[test]
    fn admin_and_application_types_are_classified() {
        assert!(MsgType::Logon.is_admin());
        assert!(MsgType::ResendRequest.is_admin());
        assert!(!MsgType::NewOrderSingle.is_admin());
        assert!(!MsgType::ExecutionReport.is_admin());
    }
}
