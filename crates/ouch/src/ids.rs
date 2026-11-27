//! The DeepOUCH `u64` client-order-id mapping (ADR 0012 §5.3).
//!
//! `engine-core` keys idempotency on `ClientOrderId([u8; 20])`, which rejects
//! an empty value and any embedded NUL. DeepOUCH uses a `u64`. The bridge is a
//! zero-padded 20-byte ASCII decimal string: no NUL byte, no collision, and
//! `u64::MAX` is exactly 20 digits.
//!
//! `0` is not a valid order id on the wire; it is reserved for "no original
//! id" in cancel/replace responses.
#![forbid(unsafe_code)]

use deepbook_engine_core::ClientOrderId;

/// Map a non-zero `u64` to the engine's 20-byte client order id.
///
/// Returns `None` for `0`, which is the protocol's invalid/sentinel value.
#[must_use]
pub fn to_client_order_id(value: u64) -> Option<ClientOrderId> {
    if value == 0 {
        return None;
    }
    let mut bytes = [b'0'; ClientOrderId::LENGTH];
    let mut remaining = value;
    let mut index = bytes.len();
    while remaining > 0 {
        index -= 1;
        bytes[index] = b'0' + u8::try_from(remaining % 10).expect("a decimal digit");
        remaining /= 10;
    }
    Some(ClientOrderId::from_padded_bytes(bytes))
}

/// Map an engine client order id back to the wire `u64`.
///
/// Returns `None` for an empty id or one that is not all ASCII decimal digits
/// (for example a FIX `ClOrdID` that did not come from DeepOUCH).
#[must_use]
pub fn from_client_order_id(id: ClientOrderId) -> Option<u64> {
    let bytes = id.as_slice();
    if bytes.is_empty() {
        return None;
    }
    let mut value = 0_u64;
    for byte in bytes {
        if !byte.is_ascii_digit() {
            return None;
        }
        value = value.checked_mul(10)?.checked_add(u64::from(byte - b'0'))?;
    }
    Some(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_non_zero_values_are_ids() {
        assert!(to_client_order_id(0).is_none());
        assert!(to_client_order_id(1).is_some());
    }

    #[test]
    fn the_decimal_string_is_twenty_digits() {
        let id = to_client_order_id(42).expect("id");
        assert_eq!(id.as_bytes(), b"00000000000000000042");
        assert_eq!(id.as_slice(), b"00000000000000000042");
        assert!(!id.as_bytes().contains(&0));
    }

    #[test]
    fn u64_max_fills_the_width_exactly() {
        let id = to_client_order_id(u64::MAX).expect("id");
        assert_eq!(id.as_bytes(), b"18446744073709551615");
        assert_eq!(from_client_order_id(id), Some(u64::MAX));
    }

    #[test]
    fn values_round_trip() {
        for value in [1, 9, 10, 42, 999_999, u64::MAX] {
            let id = to_client_order_id(value).expect("id");
            assert_eq!(from_client_order_id(id), Some(value));
        }
    }

    #[test]
    fn non_decimal_ids_do_not_reverse() {
        let id: ClientOrderId = "ABC-1".parse().expect("client order id");
        assert_eq!(from_client_order_id(id), None);
        let id: ClientOrderId = "42".parse().expect("client order id");
        assert_eq!(from_client_order_id(id), Some(42));
        let empty = ClientOrderId::from_padded_bytes([0; ClientOrderId::LENGTH]);
        assert_eq!(from_client_order_id(empty), None);
    }
}
