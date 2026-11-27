//! DeepOUCH/1.0: the fixed-width binary order-entry protocol (ADR 0012).
//!
//! This crate is the wire layer only: frame codec, message catalog, the
//! `u64`/`ClientOrderId` bridge, and the enum byte tables. The gateway owns
//! sessions, journals, authentication and the mapping to `engine-core`.
#![forbid(unsafe_code)]

pub mod ids;
pub mod wire;

pub use ids::{from_client_order_id, to_client_order_id};
pub use wire::{
    FrameError, HEADER_LEN, MAX_PAYLOAD, Message, VERSION, decode, decode_exact, encode, frame_len,
};
