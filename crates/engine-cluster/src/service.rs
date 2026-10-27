//! The deepmsg `ClusteredService` adapter.
//!
//! It decodes the command record the gateway offered, applies it to
//! [`Engine`] with the log position from the callback header, and offers each
//! engine event back to the originating cluster session. `on_take_snapshot`
//! writes the `DBS1` record in as many records as the snapshot publication's
//! maximum payload needs; `on_start` concatenates them and restores.

use std::fmt;

use deepbook_engine_core::wire::{decode_command, encode_event};
use deepbook_engine_core::{Engine, EngineConfig, Event};
use deepbook_types::LogPos;
use deepmsg_cluster_service::service::{SnapshotReader, SnapshotWriter};
use deepmsg_cluster_service::{
    Cluster, ClusteredService, ContainerError, LogHeader, Role, SessionId,
};
use deepmsg_codec_cluster::close_reason::CloseReason;

use crate::snapshot::{CodecError, decode, encode};

/// A service-layer failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServiceError {
    /// The command record was not understood.
    Wire(deepbook_engine_core::WireError),
    /// The snapshot record was not understood.
    Codec(CodecError),
    /// The snapshot did not fit the configured engine.
    Snapshot(deepbook_engine_core::SnapshotError),
}

impl fmt::Display for ServiceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Wire(error) => write!(formatter, "{error}"),
            Self::Codec(error) => write!(formatter, "{error}"),
            Self::Snapshot(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for ServiceError {}

/// The engine service.
pub struct EngineService {
    config: EngineConfig,
    engine: Engine,
}

impl EngineService {
    /// A fresh engine with the deployment configuration.
    #[must_use]
    pub fn new(config: EngineConfig) -> Self {
        Self {
            config,
            engine: Engine::new(config),
        }
    }

    /// Restore from a `DBS1` snapshot record.
    ///
    /// # Errors
    ///
    /// The record is corrupt or does not fit the configuration.
    pub fn from_snapshot(config: EngineConfig, bytes: &[u8]) -> Result<Self, ServiceError> {
        let snapshot = decode(bytes).map_err(ServiceError::Codec)?;
        let engine = Engine::restore(config, &snapshot).map_err(ServiceError::Snapshot)?;
        Ok(Self { config, engine })
    }

    /// The engine, for a test or an operator.
    #[must_use]
    pub const fn engine(&self) -> &Engine {
        &self.engine
    }

    /// The engine's book hash.
    #[must_use]
    pub fn book_hash(&self) -> u64 {
        self.engine.book_hash()
    }

    /// The snapshot record.
    #[must_use]
    pub fn snapshot_bytes(&self) -> Vec<u8> {
        encode(&self.engine.snapshot())
    }

    /// Decode and apply one command at `log_pos`.
    ///
    /// # Errors
    ///
    /// The command record was not understood.
    pub fn apply(&mut self, payload: &[u8], log_pos: LogPos) -> Result<Vec<Event>, ServiceError> {
        let command = decode_command(payload).map_err(ServiceError::Wire)?;
        let mut events = Vec::new();
        self.engine.on_command(&command, log_pos, &mut events);
        Ok(events)
    }
}

impl ClusteredService for EngineService {
    fn on_start(
        &mut self,
        cluster: &mut Cluster<'_>,
        snapshot: Option<&mut SnapshotReader<'_>>,
    ) -> Result<(), ContainerError> {
        if let Some(reader) = snapshot {
            let bytes = read_snapshot(cluster, reader)?;
            let decoded = decode(&bytes).map_err(container)?;
            self.engine = Engine::restore(self.config, &decoded).map_err(container)?;
        }
        Ok(())
    }

    fn on_session_open(
        &mut self,
        _cluster: &mut Cluster<'_>,
        _session: SessionId,
        _timestamp: i64,
    ) -> Result<(), ContainerError> {
        Ok(())
    }

    fn on_session_close(
        &mut self,
        _cluster: &mut Cluster<'_>,
        _session: SessionId,
        _timestamp: i64,
        _reason: CloseReason,
    ) -> Result<(), ContainerError> {
        Ok(())
    }

    fn on_session_message(
        &mut self,
        cluster: &mut Cluster<'_>,
        session: Option<SessionId>,
        _timestamp: i64,
        payload: &[u8],
        header: &LogHeader,
    ) -> Result<(), ContainerError> {
        let events = self.apply(payload, header.position).map_err(container)?;
        if let Some(session) = session {
            for event in &events {
                let bytes = encode_event(event);
                offer_to_session(cluster, session, &bytes)?;
            }
        }
        Ok(())
    }

    fn on_timer_event(
        &mut self,
        _cluster: &mut Cluster<'_>,
        _correlation_id: i64,
        _timestamp: i64,
    ) -> Result<(), ContainerError> {
        Ok(())
    }

    fn on_take_snapshot(&mut self, writer: &mut SnapshotWriter<'_>) -> Result<(), ContainerError> {
        let bytes = self.snapshot_bytes();
        let chunk = writer.max_payload_length().max(1);
        for part in bytes.chunks(chunk) {
            loop {
                if writer.offer(part) > 0 {
                    break;
                }
                writer.idle();
            }
        }
        Ok(())
    }

    fn on_role_change(&mut self, _new_role: Role) -> Result<(), ContainerError> {
        Ok(())
    }

    fn on_terminate(&mut self, _cluster: &mut Cluster<'_>) -> Result<(), ContainerError> {
        Ok(())
    }
}

fn read_snapshot(
    cluster: &mut Cluster<'_>,
    reader: &mut SnapshotReader<'_>,
) -> Result<Vec<u8>, ContainerError> {
    let mut bytes = Vec::new();
    while !reader.is_end_of_stream() {
        let fragments = reader.poll(16, |fragment| {
            if let Some((buffer, offset, length)) = fragment.payload_region() {
                for index in 0..length {
                    if let Some(byte) = buffer.load_u8(offset + index) {
                        bytes.push(byte);
                    }
                }
            }
        });
        if fragments == 0 {
            cluster.idle(0)?;
        }
    }
    Ok(bytes)
}

fn offer_to_session(
    cluster: &mut Cluster<'_>,
    session: SessionId,
    payload: &[u8],
) -> Result<(), ContainerError> {
    for _ in 0..3 {
        let offered = {
            let Some(mut view) = cluster.session(session) else {
                return Ok(());
            };
            view.offer(payload)?
        };
        if offered > 0 {
            return Ok(());
        }
        cluster.idle(1)?;
    }
    Ok(())
}

fn container(error: impl fmt::Display) -> ContainerError {
    ContainerError::Cluster(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use deepbook_engine_core::ClientOrderId;
    use deepbook_engine_core::wire::{encode_command, encode_event};
    use deepbook_engine_core::{Command, NewOrder, OrdType, RiskConfig, TimeInForce};
    use deepbook_types::{AccountId, Price, Qty, Side};

    fn config() -> EngineConfig {
        EngineConfig {
            symbol: 1,
            max_orders: 64,
            recent_order_window: 64,
            risk: RiskConfig::disabled(),
        }
    }

    fn cl(value: &str) -> ClientOrderId {
        value.parse().expect("client order id")
    }

    fn limit(account: AccountId, id: &str, side: Side, price: Price, qty: Qty) -> Command {
        Command::NewOrder(NewOrder {
            symbol: 1,
            account,
            cl_ord_id: cl(id),
            side,
            price: Some(price),
            qty,
            ord_type: OrdType::Limit,
            time_in_force: TimeInForce::GoodTillCanceled,
            post_only: false,
        })
    }

    #[test]
    fn a_command_becomes_events() {
        let mut service = EngineService::new(config());
        let events = service
            .apply(&encode_command(&limit(1, "S1", Side::Sell, 100, 10)), 1)
            .expect("apply");
        assert!(!events.is_empty());
        let bytes = encode_event(&events[0]);
        assert!(deepbook_engine_core::decode_event(&bytes).is_ok());
    }

    #[test]
    fn a_snapshot_restores_the_service_and_continues_identically() {
        let mut service = EngineService::new(config());
        let commands = [
            limit(1, "S1", Side::Sell, 100, 10),
            limit(2, "B1", Side::Buy, 100, 4),
            limit(3, "S2", Side::Sell, 101, 20),
            Command::KillSwitch { account: Some(1) },
        ];
        for (index, command) in commands.iter().enumerate() {
            service
                .apply(&encode_command(command), index as LogPos + 1)
                .expect("apply");
        }

        let snapshot = service.snapshot_bytes();
        let mut restored = EngineService::from_snapshot(config(), &snapshot).expect("restore");
        assert_eq!(service.book_hash(), restored.book_hash());

        let continuation = [
            limit(3, "B2", Side::Buy, 100, 5),
            limit(4, "S3", Side::Sell, 99, 7),
        ];
        for (index, command) in continuation.iter().enumerate() {
            let log = index as LogPos + 1;
            let first = service.apply(&encode_command(command), log).expect("first");
            let second = restored
                .apply(&encode_command(command), log)
                .expect("second");
            assert_eq!(first, second);
        }
        assert_eq!(service.book_hash(), restored.book_hash());
    }
}
