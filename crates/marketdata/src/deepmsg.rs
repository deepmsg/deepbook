//! The deepmsg transport for market data.
//!
//! One media driver, one IPC/UDP publication for incrementals, and a
//! request/response pair for recovery. The codec is [`crate::wire`]; the
//! publisher and subscriber are thin wrappers over `deepmsg-client`.
//!
//! The publisher and subscriber can share a driver in a test (or be on
//! different hosts with a UDP channel in a deployment). The recovery request
//! is retried until the subscriber is current, because the same send-side loss
//! generator that creates a book gap can also drop a recovery datagram.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use deepmsg_client::client::Client;
use deepmsg_core::logbuffer::append::Appended;

use crate::message::{Incremental, RecoveryRequest, RecoveryResponse, Snapshot};
use crate::service::MarketDataService;
use crate::subscriber::Subscriber;
use crate::wire::{WireMessage, decode, encode};

/// Transport configuration.
#[derive(Clone, Debug)]
pub struct DeepmsgConfig {
    /// The driver's aeron directory.
    pub aeron_dir: PathBuf,
    /// Publication channel for incrementals.
    pub incremental_channel: String,
    /// Subscription channel for incrementals (may add `reliable=false`).
    pub incremental_subscription_channel: String,
    /// Incremental stream id.
    pub incremental_stream_id: i32,
    /// Subscriber → service recovery request publication.
    pub recovery_request_channel: String,
    /// Service-side recovery request subscription.
    pub recovery_request_subscription_channel: String,
    /// Service → subscriber recovery response publication.
    pub recovery_response_channel: String,
    /// Subscriber-side recovery response subscription (may add
    /// `reliable=false`).
    pub recovery_response_subscription_channel: String,
    /// Recovery stream id.
    pub recovery_stream_id: i32,
    /// Subscription fragment limit per poll.
    pub fragment_limit: usize,
    /// How long one offer may retry before it fails.
    pub offer_timeout: Duration,
    /// How often an unanswered recovery request is retried.
    pub recovery_retry: Duration,
}

impl DeepmsgConfig {
    /// A configuration with a 5 s offer timeout and a 100 ms recovery retry.
    #[allow(clippy::too_many_arguments)] // one per channel endpoint
    #[must_use]
    pub fn new(
        aeron_dir: impl Into<PathBuf>,
        incremental_channel: impl Into<String>,
        incremental_subscription_channel: impl Into<String>,
        recovery_request_channel: impl Into<String>,
        recovery_request_subscription_channel: impl Into<String>,
        recovery_response_channel: impl Into<String>,
        recovery_response_subscription_channel: impl Into<String>,
    ) -> Self {
        Self {
            aeron_dir: aeron_dir.into(),
            incremental_channel: incremental_channel.into(),
            incremental_subscription_channel: incremental_subscription_channel.into(),
            incremental_stream_id: 2001,
            recovery_request_channel: recovery_request_channel.into(),
            recovery_request_subscription_channel: recovery_request_subscription_channel.into(),
            recovery_response_channel: recovery_response_channel.into(),
            recovery_response_subscription_channel: recovery_response_subscription_channel.into(),
            recovery_stream_id: 2002,
            fragment_limit: 64,
            offer_timeout: Duration::from_secs(5),
            recovery_retry: Duration::from_millis(100),
        }
    }
}

/// A transport failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeepmsgError {
    /// The client could not connect to a driver.
    Connect(String),
    /// A publication or subscription command failed.
    Command(String),
    /// An offer did not complete before its deadline.
    Offer(String),
}

impl std::fmt::Display for DeepmsgError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connect(detail) => write!(formatter, "connect: {detail}"),
            Self::Command(detail) => write!(formatter, "command: {detail}"),
            Self::Offer(detail) => write!(formatter, "offer: {detail}"),
        }
    }
}

impl std::error::Error for DeepmsgError {}

/// The publishing side: incrementals out, recovery requests in.
pub struct DeepmsgPublisher {
    client: Client,
    incremental_publication: i64,
    recovery_request_subscription: i64,
    recovery_response_publication: i64,
    offer_timeout: Duration,
    fragment_limit: usize,
    requests_served: usize,
}

impl DeepmsgPublisher {
    /// Connect to the driver and add the three endpoints.
    ///
    /// # Errors
    ///
    /// The client or a command failed.
    pub fn connect(config: &DeepmsgConfig) -> Result<Self, DeepmsgError> {
        let mut client = Client::connect(&config.aeron_dir)
            .map_err(|error| DeepmsgError::Connect(error.to_string()))?;
        let incremental_publication = client
            .add_publication(
                &config.incremental_channel,
                config.incremental_stream_id,
                config.offer_timeout,
            )
            .map_err(|error| DeepmsgError::Command(error.to_string()))?;
        let recovery_request_subscription = client
            .add_subscription(
                &config.recovery_request_subscription_channel,
                config.recovery_stream_id,
                config.offer_timeout,
            )
            .map_err(|error| DeepmsgError::Command(error.to_string()))?;
        let recovery_response_publication = client
            .add_publication(
                &config.recovery_response_channel,
                config.recovery_stream_id,
                config.offer_timeout,
            )
            .map_err(|error| DeepmsgError::Command(error.to_string()))?;
        Ok(Self {
            client,
            incremental_publication,
            recovery_request_subscription,
            recovery_response_publication,
            offer_timeout: config.offer_timeout,
            fragment_limit: config.fragment_limit,
            requests_served: 0,
        })
    }

    /// Publish one incremental.
    ///
    /// # Errors
    ///
    /// The offer did not complete.
    pub fn publish_incremental(&mut self, incremental: Incremental) -> Result<(), DeepmsgError> {
        let bytes = encode(&WireMessage::Incremental(incremental));
        self.publish_frame(&bytes)
    }

    /// Publish a broadcast snapshot.
    ///
    /// # Errors
    ///
    /// The offer did not complete.
    pub fn publish_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), DeepmsgError> {
        let bytes = encode(&WireMessage::Snapshot(snapshot.clone()));
        self.publish_frame(&bytes)
    }

    /// Publish raw wire bytes on the incremental publication. The loss test
    /// pads a frame to one datagram with this.
    ///
    /// # Errors
    ///
    /// The offer did not complete.
    pub fn publish_frame(&mut self, frame: &[u8]) -> Result<(), DeepmsgError> {
        offer(
            &mut self.client,
            self.incremental_publication,
            frame,
            self.offer_timeout,
        )
    }

    /// Answer every pending recovery request.
    ///
    /// # Errors
    ///
    /// An offer did not complete.
    pub fn poll_recovery(&mut self, service: &MarketDataService) -> Result<usize, DeepmsgError> {
        let _ = self.client.poll();
        let mut requests: Vec<RecoveryRequest> = Vec::new();
        self.client.poll_subscription(
            self.recovery_request_subscription,
            self.fragment_limit,
            |message| {
                if let Ok(WireMessage::RecoveryRequest(request)) = decode(message.payload) {
                    requests.push(request);
                }
            },
        );
        let count = requests.len();
        for request in requests {
            let response = service.recover(&request);
            let bytes = encode(&WireMessage::RecoveryResponse(response));
            offer(
                &mut self.client,
                self.recovery_response_publication,
                &bytes,
                self.offer_timeout,
            )?;
        }
        self.requests_served += count;
        let _ = self.client.poll();
        Ok(count)
    }

    /// How many recovery requests this publisher has answered.
    #[must_use]
    pub const fn requests_served(&self) -> usize {
        self.requests_served
    }
}

/// The consuming side: incrementals in, recovery requests out.
pub struct DeepmsgSubscriber {
    client: Client,
    incremental_subscription: i64,
    recovery_request_publication: i64,
    recovery_response_subscription: i64,
    offer_timeout: Duration,
    fragment_limit: usize,
    recovery_retry: Duration,
    last_request: Option<Instant>,
    received: usize,
    requests: usize,
}

impl DeepmsgSubscriber {
    /// Connect to the driver and add the three endpoints.
    ///
    /// # Errors
    ///
    /// The client or a command failed.
    pub fn connect(config: &DeepmsgConfig) -> Result<Self, DeepmsgError> {
        let mut client = Client::connect(&config.aeron_dir)
            .map_err(|error| DeepmsgError::Connect(error.to_string()))?;
        let incremental_subscription = client
            .add_subscription(
                &config.incremental_subscription_channel,
                config.incremental_stream_id,
                config.offer_timeout,
            )
            .map_err(|error| DeepmsgError::Command(error.to_string()))?;
        let recovery_request_publication = client
            .add_publication(
                &config.recovery_request_channel,
                config.recovery_stream_id,
                config.offer_timeout,
            )
            .map_err(|error| DeepmsgError::Command(error.to_string()))?;
        let recovery_response_subscription = client
            .add_subscription(
                &config.recovery_response_subscription_channel,
                config.recovery_stream_id,
                config.offer_timeout,
            )
            .map_err(|error| DeepmsgError::Command(error.to_string()))?;
        Ok(Self {
            client,
            incremental_subscription,
            recovery_request_publication,
            recovery_response_subscription,
            offer_timeout: config.offer_timeout,
            fragment_limit: config.fragment_limit,
            recovery_retry: config.recovery_retry,
            last_request: None,
            received: 0,
            requests: 0,
        })
    }

    /// Poll incrementals and recovery responses, and request recovery when the
    /// subscriber has a hole. The caller drives this in a loop.
    ///
    /// # Errors
    ///
    /// A recovery request could not be offered.
    pub fn poll(&mut self, subscriber: &mut Subscriber) -> Result<usize, DeepmsgError> {
        let _ = self.client.poll();
        let mut work = 0;

        let mut messages: Vec<WireMessage> = Vec::new();
        self.client.poll_subscription(
            self.incremental_subscription,
            self.fragment_limit,
            |message| {
                if let Ok(wire) = decode(message.payload) {
                    messages.push(wire);
                }
            },
        );
        for message in messages {
            match message {
                WireMessage::Incremental(incremental) => {
                    self.received += 1;
                    let _ = subscriber.on_incremental(&incremental);
                    work += 1;
                }
                WireMessage::Snapshot(snapshot) => {
                    let _ = subscriber.on_snapshot(&snapshot);
                    work += 1;
                }
                _ => {}
            }
        }

        let mut responses: Vec<RecoveryResponse> = Vec::new();
        self.client.poll_subscription(
            self.recovery_response_subscription,
            self.fragment_limit,
            |message| {
                if let Ok(WireMessage::RecoveryResponse(response)) = decode(message.payload) {
                    responses.push(response);
                }
            },
        );
        for response in responses {
            let _ = subscriber.on_recovery(&response);
            work += 1;
        }

        if let Some(request) = subscriber.recovery_request() {
            let due = self
                .last_request
                .is_none_or(|at| at.elapsed() >= self.recovery_retry);
            if due {
                self.send_request(request)?;
                work += 1;
            }
        }
        let _ = self.client.poll();
        Ok(work)
    }

    /// Ask for recovery even when the subscriber is not flagged (used after a
    /// tail was lost and no later frame arrived).
    ///
    /// # Errors
    ///
    /// The request could not be offered.
    pub fn request_recovery(&mut self, subscriber: &Subscriber) -> Result<(), DeepmsgError> {
        let request = RecoveryRequest {
            symbol: subscriber.symbol(),
            last_md_seq: subscriber.last_md_seq().unwrap_or(0),
        };
        self.send_request(request)
    }

    fn send_request(&mut self, request: RecoveryRequest) -> Result<(), DeepmsgError> {
        let bytes = encode(&WireMessage::RecoveryRequest(request));
        offer(
            &mut self.client,
            self.recovery_request_publication,
            &bytes,
            self.offer_timeout,
        )?;
        self.last_request = Some(Instant::now());
        self.requests += 1;
        Ok(())
    }

    /// How many incremental frames were decoded.
    #[must_use]
    pub const fn received_incrementals(&self) -> usize {
        self.received
    }

    /// How many recovery requests were sent.
    #[must_use]
    pub const fn recovery_requests(&self) -> usize {
        self.requests
    }
}

fn offer(
    client: &mut Client,
    publication: i64,
    bytes: &[u8],
    timeout: Duration,
) -> Result<(), DeepmsgError> {
    let deadline = Instant::now() + timeout;
    loop {
        match client.offer(publication, bytes) {
            Some(Appended::Ok { .. }) => return Ok(()),
            Some(
                Appended::EndOfLog
                | Appended::MidRotation
                | Appended::BackPressured
                | Appended::NotConnected,
            ) => {}
            Some(other) => return Err(DeepmsgError::Offer(format!("{other:?}"))),
            None => return Err(DeepmsgError::Offer("no such publication".to_owned())),
        }
        let _ = client.poll();
        if Instant::now() >= deadline {
            return Err(DeepmsgError::Offer(format!(
                "no offer completed in {timeout:?}"
            )));
        }
        std::thread::sleep(Duration::from_micros(100));
    }
}
