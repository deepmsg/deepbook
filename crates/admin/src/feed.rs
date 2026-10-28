//! The market-data subscription behind the dashboard.
//!
//! The deepmsg client is not `Send`, so the feed lives on the admin's main
//! thread. Every turn it polls the transport, hands each message to the view
//! (trades) and then rebuilds the levels from the subscriber's mirrored book.

use std::sync::{Arc, Mutex};

use deepbook_marketdata::{DeepmsgConfig, DeepmsgError, DeepmsgSubscriber, Subscriber};
use deepbook_types::SymbolId;

use crate::state::SharedView;

/// The live subscription and its view.
pub struct Feed {
    transport: DeepmsgSubscriber,
    subscriber: Subscriber,
    view: SharedView,
}

impl Feed {
    /// Subscribe to the incremental and recovery channels in `config`.
    ///
    /// # Errors
    ///
    /// The driver or a subscription is unavailable.
    pub fn connect(
        config: &DeepmsgConfig,
        symbol: SymbolId,
        view: SharedView,
    ) -> Result<Self, DeepmsgError> {
        let transport = DeepmsgSubscriber::connect(config)?;
        Ok(Self {
            transport,
            subscriber: Subscriber::new(symbol),
            view,
        })
    }

    /// One poll: trades into the tape, then the whole level set into the view.
    ///
    /// # Errors
    ///
    /// The transport failed to offer a recovery request.
    pub fn poll(&mut self) -> Result<usize, DeepmsgError> {
        let view = Arc::clone(&self.view);
        let work = self
            .transport
            .poll_with(&mut self.subscriber, move |message| {
                if let Ok(mut view) = view.lock() {
                    view.observe(message);
                }
            })?;
        if let Ok(mut view) = self.view.lock() {
            view.apply_market(&self.subscriber);
        }
        Ok(work)
    }

    /// Whether the subscriber has a base and no outstanding hole.
    #[must_use]
    pub fn is_synced(&self) -> bool {
        self.subscriber.is_synced()
    }

    /// The view, for callers that do not hold another handle.
    #[must_use]
    pub fn view(&self) -> SharedView {
        Arc::clone(&self.view)
    }
}

/// A view handle that a caller can use without owning the feed.
#[must_use]
pub fn view_handle(view: &SharedView) -> Arc<Mutex<crate::state::View>> {
    Arc::clone(view)
}
