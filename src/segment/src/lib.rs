//! Segment library for Rust.
//!
//! This crate provides a library to the [Segment] analytics platform.
//! It is a small wrapper around the [`segment`] crate to provide a more
//! ergonomic interface.
//!
//! [Segment]: https://segment.com
//! [`segment`]: https://docs.rs/segment

use segment::message::{Batch, BatchMessage, Message, Track, User};
use segment::{Batcher, Client as _, HttpClient};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tracing::warn;
use uuid::Uuid;

/// The maximum number of undelivered events. Once this limit is reached,
/// new events will be dropped.
const MAX_PENDING_EVENTS: usize = 32_768;

/// A [Segment] API client.
///
/// Event delivery is best effort. There is no guarantee that a given event
/// will be delivered to Segment.
///
/// [Segment]: https://segment.com
pub struct Client {
    tx: Sender<BatchMessage>,
}

impl Client {
    /// Creates a new client.
    pub fn new(api_key: String) -> Client {
        let (tx, rx) = mpsc::channel(MAX_PENDING_EVENTS);

        let send_task = SendTask {
            api_key,
            http_client: HttpClient::default(),
        };
        mz_ore::task::spawn(
            || "segment_send_task",
            async move { send_task.run(rx).await },
        );

        Client { tx }
    }

    /// Sends a new [track event] to Segment.
    ///
    /// Delivery happens asynchronously on a background thread. It is best
    /// effort. There is no guarantee that the event will be delivered to
    /// Segment. Events may be dropped when the client is backlogged. Errors are
    /// logged but not returned.
    ///
    /// [track event]: https://segment.com/docs/connections/spec/track/
    pub fn track<S>(&self, user_id: Uuid, event: S, properties: serde_json::Value)
    where
        S: Into<String>,
    {
        let message = BatchMessage::Track(Track {
            user: User::UserId {
                user_id: user_id.to_string(),
            },
            event: event.into(),
            properties,
            ..Default::default()
        });
        match self.tx.try_send(message) {
            Ok(()) => (),
            Err(TrySendError::Closed(_)) => panic!("receiver must not drop first"),
            Err(TrySendError::Full(_)) => {
                warn!("dropping segment event because queue is full");
            }
        }
    }
}

struct SendTask {
    api_key: String,
    http_client: HttpClient,
}

impl SendTask {
    async fn run(&self, mut rx: Receiver<BatchMessage>) {
        // On each turn of the loop, we accumulate all outstanding messages and
        // send them to Segment in the largest batches possible. We never have
        // more than one outstanding request to Segment.
        loop {
            let mut batcher = Batcher::new(None);

            // Wait for the first event to arrive.
            match rx.recv().await {
                Some(message) => batcher = self.enqueue(batcher, message).await,
                None => return,
            };

            // Accumulate any other messages that are ready. `enqueue` may
            // flush the batch to Segment if we hit the maximum batch size.
            while let Ok(message) = rx.try_recv() {
                batcher = self.enqueue(batcher, message).await;
            }

            // Drain the queue.
            self.flush(batcher).await;
        }
    }

    async fn enqueue(&self, mut batcher: Batcher, message: BatchMessage) -> Batcher {
        match batcher.push(message) {
            Ok(None) => (),
            Ok(Some(message)) => {
                self.flush(batcher).await;
                batcher = Batcher::new(None);
                batcher
                    .push(message)
                    .expect("message cannot fail to enqueue twice");
            }
            Err(e) => {
                warn!("error enqueueing segment message: {}", e);
            }
        }
        batcher
    }

    async fn flush(&self, batcher: Batcher) {
        let message = batcher.into_message();
        if matches!(&message, Message::Batch(Batch { batch , .. }) if batch.is_empty()) {
            return;
        }
        if let Err(e) = self.http_client.send(self.api_key.clone(), message).await {
            warn!("error sending message to segment: {}", e);
        }
    }
}
