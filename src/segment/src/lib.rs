// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Segment library for Rust.
//!
//! This crate provides a library to the [Segment] analytics platform.
//! It is a small wrapper around the [`segment`] crate to provide a more
//! ergonomic interface.
//!
//! [Segment]: https://segment.com
//! [`segment`]: https://docs.rs/segment

use std::fmt;

use chrono::{DateTime, Utc};
use segment::message::{Batch, BatchMessage, Group, Message, Track, User};
use segment::{Batcher, Client as _, HttpClient};
use time::OffsetDateTime;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tracing::{error, warn};
use uuid::Uuid;

/// The maximum number of undelivered events. Once this limit is reached,
/// new events will be dropped.
const MAX_PENDING_EVENTS: usize = 32_768;

pub struct Config {
    /// The API key to use to authenticate events with Segment.
    pub api_key: String,
    /// Whether this Segment client is being used on the client side (rather
    /// than the server side).
    ///
    /// Enabling this causes the Segment server to record the IP address from
    /// which the event was sent.
    pub client_side: bool,
}

/// A [Segment] API client.
///
/// Event delivery is best effort. There is no guarantee that a given event
/// will be delivered to Segment.
///
/// [Segment]: https://segment.com
#[derive(Clone)]
pub struct Client {
    client_side: bool,
    tx: Sender<BatchMessage>,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<segment client>")
    }
}

impl Client {
    /// Creates a new client.
    pub fn new(
        Config {
            api_key,
            client_side,
        }: Config,
    ) -> Client {
        let (tx, rx) = mpsc::channel(MAX_PENDING_EVENTS);

        let send_task = SendTask {
            api_key,
            http_client: HttpClient::default(),
        };
        mz_ore::task::spawn(
            || "segment_send_task",
            async move { send_task.run(rx).await },
        );

        Client { client_side, tx }
    }

    /// Sends a new [track event] to Segment.
    ///
    /// Delivery happens asynchronously on a background thread. It is best
    /// effort. There is no guarantee that the event will be delivered to
    /// Segment. Events may be dropped when the client is backlogged. Errors are
    /// logged but not returned.
    ///
    /// [track event]: https://segment.com/docs/connections/spec/track/
    pub fn track<S>(
        &self,
        user_id: Uuid,
        event: S,
        properties: serde_json::Value,
        context: Option<serde_json::Value>,
        timestamp: Option<DateTime<Utc>>,
    ) where
        S: Into<String>,
    {
        let timestamp = match timestamp {
            None => None,
            Some(t) => match OffsetDateTime::from_unix_timestamp(t.timestamp())
                .and_then(|odt| odt.replace_nanosecond(t.timestamp_subsec_nanos()))
            {
                Ok(timestamp) => Some(timestamp),
                Err(e) => {
                    error!(%e, "failed to convert timestamp for Segment event");
                    return;
                }
            },
        };
        self.send(BatchMessage::Track(Track {
            user: User::UserId {
                user_id: user_id.to_string(),
            },
            event: event.into(),
            properties,
            context,
            timestamp,
            ..Default::default()
        }));
    }

    /// Sends a new [group event] to Segment.
    ///
    /// Delivery happens asynchronously on a background thread. It is best
    /// effort. There is no guarantee that the event will be delivered to
    /// Segment. Events may be dropped when the client is backlogged. Errors are
    /// logged but not returned.
    ///
    /// [track event]: https://segment.com/docs/connections/spec/group/
    pub fn group(&self, user_id: Uuid, group_id: Uuid, traits: serde_json::Value) {
        self.send(BatchMessage::Group(Group {
            user: User::UserId {
                user_id: user_id.to_string(),
            },
            group_id: group_id.to_string(),
            traits,
            ..Default::default()
        }));
    }

    fn send(&self, mut message: BatchMessage) {
        if self.client_side {
            // If running on the client side, pretend to be the Analytics.js
            // library by force-setting the appropriate library name in the
            // context. This is how Segment determines whether to attach an IP
            // to the incoming requests.
            let context = match &mut message {
                BatchMessage::Alias(a) => &mut a.context,
                BatchMessage::Group(i) => &mut i.context,
                BatchMessage::Identify(i) => &mut i.context,
                BatchMessage::Page(i) => &mut i.context,
                BatchMessage::Screen(i) => &mut i.context,
                BatchMessage::Track(t) => &mut t.context,
            };
            let context = context.get_or_insert_with(|| serde_json::json!({}));
            context
                .as_object_mut()
                .expect("Segment context must be object")
                .insert(
                    "library".into(),
                    serde_json::json!({
                            "name": "analytics.js",
                    }),
                );
        }

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
