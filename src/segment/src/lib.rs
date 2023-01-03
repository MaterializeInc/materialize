// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// BEGIN LINT CONFIG
// DO NOT EDIT. Automatically generated by bin/gen-lints.
// Have complaints about the noise? See the note in misc/python/materialize/cli/gen-lints.py first.
#![allow(
    clippy::style,
    clippy::complexity,
    clippy::large_enum_variant,
    clippy::mutable_key_type,
    clippy::stable_sort_primitive,
    clippy::map_entry,
    clippy::box_default
)]
#![warn(
    clippy::bool_comparison,
    clippy::clone_on_ref_ptr,
    clippy::no_effect,
    clippy::unnecessary_unwrap,
    clippy::dbg_macro,
    clippy::todo,
    clippy::wildcard_dependencies,
    clippy::zero_prefixed_literal,
    clippy::borrowed_box,
    clippy::deref_addrof,
    clippy::double_must_use,
    clippy::double_parens,
    clippy::extra_unused_lifetimes,
    clippy::needless_borrow,
    clippy::needless_question_mark,
    clippy::needless_return,
    clippy::redundant_pattern,
    clippy::redundant_slicing,
    clippy::redundant_static_lifetimes,
    clippy::single_component_path_imports,
    clippy::unnecessary_cast,
    clippy::useless_asref,
    clippy::useless_conversion,
    clippy::builtin_type_shadow,
    clippy::duplicate_underscore_argument,
    clippy::double_neg,
    clippy::unnecessary_mut_passed,
    clippy::wildcard_in_or_patterns,
    clippy::collapsible_if,
    clippy::collapsible_else_if,
    clippy::crosspointer_transmute,
    clippy::excessive_precision,
    clippy::overflow_check_conditional,
    clippy::as_conversions,
    clippy::match_overlapping_arm,
    clippy::zero_divided_by_zero,
    clippy::must_use_unit,
    clippy::suspicious_assignment_formatting,
    clippy::suspicious_else_formatting,
    clippy::suspicious_unary_op_formatting,
    clippy::mut_mutex_lock,
    clippy::print_literal,
    clippy::same_item_push,
    clippy::useless_format,
    clippy::write_literal,
    clippy::redundant_closure,
    clippy::redundant_closure_call,
    clippy::unnecessary_lazy_evaluations,
    clippy::partialeq_ne_impl,
    clippy::redundant_field_names,
    clippy::transmutes_expressible_as_ptr_casts,
    clippy::unused_async,
    clippy::disallowed_methods,
    clippy::disallowed_macros,
    clippy::from_over_into
)]
// END LINT CONFIG

//! Segment library for Rust.
//!
//! This crate provides a library to the [Segment] analytics platform.
//! It is a small wrapper around the [`segment`] crate to provide a more
//! ergonomic interface.
//!
//! [Segment]: https://segment.com
//! [`segment`]: https://docs.rs/segment

use segment::message::{Batch, BatchMessage, Group, Message, Track, User};
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
#[derive(Clone)]
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
    pub fn track<S>(
        &self,
        user_id: Uuid,
        event: S,
        properties: serde_json::Value,
        context: Option<serde_json::Value>,
    ) where
        S: Into<String>,
    {
        self.send(BatchMessage::Track(Track {
            user: User::UserId {
                user_id: user_id.to_string(),
            },
            event: event.into(),
            properties,
            context,
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

    fn send(&self, message: BatchMessage) {
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
