// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The errors of a subscription.

use std::time::Duration;

use materialize_sdk_core::{
    CheckpointError, Classification, CommitConflict, IdentityChange, ProtocolError,
    TimestampTooLarge, UsageError,
};

use crate::sink::SinkError;

/// Why a subscription failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Materialize returned an error, or the connection failed.
    #[error("{message}{}", sqlstate.as_ref().map(|s| format!(" (SQLSTATE {s})")).unwrap_or_default())]
    Server {
        /// The SQLSTATE, or `None` for a failure with no server error, such as
        /// a closed connection.
        sqlstate: Option<String>,
        /// The error message.
        message: String,
        /// The server's detail, if any.
        detail: Option<String>,
        /// The server's hint, if any.
        hint: Option<String>,
        /// How the core classified the error.
        classification: Classification,
    },
    /// The server broke the subscribe protocol.
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    /// One timestamp exceeded [`crate::LiveOptions::max_timestamp_bytes`].
    #[error(transparent)]
    TimestampTooLarge(#[from] TimestampTooLarge),
    /// The subscription was set up wrongly, such as resuming a query.
    #[error(transparent)]
    Usage(#[from] UsageError),
    /// An option is out of range.
    #[error("invalid option: {0}")]
    InvalidOption(String),
    /// The consumer left the buffer of `capacity` events full for `waited`.
    #[error("the consumer fell behind: the buffer of {capacity} events stayed full for {waited:?}")]
    ConsumerFellBehind {
        /// The buffer's capacity, in events.
        capacity: usize,
        /// How long the fetch loop waited for room.
        waited: Duration,
    },
    /// The task that fetches from the cursor ended without reporting why.
    #[error("the subscription task ended unexpectedly")]
    TaskEnded,
    /// A checkpoint write lost to another write, such as a newer worker's
    /// take-over, which fences this one, or had a wrong range.
    #[error(transparent)]
    Conflict(#[from] CommitConflict),
    /// A checkpoint could not be read or built.
    #[error(transparent)]
    Checkpoint(#[from] CheckpointError),
    /// The sink failed, on every attempt if the failure is not permanent.
    #[error("the sink failed: {0}")]
    Sink(SinkError),
    /// The sink has no checkpoint, because it was never set up.
    #[error("sink {sink_id:?} has no checkpoint, so run its setup first")]
    NotSetUp {
        /// The sink id.
        sink_id: String,
    },
    /// The subscribed object changed in a way that needs an operator.
    #[error("the subscribed object changed: {change:?}")]
    ObjectChanged {
        /// How it changed.
        change: IdentityChange,
    },
}
