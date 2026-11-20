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

//! Errors raised by the protocol core itself.

/// The server sent a stream the subscribe protocol does not allow, such as a
/// row of the wrong width.
///
/// The core refuses such a stream instead of guessing at it, because
/// continuing could commit a frontier past data that never arrived.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("subscribe protocol violation: {0}")]
pub struct ProtocolError(String);

impl ProtocolError {
    pub(crate) fn new(message: impl Into<String>) -> ProtocolError {
        ProtocolError(message.into())
    }
}

/// One timestamp held more update bytes than the limit set with
/// [`crate::Batcher::with_max_timestamp_bytes`]. The timestamp is the same on
/// every retry, so retrying cannot help.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error(
    "the updates at timestamp {timestamp} exceed the limit of {limit} bytes. Raise the limit, \
     or subscribe to a smaller materialized view"
)]
pub struct TimestampTooLarge {
    /// The timestamp.
    pub timestamp: u64,
    /// The limit.
    pub limit: usize,
}

/// The caller asked for something the core does not support, such as resuming
/// a query.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct UsageError(String);

impl UsageError {
    pub(crate) fn new(message: impl Into<String>) -> UsageError {
        UsageError(message.into())
    }
}
