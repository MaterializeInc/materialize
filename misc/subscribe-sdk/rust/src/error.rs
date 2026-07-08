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

//! The error taxonomy consumers match on to decide how to recover.

/// An error surfaced while subscribing.
///
/// Variants are grouped by the recovery a caller should attempt.
/// [`SubscribeError::is_retryable`] captures the common "reconnect and resume"
/// versus "give up" split so callers rarely need to match every variant.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SubscribeError {
    /// The requested resume point is older than the source's retained history,
    /// so the gap cannot be filled. Recover by re-snapshotting or by accepting
    /// a gap, per the caller's policy. Never retryable as-is.
    ///
    /// Widen `RETAIN HISTORY` on the subscribed object to make a larger resume
    /// window available.
    #[error(
        "resume point is older than the subscribed object's retained history \
         (compaction horizon); re-snapshot or widen RETAIN HISTORY"
    )]
    CompactionHorizon,

    /// A dependency of the subscribed object (the object itself, or its
    /// cluster) was dropped, so the stream ended. Not retryable against the
    /// same object.
    #[error("a dependency of the subscribed object was dropped: {0}")]
    DependencyDropped(String),

    /// A resumed subscription's query fingerprint does not match the one the
    /// checkpoint was taken against. Resuming would silently mix incompatible
    /// results, so it is refused.
    #[error("resumed query does not match the checkpoint (expected {expected}, got {actual})")]
    SchemaMismatch {
        /// Fingerprint recorded in the resume token.
        expected: String,
        /// Fingerprint of the query being resumed.
        actual: String,
    },

    /// A transient connection or protocol interruption. Retryable: reconnect
    /// and resume from the last token.
    #[error("transient connection error: {0}")]
    Transient(String),

    /// The consumer fell too far behind and the client-side buffer filled. The
    /// client drains the server continuously so the server never buffers
    /// unboundedly, which means a slow consumer must fail here rather than push
    /// that cost onto Materialize. Recover by consuming faster, widening the
    /// buffer, or resuming from the last token once caught up.
    #[error(
        "client-side buffer overflowed: the consumer fell behind the subscription. \
         Consume faster or widen the buffer, then resume from the last token"
    )]
    BufferOverflow,

    /// A resume token could not be decoded.
    #[error("invalid resume token: {0}")]
    InvalidToken(String),

    /// The server sent a stream that violates the subscribe protocol (e.g. a
    /// non-monotonic timestamp). Indicates a bug, not a recoverable condition.
    #[error("subscribe protocol violation: {0}")]
    Protocol(String),

    /// A non-retryable server error: an authentication failure, a malformed
    /// query, or a dataflow error that poisons the stream (Materialize repeats
    /// such an error until the data or view changes, so retrying will not help).
    #[error("fatal error: {0}")]
    Fatal(String),
}

impl SubscribeError {
    /// Whether reconnecting and resuming from the last token is a sensible
    /// automatic response. Only [`SubscribeError::Transient`] qualifies; every
    /// other variant needs an explicit policy decision or is a bug.
    pub fn is_retryable(&self) -> bool {
        matches!(self, SubscribeError::Transient(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_transient_is_retryable() {
        assert!(SubscribeError::Transient("reset".into()).is_retryable());
        assert!(!SubscribeError::CompactionHorizon.is_retryable());
        assert!(!SubscribeError::DependencyDropped("v".into()).is_retryable());
        assert!(!SubscribeError::SchemaMismatch {
            expected: "a".into(),
            actual: "b".into(),
        }
        .is_retryable());
        assert!(!SubscribeError::Fatal("auth".into()).is_retryable());
        assert!(!SubscribeError::BufferOverflow.is_retryable());
    }

    #[test]
    fn display_is_actionable() {
        // The compaction-horizon message must point the user at the fix, since
        // it is the error operators hit most and least expect.
        let msg = SubscribeError::CompactionHorizon.to_string();
        assert!(msg.contains("RETAIN HISTORY"), "{msg}");
    }
}
