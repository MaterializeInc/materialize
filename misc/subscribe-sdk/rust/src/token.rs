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

//! The opaque checkpoint that makes resuming gap-free.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use serde::{Deserialize, Serialize};

use crate::error::SubscribeError;

/// Current on-the-wire token format. Bumped only if the encoded shape changes,
/// so a stored token from an older SDK is rejected with a clear error rather
/// than misinterpreted.
const TOKEN_FORMAT: u32 = 1;

/// An opaque, serializable checkpoint marking how far a subscription has been
/// durably consumed.
///
/// A token records a *closed* frontier: every update with a timestamp strictly
/// below [`ResumeToken::frontier`] has been delivered. Resuming re-subscribes
/// with `SNAPSHOT = false AS OF frontier - 1`, which the server interprets as
/// "emit updates with timestamp strictly greater than `frontier - 1`", i.e.
/// timestamp `>= frontier`. That is exactly the set not yet delivered, so the
/// resume is gap-free and overlap-free.
///
/// The `- 1` is the single most error-prone part of the subscribe protocol.
/// It lives here, in [`ResumeToken::as_of`], so callers never compute it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResumeToken {
    format: u32,
    frontier: u64,
    fingerprint: String,
}

impl ResumeToken {
    /// Builds a token for a closed `frontier` of the query identified by
    /// `fingerprint`. Crate-internal: tokens originate from the batcher, not
    /// from user code.
    pub(crate) fn new(frontier: u64, fingerprint: impl Into<String>) -> Self {
        ResumeToken {
            format: TOKEN_FORMAT,
            frontier,
            fingerprint: fingerprint.into(),
        }
    }

    /// The closed frontier: every update below this timestamp is delivered.
    pub fn frontier(&self) -> u64 {
        self.frontier
    }

    /// The fingerprint of the query this token was taken against. Compared on
    /// resume to catch a changed query (see [`SubscribeError::SchemaMismatch`]).
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    /// The `AS OF` value for a gap-free `SNAPSHOT = false` resume.
    ///
    /// Saturates at zero so a token for the very first frontier resumes from
    /// the beginning rather than underflowing.
    pub fn as_of(&self) -> u64 {
        self.frontier.saturating_sub(1)
    }

    /// Encodes the token to a compact, URL-safe string for durable storage.
    /// The encoding is opaque: callers persist and return the bytes unchanged.
    pub fn encode(&self) -> String {
        // `to_string` on this fixed struct cannot fail, so the expect is a true
        // invariant rather than error handling.
        let json = serde_json::to_vec(self).expect("ResumeToken always serializes");
        URL_SAFE_NO_PAD.encode(json)
    }

    /// Decodes a token previously produced by [`ResumeToken::encode`].
    pub fn decode(encoded: &str) -> Result<Self, SubscribeError> {
        let bytes = URL_SAFE_NO_PAD
            .decode(encoded.as_bytes())
            .map_err(|e| SubscribeError::InvalidToken(format!("not valid base64: {e}")))?;
        let token: ResumeToken = serde_json::from_slice(&bytes)
            .map_err(|e| SubscribeError::InvalidToken(format!("malformed token: {e}")))?;
        if token.format != TOKEN_FORMAT {
            return Err(SubscribeError::InvalidToken(format!(
                "unsupported token format {} (this SDK understands {})",
                token.format, TOKEN_FORMAT
            )));
        }
        Ok(token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn as_of_is_frontier_minus_one_and_saturates() {
        assert_eq!(ResumeToken::new(0, "q").as_of(), 0);
        assert_eq!(ResumeToken::new(1, "q").as_of(), 0);
        assert_eq!(ResumeToken::new(5, "q").as_of(), 4);
        assert_eq!(ResumeToken::new(u64::MAX, "q").as_of(), u64::MAX - 1);
    }

    #[test]
    fn encode_decode_round_trips() {
        let token = ResumeToken::new(42, "fingerprint-abc");
        let decoded = ResumeToken::decode(&token.encode()).expect("round trip");
        assert_eq!(decoded, token);
        assert_eq!(decoded.frontier(), 42);
        assert_eq!(decoded.fingerprint(), "fingerprint-abc");
    }

    #[test]
    fn encoding_is_stable_and_cross_language() {
        // This exact string is also asserted by the Python SDK's test suite, so
        // a token minted by either SDK decodes in the other. If this changes,
        // the two SDKs have diverged and tokens are no longer interchangeable.
        let encoded = ResumeToken::new(42, "x").encode();
        assert_eq!(
            encoded,
            "eyJmb3JtYXQiOjEsImZyb250aWVyIjo0MiwiZmluZ2VycHJpbnQiOiJ4In0"
        );
    }

    #[test]
    fn decode_rejects_garbage() {
        assert!(ResumeToken::decode("!!! not base64 !!!").is_err());
        assert!(ResumeToken::decode(&URL_SAFE_NO_PAD.encode(b"not json")).is_err());
    }

    #[test]
    fn decode_rejects_unknown_format() {
        // A token encoded with a future format must be rejected, not
        // silently misread.
        let future = serde_json::json!({
            "format": TOKEN_FORMAT + 1,
            "frontier": 1,
            "fingerprint": "q",
        });
        let encoded = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&future).unwrap());
        let err = ResumeToken::decode(&encoded).unwrap_err();
        assert!(matches!(err, SubscribeError::InvalidToken(_)), "{err:?}");
    }
}
