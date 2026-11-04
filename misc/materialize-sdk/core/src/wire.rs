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

//! The JSON form shared by checkpoints, conformance vectors, and bindings.
//!
//! Every `u64` is a decimal string. A JavaScript number holds integers exactly
//! only up to 2^53, so a numeric `mz_timestamp` or epoch would round silently
//! in the Node package and in any JSON tool built on JavaScript.

/// Whether `s` is a number's one canonical spelling: ASCII digits with no
/// leading zero, so that every language reads and writes the same strings.
fn is_canonical(s: &str) -> bool {
    match s.as_bytes() {
        [] => false,
        [b'0'] => true,
        [b'0', ..] => false,
        digits => digits.iter().all(u8::is_ascii_digit),
    }
}

/// Parses a `u128` in its canonical form.
pub(crate) fn parse_u128(s: &str) -> Option<u128> {
    is_canonical(s).then(|| s.parse().ok()).flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_u128_accepts_only_digits() {
        assert_eq!(parse_u128("18446744073709551616"), Some(1 << 64));
        assert_eq!(parse_u128(""), None);
        assert_eq!(parse_u128("+1"), None);
        assert_eq!(parse_u128("1e3"), None);
        assert_eq!(parse_u128("007"), None);
    }
}
