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

use std::fmt;

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A `u64` in its JSON form, a decimal string.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct U64(u64);

impl Serialize for U64 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for U64 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct DecimalString;

        impl Visitor<'_> for DecimalString {
            type Value = U64;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("an unsigned 64-bit integer as a decimal string")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<U64, E> {
                parse_u64(v)
                    .map(U64)
                    .ok_or_else(|| E::custom(format!("invalid u64 {v:?}")))
            }
        }

        deserializer.deserialize_str(DecimalString)
    }
}

/// Serde adapter for a `u64` field, for use with `#[serde(with = "...")]`.
pub(crate) mod u64_string {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::U64;

    pub(crate) fn serialize<S: Serializer>(value: &u64, serializer: S) -> Result<S::Ok, S::Error> {
        U64(*value).serialize(serializer)
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
        U64::deserialize(deserializer).map(|v| v.0)
    }
}

/// Serde adapter for an `Option<u64>` field, for use with
/// `#[serde(default, with = "...")]`.
pub(crate) mod opt_u64_string {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::U64;

    pub(crate) fn serialize<S: Serializer>(
        value: &Option<u64>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        value.map(U64).serialize(serializer)
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<u64>, D::Error> {
        Ok(Option::<U64>::deserialize(deserializer)?.map(|v| v.0))
    }
}

/// Serde adapter for an `i64` field, for use with `#[serde(with = "...")]`.
pub(crate) mod i64_string {
    use std::fmt;

    use serde::de::{self, Visitor};
    use serde::{Deserializer, Serializer};

    pub(crate) fn serialize<S: Serializer>(value: &i64, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(value)
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<i64, D::Error> {
        struct DecimalString;

        impl Visitor<'_> for DecimalString {
            type Value = i64;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a signed 64-bit integer as a decimal string")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<i64, E> {
                super::parse_i64(v).ok_or_else(|| E::custom(format!("invalid i64 {v:?}")))
            }
        }

        deserializer.deserialize_str(DecimalString)
    }
}

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

/// Parses a `u64` in its canonical form.
///
/// `u64::from_str` also accepts a leading `+` and leading zeros, which
/// neither Materialize's text output nor this crate's JSON form produces.
pub(crate) fn parse_u64(s: &str) -> Option<u64> {
    is_canonical(s).then(|| s.parse().ok()).flatten()
}

/// Parses a `u128` in its canonical form.
pub(crate) fn parse_u128(s: &str) -> Option<u128> {
    is_canonical(s).then(|| s.parse().ok()).flatten()
}

/// Parses an `i64` in its canonical form, with a leading `-` when negative.
pub(crate) fn parse_i64(s: &str) -> Option<i64> {
    let canonical = match s.strip_prefix('-') {
        Some(magnitude) => is_canonical(magnitude) && magnitude != "0",
        None => is_canonical(s),
    };
    canonical.then(|| s.parse().ok()).flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_u64_accepts_only_digits() {
        assert_eq!(parse_u64("0"), Some(0));
        assert_eq!(parse_u64("18446744073709551615"), Some(u64::MAX));
        assert_eq!(parse_u64("18446744073709551616"), None);
        assert_eq!(parse_u64(""), None);
        assert_eq!(parse_u64("+1"), None);
        assert_eq!(parse_u64("-1"), None);
        assert_eq!(parse_u64(" 1"), None);
        assert_eq!(parse_u64("1.0"), None);
        assert_eq!(parse_u64("01"), None);
        assert_eq!(parse_u64("00"), None);
    }

    #[test]
    fn parse_i64_accepts_only_digits_and_a_minus() {
        assert_eq!(parse_i64("-9223372036854775808"), Some(i64::MIN));
        assert_eq!(parse_i64("9223372036854775807"), Some(i64::MAX));
        assert_eq!(parse_i64("9223372036854775808"), None);
        assert_eq!(parse_i64("-"), None);
        assert_eq!(parse_i64("+1"), None);
        assert_eq!(parse_i64("--1"), None);
        assert_eq!(parse_i64("-0"), None);
        assert_eq!(parse_i64("-01"), None);
        assert_eq!(parse_i64("0"), Some(0));
    }

    #[test]
    fn parse_u128_accepts_only_digits() {
        assert_eq!(parse_u128("18446744073709551616"), Some(1 << 64));
        assert_eq!(parse_u128(""), None);
        assert_eq!(parse_u128("+1"), None);
        assert_eq!(parse_u128("1e3"), None);
        assert_eq!(parse_u128("007"), None);
    }

    #[test]
    fn json_numbers_are_rejected() {
        assert!(serde_json::from_str::<U64>("1").is_err());
        assert_eq!(serde_json::from_str::<U64>("\"1\"").unwrap(), U64(1));
    }
}
