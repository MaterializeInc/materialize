// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Utility functions for Oneshot sources.

/// Utility trait for converting various Rust Range types into a header value.
/// according to the MDN Web Docs.
///
/// See: <https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Range>
pub trait IntoRangeHeaderValue {
    fn into_range_header_value(&self) -> String;
}

impl IntoRangeHeaderValue for std::ops::Range<usize> {
    fn into_range_header_value(&self) -> String {
        let inclusive_end = std::cmp::max(self.start, self.end.saturating_sub(1));
        (self.start..=inclusive_end).into_range_header_value()
    }
}

impl IntoRangeHeaderValue for std::ops::RangeInclusive<usize> {
    fn into_range_header_value(&self) -> String {
        // See <https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Range>.
        format!("bytes={}-{}", self.start(), self.end())
    }
}
