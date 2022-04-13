// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![cfg(feature = "test-utils")]

//! Custom [`proptest::strategy::Strategy`] implementations for the
//! [`chrono`] fields used inthe codebase.
//!
//! See the [`proptest`] docs[^1] for an example.
//!
//! [^1]: <https://altsysrq.github.io/proptest-book/proptest-derive/modifiers.html#strategy>

use chrono::{DateTime, Duration, FixedOffset, NaiveDateTime, Timelike, Utc};
use chrono_tz::{Tz, TZ_VARIANTS};
use proptest::prelude::Strategy;

pub fn any_naive_datetime() -> impl Strategy<Value = NaiveDateTime> {
    use ::chrono::naive::{MAX_DATETIME, MIN_DATETIME};
    (0..(MAX_DATETIME.nanosecond() - MIN_DATETIME.nanosecond()))
        .prop_map(|x| MIN_DATETIME + Duration::nanoseconds(x as i64))
}

pub fn any_datetime() -> impl Strategy<Value = DateTime<Utc>> {
    any_naive_datetime().prop_map(|x| DateTime::from_utc(x, Utc))
}

pub fn any_fixed_offset() -> impl Strategy<Value = FixedOffset> {
    (-86_401..86_400).prop_map(FixedOffset::east)
}

pub fn any_timezone() -> impl Strategy<Value = Tz> {
    (0..TZ_VARIANTS.len()).prop_map(|idx| *TZ_VARIANTS.get(idx).unwrap())
}
