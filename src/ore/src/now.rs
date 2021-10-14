// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Now utilities.

use std::time::SystemTime;

#[cfg(feature = "chrono")]
use chrono::{DateTime, TimeZone, Utc};

/// A type representing the number of milliseconds since the Unix epoch.
pub type EpochMillis = u64;

/// A function that returns system or mocked time.
pub type NowFn = fn() -> EpochMillis;

/// Returns the current system time.
pub fn system_time() -> EpochMillis {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("failed to get millis since epoch")
        .as_millis()
        .try_into()
        .expect("current time did not fit into u64")
}

/// Converts epoch milliseconds to a DateTime.
#[cfg(feature = "chrono")]
pub fn to_datetime(millis: EpochMillis) -> DateTime<Utc> {
    let dur = std::time::Duration::from_millis(millis);
    Utc.timestamp(dur.as_secs() as i64, dur.subsec_nanos())
}

/// Returns zero.
pub const fn now_zero() -> EpochMillis {
    0
}
