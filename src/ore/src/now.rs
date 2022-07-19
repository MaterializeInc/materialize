// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Now utilities.

use std::fmt;
use std::ops::Deref;
use std::sync::Arc;
use std::time::SystemTime;

use once_cell::sync::Lazy;

#[cfg(feature = "chrono")]
use chrono::{DateTime, TimeZone, Utc};

/// A type representing the number of milliseconds since the Unix epoch.
pub type EpochMillis = u64;

/// Converts epoch milliseconds to a DateTime.
#[cfg(feature = "chrono")]
pub fn to_datetime(millis: EpochMillis) -> DateTime<Utc> {
    let dur = std::time::Duration::from_millis(millis);
    Utc.timestamp(dur.as_secs() as i64, dur.subsec_nanos())
}

/// A function that returns system or mocked time.
// This is a newtype so that it can implement `Debug`, as closures don't
// implement `Debug` by default. It derefs to a callable so that it is
// ergonomically equivalent to a closure.
#[derive(Clone)]
pub struct NowFn(Arc<dyn Fn() -> EpochMillis + Send + Sync>);

impl NowFn {
    /// Returns now in seconds.
    pub fn as_secs(&self) -> i64 {
        ((self)() / 1_000) as i64
    }
}

impl fmt::Debug for NowFn {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("<now_fn>")
    }
}

impl Deref for NowFn {
    type Target = dyn Fn() -> EpochMillis + Send + Sync;

    fn deref(&self) -> &Self::Target {
        &(*self.0)
    }
}

impl<F> From<F> for NowFn
where
    F: Fn() -> EpochMillis + Send + Sync + 'static,
{
    fn from(f: F) -> NowFn {
        NowFn(Arc::new(f))
    }
}

fn system_time() -> EpochMillis {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("failed to get millis since epoch")
        .as_millis()
        .try_into()
        .expect("current time did not fit into u64")
}
fn now_zero() -> EpochMillis {
    0
}

/// A [`NowFn`] that returns the actual system time.
pub static SYSTEM_TIME: Lazy<NowFn> = Lazy::new(|| NowFn::from(system_time));

/// A [`NowFn`] that always returns zero.
///
/// For use in tests.
pub static NOW_ZERO: Lazy<NowFn> = Lazy::new(|| NowFn::from(now_zero));
