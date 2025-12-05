// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The public API for both compute and storage.

#![warn(missing_docs)]

use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use mz_ore::now::NowFn;

pub use mz_catalog_types::cluster::ReplicaId;

pub mod client;
pub mod metrics;

/// A function that computes the lag between the given time and wallclock time.
///
/// Because sources usually tick once per second and we collect wallclock lag measurements once per
/// second, the measured lag can be off by up to 1s. To reflect this uncertainty, we report lag
/// values rounded to seconds. We always round up to avoid underreporting.
#[derive(Clone)]
pub struct WallclockLagFn<T>(Arc<dyn Fn(T) -> Duration + Send + Sync>);

impl<T: Into<mz_repr::Timestamp>> WallclockLagFn<T> {
    /// Create a new [`WallclockLagFn`].
    pub fn new(now: NowFn) -> Self {
        let inner = Arc::new(move |time: T| {
            let time_ts: mz_repr::Timestamp = time.into();
            let time_ms: u64 = time_ts.into();
            let lag_ms = now().saturating_sub(time_ms);
            let lag_s = lag_ms.div_ceil(1000);
            Duration::from_secs(lag_s)
        });
        Self(inner)
    }
}

impl<T> Deref for WallclockLagFn<T> {
    type Target = dyn Fn(T) -> Duration + Send + Sync;

    fn deref(&self) -> &Self::Target {
        &(*self.0)
    }
}
