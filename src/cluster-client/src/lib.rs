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

use std::fmt;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::bail;
use mz_ore::now::NowFn;
use serde::{Deserialize, Serialize};

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

/// Identifier of a replica.
#[derive(
    Clone,
    Copy,
    Debug,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    Serialize,
    Deserialize
)]
pub enum ReplicaId {
    /// A user replica.
    User(u64),
    /// A system replica.
    System(u64),
}

impl ReplicaId {
    /// Return the inner numeric ID value.
    pub fn inner_id(&self) -> u64 {
        match self {
            ReplicaId::User(id) => *id,
            ReplicaId::System(id) => *id,
        }
    }

    /// Whether this value identifies a user replica.
    pub fn is_user(&self) -> bool {
        matches!(self, Self::User(_))
    }

    /// Whether this value identifies a system replica.
    pub fn is_system(&self) -> bool {
        matches!(self, Self::System(_))
    }
}

impl fmt::Display for ReplicaId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::User(id) => write!(f, "u{}", id),
            Self::System(id) => write!(f, "s{}", id),
        }
    }
}

impl FromStr for ReplicaId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let first = s.chars().next();
        let rest = s.get(1..);
        if let (Some(prefix), Some(num)) = (first, rest) {
            let id = num.parse()?;
            match prefix {
                'u' => return Ok(Self::User(id)),
                's' => return Ok(Self::System(id)),
                _ => (),
            }
        }

        bail!("invalid replica ID: {}", s);
    }
}
