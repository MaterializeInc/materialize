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
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::bail;
use serde::{Deserialize, Serialize};

pub mod client;
pub mod metrics;

/// A function that computes the lag between the given time and wallclock time.
pub type WallclockLagFn<T> = Arc<dyn Fn(&T) -> Duration + Send + Sync>;

/// Identifier of a replica.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
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
