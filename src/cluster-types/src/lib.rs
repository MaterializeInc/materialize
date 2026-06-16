// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Lean identifier types shared across the cluster, storage, and compute stack.
//!
//! These types live in their own bottom-level crate so that crates which only
//! need the identifiers (e.g. `mz-sql-types`) do not have to depend on the much
//! heavier `mz-storage-types` / `mz-cluster-client` crates. The richer crates
//! re-export these types under their historical paths.

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use anyhow::bail;
#[cfg(any(test, feature = "proptest"))]
use proptest::arbitrary::Arbitrary;
#[cfg(any(test, feature = "proptest"))]
use proptest::strategy::{BoxedStrategy, Strategy};
use serde::{Deserialize, Serialize};
use tracing::error;

/// Logging configuration of a replica.
#[derive(
    Clone,
    Debug,
    Default,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize
)]
pub struct ComputeReplicaLogging {
    /// Whether to enable logging for the logging dataflows.
    pub log_logging: bool,
    /// The interval at which to log.
    ///
    /// A `None` value indicates that logging is disabled.
    pub interval: Option<Duration>,
}

impl ComputeReplicaLogging {
    /// Return whether logging is enabled.
    pub fn enabled(&self) -> bool {
        self.interval.is_some()
    }
}

/// Identifier of a storage instance.
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
pub enum StorageInstanceId {
    /// A system storage instance.
    System(u64),
    /// A user storage instance.
    User(u64),
}

impl StorageInstanceId {
    /// Creates a new `StorageInstanceId` in the system namespace. The top 16 bits of `id` must be
    /// 0, because this ID is packed into 48 bits of
    /// `mz_repr::GlobalId::IntrospectionSourceIndex`.
    pub fn system(id: u64) -> Option<Self> {
        Self::new(id, Self::System)
    }

    /// Creates a new `StorageInstanceId` in the user namespace. The top 16 bits of `id` must be
    /// 0, because this ID is packed into 48 bits of
    /// `mz_repr::GlobalId::IntrospectionSourceIndex`.
    pub fn user(id: u64) -> Option<Self> {
        Self::new(id, Self::User)
    }

    fn new(id: u64, variant: fn(u64) -> Self) -> Option<Self> {
        const MASK: u64 = 0xFFFF << 48;
        const WARN_MASK: u64 = 1 << 47;
        if MASK & id == 0 {
            if WARN_MASK & id != 0 {
                error!("{WARN_MASK} or more `StorageInstanceId`s allocated, we will run out soon");
            }
            Some(variant(id))
        } else {
            None
        }
    }

    pub fn inner_id(&self) -> u64 {
        match self {
            StorageInstanceId::System(id) | StorageInstanceId::User(id) => *id,
        }
    }

    pub fn is_user(&self) -> bool {
        matches!(self, Self::User(_))
    }

    pub fn is_system(&self) -> bool {
        matches!(self, Self::System(_))
    }
}

impl FromStr for StorageInstanceId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Validate the (single-byte, ASCII) tag before slicing so that a
        // multi-byte leading character doesn't slice inside a UTF-8 boundary.
        let variant = match s.chars().next() {
            Some('s') => Self::System,
            Some('u') => Self::User,
            _ => bail!("couldn't parse compute instance id {}", s),
        };
        let val: u64 = s[1..].parse()?;
        Ok(variant(val))
    }
}

impl fmt::Display for StorageInstanceId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::System(id) => write!(f, "s{}", id),
            Self::User(id) => write!(f, "u{}", id),
        }
    }
}

#[cfg(any(test, feature = "proptest"))]
impl Arbitrary for StorageInstanceId {
    type Parameters = ();
    type Strategy = BoxedStrategy<StorageInstanceId>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        // The inner id must fit in 48 bits: the top 16 are reserved because the
        // id gets packed into `mz_repr::GlobalId::IntrospectionSourceIndex` (see
        // `Self::new`). Only generate ids in that valid range so we never produce
        // an instance that couldn't actually be allocated. Build the variants
        // directly rather than via `Self::system`/`Self::user` to avoid their
        // soft "running out of IDs" warning firing during tests.
        (proptest::arbitrary::any::<bool>(), 0u64..(1 << 48))
            .prop_map(|(is_system, id)| {
                if is_system {
                    StorageInstanceId::System(id)
                } else {
                    StorageInstanceId::User(id)
                }
            })
            .boxed()
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

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[mz_ore::test]
    fn proptest_storage_instance_id_roundtrips() {
        fn testcase(og: StorageInstanceId) {
            let s = og.to_string();
            let rnd: StorageInstanceId = s.parse().unwrap();
            assert_eq!(og, rnd);
        }

        proptest!(|(id in any::<StorageInstanceId>())| {
            testcase(id);
        })
    }

    #[mz_ore::test]
    fn test_storage_instance_id_from_str() {
        assert_eq!(
            "s5".parse::<StorageInstanceId>().unwrap(),
            StorageInstanceId::System(5)
        );
        assert_eq!(
            "u5".parse::<StorageInstanceId>().unwrap(),
            StorageInstanceId::User(5)
        );

        // Regression test for a panic on multi-byte leading characters, where
        // slicing off a single byte landed inside a UTF-8 char boundary (SQL-195).
        for invalid in ["ü1", "ü", "é42", "🦀7", "", "x1", "u"] {
            assert!(
                invalid.parse::<StorageInstanceId>().is_err(),
                "expected {invalid:?} to fail to parse"
            );
        }
    }
}
