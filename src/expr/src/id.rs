// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;
use std::str::FromStr;

use anyhow::Error;
use serde::{Deserialize, Serialize};

use mz_lowertest::MzReflect;
use mz_repr::GlobalId;

// The `Arbitrary` impls are only used during testing and we gate them
// behind `cfg(feature = "test-utils")`, so `proptest` can remain a dev-dependency.
// See https://github.com/MaterializeInc/materialize/pull/11717.
#[cfg(feature = "test-utils")]
use proptest_derive::Arbitrary;

/// An opaque identifier for a dataflow component. In other words, identifies
/// the target of a [`MirRelationExpr::Get`](crate::MirRelationExpr::Get).
#[derive(
    Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize, MzReflect,
)]
#[cfg_attr(feature = "test-utils", derive(Arbitrary))]
pub enum Id {
    /// An identifier that refers to a local component of a dataflow.
    Local(LocalId),
    /// An identifier that refers to a global dataflow.
    #[cfg_attr(
        feature = "test-utils",
        proptest(value = "Id::Global(GlobalId::System(2))")
    )]
    Global(GlobalId),
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Id::Local(id) => id.fmt(f),
            Id::Global(id) => id.fmt(f),
        }
    }
}

/// The identifier for a local component of a dataflow.
#[derive(
    Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize, MzReflect,
)]
#[cfg_attr(feature = "test-utils", derive(Arbitrary))]
pub struct LocalId(pub(crate) u64);

impl LocalId {
    /// Constructs a new local identifier. It is the caller's responsibility
    /// to provide a unique `v`.
    pub fn new(v: u64) -> LocalId {
        LocalId(v)
    }
}

impl fmt::Display for LocalId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "l{}", self.0)
    }
}

/// Unique identifier for an instantiation of a source.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct SourceInstanceId {
    /// The ID of the source, shared across all instances.
    pub source_id: GlobalId,
    /// The ID of the timely dataflow containing this instantiation of this
    /// source.
    pub dataflow_id: usize,
}

impl fmt::Display for SourceInstanceId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}/{}", self.source_id, self.dataflow_id)
    }
}

/// Unique identifier for each part of a whole source.
///     Kafka -> partition
///     None -> sources that have no notion of partitioning (e.g file sources)
#[derive(Clone, Debug, Eq, Hash, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[cfg_attr(feature = "test-utils", derive(Arbitrary))]
pub enum PartitionId {
    Kafka(i32),
    None,
}

impl fmt::Display for PartitionId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PartitionId::Kafka(id) => write!(f, "{}", id),
            PartitionId::None => write!(f, "none"),
        }
    }
}

impl From<&PartitionId> for Option<String> {
    fn from(pid: &PartitionId) -> Option<String> {
        match pid {
            PartitionId::None => None,
            _ => Some(pid.to_string()),
        }
    }
}

impl FromStr for PartitionId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "none" => Ok(PartitionId::None),
            s => {
                let val: i32 = s.parse()?;
                Ok(PartitionId::Kafka(val))
            }
        }
    }
}
