// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{anyhow, Error};
use core::str::FromStr;
use serde::{Deserialize, Serialize};
use std::fmt;

/// An opaque identifier for a dataflow component. In other words, identifies
/// the target of a [`RelationExpr::Get`](crate::RelationExpr::Get).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum Id {
    /// An identifier that refers to a local component of a dataflow.
    Local(LocalId),
    /// An identifier that refers to a global dataflow.
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

/// A trait for turning [`Id`]s into human-readable strings.
pub trait IdHumanizer {
    /// Attempts to return the a human-readable string for `id`.
    fn humanize_id(&self, id: Id) -> Option<String>;
}

/// The identifier for a local component of a dataflow.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct LocalId(u64);

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

/// The identifier for a global dataflow.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum GlobalId {
    /// System namespace.
    System(u64),
    /// User namespace.
    User(u64),
}

impl GlobalId {
    /// Constructs a new global identifier in the system namespace. It is the
    /// caller's responsibility to provide a unique `v`.
    pub fn system(v: u64) -> GlobalId {
        GlobalId::System(v)
    }

    /// Constructs a new global identifier in the user namespace. It is the
    /// caller's responsiblity to provide a unique `v`.
    pub fn user(v: u64) -> GlobalId {
        GlobalId::User(v)
    }

    /// Reports whether this ID is in the system namespace.
    pub fn is_system(&self) -> bool {
        match self {
            GlobalId::System(_) => true,
            GlobalId::User(_) => false,
        }
    }

    /// Reports whether this ID is in the user namespace.
    pub fn is_user(&self) -> bool {
        match self {
            GlobalId::System(_) => false,
            GlobalId::User(_) => true,
        }
    }
}

impl FromStr for GlobalId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() < 2 {
            return Err(anyhow!("couldn't parse id {}", s));
        }
        let val: u64 = s[1..].parse()?;
        match s.chars().next().unwrap() {
            's' => Ok(GlobalId::System(val)),
            'u' => Ok(GlobalId::User(val)),
            _ => Err(anyhow!("couldn't parse id {}", s)),
        }
    }
}

impl fmt::Display for GlobalId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            GlobalId::System(id) => write!(f, "s{}", id),
            GlobalId::User(id) => write!(f, "u{}", id),
        }
    }
}

/// Unique identifier for an instantiation of a source.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct SourceInstanceId {
    // The ID of the source.
    pub source_id: GlobalId,
    // The ID of the timely dataflow containing this instantiation of this
    // source.
    pub dataflow_id: usize,
}

impl fmt::Display for SourceInstanceId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}/{}", self.source_id, self.dataflow_id)
    }
}

/// Unique identifier for each part of a whole source.
///     Kafka -> partition
///     Kinesis -> shard
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum PartitionId {
    Kafka(i32),
    Kinesis(String),
    File,
}

impl fmt::Display for PartitionId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PartitionId::Kafka(id) => write!(f, "{}", id.to_string()),
            _ => write!(f, "0"),
        }
    }
}

impl PartitionId {
    pub fn kafka_id(&self) -> Option<i32> {
        match self {
            PartitionId::Kafka(id) => Some(*id),
            _ => None,
        }
    }
}

/// Humanizer that provides no additional information.
#[derive(Debug)]
pub struct DummyHumanizer;

impl IdHumanizer for DummyHumanizer {
    fn humanize_id(&self, _: Id) -> Option<String> {
        None
    }
}

#[cfg(test)]
pub mod test_utils {
    use super::*;

    impl From<&LocalId> for char {
        fn from(id: &LocalId) -> char {
            id.0 as u8 as char
        }
    }
}
