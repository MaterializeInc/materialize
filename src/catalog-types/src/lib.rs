// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Shared types for the Materialize catalog.

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use anyhow::bail;
use serde::{Deserialize, Serialize};
use tracing::error;

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

/// Identifier of a storage instance.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum StorageInstanceId {
    /// A system storage instance.
    System(u64),
    /// A user storage instance.
    User(u64),
}

impl StorageInstanceId {
    /// Creates a new `StorageInstanceId` in the system namespace. The top 16 bits of `id` must be
    /// 0, because this ID is packed into 48 bits of
    /// [`mz_repr::GlobalId::IntrospectionSourceIndex`].
    pub fn system(id: u64) -> Option<Self> {
        Self::new(id, Self::System)
    }

    /// Creates a new `StorageInstanceId` in the user namespace. The top 16 bits of `id` must be
    /// 0, because this ID is packed into 48 bits of
    /// [`mz_repr::GlobalId::IntrospectionSourceIndex`].
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

    /// Return the inner numeric ID value.
    pub fn inner_id(&self) -> u64 {
        match self {
            StorageInstanceId::System(id) | StorageInstanceId::User(id) => *id,
        }
    }

    /// Whether this value identifies a user storage instance.
    pub fn is_user(&self) -> bool {
        matches!(self, Self::User(_))
    }

    /// Whether this value identifies a system storage instance.
    pub fn is_system(&self) -> bool {
        matches!(self, Self::System(_))
    }
}

impl FromStr for StorageInstanceId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() < 2 {
            bail!("couldn't parse compute instance id {}", s);
        }
        let val: u64 = s[1..].parse()?;
        match s.chars().next().unwrap() {
            's' => Ok(Self::System(val)),
            'u' => Ok(Self::User(val)),
            _ => bail!("couldn't parse compute instance id {}", s),
        }
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

/// Logging configuration of a replica.
#[derive(Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
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

/// The identifier for a database.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum DatabaseId {
    /// A user database.
    User(u64),
    /// A system database.
    System(u64),
}

impl DatabaseId {
    /// Whether this value identifies a user database.
    pub fn is_user(&self) -> bool {
        matches!(self, DatabaseId::User(_))
    }

    /// Whether this value identifies a system database.
    pub fn is_system(&self) -> bool {
        matches!(self, DatabaseId::System(_))
    }
}

impl fmt::Display for DatabaseId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DatabaseId::System(id) => write!(f, "s{}", id),
            DatabaseId::User(id) => write!(f, "u{}", id),
        }
    }
}

impl FromStr for DatabaseId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() < 2 {
            bail!("couldn't parse DatabaseId {}", s);
        }
        let val: u64 = s[1..].parse()?;
        match s.chars().next() {
            Some('s') => Ok(DatabaseId::System(val)),
            Some('u') => Ok(DatabaseId::User(val)),
            _ => bail!("couldn't parse DatabaseId {}", s),
        }
    }
}

/// The identifier for a schema.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum SchemaId {
    /// A user schema.
    User(u64),
    /// A system schema.
    System(u64),
}

impl SchemaId {
    /// Whether this value identifies a user schema.
    pub fn is_user(&self) -> bool {
        matches!(self, SchemaId::User(_))
    }

    /// Whether this value identifies a system schema.
    pub fn is_system(&self) -> bool {
        matches!(self, SchemaId::System(_))
    }
}

impl fmt::Display for SchemaId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SchemaId::System(id) => write!(f, "s{}", id),
            SchemaId::User(id) => write!(f, "u{}", id),
        }
    }
}

impl FromStr for SchemaId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() < 2 {
            bail!("couldn't parse SchemaId {}", s);
        }
        let val: u64 = s[1..].parse()?;
        match s.chars().next() {
            Some('s') => Ok(SchemaId::System(val)),
            Some('u') => Ok(SchemaId::User(val)),
            _ => bail!("couldn't parse SchemaId {}", s),
        }
    }
}

/// Specification for a database. Either the "ambient" database (no explicit database)
/// or a specific database by ID.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ResolvedDatabaseSpecifier {
    /// The "ambient" database, which is always present and is not named
    /// explicitly, but by omission.
    Ambient,
    /// A normal database with a name.
    Id(DatabaseId),
}

impl ResolvedDatabaseSpecifier {
    /// Returns the database ID if this is not the ambient database.
    pub fn id(&self) -> Option<DatabaseId> {
        match self {
            ResolvedDatabaseSpecifier::Ambient => None,
            ResolvedDatabaseSpecifier::Id(id) => Some(*id),
        }
    }
}

/// Specification for a schema. Either a temporary schema or a specific schema by ID.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum SchemaSpecifier {
    /// A temporary schema.
    Temporary,
    /// A normal schema with an ID.
    Id(SchemaId),
}

impl SchemaSpecifier {
    /// Whether this value identifies a system schema.
    pub fn is_system(&self) -> bool {
        match self {
            SchemaSpecifier::Temporary => false,
            SchemaSpecifier::Id(id) => id.is_system(),
        }
    }

    /// Whether this value identifies a user schema.
    /// Note: Temporary schemas are considered user schemas.
    pub fn is_user(&self) -> bool {
        match self {
            SchemaSpecifier::Temporary => true,
            SchemaSpecifier::Id(id) => id.is_user(),
        }
    }

    /// Whether this is a temporary schema.
    pub fn is_temporary(&self) -> bool {
        matches!(self, SchemaSpecifier::Temporary)
    }
}
