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
use std::mem::size_of;
use std::num::ParseIntError;
use std::ops::BitOrAssign;
use std::str::FromStr;
use std::time::Duration;

use anyhow::{anyhow, Error};
use bitflags::bitflags;
use mz_lowertest::MzReflect;
use mz_ore::str::StrExt;
use proptest::arbitrary::Arbitrary as PropArbitrary;
use proptest::prelude::*;
use proptest::strategy::{BoxedStrategy, Strategy};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use tracing::error;

/// Error returned when parsing an ID from a string fails.
#[derive(Debug, Clone)]
pub struct IdParseError {
    /// The type name of the ID being parsed.
    pub type_name: &'static str,
    /// The input string that failed to parse.
    pub input: String,
    /// Optional underlying integer parse error.
    pub int_error: Option<ParseIntError>,
}

impl fmt::Display for IdParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "couldn't parse {} {}", self.type_name, self.input)
    }
}

impl std::error::Error for IdParseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.int_error.as_ref().map(|e| e as _)
    }
}

impl IdParseError {
    fn new(type_name: &'static str, input: &str) -> Self {
        IdParseError {
            type_name,
            input: input.to_string(),
            int_error: None,
        }
    }

    fn with_int_error(type_name: &'static str, input: &str, int_error: ParseIntError) -> Self {
        IdParseError {
            type_name,
            input: input.to_string(),
            int_error: Some(int_error),
        }
    }
}

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
    type Err = IdParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let first = s.chars().next();
        let rest = s.get(1..);
        if let (Some(prefix), Some(num)) = (first, rest) {
            let id = num
                .parse()
                .map_err(|e| IdParseError::with_int_error("ReplicaId", s, e))?;
            match prefix {
                'u' => return Ok(Self::User(id)),
                's' => return Ok(Self::System(id)),
                _ => (),
            }
        }

        Err(IdParseError::new("ReplicaId", s))
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
    /// [`GlobalId::IntrospectionSourceIndex`].
    pub fn system(id: u64) -> Option<Self> {
        Self::new(id, Self::System)
    }

    /// Creates a new `StorageInstanceId` in the user namespace. The top 16 bits of `id` must be
    /// 0, because this ID is packed into 48 bits of
    /// [`GlobalId::IntrospectionSourceIndex`].
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
    type Err = IdParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() < 2 {
            return Err(IdParseError::new("StorageInstanceId", s));
        }
        let val: u64 = s[1..]
            .parse()
            .map_err(|e| IdParseError::with_int_error("StorageInstanceId", s, e))?;
        match s.chars().next().unwrap() {
            's' => Ok(Self::System(val)),
            'u' => Ok(Self::User(val)),
            _ => Err(IdParseError::new("StorageInstanceId", s)),
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
#[derive(
    Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize, Arbitrary,
)]
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
    type Err = IdParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() < 2 {
            return Err(IdParseError::new("DatabaseId", s));
        }
        let val: u64 = s[1..]
            .parse()
            .map_err(|e| IdParseError::with_int_error("DatabaseId", s, e))?;
        match s.chars().next() {
            Some('s') => Ok(DatabaseId::System(val)),
            Some('u') => Ok(DatabaseId::User(val)),
            _ => Err(IdParseError::new("DatabaseId", s)),
        }
    }
}

/// The identifier for a schema.
#[derive(
    Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize, Arbitrary,
)]
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
    type Err = IdParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() < 2 {
            return Err(IdParseError::new("SchemaId", s));
        }
        let val: u64 = s[1..]
            .parse()
            .map_err(|e| IdParseError::with_int_error("SchemaId", s, e))?;
        match s.chars().next() {
            Some('s') => Ok(SchemaId::System(val)),
            Some('u') => Ok(SchemaId::User(val)),
            _ => Err(IdParseError::new("SchemaId", s)),
        }
    }
}

/// Specification for a database. Either the "ambient" database (no explicit database)
/// or a specific database by ID.
#[derive(
    Clone, Copy, Debug, Eq, PartialEq, Hash, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
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

impl fmt::Display for ResolvedDatabaseSpecifier {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Ambient => f.write_str("<none>"),
            Self::Id(id) => write!(f, "{}", id),
        }
    }
}

impl From<DatabaseId> for ResolvedDatabaseSpecifier {
    fn from(id: DatabaseId) -> Self {
        Self::Id(id)
    }
}

impl From<Option<DatabaseId>> for ResolvedDatabaseSpecifier {
    fn from(id: Option<DatabaseId>) -> Self {
        match id {
            Some(id) => Self::Id(id),
            None => Self::Ambient,
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
    /// The ID used for temporary schemas in display format.
    pub const TEMPORARY_SCHEMA_ID: u64 = 0;

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

impl fmt::Display for SchemaSpecifier {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Temporary => write!(f, "{}", Self::TEMPORARY_SCHEMA_ID),
            Self::Id(id) => write!(f, "{}", id),
        }
    }
}

impl From<SchemaId> for SchemaSpecifier {
    fn from(id: SchemaId) -> SchemaSpecifier {
        match id {
            SchemaId::User(id) if id == SchemaSpecifier::TEMPORARY_SCHEMA_ID => {
                SchemaSpecifier::Temporary
            }
            schema_id => SchemaSpecifier::Id(schema_id),
        }
    }
}

impl From<&SchemaSpecifier> for SchemaId {
    fn from(schema_spec: &SchemaSpecifier) -> Self {
        match schema_spec {
            SchemaSpecifier::Temporary => SchemaId::User(SchemaSpecifier::TEMPORARY_SCHEMA_ID),
            SchemaSpecifier::Id(id) => *id,
        }
    }
}

impl From<SchemaSpecifier> for SchemaId {
    fn from(schema_spec: SchemaSpecifier) -> Self {
        match schema_spec {
            SchemaSpecifier::Temporary => SchemaId::User(SchemaSpecifier::TEMPORARY_SCHEMA_ID),
            SchemaSpecifier::Id(id) => id,
        }
    }
}

const ROLE_ID_SYSTEM_CHAR: char = 's';
const ROLE_ID_SYSTEM_BYTE: u8 = b's';
const ROLE_ID_PREDEFINED_CHAR: char = 'g';
const ROLE_ID_PREDEFINED_BYTE: u8 = b'g';
const ROLE_ID_USER_CHAR: char = 'u';
const ROLE_ID_USER_BYTE: u8 = b'u';
const ROLE_ID_PUBLIC_CHAR: char = 'p';
const ROLE_ID_PUBLIC_BYTE: u8 = b'p';

/// The identifier for a role.
#[derive(
    Arbitrary,
    Clone,
    Copy,
    Debug,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    Serialize,
    Deserialize,
    MzReflect,
)]
pub enum RoleId {
    System(u64),
    /// Like system roles, these are roles built into the system. However, they are grantable to
    /// users and provide access to certain, commonly needed, privileged capabilities and
    /// information (modelled after <https://www.postgresql.org/docs/16/predefined-roles.html>).
    Predefined(u64),
    User(u64),
    Public,
}

impl RoleId {
    pub fn is_system(&self) -> bool {
        matches!(self, Self::System(_))
    }

    pub fn is_user(&self) -> bool {
        matches!(self, Self::User(_))
    }

    pub fn is_public(&self) -> bool {
        matches!(self, Self::Public)
    }

    pub fn is_predefined(&self) -> bool {
        matches!(self, Self::Predefined(_))
    }

    pub fn is_builtin(&self) -> bool {
        self.is_public() || self.is_system() || self.is_predefined()
    }

    pub fn encode_binary(&self) -> Vec<u8> {
        let mut res = Vec::with_capacity(Self::binary_size());
        match self {
            RoleId::System(id) => {
                res.push(ROLE_ID_SYSTEM_BYTE);
                res.extend_from_slice(&id.to_le_bytes());
            }
            RoleId::Predefined(id) => {
                res.push(ROLE_ID_PREDEFINED_BYTE);
                res.extend_from_slice(&id.to_le_bytes());
            }
            RoleId::User(id) => {
                res.push(ROLE_ID_USER_BYTE);
                res.extend_from_slice(&id.to_le_bytes());
            }
            RoleId::Public => {
                res.push(ROLE_ID_PUBLIC_BYTE);
                res.extend_from_slice(&0_u64.to_le_bytes());
            }
        }
        res
    }

    pub fn decode_binary(raw: &[u8]) -> Result<RoleId, Error> {
        if raw.len() != RoleId::binary_size() {
            return Err(anyhow!(
                "invalid binary size, expecting {}, found {}",
                RoleId::binary_size(),
                raw.len()
            ));
        }

        let variant = raw[0];
        let id = u64::from_le_bytes(raw[1..].try_into()?);

        match variant {
            ROLE_ID_SYSTEM_BYTE => Ok(RoleId::System(id)),
            ROLE_ID_PREDEFINED_BYTE => Ok(RoleId::Predefined(id)),
            ROLE_ID_USER_BYTE => Ok(RoleId::User(id)),
            ROLE_ID_PUBLIC_BYTE => Ok(RoleId::Public),
            _ => Err(anyhow!("unrecognized role id variant byte '{variant}'")),
        }
    }

    pub const fn binary_size() -> usize {
        1 + std::mem::size_of::<u64>()
    }
}

impl FromStr for RoleId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        fn parse_u64(s: &str) -> Result<u64, Error> {
            if s.len() < 2 {
                return Err(anyhow!("couldn't parse role id '{s}'"));
            }
            s[1..]
                .parse()
                .map_err(|_| anyhow!("couldn't parse role id '{s}'"))
        }

        match s.chars().next() {
            Some(ROLE_ID_SYSTEM_CHAR) => {
                let val = parse_u64(s)?;
                Ok(Self::System(val))
            }
            Some(ROLE_ID_PREDEFINED_CHAR) => {
                let val = parse_u64(s)?;
                Ok(Self::Predefined(val))
            }
            Some(ROLE_ID_USER_CHAR) => {
                let val = parse_u64(s)?;
                Ok(Self::User(val))
            }
            Some(ROLE_ID_PUBLIC_CHAR) => {
                if s.len() == 1 {
                    Ok(Self::Public)
                } else {
                    Err(anyhow!("couldn't parse role id '{s}'"))
                }
            }
            _ => Err(anyhow!("couldn't parse role id '{s}'")),
        }
    }
}

impl fmt::Display for RoleId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::System(id) => write!(f, "{ROLE_ID_SYSTEM_CHAR}{id}"),
            Self::Predefined(id) => write!(f, "{ROLE_ID_PREDEFINED_CHAR}{id}"),
            Self::User(id) => write!(f, "{ROLE_ID_USER_CHAR}{id}"),
            Self::Public => write!(f, "{ROLE_ID_PUBLIC_CHAR}"),
        }
    }
}

const NETWORK_POLICY_ID_SYSTEM_CHAR: char = 's';
const NETWORK_POLICY_ID_USER_CHAR: char = 'u';

/// The identifier for a network policy.
#[derive(
    Arbitrary,
    Clone,
    Copy,
    Debug,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    Serialize,
    Deserialize,
    MzReflect,
)]
pub enum NetworkPolicyId {
    System(u64),
    User(u64),
}

impl NetworkPolicyId {
    pub fn is_system(&self) -> bool {
        matches!(self, Self::System(_))
    }

    pub fn is_user(&self) -> bool {
        matches!(self, Self::User(_))
    }

    pub fn is_builtin(&self) -> bool {
        self.is_system()
    }
}

impl FromStr for NetworkPolicyId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        fn parse_u64(s: &str) -> Result<u64, Error> {
            if s.len() < 2 {
                return Err(anyhow!("couldn't parse network policy id '{s}'"));
            }
            s[1..]
                .parse()
                .map_err(|_| anyhow!("couldn't parse network policy id '{s}'"))
        }

        match s.chars().next() {
            Some(NETWORK_POLICY_ID_SYSTEM_CHAR) => {
                let val = parse_u64(s)?;
                Ok(Self::System(val))
            }
            Some(NETWORK_POLICY_ID_USER_CHAR) => {
                let val = parse_u64(s)?;
                Ok(Self::User(val))
            }
            _ => Err(anyhow!("couldn't parse network policy id '{s}'")),
        }
    }
}

impl fmt::Display for NetworkPolicyId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::System(id) => write!(f, "{NETWORK_POLICY_ID_SYSTEM_CHAR}{id}"),
            Self::User(id) => write!(f, "{NETWORK_POLICY_ID_USER_CHAR}{id}"),
        }
    }
}

/// The identifier for an item within the Catalog.
#[derive(
    Arbitrary,
    Clone,
    Copy,
    Debug,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    Serialize,
    Deserialize,
    MzReflect,
)]
pub enum CatalogItemId {
    /// System namespace.
    System(u64),
    /// Introspection Source Index namespace.
    IntrospectionSourceIndex(u64),
    /// User namespace.
    User(u64),
    /// Transient item.
    Transient(u64),
}

impl CatalogItemId {
    /// Reports whether this ID is in the system namespace.
    pub fn is_system(&self) -> bool {
        matches!(
            self,
            CatalogItemId::System(_) | CatalogItemId::IntrospectionSourceIndex(_)
        )
    }

    /// Reports whether this ID is in the user namespace.
    pub fn is_user(&self) -> bool {
        matches!(self, CatalogItemId::User(_))
    }

    /// Reports whether this ID is for a transient item.
    pub fn is_transient(&self) -> bool {
        matches!(self, CatalogItemId::Transient(_))
    }
}

impl FromStr for CatalogItemId {
    type Err = Error;

    fn from_str(mut s: &str) -> Result<Self, Self::Err> {
        if s.len() < 2 {
            return Err(anyhow!("couldn't parse id {}", s));
        }
        let tag = s.chars().next().unwrap();
        s = &s[1..];
        let variant = match tag {
            's' => {
                if Some('i') == s.chars().next() {
                    s = &s[1..];
                    CatalogItemId::IntrospectionSourceIndex
                } else {
                    CatalogItemId::System
                }
            }
            'u' => CatalogItemId::User,
            't' => CatalogItemId::Transient,
            _ => return Err(anyhow!("couldn't parse id {}", s)),
        };
        let val: u64 = s.parse()?;
        Ok(variant(val))
    }
}

impl fmt::Display for CatalogItemId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CatalogItemId::System(id) => write!(f, "s{}", id),
            CatalogItemId::IntrospectionSourceIndex(id) => write!(f, "si{}", id),
            CatalogItemId::User(id) => write!(f, "u{}", id),
            CatalogItemId::Transient(id) => write!(f, "t{}", id),
        }
    }
}

/// The identifier for an item/object.
///
/// WARNING: `GlobalId`'s `Ord` implementation does not express a dependency order.
/// One should explicitly topologically sort objects by their dependencies, rather
/// than rely on the order of identifiers.
#[derive(
    Arbitrary,
    Clone,
    Copy,
    Debug,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    Serialize,
    Deserialize,
    MzReflect,
    columnar::Columnar,
)]
pub enum GlobalId {
    /// System namespace.
    System(u64),
    /// Introspection Source Index namespace.
    IntrospectionSourceIndex(u64),
    /// User namespace.
    User(u64),
    /// Transient namespace.
    Transient(u64),
    /// Dummy id for query being explained
    Explain,
}

impl GlobalId {
    /// Reports whether this ID is in the system namespace.
    pub fn is_system(&self) -> bool {
        matches!(
            self,
            GlobalId::System(_) | GlobalId::IntrospectionSourceIndex(_)
        )
    }

    /// Reports whether this ID is in the user namespace.
    pub fn is_user(&self) -> bool {
        matches!(self, GlobalId::User(_))
    }

    /// Reports whether this ID is in the transient namespace.
    pub fn is_transient(&self) -> bool {
        matches!(self, GlobalId::Transient(_))
    }
}

impl FromStr for GlobalId {
    type Err = Error;

    fn from_str(mut s: &str) -> Result<Self, Self::Err> {
        if s.len() < 2 {
            return Err(anyhow!("couldn't parse id {}", s));
        }
        if s == "Explained Query" {
            return Ok(GlobalId::Explain);
        }
        let tag = s.chars().next().unwrap();
        s = &s[1..];
        let variant = match tag {
            's' => {
                if Some('i') == s.chars().next() {
                    s = &s[1..];
                    GlobalId::IntrospectionSourceIndex
                } else {
                    GlobalId::System
                }
            }
            'u' => GlobalId::User,
            't' => GlobalId::Transient,
            _ => return Err(anyhow!("couldn't parse id {}", s)),
        };
        let val: u64 = s.parse()?;
        Ok(variant(val))
    }
}

impl fmt::Display for GlobalId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            GlobalId::System(id) => write!(f, "s{}", id),
            GlobalId::IntrospectionSourceIndex(id) => write!(f, "si{}", id),
            GlobalId::User(id) => write!(f, "u{}", id),
            GlobalId::Transient(id) => write!(f, "t{}", id),
            GlobalId::Explain => write!(f, "Explained Query"),
        }
    }
}

/// A version of a relation.
///
/// A new version is created any time a relation is altered.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Hash,
    MzReflect,
    Arbitrary,
)]
pub struct RelationVersion(u64);

impl RelationVersion {
    /// Returns the "root" or "initial" version of a relation.
    pub fn root() -> Self {
        RelationVersion(0)
    }

    /// Returns an instance of [`RelationVersion`] which is "one" higher than `self`.
    pub fn bump(&self) -> Self {
        let next_version = self
            .0
            .checked_add(1)
            .expect("added more than u64::MAX columns?");
        RelationVersion(next_version)
    }

    /// Consume a [`RelationVersion`] returning the raw value.
    ///
    /// Should __only__ be used for serialization.
    pub fn into_raw(self) -> u64 {
        self.0
    }

    /// Create a [`RelationVersion`] from a raw value.
    ///
    /// Should __only__ be used for serialization.
    pub fn from_raw(val: u64) -> RelationVersion {
        RelationVersion(val)
    }
}

impl fmt::Display for RelationVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}", self.0)
    }
}

// Append
const INSERT_CHAR: char = 'a';
// Read
const SELECT_CHAR: char = 'r';
// Write
const UPDATE_CHAR: char = 'w';
// Delete
const DELETE_CHAR: char = 'd';
// Usage
const USAGE_CHAR: char = 'U';
// Create
const CREATE_CHAR: char = 'C';
// Role
const CREATE_ROLE_CHAR: char = 'R';
// dataBase
const CREATE_DB_CHAR: char = 'B';
// compute Node
const CREATE_CLUSTER_CHAR: char = 'N';
// compute network Policy
const CREATE_NETWORK_POLICY_CHAR: char = 'P';

const INSERT_STR: &str = "INSERT";
const SELECT_STR: &str = "SELECT";
const UPDATE_STR: &str = "UPDATE";
const DELETE_STR: &str = "DELETE";
const USAGE_STR: &str = "USAGE";
const CREATE_STR: &str = "CREATE";
const CREATE_ROLE_STR: &str = "CREATEROLE";
const CREATE_DB_STR: &str = "CREATEDB";
const CREATE_CLUSTER_STR: &str = "CREATECLUSTER";
const CREATE_NETWORK_POLICY_STR: &str = "CREATENETWORKPOLICY";

bitflags! {
    /// A bit flag representing all the privileges that can be granted to a role.
    ///
    /// Modeled after:
    /// https://github.com/postgres/postgres/blob/7f5b19817eaf38e70ad1153db4e644ee9456853e/src/include/nodes/parsenodes.h#L74-L101
    ///
    /// The lower 32 bits are used for different privilege types.
    ///
    /// The upper 32 bits indicate a grant option on the privilege for the current bit shifted
    /// right by 32 bits (Currently unimplemented in Materialize).
    ///
    /// Privileges that exist in Materialize but not PostgreSQL start at the highest available bit
    /// and move down towards the PostgreSQL compatible bits. This is try to avoid collisions with
    /// privileges that PostgreSQL may add in the future.
    #[derive(Serialize, Deserialize)]
    pub struct AclMode: u64 {
        // PostgreSQL compatible privileges.
        const INSERT = 1 << 0;
        const SELECT = 1 << 1;
        const UPDATE = 1 << 2;
        const DELETE = 1 << 3;
        const USAGE = 1 << 8;
        const CREATE = 1 << 9;

        // Materialize custom privileges.
        const CREATE_CLUSTER = 1 << 29;
        const CREATE_DB = 1 << 30;
        const CREATE_ROLE = 1 << 31;
        const CREATE_NETWORK_POLICY = 1 << 32;

        // No additional privileges should be defined at a bit larger than 1 << 31. Those bits are
        // reserved for grant options.
    }
}

impl AclMode {
    pub fn parse_single_privilege(s: &str) -> Result<Self, Error> {
        match s.trim().to_uppercase().as_str() {
            INSERT_STR => Ok(AclMode::INSERT),
            SELECT_STR => Ok(AclMode::SELECT),
            UPDATE_STR => Ok(AclMode::UPDATE),
            DELETE_STR => Ok(AclMode::DELETE),
            USAGE_STR => Ok(AclMode::USAGE),
            CREATE_STR => Ok(AclMode::CREATE),
            CREATE_ROLE_STR => Ok(AclMode::CREATE_ROLE),
            CREATE_DB_STR => Ok(AclMode::CREATE_DB),
            CREATE_CLUSTER_STR => Ok(AclMode::CREATE_CLUSTER),
            CREATE_NETWORK_POLICY_STR => Ok(AclMode::CREATE_NETWORK_POLICY),
            _ => Err(anyhow!("{}", s.quoted())),
        }
    }

    pub fn parse_multiple_privileges(s: &str) -> Result<Self, Error> {
        let mut acl_mode = AclMode::empty();
        for privilege in s.split(',') {
            let privilege = AclMode::parse_single_privilege(privilege)?;
            acl_mode.bitor_assign(privilege);
        }
        Ok(acl_mode)
    }

    pub fn to_error_string(&self) -> String {
        self.explode().join(", ")
    }

    pub fn explode(&self) -> Vec<&'static str> {
        let mut privileges = Vec::new();
        if self.contains(AclMode::SELECT) {
            privileges.push(SELECT_STR);
        }
        if self.contains(AclMode::INSERT) {
            privileges.push(INSERT_STR);
        }
        if self.contains(AclMode::UPDATE) {
            privileges.push(UPDATE_STR);
        }
        if self.contains(AclMode::DELETE) {
            privileges.push(DELETE_STR);
        }
        if self.contains(AclMode::USAGE) {
            privileges.push(USAGE_STR);
        }
        if self.contains(AclMode::CREATE) {
            privileges.push(CREATE_STR);
        }
        if self.contains(AclMode::CREATE_ROLE) {
            privileges.push(CREATE_ROLE_STR);
        }
        if self.contains(AclMode::CREATE_DB) {
            privileges.push(CREATE_DB_STR);
        }
        if self.contains(AclMode::CREATE_CLUSTER) {
            privileges.push(CREATE_CLUSTER_STR);
        }
        if self.contains(AclMode::CREATE_NETWORK_POLICY) {
            privileges.push(CREATE_NETWORK_POLICY_STR);
        }
        privileges
    }
}

impl FromStr for AclMode {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut acl_mode = AclMode::empty();
        for c in s.chars() {
            match c {
                INSERT_CHAR => acl_mode.bitor_assign(AclMode::INSERT),
                SELECT_CHAR => acl_mode.bitor_assign(AclMode::SELECT),
                UPDATE_CHAR => acl_mode.bitor_assign(AclMode::UPDATE),
                DELETE_CHAR => acl_mode.bitor_assign(AclMode::DELETE),
                USAGE_CHAR => acl_mode.bitor_assign(AclMode::USAGE),
                CREATE_CHAR => acl_mode.bitor_assign(AclMode::CREATE),
                CREATE_ROLE_CHAR => acl_mode.bitor_assign(AclMode::CREATE_ROLE),
                CREATE_DB_CHAR => acl_mode.bitor_assign(AclMode::CREATE_DB),
                CREATE_CLUSTER_CHAR => acl_mode.bitor_assign(AclMode::CREATE_CLUSTER),
                CREATE_NETWORK_POLICY_CHAR => acl_mode.bitor_assign(AclMode::CREATE_NETWORK_POLICY),
                _ => return Err(anyhow!("invalid privilege '{c}' in acl mode '{s}'")),
            }
        }
        Ok(acl_mode)
    }
}

impl fmt::Display for AclMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // The order of each flag matches PostgreSQL, which uses the same order that they're
        // defined in.
        if self.contains(AclMode::INSERT) {
            write!(f, "{INSERT_CHAR}")?;
        }
        if self.contains(AclMode::SELECT) {
            write!(f, "{SELECT_CHAR}")?;
        }
        if self.contains(AclMode::UPDATE) {
            write!(f, "{UPDATE_CHAR}")?;
        }
        if self.contains(AclMode::DELETE) {
            write!(f, "{DELETE_CHAR}")?;
        }
        if self.contains(AclMode::USAGE) {
            write!(f, "{USAGE_CHAR}")?;
        }
        if self.contains(AclMode::CREATE) {
            write!(f, "{CREATE_CHAR}")?;
        }
        if self.contains(AclMode::CREATE_ROLE) {
            write!(f, "{CREATE_ROLE_CHAR}")?;
        }
        if self.contains(AclMode::CREATE_DB) {
            write!(f, "{CREATE_DB_CHAR}")?;
        }
        if self.contains(AclMode::CREATE_CLUSTER) {
            write!(f, "{CREATE_CLUSTER_CHAR}")?;
        }
        if self.contains(AclMode::CREATE_NETWORK_POLICY) {
            write!(f, "{CREATE_NETWORK_POLICY_CHAR}")?;
        }
        Ok(())
    }
}

impl PropArbitrary for AclMode {
    type Parameters = ();
    type Strategy = BoxedStrategy<AclMode>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        proptest::bits::BitSetStrategy::masked(AclMode::all().bits)
            .prop_map(|bits| AclMode::from_bits(bits).expect("invalid proptest implementation"))
            .boxed()
    }
}

/// A list of privileges granted to a role in Materialize.
///
/// This is modelled after the AclItem type in PostgreSQL, but the OIDs are replaced with RoleIds
/// because we don't use OID as persistent identifiers for roles.
///
/// See: <https://github.com/postgres/postgres/blob/7f5b19817eaf38e70ad1153db4e644ee9456853e/src/include/utils/acl.h#L48-L59>
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Hash, Deserialize, Arbitrary,
)]
pub struct MzAclItem {
    /// Role that this item grants privileges to.
    pub grantee: RoleId,
    /// Grantor of privileges.
    pub grantor: RoleId,
    /// Privileges bit flag.
    pub acl_mode: AclMode,
}

impl MzAclItem {
    pub fn empty(grantee: RoleId, grantor: RoleId) -> MzAclItem {
        MzAclItem {
            grantee,
            grantor,
            acl_mode: AclMode::empty(),
        }
    }

    pub fn encode_binary(&self) -> Vec<u8> {
        let mut res = Vec::with_capacity(Self::binary_size());
        res.extend_from_slice(&self.grantee.encode_binary());
        res.extend_from_slice(&self.grantor.encode_binary());
        res.extend_from_slice(&self.acl_mode.bits().to_le_bytes());
        res
    }

    pub fn decode_binary(raw: &[u8]) -> Result<MzAclItem, Error> {
        if raw.len() != MzAclItem::binary_size() {
            return Err(anyhow!(
                "invalid binary size, expecting {}, found {}",
                MzAclItem::binary_size(),
                raw.len()
            ));
        }

        let role_id_size = RoleId::binary_size();

        let grantee = RoleId::decode_binary(&raw[0..role_id_size])?;
        let raw = &raw[role_id_size..];
        let grantor = RoleId::decode_binary(&raw[0..role_id_size])?;
        let raw = &raw[role_id_size..];
        let acl_mode = u64::from_le_bytes(raw.try_into()?);

        Ok(MzAclItem {
            grantee,
            grantor,
            acl_mode: AclMode { bits: acl_mode },
        })
    }

    pub const fn binary_size() -> usize {
        RoleId::binary_size() + RoleId::binary_size() + size_of::<u64>()
    }
}

impl FromStr for MzAclItem {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<_> = s.split('=').collect();
        let &[grantee, rest] = parts.as_slice() else {
            return Err(anyhow!("invalid mz_aclitem '{s}'"));
        };

        let parts: Vec<_> = rest.split('/').collect();
        let &[acl_mode, grantor] = parts.as_slice() else {
            return Err(anyhow!("invalid mz_aclitem '{s}'"));
        };

        let grantee: RoleId = if grantee.is_empty() {
            RoleId::Public
        } else {
            grantee.parse()?
        };
        let acl_mode: AclMode = acl_mode.parse()?;
        let grantor: RoleId = grantor.parse()?;

        Ok(MzAclItem {
            grantee,
            grantor,
            acl_mode,
        })
    }
}

impl fmt::Display for MzAclItem {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if !self.grantee.is_public() {
            write!(f, "{}", self.grantee)?;
        }
        write!(f, "={}/{}", self.acl_mode, self.grantor)
    }
}
