// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Structured name leaf types for SQL objects.

use std::fmt;
use std::str::FromStr;
use std::sync::LazyLock;

use mz_cluster_types::{ReplicaId, StorageInstanceId as ClusterId};
use mz_repr::CatalogItemId;
use mz_repr::network_policy_id::NetworkPolicyId;
use mz_repr::role_id::RoleId;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use uncased::UncasedStr;

use crate::ParseError;

/// An id of a database.
#[derive(
    Debug,
    Clone,
    Copy,
    Eq,
    PartialEq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Arbitrary
)]
pub enum ResolvedDatabaseSpecifier {
    /// The "ambient" database, which is always present and is not named
    /// explicitly, but by omission.
    Ambient,
    /// A normal database with a name.
    Id(DatabaseId),
}

impl ResolvedDatabaseSpecifier {
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

/*
 * TODO(jkosh44) It's possible that in order to fix
 * https://github.com/MaterializeInc/database-issues/issues/2689 we will need to assign temporary
 * schemas unique Ids. If/when that happens we can remove this enum and refer to all schemas by
 * their Id.
 */
/// An id of a schema.
#[derive(
    Debug,
    Clone,
    Copy,
    Eq,
    PartialEq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize
)]
pub enum SchemaSpecifier {
    /// A temporary schema
    Temporary,
    /// A normal database with a name.
    Id(SchemaId),
}

impl SchemaSpecifier {
    const TEMPORARY_SCHEMA_ID: u64 = 0;

    pub fn is_system(&self) -> bool {
        match self {
            SchemaSpecifier::Temporary => false,
            SchemaSpecifier::Id(id) => id.is_system(),
        }
    }

    pub fn is_user(&self) -> bool {
        match self {
            SchemaSpecifier::Temporary => true,
            SchemaSpecifier::Id(id) => id.is_user(),
        }
    }

    pub fn is_temporary(&self) -> bool {
        matches!(self, SchemaSpecifier::Temporary)
    }
}

impl fmt::Display for SchemaSpecifier {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Temporary => f.write_str(format!("{}", Self::TEMPORARY_SCHEMA_ID).as_str()),
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
            SchemaSpecifier::Id(id) => id.clone(),
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

/// The identifier for a schema.
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
    Deserialize,
    Arbitrary
)]
pub enum SchemaId {
    User(u64),
    System(u64),
}

impl SchemaId {
    pub fn is_user(&self) -> bool {
        matches!(self, SchemaId::User(_))
    }

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
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err = || ParseError::Unstructured(format!("couldn't parse SchemaId {}", s));
        // Validate the (single-byte, ASCII) tag before slicing so that a
        // multi-byte leading character doesn't slice inside a UTF-8 boundary.
        let variant = match s.chars().next() {
            Some('s') => SchemaId::System,
            Some('u') => SchemaId::User,
            _ => return Err(err()),
        };
        let val: u64 = s[1..].parse().map_err(|_| err())?;
        Ok(variant(val))
    }
}

/// The identifier for a database.
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
    Deserialize,
    Arbitrary
)]
pub enum DatabaseId {
    User(u64),
    System(u64),
}

impl DatabaseId {
    pub fn is_user(&self) -> bool {
        matches!(self, DatabaseId::User(_))
    }

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
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err = || ParseError::Unstructured(format!("couldn't parse DatabaseId {}", s));
        // Validate the (single-byte, ASCII) tag before slicing so that a
        // multi-byte leading character doesn't slice inside a UTF-8 boundary.
        let variant = match s.chars().next() {
            Some('s') => DatabaseId::System,
            Some('u') => DatabaseId::User,
            _ => return Err(err()),
        };
        let val: u64 = s[1..].parse().map_err(|_| err())?;
        Ok(variant(val))
    }
}

/// Comments can be applied to multiple kinds of objects (e.g. Tables and Role), so we need a way
/// to represent these different types and their IDs (e.g. [`CatalogItemId`] and [`RoleId`]), as
/// well as the inner kind of object that is represented, e.g. [`CatalogItemId`] is used to
/// identify both Tables and Views. No other kind of ID encapsulates all of this, hence this new
/// "*Id" type.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
pub enum CommentObjectId {
    Table(CatalogItemId),
    View(CatalogItemId),
    MaterializedView(CatalogItemId),
    Source(CatalogItemId),
    Sink(CatalogItemId),
    Index(CatalogItemId),
    Func(CatalogItemId),
    Connection(CatalogItemId),
    Type(CatalogItemId),
    Secret(CatalogItemId),
    Role(RoleId),
    Database(DatabaseId),
    Schema((ResolvedDatabaseSpecifier, SchemaSpecifier)),
    Cluster(ClusterId),
    ClusterReplica((ClusterId, ReplicaId)),
    NetworkPolicy(NetworkPolicyId),
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct ItemQualifiers {
    pub database_spec: ResolvedDatabaseSpecifier,
    pub schema_spec: SchemaSpecifier,
}

pub static PUBLIC_ROLE_NAME: LazyLock<&UncasedStr> = LazyLock::new(|| UncasedStr::new("PUBLIC"));

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ObjectId {
    Cluster(ClusterId),
    ClusterReplica((ClusterId, ReplicaId)),
    Database(DatabaseId),
    Schema((ResolvedDatabaseSpecifier, SchemaSpecifier)),
    Role(RoleId),
    Item(CatalogItemId),
    NetworkPolicy(NetworkPolicyId),
}

impl ObjectId {
    pub fn unwrap_cluster_id(self) -> ClusterId {
        match self {
            ObjectId::Cluster(id) => id,
            _ => panic!("ObjectId::unwrap_cluster_id called on {self:?}"),
        }
    }
    pub fn unwrap_cluster_replica_id(self) -> (ClusterId, ReplicaId) {
        match self {
            ObjectId::ClusterReplica(id) => id,
            _ => panic!("ObjectId::unwrap_cluster_replica_id called on {self:?}"),
        }
    }
    pub fn unwrap_database_id(self) -> DatabaseId {
        match self {
            ObjectId::Database(id) => id,
            _ => panic!("ObjectId::unwrap_database_id called on {self:?}"),
        }
    }
    pub fn unwrap_schema_id(self) -> (ResolvedDatabaseSpecifier, SchemaSpecifier) {
        match self {
            ObjectId::Schema(id) => id,
            _ => panic!("ObjectId::unwrap_schema_id called on {self:?}"),
        }
    }
    pub fn unwrap_role_id(self) -> RoleId {
        match self {
            ObjectId::Role(id) => id,
            _ => panic!("ObjectId::unwrap_role_id called on {self:?}"),
        }
    }
    pub fn unwrap_item_id(self) -> CatalogItemId {
        match self {
            ObjectId::Item(id) => id,
            _ => panic!("ObjectId::unwrap_item_id called on {self:?}"),
        }
    }

    pub fn is_system(&self) -> bool {
        match self {
            ObjectId::Cluster(cluster_id) => cluster_id.is_system(),
            ObjectId::ClusterReplica((_cluster_id, replica_id)) => replica_id.is_system(),
            ObjectId::Database(database_id) => database_id.is_system(),
            ObjectId::Schema((_database_id, schema_id)) => schema_id.is_system(),
            ObjectId::Role(role_id) => role_id.is_system(),
            ObjectId::Item(global_id) => global_id.is_system(),
            ObjectId::NetworkPolicy(network_policy_id) => network_policy_id.is_system(),
        }
    }

    pub fn is_user(&self) -> bool {
        match self {
            ObjectId::Cluster(cluster_id) => cluster_id.is_user(),
            ObjectId::ClusterReplica((_cluster_id, replica_id)) => replica_id.is_user(),
            ObjectId::Database(database_id) => database_id.is_user(),
            ObjectId::Schema((_database_id, schema_id)) => schema_id.is_user(),
            ObjectId::Role(role_id) => role_id.is_user(),
            ObjectId::Item(global_id) => global_id.is_user(),
            ObjectId::NetworkPolicy(network_policy_id) => network_policy_id.is_user(),
        }
    }
}

impl fmt::Display for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ObjectId::Cluster(cluster_id) => write!(f, "C{cluster_id}"),
            ObjectId::ClusterReplica((cluster_id, replica_id)) => {
                write!(f, "CR{cluster_id}.{replica_id}")
            }
            ObjectId::Database(database_id) => write!(f, "D{database_id}"),
            ObjectId::Schema((database_spec, schema_spec)) => {
                let database_id = match database_spec {
                    ResolvedDatabaseSpecifier::Ambient => "".to_string(),
                    ResolvedDatabaseSpecifier::Id(database_id) => format!("{database_id}."),
                };
                write!(f, "S{database_id}{schema_spec}")
            }
            ObjectId::Role(role_id) => write!(f, "R{role_id}"),
            ObjectId::Item(item_id) => write!(f, "I{item_id}"),
            ObjectId::NetworkPolicy(network_policy_id) => write!(f, "NP{network_policy_id}"),
        }
    }
}

impl From<ClusterId> for ObjectId {
    fn from(id: ClusterId) -> Self {
        ObjectId::Cluster(id)
    }
}

impl From<&ClusterId> for ObjectId {
    fn from(id: &ClusterId) -> Self {
        ObjectId::Cluster(*id)
    }
}

impl From<(ClusterId, ReplicaId)> for ObjectId {
    fn from(id: (ClusterId, ReplicaId)) -> Self {
        ObjectId::ClusterReplica(id)
    }
}

impl From<&(ClusterId, ReplicaId)> for ObjectId {
    fn from(id: &(ClusterId, ReplicaId)) -> Self {
        ObjectId::ClusterReplica(*id)
    }
}

impl From<DatabaseId> for ObjectId {
    fn from(id: DatabaseId) -> Self {
        ObjectId::Database(id)
    }
}

impl From<&DatabaseId> for ObjectId {
    fn from(id: &DatabaseId) -> Self {
        ObjectId::Database(*id)
    }
}

impl From<ItemQualifiers> for ObjectId {
    fn from(qualifiers: ItemQualifiers) -> Self {
        ObjectId::Schema((qualifiers.database_spec, qualifiers.schema_spec))
    }
}

impl From<&ItemQualifiers> for ObjectId {
    fn from(qualifiers: &ItemQualifiers) -> Self {
        ObjectId::Schema((qualifiers.database_spec, qualifiers.schema_spec))
    }
}

impl From<(ResolvedDatabaseSpecifier, SchemaSpecifier)> for ObjectId {
    fn from(id: (ResolvedDatabaseSpecifier, SchemaSpecifier)) -> Self {
        ObjectId::Schema(id)
    }
}

impl From<&(ResolvedDatabaseSpecifier, SchemaSpecifier)> for ObjectId {
    fn from(id: &(ResolvedDatabaseSpecifier, SchemaSpecifier)) -> Self {
        ObjectId::Schema(*id)
    }
}

impl From<RoleId> for ObjectId {
    fn from(id: RoleId) -> Self {
        ObjectId::Role(id)
    }
}

impl From<&RoleId> for ObjectId {
    fn from(id: &RoleId) -> Self {
        ObjectId::Role(*id)
    }
}

impl From<CatalogItemId> for ObjectId {
    fn from(id: CatalogItemId) -> Self {
        ObjectId::Item(id)
    }
}

impl From<&CatalogItemId> for ObjectId {
    fn from(id: &CatalogItemId) -> Self {
        ObjectId::Item(*id)
    }
}

impl From<CommentObjectId> for ObjectId {
    fn from(id: CommentObjectId) -> Self {
        match id {
            CommentObjectId::Table(item_id)
            | CommentObjectId::View(item_id)
            | CommentObjectId::MaterializedView(item_id)
            | CommentObjectId::Source(item_id)
            | CommentObjectId::Sink(item_id)
            | CommentObjectId::Index(item_id)
            | CommentObjectId::Func(item_id)
            | CommentObjectId::Connection(item_id)
            | CommentObjectId::Type(item_id)
            | CommentObjectId::Secret(item_id) => ObjectId::Item(item_id),
            CommentObjectId::Role(id) => ObjectId::Role(id),
            CommentObjectId::Database(id) => ObjectId::Database(id),
            CommentObjectId::Schema(id) => ObjectId::Schema(id),
            CommentObjectId::Cluster(id) => ObjectId::Cluster(id),
            CommentObjectId::ClusterReplica(id) => ObjectId::ClusterReplica(id),
            CommentObjectId::NetworkPolicy(id) => ObjectId::NetworkPolicy(id),
        }
    }
}
