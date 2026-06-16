// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! In-memory catalog state-update types.
//!
//! These model a single update to the catalog state as it flows through the
//! catalog pipeline. They live here, rather than in `mz-catalog`'s `memory`
//! module, so that `mz-catalog-durable` can produce them without depending on
//! `mz-catalog`.

use std::collections::BTreeMap;

use mz_adapter_types::connection::ConnectionId;
use mz_repr::adt::mz_acl_item::MzAclItem;
use mz_repr::role_id::RoleId;
use mz_repr::{CatalogItemId, Diff, GlobalId, RelationVersion, Timestamp};
use mz_sql_types::catalog::CatalogItemType;
use mz_sql_types::names::SchemaId;

use crate::objects;
use crate::objects::item_type;

/// A single update to the catalog state.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct StateUpdate {
    pub kind: StateUpdateKind,
    pub ts: Timestamp,
    pub diff: StateDiff,
}

/// The contents of a single state update.
///
/// Variants are listed in dependency order.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum StateUpdateKind {
    Role(objects::Role),
    RoleAuth(objects::RoleAuth),
    Database(objects::Database),
    Schema(objects::Schema),
    DefaultPrivilege(objects::DefaultPrivilege),
    SystemPrivilege(MzAclItem),
    SystemConfiguration(objects::SystemConfiguration),
    Cluster(objects::Cluster),
    NetworkPolicy(objects::NetworkPolicy),
    IntrospectionSourceIndex(objects::IntrospectionSourceIndex),
    ClusterReplica(objects::ClusterReplica),
    SourceReferences(objects::SourceReferences),
    SystemObjectMapping(objects::SystemObjectMapping),
    // Temporary items are not actually updated via the durable catalog, but
    // this allows us to model them the same way as all other items in parts of
    // the pipeline.
    TemporaryItem(TemporaryItem),
    Item(objects::Item),
    Comment(objects::Comment),
    AuditLog(objects::AuditLog),
    // Storage updates.
    StorageCollectionMetadata(objects::StorageCollectionMetadata),
    UnfinalizedShard(objects::UnfinalizedShard),
}

/// Valid diffs for catalog state updates.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub enum StateDiff {
    Retraction,
    Addition,
}

impl From<StateDiff> for Diff {
    fn from(diff: StateDiff) -> Self {
        match diff {
            StateDiff::Retraction => Diff::MINUS_ONE,
            StateDiff::Addition => Diff::ONE,
        }
    }
}
impl TryFrom<Diff> for StateDiff {
    type Error = String;

    fn try_from(diff: Diff) -> Result<Self, Self::Error> {
        match diff {
            Diff::MINUS_ONE => Ok(Self::Retraction),
            Diff::ONE => Ok(Self::Addition),
            diff => Err(format!("invalid diff {diff}")),
        }
    }
}

/// Information needed to process an update to a temporary item.
#[derive(Debug, Clone, Ord, PartialOrd, PartialEq, Eq)]
pub struct TemporaryItem {
    pub id: CatalogItemId,
    pub oid: u32,
    pub global_id: GlobalId,
    pub schema_id: SchemaId,
    pub name: String,
    pub conn_id: Option<ConnectionId>,
    pub create_sql: String,
    pub owner_id: RoleId,
    pub privileges: Vec<MzAclItem>,
    pub extra_versions: BTreeMap<RelationVersion, GlobalId>,
}

impl TemporaryItem {
    pub fn item_type(&self) -> CatalogItemType {
        item_type(&self.create_sql)
    }
}

/// The same as [`StateUpdateKind`], but without `TemporaryItem` so we can derive [`Ord`].
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub enum BootstrapStateUpdateKind {
    Role(objects::Role),
    RoleAuth(objects::RoleAuth),
    Database(objects::Database),
    Schema(objects::Schema),
    DefaultPrivilege(objects::DefaultPrivilege),
    SystemPrivilege(MzAclItem),
    SystemConfiguration(objects::SystemConfiguration),
    Cluster(objects::Cluster),
    NetworkPolicy(objects::NetworkPolicy),
    IntrospectionSourceIndex(objects::IntrospectionSourceIndex),
    ClusterReplica(objects::ClusterReplica),
    SourceReferences(objects::SourceReferences),
    SystemObjectMapping(objects::SystemObjectMapping),
    Item(objects::Item),
    Comment(objects::Comment),
    AuditLog(objects::AuditLog),
    // Storage updates.
    StorageCollectionMetadata(objects::StorageCollectionMetadata),
    UnfinalizedShard(objects::UnfinalizedShard),
}

impl From<BootstrapStateUpdateKind> for StateUpdateKind {
    fn from(value: BootstrapStateUpdateKind) -> Self {
        match value {
            BootstrapStateUpdateKind::Role(kind) => StateUpdateKind::Role(kind),
            BootstrapStateUpdateKind::RoleAuth(kind) => StateUpdateKind::RoleAuth(kind),
            BootstrapStateUpdateKind::Database(kind) => StateUpdateKind::Database(kind),
            BootstrapStateUpdateKind::Schema(kind) => StateUpdateKind::Schema(kind),
            BootstrapStateUpdateKind::DefaultPrivilege(kind) => {
                StateUpdateKind::DefaultPrivilege(kind)
            }
            BootstrapStateUpdateKind::SystemPrivilege(kind) => {
                StateUpdateKind::SystemPrivilege(kind)
            }
            BootstrapStateUpdateKind::SystemConfiguration(kind) => {
                StateUpdateKind::SystemConfiguration(kind)
            }
            BootstrapStateUpdateKind::SourceReferences(kind) => {
                StateUpdateKind::SourceReferences(kind)
            }
            BootstrapStateUpdateKind::Cluster(kind) => StateUpdateKind::Cluster(kind),
            BootstrapStateUpdateKind::NetworkPolicy(kind) => StateUpdateKind::NetworkPolicy(kind),
            BootstrapStateUpdateKind::IntrospectionSourceIndex(kind) => {
                StateUpdateKind::IntrospectionSourceIndex(kind)
            }
            BootstrapStateUpdateKind::ClusterReplica(kind) => StateUpdateKind::ClusterReplica(kind),
            BootstrapStateUpdateKind::SystemObjectMapping(kind) => {
                StateUpdateKind::SystemObjectMapping(kind)
            }
            BootstrapStateUpdateKind::Item(kind) => StateUpdateKind::Item(kind),
            BootstrapStateUpdateKind::Comment(kind) => StateUpdateKind::Comment(kind),
            BootstrapStateUpdateKind::AuditLog(kind) => StateUpdateKind::AuditLog(kind),
            BootstrapStateUpdateKind::StorageCollectionMetadata(kind) => {
                StateUpdateKind::StorageCollectionMetadata(kind)
            }
            BootstrapStateUpdateKind::UnfinalizedShard(kind) => {
                StateUpdateKind::UnfinalizedShard(kind)
            }
        }
    }
}

impl TryFrom<StateUpdateKind> for BootstrapStateUpdateKind {
    type Error = TemporaryItem;

    fn try_from(value: StateUpdateKind) -> Result<Self, Self::Error> {
        match value {
            StateUpdateKind::Role(kind) => Ok(BootstrapStateUpdateKind::Role(kind)),
            StateUpdateKind::RoleAuth(kind) => Ok(BootstrapStateUpdateKind::RoleAuth(kind)),
            StateUpdateKind::Database(kind) => Ok(BootstrapStateUpdateKind::Database(kind)),
            StateUpdateKind::Schema(kind) => Ok(BootstrapStateUpdateKind::Schema(kind)),
            StateUpdateKind::DefaultPrivilege(kind) => {
                Ok(BootstrapStateUpdateKind::DefaultPrivilege(kind))
            }
            StateUpdateKind::SystemPrivilege(kind) => {
                Ok(BootstrapStateUpdateKind::SystemPrivilege(kind))
            }
            StateUpdateKind::SystemConfiguration(kind) => {
                Ok(BootstrapStateUpdateKind::SystemConfiguration(kind))
            }
            StateUpdateKind::Cluster(kind) => Ok(BootstrapStateUpdateKind::Cluster(kind)),
            StateUpdateKind::NetworkPolicy(kind) => {
                Ok(BootstrapStateUpdateKind::NetworkPolicy(kind))
            }
            StateUpdateKind::IntrospectionSourceIndex(kind) => {
                Ok(BootstrapStateUpdateKind::IntrospectionSourceIndex(kind))
            }
            StateUpdateKind::ClusterReplica(kind) => {
                Ok(BootstrapStateUpdateKind::ClusterReplica(kind))
            }
            StateUpdateKind::SourceReferences(kind) => {
                Ok(BootstrapStateUpdateKind::SourceReferences(kind))
            }
            StateUpdateKind::SystemObjectMapping(kind) => {
                Ok(BootstrapStateUpdateKind::SystemObjectMapping(kind))
            }
            StateUpdateKind::TemporaryItem(kind) => Err(kind),
            StateUpdateKind::Item(kind) => Ok(BootstrapStateUpdateKind::Item(kind)),
            StateUpdateKind::Comment(kind) => Ok(BootstrapStateUpdateKind::Comment(kind)),
            StateUpdateKind::AuditLog(kind) => Ok(BootstrapStateUpdateKind::AuditLog(kind)),
            StateUpdateKind::StorageCollectionMetadata(kind) => {
                Ok(BootstrapStateUpdateKind::StorageCollectionMetadata(kind))
            }
            StateUpdateKind::UnfinalizedShard(kind) => {
                Ok(BootstrapStateUpdateKind::UnfinalizedShard(kind))
            }
        }
    }
}
