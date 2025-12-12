// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module is responsible for serializing objects from crates other than
//! `mz_catalog` into protobuf.
//!
//! The reason this module doesn't exist in the `mz_catalog` crate itself is
//! because of Rust's orphan rules.

use std::time::Duration;

use mz_compute_types::config::ComputeReplicaLogging;
use mz_controller_types::ReplicaId;
use mz_proto::{ProtoMapEntry, ProtoType, RustType, TryFromProtoError};
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem};
use mz_repr::network_policy_id::NetworkPolicyId;
use mz_repr::role_id::RoleId;
use mz_repr::{CatalogItemId, GlobalId, RelationVersion, Timestamp};
use mz_sql::catalog::{CatalogItemType, ObjectType, RoleAttributes, RoleMembership, RoleVars};
use mz_sql::names::{
    CommentObjectId, DatabaseId, ResolvedDatabaseSpecifier, SchemaId, SchemaSpecifier,
};
use mz_sql::plan::{
    ClusterSchedule, NetworkPolicyRule, NetworkPolicyRuleAction, NetworkPolicyRuleDirection,
    PolicyAddress,
};
use mz_sql::session::vars::OwnedVarInput;
use mz_storage_types::instances::StorageInstanceId;

impl From<String> for crate::objects::StringWrapper {
    fn from(value: String) -> Self {
        crate::objects::StringWrapper { inner: value }
    }
}

impl RustType<crate::objects::Duration> for Duration {
    fn into_proto(&self) -> crate::objects::Duration {
        crate::objects::Duration {
            secs: self.as_secs(),
            nanos: self.subsec_nanos(),
        }
    }

    fn from_proto(proto: crate::objects::Duration) -> Result<Self, TryFromProtoError> {
        Ok(Duration::new(proto.secs, proto.nanos))
    }
}

impl RustType<crate::objects::RoleId> for RoleId {
    fn into_proto(&self) -> crate::objects::RoleId {
        match self {
            RoleId::User(id) => crate::objects::RoleId::User(*id),
            RoleId::System(id) => crate::objects::RoleId::System(*id),
            RoleId::Predefined(id) => crate::objects::RoleId::Predefined(*id),
            RoleId::Public => crate::objects::RoleId::Public,
        }
    }

    fn from_proto(proto: crate::objects::RoleId) -> Result<Self, TryFromProtoError> {
        let id = match proto {
            crate::objects::RoleId::User(id) => RoleId::User(id),
            crate::objects::RoleId::System(id) => RoleId::System(id),
            crate::objects::RoleId::Predefined(id) => RoleId::Predefined(id),
            crate::objects::RoleId::Public => RoleId::Public,
        };
        Ok(id)
    }
}

impl RustType<crate::objects::AclMode> for AclMode {
    fn into_proto(&self) -> crate::objects::AclMode {
        crate::objects::AclMode {
            bitflags: self.bits(),
        }
    }

    fn from_proto(proto: crate::objects::AclMode) -> Result<Self, TryFromProtoError> {
        AclMode::from_bits(proto.bitflags).ok_or_else(|| {
            TryFromProtoError::InvalidBitFlags(format!("Invalid AclMode from catalog {proto:?}"))
        })
    }
}

impl RustType<crate::objects::MzAclItem> for MzAclItem {
    fn into_proto(&self) -> crate::objects::MzAclItem {
        crate::objects::MzAclItem {
            grantee: self.grantee.into_proto(),
            grantor: self.grantor.into_proto(),
            acl_mode: self.acl_mode.into_proto(),
        }
    }

    fn from_proto(proto: crate::objects::MzAclItem) -> Result<Self, TryFromProtoError> {
        Ok(MzAclItem {
            grantee: proto.grantee.into_rust()?,
            grantor: proto.grantor.into_rust()?,
            acl_mode: proto.acl_mode.into_rust()?,
        })
    }
}

impl RustType<crate::objects::RoleAttributes> for RoleAttributes {
    fn into_proto(&self) -> crate::objects::RoleAttributes {
        crate::objects::RoleAttributes {
            inherit: self.inherit,
            superuser: self.superuser,
            login: self.login,
        }
    }

    fn from_proto(proto: crate::objects::RoleAttributes) -> Result<Self, TryFromProtoError> {
        let mut attributes = RoleAttributes::new();

        attributes.inherit = proto.inherit;
        attributes.superuser = proto.superuser;
        attributes.login = proto.login;

        Ok(attributes)
    }
}

impl RustType<crate::objects::RoleVar> for OwnedVarInput {
    fn into_proto(&self) -> crate::objects::RoleVar {
        match self.clone() {
            OwnedVarInput::Flat(v) => crate::objects::RoleVar::Flat(v),
            OwnedVarInput::SqlSet(entries) => crate::objects::RoleVar::SqlSet(entries),
        }
    }

    fn from_proto(proto: crate::objects::RoleVar) -> Result<Self, TryFromProtoError> {
        let result = match proto {
            crate::objects::RoleVar::Flat(v) => OwnedVarInput::Flat(v),
            crate::objects::RoleVar::SqlSet(entries) => OwnedVarInput::SqlSet(entries),
        };
        Ok(result)
    }
}

impl RustType<crate::objects::RoleVars> for RoleVars {
    fn into_proto(&self) -> crate::objects::RoleVars {
        let entries = self
            .map
            .clone()
            .into_iter()
            .map(|(key, val)| crate::objects::RoleVarsEntry {
                key,
                val: val.into_proto(),
            })
            .collect();

        crate::objects::RoleVars { entries }
    }

    fn from_proto(proto: crate::objects::RoleVars) -> Result<Self, TryFromProtoError> {
        let map = proto
            .entries
            .into_iter()
            .map(|entry| {
                let val = entry.val.into_rust()?;
                Ok::<_, TryFromProtoError>((entry.key, val))
            })
            .collect::<Result<_, _>>()?;

        Ok(RoleVars { map })
    }
}

impl RustType<crate::objects::NetworkPolicyId> for NetworkPolicyId {
    fn into_proto(&self) -> crate::objects::NetworkPolicyId {
        match self {
            NetworkPolicyId::User(id) => crate::objects::NetworkPolicyId::User(*id),
            NetworkPolicyId::System(id) => crate::objects::NetworkPolicyId::System(*id),
        }
    }

    fn from_proto(proto: crate::objects::NetworkPolicyId) -> Result<Self, TryFromProtoError> {
        let id = match proto {
            crate::objects::NetworkPolicyId::User(id) => NetworkPolicyId::User(id),
            crate::objects::NetworkPolicyId::System(id) => NetworkPolicyId::System(id),
        };
        Ok(id)
    }
}

impl RustType<crate::objects::CatalogItemType> for CatalogItemType {
    fn into_proto(&self) -> crate::objects::CatalogItemType {
        match self {
            CatalogItemType::Table => crate::objects::CatalogItemType::Table,
            CatalogItemType::Source => crate::objects::CatalogItemType::Source,
            CatalogItemType::Sink => crate::objects::CatalogItemType::Sink,
            CatalogItemType::View => crate::objects::CatalogItemType::View,
            CatalogItemType::MaterializedView => crate::objects::CatalogItemType::MaterializedView,
            CatalogItemType::Index => crate::objects::CatalogItemType::Index,
            CatalogItemType::Type => crate::objects::CatalogItemType::Type,
            CatalogItemType::Func => crate::objects::CatalogItemType::Func,
            CatalogItemType::Secret => crate::objects::CatalogItemType::Secret,
            CatalogItemType::Connection => crate::objects::CatalogItemType::Connection,
            CatalogItemType::ContinualTask => crate::objects::CatalogItemType::ContinualTask,
        }
    }

    fn from_proto(proto: crate::objects::CatalogItemType) -> Result<Self, TryFromProtoError> {
        let item_type = match proto {
            crate::objects::CatalogItemType::Table => CatalogItemType::Table,
            crate::objects::CatalogItemType::Source => CatalogItemType::Source,
            crate::objects::CatalogItemType::Sink => CatalogItemType::Sink,
            crate::objects::CatalogItemType::View => CatalogItemType::View,
            crate::objects::CatalogItemType::MaterializedView => CatalogItemType::MaterializedView,
            crate::objects::CatalogItemType::Index => CatalogItemType::Index,
            crate::objects::CatalogItemType::Type => CatalogItemType::Type,
            crate::objects::CatalogItemType::Func => CatalogItemType::Func,
            crate::objects::CatalogItemType::Secret => CatalogItemType::Secret,
            crate::objects::CatalogItemType::Connection => CatalogItemType::Connection,
            crate::objects::CatalogItemType::ContinualTask => CatalogItemType::ContinualTask,
            crate::objects::CatalogItemType::Unknown => {
                return Err(TryFromProtoError::unknown_enum_variant("CatalogItemType"));
            }
        };
        Ok(item_type)
    }
}

impl RustType<crate::objects::ObjectType> for ObjectType {
    fn into_proto(&self) -> crate::objects::ObjectType {
        match self {
            ObjectType::Table => crate::objects::ObjectType::Table,
            ObjectType::View => crate::objects::ObjectType::View,
            ObjectType::MaterializedView => crate::objects::ObjectType::MaterializedView,
            ObjectType::Source => crate::objects::ObjectType::Source,
            ObjectType::Sink => crate::objects::ObjectType::Sink,
            ObjectType::Index => crate::objects::ObjectType::Index,
            ObjectType::Type => crate::objects::ObjectType::Type,
            ObjectType::Role => crate::objects::ObjectType::Role,
            ObjectType::Cluster => crate::objects::ObjectType::Cluster,
            ObjectType::ClusterReplica => crate::objects::ObjectType::ClusterReplica,
            ObjectType::Secret => crate::objects::ObjectType::Secret,
            ObjectType::Connection => crate::objects::ObjectType::Connection,
            ObjectType::Database => crate::objects::ObjectType::Database,
            ObjectType::Schema => crate::objects::ObjectType::Schema,
            ObjectType::Func => crate::objects::ObjectType::Func,
            ObjectType::ContinualTask => crate::objects::ObjectType::ContinualTask,
            ObjectType::NetworkPolicy => crate::objects::ObjectType::NetworkPolicy,
        }
    }

    fn from_proto(proto: crate::objects::ObjectType) -> Result<Self, TryFromProtoError> {
        match proto {
            crate::objects::ObjectType::Table => Ok(ObjectType::Table),
            crate::objects::ObjectType::View => Ok(ObjectType::View),
            crate::objects::ObjectType::MaterializedView => Ok(ObjectType::MaterializedView),
            crate::objects::ObjectType::Source => Ok(ObjectType::Source),
            crate::objects::ObjectType::Sink => Ok(ObjectType::Sink),
            crate::objects::ObjectType::Index => Ok(ObjectType::Index),
            crate::objects::ObjectType::Type => Ok(ObjectType::Type),
            crate::objects::ObjectType::Role => Ok(ObjectType::Role),
            crate::objects::ObjectType::Cluster => Ok(ObjectType::Cluster),
            crate::objects::ObjectType::ClusterReplica => Ok(ObjectType::ClusterReplica),
            crate::objects::ObjectType::Secret => Ok(ObjectType::Secret),
            crate::objects::ObjectType::Connection => Ok(ObjectType::Connection),
            crate::objects::ObjectType::Database => Ok(ObjectType::Database),
            crate::objects::ObjectType::Schema => Ok(ObjectType::Schema),
            crate::objects::ObjectType::Func => Ok(ObjectType::Func),
            crate::objects::ObjectType::ContinualTask => Ok(ObjectType::ContinualTask),
            crate::objects::ObjectType::NetworkPolicy => Ok(ObjectType::NetworkPolicy),
            crate::objects::ObjectType::Unknown => Err(TryFromProtoError::unknown_enum_variant(
                "ObjectType::Unknown",
            )),
        }
    }
}

impl RustType<crate::objects::RoleMembership> for RoleMembership {
    fn into_proto(&self) -> crate::objects::RoleMembership {
        crate::objects::RoleMembership {
            map: self
                .map
                .iter()
                .map(|(key, val)| crate::objects::RoleMembershipEntry {
                    key: key.into_proto(),
                    value: val.into_proto(),
                })
                .collect(),
        }
    }

    fn from_proto(proto: crate::objects::RoleMembership) -> Result<Self, TryFromProtoError> {
        Ok(RoleMembership {
            map: proto
                .map
                .into_iter()
                .map(|e| {
                    let key = e.key.into_rust()?;
                    let val = e.value.into_rust()?;

                    Ok((key, val))
                })
                .collect::<Result<_, TryFromProtoError>>()?,
        })
    }
}

impl RustType<crate::objects::ResolvedDatabaseSpecifier> for ResolvedDatabaseSpecifier {
    fn into_proto(&self) -> crate::objects::ResolvedDatabaseSpecifier {
        match self {
            ResolvedDatabaseSpecifier::Ambient => {
                crate::objects::ResolvedDatabaseSpecifier::Ambient
            }
            ResolvedDatabaseSpecifier::Id(database_id) => {
                crate::objects::ResolvedDatabaseSpecifier::Id(database_id.into_proto())
            }
        }
    }

    fn from_proto(
        proto: crate::objects::ResolvedDatabaseSpecifier,
    ) -> Result<Self, TryFromProtoError> {
        let spec = match proto {
            crate::objects::ResolvedDatabaseSpecifier::Ambient => {
                ResolvedDatabaseSpecifier::Ambient
            }
            crate::objects::ResolvedDatabaseSpecifier::Id(database_id) => {
                ResolvedDatabaseSpecifier::Id(database_id.into_rust()?)
            }
        };
        Ok(spec)
    }
}

impl RustType<crate::objects::SchemaSpecifier> for SchemaSpecifier {
    fn into_proto(&self) -> crate::objects::SchemaSpecifier {
        match self {
            SchemaSpecifier::Temporary => crate::objects::SchemaSpecifier::Temporary,
            SchemaSpecifier::Id(schema_id) => {
                crate::objects::SchemaSpecifier::Id(schema_id.into_proto())
            }
        }
    }

    fn from_proto(proto: crate::objects::SchemaSpecifier) -> Result<Self, TryFromProtoError> {
        let spec = match proto {
            crate::objects::SchemaSpecifier::Temporary => SchemaSpecifier::Temporary,
            crate::objects::SchemaSpecifier::Id(schema_id) => {
                SchemaSpecifier::Id(schema_id.into_rust()?)
            }
        };
        Ok(spec)
    }
}

impl RustType<crate::objects::SchemaId> for SchemaId {
    fn into_proto(&self) -> crate::objects::SchemaId {
        match self {
            SchemaId::User(id) => crate::objects::SchemaId::User(*id),
            SchemaId::System(id) => crate::objects::SchemaId::System(*id),
        }
    }

    fn from_proto(proto: crate::objects::SchemaId) -> Result<Self, TryFromProtoError> {
        let id = match proto {
            crate::objects::SchemaId::User(id) => SchemaId::User(id),
            crate::objects::SchemaId::System(id) => SchemaId::System(id),
        };
        Ok(id)
    }
}

impl RustType<crate::objects::DatabaseId> for DatabaseId {
    fn into_proto(&self) -> crate::objects::DatabaseId {
        match self {
            DatabaseId::User(id) => crate::objects::DatabaseId::User(*id),
            DatabaseId::System(id) => crate::objects::DatabaseId::System(*id),
        }
    }

    fn from_proto(proto: crate::objects::DatabaseId) -> Result<Self, TryFromProtoError> {
        match proto {
            crate::objects::DatabaseId::User(id) => Ok(DatabaseId::User(id)),
            crate::objects::DatabaseId::System(id) => Ok(DatabaseId::System(id)),
        }
    }
}

impl RustType<crate::objects::CommentObject> for CommentObjectId {
    fn into_proto(&self) -> crate::objects::CommentObject {
        match self {
            CommentObjectId::Table(global_id) => {
                crate::objects::CommentObject::Table(global_id.into_proto())
            }
            CommentObjectId::View(global_id) => {
                crate::objects::CommentObject::View(global_id.into_proto())
            }
            CommentObjectId::MaterializedView(global_id) => {
                crate::objects::CommentObject::MaterializedView(global_id.into_proto())
            }
            CommentObjectId::Source(global_id) => {
                crate::objects::CommentObject::Source(global_id.into_proto())
            }
            CommentObjectId::Sink(global_id) => {
                crate::objects::CommentObject::Sink(global_id.into_proto())
            }
            CommentObjectId::Index(global_id) => {
                crate::objects::CommentObject::Index(global_id.into_proto())
            }
            CommentObjectId::Func(global_id) => {
                crate::objects::CommentObject::Func(global_id.into_proto())
            }
            CommentObjectId::Connection(global_id) => {
                crate::objects::CommentObject::Connection(global_id.into_proto())
            }
            CommentObjectId::Type(global_id) => {
                crate::objects::CommentObject::Type(global_id.into_proto())
            }
            CommentObjectId::Secret(global_id) => {
                crate::objects::CommentObject::Secret(global_id.into_proto())
            }
            CommentObjectId::Role(role_id) => {
                crate::objects::CommentObject::Role(role_id.into_proto())
            }
            CommentObjectId::Database(database_id) => {
                crate::objects::CommentObject::Database(database_id.into_proto())
            }
            CommentObjectId::ContinualTask(global_id) => {
                crate::objects::CommentObject::ContinualTask(global_id.into_proto())
            }
            CommentObjectId::NetworkPolicy(network_policy_id) => {
                crate::objects::CommentObject::NetworkPolicy(network_policy_id.into_proto())
            }
            CommentObjectId::Schema((database, schema)) => {
                crate::objects::CommentObject::Schema(crate::objects::ResolvedSchema {
                    database: database.into_proto(),
                    schema: schema.into_proto(),
                })
            }
            CommentObjectId::Cluster(cluster_id) => {
                crate::objects::CommentObject::Cluster(cluster_id.into_proto())
            }
            CommentObjectId::ClusterReplica((cluster_id, replica_id)) => {
                let cluster_replica_id = crate::objects::ClusterReplicaId {
                    cluster_id: cluster_id.into_proto(),
                    replica_id: replica_id.into_proto(),
                };
                crate::objects::CommentObject::ClusterReplica(cluster_replica_id)
            }
        }
    }

    fn from_proto(proto: crate::objects::CommentObject) -> Result<Self, TryFromProtoError> {
        let id = match proto {
            crate::objects::CommentObject::Table(item_id) => {
                CommentObjectId::Table(item_id.into_rust()?)
            }
            crate::objects::CommentObject::View(item_id) => {
                CommentObjectId::View(item_id.into_rust()?)
            }
            crate::objects::CommentObject::MaterializedView(item_id) => {
                CommentObjectId::MaterializedView(item_id.into_rust()?)
            }
            crate::objects::CommentObject::Source(item_id) => {
                CommentObjectId::Source(item_id.into_rust()?)
            }
            crate::objects::CommentObject::Sink(item_id) => {
                CommentObjectId::Sink(item_id.into_rust()?)
            }
            crate::objects::CommentObject::Index(item_id) => {
                CommentObjectId::Index(item_id.into_rust()?)
            }
            crate::objects::CommentObject::Func(item_id) => {
                CommentObjectId::Func(item_id.into_rust()?)
            }
            crate::objects::CommentObject::Connection(item_id) => {
                CommentObjectId::Connection(item_id.into_rust()?)
            }
            crate::objects::CommentObject::Type(item_id) => {
                CommentObjectId::Type(item_id.into_rust()?)
            }
            crate::objects::CommentObject::Secret(item_id) => {
                CommentObjectId::Secret(item_id.into_rust()?)
            }
            crate::objects::CommentObject::ContinualTask(item_id) => {
                CommentObjectId::ContinualTask(item_id.into_rust()?)
            }
            crate::objects::CommentObject::NetworkPolicy(global_id) => {
                CommentObjectId::NetworkPolicy(global_id.into_rust()?)
            }
            crate::objects::CommentObject::Role(role_id) => {
                CommentObjectId::Role(role_id.into_rust()?)
            }
            crate::objects::CommentObject::Database(database_id) => {
                CommentObjectId::Database(database_id.into_rust()?)
            }
            crate::objects::CommentObject::Schema(resolved_schema) => {
                let database = resolved_schema.database.into_rust()?;
                let schema = resolved_schema.schema.into_rust()?;
                CommentObjectId::Schema((database, schema))
            }
            crate::objects::CommentObject::Cluster(cluster_id) => {
                CommentObjectId::Cluster(cluster_id.into_rust()?)
            }
            crate::objects::CommentObject::ClusterReplica(cluster_replica_id) => {
                let cluster_id = cluster_replica_id.cluster_id.into_rust()?;
                let replica_id = cluster_replica_id.replica_id.into_rust()?;
                CommentObjectId::ClusterReplica((cluster_id, replica_id))
            }
        };
        Ok(id)
    }
}

impl RustType<crate::objects::EpochMillis> for u64 {
    fn into_proto(&self) -> crate::objects::EpochMillis {
        crate::objects::EpochMillis { millis: *self }
    }

    fn from_proto(proto: crate::objects::EpochMillis) -> Result<Self, TryFromProtoError> {
        Ok(proto.millis)
    }
}

impl RustType<crate::objects::Timestamp> for Timestamp {
    fn into_proto(&self) -> crate::objects::Timestamp {
        crate::objects::Timestamp {
            internal: self.into(),
        }
    }

    fn from_proto(proto: crate::objects::Timestamp) -> Result<Self, TryFromProtoError> {
        Ok(Timestamp::new(proto.internal))
    }
}

impl RustType<crate::objects::CatalogItemId> for CatalogItemId {
    fn into_proto(&self) -> crate::objects::CatalogItemId {
        match self {
            CatalogItemId::System(x) => crate::objects::CatalogItemId::System(*x),
            CatalogItemId::IntrospectionSourceIndex(x) => {
                crate::objects::CatalogItemId::IntrospectionSourceIndex(*x)
            }
            CatalogItemId::User(x) => crate::objects::CatalogItemId::User(*x),
            CatalogItemId::Transient(x) => crate::objects::CatalogItemId::Transient(*x),
        }
    }

    fn from_proto(proto: crate::objects::CatalogItemId) -> Result<Self, TryFromProtoError> {
        match proto {
            crate::objects::CatalogItemId::System(x) => Ok(CatalogItemId::System(x)),
            crate::objects::CatalogItemId::IntrospectionSourceIndex(x) => {
                Ok(CatalogItemId::IntrospectionSourceIndex(x))
            }
            crate::objects::CatalogItemId::User(x) => Ok(CatalogItemId::User(x)),
            crate::objects::CatalogItemId::Transient(x) => Ok(CatalogItemId::Transient(x)),
        }
    }
}

impl RustType<crate::objects::GlobalId> for GlobalId {
    fn into_proto(&self) -> crate::objects::GlobalId {
        match self {
            GlobalId::System(x) => crate::objects::GlobalId::System(*x),
            GlobalId::IntrospectionSourceIndex(x) => {
                crate::objects::GlobalId::IntrospectionSourceIndex(*x)
            }
            GlobalId::User(x) => crate::objects::GlobalId::User(*x),
            GlobalId::Transient(x) => crate::objects::GlobalId::Transient(*x),
            GlobalId::Explain => crate::objects::GlobalId::Explain,
        }
    }

    fn from_proto(proto: crate::objects::GlobalId) -> Result<Self, TryFromProtoError> {
        match proto {
            crate::objects::GlobalId::System(x) => Ok(GlobalId::System(x)),
            crate::objects::GlobalId::IntrospectionSourceIndex(x) => {
                Ok(GlobalId::IntrospectionSourceIndex(x))
            }
            crate::objects::GlobalId::User(x) => Ok(GlobalId::User(x)),
            crate::objects::GlobalId::Transient(x) => Ok(GlobalId::Transient(x)),
            crate::objects::GlobalId::Explain => Ok(GlobalId::Explain),
        }
    }
}

impl RustType<crate::objects::ClusterId> for StorageInstanceId {
    fn into_proto(&self) -> crate::objects::ClusterId {
        match self {
            StorageInstanceId::User(id) => crate::objects::ClusterId::User(*id),
            StorageInstanceId::System(id) => crate::objects::ClusterId::System(*id),
        }
    }

    fn from_proto(proto: crate::objects::ClusterId) -> Result<Self, TryFromProtoError> {
        let id = match proto {
            crate::objects::ClusterId::User(id) => {
                StorageInstanceId::user(id).ok_or_else(|| {
                    TryFromProtoError::InvalidPersistState(format!(
                        "{id} is not a valid StorageInstanceId"
                    ))
                })?
            }
            crate::objects::ClusterId::System(id) => {
                StorageInstanceId::system(id).ok_or_else(|| {
                    TryFromProtoError::InvalidPersistState(format!(
                        "{id} is not a valid StorageInstanceId"
                    ))
                })?
            }
        };
        Ok(id)
    }
}

impl RustType<crate::objects::ReplicaId> for ReplicaId {
    fn into_proto(&self) -> crate::objects::ReplicaId {
        match self {
            Self::System(id) => crate::objects::ReplicaId::System(*id),
            Self::User(id) => crate::objects::ReplicaId::User(*id),
        }
    }

    fn from_proto(proto: crate::objects::ReplicaId) -> Result<Self, TryFromProtoError> {
        match proto {
            crate::objects::ReplicaId::System(id) => Ok(Self::System(id)),
            crate::objects::ReplicaId::User(id) => Ok(Self::User(id)),
        }
    }
}

impl ProtoMapEntry<String, String> for crate::objects::OptimizerFeatureOverride {
    fn from_rust<'a>(entry: (&'a String, &'a String)) -> Self {
        crate::objects::OptimizerFeatureOverride {
            name: entry.0.into_proto(),
            value: entry.1.into_proto(),
        }
    }

    fn into_rust(self) -> Result<(String, String), TryFromProtoError> {
        Ok((self.name.into_rust()?, self.value.into_rust()?))
    }
}

impl RustType<crate::objects::ClusterSchedule> for ClusterSchedule {
    fn into_proto(&self) -> crate::objects::ClusterSchedule {
        match self {
            ClusterSchedule::Manual => crate::objects::ClusterSchedule::Manual,
            ClusterSchedule::Refresh {
                hydration_time_estimate,
            } => crate::objects::ClusterSchedule::Refresh(
                crate::objects::ClusterScheduleRefreshOptions {
                    rehydration_time_estimate: hydration_time_estimate.into_proto(),
                },
            ),
        }
    }

    fn from_proto(proto: crate::objects::ClusterSchedule) -> Result<Self, TryFromProtoError> {
        match proto {
            crate::objects::ClusterSchedule::Manual => Ok(ClusterSchedule::Manual),
            crate::objects::ClusterSchedule::Refresh(csro) => Ok(ClusterSchedule::Refresh {
                hydration_time_estimate: csro.rehydration_time_estimate.into_rust()?,
            }),
        }
    }
}

impl RustType<crate::objects::ReplicaLogging> for ComputeReplicaLogging {
    fn into_proto(&self) -> crate::objects::ReplicaLogging {
        crate::objects::ReplicaLogging {
            log_logging: self.log_logging,
            interval: self.interval.into_proto(),
        }
    }

    fn from_proto(proto: crate::objects::ReplicaLogging) -> Result<Self, TryFromProtoError> {
        Ok(ComputeReplicaLogging {
            log_logging: proto.log_logging,
            interval: proto.interval.into_rust()?,
        })
    }
}

impl RustType<crate::objects::Version> for RelationVersion {
    fn into_proto(&self) -> crate::objects::Version {
        crate::objects::Version {
            value: self.into_raw(),
        }
    }

    fn from_proto(proto: crate::objects::Version) -> Result<Self, TryFromProtoError> {
        Ok(RelationVersion::from_raw(proto.value))
    }
}

impl RustType<crate::objects::NetworkPolicyRule> for NetworkPolicyRule {
    fn into_proto(&self) -> crate::objects::NetworkPolicyRule {
        crate::objects::NetworkPolicyRule {
            name: self.name.clone(),
            action: match self.action {
                NetworkPolicyRuleAction::Allow => crate::objects::NetworkPolicyRuleAction::Allow,
            },
            direction: match self.direction {
                NetworkPolicyRuleDirection::Ingress => {
                    crate::objects::NetworkPolicyRuleDirection::Ingress
                }
            },
            address: self.address.clone().to_string(),
        }
    }

    fn from_proto(proto: crate::objects::NetworkPolicyRule) -> Result<Self, TryFromProtoError> {
        Ok(NetworkPolicyRule {
            name: proto.name,
            action: match proto.action {
                crate::objects::NetworkPolicyRuleAction::Allow => NetworkPolicyRuleAction::Allow,
            },
            address: PolicyAddress::from(proto.address),
            direction: match proto.direction {
                crate::objects::NetworkPolicyRuleDirection::Ingress => {
                    NetworkPolicyRuleDirection::Ingress
                }
            },
        })
    }
}
