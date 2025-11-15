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

use mz_catalog_types::cluster::ReplicaId;
use mz_catalog_types::cluster::StorageInstanceId;
use mz_catalog_types::compute::ComputeReplicaLogging;
use mz_proto::{IntoRustIfSome, ProtoMapEntry, ProtoType, RustType, TryFromProtoError};
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem};
use mz_repr::network_policy_id::NetworkPolicyId;
use mz_repr::role_id::RoleId;
use mz_repr::{CatalogItemId, GlobalId, RelationVersion};
use mz_sql::catalog::{CatalogItemType, ObjectType, RoleAttributes, RoleMembership, RoleVars};
use mz_sql::names::{
    CommentObjectId, DatabaseId, ResolvedDatabaseSpecifier, SchemaId, SchemaSpecifier,
};
use mz_sql::plan::{
    ClusterSchedule, NetworkPolicyRule, NetworkPolicyRuleAction, NetworkPolicyRuleDirection,
    PolicyAddress,
};
use mz_sql::session::vars::OwnedVarInput;

use crate::objects::Empty;

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
        let value = match self {
            RoleId::User(id) => crate::objects::role_id::Value::User(*id),
            RoleId::System(id) => crate::objects::role_id::Value::System(*id),
            RoleId::Predefined(id) => crate::objects::role_id::Value::Predefined(*id),
            RoleId::Public => crate::objects::role_id::Value::Public(Default::default()),
        };

        crate::objects::RoleId { value: Some(value) }
    }

    fn from_proto(proto: crate::objects::RoleId) -> Result<Self, TryFromProtoError> {
        let value = proto
            .value
            .ok_or_else(|| TryFromProtoError::missing_field("RoleId::value"))?;
        let id = match value {
            crate::objects::role_id::Value::User(id) => RoleId::User(id),
            crate::objects::role_id::Value::System(id) => RoleId::System(id),
            crate::objects::role_id::Value::Predefined(id) => RoleId::Predefined(id),
            crate::objects::role_id::Value::Public(_) => RoleId::Public,
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
            grantee: Some(self.grantee.into_proto()),
            grantor: Some(self.grantor.into_proto()),
            acl_mode: Some(self.acl_mode.into_proto()),
        }
    }

    fn from_proto(proto: crate::objects::MzAclItem) -> Result<Self, TryFromProtoError> {
        Ok(MzAclItem {
            grantee: proto.grantee.into_rust_if_some("MzAclItem::grantee")?,
            grantor: proto.grantor.into_rust_if_some("MzAclItem::grantor")?,
            acl_mode: proto.acl_mode.into_rust_if_some("MzAclItem::acl_mode")?,
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

impl RustType<crate::objects::role_vars::entry::Val> for OwnedVarInput {
    fn into_proto(&self) -> crate::objects::role_vars::entry::Val {
        match self.clone() {
            OwnedVarInput::Flat(v) => crate::objects::role_vars::entry::Val::Flat(v),
            OwnedVarInput::SqlSet(entries) => {
                crate::objects::role_vars::entry::Val::SqlSet(crate::objects::role_vars::SqlSet {
                    entries,
                })
            }
        }
    }

    fn from_proto(proto: crate::objects::role_vars::entry::Val) -> Result<Self, TryFromProtoError> {
        let result = match proto {
            crate::objects::role_vars::entry::Val::Flat(v) => OwnedVarInput::Flat(v),
            crate::objects::role_vars::entry::Val::SqlSet(crate::objects::role_vars::SqlSet {
                entries,
            }) => OwnedVarInput::SqlSet(entries),
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
            .map(|(key, val)| crate::objects::role_vars::Entry {
                key,
                val: Some(val.into_proto()),
            })
            .collect();

        crate::objects::RoleVars { entries }
    }

    fn from_proto(proto: crate::objects::RoleVars) -> Result<Self, TryFromProtoError> {
        let map = proto
            .entries
            .into_iter()
            .map(|entry| {
                let val = entry.val.into_rust_if_some("role_vars::Entry::Val")?;
                Ok::<_, TryFromProtoError>((entry.key, val))
            })
            .collect::<Result<_, _>>()?;

        Ok(RoleVars { map })
    }
}

impl RustType<crate::objects::NetworkPolicyId> for NetworkPolicyId {
    fn into_proto(&self) -> crate::objects::NetworkPolicyId {
        let value = match self {
            NetworkPolicyId::User(id) => crate::objects::network_policy_id::Value::User(*id),
            NetworkPolicyId::System(id) => crate::objects::network_policy_id::Value::System(*id),
        };

        crate::objects::NetworkPolicyId { value: Some(value) }
    }

    fn from_proto(proto: crate::objects::NetworkPolicyId) -> Result<Self, TryFromProtoError> {
        let value = proto
            .value
            .ok_or_else(|| TryFromProtoError::missing_field("NetworkPolicyId::value"))?;
        let id = match value {
            crate::objects::network_policy_id::Value::User(id) => NetworkPolicyId::User(id),
            crate::objects::network_policy_id::Value::System(id) => NetworkPolicyId::System(id),
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
                .map(|(key, val)| crate::objects::role_membership::Entry {
                    key: Some(key.into_proto()),
                    value: Some(val.into_proto()),
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
                    let key = e.key.into_rust_if_some("RoleMembership::Entry::key")?;
                    let val = e.value.into_rust_if_some("RoleMembership::Entry::value")?;

                    Ok((key, val))
                })
                .collect::<Result<_, TryFromProtoError>>()?,
        })
    }
}

impl RustType<crate::objects::ResolvedDatabaseSpecifier> for ResolvedDatabaseSpecifier {
    fn into_proto(&self) -> crate::objects::ResolvedDatabaseSpecifier {
        let spec = match self {
            ResolvedDatabaseSpecifier::Ambient => {
                crate::objects::resolved_database_specifier::Spec::Ambient(Default::default())
            }
            ResolvedDatabaseSpecifier::Id(database_id) => {
                crate::objects::resolved_database_specifier::Spec::Id(database_id.into_proto())
            }
        };
        crate::objects::ResolvedDatabaseSpecifier { spec: Some(spec) }
    }

    fn from_proto(
        proto: crate::objects::ResolvedDatabaseSpecifier,
    ) -> Result<Self, TryFromProtoError> {
        let spec = proto
            .spec
            .ok_or_else(|| TryFromProtoError::missing_field("ResolvedDatabaseSpecifier::spec"))?;
        let spec = match spec {
            crate::objects::resolved_database_specifier::Spec::Ambient(_) => {
                ResolvedDatabaseSpecifier::Ambient
            }
            crate::objects::resolved_database_specifier::Spec::Id(database_id) => {
                ResolvedDatabaseSpecifier::Id(database_id.into_rust()?)
            }
        };
        Ok(spec)
    }
}

impl RustType<crate::objects::SchemaSpecifier> for SchemaSpecifier {
    fn into_proto(&self) -> crate::objects::SchemaSpecifier {
        let spec = match self {
            SchemaSpecifier::Temporary => {
                crate::objects::schema_specifier::Spec::Temporary(Default::default())
            }
            SchemaSpecifier::Id(schema_id) => {
                crate::objects::schema_specifier::Spec::Id(schema_id.into_proto())
            }
        };
        crate::objects::SchemaSpecifier { spec: Some(spec) }
    }

    fn from_proto(proto: crate::objects::SchemaSpecifier) -> Result<Self, TryFromProtoError> {
        let spec = proto
            .spec
            .ok_or_else(|| TryFromProtoError::missing_field("SchemaSpecifier::spec"))?;
        let spec = match spec {
            crate::objects::schema_specifier::Spec::Temporary(_) => SchemaSpecifier::Temporary,
            crate::objects::schema_specifier::Spec::Id(schema_id) => {
                SchemaSpecifier::Id(schema_id.into_rust()?)
            }
        };
        Ok(spec)
    }
}

impl RustType<crate::objects::SchemaId> for SchemaId {
    fn into_proto(&self) -> crate::objects::SchemaId {
        let value = match self {
            SchemaId::User(id) => crate::objects::schema_id::Value::User(*id),
            SchemaId::System(id) => crate::objects::schema_id::Value::System(*id),
        };

        crate::objects::SchemaId { value: Some(value) }
    }

    fn from_proto(proto: crate::objects::SchemaId) -> Result<Self, TryFromProtoError> {
        let value = proto
            .value
            .ok_or_else(|| TryFromProtoError::missing_field("SchemaId::value"))?;
        let id = match value {
            crate::objects::schema_id::Value::User(id) => SchemaId::User(id),
            crate::objects::schema_id::Value::System(id) => SchemaId::System(id),
        };
        Ok(id)
    }
}

impl RustType<crate::objects::DatabaseId> for DatabaseId {
    fn into_proto(&self) -> crate::objects::DatabaseId {
        let value = match self {
            DatabaseId::User(id) => crate::objects::database_id::Value::User(*id),
            DatabaseId::System(id) => crate::objects::database_id::Value::System(*id),
        };

        crate::objects::DatabaseId { value: Some(value) }
    }

    fn from_proto(proto: crate::objects::DatabaseId) -> Result<Self, TryFromProtoError> {
        match proto.value {
            Some(crate::objects::database_id::Value::User(id)) => Ok(DatabaseId::User(id)),
            Some(crate::objects::database_id::Value::System(id)) => Ok(DatabaseId::System(id)),
            None => Err(TryFromProtoError::missing_field("DatabaseId::value")),
        }
    }
}

impl RustType<crate::objects::comment_key::Object> for CommentObjectId {
    fn into_proto(&self) -> crate::objects::comment_key::Object {
        match self {
            CommentObjectId::Table(global_id) => {
                crate::objects::comment_key::Object::Table(global_id.into_proto())
            }
            CommentObjectId::View(global_id) => {
                crate::objects::comment_key::Object::View(global_id.into_proto())
            }
            CommentObjectId::MaterializedView(global_id) => {
                crate::objects::comment_key::Object::MaterializedView(global_id.into_proto())
            }
            CommentObjectId::Source(global_id) => {
                crate::objects::comment_key::Object::Source(global_id.into_proto())
            }
            CommentObjectId::Sink(global_id) => {
                crate::objects::comment_key::Object::Sink(global_id.into_proto())
            }
            CommentObjectId::Index(global_id) => {
                crate::objects::comment_key::Object::Index(global_id.into_proto())
            }
            CommentObjectId::Func(global_id) => {
                crate::objects::comment_key::Object::Func(global_id.into_proto())
            }
            CommentObjectId::Connection(global_id) => {
                crate::objects::comment_key::Object::Connection(global_id.into_proto())
            }
            CommentObjectId::Type(global_id) => {
                crate::objects::comment_key::Object::Type(global_id.into_proto())
            }
            CommentObjectId::Secret(global_id) => {
                crate::objects::comment_key::Object::Secret(global_id.into_proto())
            }
            CommentObjectId::Role(role_id) => {
                crate::objects::comment_key::Object::Role(role_id.into_proto())
            }
            CommentObjectId::Database(database_id) => {
                crate::objects::comment_key::Object::Database(database_id.into_proto())
            }
            CommentObjectId::ContinualTask(global_id) => {
                crate::objects::comment_key::Object::ContinualTask(global_id.into_proto())
            }
            CommentObjectId::NetworkPolicy(network_policy_id) => {
                crate::objects::comment_key::Object::NetworkPolicy(network_policy_id.into_proto())
            }
            CommentObjectId::Schema((database, schema)) => {
                crate::objects::comment_key::Object::Schema(crate::objects::ResolvedSchema {
                    database: Some(database.into_proto()),
                    schema: Some(schema.into_proto()),
                })
            }
            CommentObjectId::Cluster(cluster_id) => {
                crate::objects::comment_key::Object::Cluster(cluster_id.into_proto())
            }
            CommentObjectId::ClusterReplica((cluster_id, replica_id)) => {
                let cluster_replica_id = crate::objects::ClusterReplicaId {
                    cluster_id: Some(cluster_id.into_proto()),
                    replica_id: Some(replica_id.into_proto()),
                };
                crate::objects::comment_key::Object::ClusterReplica(cluster_replica_id)
            }
        }
    }

    fn from_proto(proto: crate::objects::comment_key::Object) -> Result<Self, TryFromProtoError> {
        let id = match proto {
            crate::objects::comment_key::Object::Table(item_id) => {
                CommentObjectId::Table(item_id.into_rust()?)
            }
            crate::objects::comment_key::Object::View(item_id) => {
                CommentObjectId::View(item_id.into_rust()?)
            }
            crate::objects::comment_key::Object::MaterializedView(item_id) => {
                CommentObjectId::MaterializedView(item_id.into_rust()?)
            }
            crate::objects::comment_key::Object::Source(item_id) => {
                CommentObjectId::Source(item_id.into_rust()?)
            }
            crate::objects::comment_key::Object::Sink(item_id) => {
                CommentObjectId::Sink(item_id.into_rust()?)
            }
            crate::objects::comment_key::Object::Index(item_id) => {
                CommentObjectId::Index(item_id.into_rust()?)
            }
            crate::objects::comment_key::Object::Func(item_id) => {
                CommentObjectId::Func(item_id.into_rust()?)
            }
            crate::objects::comment_key::Object::Connection(item_id) => {
                CommentObjectId::Connection(item_id.into_rust()?)
            }
            crate::objects::comment_key::Object::Type(item_id) => {
                CommentObjectId::Type(item_id.into_rust()?)
            }
            crate::objects::comment_key::Object::Secret(item_id) => {
                CommentObjectId::Secret(item_id.into_rust()?)
            }
            crate::objects::comment_key::Object::ContinualTask(item_id) => {
                CommentObjectId::ContinualTask(item_id.into_rust()?)
            }
            crate::objects::comment_key::Object::NetworkPolicy(global_id) => {
                CommentObjectId::NetworkPolicy(global_id.into_rust()?)
            }
            crate::objects::comment_key::Object::Role(role_id) => {
                CommentObjectId::Role(role_id.into_rust()?)
            }
            crate::objects::comment_key::Object::Database(database_id) => {
                CommentObjectId::Database(database_id.into_rust()?)
            }
            crate::objects::comment_key::Object::Schema(resolved_schema) => {
                let database = resolved_schema
                    .database
                    .into_rust_if_some("ResolvedSchema::database")?;
                let schema = resolved_schema
                    .schema
                    .into_rust_if_some("ResolvedSchema::schema")?;
                CommentObjectId::Schema((database, schema))
            }
            crate::objects::comment_key::Object::Cluster(cluster_id) => {
                CommentObjectId::Cluster(cluster_id.into_rust()?)
            }
            crate::objects::comment_key::Object::ClusterReplica(cluster_replica_id) => {
                let cluster_id = cluster_replica_id
                    .cluster_id
                    .into_rust_if_some("ClusterReplicaId::cluster_id")?;
                let replica_id = cluster_replica_id
                    .replica_id
                    .into_rust_if_some("ClusterReplicaId::replica_id")?;
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

impl RustType<crate::objects::CatalogItemId> for CatalogItemId {
    fn into_proto(&self) -> crate::objects::CatalogItemId {
        crate::objects::CatalogItemId {
            value: Some(match self {
                CatalogItemId::System(x) => crate::objects::catalog_item_id::Value::System(*x),
                CatalogItemId::IntrospectionSourceIndex(x) => {
                    crate::objects::catalog_item_id::Value::IntrospectionSourceIndex(*x)
                }
                CatalogItemId::User(x) => crate::objects::catalog_item_id::Value::User(*x),
                CatalogItemId::Transient(x) => {
                    crate::objects::catalog_item_id::Value::Transient(*x)
                }
            }),
        }
    }

    fn from_proto(proto: crate::objects::CatalogItemId) -> Result<Self, TryFromProtoError> {
        match proto.value {
            Some(crate::objects::catalog_item_id::Value::System(x)) => Ok(CatalogItemId::System(x)),
            Some(crate::objects::catalog_item_id::Value::IntrospectionSourceIndex(x)) => {
                Ok(CatalogItemId::IntrospectionSourceIndex(x))
            }
            Some(crate::objects::catalog_item_id::Value::User(x)) => Ok(CatalogItemId::User(x)),
            Some(crate::objects::catalog_item_id::Value::Transient(x)) => {
                Ok(CatalogItemId::Transient(x))
            }
            None => Err(TryFromProtoError::missing_field("CatalogItemId::kind")),
        }
    }
}

impl RustType<crate::objects::GlobalId> for GlobalId {
    fn into_proto(&self) -> crate::objects::GlobalId {
        crate::objects::GlobalId {
            value: Some(match self {
                GlobalId::System(x) => crate::objects::global_id::Value::System(*x),
                GlobalId::IntrospectionSourceIndex(x) => {
                    crate::objects::global_id::Value::IntrospectionSourceIndex(*x)
                }
                GlobalId::User(x) => crate::objects::global_id::Value::User(*x),
                GlobalId::Transient(x) => crate::objects::global_id::Value::Transient(*x),
                GlobalId::Explain => crate::objects::global_id::Value::Explain(Default::default()),
            }),
        }
    }

    fn from_proto(proto: crate::objects::GlobalId) -> Result<Self, TryFromProtoError> {
        match proto.value {
            Some(crate::objects::global_id::Value::System(x)) => Ok(GlobalId::System(x)),
            Some(crate::objects::global_id::Value::IntrospectionSourceIndex(x)) => {
                Ok(GlobalId::IntrospectionSourceIndex(x))
            }
            Some(crate::objects::global_id::Value::User(x)) => Ok(GlobalId::User(x)),
            Some(crate::objects::global_id::Value::Transient(x)) => Ok(GlobalId::Transient(x)),
            Some(crate::objects::global_id::Value::Explain(_)) => Ok(GlobalId::Explain),
            None => Err(TryFromProtoError::missing_field("GlobalId::kind")),
        }
    }
}

impl RustType<crate::objects::ClusterId> for StorageInstanceId {
    fn into_proto(&self) -> crate::objects::ClusterId {
        let value = match self {
            StorageInstanceId::User(id) => crate::objects::cluster_id::Value::User(*id),
            StorageInstanceId::System(id) => crate::objects::cluster_id::Value::System(*id),
        };

        crate::objects::ClusterId { value: Some(value) }
    }

    fn from_proto(proto: crate::objects::ClusterId) -> Result<Self, TryFromProtoError> {
        let value = proto
            .value
            .ok_or_else(|| TryFromProtoError::missing_field("ClusterId::value"))?;
        let id = match value {
            crate::objects::cluster_id::Value::User(id) => {
                StorageInstanceId::user(id).ok_or_else(|| {
                    TryFromProtoError::InvalidPersistState(format!(
                        "{id} is not a valid StorageInstanceId"
                    ))
                })?
            }
            crate::objects::cluster_id::Value::System(id) => StorageInstanceId::system(id)
                .ok_or_else(|| {
                    TryFromProtoError::InvalidPersistState(format!(
                        "{id} is not a valid StorageInstanceId"
                    ))
                })?,
        };
        Ok(id)
    }
}

impl RustType<crate::objects::ReplicaId> for ReplicaId {
    fn into_proto(&self) -> crate::objects::ReplicaId {
        use crate::objects::replica_id::Value::*;
        crate::objects::ReplicaId {
            value: Some(match self {
                Self::System(id) => System(*id),
                Self::User(id) => User(*id),
            }),
        }
    }

    fn from_proto(proto: crate::objects::ReplicaId) -> Result<Self, TryFromProtoError> {
        use crate::objects::replica_id::Value::*;
        match proto.value {
            Some(System(id)) => Ok(Self::System(id)),
            Some(User(id)) => Ok(Self::User(id)),
            None => Err(TryFromProtoError::missing_field("ReplicaId::value")),
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
            ClusterSchedule::Manual => crate::objects::ClusterSchedule {
                value: Some(crate::objects::cluster_schedule::Value::Manual(Empty {})),
            },
            ClusterSchedule::Refresh {
                hydration_time_estimate,
            } => crate::objects::ClusterSchedule {
                value: Some(crate::objects::cluster_schedule::Value::Refresh(
                    crate::objects::ClusterScheduleRefreshOptions {
                        rehydration_time_estimate: Some(hydration_time_estimate.into_proto()),
                    },
                )),
            },
        }
    }

    fn from_proto(proto: crate::objects::ClusterSchedule) -> Result<Self, TryFromProtoError> {
        match proto.value {
            None => Ok(Default::default()),
            Some(crate::objects::cluster_schedule::Value::Manual(Empty {})) => {
                Ok(ClusterSchedule::Manual)
            }
            Some(crate::objects::cluster_schedule::Value::Refresh(csro)) => {
                Ok(ClusterSchedule::Refresh {
                    hydration_time_estimate: csro
                        .rehydration_time_estimate
                        .into_rust_if_some("rehydration_time_estimate")?,
                })
            }
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
        use crate::objects::network_policy_rule::{Action, Direction};
        crate::objects::NetworkPolicyRule {
            name: self.name.clone(),
            action: match self.action {
                NetworkPolicyRuleAction::Allow => Some(Action::Allow(Empty {})),
            },
            direction: match self.direction {
                NetworkPolicyRuleDirection::Ingress => Some(Direction::Ingress(Empty {})),
            },
            address: self.address.clone().to_string(),
        }
    }

    fn from_proto(proto: crate::objects::NetworkPolicyRule) -> Result<Self, TryFromProtoError> {
        use crate::objects::network_policy_rule::{Action, Direction};
        Ok(NetworkPolicyRule {
            name: proto.name,
            action: match proto
                .action
                .ok_or_else(|| TryFromProtoError::missing_field("NetworkPolicyRule::action"))?
            {
                Action::Allow(_) => NetworkPolicyRuleAction::Allow,
            },
            address: PolicyAddress::from(proto.address),
            direction: match proto
                .direction
                .ok_or_else(|| TryFromProtoError::missing_field("NetworkPolicyRule::direction"))?
            {
                Direction::Ingress(_) => NetworkPolicyRuleDirection::Ingress,
            },
        })
    }
}
