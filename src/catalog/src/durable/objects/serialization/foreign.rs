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

use std::time::Duration;

use mz_compute_types::config::ComputeReplicaLogging;
use mz_controller_types::ReplicaId;
use mz_persist_types::ShardId;
use mz_proto::TryFromProtoError;
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem};
use mz_repr::network_policy_id::NetworkPolicyId;
use mz_repr::role_id::RoleId;
use mz_repr::{CatalogItemId, GlobalId, RelationVersion};
use mz_sql::catalog::{
    AutoProvisionSource, CatalogItemType, ObjectType, RoleAttributes, RoleMembership, RoleVars,
};
use mz_sql::names::{
    CommentObjectId, DatabaseId, ResolvedDatabaseSpecifier, SchemaId, SchemaSpecifier,
};
use mz_sql::plan::{
    AutoScalingStrategy, ClusterSchedule, NetworkPolicyRule, NetworkPolicyRuleAction,
    NetworkPolicyRuleDirection, OnHydration, OnTimeoutAction, PolicyAddress,
};
use mz_sql::session::vars::OwnedVarInput;
use mz_storage_types::instances::StorageInstanceId;

use crate::durable::objects::serialization::{ProtoMapEntry, ProtoType, RustType, proto};

impl RustType<proto::Duration> for Duration {
    fn into_proto(&self) -> proto::Duration {
        proto::Duration {
            secs: self.as_secs(),
            nanos: self.subsec_nanos(),
        }
    }

    fn from_proto(proto: proto::Duration) -> Result<Self, TryFromProtoError> {
        Ok(Duration::new(proto.secs, proto.nanos))
    }
}

impl RustType<proto::RoleId> for RoleId {
    fn into_proto(&self) -> proto::RoleId {
        match self {
            RoleId::User(id) => proto::RoleId::User(*id),
            RoleId::System(id) => proto::RoleId::System(*id),
            RoleId::Predefined(id) => proto::RoleId::Predefined(*id),
            RoleId::Public => proto::RoleId::Public,
        }
    }

    fn from_proto(proto: proto::RoleId) -> Result<Self, TryFromProtoError> {
        let id = match proto {
            proto::RoleId::User(id) => RoleId::User(id),
            proto::RoleId::System(id) => RoleId::System(id),
            proto::RoleId::Predefined(id) => RoleId::Predefined(id),
            proto::RoleId::Public => RoleId::Public,
        };
        Ok(id)
    }
}

impl RustType<proto::AclMode> for AclMode {
    fn into_proto(&self) -> proto::AclMode {
        proto::AclMode {
            bitflags: self.bits(),
        }
    }

    fn from_proto(proto: proto::AclMode) -> Result<Self, TryFromProtoError> {
        AclMode::from_bits(proto.bitflags).ok_or_else(|| {
            TryFromProtoError::InvalidBitFlags(format!("Invalid AclMode from catalog {proto:?}"))
        })
    }
}

impl RustType<proto::MzAclItem> for MzAclItem {
    fn into_proto(&self) -> proto::MzAclItem {
        proto::MzAclItem {
            grantee: self.grantee.into_proto(),
            grantor: self.grantor.into_proto(),
            acl_mode: self.acl_mode.into_proto(),
        }
    }

    fn from_proto(proto: proto::MzAclItem) -> Result<Self, TryFromProtoError> {
        Ok(MzAclItem {
            grantee: proto.grantee.into_rust()?,
            grantor: proto.grantor.into_rust()?,
            acl_mode: proto.acl_mode.into_rust()?,
        })
    }
}

impl RustType<proto::AutoProvisionSource> for AutoProvisionSource {
    fn into_proto(&self) -> proto::AutoProvisionSource {
        match self {
            AutoProvisionSource::Oidc => proto::AutoProvisionSource::Oidc,
            AutoProvisionSource::Frontegg => proto::AutoProvisionSource::Frontegg,
            AutoProvisionSource::None => proto::AutoProvisionSource::None,
        }
    }

    fn from_proto(proto: proto::AutoProvisionSource) -> Result<Self, TryFromProtoError> {
        Ok(match proto {
            proto::AutoProvisionSource::Oidc => AutoProvisionSource::Oidc,
            proto::AutoProvisionSource::Frontegg => AutoProvisionSource::Frontegg,
            proto::AutoProvisionSource::None => AutoProvisionSource::None,
        })
    }
}

impl RustType<proto::RoleAttributes> for RoleAttributes {
    fn into_proto(&self) -> proto::RoleAttributes {
        proto::RoleAttributes {
            inherit: self.inherit,
            superuser: self.superuser,
            login: self.login,
            auto_provision_source: self.auto_provision_source.into_proto(),
        }
    }

    fn from_proto(proto: proto::RoleAttributes) -> Result<Self, TryFromProtoError> {
        let mut attributes = RoleAttributes::new();

        attributes.inherit = proto.inherit;
        attributes.superuser = proto.superuser;
        attributes.login = proto.login;
        attributes.auto_provision_source = proto.auto_provision_source.into_rust()?;

        Ok(attributes)
    }
}

impl RustType<proto::RoleVar> for OwnedVarInput {
    fn into_proto(&self) -> proto::RoleVar {
        match self.clone() {
            OwnedVarInput::Flat(v) => proto::RoleVar::Flat(v),
            OwnedVarInput::SqlSet(entries) => proto::RoleVar::SqlSet(entries),
        }
    }

    fn from_proto(proto: proto::RoleVar) -> Result<Self, TryFromProtoError> {
        let result = match proto {
            proto::RoleVar::Flat(v) => OwnedVarInput::Flat(v),
            proto::RoleVar::SqlSet(entries) => OwnedVarInput::SqlSet(entries),
        };
        Ok(result)
    }
}

impl RustType<proto::RoleVars> for RoleVars {
    fn into_proto(&self) -> proto::RoleVars {
        let entries = self
            .map
            .clone()
            .into_iter()
            .map(|(key, val)| proto::RoleVarsEntry {
                key,
                val: val.into_proto(),
            })
            .collect();

        proto::RoleVars { entries }
    }

    fn from_proto(proto: proto::RoleVars) -> Result<Self, TryFromProtoError> {
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

impl RustType<proto::NetworkPolicyId> for NetworkPolicyId {
    fn into_proto(&self) -> proto::NetworkPolicyId {
        match self {
            NetworkPolicyId::User(id) => proto::NetworkPolicyId::User(*id),
            NetworkPolicyId::System(id) => proto::NetworkPolicyId::System(*id),
        }
    }

    fn from_proto(proto: proto::NetworkPolicyId) -> Result<Self, TryFromProtoError> {
        let id = match proto {
            proto::NetworkPolicyId::User(id) => NetworkPolicyId::User(id),
            proto::NetworkPolicyId::System(id) => NetworkPolicyId::System(id),
        };
        Ok(id)
    }
}

impl RustType<proto::CatalogItemType> for CatalogItemType {
    fn into_proto(&self) -> proto::CatalogItemType {
        match self {
            CatalogItemType::Table => proto::CatalogItemType::Table,
            CatalogItemType::Source => proto::CatalogItemType::Source,
            CatalogItemType::Sink => proto::CatalogItemType::Sink,
            CatalogItemType::View => proto::CatalogItemType::View,
            CatalogItemType::MaterializedView => proto::CatalogItemType::MaterializedView,
            CatalogItemType::Index => proto::CatalogItemType::Index,
            CatalogItemType::Type => proto::CatalogItemType::Type,
            CatalogItemType::Func => proto::CatalogItemType::Func,
            CatalogItemType::Secret => proto::CatalogItemType::Secret,
            CatalogItemType::Connection => proto::CatalogItemType::Connection,
            CatalogItemType::MetricSink => proto::CatalogItemType::MetricSink,
        }
    }

    fn from_proto(proto: proto::CatalogItemType) -> Result<Self, TryFromProtoError> {
        let item_type = match proto {
            proto::CatalogItemType::Table => CatalogItemType::Table,
            proto::CatalogItemType::Source => CatalogItemType::Source,
            proto::CatalogItemType::Sink => CatalogItemType::Sink,
            proto::CatalogItemType::View => CatalogItemType::View,
            proto::CatalogItemType::MaterializedView => CatalogItemType::MaterializedView,
            proto::CatalogItemType::Index => CatalogItemType::Index,
            proto::CatalogItemType::Type => CatalogItemType::Type,
            proto::CatalogItemType::Func => CatalogItemType::Func,
            proto::CatalogItemType::Secret => CatalogItemType::Secret,
            proto::CatalogItemType::Connection => CatalogItemType::Connection,
            proto::CatalogItemType::MetricSink => CatalogItemType::MetricSink,
            proto::CatalogItemType::Unknown => {
                return Err(TryFromProtoError::unknown_enum_variant("CatalogItemType"));
            }
        };
        Ok(item_type)
    }
}

impl RustType<proto::ObjectType> for ObjectType {
    fn into_proto(&self) -> proto::ObjectType {
        match self {
            ObjectType::Table => proto::ObjectType::Table,
            ObjectType::View => proto::ObjectType::View,
            ObjectType::MaterializedView => proto::ObjectType::MaterializedView,
            ObjectType::Source => proto::ObjectType::Source,
            ObjectType::Sink => proto::ObjectType::Sink,
            ObjectType::Index => proto::ObjectType::Index,
            ObjectType::Type => proto::ObjectType::Type,
            ObjectType::Role => proto::ObjectType::Role,
            ObjectType::Cluster => proto::ObjectType::Cluster,
            ObjectType::ClusterReplica => proto::ObjectType::ClusterReplica,
            ObjectType::Secret => proto::ObjectType::Secret,
            ObjectType::Connection => proto::ObjectType::Connection,
            ObjectType::Database => proto::ObjectType::Database,
            ObjectType::Schema => proto::ObjectType::Schema,
            ObjectType::Func => proto::ObjectType::Func,
            ObjectType::NetworkPolicy => proto::ObjectType::NetworkPolicy,
            ObjectType::MetricSink => proto::ObjectType::MetricSink,
        }
    }

    fn from_proto(proto: proto::ObjectType) -> Result<Self, TryFromProtoError> {
        match proto {
            proto::ObjectType::Table => Ok(ObjectType::Table),
            proto::ObjectType::View => Ok(ObjectType::View),
            proto::ObjectType::MaterializedView => Ok(ObjectType::MaterializedView),
            proto::ObjectType::Source => Ok(ObjectType::Source),
            proto::ObjectType::Sink => Ok(ObjectType::Sink),
            proto::ObjectType::Index => Ok(ObjectType::Index),
            proto::ObjectType::Type => Ok(ObjectType::Type),
            proto::ObjectType::Role => Ok(ObjectType::Role),
            proto::ObjectType::Cluster => Ok(ObjectType::Cluster),
            proto::ObjectType::ClusterReplica => Ok(ObjectType::ClusterReplica),
            proto::ObjectType::Secret => Ok(ObjectType::Secret),
            proto::ObjectType::Connection => Ok(ObjectType::Connection),
            proto::ObjectType::Database => Ok(ObjectType::Database),
            proto::ObjectType::Schema => Ok(ObjectType::Schema),
            proto::ObjectType::Func => Ok(ObjectType::Func),
            proto::ObjectType::NetworkPolicy => Ok(ObjectType::NetworkPolicy),
            proto::ObjectType::MetricSink => Ok(ObjectType::MetricSink),
            proto::ObjectType::Unknown => Err(TryFromProtoError::unknown_enum_variant(
                "ObjectType::Unknown",
            )),
        }
    }
}

impl RustType<proto::RoleMembership> for RoleMembership {
    fn into_proto(&self) -> proto::RoleMembership {
        proto::RoleMembership {
            map: self
                .map
                .iter()
                .map(|(key, val)| proto::RoleMembershipEntry {
                    key: key.into_proto(),
                    value: val.into_proto(),
                })
                .collect(),
        }
    }

    fn from_proto(proto: proto::RoleMembership) -> Result<Self, TryFromProtoError> {
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

impl RustType<proto::ResolvedDatabaseSpecifier> for ResolvedDatabaseSpecifier {
    fn into_proto(&self) -> proto::ResolvedDatabaseSpecifier {
        match self {
            ResolvedDatabaseSpecifier::Ambient => proto::ResolvedDatabaseSpecifier::Ambient,
            ResolvedDatabaseSpecifier::Id(database_id) => {
                proto::ResolvedDatabaseSpecifier::Id(database_id.into_proto())
            }
        }
    }

    fn from_proto(proto: proto::ResolvedDatabaseSpecifier) -> Result<Self, TryFromProtoError> {
        let spec = match proto {
            proto::ResolvedDatabaseSpecifier::Ambient => ResolvedDatabaseSpecifier::Ambient,
            proto::ResolvedDatabaseSpecifier::Id(database_id) => {
                ResolvedDatabaseSpecifier::Id(database_id.into_rust()?)
            }
        };
        Ok(spec)
    }
}

impl RustType<proto::SchemaSpecifier> for SchemaSpecifier {
    fn into_proto(&self) -> proto::SchemaSpecifier {
        match self {
            SchemaSpecifier::Temporary => proto::SchemaSpecifier::Temporary,
            SchemaSpecifier::Id(schema_id) => proto::SchemaSpecifier::Id(schema_id.into_proto()),
        }
    }

    fn from_proto(proto: proto::SchemaSpecifier) -> Result<Self, TryFromProtoError> {
        let spec = match proto {
            proto::SchemaSpecifier::Temporary => SchemaSpecifier::Temporary,
            proto::SchemaSpecifier::Id(schema_id) => SchemaSpecifier::Id(schema_id.into_rust()?),
        };
        Ok(spec)
    }
}

impl RustType<proto::SchemaId> for SchemaId {
    fn into_proto(&self) -> proto::SchemaId {
        match self {
            SchemaId::User(id) => proto::SchemaId::User(*id),
            SchemaId::System(id) => proto::SchemaId::System(*id),
        }
    }

    fn from_proto(proto: proto::SchemaId) -> Result<Self, TryFromProtoError> {
        let id = match proto {
            proto::SchemaId::User(id) => SchemaId::User(id),
            proto::SchemaId::System(id) => SchemaId::System(id),
        };
        Ok(id)
    }
}

impl RustType<proto::DatabaseId> for DatabaseId {
    fn into_proto(&self) -> proto::DatabaseId {
        match self {
            DatabaseId::User(id) => proto::DatabaseId::User(*id),
            DatabaseId::System(id) => proto::DatabaseId::System(*id),
        }
    }

    fn from_proto(proto: proto::DatabaseId) -> Result<Self, TryFromProtoError> {
        match proto {
            proto::DatabaseId::User(id) => Ok(DatabaseId::User(id)),
            proto::DatabaseId::System(id) => Ok(DatabaseId::System(id)),
        }
    }
}

impl RustType<proto::CommentObject> for CommentObjectId {
    fn into_proto(&self) -> proto::CommentObject {
        match self {
            CommentObjectId::Table(global_id) => {
                proto::CommentObject::Table(global_id.into_proto())
            }
            CommentObjectId::View(global_id) => proto::CommentObject::View(global_id.into_proto()),
            CommentObjectId::MaterializedView(global_id) => {
                proto::CommentObject::MaterializedView(global_id.into_proto())
            }
            CommentObjectId::Source(global_id) => {
                proto::CommentObject::Source(global_id.into_proto())
            }
            CommentObjectId::Sink(global_id) => proto::CommentObject::Sink(global_id.into_proto()),
            CommentObjectId::MetricSink(global_id) => {
                proto::CommentObject::MetricSink(global_id.into_proto())
            }
            CommentObjectId::Index(global_id) => {
                proto::CommentObject::Index(global_id.into_proto())
            }
            CommentObjectId::Func(global_id) => proto::CommentObject::Func(global_id.into_proto()),
            CommentObjectId::Connection(global_id) => {
                proto::CommentObject::Connection(global_id.into_proto())
            }
            CommentObjectId::Type(global_id) => proto::CommentObject::Type(global_id.into_proto()),
            CommentObjectId::Secret(global_id) => {
                proto::CommentObject::Secret(global_id.into_proto())
            }
            CommentObjectId::Role(role_id) => proto::CommentObject::Role(role_id.into_proto()),
            CommentObjectId::Database(database_id) => {
                proto::CommentObject::Database(database_id.into_proto())
            }
            CommentObjectId::NetworkPolicy(network_policy_id) => {
                proto::CommentObject::NetworkPolicy(network_policy_id.into_proto())
            }
            CommentObjectId::Schema((database, schema)) => {
                proto::CommentObject::Schema(proto::ResolvedSchema {
                    database: database.into_proto(),
                    schema: schema.into_proto(),
                })
            }
            CommentObjectId::Cluster(cluster_id) => {
                proto::CommentObject::Cluster(cluster_id.into_proto())
            }
            CommentObjectId::ClusterReplica((cluster_id, replica_id)) => {
                let cluster_replica_id = proto::ClusterReplicaId {
                    cluster_id: cluster_id.into_proto(),
                    replica_id: replica_id.into_proto(),
                };
                proto::CommentObject::ClusterReplica(cluster_replica_id)
            }
        }
    }

    fn from_proto(proto: proto::CommentObject) -> Result<Self, TryFromProtoError> {
        let id = match proto {
            proto::CommentObject::Table(item_id) => CommentObjectId::Table(item_id.into_rust()?),
            proto::CommentObject::View(item_id) => CommentObjectId::View(item_id.into_rust()?),
            proto::CommentObject::MaterializedView(item_id) => {
                CommentObjectId::MaterializedView(item_id.into_rust()?)
            }
            proto::CommentObject::Source(item_id) => CommentObjectId::Source(item_id.into_rust()?),
            proto::CommentObject::Sink(item_id) => CommentObjectId::Sink(item_id.into_rust()?),
            proto::CommentObject::MetricSink(item_id) => {
                CommentObjectId::MetricSink(item_id.into_rust()?)
            }
            proto::CommentObject::Index(item_id) => CommentObjectId::Index(item_id.into_rust()?),
            proto::CommentObject::Func(item_id) => CommentObjectId::Func(item_id.into_rust()?),
            proto::CommentObject::Connection(item_id) => {
                CommentObjectId::Connection(item_id.into_rust()?)
            }
            proto::CommentObject::Type(item_id) => CommentObjectId::Type(item_id.into_rust()?),
            proto::CommentObject::Secret(item_id) => CommentObjectId::Secret(item_id.into_rust()?),
            proto::CommentObject::NetworkPolicy(global_id) => {
                CommentObjectId::NetworkPolicy(global_id.into_rust()?)
            }
            proto::CommentObject::Role(role_id) => CommentObjectId::Role(role_id.into_rust()?),
            proto::CommentObject::Database(database_id) => {
                CommentObjectId::Database(database_id.into_rust()?)
            }
            proto::CommentObject::Schema(resolved_schema) => {
                let database = resolved_schema.database.into_rust()?;
                let schema = resolved_schema.schema.into_rust()?;
                CommentObjectId::Schema((database, schema))
            }
            proto::CommentObject::Cluster(cluster_id) => {
                CommentObjectId::Cluster(cluster_id.into_rust()?)
            }
            proto::CommentObject::ClusterReplica(cluster_replica_id) => {
                let cluster_id = cluster_replica_id.cluster_id.into_rust()?;
                let replica_id = cluster_replica_id.replica_id.into_rust()?;
                CommentObjectId::ClusterReplica((cluster_id, replica_id))
            }
        };
        Ok(id)
    }
}

impl RustType<proto::EpochMillis> for u64 {
    fn into_proto(&self) -> proto::EpochMillis {
        proto::EpochMillis { millis: *self }
    }

    fn from_proto(proto: proto::EpochMillis) -> Result<Self, TryFromProtoError> {
        Ok(proto.millis)
    }
}

impl RustType<proto::CatalogItemId> for CatalogItemId {
    fn into_proto(&self) -> proto::CatalogItemId {
        match self {
            CatalogItemId::System(x) => proto::CatalogItemId::System(*x),
            CatalogItemId::IntrospectionSourceIndex(x) => {
                proto::CatalogItemId::IntrospectionSourceIndex(*x)
            }
            CatalogItemId::User(x) => proto::CatalogItemId::User(*x),
            CatalogItemId::Transient(x) => proto::CatalogItemId::Transient(*x),
        }
    }

    fn from_proto(proto: proto::CatalogItemId) -> Result<Self, TryFromProtoError> {
        match proto {
            proto::CatalogItemId::System(x) => Ok(CatalogItemId::System(x)),
            proto::CatalogItemId::IntrospectionSourceIndex(x) => {
                Ok(CatalogItemId::IntrospectionSourceIndex(x))
            }
            proto::CatalogItemId::User(x) => Ok(CatalogItemId::User(x)),
            proto::CatalogItemId::Transient(x) => Ok(CatalogItemId::Transient(x)),
        }
    }
}

impl RustType<proto::GlobalId> for GlobalId {
    fn into_proto(&self) -> proto::GlobalId {
        match self {
            GlobalId::System(x) => proto::GlobalId::System(*x),
            GlobalId::IntrospectionSourceIndex(x) => proto::GlobalId::IntrospectionSourceIndex(*x),
            GlobalId::User(x) => proto::GlobalId::User(*x),
            GlobalId::Transient(x) => proto::GlobalId::Transient(*x),
            GlobalId::Explain => proto::GlobalId::Explain,
        }
    }

    fn from_proto(proto: proto::GlobalId) -> Result<Self, TryFromProtoError> {
        match proto {
            proto::GlobalId::System(x) => Ok(GlobalId::System(x)),
            proto::GlobalId::IntrospectionSourceIndex(x) => {
                Ok(GlobalId::IntrospectionSourceIndex(x))
            }
            proto::GlobalId::User(x) => Ok(GlobalId::User(x)),
            proto::GlobalId::Transient(x) => Ok(GlobalId::Transient(x)),
            proto::GlobalId::Explain => Ok(GlobalId::Explain),
        }
    }
}

impl RustType<proto::ClusterId> for StorageInstanceId {
    fn into_proto(&self) -> proto::ClusterId {
        match self {
            StorageInstanceId::User(id) => proto::ClusterId::User(*id),
            StorageInstanceId::System(id) => proto::ClusterId::System(*id),
        }
    }

    fn from_proto(proto: proto::ClusterId) -> Result<Self, TryFromProtoError> {
        let id = match proto {
            proto::ClusterId::User(id) => StorageInstanceId::user(id).ok_or_else(|| {
                TryFromProtoError::InvalidPersistState(format!(
                    "{id} is not a valid StorageInstanceId"
                ))
            })?,
            proto::ClusterId::System(id) => StorageInstanceId::system(id).ok_or_else(|| {
                TryFromProtoError::InvalidPersistState(format!(
                    "{id} is not a valid StorageInstanceId"
                ))
            })?,
        };
        Ok(id)
    }
}

impl RustType<proto::ReplicaId> for ReplicaId {
    fn into_proto(&self) -> proto::ReplicaId {
        match self {
            Self::System(id) => proto::ReplicaId::System(*id),
            Self::User(id) => proto::ReplicaId::User(*id),
        }
    }

    fn from_proto(proto: proto::ReplicaId) -> Result<Self, TryFromProtoError> {
        match proto {
            proto::ReplicaId::System(id) => Ok(Self::System(id)),
            proto::ReplicaId::User(id) => Ok(Self::User(id)),
        }
    }
}

impl ProtoMapEntry<String, String> for proto::OptimizerFeatureOverride {
    fn from_rust<'a>(entry: (&'a String, &'a String)) -> Self {
        proto::OptimizerFeatureOverride {
            name: entry.0.into_proto(),
            value: entry.1.into_proto(),
        }
    }

    fn into_rust(self) -> Result<(String, String), TryFromProtoError> {
        Ok((self.name.into_rust()?, self.value.into_rust()?))
    }
}

impl RustType<proto::ClusterSchedule> for ClusterSchedule {
    fn into_proto(&self) -> proto::ClusterSchedule {
        match self {
            ClusterSchedule::Manual => proto::ClusterSchedule::Manual,
            ClusterSchedule::Refresh {
                hydration_time_estimate,
            } => proto::ClusterSchedule::Refresh(proto::ClusterScheduleRefreshOptions {
                rehydration_time_estimate: hydration_time_estimate.into_proto(),
            }),
        }
    }

    fn from_proto(proto: proto::ClusterSchedule) -> Result<Self, TryFromProtoError> {
        match proto {
            proto::ClusterSchedule::Manual => Ok(ClusterSchedule::Manual),
            proto::ClusterSchedule::Refresh(csro) => Ok(ClusterSchedule::Refresh {
                hydration_time_estimate: csro.rehydration_time_estimate.into_rust()?,
            }),
        }
    }
}

impl RustType<proto::AutoScalingStrategy> for AutoScalingStrategy {
    fn into_proto(&self) -> proto::AutoScalingStrategy {
        proto::AutoScalingStrategy {
            on_hydration: self.on_hydration.into_proto(),
        }
    }

    fn from_proto(proto: proto::AutoScalingStrategy) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            on_hydration: proto.on_hydration.into_rust()?,
        })
    }
}

impl RustType<proto::OnHydration> for OnHydration {
    fn into_proto(&self) -> proto::OnHydration {
        proto::OnHydration {
            hydration_size: self.hydration_size.clone(),
            linger_duration: self.linger_duration.into_proto(),
        }
    }

    fn from_proto(proto: proto::OnHydration) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            hydration_size: proto.hydration_size,
            linger_duration: proto.linger_duration.into_rust()?,
        })
    }
}

impl RustType<proto::OnTimeoutAction> for OnTimeoutAction {
    fn into_proto(&self) -> proto::OnTimeoutAction {
        match self {
            OnTimeoutAction::Commit => proto::OnTimeoutAction::Commit,
            OnTimeoutAction::Rollback => proto::OnTimeoutAction::Rollback,
        }
    }

    fn from_proto(proto: proto::OnTimeoutAction) -> Result<Self, TryFromProtoError> {
        Ok(match proto {
            proto::OnTimeoutAction::Commit => OnTimeoutAction::Commit,
            proto::OnTimeoutAction::Rollback => OnTimeoutAction::Rollback,
        })
    }
}

impl RustType<proto::ReplicaLogging> for ComputeReplicaLogging {
    fn into_proto(&self) -> proto::ReplicaLogging {
        proto::ReplicaLogging {
            log_logging: self.log_logging,
            interval: self.interval.into_proto(),
        }
    }

    fn from_proto(proto: proto::ReplicaLogging) -> Result<Self, TryFromProtoError> {
        Ok(ComputeReplicaLogging {
            log_logging: proto.log_logging,
            interval: proto.interval.into_rust()?,
        })
    }
}

impl RustType<proto::Version> for RelationVersion {
    fn into_proto(&self) -> proto::Version {
        proto::Version {
            value: self.into_raw(),
        }
    }

    fn from_proto(proto: proto::Version) -> Result<Self, TryFromProtoError> {
        Ok(RelationVersion::from_raw(proto.value))
    }
}

impl RustType<proto::NetworkPolicyRule> for NetworkPolicyRule {
    fn into_proto(&self) -> proto::NetworkPolicyRule {
        proto::NetworkPolicyRule {
            name: self.name.clone(),
            action: match self.action {
                NetworkPolicyRuleAction::Allow => proto::NetworkPolicyRuleAction::Allow,
            },
            direction: match self.direction {
                NetworkPolicyRuleDirection::Ingress => proto::NetworkPolicyRuleDirection::Ingress,
            },
            address: self.address.clone().to_string(),
        }
    }

    fn from_proto(proto: proto::NetworkPolicyRule) -> Result<Self, TryFromProtoError> {
        Ok(NetworkPolicyRule {
            name: proto.name,
            action: match proto.action {
                proto::NetworkPolicyRuleAction::Allow => NetworkPolicyRuleAction::Allow,
            },
            address: PolicyAddress::from(proto.address),
            direction: match proto.direction {
                proto::NetworkPolicyRuleDirection::Ingress => NetworkPolicyRuleDirection::Ingress,
            },
        })
    }
}

impl RustType<String> for ShardId {
    // Delegates to the `mz_proto` encoding, so that both stay identical.
    fn into_proto(&self) -> String {
        mz_proto::RustType::into_proto(self)
    }

    fn from_proto(proto: String) -> Result<Self, TryFromProtoError> {
        mz_proto::RustType::from_proto(proto)
    }
}
