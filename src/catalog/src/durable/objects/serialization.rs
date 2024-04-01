// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module is responsible for serializing catalog objects into Protobuf.

use mz_audit_log::{
    AlterDefaultPrivilegeV1, AlterRetainHistoryV1, AlterSetClusterV1, AlterSourceSinkV1,
    CreateClusterReplicaV1, CreateSourceSinkV1, CreateSourceSinkV2, CreateSourceSinkV3,
    DropClusterReplicaV1, EventDetails, EventType, EventV1, FromPreviousIdV1, FullNameV1,
    GrantRoleV1, GrantRoleV2, IdFullNameV1, IdNameV1, RenameClusterReplicaV1, RenameClusterV1,
    RenameItemV1, RenameSchemaV1, RevokeRoleV1, RevokeRoleV2, SchemaV1, SchemaV2, StorageUsageV1,
    ToNewIdV1, UpdateItemV1, UpdateOwnerV1, UpdatePrivilegeV1, VersionedEvent,
    VersionedStorageUsage,
};
use mz_compute_client::controller::ComputeReplicaLogging;
use mz_controller_types::ReplicaId;
use mz_ore::cast::CastFrom;
use mz_proto::{IntoRustIfSome, ProtoMapEntry, ProtoType, RustType, TryFromProtoError};
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem};
use mz_repr::role_id::RoleId;
use mz_repr::{GlobalId, Timestamp};
use mz_sql::catalog::{CatalogItemType, ObjectType, RoleAttributes, RoleMembership, RoleVars};
use mz_sql::names::{
    CommentObjectId, DatabaseId, ResolvedDatabaseSpecifier, SchemaId, SchemaSpecifier,
};
use mz_sql::plan::ClusterSchedule;
use mz_sql::session::vars::OwnedVarInput;
use mz_storage_types::instances::StorageInstanceId;
use std::time::Duration;

use crate::durable::objects::serialization::proto::{
    cluster_schedule, ClusterScheduleRefreshOptions, Empty,
};
use crate::durable::objects::state_update::StateUpdateKindRaw;
use crate::durable::objects::{
    AuditLogKey, ClusterIntrospectionSourceIndexKey, ClusterIntrospectionSourceIndexValue,
    ClusterKey, ClusterReplicaKey, ClusterReplicaValue, ClusterValue, CommentKey, CommentValue,
    ConfigKey, ConfigValue, DatabaseKey, DatabaseValue, DefaultPrivilegesKey,
    DefaultPrivilegesValue, GidMappingKey, GidMappingValue, IdAllocKey, IdAllocValue, ItemKey,
    ItemValue, PersistTxnShardValue, RoleKey, RoleValue, SchemaKey, SchemaValue,
    ServerConfigurationKey, ServerConfigurationValue, SettingKey, SettingValue,
    StorageCollectionMetadataKey, StorageCollectionMetadataValue, StorageUsageKey,
    SystemPrivilegesKey, SystemPrivilegesValue, UnfinalizedShardKey,
};
use crate::durable::{
    ClusterConfig, ClusterVariant, ClusterVariantManaged, ReplicaConfig, ReplicaLocation,
};

pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/objects.rs"));
}

impl From<proto::StateUpdateKind> for StateUpdateKindRaw {
    fn from(value: proto::StateUpdateKind) -> Self {
        StateUpdateKindRaw::from_serde(&value)
    }
}

impl TryFrom<StateUpdateKindRaw> for proto::StateUpdateKind {
    type Error = String;

    fn try_from(value: StateUpdateKindRaw) -> Result<Self, Self::Error> {
        value.try_to_serde::<Self>().map_err(|err| err.to_string())
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
            ClusterSchedule::Manual => proto::ClusterSchedule {
                value: Some(cluster_schedule::Value::Manual(Empty {})),
            },
            ClusterSchedule::Refresh {
                rehydration_time_estimate,
            } => proto::ClusterSchedule {
                value: Some(cluster_schedule::Value::Refresh(
                    ClusterScheduleRefreshOptions {
                        rehydration_time_estimate: Some(rehydration_time_estimate.into_proto()),
                    },
                )),
            },
        }
    }

    fn from_proto(proto: proto::ClusterSchedule) -> Result<Self, TryFromProtoError> {
        match proto.value {
            None => Ok(Default::default()),
            Some(cluster_schedule::Value::Manual(Empty {})) => Ok(ClusterSchedule::Manual),
            Some(cluster_schedule::Value::Refresh(csro)) => Ok(ClusterSchedule::Refresh {
                rehydration_time_estimate: csro
                    .rehydration_time_estimate
                    .into_rust_if_some("rehydration_time_estimate")?,
            }),
        }
    }
}

impl RustType<proto::ClusterConfig> for ClusterConfig {
    fn into_proto(&self) -> proto::ClusterConfig {
        proto::ClusterConfig {
            variant: Some(self.variant.into_proto()),
        }
    }

    fn from_proto(proto: proto::ClusterConfig) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            variant: proto.variant.into_rust_if_some("ClusterConfig::variant")?,
        })
    }
}

impl RustType<proto::cluster_config::Variant> for ClusterVariant {
    fn into_proto(&self) -> proto::cluster_config::Variant {
        match self {
            ClusterVariant::Managed(ClusterVariantManaged {
                size,
                availability_zones,
                logging,
                replication_factor,
                disk,
                optimizer_feature_overrides,
                schedule,
            }) => proto::cluster_config::Variant::Managed(proto::cluster_config::ManagedCluster {
                size: size.to_string(),
                availability_zones: availability_zones.clone(),
                logging: Some(logging.into_proto()),
                replication_factor: *replication_factor,
                disk: *disk,
                optimizer_feature_overrides: optimizer_feature_overrides.into_proto(),
                schedule: Some(schedule.into_proto()),
            }),
            ClusterVariant::Unmanaged => proto::cluster_config::Variant::Unmanaged(proto::Empty {}),
        }
    }

    fn from_proto(proto: proto::cluster_config::Variant) -> Result<Self, TryFromProtoError> {
        match proto {
            proto::cluster_config::Variant::Unmanaged(_) => Ok(Self::Unmanaged),
            proto::cluster_config::Variant::Managed(managed) => {
                Ok(Self::Managed(ClusterVariantManaged {
                    size: managed.size,
                    availability_zones: managed.availability_zones,
                    logging: managed
                        .logging
                        .into_rust_if_some("ManagedCluster::logging")?,
                    replication_factor: managed.replication_factor,
                    disk: managed.disk,
                    optimizer_feature_overrides: managed.optimizer_feature_overrides.into_rust()?,
                    schedule: managed.schedule.unwrap_or_default().into_rust()?,
                }))
            }
        }
    }
}

impl RustType<proto::ReplicaConfig> for ReplicaConfig {
    fn into_proto(&self) -> proto::ReplicaConfig {
        proto::ReplicaConfig {
            logging: Some(self.logging.into_proto()),
            location: Some(self.location.into_proto()),
        }
    }

    fn from_proto(proto: proto::ReplicaConfig) -> Result<Self, TryFromProtoError> {
        Ok(ReplicaConfig {
            location: proto
                .location
                .into_rust_if_some("ReplicaConfig::location")?,
            logging: proto.logging.into_rust_if_some("ReplicaConfig::logging")?,
        })
    }
}

impl RustType<proto::replica_config::Location> for ReplicaLocation {
    fn into_proto(&self) -> proto::replica_config::Location {
        match self {
            ReplicaLocation::Unmanaged {
                storagectl_addrs,
                storage_addrs,
                computectl_addrs,
                compute_addrs,
                workers,
            } => proto::replica_config::Location::Unmanaged(
                proto::replica_config::UnmanagedLocation {
                    storagectl_addrs: storagectl_addrs.clone(),
                    storage_addrs: storage_addrs.clone(),
                    computectl_addrs: computectl_addrs.clone(),
                    compute_addrs: compute_addrs.clone(),
                    workers: CastFrom::cast_from(*workers),
                },
            ),
            ReplicaLocation::Managed {
                size,
                availability_zone,
                disk,
                billed_as,
                internal,
            } => proto::replica_config::Location::Managed(proto::replica_config::ManagedLocation {
                size: size.to_string(),
                availability_zone: availability_zone.clone(),
                disk: *disk,
                billed_as: billed_as.clone(),
                internal: *internal,
            }),
        }
    }

    fn from_proto(proto: proto::replica_config::Location) -> Result<Self, TryFromProtoError> {
        match proto {
            proto::replica_config::Location::Unmanaged(location) => {
                Ok(ReplicaLocation::Unmanaged {
                    storagectl_addrs: location.storagectl_addrs,
                    storage_addrs: location.storage_addrs,
                    computectl_addrs: location.computectl_addrs,
                    compute_addrs: location.compute_addrs,
                    workers: CastFrom::cast_from(location.workers),
                })
            }
            proto::replica_config::Location::Managed(location) => Ok(ReplicaLocation::Managed {
                availability_zone: location.availability_zone,
                billed_as: location.billed_as,
                disk: location.disk,
                internal: location.internal,
                size: location.size,
            }),
        }
    }
}

impl RustType<proto::SettingKey> for SettingKey {
    fn into_proto(&self) -> proto::SettingKey {
        proto::SettingKey {
            name: self.name.to_string(),
        }
    }

    fn from_proto(proto: proto::SettingKey) -> Result<Self, TryFromProtoError> {
        Ok(SettingKey { name: proto.name })
    }
}

impl RustType<proto::SettingValue> for SettingValue {
    fn into_proto(&self) -> proto::SettingValue {
        proto::SettingValue {
            value: self.value.to_string(),
        }
    }

    fn from_proto(proto: proto::SettingValue) -> Result<Self, TryFromProtoError> {
        Ok(SettingValue { value: proto.value })
    }
}

impl RustType<proto::IdAllocKey> for IdAllocKey {
    fn into_proto(&self) -> proto::IdAllocKey {
        proto::IdAllocKey {
            name: self.name.to_string(),
        }
    }

    fn from_proto(proto: proto::IdAllocKey) -> Result<Self, TryFromProtoError> {
        Ok(IdAllocKey { name: proto.name })
    }
}

impl RustType<proto::IdAllocValue> for IdAllocValue {
    fn into_proto(&self) -> proto::IdAllocValue {
        proto::IdAllocValue {
            next_id: self.next_id,
        }
    }

    fn from_proto(proto: proto::IdAllocValue) -> Result<Self, TryFromProtoError> {
        Ok(IdAllocValue {
            next_id: proto.next_id,
        })
    }
}

impl RustType<proto::GidMappingKey> for GidMappingKey {
    fn into_proto(&self) -> proto::GidMappingKey {
        proto::GidMappingKey {
            schema_name: self.schema_name.to_string(),
            object_type: self.object_type.into_proto().into(),
            object_name: self.object_name.to_string(),
        }
    }

    fn from_proto(proto: proto::GidMappingKey) -> Result<Self, TryFromProtoError> {
        let object_type = proto::CatalogItemType::from_i32(proto.object_type)
            .ok_or_else(|| TryFromProtoError::unknown_enum_variant("CatalogItemType"))?;
        Ok(GidMappingKey {
            schema_name: proto.schema_name,
            object_type: object_type.into_rust()?,
            object_name: proto.object_name,
        })
    }
}

impl RustType<proto::GidMappingValue> for GidMappingValue {
    fn into_proto(&self) -> proto::GidMappingValue {
        proto::GidMappingValue {
            id: self.id,
            fingerprint: self.fingerprint.to_string(),
        }
    }

    fn from_proto(proto: proto::GidMappingValue) -> Result<Self, TryFromProtoError> {
        Ok(GidMappingValue {
            id: proto.id,
            fingerprint: proto.fingerprint,
        })
    }
}

impl RustType<proto::ClusterKey> for ClusterKey {
    fn into_proto(&self) -> proto::ClusterKey {
        proto::ClusterKey {
            id: Some(self.id.into_proto()),
        }
    }

    fn from_proto(proto: proto::ClusterKey) -> Result<Self, TryFromProtoError> {
        Ok(ClusterKey {
            id: proto.id.into_rust_if_some("ClusterKey::id")?,
        })
    }
}

impl RustType<proto::ClusterValue> for ClusterValue {
    fn into_proto(&self) -> proto::ClusterValue {
        proto::ClusterValue {
            name: self.name.to_string(),
            config: Some(self.config.into_proto()),
            owner_id: Some(self.owner_id.into_proto()),
            privileges: self.privileges.into_proto(),
        }
    }

    fn from_proto(proto: proto::ClusterValue) -> Result<Self, TryFromProtoError> {
        Ok(ClusterValue {
            name: proto.name,
            config: proto.config.unwrap_or_default().into_rust()?,
            owner_id: proto.owner_id.into_rust_if_some("ClusterValue::owner_id")?,
            privileges: proto.privileges.into_rust()?,
        })
    }
}

impl RustType<proto::ClusterIntrospectionSourceIndexKey> for ClusterIntrospectionSourceIndexKey {
    fn into_proto(&self) -> proto::ClusterIntrospectionSourceIndexKey {
        proto::ClusterIntrospectionSourceIndexKey {
            cluster_id: Some(self.cluster_id.into_proto()),
            name: self.name.to_string(),
        }
    }

    fn from_proto(
        proto: proto::ClusterIntrospectionSourceIndexKey,
    ) -> Result<Self, TryFromProtoError> {
        Ok(ClusterIntrospectionSourceIndexKey {
            cluster_id: proto
                .cluster_id
                .into_rust_if_some("ClusterIntrospectionSourceIndexKey::cluster_id")?,
            name: proto.name,
        })
    }
}

impl RustType<proto::ClusterIntrospectionSourceIndexValue>
    for ClusterIntrospectionSourceIndexValue
{
    fn into_proto(&self) -> proto::ClusterIntrospectionSourceIndexValue {
        proto::ClusterIntrospectionSourceIndexValue {
            index_id: self.index_id,
            oid: self.oid,
        }
    }

    fn from_proto(
        proto: proto::ClusterIntrospectionSourceIndexValue,
    ) -> Result<Self, TryFromProtoError> {
        Ok(ClusterIntrospectionSourceIndexValue {
            index_id: proto.index_id,
            oid: proto.oid,
        })
    }
}

impl RustType<proto::ClusterReplicaKey> for ClusterReplicaKey {
    fn into_proto(&self) -> proto::ClusterReplicaKey {
        proto::ClusterReplicaKey {
            id: Some(self.id.into_proto()),
        }
    }

    fn from_proto(proto: proto::ClusterReplicaKey) -> Result<Self, TryFromProtoError> {
        Ok(ClusterReplicaKey {
            id: proto.id.into_rust_if_some("ClusterReplicaKey::id")?,
        })
    }
}

impl RustType<proto::ClusterReplicaValue> for ClusterReplicaValue {
    fn into_proto(&self) -> proto::ClusterReplicaValue {
        proto::ClusterReplicaValue {
            cluster_id: Some(self.cluster_id.into_proto()),
            name: self.name.to_string(),
            config: Some(self.config.into_proto()),
            owner_id: Some(self.owner_id.into_proto()),
        }
    }

    fn from_proto(proto: proto::ClusterReplicaValue) -> Result<Self, TryFromProtoError> {
        Ok(ClusterReplicaValue {
            cluster_id: proto
                .cluster_id
                .into_rust_if_some("ClusterReplicaValue::cluster_id")?,
            name: proto.name,
            config: proto
                .config
                .into_rust_if_some("ClusterReplicaValue::config")?,
            owner_id: proto
                .owner_id
                .into_rust_if_some("ClusterReplicaValue::owner_id")?,
        })
    }
}

impl RustType<proto::DatabaseKey> for DatabaseKey {
    fn into_proto(&self) -> proto::DatabaseKey {
        proto::DatabaseKey {
            id: Some(self.id.into_proto()),
        }
    }

    fn from_proto(proto: proto::DatabaseKey) -> Result<Self, TryFromProtoError> {
        Ok(DatabaseKey {
            id: proto.id.into_rust_if_some("DatabaseKey::value")?,
        })
    }
}

impl RustType<proto::DatabaseValue> for DatabaseValue {
    fn into_proto(&self) -> proto::DatabaseValue {
        proto::DatabaseValue {
            name: self.name.clone(),
            owner_id: Some(self.owner_id.into_proto()),
            privileges: self.privileges.into_proto(),
            oid: self.oid,
        }
    }

    fn from_proto(proto: proto::DatabaseValue) -> Result<Self, TryFromProtoError> {
        Ok(DatabaseValue {
            name: proto.name,
            owner_id: (proto
                .owner_id
                .into_rust_if_some("DatabaseValue::owner_id")?),
            privileges: proto.privileges.into_rust()?,
            oid: proto.oid,
        })
    }
}

impl RustType<proto::SchemaKey> for SchemaKey {
    fn into_proto(&self) -> proto::SchemaKey {
        proto::SchemaKey {
            id: Some(self.id.into_proto()),
        }
    }

    fn from_proto(proto: proto::SchemaKey) -> Result<Self, TryFromProtoError> {
        Ok(SchemaKey {
            id: proto.id.into_rust_if_some("SchemaKey::id")?,
        })
    }
}

impl RustType<proto::SchemaValue> for SchemaValue {
    fn into_proto(&self) -> proto::SchemaValue {
        proto::SchemaValue {
            name: self.name.clone(),
            database_id: self.database_id.map(|id| id.into_proto()),
            owner_id: Some(self.owner_id.into_proto()),
            privileges: self.privileges.into_proto(),
            oid: self.oid,
        }
    }

    fn from_proto(proto: proto::SchemaValue) -> Result<Self, TryFromProtoError> {
        Ok(SchemaValue {
            name: proto.name,
            database_id: proto.database_id.into_rust()?,
            owner_id: (proto.owner_id.into_rust_if_some("SchemaValue::owner_id")?),
            privileges: proto.privileges.into_rust()?,
            oid: proto.oid,
        })
    }
}

impl RustType<proto::ItemKey> for ItemKey {
    fn into_proto(&self) -> proto::ItemKey {
        proto::ItemKey {
            gid: Some(self.gid.into_proto()),
        }
    }

    fn from_proto(proto: proto::ItemKey) -> Result<Self, TryFromProtoError> {
        Ok(ItemKey {
            gid: proto.gid.into_rust_if_some("ItemKey::gid")?,
        })
    }
}

impl RustType<proto::ItemValue> for ItemValue {
    fn into_proto(&self) -> proto::ItemValue {
        let definition = proto::CatalogItem {
            value: Some(proto::catalog_item::Value::V1(proto::catalog_item::V1 {
                create_sql: self.create_sql.clone(),
            })),
        };
        proto::ItemValue {
            schema_id: Some(self.schema_id.into_proto()),
            name: self.name.to_string(),
            definition: Some(definition),
            owner_id: Some(self.owner_id.into_proto()),
            privileges: self.privileges.into_proto(),
            oid: self.oid,
        }
    }

    fn from_proto(proto: proto::ItemValue) -> Result<Self, TryFromProtoError> {
        let create_sql_value = proto
            .definition
            .ok_or_else(|| TryFromProtoError::missing_field("ItemValue::definition"))?
            .value
            .ok_or_else(|| TryFromProtoError::missing_field("CatalogItem::value"))?;
        let create_sql = match create_sql_value {
            proto::catalog_item::Value::V1(c) => c.create_sql,
        };
        Ok(ItemValue {
            schema_id: proto.schema_id.into_rust_if_some("ItemValue::schema_id")?,
            name: proto.name,
            create_sql,
            owner_id: proto.owner_id.into_rust_if_some("ItemValue::owner_id")?,
            privileges: proto.privileges.into_rust()?,
            oid: proto.oid,
        })
    }
}

impl RustType<proto::CommentKey> for CommentKey {
    fn into_proto(&self) -> proto::CommentKey {
        let sub_component = match &self.sub_component {
            Some(pos) => Some(proto::comment_key::SubComponent::ColumnPos(
                CastFrom::cast_from(*pos),
            )),
            None => None,
        };
        proto::CommentKey {
            object: Some(self.object_id.into_proto()),
            sub_component,
        }
    }

    fn from_proto(proto: proto::CommentKey) -> Result<Self, TryFromProtoError> {
        let sub_component = match proto.sub_component {
            Some(proto::comment_key::SubComponent::ColumnPos(pos)) => {
                Some(CastFrom::cast_from(pos))
            }
            None => None,
        };
        Ok(CommentKey {
            object_id: proto.object.into_rust_if_some("CommentKey::object")?,
            sub_component,
        })
    }
}

impl RustType<proto::CommentValue> for CommentValue {
    fn into_proto(&self) -> proto::CommentValue {
        proto::CommentValue {
            comment: self.comment.clone(),
        }
    }

    fn from_proto(proto: proto::CommentValue) -> Result<Self, TryFromProtoError> {
        Ok(CommentValue {
            comment: proto.comment,
        })
    }
}

impl RustType<proto::RoleKey> for RoleKey {
    fn into_proto(&self) -> proto::RoleKey {
        proto::RoleKey {
            id: Some(self.id.into_proto()),
        }
    }

    fn from_proto(proto: proto::RoleKey) -> Result<Self, TryFromProtoError> {
        Ok(RoleKey {
            id: proto.id.into_rust_if_some("RoleKey::id")?,
        })
    }
}

impl RustType<proto::RoleValue> for RoleValue {
    fn into_proto(&self) -> proto::RoleValue {
        proto::RoleValue {
            name: self.name.to_string(),
            attributes: Some(self.attributes.into_proto()),
            membership: Some(self.membership.into_proto()),
            vars: Some(self.vars.into_proto()),
            oid: self.oid,
        }
    }

    fn from_proto(proto: proto::RoleValue) -> Result<Self, TryFromProtoError> {
        Ok(RoleValue {
            name: proto.name,
            attributes: proto
                .attributes
                .into_rust_if_some("RoleValue::attributes")?,
            membership: proto
                .membership
                .into_rust_if_some("RoleValue::membership")?,
            vars: proto.vars.into_rust_if_some("RoleValue::vars")?,
            oid: proto.oid,
        })
    }
}

impl RustType<proto::ConfigKey> for ConfigKey {
    fn into_proto(&self) -> proto::ConfigKey {
        proto::ConfigKey {
            key: self.key.to_string(),
        }
    }

    fn from_proto(proto: proto::ConfigKey) -> Result<Self, TryFromProtoError> {
        Ok(ConfigKey { key: proto.key })
    }
}

impl RustType<proto::ConfigValue> for ConfigValue {
    fn into_proto(&self) -> proto::ConfigValue {
        proto::ConfigValue { value: self.value }
    }

    fn from_proto(proto: proto::ConfigValue) -> Result<Self, TryFromProtoError> {
        Ok(ConfigValue { value: proto.value })
    }
}

impl RustType<proto::AuditLogKey> for AuditLogKey {
    fn into_proto(&self) -> proto::AuditLogKey {
        proto::AuditLogKey {
            event: Some(self.event.into_proto()),
        }
    }

    fn from_proto(proto: proto::AuditLogKey) -> Result<Self, TryFromProtoError> {
        Ok(AuditLogKey {
            event: proto.event.into_rust_if_some("AuditLogKey::event")?,
        })
    }
}

impl RustType<proto::StorageUsageKey> for StorageUsageKey {
    fn into_proto(&self) -> proto::StorageUsageKey {
        proto::StorageUsageKey {
            usage: Some(self.metric.into_proto()),
        }
    }

    fn from_proto(proto: proto::StorageUsageKey) -> Result<Self, TryFromProtoError> {
        Ok(StorageUsageKey {
            metric: proto.usage.into_rust_if_some("StorageUsageKey::usage")?,
        })
    }
}

impl RustType<proto::StorageCollectionMetadataKey> for StorageCollectionMetadataKey {
    fn into_proto(&self) -> proto::StorageCollectionMetadataKey {
        proto::StorageCollectionMetadataKey {
            id: Some(self.id.into_proto()),
        }
    }

    fn from_proto(proto: proto::StorageCollectionMetadataKey) -> Result<Self, TryFromProtoError> {
        Ok(StorageCollectionMetadataKey {
            id: proto
                .id
                .into_rust_if_some("StorageCollectionMetadataKey::id")?,
        })
    }
}

impl RustType<proto::StorageCollectionMetadataValue> for StorageCollectionMetadataValue {
    fn into_proto(&self) -> proto::StorageCollectionMetadataValue {
        proto::StorageCollectionMetadataValue {
            shard: self.shard.to_string(),
        }
    }

    fn from_proto(proto: proto::StorageCollectionMetadataValue) -> Result<Self, TryFromProtoError> {
        Ok(StorageCollectionMetadataValue { shard: proto.shard })
    }
}

impl RustType<proto::UnfinalizedShardKey> for UnfinalizedShardKey {
    fn into_proto(&self) -> proto::UnfinalizedShardKey {
        proto::UnfinalizedShardKey {
            shard: self.shard.to_string(),
        }
    }

    fn from_proto(proto: proto::UnfinalizedShardKey) -> Result<Self, TryFromProtoError> {
        Ok(UnfinalizedShardKey { shard: proto.shard })
    }
}

impl RustType<proto::PersistTxnShardValue> for PersistTxnShardValue {
    fn into_proto(&self) -> proto::PersistTxnShardValue {
        proto::PersistTxnShardValue {
            shard: self.shard.to_string(),
        }
    }

    fn from_proto(proto: proto::PersistTxnShardValue) -> Result<Self, TryFromProtoError> {
        Ok(PersistTxnShardValue { shard: proto.shard })
    }
}

impl RustType<proto::ServerConfigurationKey> for ServerConfigurationKey {
    fn into_proto(&self) -> proto::ServerConfigurationKey {
        proto::ServerConfigurationKey {
            name: self.name.clone(),
        }
    }

    fn from_proto(proto: proto::ServerConfigurationKey) -> Result<Self, TryFromProtoError> {
        Ok(ServerConfigurationKey { name: proto.name })
    }
}

impl RustType<proto::ServerConfigurationValue> for ServerConfigurationValue {
    fn into_proto(&self) -> proto::ServerConfigurationValue {
        proto::ServerConfigurationValue {
            value: self.value.clone(),
        }
    }

    fn from_proto(proto: proto::ServerConfigurationValue) -> Result<Self, TryFromProtoError> {
        Ok(ServerConfigurationValue { value: proto.value })
    }
}

impl RustType<proto::DefaultPrivilegesKey> for DefaultPrivilegesKey {
    fn into_proto(&self) -> proto::DefaultPrivilegesKey {
        proto::DefaultPrivilegesKey {
            role_id: Some(self.role_id.into_proto()),
            database_id: self.database_id.map(|database_id| database_id.into_proto()),
            schema_id: self.schema_id.map(|schema_id| schema_id.into_proto()),
            object_type: self.object_type.into_proto().into(),
            grantee: Some(self.grantee.into_proto()),
        }
    }

    fn from_proto(proto: proto::DefaultPrivilegesKey) -> Result<Self, TryFromProtoError> {
        Ok(DefaultPrivilegesKey {
            role_id: proto
                .role_id
                .into_rust_if_some("DefaultPrivilegesKey::role_id")?,
            database_id: proto.database_id.into_rust()?,
            schema_id: proto.schema_id.into_rust()?,
            object_type: proto::ObjectType::from_i32(proto.object_type)
                .ok_or_else(|| TryFromProtoError::unknown_enum_variant("ObjectType"))?
                .into_rust()?,
            grantee: proto
                .grantee
                .into_rust_if_some("DefaultPrivilegesKey::grantee")?,
        })
    }
}

impl RustType<proto::DefaultPrivilegesValue> for DefaultPrivilegesValue {
    fn into_proto(&self) -> proto::DefaultPrivilegesValue {
        proto::DefaultPrivilegesValue {
            privileges: Some(self.privileges.into_proto()),
        }
    }

    fn from_proto(proto: proto::DefaultPrivilegesValue) -> Result<Self, TryFromProtoError> {
        Ok(DefaultPrivilegesValue {
            privileges: proto
                .privileges
                .into_rust_if_some("DefaultPrivilegesValue::privileges")?,
        })
    }
}

impl RustType<proto::SystemPrivilegesKey> for SystemPrivilegesKey {
    fn into_proto(&self) -> proto::SystemPrivilegesKey {
        proto::SystemPrivilegesKey {
            grantee: Some(self.grantee.into_proto()),
            grantor: Some(self.grantor.into_proto()),
        }
    }

    fn from_proto(proto: proto::SystemPrivilegesKey) -> Result<Self, TryFromProtoError> {
        Ok(SystemPrivilegesKey {
            grantee: proto
                .grantee
                .into_rust_if_some("SystemPrivilegesKey::grantee")?,
            grantor: proto
                .grantor
                .into_rust_if_some("SystemPrivilegesKey::grantor")?,
        })
    }
}

impl RustType<proto::SystemPrivilegesValue> for SystemPrivilegesValue {
    fn into_proto(&self) -> proto::SystemPrivilegesValue {
        proto::SystemPrivilegesValue {
            acl_mode: Some(self.acl_mode.into_proto()),
        }
    }

    fn from_proto(proto: proto::SystemPrivilegesValue) -> Result<Self, TryFromProtoError> {
        Ok(SystemPrivilegesValue {
            acl_mode: proto
                .acl_mode
                .into_rust_if_some("SystemPrivilegesKey::acl_mode")?,
        })
    }
}

impl RustType<proto::ClusterId> for StorageInstanceId {
    fn into_proto(&self) -> proto::ClusterId {
        let value = match self {
            StorageInstanceId::User(id) => proto::cluster_id::Value::User(*id),
            StorageInstanceId::System(id) => proto::cluster_id::Value::System(*id),
        };

        proto::ClusterId { value: Some(value) }
    }

    fn from_proto(proto: proto::ClusterId) -> Result<Self, TryFromProtoError> {
        let value = proto
            .value
            .ok_or_else(|| TryFromProtoError::missing_field("ClusterId::value"))?;
        let id = match value {
            proto::cluster_id::Value::User(id) => StorageInstanceId::User(id),
            proto::cluster_id::Value::System(id) => StorageInstanceId::System(id),
        };
        Ok(id)
    }
}

impl RustType<proto::ReplicaId> for ReplicaId {
    fn into_proto(&self) -> proto::ReplicaId {
        use proto::replica_id::Value::*;
        proto::ReplicaId {
            value: Some(match self {
                Self::System(id) => System(*id),
                Self::User(id) => User(*id),
            }),
        }
    }

    fn from_proto(proto: proto::ReplicaId) -> Result<Self, TryFromProtoError> {
        use proto::replica_id::Value::*;
        match proto.value {
            Some(System(id)) => Ok(Self::System(id)),
            Some(User(id)) => Ok(Self::User(id)),
            None => Err(TryFromProtoError::missing_field("ReplicaId::value")),
        }
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

impl RustType<proto::RoleId> for RoleId {
    fn into_proto(&self) -> proto::RoleId {
        let value = match self {
            RoleId::User(id) => proto::role_id::Value::User(*id),
            RoleId::System(id) => proto::role_id::Value::System(*id),
            RoleId::Predefined(id) => proto::role_id::Value::Predefined(*id),
            RoleId::Public => proto::role_id::Value::Public(Default::default()),
        };

        proto::RoleId { value: Some(value) }
    }

    fn from_proto(proto: proto::RoleId) -> Result<Self, TryFromProtoError> {
        let value = proto
            .value
            .ok_or_else(|| TryFromProtoError::missing_field("RoleId::value"))?;
        let id = match value {
            proto::role_id::Value::User(id) => RoleId::User(id),
            proto::role_id::Value::System(id) => RoleId::System(id),
            proto::role_id::Value::Predefined(id) => RoleId::Predefined(id),
            proto::role_id::Value::Public(_) => RoleId::Public,
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
            grantee: Some(self.grantee.into_proto()),
            grantor: Some(self.grantor.into_proto()),
            acl_mode: Some(self.acl_mode.into_proto()),
        }
    }

    fn from_proto(proto: proto::MzAclItem) -> Result<Self, TryFromProtoError> {
        Ok(MzAclItem {
            grantee: proto.grantee.into_rust_if_some("MzAclItem::grantee")?,
            grantor: proto.grantor.into_rust_if_some("MzAclItem::grantor")?,
            acl_mode: proto.acl_mode.into_rust_if_some("MzAclItem::acl_mode")?,
        })
    }
}

impl RustType<proto::RoleAttributes> for RoleAttributes {
    fn into_proto(&self) -> proto::RoleAttributes {
        proto::RoleAttributes {
            inherit: self.inherit,
        }
    }

    fn from_proto(proto: proto::RoleAttributes) -> Result<Self, TryFromProtoError> {
        let mut attributes = RoleAttributes::new();

        attributes.inherit = proto.inherit;

        Ok(attributes)
    }
}

impl RustType<proto::role_vars::entry::Val> for OwnedVarInput {
    fn into_proto(&self) -> proto::role_vars::entry::Val {
        match self.clone() {
            OwnedVarInput::Flat(v) => proto::role_vars::entry::Val::Flat(v),
            OwnedVarInput::SqlSet(entries) => {
                proto::role_vars::entry::Val::SqlSet(proto::role_vars::SqlSet { entries })
            }
        }
    }

    fn from_proto(proto: proto::role_vars::entry::Val) -> Result<Self, TryFromProtoError> {
        let result = match proto {
            proto::role_vars::entry::Val::Flat(v) => OwnedVarInput::Flat(v),
            proto::role_vars::entry::Val::SqlSet(proto::role_vars::SqlSet { entries }) => {
                OwnedVarInput::SqlSet(entries)
            }
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
            .map(|(key, val)| proto::role_vars::Entry {
                key,
                val: Some(val.into_proto()),
            })
            .collect();

        proto::RoleVars { entries }
    }

    fn from_proto(proto: proto::RoleVars) -> Result<Self, TryFromProtoError> {
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
                .map(|(key, val)| proto::role_membership::Entry {
                    key: Some(key.into_proto()),
                    value: Some(val.into_proto()),
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
                    let key = e.key.into_rust_if_some("RoleMembership::Entry::key")?;
                    let val = e.value.into_rust_if_some("RoleMembership::Entry::value")?;

                    Ok((key, val))
                })
                .collect::<Result<_, TryFromProtoError>>()?,
        })
    }
}

impl RustType<proto::ResolvedDatabaseSpecifier> for ResolvedDatabaseSpecifier {
    fn into_proto(&self) -> proto::ResolvedDatabaseSpecifier {
        let spec = match self {
            ResolvedDatabaseSpecifier::Ambient => {
                proto::resolved_database_specifier::Spec::Ambient(Default::default())
            }
            ResolvedDatabaseSpecifier::Id(database_id) => {
                proto::resolved_database_specifier::Spec::Id(database_id.into_proto())
            }
        };
        proto::ResolvedDatabaseSpecifier { spec: Some(spec) }
    }

    fn from_proto(proto: proto::ResolvedDatabaseSpecifier) -> Result<Self, TryFromProtoError> {
        let spec = proto
            .spec
            .ok_or_else(|| TryFromProtoError::missing_field("ResolvedDatabaseSpecifier::spec"))?;
        let spec = match spec {
            proto::resolved_database_specifier::Spec::Ambient(_) => {
                ResolvedDatabaseSpecifier::Ambient
            }
            proto::resolved_database_specifier::Spec::Id(database_id) => {
                ResolvedDatabaseSpecifier::Id(database_id.into_rust()?)
            }
        };
        Ok(spec)
    }
}

impl RustType<proto::SchemaSpecifier> for SchemaSpecifier {
    fn into_proto(&self) -> proto::SchemaSpecifier {
        let spec = match self {
            SchemaSpecifier::Temporary => {
                proto::schema_specifier::Spec::Temporary(Default::default())
            }
            SchemaSpecifier::Id(schema_id) => {
                proto::schema_specifier::Spec::Id(schema_id.into_proto())
            }
        };
        proto::SchemaSpecifier { spec: Some(spec) }
    }

    fn from_proto(proto: proto::SchemaSpecifier) -> Result<Self, TryFromProtoError> {
        let spec = proto
            .spec
            .ok_or_else(|| TryFromProtoError::missing_field("SchemaSpecifier::spec"))?;
        let spec = match spec {
            proto::schema_specifier::Spec::Temporary(_) => SchemaSpecifier::Temporary,
            proto::schema_specifier::Spec::Id(schema_id) => {
                SchemaSpecifier::Id(schema_id.into_rust()?)
            }
        };
        Ok(spec)
    }
}

impl RustType<proto::SchemaId> for SchemaId {
    fn into_proto(&self) -> proto::SchemaId {
        let value = match self {
            SchemaId::User(id) => proto::schema_id::Value::User(*id),
            SchemaId::System(id) => proto::schema_id::Value::System(*id),
        };

        proto::SchemaId { value: Some(value) }
    }

    fn from_proto(proto: proto::SchemaId) -> Result<Self, TryFromProtoError> {
        let value = proto
            .value
            .ok_or_else(|| TryFromProtoError::missing_field("SchemaId::value"))?;
        let id = match value {
            proto::schema_id::Value::User(id) => SchemaId::User(id),
            proto::schema_id::Value::System(id) => SchemaId::System(id),
        };
        Ok(id)
    }
}

impl RustType<proto::DatabaseId> for DatabaseId {
    fn into_proto(&self) -> proto::DatabaseId {
        let value = match self {
            DatabaseId::User(id) => proto::database_id::Value::User(*id),
            DatabaseId::System(id) => proto::database_id::Value::System(*id),
        };

        proto::DatabaseId { value: Some(value) }
    }

    fn from_proto(proto: proto::DatabaseId) -> Result<Self, TryFromProtoError> {
        match proto.value {
            Some(proto::database_id::Value::User(id)) => Ok(DatabaseId::User(id)),
            Some(proto::database_id::Value::System(id)) => Ok(DatabaseId::System(id)),
            None => Err(TryFromProtoError::missing_field("DatabaseId::value")),
        }
    }
}

impl RustType<proto::comment_key::Object> for CommentObjectId {
    fn into_proto(&self) -> proto::comment_key::Object {
        match self {
            CommentObjectId::Table(global_id) => {
                proto::comment_key::Object::Table(global_id.into_proto())
            }
            CommentObjectId::View(global_id) => {
                proto::comment_key::Object::View(global_id.into_proto())
            }
            CommentObjectId::MaterializedView(global_id) => {
                proto::comment_key::Object::MaterializedView(global_id.into_proto())
            }
            CommentObjectId::Source(global_id) => {
                proto::comment_key::Object::Source(global_id.into_proto())
            }
            CommentObjectId::Sink(global_id) => {
                proto::comment_key::Object::Sink(global_id.into_proto())
            }
            CommentObjectId::Index(global_id) => {
                proto::comment_key::Object::Index(global_id.into_proto())
            }
            CommentObjectId::Func(global_id) => {
                proto::comment_key::Object::Func(global_id.into_proto())
            }
            CommentObjectId::Connection(global_id) => {
                proto::comment_key::Object::Connection(global_id.into_proto())
            }
            CommentObjectId::Type(global_id) => {
                proto::comment_key::Object::Type(global_id.into_proto())
            }
            CommentObjectId::Secret(global_id) => {
                proto::comment_key::Object::Secret(global_id.into_proto())
            }
            CommentObjectId::Role(role_id) => {
                proto::comment_key::Object::Role(role_id.into_proto())
            }
            CommentObjectId::Database(database_id) => {
                proto::comment_key::Object::Database(database_id.into_proto())
            }
            CommentObjectId::Schema((database, schema)) => {
                proto::comment_key::Object::Schema(proto::ResolvedSchema {
                    database: Some(database.into_proto()),
                    schema: Some(schema.into_proto()),
                })
            }
            CommentObjectId::Cluster(cluster_id) => {
                proto::comment_key::Object::Cluster(cluster_id.into_proto())
            }
            CommentObjectId::ClusterReplica((cluster_id, replica_id)) => {
                let cluster_replica_id = proto::ClusterReplicaId {
                    cluster_id: Some(cluster_id.into_proto()),
                    replica_id: Some(replica_id.into_proto()),
                };
                proto::comment_key::Object::ClusterReplica(cluster_replica_id)
            }
        }
    }

    fn from_proto(proto: proto::comment_key::Object) -> Result<Self, TryFromProtoError> {
        let id = match proto {
            proto::comment_key::Object::Table(global_id) => {
                CommentObjectId::Table(global_id.into_rust()?)
            }
            proto::comment_key::Object::View(global_id) => {
                CommentObjectId::View(global_id.into_rust()?)
            }
            proto::comment_key::Object::MaterializedView(global_id) => {
                CommentObjectId::MaterializedView(global_id.into_rust()?)
            }
            proto::comment_key::Object::Source(global_id) => {
                CommentObjectId::Source(global_id.into_rust()?)
            }
            proto::comment_key::Object::Sink(global_id) => {
                CommentObjectId::Sink(global_id.into_rust()?)
            }
            proto::comment_key::Object::Index(global_id) => {
                CommentObjectId::Index(global_id.into_rust()?)
            }
            proto::comment_key::Object::Func(global_id) => {
                CommentObjectId::Func(global_id.into_rust()?)
            }
            proto::comment_key::Object::Connection(global_id) => {
                CommentObjectId::Connection(global_id.into_rust()?)
            }
            proto::comment_key::Object::Type(global_id) => {
                CommentObjectId::Type(global_id.into_rust()?)
            }
            proto::comment_key::Object::Secret(global_id) => {
                CommentObjectId::Secret(global_id.into_rust()?)
            }
            proto::comment_key::Object::Role(role_id) => {
                CommentObjectId::Role(role_id.into_rust()?)
            }
            proto::comment_key::Object::Database(database_id) => {
                CommentObjectId::Database(database_id.into_rust()?)
            }
            proto::comment_key::Object::Schema(resolved_schema) => {
                let database = resolved_schema
                    .database
                    .into_rust_if_some("ResolvedSchema::database")?;
                let schema = resolved_schema
                    .schema
                    .into_rust_if_some("ResolvedSchema::schema")?;
                CommentObjectId::Schema((database, schema))
            }
            proto::comment_key::Object::Cluster(cluster_id) => {
                CommentObjectId::Cluster(cluster_id.into_rust()?)
            }
            proto::comment_key::Object::ClusterReplica(cluster_replica_id) => {
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

impl RustType<proto::EpochMillis> for u64 {
    fn into_proto(&self) -> proto::EpochMillis {
        proto::EpochMillis { millis: *self }
    }

    fn from_proto(proto: proto::EpochMillis) -> Result<Self, TryFromProtoError> {
        Ok(proto.millis)
    }
}

impl RustType<proto::Timestamp> for Timestamp {
    fn into_proto(&self) -> proto::Timestamp {
        proto::Timestamp {
            internal: self.into(),
        }
    }

    fn from_proto(proto: proto::Timestamp) -> Result<Self, TryFromProtoError> {
        Ok(Timestamp::new(proto.internal))
    }
}

impl RustType<proto::GlobalId> for GlobalId {
    fn into_proto(&self) -> proto::GlobalId {
        proto::GlobalId {
            value: Some(match self {
                GlobalId::System(x) => proto::global_id::Value::System(*x),
                GlobalId::User(x) => proto::global_id::Value::User(*x),
                GlobalId::Transient(x) => proto::global_id::Value::Transient(*x),
                GlobalId::Explain => proto::global_id::Value::Explain(Default::default()),
            }),
        }
    }

    fn from_proto(proto: proto::GlobalId) -> Result<Self, TryFromProtoError> {
        match proto.value {
            Some(proto::global_id::Value::System(x)) => Ok(GlobalId::System(x)),
            Some(proto::global_id::Value::User(x)) => Ok(GlobalId::User(x)),
            Some(proto::global_id::Value::Transient(x)) => Ok(GlobalId::Transient(x)),
            Some(proto::global_id::Value::Explain(_)) => Ok(GlobalId::Explain),
            None => Err(TryFromProtoError::missing_field("GlobalId::kind")),
        }
    }
}

impl RustType<proto::audit_log_key::Event> for VersionedEvent {
    fn into_proto(&self) -> proto::audit_log_key::Event {
        match self {
            VersionedEvent::V1(event) => proto::audit_log_key::Event::V1(event.into_proto()),
        }
    }

    fn from_proto(proto: proto::audit_log_key::Event) -> Result<Self, TryFromProtoError> {
        match proto {
            proto::audit_log_key::Event::V1(event) => Ok(VersionedEvent::V1(event.into_rust()?)),
        }
    }
}

impl RustType<proto::audit_log_event_v1::EventType> for EventType {
    fn into_proto(&self) -> proto::audit_log_event_v1::EventType {
        match self {
            EventType::Create => proto::audit_log_event_v1::EventType::Create,
            EventType::Drop => proto::audit_log_event_v1::EventType::Drop,
            EventType::Alter => proto::audit_log_event_v1::EventType::Alter,
            EventType::Grant => proto::audit_log_event_v1::EventType::Grant,
            EventType::Revoke => proto::audit_log_event_v1::EventType::Revoke,
        }
    }

    fn from_proto(proto: proto::audit_log_event_v1::EventType) -> Result<Self, TryFromProtoError> {
        match proto {
            proto::audit_log_event_v1::EventType::Create => Ok(EventType::Create),
            proto::audit_log_event_v1::EventType::Drop => Ok(EventType::Drop),
            proto::audit_log_event_v1::EventType::Alter => Ok(EventType::Alter),
            proto::audit_log_event_v1::EventType::Grant => Ok(EventType::Grant),
            proto::audit_log_event_v1::EventType::Revoke => Ok(EventType::Revoke),
            proto::audit_log_event_v1::EventType::Unknown => Err(
                TryFromProtoError::unknown_enum_variant("EventType::Unknown"),
            ),
        }
    }
}

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

impl RustType<proto::audit_log_event_v1::ObjectType> for mz_audit_log::ObjectType {
    fn into_proto(&self) -> proto::audit_log_event_v1::ObjectType {
        match self {
            mz_audit_log::ObjectType::Cluster => proto::audit_log_event_v1::ObjectType::Cluster,
            mz_audit_log::ObjectType::ClusterReplica => {
                proto::audit_log_event_v1::ObjectType::ClusterReplica
            }
            mz_audit_log::ObjectType::Connection => {
                proto::audit_log_event_v1::ObjectType::Connection
            }
            mz_audit_log::ObjectType::Database => proto::audit_log_event_v1::ObjectType::Database,
            mz_audit_log::ObjectType::Func => proto::audit_log_event_v1::ObjectType::Func,
            mz_audit_log::ObjectType::Index => proto::audit_log_event_v1::ObjectType::Index,
            mz_audit_log::ObjectType::MaterializedView => {
                proto::audit_log_event_v1::ObjectType::MaterializedView
            }
            mz_audit_log::ObjectType::Role => proto::audit_log_event_v1::ObjectType::Role,
            mz_audit_log::ObjectType::Secret => proto::audit_log_event_v1::ObjectType::Secret,
            mz_audit_log::ObjectType::Schema => proto::audit_log_event_v1::ObjectType::Schema,
            mz_audit_log::ObjectType::Sink => proto::audit_log_event_v1::ObjectType::Sink,
            mz_audit_log::ObjectType::Source => proto::audit_log_event_v1::ObjectType::Source,
            mz_audit_log::ObjectType::System => proto::audit_log_event_v1::ObjectType::System,
            mz_audit_log::ObjectType::Table => proto::audit_log_event_v1::ObjectType::Table,
            mz_audit_log::ObjectType::Type => proto::audit_log_event_v1::ObjectType::Type,
            mz_audit_log::ObjectType::View => proto::audit_log_event_v1::ObjectType::View,
        }
    }

    fn from_proto(proto: proto::audit_log_event_v1::ObjectType) -> Result<Self, TryFromProtoError> {
        match proto {
            proto::audit_log_event_v1::ObjectType::Cluster => Ok(mz_audit_log::ObjectType::Cluster),
            proto::audit_log_event_v1::ObjectType::ClusterReplica => {
                Ok(mz_audit_log::ObjectType::ClusterReplica)
            }
            proto::audit_log_event_v1::ObjectType::Connection => {
                Ok(mz_audit_log::ObjectType::Connection)
            }
            proto::audit_log_event_v1::ObjectType::Database => {
                Ok(mz_audit_log::ObjectType::Database)
            }
            proto::audit_log_event_v1::ObjectType::Func => Ok(mz_audit_log::ObjectType::Func),
            proto::audit_log_event_v1::ObjectType::Index => Ok(mz_audit_log::ObjectType::Index),
            proto::audit_log_event_v1::ObjectType::MaterializedView => {
                Ok(mz_audit_log::ObjectType::MaterializedView)
            }
            proto::audit_log_event_v1::ObjectType::Role => Ok(mz_audit_log::ObjectType::Role),
            proto::audit_log_event_v1::ObjectType::Secret => Ok(mz_audit_log::ObjectType::Secret),
            proto::audit_log_event_v1::ObjectType::Schema => Ok(mz_audit_log::ObjectType::Schema),
            proto::audit_log_event_v1::ObjectType::Sink => Ok(mz_audit_log::ObjectType::Sink),
            proto::audit_log_event_v1::ObjectType::Source => Ok(mz_audit_log::ObjectType::Source),
            proto::audit_log_event_v1::ObjectType::System => Ok(mz_audit_log::ObjectType::System),
            proto::audit_log_event_v1::ObjectType::Table => Ok(mz_audit_log::ObjectType::Table),
            proto::audit_log_event_v1::ObjectType::Type => Ok(mz_audit_log::ObjectType::Type),
            proto::audit_log_event_v1::ObjectType::View => Ok(mz_audit_log::ObjectType::View),
            proto::audit_log_event_v1::ObjectType::Unknown => Err(
                TryFromProtoError::unknown_enum_variant("ObjectType::Unknown"),
            ),
        }
    }
}

impl RustType<proto::audit_log_event_v1::IdFullNameV1> for IdFullNameV1 {
    fn into_proto(&self) -> proto::audit_log_event_v1::IdFullNameV1 {
        proto::audit_log_event_v1::IdFullNameV1 {
            id: self.id.to_string(),
            name: Some(self.name.into_proto()),
        }
    }

    fn from_proto(
        proto: proto::audit_log_event_v1::IdFullNameV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(IdFullNameV1 {
            id: proto.id,
            name: proto.name.into_rust_if_some("IdFullNameV1::name")?,
        })
    }
}

impl RustType<proto::audit_log_event_v1::FullNameV1> for FullNameV1 {
    fn into_proto(&self) -> proto::audit_log_event_v1::FullNameV1 {
        proto::audit_log_event_v1::FullNameV1 {
            database: self.database.to_string(),
            schema: self.schema.to_string(),
            item: self.item.to_string(),
        }
    }

    fn from_proto(proto: proto::audit_log_event_v1::FullNameV1) -> Result<Self, TryFromProtoError> {
        Ok(FullNameV1 {
            database: proto.database,
            schema: proto.schema,
            item: proto.item,
        })
    }
}

impl RustType<proto::audit_log_event_v1::IdNameV1> for IdNameV1 {
    fn into_proto(&self) -> proto::audit_log_event_v1::IdNameV1 {
        proto::audit_log_event_v1::IdNameV1 {
            id: self.id.to_string(),
            name: self.name.to_string(),
        }
    }

    fn from_proto(proto: proto::audit_log_event_v1::IdNameV1) -> Result<Self, TryFromProtoError> {
        Ok(IdNameV1 {
            id: proto.id,
            name: proto.name,
        })
    }
}

impl RustType<proto::audit_log_event_v1::RenameItemV1> for RenameItemV1 {
    fn into_proto(&self) -> proto::audit_log_event_v1::RenameItemV1 {
        proto::audit_log_event_v1::RenameItemV1 {
            id: self.id.to_string(),
            old_name: Some(self.old_name.into_proto()),
            new_name: Some(self.new_name.into_proto()),
        }
    }

    fn from_proto(
        proto: proto::audit_log_event_v1::RenameItemV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(RenameItemV1 {
            id: proto.id,
            old_name: proto.old_name.into_rust_if_some("RenameItemV1::old_name")?,
            new_name: proto.new_name.into_rust_if_some("RenameItemV1::new_name")?,
        })
    }
}

impl RustType<proto::audit_log_event_v1::RenameClusterV1> for RenameClusterV1 {
    fn into_proto(&self) -> proto::audit_log_event_v1::RenameClusterV1 {
        proto::audit_log_event_v1::RenameClusterV1 {
            id: self.id.to_string(),
            old_name: self.old_name.into_proto(),
            new_name: self.new_name.into_proto(),
        }
    }

    fn from_proto(
        proto: proto::audit_log_event_v1::RenameClusterV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(RenameClusterV1 {
            id: proto.id,
            old_name: proto.old_name,
            new_name: proto.new_name,
        })
    }
}

impl RustType<proto::audit_log_event_v1::RenameClusterReplicaV1> for RenameClusterReplicaV1 {
    fn into_proto(&self) -> proto::audit_log_event_v1::RenameClusterReplicaV1 {
        proto::audit_log_event_v1::RenameClusterReplicaV1 {
            cluster_id: self.cluster_id.to_string(),
            replica_id: self.replica_id.to_string(),
            old_name: self.old_name.into_proto(),
            new_name: self.new_name.into_proto(),
        }
    }

    fn from_proto(
        proto: proto::audit_log_event_v1::RenameClusterReplicaV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(RenameClusterReplicaV1 {
            cluster_id: proto.cluster_id,
            replica_id: proto.replica_id,
            old_name: proto.old_name,
            new_name: proto.new_name,
        })
    }
}

impl RustType<proto::audit_log_event_v1::DropClusterReplicaV1> for DropClusterReplicaV1 {
    fn into_proto(&self) -> proto::audit_log_event_v1::DropClusterReplicaV1 {
        proto::audit_log_event_v1::DropClusterReplicaV1 {
            cluster_id: self.cluster_id.to_string(),
            cluster_name: self.cluster_name.to_string(),
            replica_id: self.replica_id.as_ref().map(|id| proto::StringWrapper {
                inner: id.to_string(),
            }),
            replica_name: self.replica_name.to_string(),
        }
    }

    fn from_proto(
        proto: proto::audit_log_event_v1::DropClusterReplicaV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(DropClusterReplicaV1 {
            cluster_id: proto.cluster_id,
            cluster_name: proto.cluster_name,
            replica_id: proto.replica_id.map(|s| s.inner),
            replica_name: proto.replica_name,
        })
    }
}

impl RustType<proto::audit_log_event_v1::CreateClusterReplicaV1> for CreateClusterReplicaV1 {
    fn into_proto(&self) -> proto::audit_log_event_v1::CreateClusterReplicaV1 {
        proto::audit_log_event_v1::CreateClusterReplicaV1 {
            cluster_id: self.cluster_id.to_string(),
            cluster_name: self.cluster_name.to_string(),
            replica_id: self.replica_id.as_ref().map(|id| proto::StringWrapper {
                inner: id.to_string(),
            }),
            replica_name: self.replica_name.to_string(),
            logical_size: self.logical_size.to_string(),
            disk: self.disk,
            billed_as: self.billed_as.clone(),
            internal: self.internal,
        }
    }

    fn from_proto(
        proto: proto::audit_log_event_v1::CreateClusterReplicaV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(CreateClusterReplicaV1 {
            cluster_id: proto.cluster_id,
            cluster_name: proto.cluster_name,
            replica_id: proto.replica_id.map(|id| id.inner),
            replica_name: proto.replica_name,
            logical_size: proto.logical_size,
            disk: proto.disk,
            billed_as: proto.billed_as,
            internal: proto.internal,
        })
    }
}

impl RustType<proto::audit_log_event_v1::CreateSourceSinkV1> for CreateSourceSinkV1 {
    fn into_proto(&self) -> proto::audit_log_event_v1::CreateSourceSinkV1 {
        proto::audit_log_event_v1::CreateSourceSinkV1 {
            id: self.id.to_string(),
            name: Some(self.name.into_proto()),
            size: self.size.as_ref().map(|s| proto::StringWrapper {
                inner: s.to_string(),
            }),
        }
    }

    fn from_proto(
        proto: proto::audit_log_event_v1::CreateSourceSinkV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(CreateSourceSinkV1 {
            id: proto.id,
            name: proto.name.into_rust_if_some("CreateSourceSinkV1::name")?,
            size: proto.size.map(|s| s.inner),
        })
    }
}

impl RustType<proto::audit_log_event_v1::CreateSourceSinkV2> for CreateSourceSinkV2 {
    fn into_proto(&self) -> proto::audit_log_event_v1::CreateSourceSinkV2 {
        proto::audit_log_event_v1::CreateSourceSinkV2 {
            id: self.id.to_string(),
            name: Some(self.name.into_proto()),
            size: self.size.as_ref().map(|s| proto::StringWrapper {
                inner: s.to_string(),
            }),
            external_type: self.external_type.to_string(),
        }
    }

    fn from_proto(
        proto: proto::audit_log_event_v1::CreateSourceSinkV2,
    ) -> Result<Self, TryFromProtoError> {
        Ok(CreateSourceSinkV2 {
            id: proto.id,
            name: proto.name.into_rust_if_some("CreateSourceSinkV2::name")?,
            size: proto.size.map(|s| s.inner),
            external_type: proto.external_type,
        })
    }
}

impl RustType<proto::audit_log_event_v1::CreateSourceSinkV3> for CreateSourceSinkV3 {
    fn into_proto(&self) -> proto::audit_log_event_v1::CreateSourceSinkV3 {
        proto::audit_log_event_v1::CreateSourceSinkV3 {
            id: self.id.to_string(),
            name: Some(self.name.into_proto()),
            external_type: self.external_type.to_string(),
        }
    }

    fn from_proto(
        proto: proto::audit_log_event_v1::CreateSourceSinkV3,
    ) -> Result<Self, TryFromProtoError> {
        Ok(CreateSourceSinkV3 {
            id: proto.id,
            name: proto.name.into_rust_if_some("CreateSourceSinkV2::name")?,
            external_type: proto.external_type,
        })
    }
}

impl RustType<proto::audit_log_event_v1::AlterSourceSinkV1> for AlterSourceSinkV1 {
    fn into_proto(&self) -> proto::audit_log_event_v1::AlterSourceSinkV1 {
        proto::audit_log_event_v1::AlterSourceSinkV1 {
            id: self.id.to_string(),
            name: Some(self.name.into_proto()),
            old_size: self.old_size.as_ref().map(|s| proto::StringWrapper {
                inner: s.to_string(),
            }),
            new_size: self.new_size.as_ref().map(|s| proto::StringWrapper {
                inner: s.to_string(),
            }),
        }
    }

    fn from_proto(
        proto: proto::audit_log_event_v1::AlterSourceSinkV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(AlterSourceSinkV1 {
            id: proto.id,
            name: proto.name.into_rust_if_some("AlterSourceSinkV1::name")?,
            old_size: proto.old_size.map(|s| s.inner),
            new_size: proto.new_size.map(|s| s.inner),
        })
    }
}

impl RustType<proto::audit_log_event_v1::AlterSetClusterV1> for AlterSetClusterV1 {
    fn into_proto(&self) -> proto::audit_log_event_v1::AlterSetClusterV1 {
        proto::audit_log_event_v1::AlterSetClusterV1 {
            id: self.id.to_string(),
            name: Some(self.name.into_proto()),
            old_cluster: self.old_cluster.as_ref().map(|s| proto::StringWrapper {
                inner: s.to_string(),
            }),
            new_cluster: self.new_cluster.as_ref().map(|s| proto::StringWrapper {
                inner: s.to_string(),
            }),
        }
    }

    fn from_proto(
        proto: proto::audit_log_event_v1::AlterSetClusterV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            id: proto.id,
            name: proto.name.into_rust_if_some("AlterSetClusterV1::name")?,
            old_cluster: proto.old_cluster.map(|s| s.inner),
            new_cluster: proto.new_cluster.map(|s| s.inner),
        })
    }
}

impl RustType<proto::audit_log_event_v1::GrantRoleV1> for GrantRoleV1 {
    fn into_proto(&self) -> proto::audit_log_event_v1::GrantRoleV1 {
        proto::audit_log_event_v1::GrantRoleV1 {
            role_id: self.role_id.to_string(),
            member_id: self.member_id.to_string(),
            grantor_id: self.grantor_id.to_string(),
        }
    }

    fn from_proto(
        proto: proto::audit_log_event_v1::GrantRoleV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(GrantRoleV1 {
            role_id: proto.role_id,
            member_id: proto.member_id,
            grantor_id: proto.grantor_id,
        })
    }
}

impl RustType<proto::audit_log_event_v1::GrantRoleV2> for GrantRoleV2 {
    fn into_proto(&self) -> proto::audit_log_event_v1::GrantRoleV2 {
        proto::audit_log_event_v1::GrantRoleV2 {
            role_id: self.role_id.to_string(),
            member_id: self.member_id.to_string(),
            grantor_id: self.grantor_id.to_string(),
            executed_by: self.executed_by.to_string(),
        }
    }

    fn from_proto(
        proto: proto::audit_log_event_v1::GrantRoleV2,
    ) -> Result<Self, TryFromProtoError> {
        Ok(GrantRoleV2 {
            role_id: proto.role_id,
            member_id: proto.member_id,
            grantor_id: proto.grantor_id,
            executed_by: proto.executed_by,
        })
    }
}

impl RustType<proto::audit_log_event_v1::RevokeRoleV1> for RevokeRoleV1 {
    fn into_proto(&self) -> proto::audit_log_event_v1::RevokeRoleV1 {
        proto::audit_log_event_v1::RevokeRoleV1 {
            role_id: self.role_id.to_string(),
            member_id: self.member_id.to_string(),
        }
    }

    fn from_proto(
        proto: proto::audit_log_event_v1::RevokeRoleV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(RevokeRoleV1 {
            role_id: proto.role_id,
            member_id: proto.member_id,
        })
    }
}

impl RustType<proto::audit_log_event_v1::RevokeRoleV2> for RevokeRoleV2 {
    fn into_proto(&self) -> proto::audit_log_event_v1::RevokeRoleV2 {
        proto::audit_log_event_v1::RevokeRoleV2 {
            role_id: self.role_id.to_string(),
            member_id: self.member_id.to_string(),
            grantor_id: self.grantor_id.to_string(),
            executed_by: self.executed_by.to_string(),
        }
    }

    fn from_proto(
        proto: proto::audit_log_event_v1::RevokeRoleV2,
    ) -> Result<Self, TryFromProtoError> {
        Ok(RevokeRoleV2 {
            role_id: proto.role_id,
            member_id: proto.member_id,
            grantor_id: proto.grantor_id,
            executed_by: proto.executed_by,
        })
    }
}

impl RustType<proto::audit_log_event_v1::UpdatePrivilegeV1> for UpdatePrivilegeV1 {
    fn into_proto(&self) -> proto::audit_log_event_v1::UpdatePrivilegeV1 {
        proto::audit_log_event_v1::UpdatePrivilegeV1 {
            object_id: self.object_id.to_string(),
            grantee_id: self.grantee_id.to_string(),
            grantor_id: self.grantor_id.to_string(),
            privileges: self.privileges.to_string(),
        }
    }

    fn from_proto(
        proto: proto::audit_log_event_v1::UpdatePrivilegeV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(UpdatePrivilegeV1 {
            object_id: proto.object_id,
            grantee_id: proto.grantee_id,
            grantor_id: proto.grantor_id,
            privileges: proto.privileges,
        })
    }
}

impl RustType<proto::audit_log_event_v1::AlterDefaultPrivilegeV1> for AlterDefaultPrivilegeV1 {
    fn into_proto(&self) -> proto::audit_log_event_v1::AlterDefaultPrivilegeV1 {
        proto::audit_log_event_v1::AlterDefaultPrivilegeV1 {
            role_id: self.role_id.to_string(),
            database_id: self.database_id.as_ref().map(|id| proto::StringWrapper {
                inner: id.to_string(),
            }),
            schema_id: self.schema_id.as_ref().map(|id| proto::StringWrapper {
                inner: id.to_string(),
            }),
            grantee_id: self.grantee_id.to_string(),
            privileges: self.privileges.to_string(),
        }
    }

    fn from_proto(
        proto: proto::audit_log_event_v1::AlterDefaultPrivilegeV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(AlterDefaultPrivilegeV1 {
            role_id: proto.role_id,
            database_id: proto.database_id.map(|id| id.inner),
            schema_id: proto.schema_id.map(|id| id.inner),
            grantee_id: proto.grantee_id,
            privileges: proto.privileges,
        })
    }
}

impl RustType<proto::audit_log_event_v1::UpdateOwnerV1> for UpdateOwnerV1 {
    fn into_proto(&self) -> proto::audit_log_event_v1::UpdateOwnerV1 {
        proto::audit_log_event_v1::UpdateOwnerV1 {
            object_id: self.object_id.to_string(),
            old_owner_id: self.old_owner_id.to_string(),
            new_owner_id: self.new_owner_id.to_string(),
        }
    }

    fn from_proto(
        proto: proto::audit_log_event_v1::UpdateOwnerV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(UpdateOwnerV1 {
            object_id: proto.object_id,
            old_owner_id: proto.old_owner_id,
            new_owner_id: proto.new_owner_id,
        })
    }
}

impl RustType<proto::audit_log_event_v1::SchemaV1> for SchemaV1 {
    fn into_proto(&self) -> proto::audit_log_event_v1::SchemaV1 {
        proto::audit_log_event_v1::SchemaV1 {
            id: self.id.to_string(),
            name: self.name.to_string(),
            database_name: self.database_name.to_string(),
        }
    }

    fn from_proto(proto: proto::audit_log_event_v1::SchemaV1) -> Result<Self, TryFromProtoError> {
        Ok(SchemaV1 {
            id: proto.id,
            name: proto.name,
            database_name: proto.database_name,
        })
    }
}

impl RustType<proto::audit_log_event_v1::SchemaV2> for SchemaV2 {
    fn into_proto(&self) -> proto::audit_log_event_v1::SchemaV2 {
        proto::audit_log_event_v1::SchemaV2 {
            id: self.id.to_string(),
            name: self.name.to_string(),
            database_name: self.database_name.as_ref().map(|d| proto::StringWrapper {
                inner: d.to_string(),
            }),
        }
    }

    fn from_proto(proto: proto::audit_log_event_v1::SchemaV2) -> Result<Self, TryFromProtoError> {
        Ok(SchemaV2 {
            id: proto.id,
            name: proto.name,
            database_name: proto.database_name.map(|d| d.inner),
        })
    }
}

impl RustType<proto::audit_log_event_v1::RenameSchemaV1> for RenameSchemaV1 {
    fn into_proto(&self) -> proto::audit_log_event_v1::RenameSchemaV1 {
        proto::audit_log_event_v1::RenameSchemaV1 {
            id: self.id.to_string(),
            database_name: self.database_name.clone(),
            old_name: self.old_name.clone(),
            new_name: self.new_name.clone(),
        }
    }

    fn from_proto(
        proto: proto::audit_log_event_v1::RenameSchemaV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(RenameSchemaV1 {
            id: proto.id,
            database_name: proto.database_name,
            old_name: proto.old_name,
            new_name: proto.new_name,
        })
    }
}

impl RustType<proto::audit_log_event_v1::UpdateItemV1> for UpdateItemV1 {
    fn into_proto(&self) -> proto::audit_log_event_v1::UpdateItemV1 {
        proto::audit_log_event_v1::UpdateItemV1 {
            id: self.id.to_string(),
            name: Some(self.name.into_proto()),
        }
    }

    fn from_proto(
        proto: proto::audit_log_event_v1::UpdateItemV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(UpdateItemV1 {
            id: proto.id,
            name: proto.name.into_rust_if_some("UpdateItemV1::name")?,
        })
    }
}

impl RustType<proto::audit_log_event_v1::AlterRetainHistoryV1> for AlterRetainHistoryV1 {
    fn into_proto(&self) -> proto::audit_log_event_v1::AlterRetainHistoryV1 {
        proto::audit_log_event_v1::AlterRetainHistoryV1 {
            id: self.id.to_string(),
            old_history: self.old_history.clone(),
            new_history: self.new_history.clone(),
        }
    }

    fn from_proto(
        proto: proto::audit_log_event_v1::AlterRetainHistoryV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(AlterRetainHistoryV1 {
            id: proto.id,
            old_history: proto.old_history,
            new_history: proto.new_history,
        })
    }
}

impl RustType<proto::audit_log_event_v1::ToNewIdV1> for ToNewIdV1 {
    fn into_proto(&self) -> proto::audit_log_event_v1::ToNewIdV1 {
        proto::audit_log_event_v1::ToNewIdV1 {
            id: self.id.to_string(),
            new_id: self.new_id.to_string(),
        }
    }

    fn from_proto(proto: proto::audit_log_event_v1::ToNewIdV1) -> Result<Self, TryFromProtoError> {
        Ok(ToNewIdV1 {
            id: proto.id,
            new_id: proto.new_id,
        })
    }
}

impl RustType<proto::audit_log_event_v1::FromPreviousIdV1> for FromPreviousIdV1 {
    fn into_proto(&self) -> proto::audit_log_event_v1::FromPreviousIdV1 {
        proto::audit_log_event_v1::FromPreviousIdV1 {
            id: self.id.to_string(),
            previous_id: self.previous_id.to_string(),
        }
    }

    fn from_proto(
        proto: proto::audit_log_event_v1::FromPreviousIdV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(FromPreviousIdV1 {
            id: proto.id,
            previous_id: proto.previous_id,
        })
    }
}

impl RustType<proto::audit_log_event_v1::Details> for EventDetails {
    fn into_proto(&self) -> proto::audit_log_event_v1::Details {
        use proto::audit_log_event_v1::Details::*;

        match self {
            EventDetails::CreateClusterReplicaV1(details) => {
                CreateClusterReplicaV1(details.into_proto())
            }
            EventDetails::DropClusterReplicaV1(details) => {
                DropClusterReplicaV1(details.into_proto())
            }
            EventDetails::CreateSourceSinkV1(details) => CreateSourceSinkV1(details.into_proto()),
            EventDetails::CreateSourceSinkV2(details) => CreateSourceSinkV2(details.into_proto()),
            EventDetails::CreateSourceSinkV3(details) => CreateSourceSinkV3(details.into_proto()),
            EventDetails::AlterSourceSinkV1(details) => AlterSourceSinkV1(details.into_proto()),
            EventDetails::AlterSetClusterV1(details) => AlterSetClusterV1(details.into_proto()),
            EventDetails::GrantRoleV1(details) => GrantRoleV1(details.into_proto()),
            EventDetails::GrantRoleV2(details) => GrantRoleV2(details.into_proto()),
            EventDetails::RevokeRoleV1(details) => RevokeRoleV1(details.into_proto()),
            EventDetails::RevokeRoleV2(details) => RevokeRoleV2(details.into_proto()),
            EventDetails::UpdatePrivilegeV1(details) => UpdatePrivilegeV1(details.into_proto()),
            EventDetails::AlterDefaultPrivilegeV1(details) => {
                AlterDefaultPrivilegeV1(details.into_proto())
            }
            EventDetails::UpdateOwnerV1(details) => UpdateOwnerV1(details.into_proto()),
            EventDetails::IdFullNameV1(details) => IdFullNameV1(details.into_proto()),
            EventDetails::RenameClusterV1(details) => RenameClusterV1(details.into_proto()),
            EventDetails::RenameClusterReplicaV1(details) => {
                RenameClusterReplicaV1(details.into_proto())
            }
            EventDetails::RenameItemV1(details) => RenameItemV1(details.into_proto()),
            EventDetails::IdNameV1(details) => IdNameV1(details.into_proto()),
            EventDetails::SchemaV1(details) => SchemaV1(details.into_proto()),
            EventDetails::SchemaV2(details) => SchemaV2(details.into_proto()),
            EventDetails::RenameSchemaV1(details) => RenameSchemaV1(details.into_proto()),
            EventDetails::UpdateItemV1(details) => UpdateItemV1(details.into_proto()),
            EventDetails::AlterRetainHistoryV1(details) => {
                AlterRetainHistoryV1(details.into_proto())
            }
            EventDetails::ToNewIdV1(details) => ToNewIdV1(details.into_proto()),
            EventDetails::FromPreviousIdV1(details) => FromPreviousIdV1(details.into_proto()),
        }
    }

    fn from_proto(proto: proto::audit_log_event_v1::Details) -> Result<Self, TryFromProtoError> {
        use proto::audit_log_event_v1::Details::*;

        match proto {
            CreateClusterReplicaV1(details) => {
                Ok(EventDetails::CreateClusterReplicaV1(details.into_rust()?))
            }
            DropClusterReplicaV1(details) => {
                Ok(EventDetails::DropClusterReplicaV1(details.into_rust()?))
            }
            CreateSourceSinkV1(details) => {
                Ok(EventDetails::CreateSourceSinkV1(details.into_rust()?))
            }
            CreateSourceSinkV2(details) => {
                Ok(EventDetails::CreateSourceSinkV2(details.into_rust()?))
            }
            CreateSourceSinkV3(details) => {
                Ok(EventDetails::CreateSourceSinkV3(details.into_rust()?))
            }
            AlterSourceSinkV1(details) => Ok(EventDetails::AlterSourceSinkV1(details.into_rust()?)),
            AlterSetClusterV1(details) => Ok(EventDetails::AlterSetClusterV1(details.into_rust()?)),
            GrantRoleV1(details) => Ok(EventDetails::GrantRoleV1(details.into_rust()?)),
            GrantRoleV2(details) => Ok(EventDetails::GrantRoleV2(details.into_rust()?)),
            RevokeRoleV1(details) => Ok(EventDetails::RevokeRoleV1(details.into_rust()?)),
            RevokeRoleV2(details) => Ok(EventDetails::RevokeRoleV2(details.into_rust()?)),
            UpdatePrivilegeV1(details) => Ok(EventDetails::UpdatePrivilegeV1(details.into_rust()?)),
            AlterDefaultPrivilegeV1(details) => {
                Ok(EventDetails::AlterDefaultPrivilegeV1(details.into_rust()?))
            }
            UpdateOwnerV1(details) => Ok(EventDetails::UpdateOwnerV1(details.into_rust()?)),
            IdFullNameV1(details) => Ok(EventDetails::IdFullNameV1(details.into_rust()?)),
            RenameClusterV1(details) => Ok(EventDetails::RenameClusterV1(details.into_rust()?)),
            RenameClusterReplicaV1(details) => {
                Ok(EventDetails::RenameClusterReplicaV1(details.into_rust()?))
            }
            RenameItemV1(details) => Ok(EventDetails::RenameItemV1(details.into_rust()?)),
            IdNameV1(details) => Ok(EventDetails::IdNameV1(details.into_rust()?)),
            SchemaV1(details) => Ok(EventDetails::SchemaV1(details.into_rust()?)),
            SchemaV2(details) => Ok(EventDetails::SchemaV2(details.into_rust()?)),
            RenameSchemaV1(details) => Ok(EventDetails::RenameSchemaV1(details.into_rust()?)),
            UpdateItemV1(details) => Ok(EventDetails::UpdateItemV1(details.into_rust()?)),
            AlterRetainHistoryV1(details) => {
                Ok(EventDetails::AlterRetainHistoryV1(details.into_rust()?))
            }
            ToNewIdV1(details) => Ok(EventDetails::ToNewIdV1(details.into_rust()?)),
            FromPreviousIdV1(details) => Ok(EventDetails::FromPreviousIdV1(details.into_rust()?)),
        }
    }
}

impl RustType<proto::AuditLogEventV1> for EventV1 {
    fn into_proto(&self) -> proto::AuditLogEventV1 {
        proto::AuditLogEventV1 {
            id: self.id,
            event_type: self.event_type.into_proto().into(),
            object_type: self.object_type.into_proto().into(),
            user: self.user.as_ref().map(|u| proto::StringWrapper {
                inner: u.to_string(),
            }),
            occurred_at: Some(proto::EpochMillis {
                millis: self.occurred_at,
            }),
            details: Some(self.details.into_proto()),
        }
    }

    fn from_proto(proto: proto::AuditLogEventV1) -> Result<Self, TryFromProtoError> {
        let event_type = proto::audit_log_event_v1::EventType::from_i32(proto.event_type)
            .ok_or_else(|| TryFromProtoError::unknown_enum_variant("EventType"))?;
        let object_type = proto::audit_log_event_v1::ObjectType::from_i32(proto.object_type)
            .ok_or_else(|| TryFromProtoError::unknown_enum_variant("ObjectType"))?;
        Ok(EventV1 {
            id: proto.id,
            event_type: event_type.into_rust()?,
            object_type: object_type.into_rust()?,
            details: proto
                .details
                .into_rust_if_some("AuditLogEventV1::details")?,
            user: proto.user.map(|u| u.inner),
            occurred_at: proto
                .occurred_at
                .into_rust_if_some("AuditLogEventV1::occurred_at")?,
        })
    }
}

impl RustType<proto::storage_usage_key::StorageUsageV1> for StorageUsageV1 {
    fn into_proto(&self) -> proto::storage_usage_key::StorageUsageV1 {
        proto::storage_usage_key::StorageUsageV1 {
            id: self.id,
            shard_id: self.shard_id.as_ref().map(|s| proto::StringWrapper {
                inner: s.to_string(),
            }),
            size_bytes: self.size_bytes,
            collection_timestamp: Some(proto::EpochMillis {
                millis: self.collection_timestamp,
            }),
        }
    }

    fn from_proto(
        proto: proto::storage_usage_key::StorageUsageV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(StorageUsageV1 {
            id: proto.id,
            shard_id: proto.shard_id.map(|s| s.inner),
            size_bytes: proto.size_bytes,
            collection_timestamp: proto
                .collection_timestamp
                .into_rust_if_some("StorageUsageKey::collection_timestamp")?,
        })
    }
}

impl RustType<proto::storage_usage_key::Usage> for VersionedStorageUsage {
    fn into_proto(&self) -> proto::storage_usage_key::Usage {
        match self {
            VersionedStorageUsage::V1(usage) => {
                proto::storage_usage_key::Usage::V1(usage.into_proto())
            }
        }
    }

    fn from_proto(proto: proto::storage_usage_key::Usage) -> Result<Self, TryFromProtoError> {
        match proto {
            proto::storage_usage_key::Usage::V1(usage) => {
                Ok(VersionedStorageUsage::V1(usage.into_rust()?))
            }
        }
    }
}

impl From<String> for proto::StringWrapper {
    fn from(value: String) -> Self {
        proto::StringWrapper { inner: value }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::fs;
    use std::io::{BufRead, BufReader};

    use mz_audit_log::{VersionedEvent, VersionedStorageUsage};
    use mz_proto::RustType;
    use proptest::prelude::*;

    use crate::durable::upgrade::{CATALOG_VERSION, MIN_CATALOG_VERSION};

    // Note: Feel free to update this path if the protos move.
    const PROTO_DIRECTORY: &str = "protos";

    #[mz_ore::test]
    fn test_assert_snapshots_exist() {
        // Get all of the files in the snapshot directory, with the `.proto` extension.
        let mut filenames: BTreeSet<_> = fs::read_dir(PROTO_DIRECTORY)
            .expect("failed to read protos dir")
            .map(|entry| entry.expect("failed to read dir entry").file_name())
            .map(|filename| filename.to_str().expect("utf8").to_string())
            .filter(|filename| filename.ends_with("proto"))
            .collect();

        // Assert objects.proto exists.
        assert!(filenames.remove("objects.proto"));

        // Assert snapshots exist for all of the versions we support.
        for version in MIN_CATALOG_VERSION..=CATALOG_VERSION {
            let filename = format!("objects_v{version}.proto");
            assert!(
                filenames.remove(&filename),
                "Missing snapshot for v{version}."
            );
        }

        // Common case. Check to make sure the user bumped the CATALOG_VERSION.
        if !filenames.is_empty()
            && filenames.remove(&format!("objects_v{}.proto", CATALOG_VERSION + 1))
        {
            panic!(
                "Found snapshot for v{}, please also bump `CATALOG_VERSION`.",
                CATALOG_VERSION + 1
            )
        }

        // Assert there aren't any extra snapshots.
        assert!(
            filenames.is_empty(),
            "Found snapshots for unsupported catalog versions {filenames:?}.\nIf you just increased `MIN_CATALOG_VERSION`, then please delete the old snapshots. If you created a new snapshot, please bump `CATALOG_VERSION`."
        );
    }

    #[mz_ore::test]
    fn test_assert_current_snapshot() {
        // Read the content from both files.
        let current = fs::File::open(format!("{PROTO_DIRECTORY}/objects.proto"))
            .map(BufReader::new)
            .expect("read current");
        let snapshot = fs::File::open(format!(
            "{PROTO_DIRECTORY}/objects_v{CATALOG_VERSION}.proto"
        ))
        .map(BufReader::new)
        .expect("read snapshot");

        // Read in all of the lines so we can compare the content of †he files.
        let current: Vec<_> = current
            .lines()
            .map(|r| r.expect("failed to read line from current"))
            // Filter out the package name, since we expect that to be different.
            .filter(|line| line != "package objects;")
            .collect();
        let snapshot: Vec<_> = snapshot
            .lines()
            .map(|r| r.expect("failed to read line from current"))
            // Filter out the package name, since we expect that to be different.
            .filter(|line| line != &format!("package objects_v{CATALOG_VERSION};"))
            .collect();

        // Note: objects.proto and objects_v<CATALOG_VERSION>.proto should be exactly the same. The
        // reason being, when bumping the catalog to the next version, CATALOG_VERSION + 1, we need a
        // snapshot to migrate _from_, which should be a snapshot of how the protos are today.
        // Hence why the two files should be exactly the same.
        similar_asserts::assert_eq!(current, snapshot);
    }

    proptest! {
        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // slow
        fn proptest_audit_log_roundtrips(event: VersionedEvent) {
            let proto = event.into_proto();
            let roundtrip = VersionedEvent::from_proto(proto).expect("valid proto");

            prop_assert_eq!(event, roundtrip);
        }

        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // slow
        fn proptest_storage_usage_roundtrips(usage: VersionedStorageUsage) {
            let proto = usage.into_proto();
            let roundtrip = VersionedStorageUsage::from_proto(proto).expect("valid proto");

            prop_assert_eq!(usage, roundtrip);
        }
    }
}
