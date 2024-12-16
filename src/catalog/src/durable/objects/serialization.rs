// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module is responsible for serializing catalog objects into Protobuf.

use mz_ore::cast::CastFrom;
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};

use crate::durable::objects::state_update::StateUpdateKindJson;
use crate::durable::objects::{
    AuditLogKey, ClusterIntrospectionSourceIndexKey, ClusterIntrospectionSourceIndexValue,
    ClusterKey, ClusterReplicaKey, ClusterReplicaValue, ClusterValue, CommentKey, CommentValue,
    ConfigKey, ConfigValue, DatabaseKey, DatabaseValue, DefaultPrivilegesKey,
    DefaultPrivilegesValue, GidMappingKey, GidMappingValue, IdAllocKey, IdAllocValue,
    IntrospectionSourceIndexCatalogItemId, IntrospectionSourceIndexGlobalId, ItemKey, ItemValue,
    NetworkPolicyKey, NetworkPolicyValue, RoleKey, RoleValue, SchemaKey, SchemaValue,
    ServerConfigurationKey, ServerConfigurationValue, SettingKey, SettingValue, SourceReference,
    SourceReferencesKey, SourceReferencesValue, StorageCollectionMetadataKey,
    StorageCollectionMetadataValue, SystemCatalogItemId, SystemGlobalId, SystemPrivilegesKey,
    SystemPrivilegesValue, TxnWalShardValue, UnfinalizedShardKey,
};
use crate::durable::{
    ClusterConfig, ClusterVariant, ClusterVariantManaged, ReplicaConfig, ReplicaLocation,
};

pub mod proto {
    pub use mz_catalog_protos::objects::*;
}

impl From<proto::StateUpdateKind> for StateUpdateKindJson {
    fn from(value: proto::StateUpdateKind) -> Self {
        StateUpdateKindJson::from_serde(value)
    }
}

impl TryFrom<StateUpdateKindJson> for proto::StateUpdateKind {
    type Error = String;

    fn try_from(value: StateUpdateKindJson) -> Result<Self, Self::Error> {
        value.try_to_serde::<Self>().map_err(|err| err.to_string())
    }
}

impl RustType<proto::ClusterConfig> for ClusterConfig {
    fn into_proto(&self) -> proto::ClusterConfig {
        proto::ClusterConfig {
            variant: Some(self.variant.into_proto()),
            workload_class: self.workload_class.clone(),
        }
    }

    fn from_proto(proto: proto::ClusterConfig) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            variant: proto.variant.into_rust_if_some("ClusterConfig::variant")?,
            workload_class: proto.workload_class,
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
                pending,
            } => proto::replica_config::Location::Managed(proto::replica_config::ManagedLocation {
                size: size.to_string(),
                availability_zone: availability_zone.clone(),
                disk: *disk,
                billed_as: billed_as.clone(),
                internal: *internal,
                pending: *pending,
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
                pending: location.pending,
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
        let object_type = proto::CatalogItemType::try_from(proto.object_type)
            .map_err(|_| TryFromProtoError::unknown_enum_variant("CatalogItemType"))?;
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
            id: self.catalog_id.0,
            global_id: Some(self.global_id.into_proto()),
            fingerprint: self.fingerprint.to_string(),
        }
    }

    fn from_proto(proto: proto::GidMappingValue) -> Result<Self, TryFromProtoError> {
        Ok(GidMappingValue {
            catalog_id: SystemCatalogItemId(proto.id),
            global_id: proto
                .global_id
                .into_rust_if_some("GidMappingValue::global_id")?,
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
            index_id: self.catalog_id.0,
            global_id: Some(self.global_id.into_proto()),
            oid: self.oid,
        }
    }

    fn from_proto(
        proto: proto::ClusterIntrospectionSourceIndexValue,
    ) -> Result<Self, TryFromProtoError> {
        Ok(ClusterIntrospectionSourceIndexValue {
            catalog_id: IntrospectionSourceIndexCatalogItemId(proto.index_id),
            global_id: proto
                .global_id
                .into_rust_if_some("ClusterIntrospectionSourceIndexValue::global_id")?,
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
            gid: Some(self.id.into_proto()),
        }
    }

    fn from_proto(proto: proto::ItemKey) -> Result<Self, TryFromProtoError> {
        Ok(ItemKey {
            id: proto.gid.into_rust_if_some("ItemKey::gid")?,
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
            global_id: Some(self.global_id.into_proto()),
            extra_versions: self
                .extra_versions
                .iter()
                .map(|(version, global_id)| proto::ItemVersion {
                    global_id: Some(global_id.into_proto()),
                    version: Some(version.into_proto()),
                })
                .collect(),
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
        let extra_versions = proto
            .extra_versions
            .into_iter()
            .map(|item_version| {
                let version = item_version
                    .version
                    .into_rust_if_some("ItemVersion::version")?;
                let global_id = item_version
                    .global_id
                    .into_rust_if_some("ItemVersion::global_id")?;
                Ok::<_, TryFromProtoError>((version, global_id))
            })
            .collect::<Result<_, _>>()?;
        Ok(ItemValue {
            schema_id: proto.schema_id.into_rust_if_some("ItemValue::schema_id")?,
            name: proto.name,
            create_sql,
            owner_id: proto.owner_id.into_rust_if_some("ItemValue::owner_id")?,
            privileges: proto.privileges.into_rust()?,
            oid: proto.oid,
            global_id: proto.global_id.into_rust_if_some("ItemValue::global_id")?,
            extra_versions,
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

impl RustType<proto::NetworkPolicyKey> for NetworkPolicyKey {
    fn into_proto(&self) -> proto::NetworkPolicyKey {
        proto::NetworkPolicyKey {
            id: Some(self.id.into_proto()),
        }
    }

    fn from_proto(proto: proto::NetworkPolicyKey) -> Result<Self, TryFromProtoError> {
        Ok(NetworkPolicyKey {
            id: proto.id.into_rust_if_some("NetworkPolicyKey::id")?,
        })
    }
}

impl RustType<proto::NetworkPolicyValue> for NetworkPolicyValue {
    fn into_proto(&self) -> proto::NetworkPolicyValue {
        proto::NetworkPolicyValue {
            name: self.name.to_string(),
            rules: self.rules.into_proto(),
            owner_id: Some(self.owner_id.into_proto()),
            privileges: self.privileges.into_proto(),
            oid: self.oid,
        }
    }

    fn from_proto(proto: proto::NetworkPolicyValue) -> Result<Self, TryFromProtoError> {
        Ok(NetworkPolicyValue {
            name: proto.name,
            rules: proto.rules.into_rust()?,
            owner_id: proto
                .owner_id
                .into_rust_if_some("NetworkPolicyValue::owner_id,")?,
            privileges: proto.privileges.into_rust()?,
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
        Ok(StorageCollectionMetadataValue {
            shard: proto.shard.into_rust()?,
        })
    }
}

impl RustType<proto::UnfinalizedShardKey> for UnfinalizedShardKey {
    fn into_proto(&self) -> proto::UnfinalizedShardKey {
        proto::UnfinalizedShardKey {
            shard: self.shard.to_string(),
        }
    }

    fn from_proto(proto: proto::UnfinalizedShardKey) -> Result<Self, TryFromProtoError> {
        Ok(UnfinalizedShardKey {
            shard: proto.shard.into_rust()?,
        })
    }
}

impl RustType<proto::TxnWalShardValue> for TxnWalShardValue {
    fn into_proto(&self) -> proto::TxnWalShardValue {
        proto::TxnWalShardValue {
            shard: self.shard.to_string(),
        }
    }

    fn from_proto(proto: proto::TxnWalShardValue) -> Result<Self, TryFromProtoError> {
        Ok(TxnWalShardValue {
            shard: proto.shard.into_rust()?,
        })
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

impl RustType<proto::SourceReferencesKey> for SourceReferencesKey {
    fn into_proto(&self) -> proto::SourceReferencesKey {
        proto::SourceReferencesKey {
            source: Some(self.source_id.into_proto()),
        }
    }
    fn from_proto(proto: proto::SourceReferencesKey) -> Result<Self, TryFromProtoError> {
        Ok(SourceReferencesKey {
            source_id: proto
                .source
                .into_rust_if_some("SourceReferencesKey::source_id")?,
        })
    }
}

impl RustType<proto::SourceReferencesValue> for SourceReferencesValue {
    fn into_proto(&self) -> proto::SourceReferencesValue {
        proto::SourceReferencesValue {
            updated_at: Some(proto::EpochMillis {
                millis: self.updated_at,
            }),
            references: self
                .references
                .iter()
                .map(|reference| reference.into_proto())
                .collect(),
        }
    }
    fn from_proto(proto: proto::SourceReferencesValue) -> Result<Self, TryFromProtoError> {
        Ok(SourceReferencesValue {
            updated_at: proto
                .updated_at
                .into_rust_if_some("SourceReferencesValue::updated_at")?,
            references: proto
                .references
                .into_iter()
                .map(|reference| reference.into_rust())
                .collect::<Result<_, _>>()?,
        })
    }
}

impl RustType<proto::SourceReference> for SourceReference {
    fn into_proto(&self) -> proto::SourceReference {
        proto::SourceReference {
            name: self.name.clone(),
            namespace: self.namespace.clone(),
            columns: self.columns.clone(),
        }
    }
    fn from_proto(proto: proto::SourceReference) -> Result<Self, TryFromProtoError> {
        Ok(SourceReference {
            name: proto.name,
            namespace: proto.namespace,
            columns: proto.columns,
        })
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
            object_type: proto::ObjectType::try_from(proto.object_type)
                .map_err(|_| TryFromProtoError::unknown_enum_variant("ObjectType"))?
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

impl RustType<proto::SystemCatalogItemId> for SystemCatalogItemId {
    fn into_proto(&self) -> proto::SystemCatalogItemId {
        proto::SystemCatalogItemId { value: self.0 }
    }

    fn from_proto(proto: proto::SystemCatalogItemId) -> Result<Self, TryFromProtoError> {
        Ok(SystemCatalogItemId(proto.value))
    }
}

impl RustType<proto::IntrospectionSourceIndexCatalogItemId>
    for IntrospectionSourceIndexCatalogItemId
{
    fn into_proto(&self) -> proto::IntrospectionSourceIndexCatalogItemId {
        proto::IntrospectionSourceIndexCatalogItemId { value: self.0 }
    }

    fn from_proto(
        proto: proto::IntrospectionSourceIndexCatalogItemId,
    ) -> Result<Self, TryFromProtoError> {
        Ok(IntrospectionSourceIndexCatalogItemId(proto.value))
    }
}

impl RustType<proto::SystemGlobalId> for SystemGlobalId {
    fn into_proto(&self) -> proto::SystemGlobalId {
        proto::SystemGlobalId { value: self.0 }
    }

    fn from_proto(proto: proto::SystemGlobalId) -> Result<Self, TryFromProtoError> {
        Ok(SystemGlobalId(proto.value))
    }
}

impl RustType<proto::IntrospectionSourceIndexGlobalId> for IntrospectionSourceIndexGlobalId {
    fn into_proto(&self) -> proto::IntrospectionSourceIndexGlobalId {
        proto::IntrospectionSourceIndexGlobalId { value: self.0 }
    }

    fn from_proto(
        proto: proto::IntrospectionSourceIndexGlobalId,
    ) -> Result<Self, TryFromProtoError> {
        Ok(IntrospectionSourceIndexGlobalId(proto.value))
    }
}

#[cfg(test)]
mod tests {
    use mz_audit_log::VersionedEvent;
    use mz_proto::RustType;
    use proptest::prelude::*;

    proptest! {
        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // slow
        fn proptest_audit_log_roundtrips(event: VersionedEvent) {
            let proto = event.into_proto();
            let roundtrip = VersionedEvent::from_proto(proto).expect("valid proto");

            prop_assert_eq!(event, roundtrip);
        }
    }
}
