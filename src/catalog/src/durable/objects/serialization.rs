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
use mz_proto::{ProtoType, RustType, TryFromProtoError};

use crate::durable::objects::state_update::StateUpdateKindJson;
use crate::durable::objects::{
    AuditLogKey, ClusterKey, ClusterReplicaKey, ClusterReplicaValue, ClusterValue, CommentKey,
    CommentValue, ConfigKey, ConfigValue, DatabaseKey, DatabaseValue, DefaultPrivilegeKey,
    DefaultPrivilegeValue, IdAllocatorKey, IdAllocatorValue, IntrospectionSourceIndexCatalogItemId,
    IntrospectionSourceIndexGlobalId, IntrospectionSourceIndexKey, IntrospectionSourceIndexValue,
    ItemKey, ItemValue, NetworkPolicyKey, NetworkPolicyValue, RoleKey, RoleValue, SchemaKey,
    SchemaValue, SettingKey, SettingValue, SourceReference, SourceReferencesKey,
    SourceReferencesValue, StorageCollectionMetadataKey, StorageCollectionMetadataValue,
    SystemCatalogItemId, SystemConfigurationKey, SystemConfigurationValue, SystemGlobalId,
    SystemObjectMappingKey, SystemObjectMappingValue, SystemPrivilegeKey, SystemPrivilegeValue,
    TxnWalShardValue, UnfinalizedShardKey,
};
use crate::durable::{
    ClusterConfig, ClusterVariant, ClusterVariantManaged, ReplicaConfig, ReplicaLocation,
};

use super::{RoleAuthKey, RoleAuthValue};

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
            variant: self.variant.into_proto(),
            workload_class: self.workload_class.clone(),
        }
    }

    fn from_proto(proto: proto::ClusterConfig) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            variant: proto.variant.into_rust()?,
            workload_class: proto.workload_class,
        })
    }
}

impl RustType<proto::ClusterVariant> for ClusterVariant {
    fn into_proto(&self) -> proto::ClusterVariant {
        match self {
            ClusterVariant::Managed(ClusterVariantManaged {
                size,
                availability_zones,
                logging,
                replication_factor,
                optimizer_feature_overrides,
                schedule,
            }) => proto::ClusterVariant::Managed(proto::ManagedCluster {
                size: size.to_string(),
                availability_zones: availability_zones.clone(),
                logging: logging.into_proto(),
                replication_factor: *replication_factor,
                optimizer_feature_overrides: optimizer_feature_overrides.into_proto(),
                schedule: schedule.into_proto(),
            }),
            ClusterVariant::Unmanaged => proto::ClusterVariant::Unmanaged,
        }
    }

    fn from_proto(proto: proto::ClusterVariant) -> Result<Self, TryFromProtoError> {
        match proto {
            proto::ClusterVariant::Unmanaged => Ok(Self::Unmanaged),
            proto::ClusterVariant::Managed(managed) => Ok(Self::Managed(ClusterVariantManaged {
                size: managed.size,
                availability_zones: managed.availability_zones,
                logging: managed.logging.into_rust()?,
                replication_factor: managed.replication_factor,
                optimizer_feature_overrides: managed.optimizer_feature_overrides.into_rust()?,
                schedule: managed.schedule.into_rust()?,
            })),
        }
    }
}

impl RustType<proto::ReplicaConfig> for ReplicaConfig {
    fn into_proto(&self) -> proto::ReplicaConfig {
        proto::ReplicaConfig {
            logging: self.logging.into_proto(),
            location: self.location.into_proto(),
        }
    }

    fn from_proto(proto: proto::ReplicaConfig) -> Result<Self, TryFromProtoError> {
        Ok(ReplicaConfig {
            location: proto.location.into_rust()?,
            logging: proto.logging.into_rust()?,
        })
    }
}

impl RustType<proto::ReplicaLocation> for ReplicaLocation {
    fn into_proto(&self) -> proto::ReplicaLocation {
        match self {
            ReplicaLocation::Unmanaged {
                storagectl_addrs,
                computectl_addrs,
            } => proto::ReplicaLocation::Unmanaged(proto::UnmanagedLocation {
                storagectl_addrs: storagectl_addrs.clone(),
                computectl_addrs: computectl_addrs.clone(),
            }),
            ReplicaLocation::Managed {
                size,
                availability_zone,
                billed_as,
                internal,
                pending,
            } => proto::ReplicaLocation::Managed(proto::ManagedLocation {
                size: size.to_string(),
                availability_zone: availability_zone.clone(),
                billed_as: billed_as.clone(),
                internal: *internal,
                pending: *pending,
            }),
        }
    }

    fn from_proto(proto: proto::ReplicaLocation) -> Result<Self, TryFromProtoError> {
        match proto {
            proto::ReplicaLocation::Unmanaged(location) => Ok(ReplicaLocation::Unmanaged {
                storagectl_addrs: location.storagectl_addrs,
                computectl_addrs: location.computectl_addrs,
            }),
            proto::ReplicaLocation::Managed(location) => Ok(ReplicaLocation::Managed {
                availability_zone: location.availability_zone,
                billed_as: location.billed_as,
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

impl RustType<proto::IdAllocatorKey> for IdAllocatorKey {
    fn into_proto(&self) -> proto::IdAllocatorKey {
        proto::IdAllocatorKey {
            name: self.name.to_string(),
        }
    }

    fn from_proto(proto: proto::IdAllocatorKey) -> Result<Self, TryFromProtoError> {
        Ok(IdAllocatorKey { name: proto.name })
    }
}

impl RustType<proto::IdAllocatorValue> for IdAllocatorValue {
    fn into_proto(&self) -> proto::IdAllocatorValue {
        proto::IdAllocatorValue {
            next_id: self.next_id,
        }
    }

    fn from_proto(proto: proto::IdAllocatorValue) -> Result<Self, TryFromProtoError> {
        Ok(IdAllocatorValue {
            next_id: proto.next_id,
        })
    }
}

impl RustType<proto::SystemObjectMappingKey> for SystemObjectMappingKey {
    fn into_proto(&self) -> proto::SystemObjectMappingKey {
        proto::SystemObjectMappingKey {
            schema_name: self.schema_name.to_string(),
            object_type: self.object_type.into_proto(),
            object_name: self.object_name.to_string(),
        }
    }

    fn from_proto(proto: proto::SystemObjectMappingKey) -> Result<Self, TryFromProtoError> {
        Ok(SystemObjectMappingKey {
            schema_name: proto.schema_name,
            object_type: proto.object_type.into_rust()?,
            object_name: proto.object_name,
        })
    }
}

impl RustType<proto::SystemObjectMappingValue> for SystemObjectMappingValue {
    fn into_proto(&self) -> proto::SystemObjectMappingValue {
        proto::SystemObjectMappingValue {
            catalog_id: self.catalog_id.into_proto(),
            global_id: self.global_id.into_proto(),
            fingerprint: self.fingerprint.to_string(),
        }
    }

    fn from_proto(proto: proto::SystemObjectMappingValue) -> Result<Self, TryFromProtoError> {
        Ok(SystemObjectMappingValue {
            catalog_id: proto.catalog_id.into_rust()?,
            global_id: proto.global_id.into_rust()?,
            fingerprint: proto.fingerprint,
        })
    }
}

impl RustType<proto::ClusterKey> for ClusterKey {
    fn into_proto(&self) -> proto::ClusterKey {
        proto::ClusterKey {
            id: self.id.into_proto(),
        }
    }

    fn from_proto(proto: proto::ClusterKey) -> Result<Self, TryFromProtoError> {
        Ok(ClusterKey {
            id: proto.id.into_rust()?,
        })
    }
}

impl RustType<proto::ClusterValue> for ClusterValue {
    fn into_proto(&self) -> proto::ClusterValue {
        proto::ClusterValue {
            name: self.name.to_string(),
            config: self.config.into_proto(),
            owner_id: self.owner_id.into_proto(),
            privileges: self.privileges.into_proto(),
        }
    }

    fn from_proto(proto: proto::ClusterValue) -> Result<Self, TryFromProtoError> {
        Ok(ClusterValue {
            name: proto.name,
            config: proto.config.into_rust()?,
            owner_id: proto.owner_id.into_rust()?,
            privileges: proto.privileges.into_rust()?,
        })
    }
}

impl RustType<proto::IntrospectionSourceIndexKey> for IntrospectionSourceIndexKey {
    fn into_proto(&self) -> proto::IntrospectionSourceIndexKey {
        proto::IntrospectionSourceIndexKey {
            cluster_id: self.cluster_id.into_proto(),
            name: self.name.to_string(),
        }
    }

    fn from_proto(proto: proto::IntrospectionSourceIndexKey) -> Result<Self, TryFromProtoError> {
        Ok(IntrospectionSourceIndexKey {
            cluster_id: proto.cluster_id.into_rust()?,
            name: proto.name,
        })
    }
}

impl RustType<proto::IntrospectionSourceIndexValue> for IntrospectionSourceIndexValue {
    fn into_proto(&self) -> proto::IntrospectionSourceIndexValue {
        proto::IntrospectionSourceIndexValue {
            catalog_id: self.catalog_id.into_proto(),
            global_id: self.global_id.into_proto(),
            oid: self.oid,
        }
    }

    fn from_proto(proto: proto::IntrospectionSourceIndexValue) -> Result<Self, TryFromProtoError> {
        Ok(IntrospectionSourceIndexValue {
            catalog_id: proto.catalog_id.into_rust()?,
            global_id: proto.global_id.into_rust()?,
            oid: proto.oid,
        })
    }
}

impl RustType<proto::ClusterReplicaKey> for ClusterReplicaKey {
    fn into_proto(&self) -> proto::ClusterReplicaKey {
        proto::ClusterReplicaKey {
            id: self.id.into_proto(),
        }
    }

    fn from_proto(proto: proto::ClusterReplicaKey) -> Result<Self, TryFromProtoError> {
        Ok(ClusterReplicaKey {
            id: proto.id.into_rust()?,
        })
    }
}

impl RustType<proto::ClusterReplicaValue> for ClusterReplicaValue {
    fn into_proto(&self) -> proto::ClusterReplicaValue {
        proto::ClusterReplicaValue {
            cluster_id: self.cluster_id.into_proto(),
            name: self.name.to_string(),
            config: self.config.into_proto(),
            owner_id: self.owner_id.into_proto(),
        }
    }

    fn from_proto(proto: proto::ClusterReplicaValue) -> Result<Self, TryFromProtoError> {
        Ok(ClusterReplicaValue {
            cluster_id: proto.cluster_id.into_rust()?,
            name: proto.name,
            config: proto.config.into_rust()?,
            owner_id: proto.owner_id.into_rust()?,
        })
    }
}

impl RustType<proto::DatabaseKey> for DatabaseKey {
    fn into_proto(&self) -> proto::DatabaseKey {
        proto::DatabaseKey {
            id: self.id.into_proto(),
        }
    }

    fn from_proto(proto: proto::DatabaseKey) -> Result<Self, TryFromProtoError> {
        Ok(DatabaseKey {
            id: proto.id.into_rust()?,
        })
    }
}

impl RustType<proto::DatabaseValue> for DatabaseValue {
    fn into_proto(&self) -> proto::DatabaseValue {
        proto::DatabaseValue {
            name: self.name.clone(),
            owner_id: self.owner_id.into_proto(),
            privileges: self.privileges.into_proto(),
            oid: self.oid,
        }
    }

    fn from_proto(proto: proto::DatabaseValue) -> Result<Self, TryFromProtoError> {
        Ok(DatabaseValue {
            name: proto.name,
            owner_id: proto.owner_id.into_rust()?,
            privileges: proto.privileges.into_rust()?,
            oid: proto.oid,
        })
    }
}

impl RustType<proto::SchemaKey> for SchemaKey {
    fn into_proto(&self) -> proto::SchemaKey {
        proto::SchemaKey {
            id: self.id.into_proto(),
        }
    }

    fn from_proto(proto: proto::SchemaKey) -> Result<Self, TryFromProtoError> {
        Ok(SchemaKey {
            id: proto.id.into_rust()?,
        })
    }
}

impl RustType<proto::SchemaValue> for SchemaValue {
    fn into_proto(&self) -> proto::SchemaValue {
        proto::SchemaValue {
            name: self.name.clone(),
            database_id: self.database_id.map(|id| id.into_proto()),
            owner_id: self.owner_id.into_proto(),
            privileges: self.privileges.into_proto(),
            oid: self.oid,
        }
    }

    fn from_proto(proto: proto::SchemaValue) -> Result<Self, TryFromProtoError> {
        Ok(SchemaValue {
            name: proto.name,
            database_id: proto.database_id.into_rust()?,
            owner_id: proto.owner_id.into_rust()?,
            privileges: proto.privileges.into_rust()?,
            oid: proto.oid,
        })
    }
}

impl RustType<proto::ItemKey> for ItemKey {
    fn into_proto(&self) -> proto::ItemKey {
        proto::ItemKey {
            gid: self.id.into_proto(),
        }
    }

    fn from_proto(proto: proto::ItemKey) -> Result<Self, TryFromProtoError> {
        Ok(ItemKey {
            id: proto.gid.into_rust()?,
        })
    }
}

impl RustType<proto::ItemValue> for ItemValue {
    fn into_proto(&self) -> proto::ItemValue {
        let definition = proto::CatalogItem::V1(proto::CatalogItemV1 {
            create_sql: self.create_sql.clone(),
        });
        proto::ItemValue {
            schema_id: self.schema_id.into_proto(),
            name: self.name.to_string(),
            definition,
            owner_id: self.owner_id.into_proto(),
            privileges: self.privileges.into_proto(),
            oid: self.oid,
            global_id: self.global_id.into_proto(),
            extra_versions: self
                .extra_versions
                .iter()
                .map(|(version, global_id)| proto::ItemVersion {
                    global_id: global_id.into_proto(),
                    version: version.into_proto(),
                })
                .collect(),
        }
    }

    fn from_proto(proto: proto::ItemValue) -> Result<Self, TryFromProtoError> {
        let create_sql = match proto.definition {
            proto::CatalogItem::V1(c) => c.create_sql,
        };
        let extra_versions = proto
            .extra_versions
            .into_iter()
            .map(|item_version| {
                let version = item_version.version.into_rust()?;
                let global_id = item_version.global_id.into_rust()?;
                Ok::<_, TryFromProtoError>((version, global_id))
            })
            .collect::<Result<_, _>>()?;
        Ok(ItemValue {
            schema_id: proto.schema_id.into_rust()?,
            name: proto.name,
            create_sql,
            owner_id: proto.owner_id.into_rust()?,
            privileges: proto.privileges.into_rust()?,
            oid: proto.oid,
            global_id: proto.global_id.into_rust()?,
            extra_versions,
        })
    }
}

impl RustType<proto::CommentKey> for CommentKey {
    fn into_proto(&self) -> proto::CommentKey {
        let sub_component = match &self.sub_component {
            Some(pos) => Some(proto::CommentSubComponent::ColumnPos(CastFrom::cast_from(
                *pos,
            ))),
            None => None,
        };
        proto::CommentKey {
            object: self.object_id.into_proto(),
            sub_component,
        }
    }

    fn from_proto(proto: proto::CommentKey) -> Result<Self, TryFromProtoError> {
        let sub_component = match proto.sub_component {
            Some(proto::CommentSubComponent::ColumnPos(pos)) => Some(CastFrom::cast_from(pos)),
            None => None,
        };
        Ok(CommentKey {
            object_id: proto.object.into_rust()?,
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
            id: self.id.into_proto(),
        }
    }

    fn from_proto(proto: proto::RoleKey) -> Result<Self, TryFromProtoError> {
        Ok(RoleKey {
            id: proto.id.into_rust()?,
        })
    }
}

impl RustType<proto::RoleValue> for RoleValue {
    fn into_proto(&self) -> proto::RoleValue {
        proto::RoleValue {
            name: self.name.to_string(),
            attributes: self.attributes.into_proto(),
            membership: self.membership.into_proto(),
            vars: self.vars.into_proto(),
            oid: self.oid,
        }
    }

    fn from_proto(proto: proto::RoleValue) -> Result<Self, TryFromProtoError> {
        Ok(RoleValue {
            name: proto.name,
            attributes: proto.attributes.into_rust()?,
            membership: proto.membership.into_rust()?,
            vars: proto.vars.into_rust()?,
            oid: proto.oid,
        })
    }
}

impl RustType<proto::RoleAuthKey> for RoleAuthKey {
    fn into_proto(&self) -> proto::RoleAuthKey {
        proto::RoleAuthKey {
            id: self.role_id.into_proto(),
        }
    }

    fn from_proto(proto: proto::RoleAuthKey) -> Result<Self, TryFromProtoError> {
        Ok(RoleAuthKey {
            role_id: proto.id.into_rust()?,
        })
    }
}

impl RustType<proto::RoleAuthValue> for RoleAuthValue {
    fn into_proto(&self) -> proto::RoleAuthValue {
        proto::RoleAuthValue {
            password_hash: self.password_hash.clone(),
            updated_at: proto::EpochMillis {
                millis: self.updated_at,
            },
        }
    }

    fn from_proto(proto: proto::RoleAuthValue) -> Result<Self, TryFromProtoError> {
        Ok(RoleAuthValue {
            password_hash: proto.password_hash,
            updated_at: proto.updated_at.into_rust()?,
        })
    }
}

impl RustType<proto::NetworkPolicyKey> for NetworkPolicyKey {
    fn into_proto(&self) -> proto::NetworkPolicyKey {
        proto::NetworkPolicyKey {
            id: self.id.into_proto(),
        }
    }

    fn from_proto(proto: proto::NetworkPolicyKey) -> Result<Self, TryFromProtoError> {
        Ok(NetworkPolicyKey {
            id: proto.id.into_rust()?,
        })
    }
}

impl RustType<proto::NetworkPolicyValue> for NetworkPolicyValue {
    fn into_proto(&self) -> proto::NetworkPolicyValue {
        proto::NetworkPolicyValue {
            name: self.name.to_string(),
            rules: self.rules.into_proto(),
            owner_id: self.owner_id.into_proto(),
            privileges: self.privileges.into_proto(),
            oid: self.oid,
        }
    }

    fn from_proto(proto: proto::NetworkPolicyValue) -> Result<Self, TryFromProtoError> {
        Ok(NetworkPolicyValue {
            name: proto.name,
            rules: proto.rules.into_rust()?,
            owner_id: proto.owner_id.into_rust()?,
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
            event: self.event.into_proto(),
        }
    }

    fn from_proto(proto: proto::AuditLogKey) -> Result<Self, TryFromProtoError> {
        Ok(AuditLogKey {
            event: proto.event.into_rust()?,
        })
    }
}

impl RustType<proto::StorageCollectionMetadataKey> for StorageCollectionMetadataKey {
    fn into_proto(&self) -> proto::StorageCollectionMetadataKey {
        proto::StorageCollectionMetadataKey {
            id: self.id.into_proto(),
        }
    }

    fn from_proto(proto: proto::StorageCollectionMetadataKey) -> Result<Self, TryFromProtoError> {
        Ok(StorageCollectionMetadataKey {
            id: proto.id.into_rust()?,
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

impl RustType<proto::SystemConfigurationKey> for SystemConfigurationKey {
    fn into_proto(&self) -> proto::SystemConfigurationKey {
        proto::SystemConfigurationKey {
            name: self.name.clone(),
        }
    }

    fn from_proto(proto: proto::SystemConfigurationKey) -> Result<Self, TryFromProtoError> {
        Ok(SystemConfigurationKey { name: proto.name })
    }
}

impl RustType<proto::SystemConfigurationValue> for SystemConfigurationValue {
    fn into_proto(&self) -> proto::SystemConfigurationValue {
        proto::SystemConfigurationValue {
            value: self.value.clone(),
        }
    }

    fn from_proto(proto: proto::SystemConfigurationValue) -> Result<Self, TryFromProtoError> {
        Ok(SystemConfigurationValue { value: proto.value })
    }
}

impl RustType<proto::SourceReferencesKey> for SourceReferencesKey {
    fn into_proto(&self) -> proto::SourceReferencesKey {
        proto::SourceReferencesKey {
            source: self.source_id.into_proto(),
        }
    }
    fn from_proto(proto: proto::SourceReferencesKey) -> Result<Self, TryFromProtoError> {
        Ok(SourceReferencesKey {
            source_id: proto.source.into_rust()?,
        })
    }
}

impl RustType<proto::SourceReferencesValue> for SourceReferencesValue {
    fn into_proto(&self) -> proto::SourceReferencesValue {
        proto::SourceReferencesValue {
            updated_at: proto::EpochMillis {
                millis: self.updated_at,
            },
            references: self
                .references
                .iter()
                .map(|reference| reference.into_proto())
                .collect(),
        }
    }
    fn from_proto(proto: proto::SourceReferencesValue) -> Result<Self, TryFromProtoError> {
        Ok(SourceReferencesValue {
            updated_at: proto.updated_at.into_rust()?,
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

impl RustType<proto::DefaultPrivilegeKey> for DefaultPrivilegeKey {
    fn into_proto(&self) -> proto::DefaultPrivilegeKey {
        proto::DefaultPrivilegeKey {
            role_id: self.role_id.into_proto(),
            database_id: self.database_id.map(|database_id| database_id.into_proto()),
            schema_id: self.schema_id.map(|schema_id| schema_id.into_proto()),
            object_type: self.object_type.into_proto(),
            grantee: self.grantee.into_proto(),
        }
    }

    fn from_proto(proto: proto::DefaultPrivilegeKey) -> Result<Self, TryFromProtoError> {
        Ok(DefaultPrivilegeKey {
            role_id: proto.role_id.into_rust()?,
            database_id: proto.database_id.into_rust()?,
            schema_id: proto.schema_id.into_rust()?,
            object_type: proto.object_type.into_rust()?,
            grantee: proto.grantee.into_rust()?,
        })
    }
}

impl RustType<proto::DefaultPrivilegeValue> for DefaultPrivilegeValue {
    fn into_proto(&self) -> proto::DefaultPrivilegeValue {
        proto::DefaultPrivilegeValue {
            privileges: self.privileges.into_proto(),
        }
    }

    fn from_proto(proto: proto::DefaultPrivilegeValue) -> Result<Self, TryFromProtoError> {
        Ok(DefaultPrivilegeValue {
            privileges: proto.privileges.into_rust()?,
        })
    }
}

impl RustType<proto::SystemPrivilegeKey> for SystemPrivilegeKey {
    fn into_proto(&self) -> proto::SystemPrivilegeKey {
        proto::SystemPrivilegeKey {
            grantee: self.grantee.into_proto(),
            grantor: self.grantor.into_proto(),
        }
    }

    fn from_proto(proto: proto::SystemPrivilegeKey) -> Result<Self, TryFromProtoError> {
        Ok(SystemPrivilegeKey {
            grantee: proto.grantee.into_rust()?,
            grantor: proto.grantor.into_rust()?,
        })
    }
}

impl RustType<proto::SystemPrivilegeValue> for SystemPrivilegeValue {
    fn into_proto(&self) -> proto::SystemPrivilegeValue {
        proto::SystemPrivilegeValue {
            acl_mode: self.acl_mode.into_proto(),
        }
    }

    fn from_proto(proto: proto::SystemPrivilegeValue) -> Result<Self, TryFromProtoError> {
        Ok(SystemPrivilegeValue {
            acl_mode: proto.acl_mode.into_rust()?,
        })
    }
}

impl RustType<proto::SystemCatalogItemId> for SystemCatalogItemId {
    fn into_proto(&self) -> proto::SystemCatalogItemId {
        proto::SystemCatalogItemId(self.0)
    }

    fn from_proto(proto: proto::SystemCatalogItemId) -> Result<Self, TryFromProtoError> {
        Ok(SystemCatalogItemId(proto.0))
    }
}

impl RustType<proto::IntrospectionSourceIndexCatalogItemId>
    for IntrospectionSourceIndexCatalogItemId
{
    fn into_proto(&self) -> proto::IntrospectionSourceIndexCatalogItemId {
        proto::IntrospectionSourceIndexCatalogItemId(self.0)
    }

    fn from_proto(
        proto: proto::IntrospectionSourceIndexCatalogItemId,
    ) -> Result<Self, TryFromProtoError> {
        Ok(IntrospectionSourceIndexCatalogItemId(proto.0))
    }
}

impl RustType<proto::SystemGlobalId> for SystemGlobalId {
    fn into_proto(&self) -> proto::SystemGlobalId {
        proto::SystemGlobalId(self.0)
    }

    fn from_proto(proto: proto::SystemGlobalId) -> Result<Self, TryFromProtoError> {
        Ok(SystemGlobalId(proto.0))
    }
}

impl RustType<proto::IntrospectionSourceIndexGlobalId> for IntrospectionSourceIndexGlobalId {
    fn into_proto(&self) -> proto::IntrospectionSourceIndexGlobalId {
        proto::IntrospectionSourceIndexGlobalId(self.0)
    }

    fn from_proto(
        proto: proto::IntrospectionSourceIndexGlobalId,
    ) -> Result<Self, TryFromProtoError> {
        Ok(IntrospectionSourceIndexGlobalId(proto.0))
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
