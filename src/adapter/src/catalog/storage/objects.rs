// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_ore::cast::CastFrom;
use mz_proto::{IntoRustIfSome, ProtoType};
use mz_stash::objects::{proto, RustType, TryFromProtoError};

use crate::catalog::storage::{
    ClusterItemVariant, ClusterReplica, DefaultPrivilegesKey, DefaultPrivilegesValue, ReplicaSet,
    SystemPrivilegesKey, SystemPrivilegesValue,
};
use crate::catalog::{
    ClusterConfig, ClusterVariant, ClusterVariantManaged, RoleMembership, SerializedCatalogItem,
};

use super::{
    AuditLogKey, ClusterIntrospectionSourceIndexKey, ClusterIntrospectionSourceIndexValue,
    ClusterItemKey, ClusterItemValue, ClusterKey, ClusterValue, ConfigKey, ConfigValue,
    DatabaseKey, DatabaseValue, GidMappingKey, GidMappingValue, IdAllocKey, IdAllocValue, ItemKey,
    ItemValue, RoleKey, RoleValue, SchemaKey, SchemaValue, SerializedReplicaConfig,
    SerializedReplicaLocation, SerializedReplicaLogging, ServerConfigurationKey,
    ServerConfigurationValue, SettingKey, SettingValue, StorageUsageKey, TimestampKey,
    TimestampValue,
};

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
            linked_object_id: self.linked_object_id.into_proto(),
            owner_id: Some(self.owner_id.into_proto()),
            privileges: self.privileges.into_proto(),
        }
    }

    fn from_proto(proto: proto::ClusterValue) -> Result<Self, TryFromProtoError> {
        Ok(ClusterValue {
            name: proto.name,
            config: proto.config.unwrap_or_default().into_rust()?,
            linked_object_id: proto.linked_object_id.into_rust()?,
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
        }
    }

    fn from_proto(
        proto: proto::ClusterIntrospectionSourceIndexValue,
    ) -> Result<Self, TryFromProtoError> {
        Ok(ClusterIntrospectionSourceIndexValue {
            index_id: proto.index_id,
        })
    }
}

impl RustType<proto::ClusterItemKey> for ClusterItemKey {
    fn into_proto(&self) -> proto::ClusterItemKey {
        proto::ClusterItemKey {
            id: Some(self.id.into_proto()),
        }
    }

    fn from_proto(proto: proto::ClusterItemKey) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            id: proto.id.into_rust_if_some("ClusterReplicaKey::id")?,
        })
    }
}

impl RustType<proto::ClusterItemValue> for ClusterItemValue {
    fn into_proto(&self) -> proto::ClusterItemValue {
        proto::ClusterItemValue {
            cluster_id: Some(self.cluster_id.into_proto()),
            name: self.name.to_string(),
            item: Some(self.item.into_proto()),
            owner_id: Some(self.owner_id.into_proto()),
        }
    }

    fn from_proto(proto: proto::ClusterItemValue) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            cluster_id: proto
                .cluster_id
                .into_rust_if_some("ClusterItemValue::cluster_id")?,
            name: proto.name,
            item: proto.item.into_rust_if_some("ClusterItemValue::item")?,
            owner_id: proto
                .owner_id
                .into_rust_if_some("ClusterItemValue::owner_id")?,
        })
    }
}

impl RustType<proto::cluster_item_value::Item> for ClusterItemVariant {
    fn into_proto(&self) -> proto::cluster_item_value::Item {
        match self {
            ClusterItemVariant::Replica(replica) => {
                proto::cluster_item_value::Item::Replica(replica.into_proto())
            }
            ClusterItemVariant::ReplicaSet(profile) => {
                proto::cluster_item_value::Item::ReplicaSet(profile.into_proto())
            }
        }
    }

    fn from_proto(proto: proto::cluster_item_value::Item) -> Result<Self, TryFromProtoError> {
        Ok(match proto {
            proto::cluster_item_value::Item::Replica(replica) => {
                Self::Replica(replica.into_rust()?)
            }
            proto::cluster_item_value::Item::ReplicaSet(profile) => {
                Self::ReplicaSet(profile.into_rust()?)
            }
        })
    }
}

impl RustType<proto::ReplicaSetConfig> for ReplicaSet {
    fn into_proto(&self) -> proto::ReplicaSetConfig {
        proto::ReplicaSetConfig {
            replica_config: Some(self.replica_config.into_proto()),
        }
    }

    fn from_proto(proto: proto::ReplicaSetConfig) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            replica_config: proto
                .replica_config
                .into_rust_if_some("ReplicaSetConfig::replica_config")?,
        })
    }
}

impl RustType<proto::ClusterReplicaConfig> for ClusterReplica {
    fn into_proto(&self) -> proto::ClusterReplicaConfig {
        proto::ClusterReplicaConfig {
            replica_set_id: self.replica_set_id.into_proto(),
            replica_config: Some(self.replica_config.into_proto()),
        }
    }

    fn from_proto(proto: proto::ClusterReplicaConfig) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            replica_set_id: proto
                .replica_set_id
                .map(|replica_set_id| replica_set_id.into_rust())
                .transpose()?,
            replica_config: proto
                .replica_config
                .into_rust_if_some("ClusterProfileConfig::replica_config")?,
        })
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
                idle_arrangement_merge_effort,
                replication_factor,
                disk,
            }) => proto::cluster_config::Variant::Managed(proto::ManagedCluster {
                size: size.to_string(),
                availability_zones: availability_zones.clone(),
                logging: Some(logging.into_proto()),
                idle_arrangement_merge_effort: idle_arrangement_merge_effort
                    .map(|effort| proto::ReplicaMergeEffort { effort }),
                replication_factor: *replication_factor,
                disk: *disk,
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
                    idle_arrangement_merge_effort: managed
                        .idle_arrangement_merge_effort
                        .map(|e| e.effort),
                    replication_factor: managed.replication_factor,
                    disk: managed.disk,
                }))
            }
        }
    }
}

impl RustType<proto::ReplicaConfig> for SerializedReplicaConfig {
    fn into_proto(&self) -> proto::ReplicaConfig {
        proto::ReplicaConfig {
            logging: Some(self.logging.into_proto()),
            location: Some(self.location.into_proto()),
            idle_arrangement_merge_effort: self
                .idle_arrangement_merge_effort
                .map(|effort| proto::ReplicaMergeEffort { effort }),
        }
    }

    fn from_proto(proto: proto::ReplicaConfig) -> Result<Self, TryFromProtoError> {
        Ok(SerializedReplicaConfig {
            location: proto
                .location
                .into_rust_if_some("ReplicaConfig::location")?,
            logging: proto.logging.into_rust_if_some("ReplicaConfig::logging")?,
            idle_arrangement_merge_effort: proto.idle_arrangement_merge_effort.map(|e| e.effort),
        })
    }
}

impl RustType<proto::ReplicaLogging> for SerializedReplicaLogging {
    fn into_proto(&self) -> proto::ReplicaLogging {
        proto::ReplicaLogging {
            log_logging: self.log_logging,
            interval: self.interval.into_proto(),
        }
    }

    fn from_proto(proto: proto::ReplicaLogging) -> Result<Self, TryFromProtoError> {
        Ok(SerializedReplicaLogging {
            log_logging: proto.log_logging,
            interval: proto.interval.into_rust()?,
        })
    }
}

impl RustType<proto::replica_config::Location> for SerializedReplicaLocation {
    fn into_proto(&self) -> proto::replica_config::Location {
        match self {
            SerializedReplicaLocation::Unmanaged {
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
            SerializedReplicaLocation::Managed {
                size,
                availability_zone,
                disk,
            } => proto::replica_config::Location::Managed(proto::replica_config::ManagedLocation {
                size: size.to_string(),
                availability_zone: availability_zone.clone(),
                disk: *disk,
            }),
        }
    }

    fn from_proto(proto: proto::replica_config::Location) -> Result<Self, TryFromProtoError> {
        match proto {
            proto::replica_config::Location::Unmanaged(location) => {
                Ok(SerializedReplicaLocation::Unmanaged {
                    storagectl_addrs: location.storagectl_addrs,
                    storage_addrs: location.storage_addrs,
                    computectl_addrs: location.computectl_addrs,
                    compute_addrs: location.compute_addrs,
                    workers: CastFrom::cast_from(location.workers),
                })
            }
            proto::replica_config::Location::Managed(location) => {
                Ok(SerializedReplicaLocation::Managed {
                    size: location.size,
                    availability_zone: location.availability_zone,
                    disk: location.disk,
                })
            }
        }
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
        }
    }

    fn from_proto(proto: proto::DatabaseValue) -> Result<Self, TryFromProtoError> {
        Ok(DatabaseValue {
            name: proto.name,
            owner_id: (proto
                .owner_id
                .into_rust_if_some("DatabaseValue::owner_id")?),
            privileges: proto.privileges.into_rust()?,
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
        }
    }

    fn from_proto(proto: proto::SchemaValue) -> Result<Self, TryFromProtoError> {
        Ok(SchemaValue {
            name: proto.name,
            database_id: proto.database_id.into_rust()?,
            owner_id: (proto
                .owner_id
                .into_rust_if_some("DatabaseValue::owner_id")?),
            privileges: proto.privileges.into_rust()?,
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

impl RustType<proto::CatalogItem> for SerializedCatalogItem {
    fn into_proto(&self) -> proto::CatalogItem {
        let value = match self.clone() {
            SerializedCatalogItem::V1 { create_sql } => {
                proto::catalog_item::Value::V1(proto::catalog_item::V1 { create_sql })
            }
        };

        proto::CatalogItem { value: Some(value) }
    }

    fn from_proto(proto: proto::CatalogItem) -> Result<Self, TryFromProtoError> {
        let value = proto
            .value
            .ok_or_else(|| TryFromProtoError::missing_field("CatalogItem::value"))?;
        match value {
            proto::catalog_item::Value::V1(c) => Ok(SerializedCatalogItem::V1 {
                create_sql: c.create_sql,
            }),
        }
    }
}

impl RustType<proto::ItemValue> for ItemValue {
    fn into_proto(&self) -> proto::ItemValue {
        proto::ItemValue {
            schema_id: Some(self.schema_id.into_proto()),
            name: self.name.to_string(),
            definition: Some(self.definition.into_proto()),
            owner_id: Some(self.owner_id.into_proto()),
            privileges: self.privileges.into_proto(),
        }
    }

    fn from_proto(proto: proto::ItemValue) -> Result<Self, TryFromProtoError> {
        Ok(ItemValue {
            schema_id: proto.schema_id.into_rust_if_some("ItemValue::schema_id")?,
            name: proto.name,
            definition: proto
                .definition
                .into_rust_if_some("ItemValue::definition")?,
            owner_id: proto.owner_id.into_rust_if_some("ItemValue::owner_id")?,
            privileges: proto.privileges.into_rust()?,
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

impl RustType<proto::RoleValue> for RoleValue {
    fn into_proto(&self) -> proto::RoleValue {
        proto::RoleValue {
            name: self.name.to_string(),
            attributes: Some(self.attributes.into_proto()),
            membership: Some(self.membership.into_proto()),
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
        })
    }
}

impl RustType<proto::TimestampKey> for TimestampKey {
    fn into_proto(&self) -> proto::TimestampKey {
        proto::TimestampKey {
            id: self.id.clone(),
        }
    }

    fn from_proto(proto: proto::TimestampKey) -> Result<Self, TryFromProtoError> {
        Ok(TimestampKey { id: proto.id })
    }
}

impl RustType<proto::TimestampValue> for TimestampValue {
    fn into_proto(&self) -> proto::TimestampValue {
        proto::TimestampValue {
            ts: Some(self.ts.into_proto()),
        }
    }

    fn from_proto(proto: proto::TimestampValue) -> Result<Self, TryFromProtoError> {
        Ok(TimestampValue {
            ts: proto.ts.into_rust_if_some("TimestampValue::ts")?,
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
