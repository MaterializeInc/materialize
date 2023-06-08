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

use crate::catalog::storage::{DefaultPrivilegesKey, DefaultPrivilegesValue};
use crate::catalog::{RoleMembership, SerializedCatalogItem, SerializedRole};

use super::{
    AuditLogKey, ClusterIntrospectionSourceIndexKey, ClusterIntrospectionSourceIndexValue,
    ClusterKey, ClusterReplicaKey, ClusterReplicaValue, ClusterValue, ConfigKey, ConfigValue,
    DatabaseKey, DatabaseNamespace, DatabaseValue, GidMappingKey, GidMappingValue, IdAllocKey,
    IdAllocValue, ItemKey, ItemValue, RoleKey, RoleValue, SchemaKey, SchemaNamespace, SchemaValue,
    SerializedReplicaConfig, SerializedReplicaLocation, SerializedReplicaLogging,
    ServerConfigurationKey, ServerConfigurationValue, SettingKey, SettingValue, StorageUsageKey,
    TimestampKey, TimestampValue,
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
            linked_object_id: self.linked_object_id.into_proto(),
            owner_id: Some(self.owner_id.into_proto()),
            privileges: self
                .privileges
                .as_ref()
                .cloned()
                .unwrap_or_default()
                .into_proto(),
        }
    }

    fn from_proto(proto: proto::ClusterValue) -> Result<Self, TryFromProtoError> {
        Ok(ClusterValue {
            name: proto.name,
            linked_object_id: proto.linked_object_id.into_rust()?,
            owner_id: proto.owner_id.into_rust_if_some("ClusterValue::owner_id")?,
            privileges: Some(proto.privileges.into_rust()?),
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

impl RustType<proto::ClusterReplicaKey> for ClusterReplicaKey {
    fn into_proto(&self) -> proto::ClusterReplicaKey {
        proto::ClusterReplicaKey {
            id: Some(proto::ReplicaId { value: self.id }),
        }
    }

    fn from_proto(proto: proto::ClusterReplicaKey) -> Result<Self, TryFromProtoError> {
        Ok(ClusterReplicaKey {
            id: proto
                .id
                .map(|id| id.value)
                .ok_or_else(|| TryFromProtoError::missing_field("ClusterReplicaKey::id"))?,
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

impl RustType<proto::ReplicaConfig> for SerializedReplicaConfig {
    fn into_proto(&self) -> proto::ReplicaConfig {
        proto::ReplicaConfig {
            logging: Some(self.logging.into_proto()),
            location: Some(self.location.into_proto()),
            idle_arrangement_merge_effort: self
                .idle_arrangement_merge_effort
                .map(|effort| proto::replica_config::MergeEffort { effort }),
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

impl RustType<proto::replica_config::Logging> for SerializedReplicaLogging {
    fn into_proto(&self) -> proto::replica_config::Logging {
        proto::replica_config::Logging {
            log_logging: self.log_logging,
            interval: self.interval.into_proto(),
        }
    }

    fn from_proto(proto: proto::replica_config::Logging) -> Result<Self, TryFromProtoError> {
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
                az_user_specified,
            } => proto::replica_config::Location::Managed(proto::replica_config::ManagedLocation {
                size: size.to_string(),
                availability_zone: availability_zone.to_string(),
                az_user_specified: *az_user_specified,
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
                    az_user_specified: location.az_user_specified,
                })
            }
        }
    }
}

impl RustType<proto::DatabaseKey> for DatabaseKey {
    fn into_proto(&self) -> proto::DatabaseKey {
        let value = match self.ns {
            None | Some(DatabaseNamespace::User) => proto::database_id::Value::User(self.id),
            Some(DatabaseNamespace::System) => proto::database_id::Value::System(self.id),
        };

        proto::DatabaseKey {
            id: Some(proto::DatabaseId { value: Some(value) }),
        }
    }

    fn from_proto(proto: proto::DatabaseKey) -> Result<Self, TryFromProtoError> {
        let id = proto
            .id
            .ok_or_else(|| TryFromProtoError::missing_field("DatabaseKey::id"))?;
        match id.value {
            Some(proto::database_id::Value::User(id)) => Ok(DatabaseKey {
                id,
                ns: Some(DatabaseNamespace::User),
            }),
            Some(proto::database_id::Value::System(id)) => Ok(DatabaseKey {
                id,
                ns: Some(DatabaseNamespace::System),
            }),
            None => Err(TryFromProtoError::missing_field("DatabaseId::value")),
        }
    }
}

impl RustType<proto::DatabaseValue> for DatabaseValue {
    fn into_proto(&self) -> proto::DatabaseValue {
        proto::DatabaseValue {
            name: self.name.clone(),
            owner_id: Some(self.owner_id.into_proto()),
            privileges: self
                .privileges
                .as_ref()
                .cloned()
                .unwrap_or_default()
                .into_proto(),
        }
    }

    fn from_proto(proto: proto::DatabaseValue) -> Result<Self, TryFromProtoError> {
        Ok(DatabaseValue {
            name: proto.name,
            owner_id: (proto
                .owner_id
                .into_rust_if_some("DatabaseValue::owner_id")?),
            privileges: Some(proto.privileges.into_rust()?),
        })
    }
}

impl RustType<proto::SchemaKey> for SchemaKey {
    fn into_proto(&self) -> proto::SchemaKey {
        let value = match self.ns {
            None | Some(SchemaNamespace::User) => proto::schema_id::Value::User(self.id),
            Some(SchemaNamespace::System) => proto::schema_id::Value::System(self.id),
        };

        proto::SchemaKey {
            id: Some(proto::SchemaId { value: Some(value) }),
        }
    }

    fn from_proto(proto: proto::SchemaKey) -> Result<Self, TryFromProtoError> {
        let id = proto
            .id
            .ok_or_else(|| TryFromProtoError::missing_field("SchemaKey::id"))?;
        match id.value {
            Some(proto::schema_id::Value::User(id)) => Ok(SchemaKey {
                id,
                ns: Some(SchemaNamespace::User),
            }),
            Some(proto::schema_id::Value::System(id)) => Ok(SchemaKey {
                id,
                ns: Some(SchemaNamespace::System),
            }),
            None => Err(TryFromProtoError::missing_field("SchemaKey::value")),
        }
    }
}

impl RustType<proto::SchemaValue> for SchemaValue {
    fn into_proto(&self) -> proto::SchemaValue {
        let database_id = match (self.database_id, self.database_ns) {
            (Some(id), Some(DatabaseNamespace::User)) | (Some(id), None) => {
                Some(proto::DatabaseId {
                    value: Some(proto::database_id::Value::User(id)),
                })
            }
            (Some(id), Some(DatabaseNamespace::System)) => Some(proto::DatabaseId {
                value: Some(proto::database_id::Value::System(id)),
            }),
            // Note: it's valid for a schema to not be associated with a database.
            (None, _) => None,
        };

        proto::SchemaValue {
            name: self.name.clone(),
            database_id,
            owner_id: Some(self.owner_id.into_proto()),
            privileges: self
                .privileges
                .as_ref()
                .cloned()
                .unwrap_or_default()
                .into_proto(),
        }
    }

    fn from_proto(proto: proto::SchemaValue) -> Result<Self, TryFromProtoError> {
        let (database_id, database_ns) = match proto.database_id {
            // Note: it's valid for a schema to not be associated with a database.
            None => (None, None),
            Some(proto::DatabaseId { value }) => {
                let value =
                    value.ok_or_else(|| TryFromProtoError::missing_field("DatabaseId::value"))?;
                match value {
                    proto::database_id::Value::User(id) => {
                        (Some(id), Some(DatabaseNamespace::User))
                    }
                    proto::database_id::Value::System(id) => {
                        (Some(id), Some(DatabaseNamespace::System))
                    }
                }
            }
        };

        Ok(SchemaValue {
            name: proto.name,
            database_id,
            database_ns,
            owner_id: (proto
                .owner_id
                .into_rust_if_some("DatabaseValue::owner_id")?),
            privileges: Some(proto.privileges.into_rust()?),
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
        let schema_id = match self.schema_ns {
            None | Some(SchemaNamespace::User) => proto::SchemaId {
                value: Some(proto::schema_id::Value::User(self.schema_id)),
            },
            Some(SchemaNamespace::System) => proto::SchemaId {
                value: Some(proto::schema_id::Value::System(self.schema_id)),
            },
        };

        proto::ItemValue {
            schema_id: Some(schema_id),
            name: self.name.to_string(),
            definition: Some(self.definition.into_proto()),
            owner_id: Some(self.owner_id.into_proto()),
            privileges: self
                .privileges
                .as_ref()
                .cloned()
                .unwrap_or_default()
                .into_proto(),
        }
    }

    fn from_proto(proto: proto::ItemValue) -> Result<Self, TryFromProtoError> {
        let schema_id = proto
            .schema_id
            .ok_or_else(|| TryFromProtoError::missing_field("ItemValue::schema_id"))?;
        let (schema_id, schema_ns) = match schema_id.value {
            Some(proto::schema_id::Value::User(id)) => (id, SchemaNamespace::User),
            Some(proto::schema_id::Value::System(id)) => (id, SchemaNamespace::System),
            None => return Err(TryFromProtoError::missing_field("SchemaId::value")),
        };

        Ok(ItemValue {
            schema_id,
            schema_ns: Some(schema_ns),
            name: proto.name,
            definition: proto
                .definition
                .into_rust_if_some("ItemValue::definition")?,
            owner_id: proto.owner_id.into_rust_if_some("ItemValue::owner_id")?,
            privileges: Some(proto.privileges.into_rust()?),
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
            name: self.role.name.to_string(),
            attributes: self.role.attributes.into_proto(),
            membership: self.role.membership.into_proto(),
        }
    }

    fn from_proto(proto: proto::RoleValue) -> Result<Self, TryFromProtoError> {
        Ok(RoleValue {
            role: SerializedRole {
                name: proto.name,
                attributes: proto.attributes.into_rust()?,
                membership: proto.membership.into_rust()?,
            },
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
            database_spec: self
                .database_spec
                .map(|database_spec| database_spec.into_proto()),
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
            database_spec: proto.database_spec.into_rust()?,
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
