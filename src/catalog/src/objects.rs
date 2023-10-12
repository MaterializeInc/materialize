// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_audit_log::{VersionedEvent, VersionedStorageUsage};
use mz_controller::clusters::ReplicaLogging;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_ore::cast::CastFrom;
use mz_proto::{IntoRustIfSome, ProtoType};
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem};
use mz_repr::role_id::RoleId;
use mz_repr::GlobalId;
use mz_sql::catalog::{
    CatalogItemType, DefaultPrivilegeAclItem, DefaultPrivilegeObject, ObjectType, RoleAttributes,
    RoleMembership, RoleVars,
};
use mz_sql::names::{CommentObjectId, DatabaseId, SchemaId};
use mz_stash_types::objects::{proto, RustType, TryFromProtoError};
use mz_storage_types::sources::Timeline;
use proptest_derive::Arbitrary;
use std::collections::BTreeMap;
use std::time::Duration;

// Structs used to pass information to outside modules.

/// A trait for representing `Self` as a key-value pair of type
/// `(K, V)` for the purpose of storing this value durably.
///
/// To encode a key-value pair, use [`DurableType::into_key_value`].
///
/// To decode a key-value pair, use [`DurableType::from_key_value`].
///
/// This trait is based on [`RustType`], however it is meant to
/// convert the types used in [`RustType`] to a more consumable and
/// condensed type.
pub(crate) trait DurableType<K, V>: Sized {
    /// Consume and convert `Self` into a `(K, V)` key-value pair.
    fn into_key_value(self) -> (K, V);

    /// Consume and convert a `(K, V)` key-value pair back into a
    /// `Self` value.
    fn from_key_value(key: K, value: V) -> Self;
}

#[derive(Debug, Clone)]
pub struct Database {
    pub id: DatabaseId,
    pub name: String,
    pub owner_id: RoleId,
    pub privileges: Vec<MzAclItem>,
}

impl DurableType<DatabaseKey, DatabaseValue> for Database {
    fn into_key_value(self) -> (DatabaseKey, DatabaseValue) {
        (
            DatabaseKey { id: self.id },
            DatabaseValue {
                name: self.name,
                owner_id: self.owner_id,
                privileges: self.privileges,
            },
        )
    }

    fn from_key_value(key: DatabaseKey, value: DatabaseValue) -> Self {
        Self {
            id: key.id,
            name: value.name,
            owner_id: value.owner_id,
            privileges: value.privileges,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Schema {
    pub id: SchemaId,
    pub name: String,
    pub database_id: Option<DatabaseId>,
    pub owner_id: RoleId,
    pub privileges: Vec<MzAclItem>,
}

impl DurableType<SchemaKey, SchemaValue> for Schema {
    fn into_key_value(self) -> (SchemaKey, SchemaValue) {
        (
            SchemaKey { id: self.id },
            SchemaValue {
                database_id: self.database_id,
                name: self.name,
                owner_id: self.owner_id,
                privileges: self.privileges,
            },
        )
    }

    fn from_key_value(key: SchemaKey, value: SchemaValue) -> Self {
        Self {
            id: key.id,
            name: value.name,
            database_id: value.database_id,
            owner_id: value.owner_id,
            privileges: value.privileges,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Role {
    pub id: RoleId,
    pub name: String,
    pub attributes: RoleAttributes,
    pub membership: RoleMembership,
    pub vars: RoleVars,
}

impl DurableType<RoleKey, RoleValue> for Role {
    fn into_key_value(self) -> (RoleKey, RoleValue) {
        (
            RoleKey { id: self.id },
            RoleValue {
                name: self.name,
                attributes: self.attributes,
                membership: self.membership,
                vars: self.vars,
            },
        )
    }

    fn from_key_value(key: RoleKey, value: RoleValue) -> Self {
        Self {
            id: key.id,
            name: value.name,
            attributes: value.attributes,
            membership: value.membership,
            vars: value.vars,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Cluster {
    pub id: ClusterId,
    pub name: String,
    pub linked_object_id: Option<GlobalId>,
    pub owner_id: RoleId,
    pub privileges: Vec<MzAclItem>,
    pub config: ClusterConfig,
}

impl DurableType<ClusterKey, ClusterValue> for Cluster {
    fn into_key_value(self) -> (ClusterKey, ClusterValue) {
        (
            ClusterKey { id: self.id },
            ClusterValue {
                name: self.name,
                linked_object_id: self.linked_object_id,
                owner_id: self.owner_id,
                privileges: self.privileges,
                config: self.config,
            },
        )
    }

    fn from_key_value(key: ClusterKey, value: ClusterValue) -> Self {
        Self {
            id: key.id,
            name: value.name,
            linked_object_id: value.linked_object_id,
            owner_id: value.owner_id,
            privileges: value.privileges,
            config: value.config,
        }
    }
}

#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Ord)]
pub struct ClusterConfig {
    pub variant: ClusterVariant,
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

#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Ord)]
pub enum ClusterVariant {
    Managed(ClusterVariantManaged),
    Unmanaged,
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
            }) => proto::cluster_config::Variant::Managed(proto::cluster_config::ManagedCluster {
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

#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Ord)]
pub struct ClusterVariantManaged {
    pub size: String,
    pub availability_zones: Vec<String>,
    pub logging: ReplicaLogging,
    pub idle_arrangement_merge_effort: Option<u32>,
    pub replication_factor: u32,
    pub disk: bool,
}

#[derive(Clone, Debug)]
pub struct IntrospectionSourceIndex {
    pub cluster_id: ClusterId,
    pub name: String,
    pub index_id: GlobalId,
}

#[derive(Debug, Clone)]
pub struct ClusterReplica {
    pub cluster_id: ClusterId,
    pub replica_id: ReplicaId,
    pub name: String,
    pub config: ReplicaConfig,
    pub owner_id: RoleId,
}

impl DurableType<ClusterReplicaKey, ClusterReplicaValue> for ClusterReplica {
    fn into_key_value(self) -> (ClusterReplicaKey, ClusterReplicaValue) {
        (
            ClusterReplicaKey {
                id: self.replica_id,
            },
            ClusterReplicaValue {
                cluster_id: self.cluster_id,
                name: self.name,
                config: self.config,
                owner_id: self.owner_id,
            },
        )
    }

    fn from_key_value(key: ClusterReplicaKey, value: ClusterReplicaValue) -> Self {
        Self {
            cluster_id: value.cluster_id,
            replica_id: key.id,
            name: value.name,
            config: value.config,
            owner_id: value.owner_id,
        }
    }
}

// The on-disk replica configuration does not match the in-memory replica configuration, so we need
// separate structs. As of writing this comment, it is mainly due to the fact that we don't persist
// the replica allocation.
#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Ord)]
pub struct ReplicaConfig {
    pub location: ReplicaLocation,
    pub logging: ReplicaLogging,
    pub idle_arrangement_merge_effort: Option<u32>,
}

impl From<mz_controller::clusters::ReplicaConfig> for ReplicaConfig {
    fn from(config: mz_controller::clusters::ReplicaConfig) -> Self {
        Self {
            location: config.location.into(),
            logging: config.compute.logging,
            idle_arrangement_merge_effort: config.compute.idle_arrangement_merge_effort,
        }
    }
}

impl RustType<proto::ReplicaConfig> for ReplicaConfig {
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
        Ok(ReplicaConfig {
            location: proto
                .location
                .into_rust_if_some("ReplicaConfig::location")?,
            logging: proto.logging.into_rust_if_some("ReplicaConfig::logging")?,
            idle_arrangement_merge_effort: proto.idle_arrangement_merge_effort.map(|e| e.effort),
        })
    }
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord)]
pub enum ReplicaLocation {
    Unmanaged {
        storagectl_addrs: Vec<String>,
        storage_addrs: Vec<String>,
        computectl_addrs: Vec<String>,
        compute_addrs: Vec<String>,
        workers: usize,
    },
    Managed {
        size: String,
        /// `Some(az)` if the AZ was specified by the user and must be respected;
        availability_zone: Option<String>,
        disk: bool,
        internal: bool,
        billed_as: Option<String>,
    },
}

impl From<mz_controller::clusters::ReplicaLocation> for ReplicaLocation {
    fn from(loc: mz_controller::clusters::ReplicaLocation) -> Self {
        match loc {
            mz_controller::clusters::ReplicaLocation::Unmanaged(
                mz_controller::clusters::UnmanagedReplicaLocation {
                    storagectl_addrs,
                    storage_addrs,
                    computectl_addrs,
                    compute_addrs,
                    workers,
                },
            ) => Self::Unmanaged {
                storagectl_addrs,
                storage_addrs,
                computectl_addrs,
                compute_addrs,
                workers,
            },
            mz_controller::clusters::ReplicaLocation::Managed(
                mz_controller::clusters::ManagedReplicaLocation {
                    allocation: _,
                    size,
                    availability_zones,
                    disk,
                    billed_as,
                    internal,
                },
            ) => ReplicaLocation::Managed {
                size,
                availability_zone:
                    if let mz_controller::clusters::ManagedReplicaAvailabilityZones::FromReplica(
                        Some(az),
                    ) = availability_zones
                    {
                        Some(az)
                    } else {
                        None
                    },
                disk,
                internal,
                billed_as,
            },
        }
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

#[derive(Clone, Debug)]
pub struct ComputeReplicaLogging {
    pub log_logging: bool,
    pub interval: Option<Duration>,
}

#[derive(Debug, Clone)]
pub struct Item {
    pub id: GlobalId,
    pub schema_id: SchemaId,
    pub name: String,
    pub create_sql: String,
    pub owner_id: RoleId,
    pub privileges: Vec<MzAclItem>,
}

impl DurableType<ItemKey, ItemValue> for Item {
    fn into_key_value(self) -> (ItemKey, ItemValue) {
        (
            ItemKey { gid: self.id },
            ItemValue {
                schema_id: self.schema_id,
                name: self.name,
                create_sql: self.create_sql,
                owner_id: self.owner_id,
                privileges: self.privileges,
            },
        )
    }

    fn from_key_value(key: ItemKey, value: ItemValue) -> Self {
        Self {
            id: key.gid,
            schema_id: value.schema_id,
            name: value.name,
            create_sql: value.create_sql,
            owner_id: value.owner_id,
            privileges: value.privileges,
        }
    }
}

#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq)]
pub struct SystemObjectDescription {
    pub schema_name: String,
    pub object_type: CatalogItemType,
    pub object_name: String,
}

#[derive(Debug, Clone)]
pub struct SystemObjectUniqueIdentifier {
    pub id: GlobalId,
    pub fingerprint: String,
}

/// Functions can share the same name as any other catalog item type
/// within a given schema.
/// For example, a function can have the same name as a type, e.g.
/// 'date'.
/// As such, system objects are keyed in the catalog storage by the
/// tuple (schema_name, object_type, object_name), which is guaranteed
/// to be unique.
#[derive(Debug, Clone)]
pub struct SystemObjectMapping {
    pub description: SystemObjectDescription,
    pub unique_identifier: SystemObjectUniqueIdentifier,
}

impl DurableType<GidMappingKey, GidMappingValue> for SystemObjectMapping {
    fn into_key_value(self) -> (GidMappingKey, GidMappingValue) {
        (
            GidMappingKey {
                schema_name: self.description.schema_name,
                object_type: self.description.object_type,
                object_name: self.description.object_name,
            },
            GidMappingValue {
                id: match self.unique_identifier.id {
                    GlobalId::System(id) => id,
                    GlobalId::User(_) => unreachable!("GID mapping cannot use a User ID"),
                    GlobalId::Transient(_) => unreachable!("GID mapping cannot use a Transient ID"),
                    GlobalId::Explain => unreachable!("GID mapping cannot use an Explain ID"),
                },
                fingerprint: self.unique_identifier.fingerprint,
            },
        )
    }

    fn from_key_value(key: GidMappingKey, value: GidMappingValue) -> Self {
        Self {
            description: SystemObjectDescription {
                schema_name: key.schema_name,
                object_type: key.object_type,
                object_name: key.object_name,
            },
            unique_identifier: SystemObjectUniqueIdentifier {
                id: GlobalId::System(value.id),
                fingerprint: value.fingerprint,
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct DefaultPrivilege {
    pub object: DefaultPrivilegeObject,
    pub acl_item: DefaultPrivilegeAclItem,
}

impl DurableType<DefaultPrivilegesKey, DefaultPrivilegesValue> for DefaultPrivilege {
    fn into_key_value(self) -> (DefaultPrivilegesKey, DefaultPrivilegesValue) {
        (
            DefaultPrivilegesKey {
                role_id: self.object.role_id,
                database_id: self.object.database_id,
                schema_id: self.object.schema_id,
                object_type: self.object.object_type,
                grantee: self.acl_item.grantee,
            },
            DefaultPrivilegesValue {
                privileges: self.acl_item.acl_mode,
            },
        )
    }

    fn from_key_value(key: DefaultPrivilegesKey, value: DefaultPrivilegesValue) -> Self {
        Self {
            object: DefaultPrivilegeObject {
                role_id: key.role_id,
                database_id: key.database_id,
                schema_id: key.schema_id,
                object_type: key.object_type,
            },
            acl_item: DefaultPrivilegeAclItem {
                grantee: key.grantee,
                acl_mode: value.privileges,
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct Comment {
    pub object_id: CommentObjectId,
    pub sub_component: Option<usize>,
    pub comment: String,
}

impl DurableType<CommentKey, CommentValue> for Comment {
    fn into_key_value(self) -> (CommentKey, CommentValue) {
        (
            CommentKey {
                object_id: self.object_id,
                sub_component: self.sub_component,
            },
            CommentValue {
                comment: self.comment,
            },
        )
    }

    fn from_key_value(key: CommentKey, value: CommentValue) -> Self {
        Self {
            object_id: key.object_id,
            sub_component: key.sub_component,
            comment: value.comment,
        }
    }
}

#[derive(Debug, Clone)]
pub struct IdAlloc {
    pub name: String,
    pub next_id: u64,
}

impl DurableType<IdAllocKey, IdAllocValue> for IdAlloc {
    fn into_key_value(self) -> (IdAllocKey, IdAllocValue) {
        (
            IdAllocKey { name: self.name },
            IdAllocValue {
                next_id: self.next_id,
            },
        )
    }

    fn from_key_value(key: IdAllocKey, value: IdAllocValue) -> Self {
        Self {
            name: key.name,
            next_id: value.next_id,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub key: String,
    pub value: u64,
}

impl DurableType<ConfigKey, ConfigValue> for Config {
    fn into_key_value(self) -> (ConfigKey, ConfigValue) {
        (
            ConfigKey { key: self.key },
            ConfigValue { value: self.value },
        )
    }

    fn from_key_value(key: ConfigKey, value: ConfigValue) -> Self {
        Self {
            key: key.key,
            value: value.value,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Setting {
    pub name: String,
    pub value: String,
}

impl DurableType<SettingKey, SettingValue> for Setting {
    fn into_key_value(self) -> (SettingKey, SettingValue) {
        (
            SettingKey { name: self.name },
            SettingValue { value: self.value },
        )
    }

    fn from_key_value(key: SettingKey, value: SettingValue) -> Self {
        Self {
            name: key.name,
            value: value.value,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TimelineTimestamp {
    pub timeline: Timeline,
    pub ts: mz_repr::Timestamp,
}

impl DurableType<TimestampKey, TimestampValue> for TimelineTimestamp {
    fn into_key_value(self) -> (TimestampKey, TimestampValue) {
        (
            TimestampKey {
                id: self.timeline.to_string(),
            },
            TimestampValue { ts: self.ts },
        )
    }

    fn from_key_value(key: TimestampKey, value: TimestampValue) -> Self {
        Self {
            timeline: key.id.parse().expect("invalid timeline persisted"),
            ts: value.ts,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SystemConfiguration {
    pub name: String,
    pub value: String,
}

impl DurableType<ServerConfigurationKey, ServerConfigurationValue> for SystemConfiguration {
    fn into_key_value(self) -> (ServerConfigurationKey, ServerConfigurationValue) {
        (
            ServerConfigurationKey { name: self.name },
            ServerConfigurationValue { value: self.value },
        )
    }

    fn from_key_value(key: ServerConfigurationKey, value: ServerConfigurationValue) -> Self {
        Self {
            name: key.name,
            value: value.value,
        }
    }
}

impl DurableType<SystemPrivilegesKey, SystemPrivilegesValue> for MzAclItem {
    fn into_key_value(self) -> (SystemPrivilegesKey, SystemPrivilegesValue) {
        (
            SystemPrivilegesKey {
                grantee: self.grantee,
                grantor: self.grantor,
            },
            SystemPrivilegesValue {
                acl_mode: self.acl_mode,
            },
        )
    }

    fn from_key_value(key: SystemPrivilegesKey, value: SystemPrivilegesValue) -> Self {
        Self {
            grantee: key.grantee,
            grantor: key.grantor,
            acl_mode: value.acl_mode,
        }
    }
}

// Structs used internally to represent on-disk state.

/// A snapshot of the current on-disk state.
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub databases: BTreeMap<proto::DatabaseKey, proto::DatabaseValue>,
    pub schemas: BTreeMap<proto::SchemaKey, proto::SchemaValue>,
    pub roles: BTreeMap<proto::RoleKey, proto::RoleValue>,
    pub items: BTreeMap<proto::ItemKey, proto::ItemValue>,
    pub comments: BTreeMap<proto::CommentKey, proto::CommentValue>,
    pub clusters: BTreeMap<proto::ClusterKey, proto::ClusterValue>,
    pub cluster_replicas: BTreeMap<proto::ClusterReplicaKey, proto::ClusterReplicaValue>,
    pub introspection_sources: BTreeMap<
        proto::ClusterIntrospectionSourceIndexKey,
        proto::ClusterIntrospectionSourceIndexValue,
    >,
    pub id_allocator: BTreeMap<proto::IdAllocKey, proto::IdAllocValue>,
    pub configs: BTreeMap<proto::ConfigKey, proto::ConfigValue>,
    pub settings: BTreeMap<proto::SettingKey, proto::SettingValue>,
    pub timestamps: BTreeMap<proto::TimestampKey, proto::TimestampValue>,
    pub system_object_mappings: BTreeMap<proto::GidMappingKey, proto::GidMappingValue>,
    pub system_configurations:
        BTreeMap<proto::ServerConfigurationKey, proto::ServerConfigurationValue>,
    pub default_privileges: BTreeMap<proto::DefaultPrivilegesKey, proto::DefaultPrivilegesValue>,
    pub system_privileges: BTreeMap<proto::SystemPrivilegesKey, proto::SystemPrivilegesValue>,
}

impl Snapshot {
    pub fn empty() -> Snapshot {
        Snapshot {
            databases: BTreeMap::new(),
            schemas: BTreeMap::new(),
            roles: BTreeMap::new(),
            items: BTreeMap::new(),
            comments: BTreeMap::new(),
            clusters: BTreeMap::new(),
            cluster_replicas: BTreeMap::new(),
            introspection_sources: BTreeMap::new(),
            id_allocator: BTreeMap::new(),
            configs: BTreeMap::new(),
            settings: BTreeMap::new(),
            timestamps: BTreeMap::new(),
            system_object_mappings: BTreeMap::new(),
            system_configurations: BTreeMap::new(),
            default_privileges: BTreeMap::new(),
            system_privileges: BTreeMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.databases.is_empty()
            && self.schemas.is_empty()
            && self.roles.is_empty()
            && self.items.is_empty()
            && self.comments.is_empty()
            && self.clusters.is_empty()
            && self.cluster_replicas.is_empty()
            && self.introspection_sources.is_empty()
            && self.id_allocator.is_empty()
            && self.configs.is_empty()
            && self.settings.is_empty()
            && self.timestamps.is_empty()
            && self.system_object_mappings.is_empty()
            && self.system_configurations.is_empty()
            && self.default_privileges.is_empty()
            && self.system_privileges.is_empty()
    }
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct SettingKey {
    pub(crate) name: String,
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

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct SettingValue {
    pub(crate) value: String,
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

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct IdAllocKey {
    pub(crate) name: String,
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

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct IdAllocValue {
    pub(crate) next_id: u64,
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

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct GidMappingKey {
    pub(crate) schema_name: String,
    pub(crate) object_type: CatalogItemType,
    pub(crate) object_name: String,
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

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct GidMappingValue {
    pub(crate) id: u64,
    pub(crate) fingerprint: String,
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

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct ClusterKey {
    pub(crate) id: ClusterId,
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

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct ClusterValue {
    pub(crate) name: String,
    pub(crate) linked_object_id: Option<GlobalId>,
    pub(crate) owner_id: RoleId,
    pub(crate) privileges: Vec<MzAclItem>,
    pub(crate) config: ClusterConfig,
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

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct ClusterIntrospectionSourceIndexKey {
    pub(crate) cluster_id: ClusterId,
    pub(crate) name: String,
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

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct ClusterIntrospectionSourceIndexValue {
    pub(crate) index_id: u64,
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

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct ClusterReplicaKey {
    pub(crate) id: ReplicaId,
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

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct ClusterReplicaValue {
    pub(crate) cluster_id: ClusterId,
    pub(crate) name: String,
    pub(crate) config: ReplicaConfig,
    pub(crate) owner_id: RoleId,
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

#[derive(Clone, Copy, Debug, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub struct DatabaseKey {
    pub(crate) id: DatabaseId,
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

#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct DatabaseValue {
    pub(crate) name: String,
    pub(crate) owner_id: RoleId,
    pub(crate) privileges: Vec<MzAclItem>,
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

#[derive(Clone, Copy, Debug, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub struct SchemaKey {
    pub(crate) id: SchemaId,
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

#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct SchemaValue {
    pub(crate) database_id: Option<DatabaseId>,
    pub(crate) name: String,
    pub(crate) owner_id: RoleId,
    pub(crate) privileges: Vec<MzAclItem>,
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

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash, Debug, Arbitrary)]
pub struct ItemKey {
    pub(crate) gid: GlobalId,
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

#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct ItemValue {
    pub(crate) schema_id: SchemaId,
    pub(crate) name: String,
    pub(crate) create_sql: String,
    pub(crate) owner_id: RoleId,
    pub(crate) privileges: Vec<MzAclItem>,
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
        })
    }
}

#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Ord)]
pub struct CommentKey {
    pub(crate) object_id: CommentObjectId,
    pub(crate) sub_component: Option<usize>,
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

#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct CommentValue {
    pub(crate) comment: String,
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

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash, Debug)]
pub struct RoleKey {
    pub(crate) id: RoleId,
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

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Debug)]
pub struct RoleValue {
    pub(crate) name: String,
    pub(crate) attributes: RoleAttributes,
    pub(crate) membership: RoleMembership,
    pub(crate) vars: RoleVars,
}

impl RustType<proto::RoleValue> for RoleValue {
    fn into_proto(&self) -> proto::RoleValue {
        proto::RoleValue {
            name: self.name.to_string(),
            attributes: Some(self.attributes.into_proto()),
            membership: Some(self.membership.into_proto()),
            vars: Some(self.vars.into_proto()),
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
        })
    }
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct ConfigKey {
    pub(crate) key: String,
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

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct ConfigValue {
    pub(crate) value: u64,
}

impl RustType<proto::ConfigValue> for ConfigValue {
    fn into_proto(&self) -> proto::ConfigValue {
        proto::ConfigValue { value: self.value }
    }

    fn from_proto(proto: proto::ConfigValue) -> Result<Self, TryFromProtoError> {
        Ok(ConfigValue { value: proto.value })
    }
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct AuditLogKey {
    pub(crate) event: VersionedEvent,
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

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct StorageUsageKey {
    pub(crate) metric: VersionedStorageUsage,
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

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct TimestampKey {
    pub(crate) id: String,
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

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct TimestampValue {
    pub(crate) ts: mz_repr::Timestamp,
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

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct ServerConfigurationKey {
    pub(crate) name: String,
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

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct ServerConfigurationValue {
    pub(crate) value: String,
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

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct DefaultPrivilegesKey {
    pub(crate) role_id: RoleId,
    pub(crate) database_id: Option<DatabaseId>,
    pub(crate) schema_id: Option<SchemaId>,
    pub(crate) object_type: ObjectType,
    pub(crate) grantee: RoleId,
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

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct DefaultPrivilegesValue {
    pub(crate) privileges: AclMode,
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

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct SystemPrivilegesKey {
    pub(crate) grantee: RoleId,
    pub(crate) grantor: RoleId,
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

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct SystemPrivilegesValue {
    pub(crate) acl_mode: AclMode,
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

#[cfg(test)]
mod test {
    use mz_proto::{ProtoType, RustType};
    use proptest::prelude::*;

    use super::{DatabaseKey, DatabaseValue, ItemKey, ItemValue, SchemaKey, SchemaValue};

    proptest! {
        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // slow
        fn proptest_database_key_roundtrip(key: DatabaseKey) {
            let proto = key.into_proto();
            let round = proto.into_rust().expect("to roundtrip");

            prop_assert_eq!(key, round);
        }

        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // slow
        fn proptest_database_value_roundtrip(value: DatabaseValue) {
            let proto = value.into_proto();
            let round = proto.into_rust().expect("to roundtrip");

            prop_assert_eq!(value, round);
        }

        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // slow
        fn proptest_schema_key_roundtrip(key: SchemaKey) {
            let proto = key.into_proto();
            let round = proto.into_rust().expect("to roundtrip");

            prop_assert_eq!(key, round);
        }

        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // slow
        fn proptest_schema_value_roundtrip(value: SchemaValue) {
            let proto = value.into_proto();
            let round = proto.into_rust().expect("to roundtrip");

            prop_assert_eq!(value, round);
        }

        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // slow
        fn proptest_item_key_roundtrip(key: ItemKey) {
            let proto = key.into_proto();
            let round = proto.into_rust().expect("to roundtrip");

            prop_assert_eq!(key, round);
        }

        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // slow
        fn proptest_item_value_roundtrip(value: ItemValue) {
            let proto = value.into_proto();
            let round = proto.into_rust().expect("to roundtrip");

            prop_assert_eq!(value, round);
        }
    }
}
