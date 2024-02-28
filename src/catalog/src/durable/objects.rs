// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod serialization;

use std::collections::BTreeMap;

use mz_audit_log::{VersionedEvent, VersionedStorageUsage};
use mz_controller::clusters::ReplicaLogging;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem};
use mz_repr::role_id::RoleId;
use mz_repr::GlobalId;
use mz_sql::catalog::{
    CatalogItemType, DefaultPrivilegeAclItem, DefaultPrivilegeObject, ObjectType, RoleAttributes,
    RoleMembership, RoleVars,
};
use mz_sql::names::{CommentObjectId, DatabaseId, SchemaId};
use mz_storage_types::sources::Timeline;
use proptest_derive::Arbitrary;

use crate::durable::objects::serialization::proto;

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
///
/// [`RustType`]: mz_proto::RustType
pub trait DurableType<K, V>: Sized {
    /// Consume and convert `Self` into a `(K, V)` key-value pair.
    fn into_key_value(self) -> (K, V);

    /// Consume and convert a `(K, V)` key-value pair back into a
    /// `Self` value.
    fn from_key_value(key: K, value: V) -> Self;
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cluster {
    pub id: ClusterId,
    pub name: String,
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

#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Ord)]
pub enum ClusterVariant {
    Managed(ClusterVariantManaged),
    Unmanaged,
}

#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Ord)]
pub struct ClusterVariantManaged {
    pub size: String,
    pub availability_zones: Vec<String>,
    pub logging: ReplicaLogging,
    pub idle_arrangement_merge_effort: Option<u32>,
    pub replication_factor: u32,
    pub disk: bool,
    pub optimizer_feature_overrides: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IntrospectionSourceIndex {
    pub cluster_id: ClusterId,
    pub name: String,
    pub index_id: GlobalId,
}

impl DurableType<ClusterIntrospectionSourceIndexKey, ClusterIntrospectionSourceIndexValue>
    for IntrospectionSourceIndex
{
    fn into_key_value(
        self,
    ) -> (
        ClusterIntrospectionSourceIndexKey,
        ClusterIntrospectionSourceIndexValue,
    ) {
        let index_id = match self.index_id {
            GlobalId::System(id) => id,
            GlobalId::User(_) => {
                unreachable!("cluster introspection source index mapping cannot use a User ID")
            }
            GlobalId::Transient(_) => {
                unreachable!("cluster introspection source index mapping cannot use a Transient ID")
            }
            GlobalId::Explain => {
                unreachable!("cluster introspection source index mapping cannot use an Explain ID")
            }
        };
        (
            ClusterIntrospectionSourceIndexKey {
                cluster_id: self.cluster_id,
                name: self.name,
            },
            ClusterIntrospectionSourceIndexValue { index_id },
        )
    }

    fn from_key_value(
        key: ClusterIntrospectionSourceIndexKey,
        value: ClusterIntrospectionSourceIndexValue,
    ) -> Self {
        Self {
            cluster_id: key.cluster_id,
            name: key.name,
            index_id: GlobalId::System(value.index_id),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
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
#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
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
#[derive(Debug, Clone, PartialEq, Eq)]
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
        let Snapshot {
            databases,
            schemas,
            roles,
            items,
            comments,
            clusters,
            cluster_replicas,
            introspection_sources,
            id_allocator,
            configs,
            settings,
            timestamps,
            system_object_mappings,
            system_configurations,
            default_privileges,
            system_privileges,
        } = self;
        databases.is_empty()
            && schemas.is_empty()
            && roles.is_empty()
            && items.is_empty()
            && comments.is_empty()
            && clusters.is_empty()
            && cluster_replicas.is_empty()
            && introspection_sources.is_empty()
            && id_allocator.is_empty()
            && configs.is_empty()
            && settings.is_empty()
            && timestamps.is_empty()
            && system_object_mappings.is_empty()
            && system_configurations.is_empty()
            && default_privileges.is_empty()
            && system_privileges.is_empty()
    }
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct SettingKey {
    pub(crate) name: String,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct SettingValue {
    pub(crate) value: String,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct IdAllocKey {
    pub(crate) name: String,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct IdAllocValue {
    pub(crate) next_id: u64,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct GidMappingKey {
    pub(crate) schema_name: String,
    pub(crate) object_type: CatalogItemType,
    pub(crate) object_name: String,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct GidMappingValue {
    pub(crate) id: u64,
    pub(crate) fingerprint: String,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct ClusterKey {
    pub(crate) id: ClusterId,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct ClusterValue {
    pub(crate) name: String,
    pub(crate) owner_id: RoleId,
    pub(crate) privileges: Vec<MzAclItem>,
    pub(crate) config: ClusterConfig,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct ClusterIntrospectionSourceIndexKey {
    pub(crate) cluster_id: ClusterId,
    pub(crate) name: String,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct ClusterIntrospectionSourceIndexValue {
    pub(crate) index_id: u64,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct ClusterReplicaKey {
    pub(crate) id: ReplicaId,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct ClusterReplicaValue {
    pub(crate) cluster_id: ClusterId,
    pub(crate) name: String,
    pub(crate) config: ReplicaConfig,
    pub(crate) owner_id: RoleId,
}

#[derive(Clone, Copy, Debug, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub struct DatabaseKey {
    pub(crate) id: DatabaseId,
}

#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct DatabaseValue {
    pub(crate) name: String,
    pub(crate) owner_id: RoleId,
    pub(crate) privileges: Vec<MzAclItem>,
}

#[derive(Clone, Copy, Debug, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub struct SchemaKey {
    pub(crate) id: SchemaId,
}

#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct SchemaValue {
    pub(crate) database_id: Option<DatabaseId>,
    pub(crate) name: String,
    pub(crate) owner_id: RoleId,
    pub(crate) privileges: Vec<MzAclItem>,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash, Debug, Arbitrary)]
pub struct ItemKey {
    pub(crate) gid: GlobalId,
}

#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct ItemValue {
    pub(crate) schema_id: SchemaId,
    pub(crate) name: String,
    pub(crate) create_sql: String,
    pub(crate) owner_id: RoleId,
    pub(crate) privileges: Vec<MzAclItem>,
}

impl ItemValue {
    pub(crate) fn item_type(&self) -> CatalogItemType {
        // NOTE(benesch): the implementation of this method is hideous, but is
        // there a better alternative? Storing the object type alongside the
        // `create_sql` would introduce the possibility of skew.
        let mut tokens = self.create_sql.split_whitespace();
        assert_eq!(tokens.next(), Some("CREATE"));
        match tokens.next() {
            Some("TABLE") => CatalogItemType::Table,
            Some("SOURCE") | Some("SUBSOURCE") => CatalogItemType::Source,
            Some("SINK") => CatalogItemType::Sink,
            Some("VIEW") => CatalogItemType::View,
            Some("MATERIALIZED") => {
                assert_eq!(tokens.next(), Some("VIEW"));
                CatalogItemType::MaterializedView
            }
            Some("INDEX") => CatalogItemType::Index,
            Some("TYPE") => CatalogItemType::Type,
            Some("FUNCTION") => CatalogItemType::Func,
            Some("SECRET") => CatalogItemType::Secret,
            Some("CONNECTION") => CatalogItemType::Connection,
            _ => panic!("unexpected create sql: {}", self.create_sql),
        }
    }
}

#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Ord)]
pub struct CommentKey {
    pub(crate) object_id: CommentObjectId,
    pub(crate) sub_component: Option<usize>,
}

#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct CommentValue {
    pub(crate) comment: String,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash, Debug)]
pub struct RoleKey {
    pub(crate) id: RoleId,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Debug)]
pub struct RoleValue {
    pub(crate) name: String,
    pub(crate) attributes: RoleAttributes,
    pub(crate) membership: RoleMembership,
    pub(crate) vars: RoleVars,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct ConfigKey {
    pub(crate) key: String,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct ConfigValue {
    pub(crate) value: u64,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct AuditLogKey {
    pub(crate) event: VersionedEvent,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct StorageUsageKey {
    pub(crate) metric: VersionedStorageUsage,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct TimestampKey {
    pub(crate) id: String,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct TimestampValue {
    pub(crate) ts: mz_repr::Timestamp,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct ServerConfigurationKey {
    pub(crate) name: String,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct ServerConfigurationValue {
    pub(crate) value: String,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct DefaultPrivilegesKey {
    pub(crate) role_id: RoleId,
    pub(crate) database_id: Option<DatabaseId>,
    pub(crate) schema_id: Option<SchemaId>,
    pub(crate) object_type: ObjectType,
    pub(crate) grantee: RoleId,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct DefaultPrivilegesValue {
    pub(crate) privileges: AclMode,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct SystemPrivilegesKey {
    pub(crate) grantee: RoleId,
    pub(crate) grantor: RoleId,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct SystemPrivilegesValue {
    pub(crate) acl_mode: AclMode,
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
