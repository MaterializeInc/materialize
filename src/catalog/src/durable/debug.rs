// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Functionality for manually modifying and displaying the catalog contents. This is helpful for
//! fixing a corrupt catalog.

use crate::durable;
use crate::durable::persist::{PersistHandle, StateUpdateKind};
use crate::durable::{
    CatalogError, AUDIT_LOG_COLLECTION, CLUSTER_COLLECTION,
    CLUSTER_INTROSPECTION_SOURCE_INDEX_COLLECTION, CLUSTER_REPLICA_COLLECTION, COMMENTS_COLLECTION,
    CONFIG_COLLECTION, DATABASES_COLLECTION, DEFAULT_PRIVILEGES_COLLECTION,
    ID_ALLOCATOR_COLLECTION, ITEM_COLLECTION, ROLES_COLLECTION, SCHEMAS_COLLECTION,
    SETTING_COLLECTION, STORAGE_USAGE_COLLECTION, SYSTEM_CONFIGURATION_COLLECTION,
    SYSTEM_GID_MAPPING_COLLECTION, SYSTEM_PRIVILEGES_COLLECTION, TIMESTAMP_COLLECTION,
};
use mz_repr::Diff;
use mz_stash::{Stash, TypedCollection};
use mz_stash_types::objects::proto;
use std::str::FromStr;

/// The contents of the catalog are logically separated into separate [`Collection`]s, which
/// describe the category of data that the content belongs to.
pub trait Collection {
    /// Type used to stores keys for [`Collection`].
    type Key;
    /// Type used to stores values for [`Collection`].
    type Value;

    /// [`CollectionType`] corresponding to [`Collection`].
    fn collection_type() -> CollectionType;

    /// Name of [`Collection`].
    fn name() -> &'static str {
        Self::collection_type().name()
    }

    /// Extract the [`CollectionTrace`] from a [`Trace`] that corresponds to [`Collection`].
    fn collection_trace(trace: Trace) -> CollectionTrace<Self>;

    /// Return [`TypedCollection`] that corresponds to [`Collection`].
    fn stash_collection() -> TypedCollection<Self::Key, Self::Value>;

    /// Generate a `StateUpdateKind` with `key` and `value` that corresponds to [`Collection`].
    fn persist_update(key: Self::Key, value: Self::Value) -> StateUpdateKind;
}

/// The type of a [`Collection`].
///
/// See [`Collection`] for more details.
pub enum CollectionType {
    AuditLog,
    Cluster,
    ClusterIntrospectionSourceIndex,
    ClusterReplica,
    Comment,
    Config,
    Database,
    DefaultPrivilege,
    IdAllocator,
    Item,
    Role,
    Schema,
    Setting,
    StorageUsage,
    SystemConfiguration,
    SystemItemMapping,
    SystemPrivilege,
    Timestamp,
}

impl CollectionType {
    pub fn name(&self) -> &'static str {
        match self {
            CollectionType::AuditLog => "audit_log",
            CollectionType::Cluster => "compute_instance",
            CollectionType::ClusterIntrospectionSourceIndex => "compute_introspection_source_index",
            CollectionType::ClusterReplica => "compute_replicas",
            CollectionType::Comment => "comments",
            CollectionType::Config => "config",
            CollectionType::Database => "database",
            CollectionType::DefaultPrivilege => "default_privileges",
            CollectionType::IdAllocator => "id_alloc",
            CollectionType::Item => "item",
            CollectionType::Role => "role",
            CollectionType::Schema => "schema",
            CollectionType::Setting => "setting",
            CollectionType::StorageUsage => "storage_usage",
            CollectionType::SystemConfiguration => "system_configuration",
            CollectionType::SystemItemMapping => "system_gid_mapping",
            CollectionType::SystemPrivilege => "system_privileges",
            CollectionType::Timestamp => "timestamp",
        }
    }
}

impl FromStr for CollectionType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == CollectionType::AuditLog.name() {
            Ok(CollectionType::AuditLog)
        } else if s == CollectionType::Cluster.name() {
            Ok(CollectionType::Cluster)
        } else if s == CollectionType::ClusterIntrospectionSourceIndex.name() {
            Ok(CollectionType::ClusterIntrospectionSourceIndex)
        } else if s == CollectionType::ClusterReplica.name() {
            Ok(CollectionType::ClusterReplica)
        } else if s == CollectionType::Comment.name() {
            Ok(CollectionType::Comment)
        } else if s == CollectionType::Config.name() {
            Ok(CollectionType::Config)
        } else if s == CollectionType::Database.name() {
            Ok(CollectionType::Database)
        } else if s == CollectionType::DefaultPrivilege.name() {
            Ok(CollectionType::DefaultPrivilege)
        } else if s == CollectionType::IdAllocator.name() {
            Ok(CollectionType::IdAllocator)
        } else if s == CollectionType::Item.name() {
            Ok(CollectionType::Item)
        } else if s == CollectionType::Role.name() {
            Ok(CollectionType::Role)
        } else if s == CollectionType::Schema.name() {
            Ok(CollectionType::Schema)
        } else if s == CollectionType::Setting.name() {
            Ok(CollectionType::Setting)
        } else if s == CollectionType::StorageUsage.name() {
            Ok(CollectionType::StorageUsage)
        } else if s == CollectionType::SystemItemMapping.name() {
            Ok(CollectionType::SystemItemMapping)
        } else if s == CollectionType::SystemConfiguration.name() {
            Ok(CollectionType::SystemConfiguration)
        } else if s == CollectionType::Timestamp.name() {
            Ok(CollectionType::Timestamp)
        } else {
            anyhow::bail!("unknown collection {s}")
        }
    }
}

/// Macro to simplify implementing [`Collection`].
///
/// The arguments to `collection_impl!` are:
/// - `$name`, which will be the name of the implementing struct.
/// - `$key`, the type used to store keys.
/// - `$value`, the type used to store values.
/// - `$collection_type`, the [`CollectionType`].
/// - `$trace_field`, the corresponding field name within a [`Trace`].
/// - `$stash_collection`, the corresponding [`TypedCollection`].
/// - `$persist_update`, the corresponding [`StateUpdateKind`] constructor.
macro_rules! collection_impl {
    ({
    name: $name:ident,
    key: $key:ty,
    value: $value:ty,
    collection_type: $collection_type:expr,
    trace_field: $trace_field:ident,
    stash_collection: $stash_collection:expr,
    persist_update: $persist_update:expr,
}) => {
        #[derive(Debug, Clone, PartialEq, Eq)]
        pub struct $name {}

        impl Collection for $name {
            type Key = $key;
            type Value = $value;

            fn collection_type() -> CollectionType {
                $collection_type
            }

            fn collection_trace(trace: Trace) -> CollectionTrace<Self> {
                trace.$trace_field
            }

            fn stash_collection() -> TypedCollection<Self::Key, Self::Value> {
                $stash_collection
            }

            fn persist_update(key: Self::Key, value: Self::Value) -> StateUpdateKind {
                $persist_update(key, value)
            }
        }
    };
}

collection_impl!({
    name: AuditLogCollection,
    key: proto::AuditLogKey,
    value: (),
    collection_type: CollectionType::AuditLog,
    trace_field: audit_log,
    stash_collection: AUDIT_LOG_COLLECTION,
    persist_update: StateUpdateKind::AuditLog,
});
collection_impl!({
    name: ClusterCollection,
    key: proto::ClusterKey,
    value: proto::ClusterValue,
    collection_type: CollectionType::Cluster,
    trace_field: clusters,
    stash_collection: CLUSTER_COLLECTION,
    persist_update: StateUpdateKind::Cluster,
});
collection_impl!({
    name: ClusterIntrospectionSourceIndexCollection,
    key: proto::ClusterIntrospectionSourceIndexKey,
    value: proto::ClusterIntrospectionSourceIndexValue,
    collection_type: CollectionType::ClusterIntrospectionSourceIndex,
    trace_field: introspection_sources,
    stash_collection: CLUSTER_INTROSPECTION_SOURCE_INDEX_COLLECTION,
    persist_update: StateUpdateKind::IntrospectionSourceIndex,
});
collection_impl!({
    name: ClusterReplicaCollection,
    key: proto::ClusterReplicaKey,
    value: proto::ClusterReplicaValue,
    collection_type: CollectionType::ClusterReplica,
    trace_field: cluster_replicas,
    stash_collection: CLUSTER_REPLICA_COLLECTION,
    persist_update: StateUpdateKind::ClusterReplica,
});
collection_impl!({
    name: CommentCollection,
    key: proto::CommentKey,
    value: proto::CommentValue,
    collection_type: CollectionType::Comment,
    trace_field: comments,
    stash_collection: COMMENTS_COLLECTION,
    persist_update: StateUpdateKind::Comment,
});
collection_impl!({
    name: ConfigCollection,
    key: proto::ConfigKey,
    value: proto::ConfigValue,
    collection_type: CollectionType::Config,
    trace_field: configs,
    stash_collection: CONFIG_COLLECTION,
    persist_update: StateUpdateKind::Config,
});
collection_impl!({
    name: DatabaseCollection,
    key: proto::DatabaseKey,
    value: proto::DatabaseValue,
    collection_type: CollectionType::Database,
    trace_field: databases,
    stash_collection: DATABASES_COLLECTION,
    persist_update: StateUpdateKind::Database,
});
collection_impl!({
    name: DefaultPrivilegeCollection,
    key: proto::DefaultPrivilegesKey,
    value: proto::DefaultPrivilegesValue,
    collection_type: CollectionType::DefaultPrivilege,
    trace_field: default_privileges,
    stash_collection: DEFAULT_PRIVILEGES_COLLECTION,
    persist_update: StateUpdateKind::DefaultPrivilege,
});
collection_impl!({
    name: IdAllocatorCollection,
    key: proto::IdAllocKey,
    value: proto::IdAllocValue,
    collection_type: CollectionType::IdAllocator,
    trace_field: id_allocator,
    stash_collection: ID_ALLOCATOR_COLLECTION,
    persist_update: StateUpdateKind::IdAllocator,
});
collection_impl!({
    name: ItemCollection,
    key: proto::ItemKey,
    value: proto::ItemValue,
    collection_type: CollectionType::Item,
    trace_field: items,
    stash_collection: ITEM_COLLECTION,
    persist_update: StateUpdateKind::Item,
});
collection_impl!({
    name: RoleCollection,
    key: proto::RoleKey,
    value: proto::RoleValue,
    collection_type: CollectionType::Role,
    trace_field: roles,
    stash_collection: ROLES_COLLECTION,
    persist_update: StateUpdateKind::Role,
});
collection_impl!({
    name: SchemaCollection,
    key: proto::SchemaKey,
    value: proto::SchemaValue,
    collection_type: CollectionType::Schema,
    trace_field: schemas,
    stash_collection: SCHEMAS_COLLECTION,
    persist_update: StateUpdateKind::Schema,
});
collection_impl!({
    name: SettingCollection,
    key: proto::SettingKey,
    value: proto::SettingValue,
    collection_type: CollectionType::Setting,
    trace_field: settings,
    stash_collection: SETTING_COLLECTION,
    persist_update: StateUpdateKind::Setting,
});
collection_impl!({
    name: StorageUsageCollection,
    key: proto::StorageUsageKey,
    value: (),
    collection_type: CollectionType::StorageUsage,
    trace_field: storage_usage,
    stash_collection: STORAGE_USAGE_COLLECTION,
    persist_update: StateUpdateKind::StorageUsage,
});
collection_impl!({
    name: SystemConfigurationCollection,
    key: proto::ServerConfigurationKey,
    value: proto::ServerConfigurationValue,
    collection_type: CollectionType::SystemConfiguration,
    trace_field: system_configurations,
    stash_collection: SYSTEM_CONFIGURATION_COLLECTION,
    persist_update: StateUpdateKind::SystemConfiguration,
});
collection_impl!({
    name: SystemItemMappingCollection,
    key: proto::GidMappingKey,
    value: proto::GidMappingValue,
    collection_type: CollectionType::SystemItemMapping,
    trace_field: system_object_mappings,
    stash_collection: SYSTEM_GID_MAPPING_COLLECTION,
    persist_update: StateUpdateKind::SystemObjectMapping,
});
collection_impl!({
    name: SystemPrivilegeCollection,
    key: proto::SystemPrivilegesKey,
    value: proto::SystemPrivilegesValue,
    collection_type: CollectionType::SystemPrivilege,
    trace_field: system_privileges,
    stash_collection: SYSTEM_PRIVILEGES_COLLECTION,
    persist_update: StateUpdateKind::SystemPrivilege,
});
collection_impl!({
    name: TimestampCollection,
    key: proto::TimestampKey,
    value: proto::TimestampValue,
    collection_type: CollectionType::Timestamp,
    trace_field: timestamps,
    stash_collection: TIMESTAMP_COLLECTION,
    persist_update: StateUpdateKind::Timestamp,
});

/// A trace of timestamped diffs for a particular [`Collection`].
///
/// The timestamps are represented as strings since different implementations use non-compatible
/// timestamp types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionTrace<T: Collection + ?Sized> {
    pub values: Vec<((T::Key, T::Value), String, Diff)>,
}

impl<T: Collection> CollectionTrace<T> {
    fn new() -> CollectionTrace<T> {
        CollectionTrace { values: Vec::new() }
    }
}

/// Catalog data structured as timestamped diffs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Trace {
    pub audit_log: CollectionTrace<AuditLogCollection>,
    pub clusters: CollectionTrace<ClusterCollection>,
    pub introspection_sources: CollectionTrace<ClusterIntrospectionSourceIndexCollection>,
    pub cluster_replicas: CollectionTrace<ClusterReplicaCollection>,
    pub comments: CollectionTrace<CommentCollection>,
    pub configs: CollectionTrace<ConfigCollection>,
    pub databases: CollectionTrace<DatabaseCollection>,
    pub default_privileges: CollectionTrace<DefaultPrivilegeCollection>,
    pub id_allocator: CollectionTrace<IdAllocatorCollection>,
    pub items: CollectionTrace<ItemCollection>,
    pub roles: CollectionTrace<RoleCollection>,
    pub schemas: CollectionTrace<SchemaCollection>,
    pub settings: CollectionTrace<SettingCollection>,
    pub storage_usage: CollectionTrace<StorageUsageCollection>,
    pub system_object_mappings: CollectionTrace<SystemItemMappingCollection>,
    pub system_configurations: CollectionTrace<SystemConfigurationCollection>,
    pub system_privileges: CollectionTrace<SystemPrivilegeCollection>,
    pub timestamps: CollectionTrace<TimestampCollection>,
}

impl Trace {
    pub(crate) fn new() -> Trace {
        Trace {
            audit_log: CollectionTrace::new(),
            clusters: CollectionTrace::new(),
            introspection_sources: CollectionTrace::new(),
            cluster_replicas: CollectionTrace::new(),
            comments: CollectionTrace::new(),
            configs: CollectionTrace::new(),
            databases: CollectionTrace::new(),
            default_privileges: CollectionTrace::new(),
            id_allocator: CollectionTrace::new(),
            items: CollectionTrace::new(),
            roles: CollectionTrace::new(),
            schemas: CollectionTrace::new(),
            settings: CollectionTrace::new(),
            storage_usage: CollectionTrace::new(),
            system_object_mappings: CollectionTrace::new(),
            system_configurations: CollectionTrace::new(),
            system_privileges: CollectionTrace::new(),
            timestamps: CollectionTrace::new(),
        }
    }
}

pub enum DebugCatalogState {
    Stash(Stash),
    Persist(PersistHandle),
}

impl DebugCatalogState {
    /// Manually update value of `key` in collection `T` to `value`.
    pub async fn edit<T: Collection>(
        &mut self,
        key: T::Key,
        value: T::Value,
    ) -> Result<Option<T::Value>, CatalogError>
    where
        T::Key: mz_stash::Data + Clone + 'static,
        T::Value: mz_stash::Data + Clone + 'static,
    {
        match self {
            DebugCatalogState::Stash(stash) => {
                durable::stash::debug_edit::<T>(stash, key, value).await
            }
            DebugCatalogState::Persist(handle) => handle.debug_edit::<T>(key, value).await,
        }
    }

    /// Manually delete `key` from collection `T`.
    pub async fn delete<T: Collection>(&mut self, key: T::Key) -> Result<(), CatalogError>
    where
        T::Key: mz_stash::Data + Clone + 'static,
        T::Value: mz_stash::Data + Clone,
    {
        match self {
            DebugCatalogState::Stash(stash) => durable::stash::debug_delete::<T>(stash, key).await,
            DebugCatalogState::Persist(handle) => handle.debug_delete::<T>(key).await,
        }
    }
}
