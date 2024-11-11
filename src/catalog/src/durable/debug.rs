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

use std::fmt::Debug;

use mz_repr::Diff;
use serde::{Deserialize, Serialize};
use serde_plain::{derive_display_from_serialize, derive_fromstr_from_deserialize};

use crate::durable::objects::serialization::proto;
use crate::durable::objects::state_update::StateUpdateKind;
use crate::durable::persist::{Timestamp, UnopenedPersistCatalogState};
use crate::durable::CatalogError;

/// The contents of the catalog are logically separated into separate [`Collection`]s, which
/// describe the category of data that the content belongs to.
pub trait Collection: Debug {
    /// Type used to stores keys for [`Collection`].
    type Key;
    /// Type used to stores values for [`Collection`].
    type Value;

    /// [`CollectionType`] corresponding to [`Collection`].
    fn collection_type() -> CollectionType;

    /// Extract the [`CollectionTrace`] from a [`Trace`] that corresponds to [`Collection`].
    fn collection_trace(trace: Trace) -> CollectionTrace<Self>;

    /// Generate a `StateUpdateKind` with `key` and `value` that corresponds to [`Collection`].
    fn update(key: Self::Key, value: Self::Value) -> StateUpdateKind;

    /// The human-readable name of this collection.
    fn name() -> String {
        Self::collection_type().to_string()
    }
}

/// The type of a [`Collection`].
///
/// See [`Collection`] for more details.
///
/// The names of each variant are used to determine the labels of each [`CollectionTrace`] when
/// dumping a [`Trace`].
#[derive(PartialEq, Eq, Hash, Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CollectionType {
    AuditLog,
    ComputeInstance,
    ComputeIntrospectionSourceIndex,
    ComputeReplicas,
    Comments,
    Config,
    Database,
    DefaultPrivileges,
    IdAlloc,
    Item,
    NetworkPolicy,
    Role,
    Schema,
    Setting,
    SourceReferences,
    SystemConfiguration,
    SystemGidMapping,
    SystemPrivileges,
    StorageCollectionMetadata,
    UnfinalizedShard,
    TxnWalShard,
}

derive_display_from_serialize!(CollectionType);
derive_fromstr_from_deserialize!(CollectionType);

/// Macro to simplify implementing [`Collection`].
///
/// The arguments to `collection_impl!` are:
/// - `$name`, which will be the name of the implementing struct.
/// - `$key`, the type used to store keys.
/// - `$value`, the type used to store values.
/// - `$collection_type`, the [`CollectionType`].
/// - `$trace_field`, the corresponding field name within a [`Trace`].
/// - `$update`, the corresponding [`StateUpdateKind`] constructor.
macro_rules! collection_impl {
    ({
    name: $name:ident,
    key: $key:ty,
    value: $value:ty,
    collection_type: $collection_type:expr,
    trace_field: $trace_field:ident,
    update: $update:expr,
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

            fn update(key: Self::Key, value: Self::Value) -> StateUpdateKind {
                $update(key, value)
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
    update: StateUpdateKind::AuditLog,
});
collection_impl!({
    name: ClusterCollection,
    key: proto::ClusterKey,
    value: proto::ClusterValue,
    collection_type: CollectionType::ComputeInstance,
    trace_field: clusters,
    update: StateUpdateKind::Cluster,
});
collection_impl!({
    name: ClusterIntrospectionSourceIndexCollection,
    key: proto::ClusterIntrospectionSourceIndexKey,
    value: proto::ClusterIntrospectionSourceIndexValue,
    collection_type: CollectionType::ComputeIntrospectionSourceIndex,
    trace_field: introspection_sources,
    update: StateUpdateKind::IntrospectionSourceIndex,
});
collection_impl!({
    name: ClusterReplicaCollection,
    key: proto::ClusterReplicaKey,
    value: proto::ClusterReplicaValue,
    collection_type: CollectionType::ComputeReplicas,
    trace_field: cluster_replicas,
    update: StateUpdateKind::ClusterReplica,
});
collection_impl!({
    name: CommentCollection,
    key: proto::CommentKey,
    value: proto::CommentValue,
    collection_type: CollectionType::Comments,
    trace_field: comments,
    update: StateUpdateKind::Comment,
});
collection_impl!({
    name: ConfigCollection,
    key: proto::ConfigKey,
    value: proto::ConfigValue,
    collection_type: CollectionType::Config,
    trace_field: configs,
    update: StateUpdateKind::Config,
});
collection_impl!({
    name: DatabaseCollection,
    key: proto::DatabaseKey,
    value: proto::DatabaseValue,
    collection_type: CollectionType::Database,
    trace_field: databases,
    update: StateUpdateKind::Database,
});
collection_impl!({
    name: DefaultPrivilegeCollection,
    key: proto::DefaultPrivilegesKey,
    value: proto::DefaultPrivilegesValue,
    collection_type: CollectionType::DefaultPrivileges,
    trace_field: default_privileges,
    update: StateUpdateKind::DefaultPrivilege,
});
collection_impl!({
    name: IdAllocatorCollection,
    key: proto::IdAllocKey,
    value: proto::IdAllocValue,
    collection_type: CollectionType::IdAlloc,
    trace_field: id_allocator,
    update: StateUpdateKind::IdAllocator,
});
collection_impl!({
    name: ItemCollection,
    key: proto::ItemKey,
    value: proto::ItemValue,
    collection_type: CollectionType::Item,
    trace_field: items,
    update: StateUpdateKind::Item,
});
collection_impl!({
    name: NetworkPolicyCollection,
    key: proto::NetworkPolicyKey,
    value: proto::NetworkPolicyValue,
    collection_type: CollectionType::NetworkPolicy,
    trace_field: network_policies,
    update: StateUpdateKind::NetworkPolicy,
});
collection_impl!({
    name: RoleCollection,
    key: proto::RoleKey,
    value: proto::RoleValue,
    collection_type: CollectionType::Role,
    trace_field: roles,
    update: StateUpdateKind::Role,
});
collection_impl!({
    name: SchemaCollection,
    key: proto::SchemaKey,
    value: proto::SchemaValue,
    collection_type: CollectionType::Schema,
    trace_field: schemas,
    update: StateUpdateKind::Schema,
});
collection_impl!({
    name: SettingCollection,
    key: proto::SettingKey,
    value: proto::SettingValue,
    collection_type: CollectionType::Setting,
    trace_field: settings,
    update: StateUpdateKind::Setting,
});
collection_impl!({
    name: SourceReferencesCollection,
    key: proto::SourceReferencesKey,
    value: proto::SourceReferencesValue,
    collection_type: CollectionType::SourceReferences,
    trace_field: source_references,
    update: StateUpdateKind::SourceReferences,
});
collection_impl!({
    name: SystemConfigurationCollection,
    key: proto::ServerConfigurationKey,
    value: proto::ServerConfigurationValue,
    collection_type: CollectionType::SystemConfiguration,
    trace_field: system_configurations,
    update: StateUpdateKind::SystemConfiguration,
});
collection_impl!({
    name: SystemItemMappingCollection,
    key: proto::GidMappingKey,
    value: proto::GidMappingValue,
    collection_type: CollectionType::SystemGidMapping,
    trace_field: system_object_mappings,
    update: StateUpdateKind::SystemObjectMapping,
});
collection_impl!({
    name: SystemPrivilegeCollection,
    key: proto::SystemPrivilegesKey,
    value: proto::SystemPrivilegesValue,
    collection_type: CollectionType::SystemPrivileges,
    trace_field: system_privileges,
    update: StateUpdateKind::SystemPrivilege,
});

collection_impl!({
    name: StorageCollectionMetadataCollection,
    key: proto::StorageCollectionMetadataKey,
    value: proto::StorageCollectionMetadataValue,
    collection_type: CollectionType::StorageCollectionMetadata,
    trace_field: storage_collection_metadata,
    update: StateUpdateKind::StorageCollectionMetadata,
});
collection_impl!({
    name: UnfinalizedShardsCollection,
    key: proto::UnfinalizedShardKey,
    value: (),
    collection_type: CollectionType::UnfinalizedShard,
    trace_field: unfinalized_shards,
    update: StateUpdateKind::UnfinalizedShard,
});
collection_impl!({
    name: TxnWalShardCollection,
    key: (),
    value: proto::TxnWalShardValue,
    collection_type: CollectionType::TxnWalShard,
    trace_field: txn_wal_shard,
    update: StateUpdateKind::TxnWalShard,
});

/// A trace of timestamped diffs for a particular [`Collection`].
///
/// The timestamps are represented as strings since different implementations use non-compatible
/// timestamp types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionTrace<T: Collection + ?Sized> {
    pub values: Vec<((T::Key, T::Value), Timestamp, Diff)>,
}

impl<T: Collection> CollectionTrace<T> {
    fn new() -> CollectionTrace<T> {
        CollectionTrace { values: Vec::new() }
    }
}

impl<T: Collection> CollectionTrace<T>
where
    T: Collection,
    T::Key: Ord,
    T::Value: Ord,
{
    fn sort(&mut self) {
        self.values
            .sort_by(|(x1, ts1, d1), (x2, ts2, d2)| ts1.cmp(ts2).then(d1.cmp(d2)).then(x1.cmp(x2)));
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
    pub network_policies: CollectionTrace<NetworkPolicyCollection>,
    pub roles: CollectionTrace<RoleCollection>,
    pub schemas: CollectionTrace<SchemaCollection>,
    pub settings: CollectionTrace<SettingCollection>,
    pub source_references: CollectionTrace<SourceReferencesCollection>,
    pub system_object_mappings: CollectionTrace<SystemItemMappingCollection>,
    pub system_configurations: CollectionTrace<SystemConfigurationCollection>,
    pub system_privileges: CollectionTrace<SystemPrivilegeCollection>,
    pub storage_collection_metadata: CollectionTrace<StorageCollectionMetadataCollection>,
    pub unfinalized_shards: CollectionTrace<UnfinalizedShardsCollection>,
    pub txn_wal_shard: CollectionTrace<TxnWalShardCollection>,
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
            network_policies: CollectionTrace::new(),
            roles: CollectionTrace::new(),
            schemas: CollectionTrace::new(),
            settings: CollectionTrace::new(),
            source_references: CollectionTrace::new(),
            system_object_mappings: CollectionTrace::new(),
            system_configurations: CollectionTrace::new(),
            system_privileges: CollectionTrace::new(),
            storage_collection_metadata: CollectionTrace::new(),
            unfinalized_shards: CollectionTrace::new(),
            txn_wal_shard: CollectionTrace::new(),
        }
    }

    pub fn sort(&mut self) {
        let Trace {
            audit_log,
            clusters,
            introspection_sources,
            cluster_replicas,
            comments,
            configs,
            databases,
            default_privileges,
            id_allocator,
            items,
            network_policies,
            roles,
            schemas,
            settings,
            source_references,
            system_object_mappings,
            system_configurations,
            system_privileges,
            storage_collection_metadata,
            unfinalized_shards,
            txn_wal_shard,
        } = self;
        audit_log.sort();
        clusters.sort();
        introspection_sources.sort();
        cluster_replicas.sort();
        comments.sort();
        configs.sort();
        databases.sort();
        default_privileges.sort();
        id_allocator.sort();
        items.sort();
        network_policies.sort();
        roles.sort();
        schemas.sort();
        settings.sort();
        source_references.sort();
        system_object_mappings.sort();
        system_configurations.sort();
        system_privileges.sort();
        storage_collection_metadata.sort();
        unfinalized_shards.sort();
        txn_wal_shard.sort();
    }
}

pub struct DebugCatalogState(pub(crate) UnopenedPersistCatalogState);

impl DebugCatalogState {
    /// Manually update value of `key` in collection `T` to `value`.
    pub async fn edit<T: Collection>(
        &mut self,
        key: T::Key,
        value: T::Value,
    ) -> Result<Option<T::Value>, CatalogError>
    where
        T::Key: PartialEq + Eq + Debug + Clone,
        T::Value: Debug + Clone,
    {
        self.0.debug_edit::<T>(key, value).await
    }

    /// Manually delete `key` from collection `T`.
    pub async fn delete<T: Collection>(&mut self, key: T::Key) -> Result<(), CatalogError>
    where
        T::Key: PartialEq + Eq + Debug + Clone,
        T::Value: Debug,
    {
        self.0.debug_delete::<T>(key).await
    }
}
