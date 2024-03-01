// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};

use anyhow::anyhow;
use derivative::Derivative;
use itertools::Itertools;

use mz_audit_log::{VersionedEvent, VersionedStorageUsage};
use mz_controller_types::{ClusterId, ReplicaId};
use mz_ore::cast::{u64_to_usize, usize_to_u64};
use mz_ore::collections::{CollectionExt, HashSet};
use mz_ore::{soft_assert_no_log, soft_assert_or_log};
use mz_pgrepr::oid::FIRST_USER_OID;
use mz_proto::{RustType, TryFromProtoError};
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem};
use mz_repr::role_id::RoleId;
use mz_repr::{Diff, GlobalId};
use mz_sql::catalog::{
    CatalogError as SqlCatalogError, CatalogItemType, ObjectType, RoleAttributes, RoleMembership,
    RoleVars,
};
use mz_sql::names::{CommentObjectId, DatabaseId, SchemaId};
use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;
use mz_sql_parser::ast::QualifiedReplica;
use mz_storage_types::controller::PersistTxnTablesImpl;
use mz_storage_types::sources::Timeline;

use crate::builtin::BuiltinLog;
use crate::durable::initialize::{PERSIST_TXN_TABLES, SYSTEM_CONFIG_SYNCED_KEY};
use crate::durable::objects::serialization::proto;
use crate::durable::objects::{
    AuditLogKey, Cluster, ClusterConfig, ClusterIntrospectionSourceIndexKey,
    ClusterIntrospectionSourceIndexValue, ClusterKey, ClusterReplica, ClusterReplicaKey,
    ClusterReplicaValue, ClusterValue, CommentKey, CommentValue, Config, ConfigKey, ConfigValue,
    Database, DatabaseKey, DatabaseValue, DefaultPrivilegesKey, DefaultPrivilegesValue,
    DurableType, GidMappingKey, GidMappingValue, IdAllocKey, IdAllocValue,
    IntrospectionSourceIndex, Item, ItemKey, ItemValue, PersistTxnShardValue, ReplicaConfig, Role,
    RoleKey, RoleValue, Schema, SchemaKey, SchemaValue, ServerConfigurationKey,
    ServerConfigurationValue, SettingKey, SettingValue, StorageMetadataKey, StorageMetadataValue,
    StorageUsageKey, SystemObjectMapping, SystemPrivilegesKey, SystemPrivilegesValue, TimestampKey,
    TimestampValue, UnfinalizedShardKey,
};
use crate::durable::{
    CatalogError, Comment, DefaultPrivilege, DurableCatalogError, DurableCatalogState, Snapshot,
    SystemConfiguration, TimelineTimestamp, CATALOG_CONTENT_VERSION_KEY, DATABASE_ID_ALLOC_KEY,
    OID_ALLOC_KEY, SCHEMA_ID_ALLOC_KEY, SYSTEM_ITEM_ALLOC_KEY, USER_ITEM_ALLOC_KEY,
    USER_ROLE_ID_ALLOC_KEY,
};

/// A [`Transaction`] batches multiple catalog operations together and commits them atomically.
#[derive(Derivative)]
#[derivative(Debug, PartialEq)]
pub struct Transaction<'a> {
    #[derivative(Debug = "ignore")]
    #[derivative(PartialEq = "ignore")]
    durable_catalog: &'a mut dyn DurableCatalogState,
    databases: TableTransaction<DatabaseKey, DatabaseValue>,
    schemas: TableTransaction<SchemaKey, SchemaValue>,
    items: TableTransaction<ItemKey, ItemValue>,
    comments: TableTransaction<CommentKey, CommentValue>,
    roles: TableTransaction<RoleKey, RoleValue>,
    clusters: TableTransaction<ClusterKey, ClusterValue>,
    cluster_replicas: TableTransaction<ClusterReplicaKey, ClusterReplicaValue>,
    introspection_sources:
        TableTransaction<ClusterIntrospectionSourceIndexKey, ClusterIntrospectionSourceIndexValue>,
    id_allocator: TableTransaction<IdAllocKey, IdAllocValue>,
    configs: TableTransaction<ConfigKey, ConfigValue>,
    settings: TableTransaction<SettingKey, SettingValue>,
    timestamps: TableTransaction<TimestampKey, TimestampValue>,
    system_gid_mapping: TableTransaction<GidMappingKey, GidMappingValue>,
    system_configurations: TableTransaction<ServerConfigurationKey, ServerConfigurationValue>,
    default_privileges: TableTransaction<DefaultPrivilegesKey, DefaultPrivilegesValue>,
    system_privileges: TableTransaction<SystemPrivilegesKey, SystemPrivilegesValue>,
    storage_metadata: TableTransaction<StorageMetadataKey, StorageMetadataValue>,
    unfinalized_shards: TableTransaction<UnfinalizedShardKey, ()>,
    persist_txn_shard: TableTransaction<(), PersistTxnShardValue>,
    // Don't make this a table transaction so that it's not read into the
    // in-memory cache.
    audit_log_updates: Vec<(proto::AuditLogKey, (), i64)>,
    storage_usage_updates: Vec<(proto::StorageUsageKey, (), i64)>,
}

impl<'a> Transaction<'a> {
    pub fn new(
        durable_catalog: &'a mut dyn DurableCatalogState,
        Snapshot {
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
            storage_metadata,
            unfinalized_shards,
            persist_txn_shard,
        }: Snapshot,
    ) -> Result<Transaction, CatalogError> {
        Ok(Transaction {
            durable_catalog,
            databases: TableTransaction::new(databases, |a: &DatabaseValue, b| a.name == b.name)?,
            schemas: TableTransaction::new(schemas, |a: &SchemaValue, b| {
                a.database_id == b.database_id && a.name == b.name
            })?,
            items: TableTransaction::new(items, |a: &ItemValue, b| {
                let a_type = a.item_type();
                let b_type = b.item_type();
                a.schema_id == b.schema_id
                    && a.name == b.name
                    && ((a_type != CatalogItemType::Type && b_type != CatalogItemType::Type)
                        || (a_type == CatalogItemType::Type && b_type.conflicts_with_type())
                        || (b_type == CatalogItemType::Type && a_type.conflicts_with_type()))
            })?,
            comments: TableTransaction::new(comments, |_a, _b| false)?,
            roles: TableTransaction::new(roles, |a: &RoleValue, b| a.name == b.name)?,
            clusters: TableTransaction::new(clusters, |a: &ClusterValue, b| a.name == b.name)?,
            cluster_replicas: TableTransaction::new(
                cluster_replicas,
                |a: &ClusterReplicaValue, b| a.cluster_id == b.cluster_id && a.name == b.name,
            )?,
            introspection_sources: TableTransaction::new(introspection_sources, |_a, _b| false)?,
            id_allocator: TableTransaction::new(id_allocator, |_a, _b| false)?,
            configs: TableTransaction::new(configs, |_a, _b| false)?,
            settings: TableTransaction::new(settings, |_a, _b| false)?,
            timestamps: TableTransaction::new(timestamps, |_a, _b| false)?,
            system_gid_mapping: TableTransaction::new(system_object_mappings, |_a, _b| false)?,
            system_configurations: TableTransaction::new(system_configurations, |_a, _b| false)?,
            default_privileges: TableTransaction::new(default_privileges, |_a, _b| false)?,
            system_privileges: TableTransaction::new(system_privileges, |_a, _b| false)?,
            storage_metadata: TableTransaction::new(
                storage_metadata,
                |a: &StorageMetadataValue, b| a.shard == b.shard,
            )?,
            unfinalized_shards: TableTransaction::new(unfinalized_shards, |_a, _b| false)?,
            persist_txn_shard: TableTransaction::new(persist_txn_shard, |_a, _b| false)?,
            audit_log_updates: Vec::new(),
            storage_usage_updates: Vec::new(),
        })
    }

    pub fn loaded_items(&self) -> Vec<Item> {
        let mut items = Vec::new();
        self.items.for_values(|k, v| {
            items.push(Item::from_key_value(k.clone(), v.clone()));
        });
        items.sort_by_key(|Item { id, .. }| *id);
        items
    }

    pub fn insert_audit_log_event(&mut self, event: VersionedEvent) {
        self.insert_audit_log_events([event]);
    }

    pub fn insert_audit_log_events(&mut self, events: impl IntoIterator<Item = VersionedEvent>) {
        let events = events
            .into_iter()
            .map(|event| (AuditLogKey { event }.into_proto(), (), 1));
        self.audit_log_updates.extend(events);
    }

    pub fn insert_storage_usage_event(&mut self, metric: VersionedStorageUsage) {
        self.insert_storage_usage_events([metric]);
    }

    pub fn insert_storage_usage_events(
        &mut self,
        metrics: impl IntoIterator<Item = VersionedStorageUsage>,
    ) {
        let metrics = metrics
            .into_iter()
            .map(|metric| (StorageUsageKey { metric }.into_proto(), (), 1));
        self.storage_usage_updates.extend(metrics);
    }

    pub fn insert_user_database(
        &mut self,
        database_name: &str,
        owner_id: RoleId,
        privileges: Vec<MzAclItem>,
    ) -> Result<(DatabaseId, u32), CatalogError> {
        let id = self.get_and_increment_id(DATABASE_ID_ALLOC_KEY.to_string())?;
        let id = DatabaseId::User(id);
        let oid = self.allocate_oid()?;
        self.insert_database(id, database_name, owner_id, privileges, oid)?;
        Ok((id, oid))
    }

    pub(crate) fn insert_database(
        &mut self,
        id: DatabaseId,
        database_name: &str,
        owner_id: RoleId,
        privileges: Vec<MzAclItem>,
        oid: u32,
    ) -> Result<u32, CatalogError> {
        match self.databases.insert(
            DatabaseKey { id },
            DatabaseValue {
                name: database_name.to_string(),
                owner_id,
                privileges,
                oid,
            },
        ) {
            Ok(_) => Ok(oid),
            Err(_) => Err(SqlCatalogError::DatabaseAlreadyExists(database_name.to_owned()).into()),
        }
    }

    pub fn insert_user_schema(
        &mut self,
        database_id: DatabaseId,
        schema_name: &str,
        owner_id: RoleId,
        privileges: Vec<MzAclItem>,
    ) -> Result<(SchemaId, u32), CatalogError> {
        let id = self.get_and_increment_id(SCHEMA_ID_ALLOC_KEY.to_string())?;
        let id = SchemaId::User(id);
        let oid = self.allocate_oid()?;
        self.insert_schema(
            id,
            Some(database_id),
            schema_name.to_string(),
            owner_id,
            privileges,
            oid,
        )?;
        Ok((id, oid))
    }

    pub(crate) fn insert_schema(
        &mut self,
        schema_id: SchemaId,
        database_id: Option<DatabaseId>,
        schema_name: String,
        owner_id: RoleId,
        privileges: Vec<MzAclItem>,
        oid: u32,
    ) -> Result<(), CatalogError> {
        match self.schemas.insert(
            SchemaKey { id: schema_id },
            SchemaValue {
                database_id,
                name: schema_name.clone(),
                owner_id,
                privileges,
                oid,
            },
        ) {
            Ok(_) => Ok(()),
            Err(_) => Err(SqlCatalogError::SchemaAlreadyExists(schema_name).into()),
        }
    }

    pub fn insert_system_role(
        &mut self,
        id: RoleId,
        name: String,
        attributes: RoleAttributes,
        membership: RoleMembership,
        vars: RoleVars,
        oid: u32,
    ) -> Result<RoleId, CatalogError> {
        soft_assert_or_log!(
            id.is_system() || id.is_public(),
            "ID {id:?} is not system or public variant"
        );
        self.insert_role(id, name, attributes, membership, vars, oid)?;
        Ok(id)
    }

    pub fn insert_user_role(
        &mut self,
        name: String,
        attributes: RoleAttributes,
        membership: RoleMembership,
        vars: RoleVars,
    ) -> Result<(RoleId, u32), CatalogError> {
        let id = self.get_and_increment_id(USER_ROLE_ID_ALLOC_KEY.to_string())?;
        let id = RoleId::User(id);
        let oid = self.allocate_oid()?;
        self.insert_role(id, name, attributes, membership, vars, oid)?;
        Ok((id, oid))
    }

    fn insert_role(
        &mut self,
        id: RoleId,
        name: String,
        attributes: RoleAttributes,
        membership: RoleMembership,
        vars: RoleVars,
        oid: u32,
    ) -> Result<(), CatalogError> {
        match self.roles.insert(
            RoleKey { id },
            RoleValue {
                name: name.clone(),
                attributes,
                membership,
                vars,
                oid,
            },
        ) {
            Ok(_) => Ok(()),
            Err(_) => Err(SqlCatalogError::RoleAlreadyExists(name).into()),
        }
    }

    /// Panics if any introspection source id is not a system id
    pub fn insert_user_cluster(
        &mut self,
        cluster_id: ClusterId,
        cluster_name: &str,
        introspection_source_indexes: Vec<(&'static BuiltinLog, GlobalId)>,
        owner_id: RoleId,
        privileges: Vec<MzAclItem>,
        config: ClusterConfig,
    ) -> Result<Vec<(&'static BuiltinLog, GlobalId, u32)>, CatalogError> {
        self.insert_cluster(
            cluster_id,
            cluster_name,
            introspection_source_indexes,
            owner_id,
            privileges,
            config,
        )
    }

    /// Panics if any introspection source id is not a system id
    pub fn insert_system_cluster(
        &mut self,
        cluster_id: ClusterId,
        cluster_name: &str,
        introspection_source_indexes: Vec<(&'static BuiltinLog, GlobalId)>,
        privileges: Vec<MzAclItem>,
        config: ClusterConfig,
    ) -> Result<Vec<(&'static BuiltinLog, GlobalId, u32)>, CatalogError> {
        self.insert_cluster(
            cluster_id,
            cluster_name,
            introspection_source_indexes,
            MZ_SYSTEM_ROLE_ID,
            privileges,
            config,
        )
    }

    fn insert_cluster(
        &mut self,
        cluster_id: ClusterId,
        cluster_name: &str,
        introspection_source_indexes: Vec<(&'static BuiltinLog, GlobalId)>,
        owner_id: RoleId,
        privileges: Vec<MzAclItem>,
        config: ClusterConfig,
    ) -> Result<Vec<(&'static BuiltinLog, GlobalId, u32)>, CatalogError> {
        if let Err(_) = self.clusters.insert(
            ClusterKey { id: cluster_id },
            ClusterValue {
                name: cluster_name.to_string(),
                owner_id,
                privileges,
                config,
            },
        ) {
            return Err(SqlCatalogError::ClusterAlreadyExists(cluster_name.to_owned()).into());
        };

        let oids = self.allocate_oids(usize_to_u64(introspection_source_indexes.len()))?;
        let introspection_source_indexes: Vec<_> = introspection_source_indexes
            .into_iter()
            .zip(oids.into_iter())
            .map(|((builtin, index_id), oid)| (builtin, index_id, oid))
            .collect();
        for (builtin, index_id, oid) in &introspection_source_indexes {
            let introspection_source_index = IntrospectionSourceIndex {
                cluster_id,
                name: builtin.name.to_string(),
                index_id: *index_id,
                oid: *oid,
            };
            let (key, value) = introspection_source_index.into_key_value();
            self.introspection_sources
                .insert(key, value)
                .expect("no uniqueness violation");
        }

        Ok(introspection_source_indexes)
    }

    pub fn rename_cluster(
        &mut self,
        cluster_id: ClusterId,
        cluster_name: &str,
        cluster_to_name: &str,
    ) -> Result<(), CatalogError> {
        let key = ClusterKey { id: cluster_id };

        match self.clusters.update(|k, v| {
            if *k == key {
                let mut value = v.clone();
                value.name = cluster_to_name.to_string();
                Some(value)
            } else {
                None
            }
        })? {
            0 => Err(SqlCatalogError::UnknownCluster(cluster_name.to_string()).into()),
            1 => Ok(()),
            n => panic!(
                "Expected to update single cluster {cluster_name} ({cluster_id}), updated {n}"
            ),
        }
    }

    pub fn check_migration_has_run(&mut self, name: String) -> Result<bool, CatalogError> {
        let key = SettingKey { name };
        // If the key does not exist, then the migration has not been run.
        let has_run = self.settings.get(&key).as_ref().is_some();

        Ok(has_run)
    }

    pub fn mark_migration_has_run(&mut self, name: String) -> Result<(), CatalogError> {
        let key = SettingKey { name };
        let val = SettingValue {
            value: true.to_string(),
        };
        self.settings.insert(key, val)?;

        Ok(())
    }

    pub fn rename_cluster_replica(
        &mut self,
        replica_id: ReplicaId,
        replica_name: &QualifiedReplica,
        replica_to_name: &str,
    ) -> Result<(), CatalogError> {
        let key = ClusterReplicaKey { id: replica_id };

        match self.cluster_replicas.update(|k, v| {
            if *k == key {
                let mut value = v.clone();
                value.name = replica_to_name.to_string();
                Some(value)
            } else {
                None
            }
        })? {
            0 => Err(SqlCatalogError::UnknownClusterReplica(replica_name.to_string()).into()),
            1 => Ok(()),
            n => panic!(
                "Expected to update single cluster replica {replica_name} ({replica_id}), updated {n}"
            ),
        }
    }

    pub fn insert_cluster_replica(
        &mut self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        replica_name: &str,
        config: ReplicaConfig,
        owner_id: RoleId,
    ) -> Result<(), CatalogError> {
        if let Err(_) = self.cluster_replicas.insert(
            ClusterReplicaKey { id: replica_id },
            ClusterReplicaValue {
                cluster_id,
                name: replica_name.into(),
                config,
                owner_id,
            },
        ) {
            let cluster = self
                .clusters
                .get(&ClusterKey { id: cluster_id })
                .expect("cluster exists");
            return Err(SqlCatalogError::DuplicateReplica(
                replica_name.to_string(),
                cluster.name.to_string(),
            )
            .into());
        };
        Ok(())
    }

    /// Updates persisted information about persisted introspection source
    /// indexes.
    ///
    /// Panics if provided id is not a system id.
    pub fn update_introspection_source_index_gids(
        &mut self,
        mappings: impl Iterator<Item = (ClusterId, impl Iterator<Item = (String, GlobalId, u32)>)>,
    ) -> Result<(), CatalogError> {
        for (cluster_id, updates) in mappings {
            for (name, index_id, oid) in updates {
                let introspection_source_index = IntrospectionSourceIndex {
                    cluster_id,
                    name,
                    index_id,
                    oid,
                };
                let (key, value) = introspection_source_index.into_key_value();

                let prev = self.introspection_sources.set(key, Some(value))?;
                if prev.is_none() {
                    return Err(SqlCatalogError::FailedBuiltinSchemaMigration(format!(
                        "{index_id}"
                    ))
                    .into());
                }
            }
        }
        Ok(())
    }

    pub fn insert_user_item(
        &mut self,
        id: GlobalId,
        schema_id: SchemaId,
        item_name: &str,
        create_sql: String,
        owner_id: RoleId,
        privileges: Vec<MzAclItem>,
    ) -> Result<u32, CatalogError> {
        let oid = self.allocate_oid()?;
        self.insert_item(
            id, oid, schema_id, item_name, create_sql, owner_id, privileges,
        )?;
        Ok(oid)
    }

    pub fn insert_item(
        &mut self,
        id: GlobalId,
        oid: u32,
        schema_id: SchemaId,
        item_name: &str,
        create_sql: String,
        owner_id: RoleId,
        privileges: Vec<MzAclItem>,
    ) -> Result<(), CatalogError> {
        match self.items.insert(
            ItemKey { gid: id },
            ItemValue {
                schema_id,
                name: item_name.to_string(),
                create_sql,
                owner_id,
                privileges,
                oid,
            },
        ) {
            Ok(_) => Ok(()),
            Err(_) => Err(SqlCatalogError::ItemAlreadyExists(id, item_name.to_owned()).into()),
        }
    }

    pub fn insert_timestamp(
        &mut self,
        timeline: Timeline,
        ts: mz_repr::Timestamp,
    ) -> Result<(), CatalogError> {
        match self.timestamps.insert(
            TimestampKey {
                id: timeline.to_string(),
            },
            TimestampValue { ts },
        ) {
            Ok(_) => Ok(()),
            Err(_) => Err(SqlCatalogError::TimelineAlreadyExists(timeline.to_string()).into()),
        }
    }

    pub fn get_and_increment_id(&mut self, key: String) -> Result<u64, CatalogError> {
        Ok(self.get_and_increment_id_by(key, 1)?.into_element())
    }

    pub fn get_and_increment_id_by(
        &mut self,
        key: String,
        amount: u64,
    ) -> Result<Vec<u64>, CatalogError> {
        let current_id = self
            .id_allocator
            .items()
            .get(&IdAllocKey { name: key.clone() })
            .unwrap_or_else(|| panic!("{key} id allocator missing"))
            .next_id;
        let next_id = current_id
            .checked_add(amount)
            .ok_or(SqlCatalogError::IdExhaustion)?;
        let prev = self
            .id_allocator
            .set(IdAllocKey { name: key }, Some(IdAllocValue { next_id }))?;
        assert_eq!(
            prev,
            Some(IdAllocValue {
                next_id: current_id
            })
        );
        Ok((current_id..next_id).collect())
    }

    pub fn allocate_system_item_ids(&mut self, amount: u64) -> Result<Vec<GlobalId>, CatalogError> {
        Ok(self
            .get_and_increment_id_by(SYSTEM_ITEM_ALLOC_KEY.to_string(), amount)?
            .into_iter()
            .map(GlobalId::System)
            .collect())
    }

    pub fn allocate_user_item_ids(&mut self, amount: u64) -> Result<Vec<GlobalId>, CatalogError> {
        Ok(self
            .get_and_increment_id_by(USER_ITEM_ALLOC_KEY.to_string(), amount)?
            .into_iter()
            .map(GlobalId::User)
            .collect())
    }

    /// Allocates `amount` OIDs. OIDs can be recycled if they aren't currently assigned to any
    /// object.
    #[mz_ore::instrument]
    fn allocate_oids(&mut self, amount: u64) -> Result<Vec<u32>, CatalogError> {
        /// Struct representing an OID for a user object. Allocated OIDs can be recycled, so when we've
        /// allocated [`u32::MAX`] we'll wrap back around to [`FIRST_USER_OID`].
        struct UserOid(u32);

        impl UserOid {
            fn new(oid: u32) -> Result<UserOid, anyhow::Error> {
                if oid < FIRST_USER_OID {
                    Err(anyhow!("invalid user OID {oid}"))
                } else {
                    Ok(UserOid(oid))
                }
            }
        }

        impl std::ops::AddAssign<u32> for UserOid {
            fn add_assign(&mut self, rhs: u32) {
                let (res, overflow) = self.0.overflowing_add(rhs);
                self.0 = if overflow { FIRST_USER_OID + res } else { res };
            }
        }

        if amount > u32::MAX.into() {
            return Err(CatalogError::Catalog(SqlCatalogError::OidExhaustion));
        }

        // This is potentially slow to do everytime we allocate an OID. A faster approach might be
        // to have an ID allocator that is updated everytime an OID is allocated or de-allocated.
        // However, benchmarking shows that this doesn't make a noticeable difference and the other
        // approach requires making sure that allocator always stays in-sync which can be
        // error-prone. If DDL starts slowing down, this is a good place to try and optimize.
        let mut allocated_oids = HashSet::with_capacity(
            self.databases.items().len()
                + self.schemas.items().len()
                + self.roles.items().len()
                + self.items.items().len()
                + self.introspection_sources.items().len(),
        );
        allocated_oids.extend(
            self.databases
                .items()
                .values()
                .map(|value| value.oid)
                .chain(self.schemas.items().values().map(|value| value.oid))
                .chain(self.roles.items().values().map(|value| value.oid))
                .chain(self.items.items().values().map(|value| value.oid))
                .chain(
                    self.introspection_sources
                        .items()
                        .values()
                        .map(|value| value.oid),
                ),
        );

        let start_oid: u32 = self
            .id_allocator
            .items()
            .get(&IdAllocKey {
                name: OID_ALLOC_KEY.to_string(),
            })
            .unwrap_or_else(|| panic!("{OID_ALLOC_KEY} id allocator missing"))
            .next_id
            .try_into()
            .expect("we should never persist an oid outside of the u32 range");
        let mut current_oid = UserOid::new(start_oid)
            .expect("we should never persist an oid outside of user OID range");
        let mut oids = Vec::new();
        while oids.len() < u64_to_usize(amount) {
            if !allocated_oids.contains(&current_oid.0) {
                oids.push(current_oid.0);
            }
            current_oid += 1;

            if current_oid.0 == start_oid && oids.len() < u64_to_usize(amount) {
                // We've exhausted all possible OIDs and still don't have `amount`.
                return Err(CatalogError::Catalog(SqlCatalogError::OidExhaustion));
            }
        }

        let next_id = current_oid.0;
        let prev = self.id_allocator.set(
            IdAllocKey {
                name: OID_ALLOC_KEY.to_string(),
            },
            Some(IdAllocValue {
                next_id: next_id.into(),
            }),
        )?;
        assert_eq!(
            prev,
            Some(IdAllocValue {
                next_id: start_oid.into(),
            })
        );

        Ok(oids)
    }

    /// Allocates a single OID. OIDs can be recycled if they aren't currently assigned to any
    /// object.
    pub fn allocate_oid(&mut self) -> Result<u32, CatalogError> {
        self.allocate_oids(1).map(|oids| oids.into_element())
    }

    pub(crate) fn insert_id_allocator(
        &mut self,
        name: String,
        next_id: u64,
    ) -> Result<(), CatalogError> {
        match self
            .id_allocator
            .insert(IdAllocKey { name: name.clone() }, IdAllocValue { next_id })
        {
            Ok(_) => Ok(()),
            Err(_) => Err(SqlCatalogError::IdAllocatorAlreadyExists(name).into()),
        }
    }

    pub fn remove_database(&mut self, id: &DatabaseId) -> Result<(), CatalogError> {
        let prev = self.databases.set(DatabaseKey { id: *id }, None)?;
        if prev.is_some() {
            Ok(())
        } else {
            Err(SqlCatalogError::UnknownDatabase(id.to_string()).into())
        }
    }

    pub fn remove_schema(
        &mut self,
        database_id: &Option<DatabaseId>,
        schema_id: &SchemaId,
    ) -> Result<(), CatalogError> {
        let prev = self.schemas.set(SchemaKey { id: *schema_id }, None)?;
        if prev.is_some() {
            Ok(())
        } else {
            let database_name = match database_id {
                Some(id) => format!("{id}."),
                None => "".to_string(),
            };
            Err(SqlCatalogError::UnknownSchema(format!("{}.{}", database_name, schema_id)).into())
        }
    }

    pub fn remove_role(&mut self, name: &str) -> Result<(), CatalogError> {
        let roles = self.roles.delete(|_k, v| v.name == name);
        assert!(
            roles.iter().all(|(k, _)| k.id.is_user()),
            "cannot delete non-user roles"
        );
        let n = roles.len();
        assert!(n <= 1);
        if n == 1 {
            Ok(())
        } else {
            Err(SqlCatalogError::UnknownRole(name.to_owned()).into())
        }
    }

    pub fn remove_cluster(&mut self, id: ClusterId) -> Result<(), CatalogError> {
        let deleted = self.clusters.delete(|k, _v| k.id == id);
        if deleted.is_empty() {
            Err(SqlCatalogError::UnknownCluster(id.to_string()).into())
        } else {
            assert_eq!(deleted.len(), 1);
            // Cascade delete introspection sources and cluster replicas.
            //
            // TODO(benesch): this doesn't seem right. Cascade deletions should
            // be entirely the domain of the higher catalog layer, not the
            // storage layer.
            self.cluster_replicas.delete(|_k, v| v.cluster_id == id);
            self.introspection_sources
                .delete(|k, _v| k.cluster_id == id);
            Ok(())
        }
    }

    pub fn remove_cluster_replica(&mut self, id: ReplicaId) -> Result<(), CatalogError> {
        let deleted = self.cluster_replicas.delete(|k, _v| k.id == id);
        if deleted.len() == 1 {
            Ok(())
        } else {
            assert!(deleted.is_empty());
            Err(SqlCatalogError::UnknownClusterReplica(id.to_string()).into())
        }
    }

    /// Removes all storage usage events in `events` from the transaction.
    pub(crate) fn remove_storage_usage_events(&mut self, events: Vec<VersionedStorageUsage>) {
        let events = events
            .into_iter()
            .map(|event| (StorageUsageKey { metric: event }.into_proto(), (), -1));
        self.storage_usage_updates.extend(events);
    }

    /// Removes item `id` from the transaction.
    ///
    /// Returns an error if `id` is not found.
    ///
    /// Runtime is linear with respect to the total number of items in the catalog.
    /// DO NOT call this function in a loop, use [`Self::remove_items`] instead.
    pub fn remove_item(&mut self, id: GlobalId) -> Result<(), CatalogError> {
        let prev = self.items.set(ItemKey { gid: id }, None)?;
        if prev.is_some() {
            Ok(())
        } else {
            Err(SqlCatalogError::UnknownItem(id.to_string()).into())
        }
    }

    /// Removes all items in `ids` from the transaction.
    ///
    /// Returns an error if any id in `ids` is not found.
    ///
    /// NOTE: On error, there still may be some items removed from the transaction. It is
    /// up to the called to either abort the transaction or commit.
    pub fn remove_items(&mut self, ids: BTreeSet<GlobalId>) -> Result<(), CatalogError> {
        let n = self.items.delete(|k, _v| ids.contains(&k.gid)).len();
        if n == ids.len() {
            Ok(())
        } else {
            let item_gids = self.items.items().keys().map(|k| k.gid).collect();
            let mut unknown = ids.difference(&item_gids);
            Err(SqlCatalogError::UnknownItem(unknown.join(", ")).into())
        }
    }

    /// Updates item `id` in the transaction to `item_name` and `item`.
    ///
    /// Returns an error if `id` is not found.
    ///
    /// Runtime is linear with respect to the total number of items in the catalog.
    /// DO NOT call this function in a loop, use [`Self::update_items`] instead.
    pub fn update_item(&mut self, id: GlobalId, item: Item) -> Result<(), CatalogError> {
        let n = self.items.update(|k, v| {
            if k.gid == id {
                let item = item.clone();
                // Schema IDs cannot change.
                assert_eq!(item.schema_id, v.schema_id);
                let (_, new_value) = item.into_key_value();
                Some(new_value)
            } else {
                None
            }
        })?;
        assert!(n <= 1);
        if n == 1 {
            Ok(())
        } else {
            Err(SqlCatalogError::UnknownItem(id.to_string()).into())
        }
    }

    /// Updates all items with ids matching the keys of `items` in the transaction, to the
    /// corresponding value in `items`.
    ///
    /// Returns an error if any id in `items` is not found.
    ///
    /// NOTE: On error, there still may be some items updated in the transaction. It is
    /// up to the called to either abort the transaction or commit.
    pub fn update_items(&mut self, items: BTreeMap<GlobalId, Item>) -> Result<(), CatalogError> {
        let n = self.items.update(|k, v| {
            if let Some(item) = items.get(&k.gid) {
                // Schema IDs cannot change.
                assert_eq!(item.schema_id, v.schema_id);
                let (_, new_value) = item.clone().into_key_value();
                Some(new_value)
            } else {
                None
            }
        })?;
        let n = usize::try_from(n).expect("Must be positive and fit in usize");
        if n == items.len() {
            Ok(())
        } else {
            let update_ids: BTreeSet<_> = items.into_keys().collect();
            let item_ids: BTreeSet<_> = self.items.items().keys().map(|k| k.gid).collect();
            let mut unknown = update_ids.difference(&item_ids);
            Err(SqlCatalogError::UnknownItem(unknown.join(", ")).into())
        }
    }

    /// Updates role `id` in the transaction to `role`.
    ///
    /// Returns an error if `id` is not found.
    ///
    /// Runtime is linear with respect to the total number of items in the catalog.
    /// DO NOT call this function in a loop, implement and use some `Self::update_roles` instead.
    /// You should model it after [`Self::update_items`].
    pub fn update_role(&mut self, id: RoleId, role: Role) -> Result<(), CatalogError> {
        let n = self.roles.update(move |k, _v| {
            if k.id == id {
                let role = role.clone();
                let (_, new_value) = role.into_key_value();
                Some(new_value)
            } else {
                None
            }
        })?;
        assert!(n <= 1);
        if n == 1 {
            Ok(())
        } else {
            Err(SqlCatalogError::UnknownItem(id.to_string()).into())
        }
    }

    /// Updates persisted mapping from system objects to global IDs and fingerprints. Each element
    /// of `mappings` should be (old-global-id, new-system-object-mapping).
    ///
    /// Panics if provided id is not a system id.
    pub fn update_system_object_mappings(
        &mut self,
        mappings: BTreeMap<GlobalId, SystemObjectMapping>,
    ) -> Result<(), CatalogError> {
        let n = self.system_gid_mapping.update(|_k, v| {
            if let Some(mapping) = mappings.get(&GlobalId::System(v.id)) {
                let (_, new_value) = mapping.clone().into_key_value();
                Some(new_value)
            } else {
                None
            }
        })?;

        if usize::try_from(n).expect("update diff should fit into usize") != mappings.len() {
            let id_str = mappings.keys().map(|id| id.to_string()).join(",");
            return Err(SqlCatalogError::FailedBuiltinSchemaMigration(id_str).into());
        }

        Ok(())
    }

    /// Updates cluster `id` in the transaction to `cluster`.
    ///
    /// Returns an error if `id` is not found.
    ///
    /// Runtime is linear with respect to the total number of clusters in the catalog.
    /// DO NOT call this function in a loop.
    pub fn update_cluster(&mut self, id: ClusterId, cluster: Cluster) -> Result<(), CatalogError> {
        let n = self.clusters.update(|k, _v| {
            if k.id == id {
                let (_, new_value) = cluster.clone().into_key_value();
                Some(new_value)
            } else {
                None
            }
        })?;
        assert!(n <= 1);
        if n == 1 {
            Ok(())
        } else {
            Err(SqlCatalogError::UnknownCluster(id.to_string()).into())
        }
    }

    /// Updates cluster replica `replica_id` in the transaction to `replica`.
    ///
    /// Returns an error if `replica_id` is not found.
    ///
    /// Runtime is linear with respect to the total number of cluster replicas in the catalog.
    /// DO NOT call this function in a loop.
    pub fn update_cluster_replica(
        &mut self,
        replica_id: ReplicaId,
        replica: ClusterReplica,
    ) -> Result<(), CatalogError> {
        let n = self.cluster_replicas.update(|k, _v| {
            if k.id == replica_id {
                let (_, new_value) = replica.clone().into_key_value();
                Some(new_value)
            } else {
                None
            }
        })?;
        assert!(n <= 1);
        if n == 1 {
            Ok(())
        } else {
            Err(SqlCatalogError::UnknownClusterReplica(replica_id.to_string()).into())
        }
    }

    /// Updates database `id` in the transaction to `database`.
    ///
    /// Returns an error if `id` is not found.
    ///
    /// Runtime is linear with respect to the total number of databases in the catalog.
    /// DO NOT call this function in a loop.
    pub fn update_database(
        &mut self,
        id: DatabaseId,
        database: Database,
    ) -> Result<(), CatalogError> {
        let n = self.databases.update(|k, _v| {
            if id == k.id {
                let (_, new_value) = database.clone().into_key_value();
                Some(new_value)
            } else {
                None
            }
        })?;
        assert!(n <= 1);
        if n == 1 {
            Ok(())
        } else {
            Err(SqlCatalogError::UnknownDatabase(id.to_string()).into())
        }
    }

    /// Updates schema `schema_id` in the transaction to `schema`.
    ///
    /// Returns an error if `schema_id` is not found.
    ///
    /// Runtime is linear with respect to the total number of schemas in the catalog.
    /// DO NOT call this function in a loop.
    pub fn update_schema(
        &mut self,
        schema_id: SchemaId,
        schema: Schema,
    ) -> Result<(), CatalogError> {
        let n = self.schemas.update(|k, _v| {
            if schema_id == k.id {
                let schema = schema.clone();
                let (_, new_value) = schema.clone().into_key_value();
                Some(new_value)
            } else {
                None
            }
        })?;
        assert!(n <= 1);
        if n == 1 {
            Ok(())
        } else {
            Err(SqlCatalogError::UnknownSchema(schema_id.to_string()).into())
        }
    }

    /// Set persisted default privilege.
    ///
    /// DO NOT call this function in a loop, use [`Self::set_default_privileges`] instead.
    pub fn set_default_privilege(
        &mut self,
        role_id: RoleId,
        database_id: Option<DatabaseId>,
        schema_id: Option<SchemaId>,
        object_type: ObjectType,
        grantee: RoleId,
        privileges: Option<AclMode>,
    ) -> Result<(), CatalogError> {
        self.default_privileges.set(
            DefaultPrivilegesKey {
                role_id,
                database_id,
                schema_id,
                object_type,
                grantee,
            },
            privileges.map(|privileges| DefaultPrivilegesValue { privileges }),
        )?;
        Ok(())
    }

    /// Set persisted default privileges.
    pub fn set_default_privileges(
        &mut self,
        default_privileges: Vec<DefaultPrivilege>,
    ) -> Result<(), CatalogError> {
        let default_privileges = default_privileges
            .into_iter()
            .map(DurableType::into_key_value)
            .map(|(k, v)| (k, Some(v)))
            .collect();
        self.default_privileges.set_many(default_privileges)?;
        Ok(())
    }

    /// Set persisted system privilege.
    ///
    /// DO NOT call this function in a loop, use [`Self::set_system_privileges`] instead.
    pub fn set_system_privilege(
        &mut self,
        grantee: RoleId,
        grantor: RoleId,
        acl_mode: Option<AclMode>,
    ) -> Result<(), CatalogError> {
        self.system_privileges.set(
            SystemPrivilegesKey { grantee, grantor },
            acl_mode.map(|acl_mode| SystemPrivilegesValue { acl_mode }),
        )?;
        Ok(())
    }

    /// Set persisted system privileges.
    pub fn set_system_privileges(
        &mut self,
        system_privileges: Vec<MzAclItem>,
    ) -> Result<(), CatalogError> {
        let system_privileges = system_privileges
            .into_iter()
            .map(DurableType::into_key_value)
            .map(|(k, v)| (k, Some(v)))
            .collect();
        self.system_privileges.set_many(system_privileges)?;
        Ok(())
    }

    /// Set persisted setting.
    pub(crate) fn set_setting(
        &mut self,
        name: String,
        value: Option<String>,
    ) -> Result<(), CatalogError> {
        self.settings.set(
            SettingKey { name },
            value.map(|value| SettingValue { value }),
        )?;
        Ok(())
    }

    pub fn set_catalog_content_version(&mut self, version: String) -> Result<(), CatalogError> {
        self.set_setting(CATALOG_CONTENT_VERSION_KEY.to_string(), Some(version))
    }

    /// Insert persisted introspection source index.
    pub fn insert_introspection_source_indexes(
        &mut self,
        introspection_source_indexes: Vec<(ClusterId, String, GlobalId)>,
    ) -> Result<Vec<IntrospectionSourceIndex>, CatalogError> {
        let oids = self.allocate_oids(usize_to_u64(introspection_source_indexes.len()))?;
        let introspection_source_indexes: Vec<_> = introspection_source_indexes
            .into_iter()
            .zip(oids.into_iter())
            .map(
                |((cluster_id, name, index_id), oid)| IntrospectionSourceIndex {
                    cluster_id,
                    name,
                    index_id,
                    oid,
                },
            )
            .collect();

        for introspection_source_index in &introspection_source_indexes {
            let (key, value) = introspection_source_index.clone().into_key_value();
            self.introspection_sources.insert(key, value)?;
        }

        Ok(introspection_source_indexes)
    }

    /// Set persisted system object mappings.
    pub fn set_system_object_mappings(
        &mut self,
        mappings: Vec<SystemObjectMapping>,
    ) -> Result<(), CatalogError> {
        let mappings = mappings
            .into_iter()
            .map(DurableType::into_key_value)
            .map(|(k, v)| (k, Some(v)))
            .collect();
        self.system_gid_mapping.set_many(mappings)?;
        Ok(())
    }

    /// Set persisted timestamp.
    pub fn set_timestamp(
        &mut self,
        timeline: Timeline,
        ts: mz_repr::Timestamp,
    ) -> Result<(), CatalogError> {
        let timeline_timestamp = TimelineTimestamp { timeline, ts };
        let (key, value) = timeline_timestamp.into_key_value();
        self.timestamps.set(key, Some(value))?;
        Ok(())
    }

    /// Set persisted replica.
    pub fn set_replicas(&mut self, replicas: Vec<ClusterReplica>) -> Result<(), CatalogError> {
        let replicas = replicas
            .into_iter()
            .map(DurableType::into_key_value)
            .map(|(k, v)| (k, Some(v)))
            .collect();
        self.cluster_replicas.set_many(replicas)?;
        Ok(())
    }

    /// Set persisted configuration.
    pub(crate) fn set_config(
        &mut self,
        key: String,
        value: Option<u64>,
    ) -> Result<(), CatalogError> {
        match value {
            Some(value) => {
                let config = Config { key, value };
                let (key, value) = config.into_key_value();
                self.configs.set(key, Some(value))?;
            }
            None => {
                self.configs.set(ConfigKey { key }, None)?;
            }
        }
        Ok(())
    }

    /// Updates the catalog `persist_txn_tables` "config" value to
    /// match the `persist_txn_tables` "system var" value.
    ///
    /// These are mirrored so that we can toggle the flag with Launch Darkly,
    /// but use it in boot before Launch Darkly is available.
    pub fn set_persist_txn_tables(
        &mut self,
        value: PersistTxnTablesImpl,
    ) -> Result<(), CatalogError> {
        self.set_config(PERSIST_TXN_TABLES.into(), Some(u64::from(value)))?;
        Ok(())
    }

    /// Updates the catalog `system_config_synced` "config" value to true.
    pub fn set_system_config_synced_once(&mut self) -> Result<(), CatalogError> {
        self.set_config(SYSTEM_CONFIG_SYNCED_KEY.into(), Some(1))
    }

    pub fn update_comment(
        &mut self,
        object_id: CommentObjectId,
        sub_component: Option<usize>,
        comment: Option<String>,
    ) -> Result<(), CatalogError> {
        let key = CommentKey {
            object_id,
            sub_component,
        };
        let value = comment.map(|c| CommentValue { comment: c });
        self.comments.set(key, value)?;

        Ok(())
    }

    pub fn drop_comments(
        &mut self,
        object_id: CommentObjectId,
    ) -> Result<Vec<(CommentObjectId, Option<usize>, String)>, CatalogError> {
        let deleted = self.comments.delete(|k, _v| k.object_id == object_id);
        let deleted = deleted
            .into_iter()
            .map(|(k, v)| (k.object_id, k.sub_component, v.comment))
            .collect();
        Ok(deleted)
    }

    /// Upserts persisted system configuration `name` to `value`.
    pub fn upsert_system_config(&mut self, name: &str, value: String) -> Result<(), CatalogError> {
        let key = ServerConfigurationKey {
            name: name.to_string(),
        };
        let value = ServerConfigurationValue { value };
        self.system_configurations.set(key, Some(value))?;
        Ok(())
    }

    /// Removes persisted system configuration `name`.
    pub fn remove_system_config(&mut self, name: &str) {
        let key = ServerConfigurationKey {
            name: name.to_string(),
        };
        self.system_configurations
            .set(key, None)
            .expect("cannot have uniqueness violation");
    }

    /// Removes all persisted system configurations.
    pub fn clear_system_configs(&mut self) {
        self.system_configurations.delete(|_k, _v| true);
    }

    pub(crate) fn insert_config(&mut self, key: String, value: u64) -> Result<(), CatalogError> {
        match self
            .configs
            .insert(ConfigKey { key: key.clone() }, ConfigValue { value })
        {
            Ok(_) => Ok(()),
            Err(_) => Err(SqlCatalogError::ConfigAlreadyExists(key).into()),
        }
    }

    pub fn get_clusters(&self) -> impl Iterator<Item = Cluster> {
        self.clusters
            .items()
            .clone()
            .into_iter()
            .map(|(k, v)| DurableType::from_key_value(k, v))
    }

    pub fn get_cluster_replicas(&self) -> impl Iterator<Item = ClusterReplica> {
        self.cluster_replicas
            .items()
            .clone()
            .into_iter()
            .map(|(k, v)| DurableType::from_key_value(k, v))
    }

    pub fn get_databases(&self) -> impl Iterator<Item = Database> {
        self.databases
            .items()
            .clone()
            .into_iter()
            .map(|(k, v)| DurableType::from_key_value(k, v))
    }

    pub fn get_schemas(&self) -> impl Iterator<Item = Schema> {
        self.schemas
            .items()
            .clone()
            .into_iter()
            .map(|(k, v)| DurableType::from_key_value(k, v))
    }

    pub fn get_roles(&self) -> impl Iterator<Item = Role> {
        self.roles
            .items()
            .clone()
            .into_iter()
            .map(|(k, v)| DurableType::from_key_value(k, v))
    }

    pub fn get_default_privileges(&self) -> impl Iterator<Item = DefaultPrivilege> {
        self.default_privileges
            .items()
            .clone()
            .into_iter()
            .map(|(k, v)| DurableType::from_key_value(k, v))
    }

    pub fn get_system_privileges(&self) -> impl Iterator<Item = MzAclItem> {
        self.system_privileges
            .items()
            .clone()
            .into_iter()
            .map(|(k, v)| DurableType::from_key_value(k, v))
    }

    pub fn get_comments(&self) -> impl Iterator<Item = Comment> {
        self.comments
            .items()
            .clone()
            .into_iter()
            .map(|(k, v)| DurableType::from_key_value(k, v))
    }

    pub fn get_system_configurations(&self) -> impl Iterator<Item = SystemConfiguration> {
        self.system_configurations
            .items()
            .clone()
            .into_iter()
            .map(|(k, v)| DurableType::from_key_value(k, v))
    }

    pub fn get_system_items(&self) -> impl Iterator<Item = SystemObjectMapping> {
        self.system_gid_mapping
            .items()
            .clone()
            .into_iter()
            .map(|(k, v)| DurableType::from_key_value(k, v))
    }

    pub fn get_timestamp(&self, timeline: &Timeline) -> Option<mz_repr::Timestamp> {
        self.timestamps
            .get(&TimestampKey {
                id: timeline.to_string(),
            })
            .map(|value| value.ts)
    }

    pub fn get_introspection_source_indexes(
        &mut self,
        cluster_id: ClusterId,
    ) -> BTreeMap<String, (GlobalId, u32)> {
        self.introspection_sources
            .items()
            .into_iter()
            .filter(|(k, _v)| k.cluster_id == cluster_id)
            .map(|(k, v)| (k.name, (GlobalId::System(v.index_id), v.oid)))
            .collect()
    }

    pub fn get_catalog_content_version(&self) -> Option<String> {
        self.settings
            .get(&SettingKey {
                name: CATALOG_CONTENT_VERSION_KEY.to_string(),
            })
            .map(|value| value.value.clone())
    }

    // TODO(jkosh44) Can be removed after v0.92.X
    pub fn clean_up_stash_catalog(&mut self) -> Result<(), CatalogError> {
        self.configs.set(
            ConfigKey {
                key: "tombstone".to_string(),
            },
            None,
        )?;
        self.configs.set(
            ConfigKey {
                key: "catalog_kind".to_string(),
            },
            None,
        )?;
        self.system_configurations.set(
            ServerConfigurationKey {
                name: "catalog_kind".to_string(),
            },
            None,
        )?;
        Ok(())
    }

    pub(crate) fn into_parts(self) -> (TransactionBatch, &'a mut dyn DurableCatalogState) {
        let txn_batch = TransactionBatch {
            databases: self.databases.pending(),
            schemas: self.schemas.pending(),
            items: self.items.pending(),
            comments: self.comments.pending(),
            roles: self.roles.pending(),
            clusters: self.clusters.pending(),
            cluster_replicas: self.cluster_replicas.pending(),
            introspection_sources: self.introspection_sources.pending(),
            id_allocator: self.id_allocator.pending(),
            configs: self.configs.pending(),
            settings: self.settings.pending(),
            timestamps: self.timestamps.pending(),
            system_gid_mapping: self.system_gid_mapping.pending(),
            system_configurations: self.system_configurations.pending(),
            default_privileges: self.default_privileges.pending(),
            system_privileges: self.system_privileges.pending(),
            storage_metadata: self.storage_metadata.pending(),
            unfinalized_shards: self.unfinalized_shards.pending(),
            persist_txn_shard: self.persist_txn_shard.pending(),
            audit_log_updates: self.audit_log_updates,
            storage_usage_updates: self.storage_usage_updates,
        };
        (txn_batch, self.durable_catalog)
    }

    /// Commits the storage transaction to durable storage. Any error returned indicates the catalog may be
    /// in an indeterminate state and needs to be fully re-read before proceeding. In general, this
    /// must be fatal to the calling process. We do not panic/halt inside this function itself so
    /// that errors can bubble up during initialization.
    #[mz_ore::instrument(level = "debug")]
    pub async fn commit(self) -> Result<(), CatalogError> {
        let (mut txn_batch, durable_catalog) = self.into_parts();
        let TransactionBatch {
            databases,
            schemas,
            items,
            comments,
            roles,
            clusters,
            cluster_replicas,
            introspection_sources,
            id_allocator,
            configs,
            settings,
            timestamps,
            system_gid_mapping,
            system_configurations,
            default_privileges,
            system_privileges,
            storage_metadata,
            unfinalized_shards,
            persist_txn_shard,
            audit_log_updates,
            storage_usage_updates,
        } = &mut txn_batch;
        // Consolidate in memory becuase it will likely be faster than consolidating after the
        // transaction has been made durable.
        differential_dataflow::consolidation::consolidate_updates(databases);
        differential_dataflow::consolidation::consolidate_updates(schemas);
        differential_dataflow::consolidation::consolidate_updates(items);
        differential_dataflow::consolidation::consolidate_updates(comments);
        differential_dataflow::consolidation::consolidate_updates(roles);
        differential_dataflow::consolidation::consolidate_updates(clusters);
        differential_dataflow::consolidation::consolidate_updates(cluster_replicas);
        differential_dataflow::consolidation::consolidate_updates(introspection_sources);
        differential_dataflow::consolidation::consolidate_updates(id_allocator);
        differential_dataflow::consolidation::consolidate_updates(configs);
        differential_dataflow::consolidation::consolidate_updates(settings);
        differential_dataflow::consolidation::consolidate_updates(timestamps);
        differential_dataflow::consolidation::consolidate_updates(system_gid_mapping);
        differential_dataflow::consolidation::consolidate_updates(system_configurations);
        differential_dataflow::consolidation::consolidate_updates(default_privileges);
        differential_dataflow::consolidation::consolidate_updates(system_privileges);
        differential_dataflow::consolidation::consolidate_updates(storage_metadata);
        differential_dataflow::consolidation::consolidate_updates(unfinalized_shards);
        differential_dataflow::consolidation::consolidate_updates(persist_txn_shard);
        differential_dataflow::consolidation::consolidate_updates(audit_log_updates);
        differential_dataflow::consolidation::consolidate_updates(storage_usage_updates);
        durable_catalog.commit_transaction(txn_batch).await
    }
}

/// Describes a set of changes to apply as the result of a catalog transaction.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TransactionBatch {
    pub(crate) databases: Vec<(proto::DatabaseKey, proto::DatabaseValue, Diff)>,
    pub(crate) schemas: Vec<(proto::SchemaKey, proto::SchemaValue, Diff)>,
    pub(crate) items: Vec<(proto::ItemKey, proto::ItemValue, Diff)>,
    pub(crate) comments: Vec<(proto::CommentKey, proto::CommentValue, Diff)>,
    pub(crate) roles: Vec<(proto::RoleKey, proto::RoleValue, Diff)>,
    pub(crate) clusters: Vec<(proto::ClusterKey, proto::ClusterValue, Diff)>,
    pub(crate) cluster_replicas: Vec<(proto::ClusterReplicaKey, proto::ClusterReplicaValue, Diff)>,
    pub(crate) introspection_sources: Vec<(
        proto::ClusterIntrospectionSourceIndexKey,
        proto::ClusterIntrospectionSourceIndexValue,
        Diff,
    )>,
    pub(crate) id_allocator: Vec<(proto::IdAllocKey, proto::IdAllocValue, Diff)>,
    pub(crate) configs: Vec<(proto::ConfigKey, proto::ConfigValue, Diff)>,
    pub(crate) settings: Vec<(proto::SettingKey, proto::SettingValue, Diff)>,
    pub(crate) timestamps: Vec<(proto::TimestampKey, proto::TimestampValue, Diff)>,
    pub(crate) system_gid_mapping: Vec<(proto::GidMappingKey, proto::GidMappingValue, Diff)>,
    pub(crate) system_configurations: Vec<(
        proto::ServerConfigurationKey,
        proto::ServerConfigurationValue,
        Diff,
    )>,
    pub(crate) default_privileges: Vec<(
        proto::DefaultPrivilegesKey,
        proto::DefaultPrivilegesValue,
        Diff,
    )>,
    pub(crate) system_privileges: Vec<(
        proto::SystemPrivilegesKey,
        proto::SystemPrivilegesValue,
        Diff,
    )>,
    pub(crate) storage_metadata:
        Vec<(proto::StorageMetadataKey, proto::StorageMetadataValue, Diff)>,
    pub(crate) unfinalized_shards: Vec<(proto::UnfinalizedShardKey, (), Diff)>,
    pub(crate) persist_txn_shard: Vec<((), proto::PersistTxnShardValue, Diff)>,
    pub(crate) audit_log_updates: Vec<(proto::AuditLogKey, (), Diff)>,
    pub(crate) storage_usage_updates: Vec<(proto::StorageUsageKey, (), Diff)>,
}

impl TransactionBatch {
    pub fn is_empty(&self) -> bool {
        self == &Self::default()
    }
}

/// TableTransaction emulates some features of a typical SQL transaction over
/// table for a Collection.
///
/// It supports:
/// - uniqueness constraints
/// - transactional reads and writes (including read-your-writes before commit)
///
/// `K` is the primary key type. Multiple entries with the same key are disallowed.
/// `V` is the an arbitrary value type.
#[derive(Debug, PartialEq, Eq)]
struct TableTransaction<K, V> {
    initial: BTreeMap<K, V>,
    // The desired state of keys after commit. `None` means the value will be
    // deleted.
    pending: BTreeMap<K, Option<V>>,
    uniqueness_violation: fn(a: &V, b: &V) -> bool,
}

impl<K, V> TableTransaction<K, V>
where
    K: Ord + Eq + Clone,
    V: Ord + Clone,
{
    /// Create a new TableTransaction with initial data. `uniqueness_violation` is a function
    /// whether there is a uniqueness violation among two values.
    ///
    /// Internally the catalog serializes data as protobuf. All fields in a proto message are
    /// optional, which makes using them in Rust cumbersome. Generic parameters `KP` and `VP` are
    /// protobuf types which deserialize to `K` and `V` that a [`TableTransaction`] is generic
    /// over.
    fn new<KP, VP>(
        initial: BTreeMap<KP, VP>,
        uniqueness_violation: fn(a: &V, b: &V) -> bool,
    ) -> Result<Self, TryFromProtoError>
    where
        K: RustType<KP>,
        V: RustType<VP>,
    {
        let initial = initial
            .into_iter()
            .map(RustType::from_proto)
            .collect::<Result<_, _>>()?;

        Ok(Self {
            initial,
            pending: BTreeMap::new(),
            uniqueness_violation,
        })
    }

    /// Consumes and returns the pending changes and their diffs. `Diff` is
    /// guaranteed to be 1 or -1.
    fn pending<KP, VP>(self) -> Vec<(KP, VP, Diff)>
    where
        K: RustType<KP>,
        V: RustType<VP>,
    {
        soft_assert_no_log!(self.verify().is_ok());
        // Pending describes the desired final state for some keys. K,V pairs should be
        // retracted if they already exist and were deleted or are being updated.
        self.pending
            .into_iter()
            .map(|(k, v)| match self.initial.get(&k) {
                Some(initial_v) => {
                    let mut diffs = vec![(k.clone(), initial_v.clone(), -1)];
                    if let Some(v) = v {
                        diffs.push((k, v, 1));
                    }
                    diffs
                }
                None => {
                    if let Some(v) = v {
                        vec![(k, v, 1)]
                    } else {
                        vec![]
                    }
                }
            })
            .flatten()
            .map(|(key, val, diff)| (key.into_proto(), val.into_proto(), diff))
            .collect()
    }

    fn verify(&self) -> Result<(), DurableCatalogError> {
        // Compare each value to each other value and ensure they are unique.
        let items = self.items();
        for (i, vi) in items.values().enumerate() {
            for (j, vj) in items.values().enumerate() {
                if i != j && (self.uniqueness_violation)(vi, vj) {
                    return Err(DurableCatalogError::UniquenessViolation);
                }
            }
        }
        Ok(())
    }

    /// Iterates over the items viewable in the current transaction in arbitrary
    /// order and applies `f` on all key, value pairs.
    fn for_values<F: FnMut(&K, &V)>(&self, mut f: F) {
        let mut seen = BTreeSet::new();
        for (k, v) in self.pending.iter() {
            seen.insert(k);
            // Deleted items don't exist so shouldn't be visited, but still suppress
            // visiting the key later.
            if let Some(v) = v {
                f(k, v);
            }
        }
        for (k, v) in self.initial.iter() {
            // Add on initial items that don't have updates.
            if !seen.contains(k) {
                f(k, v);
            }
        }
    }

    /// Returns the current value of `k`.
    fn get(&self, k: &K) -> Option<&V> {
        if let Some(v) = self.pending.get(k) {
            v.as_ref()
        } else if let Some(v) = self.initial.get(k) {
            Some(v)
        } else {
            None
        }
    }

    /// Returns the items viewable in the current transaction.
    fn items(&self) -> BTreeMap<K, V> {
        let mut items = BTreeMap::new();
        self.for_values(|k, v| {
            items.insert(k.clone(), v.clone());
        });
        items
    }

    /// Iterates over the items viewable in the current transaction, and provides a
    /// map where additional pending items can be inserted, which will be appended
    /// to current pending items. Does not verify uniqueness.
    fn for_values_mut<F: FnMut(&mut BTreeMap<K, Option<V>>, &K, &V)>(&mut self, mut f: F) {
        let mut pending = BTreeMap::new();
        self.for_values(|k, v| f(&mut pending, k, v));
        self.pending.extend(pending);
    }

    /// Inserts a new k,v pair.
    ///
    /// Returns an error if the uniqueness check failed or the key already exists.
    fn insert(&mut self, k: K, v: V) -> Result<(), DurableCatalogError> {
        let mut violation = None;
        self.for_values(|for_k, for_v| {
            if &k == for_k {
                violation = Some(DurableCatalogError::DuplicateKey);
            }
            if (self.uniqueness_violation)(for_v, &v) {
                violation = Some(DurableCatalogError::UniquenessViolation);
            }
        });
        if let Some(violation) = violation {
            return Err(violation.into());
        }
        self.pending.insert(k, Some(v));
        soft_assert_no_log!(self.verify().is_ok());
        Ok(())
    }

    /// Updates k, v pairs. `f` is a function that can return `Some(V)` if the
    /// value should be updated, otherwise `None`. Returns the number of changed
    /// entries.
    ///
    /// Returns an error if the uniqueness check failed.
    fn update<F: Fn(&K, &V) -> Option<V>>(&mut self, f: F) -> Result<Diff, DurableCatalogError> {
        let mut changed = 0;
        // Keep a copy of pending in case of uniqueness violation.
        let pending = self.pending.clone();
        self.for_values_mut(|p, k, v| {
            if let Some(next) = f(k, v) {
                changed += 1;
                p.insert(k.clone(), Some(next));
            }
        });
        // Check for uniqueness violation.
        if let Err(err) = self.verify() {
            self.pending = pending;
            Err(err)
        } else {
            Ok(changed)
        }
    }

    /// Set the value for a key. Returns the previous entry if the key existed,
    /// otherwise None.
    ///
    /// Returns an error if the uniqueness check failed.
    ///
    /// DO NOT call this function in a loop, use [`Self::set_many`] instead.
    fn set(&mut self, k: K, v: Option<V>) -> Result<Option<V>, DurableCatalogError> {
        // Save the pending value for the key so we can restore it in case of
        // uniqueness violation.
        let restore = self.pending.get(&k).cloned();

        let prev = match self.pending.entry(k.clone()) {
            // key hasn't been set in this txn yet.
            Entry::Vacant(e) => {
                let initial = self.initial.get(&k);
                if initial != v.as_ref() {
                    // Provided value and initial value are different. Set key.
                    e.insert(v);
                }
                // Return the initial txn's value of k.
                initial.cloned()
            }
            // key has been set in this txn. Set it and return the previous
            // pending value.
            Entry::Occupied(mut e) => e.insert(v),
        };

        // Check for uniqueness violation.
        if let Err(err) = self.verify() {
            // Revert self.pending to the state it was in before calling this
            // function.
            match restore {
                Some(v) => {
                    self.pending.insert(k, v);
                }
                None => {
                    self.pending.remove(&k);
                }
            }
            Err(err)
        } else {
            Ok(prev)
        }
    }

    /// Set the values for many keys. Returns the previous entry for each key if the key existed,
    /// otherwise None.
    ///
    /// Returns an error if any uniqueness check failed.
    fn set_many(
        &mut self,
        kvs: BTreeMap<K, Option<V>>,
    ) -> Result<BTreeMap<K, Option<V>>, DurableCatalogError> {
        let mut prevs = BTreeMap::new();
        let mut restores = BTreeMap::new();

        for (k, v) in kvs {
            // Save the pending value for the key so we can restore it in case of
            // uniqueness violation.
            let restore = self.pending.get(&k).cloned();
            restores.insert(k.clone(), restore);

            let prev = match self.pending.entry(k.clone()) {
                // key hasn't been set in this txn yet.
                Entry::Vacant(e) => {
                    let initial = self.initial.get(&k);
                    if initial != v.as_ref() {
                        // Provided value and initial value are different. Set key.
                        e.insert(v);
                    }
                    // Return the initial txn's value of k.
                    initial.cloned()
                }
                // key has been set in this txn. Set it and return the previous
                // pending value.
                Entry::Occupied(mut e) => e.insert(v),
            };
            prevs.insert(k, prev);
        }

        // Check for uniqueness violation.
        if let Err(err) = self.verify() {
            for (k, restore) in restores {
                // Revert self.pending to the state it was in before calling this
                // function.
                match restore {
                    Some(v) => {
                        self.pending.insert(k, v);
                    }
                    None => {
                        self.pending.remove(&k);
                    }
                }
            }
            Err(err)
        } else {
            Ok(prevs)
        }
    }

    /// Deletes items for which `f` returns true. Returns the keys and values of
    /// the deleted entries.
    fn delete<F: Fn(&K, &V) -> bool>(&mut self, f: F) -> Vec<(K, V)> {
        let mut deleted = Vec::new();
        self.for_values_mut(|p, k, v| {
            if f(k, v) {
                deleted.push((k.clone(), v.clone()));
                p.insert(k.clone(), None);
            }
        });
        soft_assert_no_log!(self.verify().is_ok());
        deleted
    }
}

#[mz_ore::test]
fn test_table_transaction_simple() {
    fn uniqueness_violation(a: &String, b: &String) -> bool {
        a == b
    }
    let mut table = TableTransaction::new(
        BTreeMap::from([(1i64.to_le_bytes().to_vec(), "a".to_string())]),
        uniqueness_violation,
    )
    .unwrap();

    table
        .insert(2i64.to_le_bytes().to_vec(), "b".to_string())
        .unwrap();
    table
        .insert(3i64.to_le_bytes().to_vec(), "c".to_string())
        .unwrap();
}

#[mz_ore::test]
fn test_table_transaction() {
    fn uniqueness_violation(a: &String, b: &String) -> bool {
        a == b
    }
    let mut table: BTreeMap<Vec<u8>, String> = BTreeMap::new();

    fn commit(table: &mut BTreeMap<Vec<u8>, String>, mut pending: Vec<(Vec<u8>, String, i64)>) {
        // Sort by diff so that we process retractions first.
        pending.sort_by(|a, b| a.2.cmp(&b.2));
        for (k, v, diff) in pending {
            if diff == -1 {
                let prev = table.remove(&k);
                assert_eq!(prev, Some(v));
            } else if diff == 1 {
                let prev = table.insert(k, v);
                assert_eq!(prev, None);
            } else {
                panic!("unexpected diff: {diff}");
            }
        }
    }

    table.insert(1i64.to_le_bytes().to_vec(), "v1".to_string());
    table.insert(2i64.to_le_bytes().to_vec(), "v2".to_string());
    let mut table_txn = TableTransaction::new(table.clone(), uniqueness_violation).unwrap();
    assert_eq!(table_txn.items(), table);
    assert_eq!(table_txn.delete(|_k, _v| false).len(), 0);
    assert_eq!(table_txn.delete(|_k, v| v == "v2").len(), 1);
    assert_eq!(
        table_txn.items(),
        BTreeMap::from([(1i64.to_le_bytes().to_vec(), "v1".to_string())])
    );
    assert_eq!(
        table_txn.update(|_k, _v| Some("v3".to_string())).unwrap(),
        1
    );

    // Uniqueness violation.
    table_txn
        .insert(3i64.to_le_bytes().to_vec(), "v3".to_string())
        .unwrap_err();

    table_txn
        .insert(3i64.to_le_bytes().to_vec(), "v4".to_string())
        .unwrap();
    assert_eq!(
        table_txn.items(),
        BTreeMap::from([
            (1i64.to_le_bytes().to_vec(), "v3".to_string()),
            (3i64.to_le_bytes().to_vec(), "v4".to_string()),
        ])
    );
    let err = table_txn
        .update(|_k, _v| Some("v1".to_string()))
        .unwrap_err();
    assert!(
        matches!(err, DurableCatalogError::UniquenessViolation),
        "unexpected err: {err:?}"
    );
    let pending = table_txn.pending();
    assert_eq!(
        pending,
        vec![
            (1i64.to_le_bytes().to_vec(), "v1".to_string(), -1),
            (1i64.to_le_bytes().to_vec(), "v3".to_string(), 1),
            (2i64.to_le_bytes().to_vec(), "v2".to_string(), -1),
            (3i64.to_le_bytes().to_vec(), "v4".to_string(), 1),
        ]
    );
    commit(&mut table, pending);
    assert_eq!(
        table,
        BTreeMap::from([
            (1i64.to_le_bytes().to_vec(), "v3".to_string()),
            (3i64.to_le_bytes().to_vec(), "v4".to_string())
        ])
    );

    let mut table_txn = TableTransaction::new(table.clone(), uniqueness_violation).unwrap();
    // Deleting then creating an item that has a uniqueness violation should work.
    assert_eq!(table_txn.delete(|k, _v| k == &1i64.to_le_bytes()).len(), 1);
    table_txn
        .insert(1i64.to_le_bytes().to_vec(), "v3".to_string())
        .unwrap();
    // Uniqueness violation in value.
    table_txn
        .insert(5i64.to_le_bytes().to_vec(), "v3".to_string())
        .unwrap_err();
    // Key already exists, expect error.
    table_txn
        .insert(1i64.to_le_bytes().to_vec(), "v5".to_string())
        .unwrap_err();
    assert_eq!(table_txn.delete(|k, _v| k == &1i64.to_le_bytes()).len(), 1);
    // Both the inserts work now because the key and uniqueness violation are gone.
    table_txn
        .insert(5i64.to_le_bytes().to_vec(), "v3".to_string())
        .unwrap();
    table_txn
        .insert(1i64.to_le_bytes().to_vec(), "v5".to_string())
        .unwrap();
    let pending = table_txn.pending();
    assert_eq!(
        pending,
        vec![
            (1i64.to_le_bytes().to_vec(), "v3".to_string(), -1),
            (1i64.to_le_bytes().to_vec(), "v5".to_string(), 1),
            (5i64.to_le_bytes().to_vec(), "v3".to_string(), 1),
        ]
    );
    commit(&mut table, pending);
    assert_eq!(
        table,
        BTreeMap::from([
            (1i64.to_le_bytes().to_vec(), "v5".to_string()),
            (3i64.to_le_bytes().to_vec(), "v4".to_string()),
            (5i64.to_le_bytes().to_vec(), "v3".to_string()),
        ])
    );

    let mut table_txn = TableTransaction::new(table.clone(), uniqueness_violation).unwrap();
    assert_eq!(table_txn.delete(|_k, _v| true).len(), 3);
    table_txn
        .insert(1i64.to_le_bytes().to_vec(), "v1".to_string())
        .unwrap();

    commit(&mut table, table_txn.pending());
    assert_eq!(
        table,
        BTreeMap::from([(1i64.to_le_bytes().to_vec(), "v1".to_string()),])
    );

    let mut table_txn = TableTransaction::new(table.clone(), uniqueness_violation).unwrap();
    assert_eq!(table_txn.delete(|_k, _v| true).len(), 1);
    table_txn
        .insert(1i64.to_le_bytes().to_vec(), "v2".to_string())
        .unwrap();
    commit(&mut table, table_txn.pending());
    assert_eq!(
        table,
        BTreeMap::from([(1i64.to_le_bytes().to_vec(), "v2".to_string()),])
    );

    // Verify we don't try to delete v3 or v4 during commit.
    let mut table_txn = TableTransaction::new(table.clone(), uniqueness_violation).unwrap();
    assert_eq!(table_txn.delete(|_k, _v| true).len(), 1);
    table_txn
        .insert(1i64.to_le_bytes().to_vec(), "v3".to_string())
        .unwrap();
    table_txn
        .insert(1i64.to_le_bytes().to_vec(), "v4".to_string())
        .unwrap_err();
    assert_eq!(table_txn.delete(|_k, _v| true).len(), 1);
    table_txn
        .insert(1i64.to_le_bytes().to_vec(), "v5".to_string())
        .unwrap();
    commit(&mut table, table_txn.pending());
    assert_eq!(
        table.clone().into_iter().collect::<Vec<_>>(),
        vec![(1i64.to_le_bytes().to_vec(), "v5".to_string())]
    );

    // Test `set`.
    let mut table_txn = TableTransaction::new(table.clone(), uniqueness_violation).unwrap();
    // Uniqueness violation.
    table_txn
        .set(2i64.to_le_bytes().to_vec(), Some("v5".to_string()))
        .unwrap_err();
    table_txn
        .set(3i64.to_le_bytes().to_vec(), Some("v6".to_string()))
        .unwrap();
    table_txn.set(2i64.to_le_bytes().to_vec(), None).unwrap();
    table_txn.set(1i64.to_le_bytes().to_vec(), None).unwrap();
    let pending = table_txn.pending();
    assert_eq!(
        pending,
        vec![
            (1i64.to_le_bytes().to_vec(), "v5".to_string(), -1),
            (3i64.to_le_bytes().to_vec(), "v6".to_string(), 1),
        ]
    );
    commit(&mut table, pending);
    assert_eq!(
        table,
        BTreeMap::from([(3i64.to_le_bytes().to_vec(), "v6".to_string())])
    );

    // Duplicate `set`.
    let mut table_txn = TableTransaction::new(table.clone(), uniqueness_violation).unwrap();
    table_txn
        .set(3i64.to_le_bytes().to_vec(), Some("v6".to_string()))
        .unwrap();
    let pending = table_txn.pending::<Vec<u8>, String>();
    assert!(pending.is_empty());

    // Test `set_many`.
    let mut table_txn = TableTransaction::new(table.clone(), uniqueness_violation).unwrap();
    // Uniqueness violation.
    table_txn
        .set_many(BTreeMap::from([
            (1i64.to_le_bytes().to_vec(), Some("v6".to_string())),
            (42i64.to_le_bytes().to_vec(), Some("v1".to_string())),
        ]))
        .unwrap_err();
    table_txn
        .set_many(BTreeMap::from([
            (1i64.to_le_bytes().to_vec(), Some("v6".to_string())),
            (3i64.to_le_bytes().to_vec(), Some("v1".to_string())),
        ]))
        .unwrap();
    table_txn
        .set_many(BTreeMap::from([
            (42i64.to_le_bytes().to_vec(), Some("v7".to_string())),
            (3i64.to_le_bytes().to_vec(), None),
        ]))
        .unwrap();
    let pending = table_txn.pending();
    assert_eq!(
        pending,
        vec![
            (1i64.to_le_bytes().to_vec(), "v6".to_string(), 1),
            (3i64.to_le_bytes().to_vec(), "v6".to_string(), -1),
            (42i64.to_le_bytes().to_vec(), "v7".to_string(), 1),
        ]
    );
    commit(&mut table, pending);
    assert_eq!(
        table,
        BTreeMap::from([
            (1i64.to_le_bytes().to_vec(), "v6".to_string()),
            (42i64.to_le_bytes().to_vec(), "v7".to_string())
        ])
    );

    // Duplicate `set_many`.
    let mut table_txn = TableTransaction::new(table.clone(), uniqueness_violation).unwrap();
    table_txn
        .set_many(BTreeMap::from([
            (1i64.to_le_bytes().to_vec(), Some("v6".to_string())),
            (42i64.to_le_bytes().to_vec(), Some("v7".to_string())),
        ]))
        .unwrap();
    let pending = table_txn.pending::<Vec<u8>, String>();
    assert!(pending.is_empty());
}
