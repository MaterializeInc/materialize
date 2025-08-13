// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::time::Duration;

use anyhow::anyhow;
use derivative::Derivative;
use itertools::Itertools;
use mz_audit_log::VersionedEvent;
use mz_compute_client::logging::{ComputeLog, DifferentialLog, LogVariant, TimelyLog};
use mz_controller_types::{ClusterId, ReplicaId};
use mz_ore::cast::{u64_to_usize, usize_to_u64};
use mz_ore::collections::{CollectionExt, HashSet};
use mz_ore::now::SYSTEM_TIME;
use mz_ore::vec::VecExt;
use mz_ore::{soft_assert_no_log, soft_assert_or_log};
use mz_persist_types::ShardId;
use mz_pgrepr::oid::FIRST_USER_OID;
use mz_proto::{RustType, TryFromProtoError};
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem};
use mz_repr::network_policy_id::NetworkPolicyId;
use mz_repr::role_id::RoleId;
use mz_repr::{CatalogItemId, Diff, GlobalId, RelationVersion};
use mz_sql::catalog::{
    CatalogError as SqlCatalogError, CatalogItemType, ObjectType, RoleAttributes, RoleMembership,
    RoleVars,
};
use mz_sql::names::{CommentObjectId, DatabaseId, ResolvedDatabaseSpecifier, SchemaId};
use mz_sql::plan::NetworkPolicyRule;
use mz_sql_parser::ast::QualifiedReplica;
use mz_storage_client::controller::StorageTxn;
use mz_storage_types::controller::StorageError;

use crate::builtin::BuiltinLog;
use crate::durable::initialize::{
    ENABLE_0DT_DEPLOYMENT_PANIC_AFTER_TIMEOUT, SYSTEM_CONFIG_SYNCED_KEY,
    WITH_0DT_DEPLOYMENT_DDL_CHECK_INTERVAL, WITH_0DT_DEPLOYMENT_MAX_WAIT,
};
use crate::durable::objects::serialization::proto;
use crate::durable::objects::{
    AuditLogKey, Cluster, ClusterConfig, ClusterIntrospectionSourceIndexKey,
    ClusterIntrospectionSourceIndexValue, ClusterKey, ClusterReplica, ClusterReplicaKey,
    ClusterReplicaValue, ClusterValue, CommentKey, CommentValue, Config, ConfigKey, ConfigValue,
    Database, DatabaseKey, DatabaseValue, DefaultPrivilegesKey, DefaultPrivilegesValue,
    DurableType, GidMappingKey, GidMappingValue, IdAllocKey, IdAllocValue,
    IntrospectionSourceIndex, Item, ItemKey, ItemValue, NetworkPolicyKey, NetworkPolicyValue,
    ReplicaConfig, Role, RoleKey, RoleValue, Schema, SchemaKey, SchemaValue,
    ServerConfigurationKey, ServerConfigurationValue, SettingKey, SettingValue, SourceReference,
    SourceReferencesKey, SourceReferencesValue, StorageCollectionMetadataKey,
    StorageCollectionMetadataValue, SystemObjectDescription, SystemObjectMapping,
    SystemPrivilegesKey, SystemPrivilegesValue, TxnWalShardValue, UnfinalizedShardKey,
};
use crate::durable::{
    AUDIT_LOG_ID_ALLOC_KEY, BUILTIN_MIGRATION_SHARD_KEY, CATALOG_CONTENT_VERSION_KEY, CatalogError,
    DATABASE_ID_ALLOC_KEY, DefaultPrivilege, DurableCatalogError, DurableCatalogState,
    EXPRESSION_CACHE_SHARD_KEY, NetworkPolicy, OID_ALLOC_KEY, SCHEMA_ID_ALLOC_KEY,
    STORAGE_USAGE_ID_ALLOC_KEY, SYSTEM_CLUSTER_ID_ALLOC_KEY, SYSTEM_ITEM_ALLOC_KEY,
    SYSTEM_REPLICA_ID_ALLOC_KEY, Snapshot, SystemConfiguration, USER_ITEM_ALLOC_KEY,
    USER_NETWORK_POLICY_ID_ALLOC_KEY, USER_REPLICA_ID_ALLOC_KEY, USER_ROLE_ID_ALLOC_KEY,
};
use crate::memory::objects::{StateDiff, StateUpdate, StateUpdateKind};

type Timestamp = u64;

/// A [`Transaction`] batches multiple catalog operations together and commits them atomically.
/// An operation also logically groups multiple catalog updates together.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct Transaction<'a> {
    #[derivative(Debug = "ignore")]
    #[derivative(PartialEq = "ignore")]
    durable_catalog: &'a mut dyn DurableCatalogState,
    databases: TableTransaction<DatabaseKey, DatabaseValue>,
    schemas: TableTransaction<SchemaKey, SchemaValue>,
    items: TableTransaction<ItemKey, ItemValue>,
    comments: TableTransaction<CommentKey, CommentValue>,
    roles: TableTransaction<RoleKey, RoleValue>,
    role_auth: TableTransaction<RoleAuthKey, RoleAuthValue>,
    clusters: TableTransaction<ClusterKey, ClusterValue>,
    cluster_replicas: TableTransaction<ClusterReplicaKey, ClusterReplicaValue>,
    introspection_sources:
        TableTransaction<ClusterIntrospectionSourceIndexKey, ClusterIntrospectionSourceIndexValue>,
    id_allocator: TableTransaction<IdAllocKey, IdAllocValue>,
    configs: TableTransaction<ConfigKey, ConfigValue>,
    settings: TableTransaction<SettingKey, SettingValue>,
    system_gid_mapping: TableTransaction<GidMappingKey, GidMappingValue>,
    system_configurations: TableTransaction<ServerConfigurationKey, ServerConfigurationValue>,
    default_privileges: TableTransaction<DefaultPrivilegesKey, DefaultPrivilegesValue>,
    source_references: TableTransaction<SourceReferencesKey, SourceReferencesValue>,
    system_privileges: TableTransaction<SystemPrivilegesKey, SystemPrivilegesValue>,
    network_policies: TableTransaction<NetworkPolicyKey, NetworkPolicyValue>,
    storage_collection_metadata:
        TableTransaction<StorageCollectionMetadataKey, StorageCollectionMetadataValue>,
    unfinalized_shards: TableTransaction<UnfinalizedShardKey, ()>,
    txn_wal_shard: TableTransaction<(), TxnWalShardValue>,
    // Don't make this a table transaction so that it's not read into the
    // in-memory cache.
    audit_log_updates: Vec<(AuditLogKey, Diff, Timestamp)>,
    /// The upper of `durable_catalog` at the start of the transaction.
    upper: mz_repr::Timestamp,
    /// The ID of the current operation of this transaction.
    op_id: Timestamp,
}

impl<'a> Transaction<'a> {
    pub fn new(
        durable_catalog: &'a mut dyn DurableCatalogState,
        Snapshot {
            databases,
            schemas,
            roles,
            role_auth,
            items,
            comments,
            clusters,
            network_policies,
            cluster_replicas,
            introspection_sources,
            id_allocator,
            configs,
            settings,
            source_references,
            system_object_mappings,
            system_configurations,
            default_privileges,
            system_privileges,
            storage_collection_metadata,
            unfinalized_shards,
            txn_wal_shard,
        }: Snapshot,
        upper: mz_repr::Timestamp,
    ) -> Result<Transaction<'a>, CatalogError> {
        Ok(Transaction {
            durable_catalog,
            databases: TableTransaction::new_with_uniqueness_fn(
                databases,
                |a: &DatabaseValue, b| a.name == b.name,
            )?,
            schemas: TableTransaction::new_with_uniqueness_fn(schemas, |a: &SchemaValue, b| {
                a.database_id == b.database_id && a.name == b.name
            })?,
            items: TableTransaction::new_with_uniqueness_fn(items, |a: &ItemValue, b| {
                a.schema_id == b.schema_id && a.name == b.name && {
                    // `item_type` is slow, only compute if needed.
                    let a_type = a.item_type();
                    let b_type = b.item_type();
                    (a_type != CatalogItemType::Type && b_type != CatalogItemType::Type)
                        || (a_type == CatalogItemType::Type && b_type.conflicts_with_type())
                        || (b_type == CatalogItemType::Type && a_type.conflicts_with_type())
                }
            })?,
            comments: TableTransaction::new(comments)?,
            roles: TableTransaction::new_with_uniqueness_fn(roles, |a: &RoleValue, b| {
                a.name == b.name
            })?,
            role_auth: TableTransaction::new(role_auth)?,
            clusters: TableTransaction::new_with_uniqueness_fn(clusters, |a: &ClusterValue, b| {
                a.name == b.name
            })?,
            network_policies: TableTransaction::new_with_uniqueness_fn(
                network_policies,
                |a: &NetworkPolicyValue, b| a.name == b.name,
            )?,
            cluster_replicas: TableTransaction::new_with_uniqueness_fn(
                cluster_replicas,
                |a: &ClusterReplicaValue, b| a.cluster_id == b.cluster_id && a.name == b.name,
            )?,
            introspection_sources: TableTransaction::new(introspection_sources)?,
            id_allocator: TableTransaction::new(id_allocator)?,
            configs: TableTransaction::new(configs)?,
            settings: TableTransaction::new(settings)?,
            source_references: TableTransaction::new(source_references)?,
            system_gid_mapping: TableTransaction::new(system_object_mappings)?,
            system_configurations: TableTransaction::new(system_configurations)?,
            default_privileges: TableTransaction::new(default_privileges)?,
            system_privileges: TableTransaction::new(system_privileges)?,
            storage_collection_metadata: TableTransaction::new(storage_collection_metadata)?,
            unfinalized_shards: TableTransaction::new(unfinalized_shards)?,
            // Uniqueness violations for this value occur at the key rather than
            // the value (the key is the unit struct `()` so this is a singleton
            // value).
            txn_wal_shard: TableTransaction::new(txn_wal_shard)?,
            audit_log_updates: Vec::new(),
            upper,
            op_id: 0,
        })
    }

    pub fn get_item(&self, id: &CatalogItemId) -> Option<Item> {
        let key = ItemKey { id: *id };
        self.items
            .get(&key)
            .map(|v| DurableType::from_key_value(key, v.clone()))
    }

    pub fn get_items(&self) -> impl Iterator<Item = Item> + use<> {
        self.items
            .items()
            .into_iter()
            .map(|(k, v)| DurableType::from_key_value(k.clone(), v.clone()))
            .sorted_by_key(|Item { id, .. }| *id)
    }

    pub fn insert_audit_log_event(&mut self, event: VersionedEvent) {
        self.insert_audit_log_events([event]);
    }

    pub fn insert_audit_log_events(&mut self, events: impl IntoIterator<Item = VersionedEvent>) {
        let events = events
            .into_iter()
            .map(|event| (AuditLogKey { event }, Diff::ONE, self.op_id));
        self.audit_log_updates.extend(events);
    }

    pub fn insert_user_database(
        &mut self,
        database_name: &str,
        owner_id: RoleId,
        privileges: Vec<MzAclItem>,
        temporary_oids: &HashSet<u32>,
    ) -> Result<(DatabaseId, u32), CatalogError> {
        let id = self.get_and_increment_id(DATABASE_ID_ALLOC_KEY.to_string())?;
        let id = DatabaseId::User(id);
        let oid = self.allocate_oid(temporary_oids)?;
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
            self.op_id,
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
        temporary_oids: &HashSet<u32>,
    ) -> Result<(SchemaId, u32), CatalogError> {
        let id = self.get_and_increment_id(SCHEMA_ID_ALLOC_KEY.to_string())?;
        let id = SchemaId::User(id);
        let oid = self.allocate_oid(temporary_oids)?;
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

    pub fn insert_system_schema(
        &mut self,
        schema_id: u64,
        schema_name: &str,
        owner_id: RoleId,
        privileges: Vec<MzAclItem>,
        oid: u32,
    ) -> Result<(), CatalogError> {
        let id = SchemaId::System(schema_id);
        self.insert_schema(id, None, schema_name.to_string(), owner_id, privileges, oid)
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
            self.op_id,
        ) {
            Ok(_) => Ok(()),
            Err(_) => Err(SqlCatalogError::SchemaAlreadyExists(schema_name).into()),
        }
    }

    pub fn insert_builtin_role(
        &mut self,
        id: RoleId,
        name: String,
        attributes: RoleAttributes,
        membership: RoleMembership,
        vars: RoleVars,
        oid: u32,
    ) -> Result<RoleId, CatalogError> {
        soft_assert_or_log!(id.is_builtin(), "ID {id:?} is not builtin");
        self.insert_role(id, name, attributes, membership, vars, oid)?;
        Ok(id)
    }

    pub fn insert_user_role(
        &mut self,
        name: String,
        attributes: RoleAttributes,
        membership: RoleMembership,
        vars: RoleVars,
        temporary_oids: &HashSet<u32>,
    ) -> Result<(RoleId, u32), CatalogError> {
        let id = self.get_and_increment_id(USER_ROLE_ID_ALLOC_KEY.to_string())?;
        let id = RoleId::User(id);
        let oid = self.allocate_oid(temporary_oids)?;
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
        if let Some(ref password) = attributes.password {
            let hash =
                mz_auth::hash::scram256_hash(password).expect("password hash should be valid");
            match self.role_auth.insert(
                RoleAuthKey { role_id: id },
                RoleAuthValue {
                    password_hash: Some(hash),
                    updated_at: SYSTEM_TIME(),
                },
                self.op_id,
            ) {
                Ok(_) => {}
                Err(_) => {
                    return Err(SqlCatalogError::RoleAlreadyExists(name).into());
                }
            }
        }

        match self.roles.insert(
            RoleKey { id },
            RoleValue {
                name: name.clone(),
                attributes,
                membership,
                vars,
                oid,
            },
            self.op_id,
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
        introspection_source_indexes: Vec<(&'static BuiltinLog, CatalogItemId, GlobalId)>,
        owner_id: RoleId,
        privileges: Vec<MzAclItem>,
        config: ClusterConfig,
        temporary_oids: &HashSet<u32>,
    ) -> Result<(), CatalogError> {
        self.insert_cluster(
            cluster_id,
            cluster_name,
            introspection_source_indexes,
            owner_id,
            privileges,
            config,
            temporary_oids,
        )
    }

    /// Panics if any introspection source id is not a system id
    pub fn insert_system_cluster(
        &mut self,
        cluster_name: &str,
        introspection_source_indexes: Vec<(&'static BuiltinLog, CatalogItemId, GlobalId)>,
        privileges: Vec<MzAclItem>,
        owner_id: RoleId,
        config: ClusterConfig,
        temporary_oids: &HashSet<u32>,
    ) -> Result<(), CatalogError> {
        let cluster_id = self.get_and_increment_id(SYSTEM_CLUSTER_ID_ALLOC_KEY.to_string())?;
        let cluster_id = ClusterId::system(cluster_id).ok_or(SqlCatalogError::IdExhaustion)?;
        self.insert_cluster(
            cluster_id,
            cluster_name,
            introspection_source_indexes,
            owner_id,
            privileges,
            config,
            temporary_oids,
        )
    }

    fn insert_cluster(
        &mut self,
        cluster_id: ClusterId,
        cluster_name: &str,
        introspection_source_indexes: Vec<(&'static BuiltinLog, CatalogItemId, GlobalId)>,
        owner_id: RoleId,
        privileges: Vec<MzAclItem>,
        config: ClusterConfig,
        temporary_oids: &HashSet<u32>,
    ) -> Result<(), CatalogError> {
        if let Err(_) = self.clusters.insert(
            ClusterKey { id: cluster_id },
            ClusterValue {
                name: cluster_name.to_string(),
                owner_id,
                privileges,
                config,
            },
            self.op_id,
        ) {
            return Err(SqlCatalogError::ClusterAlreadyExists(cluster_name.to_owned()).into());
        };

        let amount = usize_to_u64(introspection_source_indexes.len());
        let oids = self.allocate_oids(amount, temporary_oids)?;
        let introspection_source_indexes: Vec<_> = introspection_source_indexes
            .into_iter()
            .zip_eq(oids)
            .map(|((builtin, item_id, index_id), oid)| (builtin, item_id, index_id, oid))
            .collect();
        for (builtin, item_id, index_id, oid) in introspection_source_indexes {
            let introspection_source_index = IntrospectionSourceIndex {
                cluster_id,
                name: builtin.name.to_string(),
                item_id,
                index_id,
                oid,
            };
            let (key, value) = introspection_source_index.into_key_value();
            self.introspection_sources
                .insert(key, value, self.op_id)
                .expect("no uniqueness violation");
        }

        Ok(())
    }

    pub fn rename_cluster(
        &mut self,
        cluster_id: ClusterId,
        cluster_name: &str,
        cluster_to_name: &str,
    ) -> Result<(), CatalogError> {
        let key = ClusterKey { id: cluster_id };

        match self.clusters.update(
            |k, v| {
                if *k == key {
                    let mut value = v.clone();
                    value.name = cluster_to_name.to_string();
                    Some(value)
                } else {
                    None
                }
            },
            self.op_id,
        )? {
            Diff::ZERO => Err(SqlCatalogError::UnknownCluster(cluster_name.to_string()).into()),
            Diff::ONE => Ok(()),
            n => panic!(
                "Expected to update single cluster {cluster_name} ({cluster_id}), updated {n}"
            ),
        }
    }

    pub fn rename_cluster_replica(
        &mut self,
        replica_id: ReplicaId,
        replica_name: &QualifiedReplica,
        replica_to_name: &str,
    ) -> Result<(), CatalogError> {
        let key = ClusterReplicaKey { id: replica_id };

        match self.cluster_replicas.update(
            |k, v| {
                if *k == key {
                    let mut value = v.clone();
                    value.name = replica_to_name.to_string();
                    Some(value)
                } else {
                    None
                }
            },
            self.op_id,
        )? {
            Diff::ZERO => {
                Err(SqlCatalogError::UnknownClusterReplica(replica_name.to_string()).into())
            }
            Diff::ONE => Ok(()),
            n => panic!(
                "Expected to update single cluster replica {replica_name} ({replica_id}), updated {n}"
            ),
        }
    }

    pub fn insert_cluster_replica(
        &mut self,
        cluster_id: ClusterId,
        replica_name: &str,
        config: ReplicaConfig,
        owner_id: RoleId,
    ) -> Result<ReplicaId, CatalogError> {
        let replica_id = match cluster_id {
            ClusterId::System(_) => self.allocate_system_replica_id()?,
            ClusterId::User(_) => self.allocate_user_replica_id()?,
        };
        self.insert_cluster_replica_with_id(
            cluster_id,
            replica_id,
            replica_name,
            config,
            owner_id,
        )?;
        Ok(replica_id)
    }

    pub(crate) fn insert_cluster_replica_with_id(
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
            self.op_id,
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

    pub fn insert_user_network_policy(
        &mut self,
        name: String,
        rules: Vec<NetworkPolicyRule>,
        privileges: Vec<MzAclItem>,
        owner_id: RoleId,
        temporary_oids: &HashSet<u32>,
    ) -> Result<NetworkPolicyId, CatalogError> {
        let oid = self.allocate_oid(temporary_oids)?;
        let id = self.get_and_increment_id(USER_NETWORK_POLICY_ID_ALLOC_KEY.to_string())?;
        let id = NetworkPolicyId::User(id);
        self.insert_network_policy(id, name, rules, privileges, owner_id, oid)
    }

    pub fn insert_network_policy(
        &mut self,
        id: NetworkPolicyId,
        name: String,
        rules: Vec<NetworkPolicyRule>,
        privileges: Vec<MzAclItem>,
        owner_id: RoleId,
        oid: u32,
    ) -> Result<NetworkPolicyId, CatalogError> {
        match self.network_policies.insert(
            NetworkPolicyKey { id },
            NetworkPolicyValue {
                name: name.clone(),
                rules,
                privileges,
                owner_id,
                oid,
            },
            self.op_id,
        ) {
            Ok(_) => Ok(id),
            Err(_) => Err(SqlCatalogError::NetworkPolicyAlreadyExists(name).into()),
        }
    }

    /// Updates persisted information about persisted introspection source
    /// indexes.
    ///
    /// Panics if provided id is not a system id.
    pub fn update_introspection_source_index_gids(
        &mut self,
        mappings: impl Iterator<
            Item = (
                ClusterId,
                impl Iterator<Item = (String, CatalogItemId, GlobalId, u32)>,
            ),
        >,
    ) -> Result<(), CatalogError> {
        for (cluster_id, updates) in mappings {
            for (name, item_id, index_id, oid) in updates {
                let introspection_source_index = IntrospectionSourceIndex {
                    cluster_id,
                    name,
                    item_id,
                    index_id,
                    oid,
                };
                let (key, value) = introspection_source_index.into_key_value();

                let prev = self
                    .introspection_sources
                    .set(key, Some(value), self.op_id)?;
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
        id: CatalogItemId,
        global_id: GlobalId,
        schema_id: SchemaId,
        item_name: &str,
        create_sql: String,
        owner_id: RoleId,
        privileges: Vec<MzAclItem>,
        temporary_oids: &HashSet<u32>,
        versions: BTreeMap<RelationVersion, GlobalId>,
    ) -> Result<u32, CatalogError> {
        let oid = self.allocate_oid(temporary_oids)?;
        self.insert_item(
            id, oid, global_id, schema_id, item_name, create_sql, owner_id, privileges, versions,
        )?;
        Ok(oid)
    }

    pub fn insert_item(
        &mut self,
        id: CatalogItemId,
        oid: u32,
        global_id: GlobalId,
        schema_id: SchemaId,
        item_name: &str,
        create_sql: String,
        owner_id: RoleId,
        privileges: Vec<MzAclItem>,
        extra_versions: BTreeMap<RelationVersion, GlobalId>,
    ) -> Result<(), CatalogError> {
        match self.items.insert(
            ItemKey { id },
            ItemValue {
                schema_id,
                name: item_name.to_string(),
                create_sql,
                owner_id,
                privileges,
                oid,
                global_id,
                extra_versions,
            },
            self.op_id,
        ) {
            Ok(_) => Ok(()),
            Err(_) => Err(SqlCatalogError::ItemAlreadyExists(id, item_name.to_owned()).into()),
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
        assert!(
            key != SYSTEM_ITEM_ALLOC_KEY || !self.durable_catalog.is_bootstrap_complete(),
            "system item IDs cannot be allocated outside of bootstrap"
        );

        let current_id = self
            .id_allocator
            .items()
            .get(&IdAllocKey { name: key.clone() })
            .unwrap_or_else(|| panic!("{key} id allocator missing"))
            .next_id;
        let next_id = current_id
            .checked_add(amount)
            .ok_or(SqlCatalogError::IdExhaustion)?;
        let prev = self.id_allocator.set(
            IdAllocKey { name: key },
            Some(IdAllocValue { next_id }),
            self.op_id,
        )?;
        assert_eq!(
            prev,
            Some(IdAllocValue {
                next_id: current_id
            })
        );
        Ok((current_id..next_id).collect())
    }

    pub fn allocate_system_item_ids(
        &mut self,
        amount: u64,
    ) -> Result<Vec<(CatalogItemId, GlobalId)>, CatalogError> {
        assert!(
            !self.durable_catalog.is_bootstrap_complete(),
            "we can only allocate system item IDs during bootstrap"
        );
        Ok(self
            .get_and_increment_id_by(SYSTEM_ITEM_ALLOC_KEY.to_string(), amount)?
            .into_iter()
            // TODO(alter_table): Use separate ID allocators.
            .map(|x| (CatalogItemId::System(x), GlobalId::System(x)))
            .collect())
    }

    /// Allocates an ID for an introspection source index. These IDs are deterministically derived
    /// from the `cluster_id` and `log_variant`.
    ///
    /// Introspection source indexes are a special edge case of items. They are considered system
    /// items, but they are the only system item that can be created by the user at any time. All
    /// other system items can only be created by the system during the startup of an upgrade.
    ///
    /// Furthermore, all other system item IDs are allocated deterministically in the same order
    /// during startup. Therefore, all read-only `environmentd` processes during an upgrade will
    /// allocate the same system IDs to the same items, and due to the way catalog fencing works,
    /// only one of them can successfully write the IDs down to the catalog. This removes the need
    /// for `environmentd` processes to coordinate system IDs allocated during read-only mode.
    ///
    /// Since introspection IDs can be allocated at any time, read-only instances would either need
    /// to coordinate across processes when allocating a new ID or allocate them deterministically.
    /// We opted to allocate the IDs deterministically to avoid the overhead of coordination.
    ///
    /// Introspection source index IDs are 64 bit integers, with the following format (not to
    /// scale):
    ///
    /// -------------------------------------------------------------
    /// | Cluster ID Variant | Cluster ID Inner Value | Log Variant |
    /// |--------------------|------------------------|-------------|
    /// |       8-bits       |         48-bits        |   8-bits    |
    /// -------------------------------------------------------------
    ///
    /// Cluster ID Variant:      A unique number indicating the variant of cluster the index belongs
    ///                          to.
    /// Cluster ID Inner Value:  A per variant unique number indicating the cluster the index
    ///                          belongs to.
    /// Log Variant:             A unique number indicating the log variant this index is on.
    pub fn allocate_introspection_source_index_id(
        cluster_id: &ClusterId,
        log_variant: LogVariant,
    ) -> (CatalogItemId, GlobalId) {
        let cluster_variant: u8 = match cluster_id {
            ClusterId::System(_) => 1,
            ClusterId::User(_) => 2,
        };
        let cluster_id: u64 = cluster_id.inner_id();
        const CLUSTER_ID_MASK: u64 = 0xFFFF << 48;
        assert_eq!(
            CLUSTER_ID_MASK & cluster_id,
            0,
            "invalid cluster ID: {cluster_id}"
        );
        let log_variant: u8 = match log_variant {
            LogVariant::Timely(TimelyLog::Operates) => 1,
            LogVariant::Timely(TimelyLog::Channels) => 2,
            LogVariant::Timely(TimelyLog::Elapsed) => 3,
            LogVariant::Timely(TimelyLog::Histogram) => 4,
            LogVariant::Timely(TimelyLog::Addresses) => 5,
            LogVariant::Timely(TimelyLog::Parks) => 6,
            LogVariant::Timely(TimelyLog::MessagesSent) => 7,
            LogVariant::Timely(TimelyLog::MessagesReceived) => 8,
            LogVariant::Timely(TimelyLog::Reachability) => 9,
            LogVariant::Timely(TimelyLog::BatchesSent) => 10,
            LogVariant::Timely(TimelyLog::BatchesReceived) => 11,
            LogVariant::Differential(DifferentialLog::ArrangementBatches) => 12,
            LogVariant::Differential(DifferentialLog::ArrangementRecords) => 13,
            LogVariant::Differential(DifferentialLog::Sharing) => 14,
            LogVariant::Differential(DifferentialLog::BatcherRecords) => 15,
            LogVariant::Differential(DifferentialLog::BatcherSize) => 16,
            LogVariant::Differential(DifferentialLog::BatcherCapacity) => 17,
            LogVariant::Differential(DifferentialLog::BatcherAllocations) => 18,
            LogVariant::Compute(ComputeLog::DataflowCurrent) => 19,
            LogVariant::Compute(ComputeLog::FrontierCurrent) => 20,
            LogVariant::Compute(ComputeLog::PeekCurrent) => 21,
            LogVariant::Compute(ComputeLog::PeekDuration) => 22,
            LogVariant::Compute(ComputeLog::ImportFrontierCurrent) => 23,
            LogVariant::Compute(ComputeLog::ArrangementHeapSize) => 24,
            LogVariant::Compute(ComputeLog::ArrangementHeapCapacity) => 25,
            LogVariant::Compute(ComputeLog::ArrangementHeapAllocations) => 26,
            LogVariant::Compute(ComputeLog::ShutdownDuration) => 27,
            LogVariant::Compute(ComputeLog::ErrorCount) => 28,
            LogVariant::Compute(ComputeLog::HydrationTime) => 29,
            LogVariant::Compute(ComputeLog::LirMapping) => 30,
            LogVariant::Compute(ComputeLog::DataflowGlobal) => 31,
        };

        let mut id: u64 = u64::from(cluster_variant) << 56;
        id |= cluster_id << 8;
        id |= u64::from(log_variant);

        (
            CatalogItemId::IntrospectionSourceIndex(id),
            GlobalId::IntrospectionSourceIndex(id),
        )
    }

    pub fn allocate_user_item_ids(
        &mut self,
        amount: u64,
    ) -> Result<Vec<(CatalogItemId, GlobalId)>, CatalogError> {
        Ok(self
            .get_and_increment_id_by(USER_ITEM_ALLOC_KEY.to_string(), amount)?
            .into_iter()
            // TODO(alter_table): Use separate ID allocators.
            .map(|x| (CatalogItemId::User(x), GlobalId::User(x)))
            .collect())
    }

    pub fn allocate_user_replica_id(&mut self) -> Result<ReplicaId, CatalogError> {
        let id = self.get_and_increment_id(USER_REPLICA_ID_ALLOC_KEY.to_string())?;
        Ok(ReplicaId::User(id))
    }

    pub fn allocate_system_replica_id(&mut self) -> Result<ReplicaId, CatalogError> {
        let id = self.get_and_increment_id(SYSTEM_REPLICA_ID_ALLOC_KEY.to_string())?;
        Ok(ReplicaId::System(id))
    }

    pub fn allocate_audit_log_id(&mut self) -> Result<u64, CatalogError> {
        self.get_and_increment_id(AUDIT_LOG_ID_ALLOC_KEY.to_string())
    }

    pub fn allocate_storage_usage_ids(&mut self) -> Result<u64, CatalogError> {
        self.get_and_increment_id(STORAGE_USAGE_ID_ALLOC_KEY.to_string())
    }

    /// Allocates `amount` OIDs. OIDs can be recycled if they aren't currently assigned to any
    /// object.
    #[mz_ore::instrument]
    fn allocate_oids(
        &mut self,
        amount: u64,
        temporary_oids: &HashSet<u32>,
    ) -> Result<Vec<u32>, CatalogError> {
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
            self.databases.len()
                + self.schemas.len()
                + self.roles.len()
                + self.items.len()
                + self.introspection_sources.len()
                + temporary_oids.len(),
        );
        self.databases.for_values(|_, value| {
            allocated_oids.insert(value.oid);
        });
        self.schemas.for_values(|_, value| {
            allocated_oids.insert(value.oid);
        });
        self.roles.for_values(|_, value| {
            allocated_oids.insert(value.oid);
        });
        self.items.for_values(|_, value| {
            allocated_oids.insert(value.oid);
        });
        self.introspection_sources.for_values(|_, value| {
            allocated_oids.insert(value.oid);
        });

        let is_allocated = |oid| allocated_oids.contains(&oid) || temporary_oids.contains(&oid);

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
            if !is_allocated(current_oid.0) {
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
            self.op_id,
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
    pub fn allocate_oid(&mut self, temporary_oids: &HashSet<u32>) -> Result<u32, CatalogError> {
        self.allocate_oids(1, temporary_oids)
            .map(|oids| oids.into_element())
    }

    pub(crate) fn insert_id_allocator(
        &mut self,
        name: String,
        next_id: u64,
    ) -> Result<(), CatalogError> {
        match self.id_allocator.insert(
            IdAllocKey { name: name.clone() },
            IdAllocValue { next_id },
            self.op_id,
        ) {
            Ok(_) => Ok(()),
            Err(_) => Err(SqlCatalogError::IdAllocatorAlreadyExists(name).into()),
        }
    }

    /// Removes the database `id` from the transaction.
    ///
    /// Returns an error if `id` is not found.
    ///
    /// Runtime is linear with respect to the total number of databases in the catalog.
    /// DO NOT call this function in a loop, use [`Self::remove_databases`] instead.
    pub fn remove_database(&mut self, id: &DatabaseId) -> Result<(), CatalogError> {
        let prev = self
            .databases
            .set(DatabaseKey { id: *id }, None, self.op_id)?;
        if prev.is_some() {
            Ok(())
        } else {
            Err(SqlCatalogError::UnknownDatabase(id.to_string()).into())
        }
    }

    /// Removes all databases in `databases` from the transaction.
    ///
    /// Returns an error if any id in `databases` is not found.
    ///
    /// NOTE: On error, there still may be some databases removed from the transaction. It
    /// is up to the caller to either abort the transaction or commit.
    pub fn remove_databases(
        &mut self,
        databases: &BTreeSet<DatabaseId>,
    ) -> Result<(), CatalogError> {
        if databases.is_empty() {
            return Ok(());
        }

        let to_remove = databases
            .iter()
            .map(|id| (DatabaseKey { id: *id }, None))
            .collect();
        let mut prev = self.databases.set_many(to_remove, self.op_id)?;
        prev.retain(|_k, val| val.is_none());

        if !prev.is_empty() {
            let err = prev.keys().map(|k| k.id.to_string()).join(", ");
            return Err(SqlCatalogError::UnknownDatabase(err).into());
        }

        Ok(())
    }

    /// Removes the schema identified by `database_id` and `schema_id` from the transaction.
    ///
    /// Returns an error if `(database_id, schema_id)` is not found.
    ///
    /// Runtime is linear with respect to the total number of schemas in the catalog.
    /// DO NOT call this function in a loop, use [`Self::remove_schemas`] instead.
    pub fn remove_schema(
        &mut self,
        database_id: &Option<DatabaseId>,
        schema_id: &SchemaId,
    ) -> Result<(), CatalogError> {
        let prev = self
            .schemas
            .set(SchemaKey { id: *schema_id }, None, self.op_id)?;
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

    /// Removes all schemas in `schemas` from the transaction.
    ///
    /// Returns an error if any id in `schemas` is not found.
    ///
    /// NOTE: On error, there still may be some schemas removed from the transaction. It
    /// is up to the caller to either abort the transaction or commit.
    pub fn remove_schemas(
        &mut self,
        schemas: &BTreeMap<SchemaId, ResolvedDatabaseSpecifier>,
    ) -> Result<(), CatalogError> {
        if schemas.is_empty() {
            return Ok(());
        }

        let to_remove = schemas
            .iter()
            .map(|(schema_id, _)| (SchemaKey { id: *schema_id }, None))
            .collect();
        let mut prev = self.schemas.set_many(to_remove, self.op_id)?;
        prev.retain(|_k, v| v.is_none());

        if !prev.is_empty() {
            let err = prev
                .keys()
                .map(|k| {
                    let db_spec = schemas.get(&k.id).expect("should_exist");
                    let db_name = match db_spec {
                        ResolvedDatabaseSpecifier::Id(id) => format!("{id}."),
                        ResolvedDatabaseSpecifier::Ambient => "".to_string(),
                    };
                    format!("{}.{}", db_name, k.id)
                })
                .join(", ");

            return Err(SqlCatalogError::UnknownSchema(err).into());
        }

        Ok(())
    }

    pub fn remove_source_references(
        &mut self,
        source_id: CatalogItemId,
    ) -> Result<(), CatalogError> {
        let deleted = self
            .source_references
            .delete_by_key(SourceReferencesKey { source_id }, self.op_id)
            .is_some();
        if deleted {
            Ok(())
        } else {
            Err(SqlCatalogError::UnknownItem(source_id.to_string()).into())
        }
    }

    /// Removes all user roles in `roles` from the transaction.
    ///
    /// Returns an error if any id in `roles` is not found.
    ///
    /// NOTE: On error, there still may be some roles removed from the transaction. It
    /// is up to the caller to either abort the transaction or commit.
    pub fn remove_user_roles(&mut self, roles: &BTreeSet<RoleId>) -> Result<(), CatalogError> {
        assert!(
            roles.iter().all(|id| id.is_user()),
            "cannot delete non-user roles"
        );
        self.remove_roles(roles)
    }

    /// Removes all roles in `roles` from the transaction.
    ///
    /// Returns an error if any id in `roles` is not found.
    ///
    /// NOTE: On error, there still may be some roles removed from the transaction. It
    /// is up to the caller to either abort the transaction or commit.
    pub fn remove_roles(&mut self, roles: &BTreeSet<RoleId>) -> Result<(), CatalogError> {
        if roles.is_empty() {
            return Ok(());
        }

        let to_remove_keys = roles
            .iter()
            .map(|role_id| RoleKey { id: *role_id })
            .collect::<Vec<_>>();

        let to_remove_roles = to_remove_keys
            .iter()
            .map(|role_key| (role_key.clone(), None))
            .collect();

        let mut prev = self.roles.set_many(to_remove_roles, self.op_id)?;

        let to_remove_role_auth = to_remove_keys
            .iter()
            .map(|role_key| {
                (
                    RoleAuthKey {
                        role_id: role_key.id,
                    },
                    None,
                )
            })
            .collect();

        let mut role_auth_prev = self.role_auth.set_many(to_remove_role_auth, self.op_id)?;

        prev.retain(|_k, v| v.is_none());
        if !prev.is_empty() {
            let err = prev.keys().map(|k| k.id.to_string()).join(", ");
            return Err(SqlCatalogError::UnknownRole(err).into());
        }

        role_auth_prev.retain(|_k, v| v.is_none());
        // The reason we don't to the same check as above is that the role auth table
        // is not required to have all roles in the role table.

        Ok(())
    }

    /// Removes all cluster in `clusters` from the transaction.
    ///
    /// Returns an error if any id in `clusters` is not found.
    ///
    /// NOTE: On error, there still may be some clusters removed from the transaction. It is up to
    /// the caller to either abort the transaction or commit.
    pub fn remove_clusters(&mut self, clusters: &BTreeSet<ClusterId>) -> Result<(), CatalogError> {
        if clusters.is_empty() {
            return Ok(());
        }

        let to_remove = clusters
            .iter()
            .map(|cluster_id| (ClusterKey { id: *cluster_id }, None))
            .collect();
        let mut prev = self.clusters.set_many(to_remove, self.op_id)?;

        prev.retain(|_k, v| v.is_none());
        if !prev.is_empty() {
            let err = prev.keys().map(|k| k.id.to_string()).join(", ");
            return Err(SqlCatalogError::UnknownCluster(err).into());
        }

        // Cascade delete introspection sources and cluster replicas.
        //
        // TODO(benesch): this doesn't seem right. Cascade deletions should
        // be entirely the domain of the higher catalog layer, not the
        // storage layer.
        self.cluster_replicas
            .delete(|_k, v| clusters.contains(&v.cluster_id), self.op_id);
        self.introspection_sources
            .delete(|k, _v| clusters.contains(&k.cluster_id), self.op_id);

        Ok(())
    }

    /// Removes the cluster replica `id` from the transaction.
    ///
    /// Returns an error if `id` is not found.
    ///
    /// Runtime is linear with respect to the total number of cluster replicas in the catalog.
    /// DO NOT call this function in a loop, use [`Self::remove_cluster_replicas`] instead.
    pub fn remove_cluster_replica(&mut self, id: ReplicaId) -> Result<(), CatalogError> {
        let deleted = self
            .cluster_replicas
            .delete_by_key(ClusterReplicaKey { id }, self.op_id)
            .is_some();
        if deleted {
            Ok(())
        } else {
            Err(SqlCatalogError::UnknownClusterReplica(id.to_string()).into())
        }
    }

    /// Removes all cluster replicas in `replicas` from the transaction.
    ///
    /// Returns an error if any id in `replicas` is not found.
    ///
    /// NOTE: On error, there still may be some cluster replicas removed from the transaction. It
    /// is up to the caller to either abort the transaction or commit.
    pub fn remove_cluster_replicas(
        &mut self,
        replicas: &BTreeSet<ReplicaId>,
    ) -> Result<(), CatalogError> {
        if replicas.is_empty() {
            return Ok(());
        }

        let to_remove = replicas
            .iter()
            .map(|replica_id| (ClusterReplicaKey { id: *replica_id }, None))
            .collect();
        let mut prev = self.cluster_replicas.set_many(to_remove, self.op_id)?;

        prev.retain(|_k, v| v.is_none());
        if !prev.is_empty() {
            let err = prev.keys().map(|k| k.id.to_string()).join(", ");
            return Err(SqlCatalogError::UnknownClusterReplica(err).into());
        }

        Ok(())
    }

    /// Removes item `id` from the transaction.
    ///
    /// Returns an error if `id` is not found.
    ///
    /// Runtime is linear with respect to the total number of items in the catalog.
    /// DO NOT call this function in a loop, use [`Self::remove_items`] instead.
    pub fn remove_item(&mut self, id: CatalogItemId) -> Result<(), CatalogError> {
        let prev = self.items.set(ItemKey { id }, None, self.op_id)?;
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
    /// up to the caller to either abort the transaction or commit.
    pub fn remove_items(&mut self, ids: &BTreeSet<CatalogItemId>) -> Result<(), CatalogError> {
        if ids.is_empty() {
            return Ok(());
        }

        let ks: Vec<_> = ids.clone().into_iter().map(|id| ItemKey { id }).collect();
        let n = self.items.delete_by_keys(ks, self.op_id).len();
        if n == ids.len() {
            Ok(())
        } else {
            let item_ids = self.items.items().keys().map(|k| k.id).collect();
            let mut unknown = ids.difference(&item_ids);
            Err(SqlCatalogError::UnknownItem(unknown.join(", ")).into())
        }
    }

    /// Removes all system object mappings in `descriptions` from the transaction.
    ///
    /// Returns an error if any description in `descriptions` is not found.
    ///
    /// NOTE: On error, there still may be some items removed from the transaction. It is
    /// up to the caller to either abort the transaction or commit.
    pub fn remove_system_object_mappings(
        &mut self,
        descriptions: BTreeSet<SystemObjectDescription>,
    ) -> Result<(), CatalogError> {
        if descriptions.is_empty() {
            return Ok(());
        }

        let ks: Vec<_> = descriptions
            .clone()
            .into_iter()
            .map(|desc| GidMappingKey {
                schema_name: desc.schema_name,
                object_type: desc.object_type,
                object_name: desc.object_name,
            })
            .collect();
        let n = self.system_gid_mapping.delete_by_keys(ks, self.op_id).len();

        if n == descriptions.len() {
            Ok(())
        } else {
            let item_descriptions = self
                .system_gid_mapping
                .items()
                .keys()
                .map(|k| SystemObjectDescription {
                    schema_name: k.schema_name.clone(),
                    object_type: k.object_type.clone(),
                    object_name: k.object_name.clone(),
                })
                .collect();
            let mut unknown = descriptions.difference(&item_descriptions).map(|desc| {
                format!(
                    "{} {}.{}",
                    desc.object_type, desc.schema_name, desc.object_name
                )
            });
            Err(SqlCatalogError::UnknownItem(unknown.join(", ")).into())
        }
    }

    /// Removes all introspection source indexes in `indexes` from the transaction.
    ///
    /// Returns an error if any index in `indexes` is not found.
    ///
    /// NOTE: On error, there still may be some indexes removed from the transaction. It is
    /// up to the caller to either abort the transaction or commit.
    pub fn remove_introspection_source_indexes(
        &mut self,
        introspection_source_indexes: BTreeSet<(ClusterId, String)>,
    ) -> Result<(), CatalogError> {
        if introspection_source_indexes.is_empty() {
            return Ok(());
        }

        let ks: Vec<_> = introspection_source_indexes
            .clone()
            .into_iter()
            .map(|(cluster_id, name)| ClusterIntrospectionSourceIndexKey { cluster_id, name })
            .collect();
        let n = self
            .introspection_sources
            .delete_by_keys(ks, self.op_id)
            .len();
        if n == introspection_source_indexes.len() {
            Ok(())
        } else {
            let txn_indexes = self
                .introspection_sources
                .items()
                .keys()
                .map(|k| (k.cluster_id, k.name.clone()))
                .collect();
            let mut unknown = introspection_source_indexes
                .difference(&txn_indexes)
                .map(|(cluster_id, name)| format!("{cluster_id} {name}"));
            Err(SqlCatalogError::UnknownItem(unknown.join(", ")).into())
        }
    }

    /// Updates item `id` in the transaction to `item_name` and `item`.
    ///
    /// Returns an error if `id` is not found.
    ///
    /// Runtime is linear with respect to the total number of items in the catalog.
    /// DO NOT call this function in a loop, use [`Self::update_items`] instead.
    pub fn update_item(&mut self, id: CatalogItemId, item: Item) -> Result<(), CatalogError> {
        let updated =
            self.items
                .update_by_key(ItemKey { id }, item.into_key_value().1, self.op_id)?;
        if updated {
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
    /// up to the caller to either abort the transaction or commit.
    pub fn update_items(
        &mut self,
        items: BTreeMap<CatalogItemId, Item>,
    ) -> Result<(), CatalogError> {
        if items.is_empty() {
            return Ok(());
        }

        let update_ids: BTreeSet<_> = items.keys().cloned().collect();
        let kvs: Vec<_> = items
            .clone()
            .into_iter()
            .map(|(id, item)| (ItemKey { id }, item.into_key_value().1))
            .collect();
        let n = self.items.update_by_keys(kvs, self.op_id)?;
        let n = usize::try_from(n.into_inner()).expect("Must be positive and fit in usize");
        if n == update_ids.len() {
            Ok(())
        } else {
            let item_ids: BTreeSet<_> = self.items.items().keys().map(|k| k.id).collect();
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
        let key = RoleKey { id };
        if self.roles.get(&key).is_some() {
            let auth_key = RoleAuthKey { role_id: id };

            if let Some(ref password) = role.attributes.password {
                let hash =
                    mz_auth::hash::scram256_hash(password).expect("password hash should be valid");
                let value = RoleAuthValue {
                    password_hash: Some(hash),
                    updated_at: SYSTEM_TIME(),
                };

                if self.role_auth.get(&auth_key).is_some() {
                    self.role_auth
                        .update_by_key(auth_key.clone(), value, self.op_id)?;
                } else {
                    self.role_auth.insert(auth_key.clone(), value, self.op_id)?;
                }
            } else if self.role_auth.get(&auth_key).is_some() {
                // If the role is being updated to not have a password, we need to
                // remove the password hash from the role_auth catalog.
                let value = RoleAuthValue {
                    password_hash: None,
                    updated_at: SYSTEM_TIME(),
                };

                self.role_auth
                    .update_by_key(auth_key.clone(), value, self.op_id)?;
            }

            self.roles
                .update_by_key(key, role.into_key_value().1, self.op_id)?;

            Ok(())
        } else {
            Err(SqlCatalogError::UnknownRole(id.to_string()).into())
        }
    }

    /// Updates all [`Role`]s with ids matching the keys of `roles` in the transaction, to the
    /// corresponding value in `roles`.
    ///
    /// This function does *not* write role_authentication information to the catalog.
    /// It is purely for updating the role itself.
    ///
    /// Returns an error if any id in `roles` is not found.
    ///
    /// NOTE: On error, there still may be some roles updated in the transaction. It is
    /// up to the caller to either abort the transaction or commit.
    pub fn update_roles_without_auth(
        &mut self,
        roles: BTreeMap<RoleId, Role>,
    ) -> Result<(), CatalogError> {
        if roles.is_empty() {
            return Ok(());
        }

        let update_role_ids: BTreeSet<_> = roles.keys().cloned().collect();
        let kvs: Vec<_> = roles
            .into_iter()
            .map(|(id, role)| (RoleKey { id }, role.into_key_value().1))
            .collect();
        let n = self.roles.update_by_keys(kvs, self.op_id)?;
        let n = usize::try_from(n.into_inner()).expect("Must be positive and fit in usize");

        if n == update_role_ids.len() {
            Ok(())
        } else {
            let role_ids: BTreeSet<_> = self.roles.items().keys().map(|k| k.id).collect();
            let mut unknown = update_role_ids.difference(&role_ids);
            Err(SqlCatalogError::UnknownRole(unknown.join(", ")).into())
        }
    }

    /// Updates persisted mapping from system objects to global IDs and fingerprints. Each element
    /// of `mappings` should be (old-global-id, new-system-object-mapping).
    ///
    /// Panics if provided id is not a system id.
    pub fn update_system_object_mappings(
        &mut self,
        mappings: BTreeMap<CatalogItemId, SystemObjectMapping>,
    ) -> Result<(), CatalogError> {
        if mappings.is_empty() {
            return Ok(());
        }

        let n = self.system_gid_mapping.update(
            |_k, v| {
                if let Some(mapping) = mappings.get(&CatalogItemId::from(v.catalog_id)) {
                    let (_, new_value) = mapping.clone().into_key_value();
                    Some(new_value)
                } else {
                    None
                }
            },
            self.op_id,
        )?;

        if usize::try_from(n.into_inner()).expect("update diff should fit into usize")
            != mappings.len()
        {
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
        let updated = self.clusters.update_by_key(
            ClusterKey { id },
            cluster.into_key_value().1,
            self.op_id,
        )?;
        if updated {
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
        let updated = self.cluster_replicas.update_by_key(
            ClusterReplicaKey { id: replica_id },
            replica.into_key_value().1,
            self.op_id,
        )?;
        if updated {
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
        let updated = self.databases.update_by_key(
            DatabaseKey { id },
            database.into_key_value().1,
            self.op_id,
        )?;
        if updated {
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
        let updated = self.schemas.update_by_key(
            SchemaKey { id: schema_id },
            schema.into_key_value().1,
            self.op_id,
        )?;
        if updated {
            Ok(())
        } else {
            Err(SqlCatalogError::UnknownSchema(schema_id.to_string()).into())
        }
    }

    /// Updates `network_policy_id` in the transaction to `network policy`.
    ///
    /// Returns an error if `id` is not found.
    ///
    /// Runtime is linear with respect to the total number of databases in the catalog.
    /// DO NOT call this function in a loop.
    pub fn update_network_policy(
        &mut self,
        id: NetworkPolicyId,
        network_policy: NetworkPolicy,
    ) -> Result<(), CatalogError> {
        let updated = self.network_policies.update_by_key(
            NetworkPolicyKey { id },
            network_policy.into_key_value().1,
            self.op_id,
        )?;
        if updated {
            Ok(())
        } else {
            Err(SqlCatalogError::UnknownNetworkPolicy(id.to_string()).into())
        }
    }
    /// Removes all network policies in `network policies` from the transaction.
    ///
    /// Returns an error if any id in `network policy` is not found.
    ///
    /// NOTE: On error, there still may be some roles removed from the transaction. It
    /// is up to the caller to either abort the transaction or commit.
    pub fn remove_network_policies(
        &mut self,
        network_policies: &BTreeSet<NetworkPolicyId>,
    ) -> Result<(), CatalogError> {
        if network_policies.is_empty() {
            return Ok(());
        }

        let to_remove = network_policies
            .iter()
            .map(|policy_id| (NetworkPolicyKey { id: *policy_id }, None))
            .collect();
        let mut prev = self.network_policies.set_many(to_remove, self.op_id)?;
        assert!(
            prev.iter().all(|(k, _)| k.id.is_user()),
            "cannot delete non-user network policy"
        );

        prev.retain(|_k, v| v.is_none());
        if !prev.is_empty() {
            let err = prev.keys().map(|k| k.id.to_string()).join(", ");
            return Err(SqlCatalogError::UnknownNetworkPolicy(err).into());
        }

        Ok(())
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
            self.op_id,
        )?;
        Ok(())
    }

    /// Set persisted default privileges.
    pub fn set_default_privileges(
        &mut self,
        default_privileges: Vec<DefaultPrivilege>,
    ) -> Result<(), CatalogError> {
        if default_privileges.is_empty() {
            return Ok(());
        }

        let default_privileges = default_privileges
            .into_iter()
            .map(DurableType::into_key_value)
            .map(|(k, v)| (k, Some(v)))
            .collect();
        self.default_privileges
            .set_many(default_privileges, self.op_id)?;
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
            self.op_id,
        )?;
        Ok(())
    }

    /// Set persisted system privileges.
    pub fn set_system_privileges(
        &mut self,
        system_privileges: Vec<MzAclItem>,
    ) -> Result<(), CatalogError> {
        if system_privileges.is_empty() {
            return Ok(());
        }

        let system_privileges = system_privileges
            .into_iter()
            .map(DurableType::into_key_value)
            .map(|(k, v)| (k, Some(v)))
            .collect();
        self.system_privileges
            .set_many(system_privileges, self.op_id)?;
        Ok(())
    }

    /// Set persisted setting.
    pub fn set_setting(&mut self, name: String, value: Option<String>) -> Result<(), CatalogError> {
        self.settings.set(
            SettingKey { name },
            value.map(|value| SettingValue { value }),
            self.op_id,
        )?;
        Ok(())
    }

    pub fn set_catalog_content_version(&mut self, version: String) -> Result<(), CatalogError> {
        self.set_setting(CATALOG_CONTENT_VERSION_KEY.to_string(), Some(version))
    }

    /// Insert persisted introspection source index.
    pub fn insert_introspection_source_indexes(
        &mut self,
        introspection_source_indexes: Vec<(ClusterId, String, CatalogItemId, GlobalId)>,
        temporary_oids: &HashSet<u32>,
    ) -> Result<(), CatalogError> {
        if introspection_source_indexes.is_empty() {
            return Ok(());
        }

        let amount = usize_to_u64(introspection_source_indexes.len());
        let oids = self.allocate_oids(amount, temporary_oids)?;
        let introspection_source_indexes: Vec<_> = introspection_source_indexes
            .into_iter()
            .zip_eq(oids)
            .map(
                |((cluster_id, name, item_id, index_id), oid)| IntrospectionSourceIndex {
                    cluster_id,
                    name,
                    item_id,
                    index_id,
                    oid,
                },
            )
            .collect();

        for introspection_source_index in introspection_source_indexes {
            let (key, value) = introspection_source_index.into_key_value();
            self.introspection_sources.insert(key, value, self.op_id)?;
        }

        Ok(())
    }

    /// Set persisted system object mappings.
    pub fn set_system_object_mappings(
        &mut self,
        mappings: Vec<SystemObjectMapping>,
    ) -> Result<(), CatalogError> {
        if mappings.is_empty() {
            return Ok(());
        }

        let mappings = mappings
            .into_iter()
            .map(DurableType::into_key_value)
            .map(|(k, v)| (k, Some(v)))
            .collect();
        self.system_gid_mapping.set_many(mappings, self.op_id)?;
        Ok(())
    }

    /// Set persisted replica.
    pub fn set_replicas(&mut self, replicas: Vec<ClusterReplica>) -> Result<(), CatalogError> {
        if replicas.is_empty() {
            return Ok(());
        }

        let replicas = replicas
            .into_iter()
            .map(DurableType::into_key_value)
            .map(|(k, v)| (k, Some(v)))
            .collect();
        self.cluster_replicas.set_many(replicas, self.op_id)?;
        Ok(())
    }

    /// Set persisted configuration.
    pub fn set_config(&mut self, key: String, value: Option<u64>) -> Result<(), CatalogError> {
        match value {
            Some(value) => {
                let config = Config { key, value };
                let (key, value) = config.into_key_value();
                self.configs.set(key, Some(value), self.op_id)?;
            }
            None => {
                self.configs.set(ConfigKey { key }, None, self.op_id)?;
            }
        }
        Ok(())
    }

    /// Get the value of a persisted config.
    pub fn get_config(&self, key: String) -> Option<u64> {
        self.configs
            .get(&ConfigKey { key })
            .map(|entry| entry.value)
    }

    /// Get the value of a persisted setting.
    fn get_setting(&self, name: String) -> Option<&str> {
        self.settings
            .get(&SettingKey { name })
            .map(|entry| &*entry.value)
    }

    pub fn get_builtin_migration_shard(&self) -> Option<ShardId> {
        self.get_setting(BUILTIN_MIGRATION_SHARD_KEY.to_string())
            .map(|shard_id| shard_id.parse().expect("valid ShardId"))
    }

    pub fn set_builtin_migration_shard(&mut self, shard_id: ShardId) -> Result<(), CatalogError> {
        self.set_setting(
            BUILTIN_MIGRATION_SHARD_KEY.to_string(),
            Some(shard_id.to_string()),
        )
    }

    pub fn get_expression_cache_shard(&self) -> Option<ShardId> {
        self.get_setting(EXPRESSION_CACHE_SHARD_KEY.to_string())
            .map(|shard_id| shard_id.parse().expect("valid ShardId"))
    }

    pub fn set_expression_cache_shard(&mut self, shard_id: ShardId) -> Result<(), CatalogError> {
        self.set_setting(
            EXPRESSION_CACHE_SHARD_KEY.to_string(),
            Some(shard_id.to_string()),
        )
    }

    /// Updates the catalog `with_0dt_deployment_max_wait` "config" value to
    /// match the `with_0dt_deployment_max_wait` "system var" value.
    ///
    /// These are mirrored so that we can toggle the flag with Launch Darkly,
    /// but use it in boot before Launch Darkly is available.
    pub fn set_0dt_deployment_max_wait(&mut self, value: Duration) -> Result<(), CatalogError> {
        self.set_config(
            WITH_0DT_DEPLOYMENT_MAX_WAIT.into(),
            Some(
                value
                    .as_millis()
                    .try_into()
                    .expect("max wait fits into u64"),
            ),
        )
    }

    /// Updates the catalog `with_0dt_deployment_ddl_check_interval` "config"
    /// value to match the `with_0dt_deployment_ddl_check_interval` "system var"
    /// value.
    ///
    /// These are mirrored so that we can toggle the flag with Launch Darkly,
    /// but use it in boot before Launch Darkly is available.
    pub fn set_0dt_deployment_ddl_check_interval(
        &mut self,
        value: Duration,
    ) -> Result<(), CatalogError> {
        self.set_config(
            WITH_0DT_DEPLOYMENT_DDL_CHECK_INTERVAL.into(),
            Some(
                value
                    .as_millis()
                    .try_into()
                    .expect("ddl check interval fits into u64"),
            ),
        )
    }

    /// Updates the catalog `0dt_deployment_panic_after_timeout` "config" value to
    /// match the `0dt_deployment_panic_after_timeout` "system var" value.
    ///
    /// These are mirrored so that we can toggle the flag with Launch Darkly,
    /// but use it in boot before Launch Darkly is available.
    pub fn set_enable_0dt_deployment_panic_after_timeout(
        &mut self,
        value: bool,
    ) -> Result<(), CatalogError> {
        self.set_config(
            ENABLE_0DT_DEPLOYMENT_PANIC_AFTER_TIMEOUT.into(),
            Some(u64::from(value)),
        )
    }

    /// Removes the catalog `with_0dt_deployment_max_wait` "config" value to
    /// match the `with_0dt_deployment_max_wait` "system var" value.
    ///
    /// These are mirrored so that we can toggle the flag with LaunchDarkly,
    /// but use it in boot before LaunchDarkly is available.
    pub fn reset_0dt_deployment_max_wait(&mut self) -> Result<(), CatalogError> {
        self.set_config(WITH_0DT_DEPLOYMENT_MAX_WAIT.into(), None)
    }

    /// Removes the catalog `with_0dt_deployment_ddl_check_interval` "config"
    /// value to match the `with_0dt_deployment_ddl_check_interval` "system var"
    /// value.
    ///
    /// These are mirrored so that we can toggle the flag with LaunchDarkly, but
    /// use it in boot before LaunchDarkly is available.
    pub fn reset_0dt_deployment_ddl_check_interval(&mut self) -> Result<(), CatalogError> {
        self.set_config(WITH_0DT_DEPLOYMENT_DDL_CHECK_INTERVAL.into(), None)
    }

    /// Removes the catalog `enable_0dt_deployment_panic_after_timeout` "config"
    /// value to match the `enable_0dt_deployment_panic_after_timeout` "system
    /// var" value.
    ///
    /// These are mirrored so that we can toggle the flag with LaunchDarkly, but
    /// use it in boot before LaunchDarkly is available.
    pub fn reset_enable_0dt_deployment_panic_after_timeout(&mut self) -> Result<(), CatalogError> {
        self.set_config(ENABLE_0DT_DEPLOYMENT_PANIC_AFTER_TIMEOUT.into(), None)
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
        self.comments.set(key, value, self.op_id)?;

        Ok(())
    }

    pub fn drop_comments(
        &mut self,
        object_ids: &BTreeSet<CommentObjectId>,
    ) -> Result<(), CatalogError> {
        if object_ids.is_empty() {
            return Ok(());
        }

        self.comments
            .delete(|k, _v| object_ids.contains(&k.object_id), self.op_id);
        Ok(())
    }

    pub fn update_source_references(
        &mut self,
        source_id: CatalogItemId,
        references: Vec<SourceReference>,
        updated_at: u64,
    ) -> Result<(), CatalogError> {
        let key = SourceReferencesKey { source_id };
        let value = SourceReferencesValue {
            references,
            updated_at,
        };
        self.source_references.set(key, Some(value), self.op_id)?;
        Ok(())
    }

    /// Upserts persisted system configuration `name` to `value`.
    pub fn upsert_system_config(&mut self, name: &str, value: String) -> Result<(), CatalogError> {
        let key = ServerConfigurationKey {
            name: name.to_string(),
        };
        let value = ServerConfigurationValue { value };
        self.system_configurations
            .set(key, Some(value), self.op_id)?;
        Ok(())
    }

    /// Removes persisted system configuration `name`.
    pub fn remove_system_config(&mut self, name: &str) {
        let key = ServerConfigurationKey {
            name: name.to_string(),
        };
        self.system_configurations
            .set(key, None, self.op_id)
            .expect("cannot have uniqueness violation");
    }

    /// Removes all persisted system configurations.
    pub fn clear_system_configs(&mut self) {
        self.system_configurations.delete(|_k, _v| true, self.op_id);
    }

    pub(crate) fn insert_config(&mut self, key: String, value: u64) -> Result<(), CatalogError> {
        match self.configs.insert(
            ConfigKey { key: key.clone() },
            ConfigValue { value },
            self.op_id,
        ) {
            Ok(_) => Ok(()),
            Err(_) => Err(SqlCatalogError::ConfigAlreadyExists(key).into()),
        }
    }

    pub fn get_clusters(&self) -> impl Iterator<Item = Cluster> + use<'_> {
        self.clusters
            .items()
            .into_iter()
            .map(|(k, v)| DurableType::from_key_value(k.clone(), v.clone()))
    }

    pub fn get_cluster_replicas(&self) -> impl Iterator<Item = ClusterReplica> + use<'_> {
        self.cluster_replicas
            .items()
            .into_iter()
            .map(|(k, v)| DurableType::from_key_value(k.clone(), v.clone()))
    }

    pub fn get_roles(&self) -> impl Iterator<Item = Role> + use<'_> {
        self.roles
            .items()
            .into_iter()
            .map(|(k, v)| DurableType::from_key_value(k.clone(), v.clone()))
    }

    pub fn get_network_policies(&self) -> impl Iterator<Item = NetworkPolicy> + use<'_> {
        self.network_policies
            .items()
            .into_iter()
            .map(|(k, v)| DurableType::from_key_value(k.clone(), v.clone()))
    }

    pub fn get_system_object_mappings(
        &self,
    ) -> impl Iterator<Item = SystemObjectMapping> + use<'_> {
        self.system_gid_mapping
            .items()
            .into_iter()
            .map(|(k, v)| DurableType::from_key_value(k.clone(), v.clone()))
    }

    pub fn get_schemas(&self) -> impl Iterator<Item = Schema> + use<'_> {
        self.schemas
            .items()
            .into_iter()
            .map(|(k, v)| DurableType::from_key_value(k.clone(), v.clone()))
    }

    pub fn get_system_configurations(&self) -> impl Iterator<Item = SystemConfiguration> + use<'_> {
        self.system_configurations
            .items()
            .into_iter()
            .map(|(k, v)| DurableType::from_key_value(k.clone(), v.clone()))
    }

    pub fn get_schema(&self, id: &SchemaId) -> Option<Schema> {
        let key = SchemaKey { id: *id };
        self.schemas
            .get(&key)
            .map(|v| DurableType::from_key_value(key, v.clone()))
    }

    pub fn get_introspection_source_indexes(
        &self,
        cluster_id: ClusterId,
    ) -> BTreeMap<&str, (GlobalId, u32)> {
        self.introspection_sources
            .items()
            .into_iter()
            .filter(|(k, _v)| k.cluster_id == cluster_id)
            .map(|(k, v)| (k.name.as_str(), (v.global_id.into(), v.oid)))
            .collect()
    }

    pub fn get_catalog_content_version(&self) -> Option<&str> {
        self.settings
            .get(&SettingKey {
                name: CATALOG_CONTENT_VERSION_KEY.to_string(),
            })
            .map(|value| &*value.value)
    }

    /// Commit the current operation within the transaction. This does not cause anything to be
    /// written durably, but signals to the current transaction that we are moving on to the next
    /// operation.
    ///
    /// Returns the updates of the committed operation.
    #[must_use]
    pub fn get_and_commit_op_updates(&mut self) -> Vec<StateUpdate> {
        let updates = self.get_op_updates();
        self.commit_op();
        updates
    }

    fn get_op_updates(&self) -> Vec<StateUpdate> {
        fn get_collection_op_updates<'a, T>(
            table_txn: &'a TableTransaction<T::Key, T::Value>,
            kind_fn: impl Fn(T) -> StateUpdateKind + 'a,
            op: Timestamp,
        ) -> impl Iterator<Item = (StateUpdateKind, StateDiff)> + 'a
        where
            T::Key: Ord + Eq + Clone + Debug,
            T::Value: Ord + Clone + Debug,
            T: DurableType,
        {
            table_txn
                .pending
                .iter()
                .flat_map(|(k, vs)| vs.into_iter().map(move |v| (k, v)))
                .filter_map(move |(k, v)| {
                    if v.ts == op {
                        let key = k.clone();
                        let value = v.value.clone();
                        let diff = v.diff.clone().try_into().expect("invalid diff");
                        let update = DurableType::from_key_value(key, value);
                        let kind = kind_fn(update);
                        Some((kind, diff))
                    } else {
                        None
                    }
                })
        }

        fn get_large_collection_op_updates<'a, T>(
            collection: &'a Vec<(T::Key, Diff, Timestamp)>,
            kind_fn: impl Fn(T) -> StateUpdateKind + 'a,
            op: Timestamp,
        ) -> impl Iterator<Item = (StateUpdateKind, StateDiff)> + 'a
        where
            T::Key: Ord + Eq + Clone + Debug,
            T: DurableType<Value = ()>,
        {
            collection.iter().filter_map(move |(k, diff, ts)| {
                if *ts == op {
                    let key = k.clone();
                    let diff = diff.clone().try_into().expect("invalid diff");
                    let update = DurableType::from_key_value(key, ());
                    let kind = kind_fn(update);
                    Some((kind, diff))
                } else {
                    None
                }
            })
        }

        let Transaction {
            durable_catalog: _,
            databases,
            schemas,
            items,
            comments,
            roles,
            role_auth,
            clusters,
            network_policies,
            cluster_replicas,
            introspection_sources,
            system_gid_mapping,
            system_configurations,
            default_privileges,
            source_references,
            system_privileges,
            audit_log_updates,
            storage_collection_metadata,
            unfinalized_shards,
            // Not representable as a `StateUpdate`.
            id_allocator: _,
            configs: _,
            settings: _,
            txn_wal_shard: _,
            upper,
            op_id: _,
        } = &self;

        let updates = std::iter::empty()
            .chain(get_collection_op_updates(
                roles,
                StateUpdateKind::Role,
                self.op_id,
            ))
            .chain(get_collection_op_updates(
                role_auth,
                StateUpdateKind::RoleAuth,
                self.op_id,
            ))
            .chain(get_collection_op_updates(
                databases,
                StateUpdateKind::Database,
                self.op_id,
            ))
            .chain(get_collection_op_updates(
                schemas,
                StateUpdateKind::Schema,
                self.op_id,
            ))
            .chain(get_collection_op_updates(
                default_privileges,
                StateUpdateKind::DefaultPrivilege,
                self.op_id,
            ))
            .chain(get_collection_op_updates(
                system_privileges,
                StateUpdateKind::SystemPrivilege,
                self.op_id,
            ))
            .chain(get_collection_op_updates(
                system_configurations,
                StateUpdateKind::SystemConfiguration,
                self.op_id,
            ))
            .chain(get_collection_op_updates(
                clusters,
                StateUpdateKind::Cluster,
                self.op_id,
            ))
            .chain(get_collection_op_updates(
                network_policies,
                StateUpdateKind::NetworkPolicy,
                self.op_id,
            ))
            .chain(get_collection_op_updates(
                introspection_sources,
                StateUpdateKind::IntrospectionSourceIndex,
                self.op_id,
            ))
            .chain(get_collection_op_updates(
                cluster_replicas,
                StateUpdateKind::ClusterReplica,
                self.op_id,
            ))
            .chain(get_collection_op_updates(
                system_gid_mapping,
                StateUpdateKind::SystemObjectMapping,
                self.op_id,
            ))
            .chain(get_collection_op_updates(
                items,
                StateUpdateKind::Item,
                self.op_id,
            ))
            .chain(get_collection_op_updates(
                comments,
                StateUpdateKind::Comment,
                self.op_id,
            ))
            .chain(get_collection_op_updates(
                source_references,
                StateUpdateKind::SourceReferences,
                self.op_id,
            ))
            .chain(get_collection_op_updates(
                storage_collection_metadata,
                StateUpdateKind::StorageCollectionMetadata,
                self.op_id,
            ))
            .chain(get_collection_op_updates(
                unfinalized_shards,
                StateUpdateKind::UnfinalizedShard,
                self.op_id,
            ))
            .chain(get_large_collection_op_updates(
                audit_log_updates,
                StateUpdateKind::AuditLog,
                self.op_id,
            ))
            .map(|(kind, diff)| StateUpdate {
                kind,
                ts: upper.clone(),
                diff,
            })
            .collect();

        updates
    }

    pub fn is_savepoint(&self) -> bool {
        self.durable_catalog.is_savepoint()
    }

    fn commit_op(&mut self) {
        self.op_id += 1;
    }

    pub fn op_id(&self) -> Timestamp {
        self.op_id
    }

    pub fn upper(&self) -> mz_repr::Timestamp {
        self.upper
    }

    pub(crate) fn into_parts(self) -> (TransactionBatch, &'a mut dyn DurableCatalogState) {
        let audit_log_updates = self
            .audit_log_updates
            .into_iter()
            .map(|(k, diff, _op)| (k.into_proto(), (), diff))
            .collect();

        let txn_batch = TransactionBatch {
            databases: self.databases.pending(),
            schemas: self.schemas.pending(),
            items: self.items.pending(),
            comments: self.comments.pending(),
            roles: self.roles.pending(),
            role_auth: self.role_auth.pending(),
            clusters: self.clusters.pending(),
            cluster_replicas: self.cluster_replicas.pending(),
            network_policies: self.network_policies.pending(),
            introspection_sources: self.introspection_sources.pending(),
            id_allocator: self.id_allocator.pending(),
            configs: self.configs.pending(),
            source_references: self.source_references.pending(),
            settings: self.settings.pending(),
            system_gid_mapping: self.system_gid_mapping.pending(),
            system_configurations: self.system_configurations.pending(),
            default_privileges: self.default_privileges.pending(),
            system_privileges: self.system_privileges.pending(),
            storage_collection_metadata: self.storage_collection_metadata.pending(),
            unfinalized_shards: self.unfinalized_shards.pending(),
            txn_wal_shard: self.txn_wal_shard.pending(),
            audit_log_updates,
            upper: self.upper,
        };
        (txn_batch, self.durable_catalog)
    }

    /// Commits the storage transaction to durable storage. Any error returned outside read-only
    /// mode indicates the catalog may be in an indeterminate state and needs to be fully re-read
    /// before proceeding. In general, this must be fatal to the calling process. We do not
    /// panic/halt inside this function itself so that errors can bubble up during initialization.
    ///
    /// The transaction is committed at `commit_ts`.
    ///
    /// Returns what the upper was directly after the transaction committed.
    ///
    /// In read-only mode, this will return an error for non-empty transactions indicating that the
    /// catalog is not writeable.
    #[mz_ore::instrument(level = "debug")]
    pub(crate) async fn commit_internal(
        self,
        commit_ts: mz_repr::Timestamp,
    ) -> Result<(&'a mut dyn DurableCatalogState, mz_repr::Timestamp), CatalogError> {
        let (mut txn_batch, durable_catalog) = self.into_parts();
        let TransactionBatch {
            databases,
            schemas,
            items,
            comments,
            roles,
            role_auth,
            clusters,
            cluster_replicas,
            network_policies,
            introspection_sources,
            id_allocator,
            configs,
            source_references,
            settings,
            system_gid_mapping,
            system_configurations,
            default_privileges,
            system_privileges,
            storage_collection_metadata,
            unfinalized_shards,
            txn_wal_shard,
            audit_log_updates,
            upper,
        } = &mut txn_batch;
        // Consolidate in memory because it will likely be faster than consolidating after the
        // transaction has been made durable.
        differential_dataflow::consolidation::consolidate_updates(databases);
        differential_dataflow::consolidation::consolidate_updates(schemas);
        differential_dataflow::consolidation::consolidate_updates(items);
        differential_dataflow::consolidation::consolidate_updates(comments);
        differential_dataflow::consolidation::consolidate_updates(roles);
        differential_dataflow::consolidation::consolidate_updates(role_auth);
        differential_dataflow::consolidation::consolidate_updates(clusters);
        differential_dataflow::consolidation::consolidate_updates(cluster_replicas);
        differential_dataflow::consolidation::consolidate_updates(network_policies);
        differential_dataflow::consolidation::consolidate_updates(introspection_sources);
        differential_dataflow::consolidation::consolidate_updates(id_allocator);
        differential_dataflow::consolidation::consolidate_updates(configs);
        differential_dataflow::consolidation::consolidate_updates(settings);
        differential_dataflow::consolidation::consolidate_updates(source_references);
        differential_dataflow::consolidation::consolidate_updates(system_gid_mapping);
        differential_dataflow::consolidation::consolidate_updates(system_configurations);
        differential_dataflow::consolidation::consolidate_updates(default_privileges);
        differential_dataflow::consolidation::consolidate_updates(system_privileges);
        differential_dataflow::consolidation::consolidate_updates(storage_collection_metadata);
        differential_dataflow::consolidation::consolidate_updates(unfinalized_shards);
        differential_dataflow::consolidation::consolidate_updates(txn_wal_shard);
        differential_dataflow::consolidation::consolidate_updates(audit_log_updates);

        assert!(
            commit_ts >= *upper,
            "expected commit ts, {}, to be greater than or equal to upper, {}",
            commit_ts,
            upper
        );
        let upper = durable_catalog
            .commit_transaction(txn_batch, commit_ts)
            .await?;
        Ok((durable_catalog, upper))
    }

    /// Commits the storage transaction to durable storage. Any error returned outside read-only
    /// mode indicates the catalog may be in an indeterminate state and needs to be fully re-read
    /// before proceeding. In general, this must be fatal to the calling process. We do not
    /// panic/halt inside this function itself so that errors can bubble up during initialization.
    ///
    /// In read-only mode, this will return an error for non-empty transactions indicating that the
    /// catalog is not writeable.
    ///
    /// IMPORTANT: It is assumed that the committer of this transaction has already applied all
    /// updates from this transaction. Therefore, updates from this transaction will not be returned
    /// when calling [`crate::durable::ReadOnlyDurableCatalogState::sync_to_current_updates`] or
    /// [`crate::durable::ReadOnlyDurableCatalogState::sync_updates`].
    ///
    /// An alternative implementation would be for the caller to explicitly consume their updates
    /// after committing and only then apply the updates in-memory. While this removes assumptions
    /// about the caller in this method, in practice it results in duplicate work on every commit.
    #[mz_ore::instrument(level = "debug")]
    pub async fn commit(self, commit_ts: mz_repr::Timestamp) -> Result<(), CatalogError> {
        let op_updates = self.get_op_updates();
        assert!(
            op_updates.is_empty(),
            "unconsumed transaction updates: {op_updates:?}"
        );

        let (durable_storage, upper) = self.commit_internal(commit_ts).await?;
        // Drain all the updates from the commit since it is assumed that they were already applied.
        let updates = durable_storage.sync_updates(upper).await?;
        // Writable and savepoint catalogs should have consumed all updates before committing a
        // transaction, otherwise the commit was performed with an out of date state.
        // Read-only catalogs can only commit empty transactions, so they don't need to consume all
        // updates before committing.
        soft_assert_no_log!(
            durable_storage.is_read_only() || updates.iter().all(|update| update.ts == commit_ts),
            "unconsumed updates existed before transaction commit: commit_ts={commit_ts:?}, updates:{updates:?}"
        );
        Ok(())
    }
}

use crate::durable::async_trait;

use super::objects::{RoleAuthKey, RoleAuthValue};

#[async_trait]
impl StorageTxn<mz_repr::Timestamp> for Transaction<'_> {
    fn get_collection_metadata(&self) -> BTreeMap<GlobalId, ShardId> {
        self.storage_collection_metadata
            .items()
            .into_iter()
            .map(
                |(
                    StorageCollectionMetadataKey { id },
                    StorageCollectionMetadataValue { shard },
                )| { (*id, shard.clone()) },
            )
            .collect()
    }

    fn insert_collection_metadata(
        &mut self,
        metadata: BTreeMap<GlobalId, ShardId>,
    ) -> Result<(), StorageError<mz_repr::Timestamp>> {
        for (id, shard) in metadata {
            self.storage_collection_metadata
                .insert(
                    StorageCollectionMetadataKey { id },
                    StorageCollectionMetadataValue {
                        shard: shard.clone(),
                    },
                    self.op_id,
                )
                .map_err(|err| match err {
                    DurableCatalogError::DuplicateKey => {
                        StorageError::CollectionMetadataAlreadyExists(id)
                    }
                    DurableCatalogError::UniquenessViolation => {
                        StorageError::PersistShardAlreadyInUse(shard)
                    }
                    err => StorageError::Generic(anyhow::anyhow!(err)),
                })?;
        }
        Ok(())
    }

    fn delete_collection_metadata(&mut self, ids: BTreeSet<GlobalId>) -> Vec<(GlobalId, ShardId)> {
        let ks: Vec<_> = ids
            .into_iter()
            .map(|id| StorageCollectionMetadataKey { id })
            .collect();
        self.storage_collection_metadata
            .delete_by_keys(ks, self.op_id)
            .into_iter()
            .map(
                |(
                    StorageCollectionMetadataKey { id },
                    StorageCollectionMetadataValue { shard },
                )| (id, shard),
            )
            .collect()
    }

    fn get_unfinalized_shards(&self) -> BTreeSet<ShardId> {
        self.unfinalized_shards
            .items()
            .into_iter()
            .map(|(UnfinalizedShardKey { shard }, ())| *shard)
            .collect()
    }

    fn insert_unfinalized_shards(
        &mut self,
        s: BTreeSet<ShardId>,
    ) -> Result<(), StorageError<mz_repr::Timestamp>> {
        for shard in s {
            match self
                .unfinalized_shards
                .insert(UnfinalizedShardKey { shard }, (), self.op_id)
            {
                // Inserting duplicate keys has no effect.
                Ok(()) | Err(DurableCatalogError::DuplicateKey) => {}
                Err(e) => Err(StorageError::Generic(anyhow::anyhow!(e)))?,
            };
        }
        Ok(())
    }

    fn mark_shards_as_finalized(&mut self, shards: BTreeSet<ShardId>) {
        let ks: Vec<_> = shards
            .into_iter()
            .map(|shard| UnfinalizedShardKey { shard })
            .collect();
        let _ = self.unfinalized_shards.delete_by_keys(ks, self.op_id);
    }

    fn get_txn_wal_shard(&self) -> Option<ShardId> {
        self.txn_wal_shard
            .values()
            .iter()
            .next()
            .map(|TxnWalShardValue { shard }| *shard)
    }

    fn write_txn_wal_shard(
        &mut self,
        shard: ShardId,
    ) -> Result<(), StorageError<mz_repr::Timestamp>> {
        self.txn_wal_shard
            .insert((), TxnWalShardValue { shard }, self.op_id)
            .map_err(|err| match err {
                DurableCatalogError::DuplicateKey => StorageError::TxnWalShardAlreadyExists,
                err => StorageError::Generic(anyhow::anyhow!(err)),
            })
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
    pub(crate) role_auth: Vec<(proto::RoleAuthKey, proto::RoleAuthValue, Diff)>,
    pub(crate) clusters: Vec<(proto::ClusterKey, proto::ClusterValue, Diff)>,
    pub(crate) cluster_replicas: Vec<(proto::ClusterReplicaKey, proto::ClusterReplicaValue, Diff)>,
    pub(crate) network_policies: Vec<(proto::NetworkPolicyKey, proto::NetworkPolicyValue, Diff)>,
    pub(crate) introspection_sources: Vec<(
        proto::ClusterIntrospectionSourceIndexKey,
        proto::ClusterIntrospectionSourceIndexValue,
        Diff,
    )>,
    pub(crate) id_allocator: Vec<(proto::IdAllocKey, proto::IdAllocValue, Diff)>,
    pub(crate) configs: Vec<(proto::ConfigKey, proto::ConfigValue, Diff)>,
    pub(crate) settings: Vec<(proto::SettingKey, proto::SettingValue, Diff)>,
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
    pub(crate) source_references: Vec<(
        proto::SourceReferencesKey,
        proto::SourceReferencesValue,
        Diff,
    )>,
    pub(crate) system_privileges: Vec<(
        proto::SystemPrivilegesKey,
        proto::SystemPrivilegesValue,
        Diff,
    )>,
    pub(crate) storage_collection_metadata: Vec<(
        proto::StorageCollectionMetadataKey,
        proto::StorageCollectionMetadataValue,
        Diff,
    )>,
    pub(crate) unfinalized_shards: Vec<(proto::UnfinalizedShardKey, (), Diff)>,
    pub(crate) txn_wal_shard: Vec<((), proto::TxnWalShardValue, Diff)>,
    pub(crate) audit_log_updates: Vec<(proto::AuditLogKey, (), Diff)>,
    /// The upper of the catalog when the transaction started.
    pub(crate) upper: mz_repr::Timestamp,
}

impl TransactionBatch {
    pub fn is_empty(&self) -> bool {
        let TransactionBatch {
            databases,
            schemas,
            items,
            comments,
            roles,
            role_auth,
            clusters,
            cluster_replicas,
            network_policies,
            introspection_sources,
            id_allocator,
            configs,
            settings,
            source_references,
            system_gid_mapping,
            system_configurations,
            default_privileges,
            system_privileges,
            storage_collection_metadata,
            unfinalized_shards,
            txn_wal_shard,
            audit_log_updates,
            upper: _,
        } = self;
        databases.is_empty()
            && schemas.is_empty()
            && items.is_empty()
            && comments.is_empty()
            && roles.is_empty()
            && role_auth.is_empty()
            && clusters.is_empty()
            && cluster_replicas.is_empty()
            && network_policies.is_empty()
            && introspection_sources.is_empty()
            && id_allocator.is_empty()
            && configs.is_empty()
            && settings.is_empty()
            && source_references.is_empty()
            && system_gid_mapping.is_empty()
            && system_configurations.is_empty()
            && default_privileges.is_empty()
            && system_privileges.is_empty()
            && storage_collection_metadata.is_empty()
            && unfinalized_shards.is_empty()
            && txn_wal_shard.is_empty()
            && audit_log_updates.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TransactionUpdate<V> {
    value: V,
    ts: Timestamp,
    diff: Diff,
}

/// Utility trait to check for plan validity.
trait UniqueName {
    /// Does the item have a unique name? If yes, we can check for name equality in validity
    /// checking.
    const HAS_UNIQUE_NAME: bool;
    /// The unique name, only returns a meaningful name if [`Self::HAS_UNIQUE_NAME`] is `true`.
    fn unique_name(&self) -> &str;
}

mod unique_name {
    use crate::durable::objects::*;

    macro_rules! impl_unique_name {
        ($($t:ty),* $(,)?) => {
            $(
                impl crate::durable::transaction::UniqueName for $t {
                    const HAS_UNIQUE_NAME: bool = true;
                    fn unique_name(&self) -> &str {
                        &self.name
                    }
                }
            )*
        };
    }

    macro_rules! impl_no_unique_name {
        ($($t:ty),* $(,)?) => {
            $(
                impl crate::durable::transaction::UniqueName for $t {
                    const HAS_UNIQUE_NAME: bool = false;
                    fn unique_name(&self) -> &str {
                       ""
                    }
                }
            )*
        };
    }

    impl_unique_name! {
        ClusterReplicaValue,
        ClusterValue,
        DatabaseValue,
        ItemValue,
        NetworkPolicyValue,
        RoleValue,
        SchemaValue,
    }

    impl_no_unique_name!(
        (),
        ClusterIntrospectionSourceIndexValue,
        CommentValue,
        ConfigValue,
        DefaultPrivilegesValue,
        GidMappingValue,
        IdAllocValue,
        ServerConfigurationValue,
        SettingValue,
        SourceReferencesValue,
        StorageCollectionMetadataValue,
        SystemPrivilegesValue,
        TxnWalShardValue,
        RoleAuthValue,
    );

    #[cfg(test)]
    mod test {
        impl_no_unique_name!(String,);
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
#[derive(Debug)]
struct TableTransaction<K, V> {
    initial: BTreeMap<K, V>,
    // The desired updates to keys after commit.
    // Invariant: Value is sorted by `ts`.
    pending: BTreeMap<K, Vec<TransactionUpdate<V>>>,
    uniqueness_violation: Option<fn(a: &V, b: &V) -> bool>,
}

impl<K, V> TableTransaction<K, V>
where
    K: Ord + Eq + Clone + Debug,
    V: Ord + Clone + Debug + UniqueName,
{
    /// Create a new TableTransaction with initial data.
    ///
    /// Internally the catalog serializes data as protobuf. All fields in a proto message are
    /// optional, which makes using them in Rust cumbersome. Generic parameters `KP` and `VP` are
    /// protobuf types which deserialize to `K` and `V` that a [`TableTransaction`] is generic
    /// over.
    fn new<KP, VP>(initial: BTreeMap<KP, VP>) -> Result<Self, TryFromProtoError>
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
            uniqueness_violation: None,
        })
    }

    /// Like [`Self::new`], but you can also provide `uniqueness_violation`, which is a function
    /// that determines whether there is a uniqueness violation among two values.
    fn new_with_uniqueness_fn<KP, VP>(
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
            uniqueness_violation: Some(uniqueness_violation),
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
            .flat_map(|(k, v)| {
                let mut v: Vec<_> = v
                    .into_iter()
                    .map(|TransactionUpdate { value, ts: _, diff }| (value, diff))
                    .collect();
                differential_dataflow::consolidation::consolidate(&mut v);
                v.into_iter().map(move |(v, diff)| (k.clone(), v, diff))
            })
            .map(|(key, val, diff)| (key.into_proto(), val.into_proto(), diff))
            .collect()
    }

    /// Verifies that no items in `self` violate `self.uniqueness_violation`.
    ///
    /// Runtime is O(n^2), where n is the number of items in `self`, if
    /// [`UniqueName::HAS_UNIQUE_NAME`] is false for `V`. Prefer using [`Self::verify_keys`].
    fn verify(&self) -> Result<(), DurableCatalogError> {
        if let Some(uniqueness_violation) = self.uniqueness_violation {
            // Compare each value to each other value and ensure they are unique.
            let items = self.values();
            if V::HAS_UNIQUE_NAME {
                let by_name: BTreeMap<_, _> = items
                    .iter()
                    .enumerate()
                    .map(|(v, vi)| (vi.unique_name(), (v, vi)))
                    .collect();
                for (i, vi) in items.iter().enumerate() {
                    if let Some((j, vj)) = by_name.get(vi.unique_name()) {
                        if i != *j && uniqueness_violation(vi, *vj) {
                            return Err(DurableCatalogError::UniquenessViolation);
                        }
                    }
                }
            } else {
                for (i, vi) in items.iter().enumerate() {
                    for (j, vj) in items.iter().enumerate() {
                        if i != j && uniqueness_violation(vi, vj) {
                            return Err(DurableCatalogError::UniquenessViolation);
                        }
                    }
                }
            }
        }
        soft_assert_no_log!(
            self.pending
                .values()
                .all(|pending| { pending.is_sorted_by(|a, b| a.ts <= b.ts) }),
            "pending should be sorted by timestamp: {:?}",
            self.pending
        );
        Ok(())
    }

    /// Verifies that no items in `self` violate `self.uniqueness_violation` with `keys`.
    ///
    /// Runtime is O(n * k), where n is the number of items in `self` and k is the number of
    /// items in `keys`.
    fn verify_keys<'a>(
        &self,
        keys: impl IntoIterator<Item = &'a K>,
    ) -> Result<(), DurableCatalogError>
    where
        K: 'a,
    {
        if let Some(uniqueness_violation) = self.uniqueness_violation {
            let entries: Vec<_> = keys
                .into_iter()
                .filter_map(|key| self.get(key).map(|value| (key, value)))
                .collect();
            // Compare each value in `entries` to each value in `self` and ensure they are unique.
            for (ki, vi) in self.items() {
                for (kj, vj) in &entries {
                    if ki != *kj && uniqueness_violation(vi, vj) {
                        return Err(DurableCatalogError::UniquenessViolation);
                    }
                }
            }
        }
        soft_assert_no_log!(self.verify().is_ok());
        Ok(())
    }

    /// Iterates over the items viewable in the current transaction in arbitrary
    /// order and applies `f` on all key, value pairs.
    fn for_values<'a, F: FnMut(&'a K, &'a V)>(&'a self, mut f: F) {
        let mut seen = BTreeSet::new();
        for k in self.pending.keys() {
            seen.insert(k);
            let v = self.get(k);
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
        let pending = self.pending.get(k).map(Vec::as_slice).unwrap_or_default();
        let mut updates = Vec::with_capacity(pending.len() + 1);
        if let Some(initial) = self.initial.get(k) {
            updates.push((initial, Diff::ONE));
        }
        updates.extend(
            pending
                .into_iter()
                .map(|TransactionUpdate { value, ts: _, diff }| (value, *diff)),
        );

        differential_dataflow::consolidation::consolidate(&mut updates);
        assert!(updates.len() <= 1);
        updates.into_iter().next().map(|(v, _)| v)
    }

    /// Returns the items viewable in the current transaction. The items are
    /// cloned, so this is an expensive operation. Prefer using [`Self::items`], or
    /// [`Self::for_values`].
    // Used by tests.
    #[cfg(test)]
    fn items_cloned(&self) -> BTreeMap<K, V> {
        let mut items = BTreeMap::new();
        self.for_values(|k, v| {
            items.insert(k.clone(), v.clone());
        });
        items
    }

    /// Returns the items viewable in the current transaction as references. Returns a map
    /// of references.
    fn items(&self) -> BTreeMap<&K, &V> {
        let mut items = BTreeMap::new();
        self.for_values(|k, v| {
            items.insert(k, v);
        });
        items
    }

    /// Returns the values viewable in the current transaction as references.
    fn values(&self) -> BTreeSet<&V> {
        let mut items = BTreeSet::new();
        self.for_values(|_, v| {
            items.insert(v);
        });
        items
    }

    /// Returns the number of items viewable in the current transaction.
    fn len(&self) -> usize {
        let mut count = 0;
        self.for_values(|_, _| {
            count += 1;
        });
        count
    }

    /// Iterates over the items viewable in the current transaction, and provides a
    /// map where additional pending items can be inserted, which will be appended
    /// to current pending items. Does not verify uniqueness.
    fn for_values_mut<F: FnMut(&mut BTreeMap<K, Vec<TransactionUpdate<V>>>, &K, &V)>(
        &mut self,
        mut f: F,
    ) {
        let mut pending = BTreeMap::new();
        self.for_values(|k, v| f(&mut pending, k, v));
        for (k, updates) in pending {
            self.pending.entry(k).or_default().extend(updates);
        }
    }

    /// Inserts a new k,v pair.
    ///
    /// Returns an error if the uniqueness check failed or the key already exists.
    fn insert(&mut self, k: K, v: V, ts: Timestamp) -> Result<(), DurableCatalogError> {
        let mut violation = None;
        self.for_values(|for_k, for_v| {
            if &k == for_k {
                violation = Some(DurableCatalogError::DuplicateKey);
            }
            if let Some(uniqueness_violation) = self.uniqueness_violation {
                if uniqueness_violation(for_v, &v) {
                    violation = Some(DurableCatalogError::UniquenessViolation);
                }
            }
        });
        if let Some(violation) = violation {
            return Err(violation);
        }
        self.pending.entry(k).or_default().push(TransactionUpdate {
            value: v,
            ts,
            diff: Diff::ONE,
        });
        soft_assert_no_log!(self.verify().is_ok());
        Ok(())
    }

    /// Updates k, v pairs. `f` is a function that can return `Some(V)` if the
    /// value should be updated, otherwise `None`. Returns the number of changed
    /// entries.
    ///
    /// Returns an error if the uniqueness check failed.
    ///
    /// Prefer using [`Self::update_by_key`] or [`Self::update_by_keys`], which generally have
    /// better performance.
    fn update<F: Fn(&K, &V) -> Option<V>>(
        &mut self,
        f: F,
        ts: Timestamp,
    ) -> Result<Diff, DurableCatalogError> {
        let mut changed = Diff::ZERO;
        let mut keys = BTreeSet::new();
        // Keep a copy of pending in case of uniqueness violation.
        let pending = self.pending.clone();
        self.for_values_mut(|p, k, v| {
            if let Some(next) = f(k, v) {
                changed += Diff::ONE;
                keys.insert(k.clone());
                let updates = p.entry(k.clone()).or_default();
                updates.push(TransactionUpdate {
                    value: v.clone(),
                    ts,
                    diff: Diff::MINUS_ONE,
                });
                updates.push(TransactionUpdate {
                    value: next,
                    ts,
                    diff: Diff::ONE,
                });
            }
        });
        // Check for uniqueness violation.
        if let Err(err) = self.verify_keys(&keys) {
            self.pending = pending;
            Err(err)
        } else {
            Ok(changed)
        }
    }

    /// Updates `k`, `v` pair if `k` already exists in `self`.
    ///
    /// Returns `true` if `k` was updated, `false` otherwise.
    /// Returns an error if the uniqueness check failed.
    fn update_by_key(&mut self, k: K, v: V, ts: Timestamp) -> Result<bool, DurableCatalogError> {
        if let Some(cur_v) = self.get(&k) {
            if v != *cur_v {
                self.set(k, Some(v), ts)?;
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Updates k, v pairs. Keys that don't already exist in `self` are ignored.
    ///
    /// Returns the number of changed entries.
    /// Returns an error if the uniqueness check failed.
    fn update_by_keys(
        &mut self,
        kvs: impl IntoIterator<Item = (K, V)>,
        ts: Timestamp,
    ) -> Result<Diff, DurableCatalogError> {
        let kvs: Vec<_> = kvs
            .into_iter()
            .filter_map(|(k, v)| match self.get(&k) {
                // Record if updating this entry would be a no-op.
                Some(cur_v) => Some((*cur_v == v, k, v)),
                None => None,
            })
            .collect();
        let changed = kvs.len();
        let changed =
            Diff::try_from(changed).map_err(|e| DurableCatalogError::Internal(e.to_string()))?;
        let kvs = kvs
            .into_iter()
            // Filter out no-ops to save some work.
            .filter(|(no_op, _, _)| !no_op)
            .map(|(_, k, v)| (k, Some(v)))
            .collect();
        self.set_many(kvs, ts)?;
        Ok(changed)
    }

    /// Set the value for a key. Returns the previous entry if the key existed,
    /// otherwise None.
    ///
    /// Returns an error if the uniqueness check failed.
    ///
    /// DO NOT call this function in a loop, use [`Self::set_many`] instead.
    fn set(&mut self, k: K, v: Option<V>, ts: Timestamp) -> Result<Option<V>, DurableCatalogError> {
        let prev = self.get(&k).cloned();
        let entry = self.pending.entry(k.clone()).or_default();
        let restore_len = entry.len();

        match (v, prev.clone()) {
            (Some(v), Some(prev)) => {
                entry.push(TransactionUpdate {
                    value: prev,
                    ts,
                    diff: Diff::MINUS_ONE,
                });
                entry.push(TransactionUpdate {
                    value: v,
                    ts,
                    diff: Diff::ONE,
                });
            }
            (Some(v), None) => {
                entry.push(TransactionUpdate {
                    value: v,
                    ts,
                    diff: Diff::ONE,
                });
            }
            (None, Some(prev)) => {
                entry.push(TransactionUpdate {
                    value: prev,
                    ts,
                    diff: Diff::MINUS_ONE,
                });
            }
            (None, None) => {}
        }

        // Check for uniqueness violation.
        if let Err(err) = self.verify_keys([&k]) {
            // Revert self.pending to the state it was in before calling this
            // function.
            let pending = self.pending.get_mut(&k).expect("inserted above");
            pending.truncate(restore_len);
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
        ts: Timestamp,
    ) -> Result<BTreeMap<K, Option<V>>, DurableCatalogError> {
        if kvs.is_empty() {
            return Ok(BTreeMap::new());
        }

        let mut prevs = BTreeMap::new();
        let mut restores = BTreeMap::new();

        for (k, v) in kvs {
            let prev = self.get(&k).cloned();
            let entry = self.pending.entry(k.clone()).or_default();
            restores.insert(k.clone(), entry.len());

            match (v, prev.clone()) {
                (Some(v), Some(prev)) => {
                    entry.push(TransactionUpdate {
                        value: prev,
                        ts,
                        diff: Diff::MINUS_ONE,
                    });
                    entry.push(TransactionUpdate {
                        value: v,
                        ts,
                        diff: Diff::ONE,
                    });
                }
                (Some(v), None) => {
                    entry.push(TransactionUpdate {
                        value: v,
                        ts,
                        diff: Diff::ONE,
                    });
                }
                (None, Some(prev)) => {
                    entry.push(TransactionUpdate {
                        value: prev,
                        ts,
                        diff: Diff::MINUS_ONE,
                    });
                }
                (None, None) => {}
            }

            prevs.insert(k, prev);
        }

        // Check for uniqueness violation.
        if let Err(err) = self.verify_keys(prevs.keys()) {
            for (k, restore_len) in restores {
                // Revert self.pending to the state it was in before calling this
                // function.
                let pending = self.pending.get_mut(&k).expect("inserted above");
                pending.truncate(restore_len);
            }
            Err(err)
        } else {
            Ok(prevs)
        }
    }

    /// Deletes items for which `f` returns true. Returns the keys and values of
    /// the deleted entries.
    ///
    /// Prefer using [`Self::delete_by_key`] or [`Self::delete_by_keys`], which generally have
    /// better performance.
    fn delete<F: Fn(&K, &V) -> bool>(&mut self, f: F, ts: Timestamp) -> Vec<(K, V)> {
        let mut deleted = Vec::new();
        self.for_values_mut(|p, k, v| {
            if f(k, v) {
                deleted.push((k.clone(), v.clone()));
                p.entry(k.clone()).or_default().push(TransactionUpdate {
                    value: v.clone(),
                    ts,
                    diff: Diff::MINUS_ONE,
                });
            }
        });
        soft_assert_no_log!(self.verify().is_ok());
        deleted
    }

    /// Deletes item with key `k`.
    ///
    /// Returns the value of the deleted entry, if it existed.
    fn delete_by_key(&mut self, k: K, ts: Timestamp) -> Option<V> {
        self.set(k, None, ts)
            .expect("deleting an entry cannot violate uniqueness")
    }

    /// Deletes items with key in `ks`.
    ///
    /// Returns the keys and values of the deleted entries.
    fn delete_by_keys(&mut self, ks: impl IntoIterator<Item = K>, ts: Timestamp) -> Vec<(K, V)> {
        let kvs = ks.into_iter().map(|k| (k, None)).collect();
        let prevs = self
            .set_many(kvs, ts)
            .expect("deleting entries cannot violate uniqueness");
        prevs
            .into_iter()
            .filter_map(|(k, v)| v.map(|v| (k, v)))
            .collect()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use mz_ore::{assert_none, assert_ok};

    use mz_ore::now::SYSTEM_TIME;
    use mz_persist_client::PersistClient;

    use crate::durable::{TestCatalogStateBuilder, test_bootstrap_args};
    use crate::memory;

    #[mz_ore::test]
    fn test_table_transaction_simple() {
        fn uniqueness_violation(a: &String, b: &String) -> bool {
            a == b
        }
        let mut table = TableTransaction::new_with_uniqueness_fn(
            BTreeMap::from([(1i64.to_le_bytes().to_vec(), "a".to_string())]),
            uniqueness_violation,
        )
        .unwrap();

        // Ideally, we compare for errors here, but it's hard/impossible to implement PartialEq
        // for DurableCatalogError.
        assert_ok!(table.insert(2i64.to_le_bytes().to_vec(), "b".to_string(), 0));
        assert_ok!(table.insert(3i64.to_le_bytes().to_vec(), "c".to_string(), 0));
        assert!(
            table
                .insert(1i64.to_le_bytes().to_vec(), "c".to_string(), 0)
                .is_err()
        );
        assert!(
            table
                .insert(4i64.to_le_bytes().to_vec(), "c".to_string(), 0)
                .is_err()
        );
    }

    #[mz_ore::test]
    fn test_table_transaction() {
        fn uniqueness_violation(a: &String, b: &String) -> bool {
            a == b
        }
        let mut table: BTreeMap<Vec<u8>, String> = BTreeMap::new();

        fn commit(
            table: &mut BTreeMap<Vec<u8>, String>,
            mut pending: Vec<(Vec<u8>, String, Diff)>,
        ) {
            // Sort by diff so that we process retractions first.
            pending.sort_by(|a, b| a.2.cmp(&b.2));
            for (k, v, diff) in pending {
                if diff == Diff::MINUS_ONE {
                    let prev = table.remove(&k);
                    assert_eq!(prev, Some(v));
                } else if diff == Diff::ONE {
                    let prev = table.insert(k, v);
                    assert_eq!(prev, None);
                } else {
                    panic!("unexpected diff: {diff}");
                }
            }
        }

        table.insert(1i64.to_le_bytes().to_vec(), "v1".to_string());
        table.insert(2i64.to_le_bytes().to_vec(), "v2".to_string());
        let mut table_txn =
            TableTransaction::new_with_uniqueness_fn(table.clone(), uniqueness_violation).unwrap();
        assert_eq!(table_txn.items_cloned(), table);
        assert_eq!(table_txn.delete(|_k, _v| false, 0).len(), 0);
        assert_eq!(table_txn.delete(|_k, v| v == "v2", 1).len(), 1);
        assert_eq!(
            table_txn.items_cloned(),
            BTreeMap::from([(1i64.to_le_bytes().to_vec(), "v1".to_string())])
        );
        assert_eq!(
            table_txn
                .update(|_k, _v| Some("v3".to_string()), 2)
                .unwrap(),
            Diff::ONE
        );

        // Uniqueness violation.
        table_txn
            .insert(3i64.to_le_bytes().to_vec(), "v3".to_string(), 3)
            .unwrap_err();

        table_txn
            .insert(3i64.to_le_bytes().to_vec(), "v4".to_string(), 4)
            .unwrap();
        assert_eq!(
            table_txn.items_cloned(),
            BTreeMap::from([
                (1i64.to_le_bytes().to_vec(), "v3".to_string()),
                (3i64.to_le_bytes().to_vec(), "v4".to_string()),
            ])
        );
        let err = table_txn
            .update(|_k, _v| Some("v1".to_string()), 5)
            .unwrap_err();
        assert!(
            matches!(err, DurableCatalogError::UniquenessViolation),
            "unexpected err: {err:?}"
        );
        let pending = table_txn.pending();
        assert_eq!(
            pending,
            vec![
                (
                    1i64.to_le_bytes().to_vec(),
                    "v1".to_string(),
                    Diff::MINUS_ONE
                ),
                (1i64.to_le_bytes().to_vec(), "v3".to_string(), Diff::ONE),
                (
                    2i64.to_le_bytes().to_vec(),
                    "v2".to_string(),
                    Diff::MINUS_ONE
                ),
                (3i64.to_le_bytes().to_vec(), "v4".to_string(), Diff::ONE),
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

        let mut table_txn =
            TableTransaction::new_with_uniqueness_fn(table.clone(), uniqueness_violation).unwrap();
        // Deleting then creating an item that has a uniqueness violation should work.
        assert_eq!(
            table_txn.delete(|k, _v| k == &1i64.to_le_bytes(), 0).len(),
            1
        );
        table_txn
            .insert(1i64.to_le_bytes().to_vec(), "v3".to_string(), 0)
            .unwrap();
        // Uniqueness violation in value.
        table_txn
            .insert(5i64.to_le_bytes().to_vec(), "v3".to_string(), 0)
            .unwrap_err();
        // Key already exists, expect error.
        table_txn
            .insert(1i64.to_le_bytes().to_vec(), "v5".to_string(), 0)
            .unwrap_err();
        assert_eq!(
            table_txn.delete(|k, _v| k == &1i64.to_le_bytes(), 0).len(),
            1
        );
        // Both the inserts work now because the key and uniqueness violation are gone.
        table_txn
            .insert(5i64.to_le_bytes().to_vec(), "v3".to_string(), 0)
            .unwrap();
        table_txn
            .insert(1i64.to_le_bytes().to_vec(), "v5".to_string(), 0)
            .unwrap();
        let pending = table_txn.pending();
        assert_eq!(
            pending,
            vec![
                (
                    1i64.to_le_bytes().to_vec(),
                    "v3".to_string(),
                    Diff::MINUS_ONE
                ),
                (1i64.to_le_bytes().to_vec(), "v5".to_string(), Diff::ONE),
                (5i64.to_le_bytes().to_vec(), "v3".to_string(), Diff::ONE),
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

        let mut table_txn =
            TableTransaction::new_with_uniqueness_fn(table.clone(), uniqueness_violation).unwrap();
        assert_eq!(table_txn.delete(|_k, _v| true, 0).len(), 3);
        table_txn
            .insert(1i64.to_le_bytes().to_vec(), "v1".to_string(), 0)
            .unwrap();

        commit(&mut table, table_txn.pending());
        assert_eq!(
            table,
            BTreeMap::from([(1i64.to_le_bytes().to_vec(), "v1".to_string()),])
        );

        let mut table_txn =
            TableTransaction::new_with_uniqueness_fn(table.clone(), uniqueness_violation).unwrap();
        assert_eq!(table_txn.delete(|_k, _v| true, 0).len(), 1);
        table_txn
            .insert(1i64.to_le_bytes().to_vec(), "v2".to_string(), 0)
            .unwrap();
        commit(&mut table, table_txn.pending());
        assert_eq!(
            table,
            BTreeMap::from([(1i64.to_le_bytes().to_vec(), "v2".to_string()),])
        );

        // Verify we don't try to delete v3 or v4 during commit.
        let mut table_txn =
            TableTransaction::new_with_uniqueness_fn(table.clone(), uniqueness_violation).unwrap();
        assert_eq!(table_txn.delete(|_k, _v| true, 0).len(), 1);
        table_txn
            .insert(1i64.to_le_bytes().to_vec(), "v3".to_string(), 0)
            .unwrap();
        table_txn
            .insert(1i64.to_le_bytes().to_vec(), "v4".to_string(), 1)
            .unwrap_err();
        assert_eq!(table_txn.delete(|_k, _v| true, 1).len(), 1);
        table_txn
            .insert(1i64.to_le_bytes().to_vec(), "v5".to_string(), 1)
            .unwrap();
        commit(&mut table, table_txn.pending());
        assert_eq!(
            table.clone().into_iter().collect::<Vec<_>>(),
            vec![(1i64.to_le_bytes().to_vec(), "v5".to_string())]
        );

        // Test `set`.
        let mut table_txn =
            TableTransaction::new_with_uniqueness_fn(table.clone(), uniqueness_violation).unwrap();
        // Uniqueness violation.
        table_txn
            .set(2i64.to_le_bytes().to_vec(), Some("v5".to_string()), 0)
            .unwrap_err();
        table_txn
            .set(3i64.to_le_bytes().to_vec(), Some("v6".to_string()), 1)
            .unwrap();
        table_txn.set(2i64.to_le_bytes().to_vec(), None, 2).unwrap();
        table_txn.set(1i64.to_le_bytes().to_vec(), None, 2).unwrap();
        let pending = table_txn.pending();
        assert_eq!(
            pending,
            vec![
                (
                    1i64.to_le_bytes().to_vec(),
                    "v5".to_string(),
                    Diff::MINUS_ONE
                ),
                (3i64.to_le_bytes().to_vec(), "v6".to_string(), Diff::ONE),
            ]
        );
        commit(&mut table, pending);
        assert_eq!(
            table,
            BTreeMap::from([(3i64.to_le_bytes().to_vec(), "v6".to_string())])
        );

        // Duplicate `set`.
        let mut table_txn =
            TableTransaction::new_with_uniqueness_fn(table.clone(), uniqueness_violation).unwrap();
        table_txn
            .set(3i64.to_le_bytes().to_vec(), Some("v6".to_string()), 0)
            .unwrap();
        let pending = table_txn.pending::<Vec<u8>, String>();
        assert!(pending.is_empty());

        // Test `set_many`.
        let mut table_txn =
            TableTransaction::new_with_uniqueness_fn(table.clone(), uniqueness_violation).unwrap();
        // Uniqueness violation.
        table_txn
            .set_many(
                BTreeMap::from([
                    (1i64.to_le_bytes().to_vec(), Some("v6".to_string())),
                    (42i64.to_le_bytes().to_vec(), Some("v1".to_string())),
                ]),
                0,
            )
            .unwrap_err();
        table_txn
            .set_many(
                BTreeMap::from([
                    (1i64.to_le_bytes().to_vec(), Some("v6".to_string())),
                    (3i64.to_le_bytes().to_vec(), Some("v1".to_string())),
                ]),
                1,
            )
            .unwrap();
        table_txn
            .set_many(
                BTreeMap::from([
                    (42i64.to_le_bytes().to_vec(), Some("v7".to_string())),
                    (3i64.to_le_bytes().to_vec(), None),
                ]),
                2,
            )
            .unwrap();
        let pending = table_txn.pending();
        assert_eq!(
            pending,
            vec![
                (1i64.to_le_bytes().to_vec(), "v6".to_string(), Diff::ONE),
                (
                    3i64.to_le_bytes().to_vec(),
                    "v6".to_string(),
                    Diff::MINUS_ONE
                ),
                (42i64.to_le_bytes().to_vec(), "v7".to_string(), Diff::ONE),
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
        let mut table_txn =
            TableTransaction::new_with_uniqueness_fn(table.clone(), uniqueness_violation).unwrap();
        table_txn
            .set_many(
                BTreeMap::from([
                    (1i64.to_le_bytes().to_vec(), Some("v6".to_string())),
                    (42i64.to_le_bytes().to_vec(), Some("v7".to_string())),
                ]),
                0,
            )
            .unwrap();
        let pending = table_txn.pending::<Vec<u8>, String>();
        assert!(pending.is_empty());
        commit(&mut table, pending);
        assert_eq!(
            table,
            BTreeMap::from([
                (1i64.to_le_bytes().to_vec(), "v6".to_string()),
                (42i64.to_le_bytes().to_vec(), "v7".to_string())
            ])
        );

        // Test `update_by_key`
        let mut table_txn =
            TableTransaction::new_with_uniqueness_fn(table.clone(), uniqueness_violation).unwrap();
        // Uniqueness violation.
        table_txn
            .update_by_key(1i64.to_le_bytes().to_vec(), "v7".to_string(), 0)
            .unwrap_err();
        assert!(
            table_txn
                .update_by_key(1i64.to_le_bytes().to_vec(), "v8".to_string(), 1)
                .unwrap()
        );
        assert!(
            !table_txn
                .update_by_key(5i64.to_le_bytes().to_vec(), "v8".to_string(), 2)
                .unwrap()
        );
        let pending = table_txn.pending();
        assert_eq!(
            pending,
            vec![
                (
                    1i64.to_le_bytes().to_vec(),
                    "v6".to_string(),
                    Diff::MINUS_ONE
                ),
                (1i64.to_le_bytes().to_vec(), "v8".to_string(), Diff::ONE),
            ]
        );
        commit(&mut table, pending);
        assert_eq!(
            table,
            BTreeMap::from([
                (1i64.to_le_bytes().to_vec(), "v8".to_string()),
                (42i64.to_le_bytes().to_vec(), "v7".to_string())
            ])
        );

        // Duplicate `update_by_key`.
        let mut table_txn =
            TableTransaction::new_with_uniqueness_fn(table.clone(), uniqueness_violation).unwrap();
        assert!(
            table_txn
                .update_by_key(1i64.to_le_bytes().to_vec(), "v8".to_string(), 0)
                .unwrap()
        );
        let pending = table_txn.pending::<Vec<u8>, String>();
        assert!(pending.is_empty());
        commit(&mut table, pending);
        assert_eq!(
            table,
            BTreeMap::from([
                (1i64.to_le_bytes().to_vec(), "v8".to_string()),
                (42i64.to_le_bytes().to_vec(), "v7".to_string())
            ])
        );

        // Test `update_by_keys`
        let mut table_txn =
            TableTransaction::new_with_uniqueness_fn(table.clone(), uniqueness_violation).unwrap();
        // Uniqueness violation.
        table_txn
            .update_by_keys(
                [
                    (1i64.to_le_bytes().to_vec(), "v7".to_string()),
                    (5i64.to_le_bytes().to_vec(), "v7".to_string()),
                ],
                0,
            )
            .unwrap_err();
        let n = table_txn
            .update_by_keys(
                [
                    (1i64.to_le_bytes().to_vec(), "v9".to_string()),
                    (5i64.to_le_bytes().to_vec(), "v7".to_string()),
                ],
                1,
            )
            .unwrap();
        assert_eq!(n, Diff::ONE);
        let n = table_txn
            .update_by_keys(
                [
                    (15i64.to_le_bytes().to_vec(), "v9".to_string()),
                    (5i64.to_le_bytes().to_vec(), "v7".to_string()),
                ],
                2,
            )
            .unwrap();
        assert_eq!(n, Diff::ZERO);
        let pending = table_txn.pending();
        assert_eq!(
            pending,
            vec![
                (
                    1i64.to_le_bytes().to_vec(),
                    "v8".to_string(),
                    Diff::MINUS_ONE
                ),
                (1i64.to_le_bytes().to_vec(), "v9".to_string(), Diff::ONE),
            ]
        );
        commit(&mut table, pending);
        assert_eq!(
            table,
            BTreeMap::from([
                (1i64.to_le_bytes().to_vec(), "v9".to_string()),
                (42i64.to_le_bytes().to_vec(), "v7".to_string())
            ])
        );

        // Duplicate `update_by_keys`.
        let mut table_txn =
            TableTransaction::new_with_uniqueness_fn(table.clone(), uniqueness_violation).unwrap();
        let n = table_txn
            .update_by_keys(
                [
                    (1i64.to_le_bytes().to_vec(), "v9".to_string()),
                    (42i64.to_le_bytes().to_vec(), "v7".to_string()),
                ],
                0,
            )
            .unwrap();
        assert_eq!(n, Diff::from(2));
        let pending = table_txn.pending::<Vec<u8>, String>();
        assert!(pending.is_empty());
        commit(&mut table, pending);
        assert_eq!(
            table,
            BTreeMap::from([
                (1i64.to_le_bytes().to_vec(), "v9".to_string()),
                (42i64.to_le_bytes().to_vec(), "v7".to_string())
            ])
        );

        // Test `delete_by_key`
        let mut table_txn =
            TableTransaction::new_with_uniqueness_fn(table.clone(), uniqueness_violation).unwrap();
        let prev = table_txn.delete_by_key(1i64.to_le_bytes().to_vec(), 0);
        assert_eq!(prev, Some("v9".to_string()));
        let prev = table_txn.delete_by_key(5i64.to_le_bytes().to_vec(), 1);
        assert_none!(prev);
        let prev = table_txn.delete_by_key(1i64.to_le_bytes().to_vec(), 2);
        assert_none!(prev);
        let pending = table_txn.pending();
        assert_eq!(
            pending,
            vec![(
                1i64.to_le_bytes().to_vec(),
                "v9".to_string(),
                Diff::MINUS_ONE
            ),]
        );
        commit(&mut table, pending);
        assert_eq!(
            table,
            BTreeMap::from([(42i64.to_le_bytes().to_vec(), "v7".to_string())])
        );

        // Test `delete_by_keys`
        let mut table_txn =
            TableTransaction::new_with_uniqueness_fn(table.clone(), uniqueness_violation).unwrap();
        let prevs = table_txn.delete_by_keys(
            [42i64.to_le_bytes().to_vec(), 55i64.to_le_bytes().to_vec()],
            0,
        );
        assert_eq!(
            prevs,
            vec![(42i64.to_le_bytes().to_vec(), "v7".to_string())]
        );
        let prevs = table_txn.delete_by_keys(
            [42i64.to_le_bytes().to_vec(), 55i64.to_le_bytes().to_vec()],
            1,
        );
        assert_eq!(prevs, vec![]);
        let prevs = table_txn.delete_by_keys(
            [10i64.to_le_bytes().to_vec(), 55i64.to_le_bytes().to_vec()],
            2,
        );
        assert_eq!(prevs, vec![]);
        let pending = table_txn.pending();
        assert_eq!(
            pending,
            vec![(
                42i64.to_le_bytes().to_vec(),
                "v7".to_string(),
                Diff::MINUS_ONE
            ),]
        );
        commit(&mut table, pending);
        assert_eq!(table, BTreeMap::new());
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn test_savepoint() {
        let persist_client = PersistClient::new_for_tests().await;
        let state_builder =
            TestCatalogStateBuilder::new(persist_client).with_default_deploy_generation();

        // Initialize catalog.
        let _ = state_builder
            .clone()
            .unwrap_build()
            .await
            .open(SYSTEM_TIME().into(), &test_bootstrap_args())
            .await
            .unwrap()
            .0;
        let mut savepoint_state = state_builder
            .unwrap_build()
            .await
            .open_savepoint(SYSTEM_TIME().into(), &test_bootstrap_args())
            .await
            .unwrap()
            .0;

        let initial_snapshot = savepoint_state.sync_to_current_updates().await.unwrap();
        assert!(!initial_snapshot.is_empty());

        let db_name = "db";
        let db_owner = RoleId::User(42);
        let db_privileges = Vec::new();
        let mut txn = savepoint_state.transaction().await.unwrap();
        let (db_id, db_oid) = txn
            .insert_user_database(db_name, db_owner, db_privileges.clone(), &HashSet::new())
            .unwrap();
        let commit_ts = txn.upper();
        txn.commit_internal(commit_ts).await.unwrap();
        let updates = savepoint_state.sync_to_current_updates().await.unwrap();
        let update = updates.into_element();

        assert_eq!(update.diff, StateDiff::Addition);

        let db = match update.kind {
            memory::objects::StateUpdateKind::Database(db) => db,
            update => panic!("unexpected update: {update:?}"),
        };

        assert_eq!(db_id, db.id);
        assert_eq!(db_oid, db.oid);
        assert_eq!(db_name, db.name);
        assert_eq!(db_owner, db.owner_id);
        assert_eq!(db_privileges, db.privileges);
    }

    #[mz_ore::test]
    fn test_allocate_introspection_source_index_id() {
        let cluster_variant: u8 = 0b0000_0001;
        let cluster_id_inner: u64 =
            0b0000_0000_1100_0101_1100_0011_1010_1101_0000_1011_1111_1001_0110_1010;
        let timely_messages_received_log_variant: u8 = 0b0000_1000;

        let cluster_id = ClusterId::System(cluster_id_inner);
        let log_variant = LogVariant::Timely(TimelyLog::MessagesReceived);

        let introspection_source_index_id: u64 =
            0b0000_0001_1100_0101_1100_0011_1010_1101_0000_1011_1111_1001_0110_1010_0000_1000;

        // Sanity check that `introspection_source_index_id` contains `cluster_variant`.
        {
            let mut cluster_variant_mask = 0xFF << 56;
            cluster_variant_mask &= introspection_source_index_id;
            cluster_variant_mask >>= 56;
            assert_eq!(cluster_variant_mask, u64::from(cluster_variant));
        }

        // Sanity check that `introspection_source_index_id` contains `cluster_id_inner`.
        {
            let mut cluster_id_inner_mask = 0xFFFF_FFFF_FFFF << 8;
            cluster_id_inner_mask &= introspection_source_index_id;
            cluster_id_inner_mask >>= 8;
            assert_eq!(cluster_id_inner_mask, cluster_id_inner);
        }

        // Sanity check that `introspection_source_index_id` contains `timely_messages_received_log_variant`.
        {
            let mut log_variant_mask = 0xFF;
            log_variant_mask &= introspection_source_index_id;
            assert_eq!(
                log_variant_mask,
                u64::from(timely_messages_received_log_variant)
            );
        }

        let (catalog_item_id, global_id) =
            Transaction::allocate_introspection_source_index_id(&cluster_id, log_variant);

        assert_eq!(
            catalog_item_id,
            CatalogItemId::IntrospectionSourceIndex(introspection_source_index_id)
        );
        assert_eq!(
            global_id,
            GlobalId::IntrospectionSourceIndex(introspection_source_index_id)
        );
    }
}
