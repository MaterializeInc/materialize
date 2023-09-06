// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::hash::Hash;
use std::iter::once;
use std::pin;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use itertools::Itertools;
use mz_audit_log::{VersionedEvent, VersionedStorageUsage};
use mz_controller::clusters::{ClusterId, ReplicaId, ReplicaLogging};
use mz_ore::collections::CollectionExt;
use mz_ore::now::NowFn;
use mz_ore::retry::Retry;
use mz_proto::{ProtoType, RustType};
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem};
use mz_repr::role_id::RoleId;
use mz_repr::GlobalId;
use mz_sql::catalog::{
    CatalogError as SqlCatalogError, CatalogItemType, DefaultPrivilegeAclItem,
    DefaultPrivilegeObject, ObjectType, RoleAttributes, RoleMembership,
};
use mz_sql::names::{
    CommentObjectId, DatabaseId, ItemQualifiers, QualifiedItemName, ResolvedDatabaseSpecifier,
    SchemaId, SchemaSpecifier,
};
use mz_sql_parser::ast::QualifiedReplica;
use mz_stash::objects::proto;
use mz_stash::{AppendBatch, Stash, StashError, TableTransaction, TypedCollection};
use mz_storage_client::types::sources::Timeline;
use proptest_derive::Arbitrary;

use crate::catalog::builtin::{
    BuiltinLog, BUILTIN_CLUSTERS, BUILTIN_CLUSTER_REPLICAS, BUILTIN_PREFIXES,
};
use crate::catalog::error::{Error, ErrorKind};
use crate::catalog::storage::stash::DEPLOY_GENERATION;
use crate::catalog::{is_reserved_name, ClusterConfig, ClusterVariant};

pub mod objects;
pub mod stash;

pub use stash::{
    AUDIT_LOG_COLLECTION, CLUSTER_COLLECTION, CLUSTER_INTROSPECTION_SOURCE_INDEX_COLLECTION,
    CLUSTER_REPLICA_COLLECTION, COMMENTS_COLLECTION, CONFIG_COLLECTION, DATABASES_COLLECTION,
    DEFAULT_PRIVILEGES_COLLECTION, ID_ALLOCATOR_COLLECTION, ITEM_COLLECTION, ROLES_COLLECTION,
    SCHEMAS_COLLECTION, SETTING_COLLECTION, STORAGE_USAGE_COLLECTION,
    SYSTEM_CONFIGURATION_COLLECTION, SYSTEM_GID_MAPPING_COLLECTION, SYSTEM_PRIVILEGES_COLLECTION,
    TIMESTAMP_COLLECTION,
};

pub const MZ_SYSTEM_ROLE_ID: RoleId = RoleId::System(1);
pub const MZ_SUPPORT_ROLE_ID: RoleId = RoleId::System(2);

const DATABASE_ID_ALLOC_KEY: &str = "database";
const SCHEMA_ID_ALLOC_KEY: &str = "schema";
const USER_ROLE_ID_ALLOC_KEY: &str = "user_role";
const USER_CLUSTER_ID_ALLOC_KEY: &str = "user_compute";
const SYSTEM_CLUSTER_ID_ALLOC_KEY: &str = "system_compute";
const USER_REPLICA_ID_ALLOC_KEY: &str = "replica";
const SYSTEM_REPLICA_ID_ALLOC_KEY: &str = "system_replica";
pub(crate) const AUDIT_LOG_ID_ALLOC_KEY: &str = "auditlog";
pub(crate) const STORAGE_USAGE_ID_ALLOC_KEY: &str = "storage_usage";

fn add_new_builtin_clusters_migration(txn: &mut Transaction<'_>) -> Result<(), Error> {
    let cluster_names: BTreeSet<_> = txn
        .clusters
        .items()
        .into_values()
        .map(|value| value.name)
        .collect();

    for builtin_cluster in &*BUILTIN_CLUSTERS {
        assert!(
            is_reserved_name(builtin_cluster.name),
            "builtin cluster {builtin_cluster:?} must start with one of the following prefixes {}",
            BUILTIN_PREFIXES.join(", ")
        );
        if !cluster_names.contains(builtin_cluster.name) {
            let id = txn.get_and_increment_id(SYSTEM_CLUSTER_ID_ALLOC_KEY.to_string())?;
            let id = ClusterId::System(id);
            txn.insert_system_cluster(
                id,
                builtin_cluster.name,
                &vec![],
                builtin_cluster.privileges.clone(),
                ClusterConfig {
                    // TODO: Should builtin clusters be managed or unmanaged?
                    variant: ClusterVariant::Unmanaged,
                },
            )?;
        }
    }
    Ok(())
}

fn add_new_builtin_cluster_replicas_migration(
    txn: &mut Transaction<'_>,
    bootstrap_args: &BootstrapArgs,
) -> Result<(), Error> {
    let cluster_lookup: BTreeMap<_, _> = txn
        .clusters
        .items()
        .into_iter()
        .map(|(key, value)| (value.name, key.id))
        .collect();

    let replicas: BTreeMap<_, _> =
        txn.cluster_replicas
            .items()
            .into_values()
            .fold(BTreeMap::new(), |mut acc, value| {
                acc.entry(value.cluster_id)
                    .or_insert_with(BTreeSet::new)
                    .insert(value.name);
                acc
            });

    for builtin_replica in &*BUILTIN_CLUSTER_REPLICAS {
        let cluster_id = cluster_lookup
            .get(builtin_replica.cluster_name)
            .expect("builtin cluster replica references non-existent cluster");

        let replica_names = replicas.get(cluster_id);
        if matches!(replica_names, None)
            || matches!(replica_names, Some(names) if !names.contains(builtin_replica.name))
        {
            let replica_id = txn.get_and_increment_id(SYSTEM_REPLICA_ID_ALLOC_KEY.to_string())?;
            let replica_id = ReplicaId::System(replica_id);
            let config = builtin_cluster_replica_config(bootstrap_args);
            txn.insert_cluster_replica(
                *cluster_id,
                replica_id,
                builtin_replica.name,
                &config,
                MZ_SYSTEM_ROLE_ID,
            )?;
        }
    }
    Ok(())
}

fn builtin_cluster_replica_config(bootstrap_args: &BootstrapArgs) -> ReplicaConfig {
    ReplicaConfig {
        location: ReplicaLocation::Managed {
            size: bootstrap_args.builtin_cluster_replica_size.clone(),
            availability_zone: None,
            disk: false,
        },
        logging: default_logging_config(),
        idle_arrangement_merge_effort: None,
    }
}

fn default_logging_config() -> ReplicaLogging {
    ReplicaLogging {
        log_logging: false,
        interval: Some(Duration::from_secs(1)),
    }
}

#[derive(Clone, Debug)]
pub struct BootstrapArgs {
    pub default_cluster_replica_size: String,
    pub builtin_cluster_replica_size: String,
    pub bootstrap_role: Option<String>,
}

/// A [`Connection`] represent an open connection to the stash. It exposes optimized methods for
/// executing a single operation against the stash. If the consumer needs to execute multiple
/// operations atomically, then they should start a transaction via [`Connection::transaction`].
///
/// The [`super::Catalog`] should never interact directly through the stash, instead it should
/// always interact through a [`Connection`].
#[derive(Debug)]
pub struct Connection {
    stash: Stash,
}

impl Connection {
    /// Opens a new [`Connection`] to the stash. Optionally initialize the stash if it has not
    /// been initialized and perform any migrations needed.
    #[tracing::instrument(name = "storage::open", level = "info", skip_all)]
    pub async fn open(
        mut stash: Stash,
        now: NowFn,
        bootstrap_args: &BootstrapArgs,
        deploy_generation: Option<u64>,
    ) -> Result<Connection, Error> {
        let retry = Retry::default()
            .clamp_backoff(Duration::from_secs(1))
            .max_duration(Duration::from_secs(30))
            .into_retry_stream();
        let mut retry = pin::pin!(retry);
        loop {
            match Self::open_inner(stash, now.clone(), bootstrap_args, deploy_generation).await {
                Ok(conn) => {
                    return Ok(conn);
                }
                Err((given_stash, err)) => {
                    stash = given_stash;
                    let should_retry =
                        matches!(&err, Error{ kind: ErrorKind::Stash(se)} if se.should_retry());
                    if !should_retry || retry.next().await.is_none() {
                        return Err(err);
                    }
                }
            }
        }
    }

    #[tracing::instrument(name = "storage::open_inner", level = "info", skip_all)]
    async fn open_inner(
        mut stash: Stash,
        now: NowFn,
        bootstrap_args: &BootstrapArgs,
        deploy_generation: Option<u64>,
    ) -> Result<Connection, (Stash, Error)> {
        // Initialize the Stash if it hasn't been already
        let mut conn = if !match stash.is_initialized().await {
            Ok(is_init) => is_init,
            Err(e) => {
                return Err((stash, e.into()));
            }
        } {
            // Get the current timestamp so we can record when we booted. We don't have to worry
            // about `boot_ts` being less than a previously used timestamp because the stash is
            // uninitialized and there are no previous timestamps.
            let boot_ts = now();

            // Initialize the Stash
            let args = bootstrap_args.clone();
            match stash
                .with_transaction(move |mut tx| {
                    Box::pin(async move {
                        stash::initialize(&mut tx, &args, boot_ts, deploy_generation).await
                    })
                })
                .await
            {
                Ok(()) => {}
                Err(e) => return Err((stash, e.into())),
            }

            Connection { stash }
        } else {
            // Before we do anything with the Stash, we need to run any pending upgrades and
            // initialize new collections.
            if !stash.is_readonly() {
                match stash.upgrade().await {
                    Ok(()) => {}
                    Err(e) => {
                        return Err((stash, e.into()));
                    }
                }
            }

            let mut conn = Connection { stash };

            if !conn.stash.is_readonly() {
                if let Some(deploy_generation) = deploy_generation {
                    match conn.persist_deploy_generation(deploy_generation).await {
                        Ok(()) => {}
                        Err(e) => {
                            return Err((conn.stash, e));
                        }
                    }
                }
            }

            conn
        };

        // Add any new builtin Clusters or Cluster Replicas that may be newly defined.
        if !conn.stash.is_readonly() {
            let mut txn = match transaction(&mut conn.stash).await {
                Ok(txn) => txn,
                Err(e) => {
                    return Err((conn.stash, e));
                }
            };

            match add_new_builtin_clusters_migration(&mut txn) {
                Ok(()) => {}
                Err(e) => {
                    return Err((conn.stash, e));
                }
            }
            match add_new_builtin_cluster_replicas_migration(&mut txn, bootstrap_args) {
                Ok(()) => {}
                Err(e) => {
                    return Err((conn.stash, e));
                }
            }
            match txn.commit().await {
                Ok(()) => {}
                Err(e) => {
                    return Err((conn.stash, e));
                }
            }
        }

        Ok(conn)
    }

    pub(crate) async fn set_connect_timeout(&mut self, connect_timeout: Duration) {
        self.stash.set_connect_timeout(connect_timeout).await;
    }

    pub(crate) fn is_read_only(&self) -> bool {
        self.stash.is_readonly()
    }

    async fn get_setting(&mut self, key: &str) -> Result<Option<String>, Error> {
        let v = SETTING_COLLECTION
            .peek_key_one(
                &mut self.stash,
                proto::SettingKey {
                    name: key.to_string(),
                },
            )
            .await?;
        Ok(v.map(|v| v.value))
    }

    async fn set_setting(&mut self, key: &str, value: &str) -> Result<(), Error> {
        let key = proto::SettingKey {
            name: key.to_string(),
        };
        let value = proto::SettingValue {
            value: value.into(),
        };
        SETTING_COLLECTION
            .upsert(&mut self.stash, once((key, value)))
            .await
            .map_err(|e| e.into())
    }

    pub async fn get_catalog_content_version(&mut self) -> Result<Option<String>, Error> {
        self.get_setting("catalog_content_version").await
    }

    pub async fn set_catalog_content_version(&mut self, new_version: &str) -> Result<(), Error> {
        self.set_setting("catalog_content_version", new_version)
            .await
    }

    #[tracing::instrument(level = "info", skip_all)]
    pub async fn load_databases(&mut self) -> Result<Vec<Database>, Error> {
        let entries = DATABASES_COLLECTION.peek_one(&mut self.stash).await?;
        let databases = entries
            .into_iter()
            .map(RustType::from_proto)
            .map_ok(|(k, v): (DatabaseKey, DatabaseValue)| Database {
                id: k.id,
                name: v.name,
                owner_id: v.owner_id,
                privileges: v.privileges,
            })
            .collect::<Result<_, _>>()?;

        Ok(databases)
    }

    #[tracing::instrument(level = "info", skip_all)]
    pub async fn load_schemas(&mut self) -> Result<Vec<Schema>, Error> {
        let entries = SCHEMAS_COLLECTION.peek_one(&mut self.stash).await?;
        let schemas = entries
            .into_iter()
            .map(RustType::from_proto)
            .map_ok(|(k, v): (SchemaKey, SchemaValue)| Schema {
                id: k.id,
                name: v.name,
                database_id: v.database_id,
                owner_id: v.owner_id,
                privileges: v.privileges,
            })
            .collect::<Result<_, _>>()?;

        Ok(schemas)
    }

    #[tracing::instrument(level = "info", skip_all)]
    pub async fn load_roles(&mut self) -> Result<Vec<Role>, Error> {
        let entries = ROLES_COLLECTION.peek_one(&mut self.stash).await?;
        let roles = entries
            .into_iter()
            .map(RustType::from_proto)
            .map_ok(|(k, v): (RoleKey, RoleValue)| Role {
                id: k.id,
                name: v.name,
                attributes: v.attributes,
                membership: v.membership,
            })
            .collect::<Result<_, _>>()?;

        Ok(roles)
    }

    #[tracing::instrument(level = "info", skip_all)]
    pub async fn load_clusters(&mut self) -> Result<Vec<Cluster>, Error> {
        let entries = CLUSTER_COLLECTION.peek_one(&mut self.stash).await?;
        let clusters = entries
            .into_iter()
            .map(RustType::from_proto)
            .map_ok(|(k, v): (ClusterKey, ClusterValue)| Cluster {
                id: k.id,
                name: v.name,
                linked_object_id: v.linked_object_id,
                owner_id: v.owner_id,
                privileges: v.privileges,
                config: v.config,
            })
            .collect::<Result<_, _>>()?;

        Ok(clusters)
    }

    #[tracing::instrument(level = "info", skip_all)]
    pub async fn load_cluster_replicas(&mut self) -> Result<Vec<ClusterReplica>, Error> {
        let entries = CLUSTER_REPLICA_COLLECTION.peek_one(&mut self.stash).await?;
        let replicas = entries
            .into_iter()
            .map(RustType::from_proto)
            .map_ok(
                |(k, v): (ClusterReplicaKey, ClusterReplicaValue)| ClusterReplica {
                    cluster_id: v.cluster_id,
                    replica_id: k.id,
                    name: v.name,
                    config: v.config,
                    owner_id: v.owner_id,
                },
            )
            .collect::<Result<_, _>>()?;

        Ok(replicas)
    }

    #[tracing::instrument(level = "info", skip_all)]
    pub async fn load_audit_log(&mut self) -> Result<impl Iterator<Item = VersionedEvent>, Error> {
        let entries = AUDIT_LOG_COLLECTION.peek_one(&mut self.stash).await?;
        let logs: Vec<_> = entries
            .into_keys()
            .map(AuditLogKey::from_proto)
            .map_ok(|e| e.event)
            .collect::<Result<_, _>>()?;

        Ok(logs.into_iter())
    }

    /// Loads storage usage events and permanently deletes from the stash those
    /// that happened more than the retention period ago from boot_ts.
    #[tracing::instrument(level = "info", skip_all)]
    pub async fn fetch_and_prune_storage_usage(
        &mut self,
        retention_period: Option<Duration>,
        boot_ts: mz_repr::Timestamp,
    ) -> Result<Vec<VersionedStorageUsage>, Error> {
        // If no usage retention period is set, set the cutoff to MIN so nothing
        // is removed.
        let cutoff_ts = match retention_period {
            None => u128::MIN,
            Some(period) => u128::from(boot_ts).saturating_sub(period.as_millis()),
        };
        let is_read_only = self.is_read_only();
        Ok(self
            .stash
            .with_transaction(move |tx| {
                Box::pin(async move {
                    let collection = STORAGE_USAGE_COLLECTION.from_tx(&tx).await?;
                    let rows = tx.peek_one(collection).await?;
                    let mut events = Vec::with_capacity(rows.len());
                    let mut batch = collection.make_batch_tx(&tx).await?;
                    for ev in rows.into_keys() {
                        let event: StorageUsageKey = ev.clone().into_rust()?;
                        if u128::from(event.metric.timestamp()) >= cutoff_ts {
                            events.push(event.metric);
                        } else if retention_period.is_some() {
                            collection.append_to_batch(&mut batch, &ev, &(), -1);
                        }
                    }
                    // Delete things only if a retention period is
                    // specified (otherwise opening readonly catalogs
                    // can fail).
                    if retention_period.is_some() && !is_read_only {
                        tx.append(vec![batch]).await?;
                    }
                    Ok(events)
                })
            })
            .await?)
    }

    /// Load the persisted mapping of system object to global ID. Key is (schema-name, object-name).
    #[tracing::instrument(level = "info", skip_all)]
    pub async fn load_system_gids(
        &mut self,
    ) -> Result<BTreeMap<(String, CatalogItemType, String), (GlobalId, String)>, Error> {
        let entries = SYSTEM_GID_MAPPING_COLLECTION
            .peek_one(&mut self.stash)
            .await?;
        let system_gid_mappings = entries
            .into_iter()
            .map(RustType::from_proto)
            .map_ok(|(k, v): (GidMappingKey, GidMappingValue)| {
                (
                    (k.schema_name, k.object_type, k.object_name),
                    (GlobalId::System(v.id), v.fingerprint),
                )
            })
            .collect::<Result<_, _>>()?;

        Ok(system_gid_mappings)
    }

    #[tracing::instrument(level = "info", skip_all)]
    pub async fn load_introspection_source_index_gids(
        &mut self,
        cluster_id: ClusterId,
    ) -> Result<BTreeMap<String, GlobalId>, Error> {
        let entries = CLUSTER_INTROSPECTION_SOURCE_INDEX_COLLECTION
            .peek_one(&mut self.stash)
            .await?;
        let sources = entries
            .into_iter()
            .map(RustType::from_proto)
            .filter_map_ok(
                |(k, v): (
                    ClusterIntrospectionSourceIndexKey,
                    ClusterIntrospectionSourceIndexValue,
                )| {
                    if k.cluster_id == cluster_id {
                        Some((k.name, GlobalId::System(v.index_id)))
                    } else {
                        None
                    }
                },
            )
            .collect::<Result<_, _>>()?;

        Ok(sources)
    }

    /// Load the persisted default privileges.
    #[tracing::instrument(level = "info", skip_all)]
    pub async fn load_default_privileges(
        &mut self,
    ) -> Result<Vec<(DefaultPrivilegeObject, DefaultPrivilegeAclItem)>, Error> {
        Ok(DEFAULT_PRIVILEGES_COLLECTION
            .peek_one(&mut self.stash)
            .await?
            .into_iter()
            .map(RustType::from_proto)
            .map_ok(|(k, v): (DefaultPrivilegesKey, DefaultPrivilegesValue)| {
                (
                    DefaultPrivilegeObject::new(
                        k.role_id,
                        k.database_id,
                        k.schema_id,
                        k.object_type,
                    ),
                    DefaultPrivilegeAclItem::new(k.grantee, v.privileges),
                )
            })
            .collect::<Result<_, _>>()?)
    }

    /// Load the persisted system privileges.
    #[tracing::instrument(level = "info", skip_all)]
    pub async fn load_system_privileges(&mut self) -> Result<Vec<MzAclItem>, Error> {
        Ok(SYSTEM_PRIVILEGES_COLLECTION
            .peek_one(&mut self.stash)
            .await?
            .into_iter()
            .map(RustType::from_proto)
            .map_ok(
                |(k, v): (SystemPrivilegesKey, SystemPrivilegesValue)| MzAclItem {
                    grantee: k.grantee,
                    grantor: k.grantor,
                    acl_mode: v.acl_mode,
                },
            )
            .collect::<Result<_, _>>()?)
    }

    /// Load the persisted server configurations.
    #[tracing::instrument(level = "info", skip_all)]
    pub async fn load_system_configuration(&mut self) -> Result<BTreeMap<String, String>, Error> {
        SYSTEM_CONFIGURATION_COLLECTION
            .peek_one(&mut self.stash)
            .await?
            .into_iter()
            .map(|(k, v)| Ok((k.name, v.value)))
            .collect()
    }

    /// Load all comments.
    #[tracing::instrument(level = "info", skip_all)]
    pub async fn load_comments(
        &mut self,
    ) -> Result<Vec<(CommentObjectId, Option<usize>, String)>, Error> {
        let comments = COMMENTS_COLLECTION
            .peek_one(&mut self.stash)
            .await?
            .into_iter()
            .map(RustType::from_proto)
            .map_ok(|(k, v): (CommentKey, CommentValue)| (k.object_id, k.sub_component, v.comment))
            .collect::<Result<_, _>>()?;

        Ok(comments)
    }

    /// Persist mapping from system objects to global IDs and fingerprints.
    ///
    /// Panics if provided id is not a system id.
    pub async fn set_system_object_mapping(
        &mut self,
        mappings: Vec<SystemObjectMapping>,
    ) -> Result<(), Error> {
        if mappings.is_empty() {
            return Ok(());
        }

        let mappings = mappings
            .into_iter()
            .map(|mapping| {
                let id = if let GlobalId::System(id) = mapping.id {
                    id
                } else {
                    panic!("non-system id provided")
                };
                (
                    GidMappingKey {
                        schema_name: mapping.schema_name,
                        object_type: mapping.object_type,
                        object_name: mapping.object_name,
                    },
                    GidMappingValue {
                        id,
                        fingerprint: mapping.fingerprint,
                    },
                )
            })
            .map(|e| RustType::into_proto(&e));
        SYSTEM_GID_MAPPING_COLLECTION
            .upsert(&mut self.stash, mappings)
            .await
            .map_err(|e| e.into())
    }

    /// Panics if provided id is not a system id
    pub async fn set_introspection_source_index_gids(
        &mut self,
        mappings: Vec<(ClusterId, &str, GlobalId)>,
    ) -> Result<(), Error> {
        if mappings.is_empty() {
            return Ok(());
        }

        let mappings = mappings
            .into_iter()
            .map(|(cluster_id, name, index_id)| {
                let index_id = if let GlobalId::System(id) = index_id {
                    id
                } else {
                    panic!("non-system id provided")
                };
                (
                    ClusterIntrospectionSourceIndexKey {
                        cluster_id,
                        name: name.to_string(),
                    },
                    ClusterIntrospectionSourceIndexValue { index_id },
                )
            })
            .map(|e| RustType::into_proto(&e));
        CLUSTER_INTROSPECTION_SOURCE_INDEX_COLLECTION
            .upsert(&mut self.stash, mappings)
            .await
            .map_err(|e| e.into())
    }

    /// Set the configuration of a replica.
    /// This accepts only one item, as we currently use this only for the default cluster
    pub async fn set_replica_config(
        &mut self,
        replica_id: ReplicaId,
        cluster_id: ClusterId,
        name: String,
        config: ReplicaConfig,
        owner_id: RoleId,
    ) -> Result<(), Error> {
        let key = ClusterReplicaKey { id: replica_id }.into_proto();
        let val = ClusterReplicaValue {
            cluster_id,
            name,
            config,
            owner_id,
        }
        .into_proto();
        CLUSTER_REPLICA_COLLECTION
            .upsert_key(&mut self.stash, key, |_| Ok::<_, Error>(val))
            .await??;
        Ok(())
    }

    pub async fn allocate_system_ids(&mut self, amount: u64) -> Result<Vec<GlobalId>, Error> {
        let id = self.allocate_id("system", amount).await?;

        Ok(id.into_iter().map(GlobalId::System).collect())
    }

    pub async fn allocate_user_id(&mut self) -> Result<GlobalId, Error> {
        let id = self.allocate_id("user", 1).await?;
        let id = id.into_element();
        Ok(GlobalId::User(id))
    }

    pub async fn allocate_system_cluster_id(&mut self) -> Result<ClusterId, Error> {
        let id = self.allocate_id(SYSTEM_CLUSTER_ID_ALLOC_KEY, 1).await?;
        let id = id.into_element();
        Ok(ClusterId::System(id))
    }

    pub async fn allocate_user_cluster_id(&mut self) -> Result<ClusterId, Error> {
        let id = self.allocate_id(USER_CLUSTER_ID_ALLOC_KEY, 1).await?;
        let id = id.into_element();
        Ok(ClusterId::User(id))
    }

    pub async fn allocate_user_replica_id(&mut self) -> Result<ReplicaId, Error> {
        let id = self.allocate_id(USER_REPLICA_ID_ALLOC_KEY, 1).await?;
        let id = id.into_element();
        Ok(ReplicaId::User(id))
    }

    /// Get the next system replica id without allocating it.
    pub async fn get_next_system_replica_id(&mut self) -> Result<u64, Error> {
        self.get_next_id(SYSTEM_REPLICA_ID_ALLOC_KEY).await
    }

    /// Get the next user replica id without allocating it.
    pub async fn get_next_user_replica_id(&mut self) -> Result<u64, Error> {
        self.get_next_id(USER_REPLICA_ID_ALLOC_KEY).await
    }

    async fn get_next_id(&mut self, id_type: &str) -> Result<u64, Error> {
        ID_ALLOCATOR_COLLECTION
            .peek_key_one(
                &mut self.stash,
                IdAllocKey {
                    name: id_type.to_string(),
                }
                .into_proto(),
            )
            .await
            .map(|x| x.expect("must exist").next_id)
            .map_err(Into::into)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn allocate_id(&mut self, id_type: &str, amount: u64) -> Result<Vec<u64>, Error> {
        if amount == 0 {
            return Ok(Vec::new());
        }
        let key = IdAllocKey {
            name: id_type.to_string(),
        }
        .into_proto();
        let (prev, next) = ID_ALLOCATOR_COLLECTION
            .upsert_key(&mut self.stash, key, move |prev| {
                let id = prev.expect("must exist").next_id;
                match id.checked_add(amount) {
                    Some(next_gid) => Ok(IdAllocValue { next_id: next_gid }.into_proto()),
                    None => Err(Error::new(ErrorKind::IdExhaustion)),
                }
            })
            .await??;
        let id = prev.expect("must exist").next_id;
        Ok((id..next.next_id).collect())
    }

    /// Gets a global timestamp for a timeline that has been persisted to disk.
    ///
    /// Returns `None` if no persisted timestamp for the specified timeline exists.
    pub async fn try_get_persisted_timestamp(
        &mut self,
        timeline: &Timeline,
    ) -> Result<Option<mz_repr::Timestamp>, Error> {
        let key = proto::TimestampKey {
            id: timeline.to_string(),
        };
        let val: Option<TimestampValue> = TIMESTAMP_COLLECTION
            .peek_key_one(&mut self.stash, key)
            .await?
            .map(RustType::from_proto)
            .transpose()?;

        Ok(val.map(|v| v.ts))
    }

    /// Get all global timestamps that has been persisted to disk.
    pub async fn get_all_persisted_timestamps(
        &mut self,
    ) -> Result<BTreeMap<Timeline, mz_repr::Timestamp>, Error> {
        let entries = TIMESTAMP_COLLECTION.peek_one(&mut self.stash).await?;
        let timestamps = entries
            .into_iter()
            .map(RustType::from_proto)
            .map_ok(|(k, v): (TimestampKey, TimestampValue)| {
                (k.id.parse().expect("invalid timeline persisted"), v.ts)
            })
            .collect::<Result<_, _>>()?;

        Ok(timestamps)
    }

    /// Persist new global timestamp for a timeline to disk.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn persist_timestamp(
        &mut self,
        timeline: &Timeline,
        timestamp: mz_repr::Timestamp,
    ) -> Result<(), Error> {
        let key = proto::TimestampKey {
            id: timeline.to_string(),
        };
        let (prev, next) = TIMESTAMP_COLLECTION
            .upsert_key(&mut self.stash, key, move |_| {
                Ok::<_, Error>(TimestampValue { ts: timestamp }.into_proto())
            })
            .await??;
        if let Some(prev) = prev {
            assert!(next >= prev, "global timestamp must always go up");
        }
        Ok(())
    }

    pub async fn persist_deploy_generation(&mut self, deploy_generation: u64) -> Result<(), Error> {
        CONFIG_COLLECTION
            .upsert_key(
                &mut self.stash,
                proto::ConfigKey {
                    key: DEPLOY_GENERATION.into(),
                },
                move |_| {
                    Ok::<_, Error>(proto::ConfigValue {
                        value: deploy_generation,
                    })
                },
            )
            .await??;
        Ok(())
    }

    /// Creates a new [`Transaction`].
    pub async fn transaction<'a>(&'a mut self) -> Result<Transaction<'a>, Error> {
        transaction(&mut self.stash).await
    }

    /// Confirms that this [`Connection`] is connected as the stash leader.
    pub async fn confirm_leadership(&mut self) -> Result<(), Error> {
        Ok(self.stash.confirm_leadership().await?)
    }
}

#[tracing::instrument(name = "storage::transaction", level = "debug", skip_all)]
async fn transaction<'a>(stash: &'a mut Stash) -> Result<Transaction<'a>, Error> {
    let (
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
        system_gid_mapping,
        system_configurations,
        default_privileges,
        system_privileges,
    ) = stash
        .with_transaction(|tx| {
            Box::pin(async move {
                // Peek the catalog collections in any order and a single transaction.
                futures::try_join!(
                    tx.peek_one(tx.collection(DATABASES_COLLECTION.name()).await?),
                    tx.peek_one(tx.collection(SCHEMAS_COLLECTION.name()).await?),
                    tx.peek_one(tx.collection(ROLES_COLLECTION.name()).await?),
                    tx.peek_one(tx.collection(ITEM_COLLECTION.name()).await?),
                    tx.peek_one(tx.collection(COMMENTS_COLLECTION.name()).await?),
                    tx.peek_one(tx.collection(CLUSTER_COLLECTION.name()).await?),
                    tx.peek_one(tx.collection(CLUSTER_REPLICA_COLLECTION.name()).await?),
                    tx.peek_one(
                        tx.collection(CLUSTER_INTROSPECTION_SOURCE_INDEX_COLLECTION.name())
                            .await?,
                    ),
                    tx.peek_one(tx.collection(ID_ALLOCATOR_COLLECTION.name()).await?),
                    tx.peek_one(tx.collection(CONFIG_COLLECTION.name()).await?),
                    tx.peek_one(tx.collection(SETTING_COLLECTION.name()).await?),
                    tx.peek_one(tx.collection(TIMESTAMP_COLLECTION.name()).await?),
                    tx.peek_one(tx.collection(SYSTEM_GID_MAPPING_COLLECTION.name()).await?),
                    tx.peek_one(
                        tx.collection(SYSTEM_CONFIGURATION_COLLECTION.name())
                            .await?
                    ),
                    tx.peek_one(tx.collection(DEFAULT_PRIVILEGES_COLLECTION.name()).await?),
                    tx.peek_one(tx.collection(SYSTEM_PRIVILEGES_COLLECTION.name()).await?),
                )
            })
        })
        .await?;

    Ok(Transaction {
        stash,
        databases: TableTransaction::new(databases, |a: &DatabaseValue, b| a.name == b.name)?,
        schemas: TableTransaction::new(schemas, |a: &SchemaValue, b| {
            a.database_id == b.database_id && a.name == b.name
        })?,
        items: TableTransaction::new(items, |a: &ItemValue, b| {
            a.schema_id == b.schema_id && a.name == b.name
        })?,
        comments: TableTransaction::new(comments, |a: &CommentValue, b| a.comment == b.comment)?,
        roles: TableTransaction::new(roles, |a: &RoleValue, b| a.name == b.name)?,
        clusters: TableTransaction::new(clusters, |a: &ClusterValue, b| a.name == b.name)?,
        cluster_replicas: TableTransaction::new(cluster_replicas, |a: &ClusterReplicaValue, b| {
            a.cluster_id == b.cluster_id && a.name == b.name
        })?,
        introspection_sources: TableTransaction::new(introspection_sources, |_a, _b| false)?,
        id_allocator: TableTransaction::new(id_allocator, |_a, _b| false)?,
        configs: TableTransaction::new(configs, |_a, _b| false)?,
        settings: TableTransaction::new(settings, |_a, _b| false)?,
        timestamps: TableTransaction::new(timestamps, |_a, _b| false)?,
        system_gid_mapping: TableTransaction::new(system_gid_mapping, |_a, _b| false)?,
        system_configurations: TableTransaction::new(system_configurations, |_a, _b| false)?,
        default_privileges: TableTransaction::new(default_privileges, |_a, _b| false)?,
        system_privileges: TableTransaction::new(system_privileges, |_a, _b| false)?,
        audit_log_updates: Vec::new(),
        storage_usage_updates: Vec::new(),
    })
}

/// A [`Transaction`] batches multiple [`Connection`] operations together and commits them
/// atomically.
pub struct Transaction<'a> {
    stash: &'a mut Stash,
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
    // Don't make this a table transaction so that it's not read into the stash
    // memory cache.
    audit_log_updates: Vec<(proto::AuditLogKey, (), i64)>,
    storage_usage_updates: Vec<(proto::StorageUsageKey, (), i64)>,
}

impl<'a> Transaction<'a> {
    pub(crate) fn loaded_items(&self) -> Vec<Item> {
        let databases = self.databases.items();
        let schemas = self.schemas.items();
        let mut items = Vec::new();
        self.items.for_values(|k, v| {
            let schema_key = SchemaKey { id: v.schema_id };
            let schema = match schemas.get(&schema_key) {
                Some(schema) => schema,
                None => panic!(
                    "corrupt stash! unknown schema id {}, for item with key \
                        {k:?} and value {v:?}",
                    v.schema_id
                ),
            };
            let database_spec = match schema.database_id {
                Some(id) => {
                    let key = DatabaseKey { id };
                    if databases.get(&key).is_none() {
                        panic!(
                            "corrupt stash! unknown database id {key:?}, for item with key \
                        {k:?} and value {v:?}"
                        );
                    }
                    ResolvedDatabaseSpecifier::from(id)
                }
                None => ResolvedDatabaseSpecifier::Ambient,
            };
            items.push(Item {
                id: k.gid,
                name: QualifiedItemName {
                    qualifiers: ItemQualifiers {
                        database_spec,
                        schema_spec: SchemaSpecifier::from(v.schema_id),
                    },
                    item: v.name.clone(),
                },
                create_sql: v.create_sql.clone(),
                owner_id: v.owner_id,
                privileges: v.privileges.clone(),
            });
        });
        items.sort_by_key(|Item { id, .. }| *id);
        items
    }

    pub(crate) fn insert_audit_log_event(&mut self, event: VersionedEvent) {
        self.audit_log_updates
            .push((AuditLogKey { event }.into_proto(), (), 1));
    }

    pub(crate) fn insert_storage_usage_event(&mut self, metric: VersionedStorageUsage) {
        self.storage_usage_updates
            .push((StorageUsageKey { metric }.into_proto(), (), 1));
    }

    pub(crate) fn insert_user_database(
        &mut self,
        database_name: &str,
        owner_id: RoleId,
        privileges: Vec<MzAclItem>,
    ) -> Result<DatabaseId, Error> {
        let id = self.get_and_increment_id(DATABASE_ID_ALLOC_KEY.to_string())?;
        match self.databases.insert(
            DatabaseKey {
                // TODO(parkertimmerman): Support creating databases in the System namespace.
                id: DatabaseId::User(id),
            },
            DatabaseValue {
                name: database_name.to_string(),
                owner_id,
                privileges,
            },
        ) {
            // TODO(parkertimmerman): Support creating databases in the System namespace.
            Ok(_) => Ok(DatabaseId::User(id)),
            Err(_) => Err(Error::new(ErrorKind::DatabaseAlreadyExists(
                database_name.to_owned(),
            ))),
        }
    }

    pub(crate) fn insert_user_schema(
        &mut self,
        database_id: DatabaseId,
        schema_name: &str,
        owner_id: RoleId,
        privileges: Vec<MzAclItem>,
    ) -> Result<SchemaId, Error> {
        let id = self.get_and_increment_id(SCHEMA_ID_ALLOC_KEY.to_string())?;
        match self.schemas.insert(
            SchemaKey {
                // TODO(parkertimmerman): Support creating schemas in the System namespace.
                id: SchemaId::User(id),
            },
            SchemaValue {
                database_id: Some(database_id),
                name: schema_name.to_string(),
                owner_id,
                privileges,
            },
        ) {
            // TODO(parkertimmerman): Support creating schemas in the System namespace.
            Ok(_) => Ok(SchemaId::User(id)),
            Err(_) => Err(Error::new(ErrorKind::SchemaAlreadyExists(
                schema_name.to_owned(),
            ))),
        }
    }

    pub(crate) fn insert_user_role(
        &mut self,
        name: String,
        attributes: RoleAttributes,
        membership: RoleMembership,
    ) -> Result<RoleId, Error> {
        let id = self.get_and_increment_id(USER_ROLE_ID_ALLOC_KEY.to_string())?;
        let id = RoleId::User(id);
        match self.roles.insert(
            RoleKey { id },
            RoleValue {
                name: name.clone(),
                attributes,
                membership,
            },
        ) {
            Ok(_) => Ok(id),
            Err(_) => Err(Error::new(ErrorKind::RoleAlreadyExists(name))),
        }
    }

    /// Panics if any introspection source id is not a system id
    pub(crate) fn insert_user_cluster(
        &mut self,
        cluster_id: ClusterId,
        cluster_name: &str,
        linked_object_id: Option<GlobalId>,
        introspection_source_indexes: &Vec<(&'static BuiltinLog, GlobalId)>,
        owner_id: RoleId,
        privileges: Vec<MzAclItem>,
        config: ClusterConfig,
    ) -> Result<(), Error> {
        self.insert_cluster(
            cluster_id,
            cluster_name,
            linked_object_id,
            introspection_source_indexes,
            owner_id,
            privileges,
            config,
        )
    }

    /// Panics if any introspection source id is not a system id
    fn insert_system_cluster(
        &mut self,
        cluster_id: ClusterId,
        cluster_name: &str,
        introspection_source_indexes: &Vec<(&'static BuiltinLog, GlobalId)>,
        privileges: Vec<MzAclItem>,
        config: ClusterConfig,
    ) -> Result<(), Error> {
        self.insert_cluster(
            cluster_id,
            cluster_name,
            None,
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
        linked_object_id: Option<GlobalId>,
        introspection_source_indexes: &Vec<(&'static BuiltinLog, GlobalId)>,
        owner_id: RoleId,
        privileges: Vec<MzAclItem>,
        config: ClusterConfig,
    ) -> Result<(), Error> {
        if let Err(_) = self.clusters.insert(
            ClusterKey { id: cluster_id },
            ClusterValue {
                name: cluster_name.to_string(),
                linked_object_id,
                owner_id,
                privileges,
                config,
            },
        ) {
            return Err(Error::new(ErrorKind::ClusterAlreadyExists(
                cluster_name.to_owned(),
            )));
        };

        for (builtin, index_id) in introspection_source_indexes {
            let index_id = if let GlobalId::System(id) = index_id {
                *id
            } else {
                panic!("non-system id provided")
            };
            self.introspection_sources
                .insert(
                    ClusterIntrospectionSourceIndexKey {
                        cluster_id,
                        name: builtin.name.to_string(),
                    },
                    ClusterIntrospectionSourceIndexValue { index_id },
                )
                .expect("no uniqueness violation");
        }

        Ok(())
    }

    pub(crate) fn rename_cluster(
        &mut self,
        cluster_id: ClusterId,
        cluster_name: &str,
        cluster_to_name: &str,
    ) -> Result<(), Error> {
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

    pub(crate) fn check_migration_has_run(&mut self, name: String) -> Result<bool, Error> {
        let key = SettingKey { name };
        // If the key does not exist, then the migration has not been run.
        let has_run = self.settings.get(&key).as_ref().is_some();

        Ok(has_run)
    }

    pub(crate) fn mark_migration_has_run(&mut self, name: String) -> Result<(), Error> {
        let key = SettingKey { name };
        let val = SettingValue {
            value: true.to_string(),
        };
        self.settings.insert(key, val)?;

        Ok(())
    }

    pub(crate) fn rename_cluster_replica(
        &mut self,
        replica_id: ReplicaId,
        replica_name: &QualifiedReplica,
        replica_to_name: &str,
    ) -> Result<(), Error> {
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

    pub(crate) fn insert_cluster_replica(
        &mut self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        replica_name: &str,
        config: &ReplicaConfig,
        owner_id: RoleId,
    ) -> Result<(), Error> {
        if let Err(_) = self.cluster_replicas.insert(
            ClusterReplicaKey { id: replica_id },
            ClusterReplicaValue {
                cluster_id,
                name: replica_name.into(),
                config: config.clone(),
                owner_id,
            },
        ) {
            let cluster = self
                .clusters
                .get(&ClusterKey { id: cluster_id })
                .expect("cluster exists");
            return Err(Error::new(ErrorKind::DuplicateReplica(
                replica_name.to_string(),
                cluster.name.to_string(),
            )));
        };
        Ok(())
    }

    /// Updates persisted information about persisted introspection source
    /// indexes.
    ///
    /// Panics if provided id is not a system id.
    pub(crate) fn update_introspection_source_index_gids(
        &mut self,
        mappings: impl Iterator<Item = (ClusterId, impl Iterator<Item = (String, GlobalId)>)>,
    ) -> Result<(), Error> {
        for (cluster_id, updates) in mappings {
            for (name, id) in updates {
                let index_id = if let GlobalId::System(index_id) = id {
                    index_id
                } else {
                    panic!("Introspection source index should have a system id")
                };
                let prev = self.introspection_sources.set(
                    ClusterIntrospectionSourceIndexKey { cluster_id, name },
                    Some(ClusterIntrospectionSourceIndexValue { index_id }),
                )?;
                if prev.is_none() {
                    return Err(Error {
                        kind: ErrorKind::FailedBuiltinSchemaMigration(format!("{id}")),
                    });
                }
            }
        }
        Ok(())
    }

    pub(crate) fn insert_item(
        &mut self,
        id: GlobalId,
        schema_id: SchemaId,
        item_name: &str,
        create_sql: String,
        owner_id: RoleId,
        privileges: Vec<MzAclItem>,
    ) -> Result<(), Error> {
        match self.items.insert(
            ItemKey { gid: id },
            ItemValue {
                schema_id,
                name: item_name.to_string(),
                create_sql,
                owner_id,
                privileges,
            },
        ) {
            Ok(_) => Ok(()),
            Err(_) => Err(Error::new(ErrorKind::ItemAlreadyExists(
                id,
                item_name.to_owned(),
            ))),
        }
    }

    pub(crate) fn get_and_increment_id(&mut self, key: String) -> Result<u64, Error> {
        let id = self
            .id_allocator
            .items()
            .get(&IdAllocKey { name: key.clone() })
            .unwrap_or_else(|| panic!("{key} id allocator missing"))
            .next_id;
        let next_id = id
            .checked_add(1)
            .ok_or_else(|| Error::new(ErrorKind::IdExhaustion))?;
        let prev = self
            .id_allocator
            .set(IdAllocKey { name: key }, Some(IdAllocValue { next_id }))?;
        assert!(prev.is_some());
        Ok(id)
    }

    pub(crate) fn remove_database(&mut self, id: &DatabaseId) -> Result<(), Error> {
        let prev = self.databases.set(DatabaseKey { id: *id }, None)?;
        if prev.is_some() {
            Ok(())
        } else {
            Err(SqlCatalogError::UnknownDatabase(id.to_string()).into())
        }
    }

    pub(crate) fn remove_schema(
        &mut self,
        database_id: &Option<DatabaseId>,
        schema_id: &SchemaId,
    ) -> Result<(), Error> {
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

    pub(crate) fn remove_role(&mut self, name: &str) -> Result<(), Error> {
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

    pub(crate) fn remove_cluster(&mut self, id: ClusterId) -> Result<(), Error> {
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

    pub(crate) fn remove_cluster_replica(&mut self, id: ReplicaId) -> Result<(), Error> {
        let deleted = self.cluster_replicas.delete(|k, _v| k.id == id);
        if deleted.len() == 1 {
            Ok(())
        } else {
            assert!(deleted.is_empty());
            Err(SqlCatalogError::UnknownClusterReplica(id.to_string()).into())
        }
    }

    /// Removes item `id` from the transaction.
    ///
    /// Returns an error if `id` is not found.
    ///
    /// Runtime is linear with respect to the total number of items in the stash.
    /// DO NOT call this function in a loop, use [`Self::remove_items`] instead.
    pub(crate) fn remove_item(&mut self, id: GlobalId) -> Result<(), Error> {
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
    pub(crate) fn remove_items(&mut self, ids: BTreeSet<GlobalId>) -> Result<(), Error> {
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
    /// Runtime is linear with respect to the total number of items in the stash.
    /// DO NOT call this function in a loop, use [`Self::update_items`] instead.
    pub(crate) fn update_item(&mut self, id: GlobalId, item: Item) -> Result<(), Error> {
        let n = self.items.update(|k, v| {
            if k.gid == id {
                let item = item.clone();
                // Schema IDs cannot change.
                assert_eq!(
                    SchemaId::from(item.name.qualifiers.schema_spec),
                    v.schema_id
                );
                Some(ItemValue {
                    schema_id: v.schema_id,
                    name: item.name.item,
                    create_sql: item.create_sql,
                    owner_id: item.owner_id,
                    privileges: item.privileges,
                })
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
    pub(crate) fn update_items(&mut self, items: BTreeMap<GlobalId, Item>) -> Result<(), Error> {
        let n = self.items.update(|k, v| {
            if let Some(item) = items.get(&k.gid) {
                // Schema IDs cannot change.
                assert_eq!(
                    SchemaId::from(item.name.qualifiers.schema_spec),
                    v.schema_id
                );
                Some(ItemValue {
                    schema_id: v.schema_id,
                    name: item.name.item.clone(),
                    create_sql: item.create_sql.clone(),
                    owner_id: item.owner_id.clone(),
                    privileges: item.privileges.clone(),
                })
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
    /// Runtime is linear with respect to the total number of items in the stash.
    /// DO NOT call this function in a loop, implement and use some `Self::update_roles` instead.
    /// You should model it after [`Self::update_items`].
    pub(crate) fn update_role(&mut self, id: RoleId, role: Role) -> Result<(), Error> {
        let n = self.roles.update(move |k, _v| {
            if k.id == id {
                let role = role.clone();
                Some(RoleValue::from(role))
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
    pub(crate) fn update_system_object_mappings(
        &mut self,
        mappings: BTreeMap<GlobalId, SystemObjectMapping>,
    ) -> Result<(), Error> {
        let n = self.system_gid_mapping.update(|_k, v| {
            if let Some(mapping) = mappings.get(&GlobalId::System(v.id)) {
                let id = if let GlobalId::System(id) = mapping.id {
                    id
                } else {
                    panic!("non-system id provided")
                };
                Some(GidMappingValue {
                    id,
                    fingerprint: mapping.fingerprint.clone(),
                })
            } else {
                None
            }
        })?;

        if usize::try_from(n).expect("update diff should fit into usize") != mappings.len() {
            let id_str = mappings.keys().map(|id| id.to_string()).join(",");
            return Err(Error {
                kind: ErrorKind::FailedBuiltinSchemaMigration(id_str),
            });
        }

        Ok(())
    }

    /// Updates cluster `id` in the transaction to `cluster`.
    ///
    /// Returns an error if `id` is not found.
    ///
    /// Runtime is linear with respect to the total number of clusters in the stash.
    /// DO NOT call this function in a loop.
    pub(crate) fn update_cluster(&mut self, id: ClusterId, cluster: Cluster) -> Result<(), Error> {
        let n = self.clusters.update(|k, _v| {
            if k.id == id {
                let cluster = cluster.clone();
                Some(ClusterValue {
                    name: cluster.name,
                    linked_object_id: cluster.linked_object_id,
                    owner_id: cluster.owner_id,
                    privileges: cluster.privileges,
                    config: cluster.config,
                })
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
    /// Runtime is linear with respect to the total number of cluster replicas in the stash.
    /// DO NOT call this function in a loop.
    pub(crate) fn update_cluster_replica(
        &mut self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        replica: ClusterReplica,
    ) -> Result<(), Error> {
        let n = self.cluster_replicas.update(|k, _v| {
            if k.id == replica_id {
                let replica = replica.clone();
                Some(ClusterReplicaValue {
                    cluster_id,
                    name: replica.name,
                    config: replica.config,
                    owner_id: replica.owner_id,
                })
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
    /// Runtime is linear with respect to the total number of databases in the stash.
    /// DO NOT call this function in a loop.
    pub(crate) fn update_database(
        &mut self,
        id: DatabaseId,
        database: Database,
    ) -> Result<(), Error> {
        let n = self.databases.update(|k, _v| {
            if id == k.id {
                let database = database.clone();
                Some(DatabaseValue {
                    name: database.name,
                    owner_id: database.owner_id,
                    privileges: database.privileges,
                })
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
    /// Runtime is linear with respect to the total number of schemas in the stash.
    /// DO NOT call this function in a loop.
    pub(crate) fn update_schema(
        &mut self,
        database_id: Option<DatabaseId>,
        schema_id: SchemaId,
        schema: Schema,
    ) -> Result<(), Error> {
        let n = self.schemas.update(|k, _v| {
            if schema_id == k.id {
                let schema = schema.clone();
                Some(SchemaValue {
                    database_id,
                    name: schema.name,
                    owner_id: schema.owner_id,
                    privileges: schema.privileges,
                })
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
    pub(crate) fn set_default_privilege(
        &mut self,
        role_id: RoleId,
        database_id: Option<DatabaseId>,
        schema_id: Option<SchemaId>,
        object_type: ObjectType,
        grantee: RoleId,
        privileges: Option<AclMode>,
    ) -> Result<(), Error> {
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

    /// Set persisted system privilege.
    pub(crate) fn set_system_privilege(
        &mut self,
        grantee: RoleId,
        grantor: RoleId,
        acl_mode: Option<AclMode>,
    ) -> Result<(), Error> {
        self.system_privileges.set(
            SystemPrivilegesKey { grantee, grantor },
            acl_mode.map(|acl_mode| SystemPrivilegesValue { acl_mode }),
        )?;
        Ok(())
    }

    pub(crate) fn update_comment(
        &mut self,
        object_id: CommentObjectId,
        sub_component: Option<usize>,
        comment: Option<String>,
    ) -> Result<(), Error> {
        let key = CommentKey {
            object_id,
            sub_component,
        };
        let value = comment.map(|c| CommentValue { comment: c });
        self.comments.set(key, value)?;

        Ok(())
    }

    pub(crate) fn drop_comments(
        &mut self,
        object_id: CommentObjectId,
    ) -> Result<Vec<(CommentObjectId, Option<usize>, String)>, Error> {
        let deleted = self.comments.delete(|k, _v| k.object_id == object_id);
        let deleted = deleted
            .into_iter()
            .map(|(k, v)| (k.object_id, k.sub_component, v.comment))
            .collect();
        Ok(deleted)
    }

    /// Upserts persisted system configuration `name` to `value`.
    pub(crate) fn upsert_system_config(&mut self, name: &str, value: String) -> Result<(), Error> {
        let key = ServerConfigurationKey {
            name: name.to_string(),
        };
        let value = ServerConfigurationValue { value };
        self.system_configurations.set(key, Some(value))?;
        Ok(())
    }

    /// Removes persisted system configuration `name`.
    pub(crate) fn remove_system_config(&mut self, name: &str) {
        let key = ServerConfigurationKey {
            name: name.to_string(),
        };
        self.system_configurations
            .set(key, None)
            .expect("cannot have uniqueness violation");
    }

    /// Removes all persisted system configurations.
    pub(crate) fn clear_system_configs(&mut self) {
        self.system_configurations.delete(|_k, _v| true);
    }

    pub(crate) fn remove_timestamp(&mut self, timeline: Timeline) {
        let timeline_str = timeline.to_string();
        let prev = self
            .timestamps
            .set(TimestampKey { id: timeline_str }, None)
            .expect("cannot have uniqueness violation");
        assert!(prev.is_some());
    }

    /// Commits the storage transaction to the stash. Any error returned indicates the stash may be
    /// in an indeterminate state and needs to be fully re-read before proceeding. In general, this
    /// must be fatal to the calling process. We do not panic/halt inside this function itself so
    /// that errors can bubble up during initialization.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn commit(self) -> Result<(), Error> {
        self.commit_inner().await
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn commit_inner(self) -> Result<(), Error> {
        async fn add_batch<'tx, K, V>(
            tx: &'tx mz_stash::Transaction<'tx>,
            batches: &mut Vec<AppendBatch>,
            typed: &'tx TypedCollection<K, V>,
            changes: &[(K, V, mz_stash::Diff)],
        ) -> Result<(), StashError>
        where
            K: mz_stash::Data + 'tx,
            V: mz_stash::Data + 'tx,
        {
            if changes.is_empty() {
                return Ok(());
            }
            let collection = typed.from_tx(tx).await?;
            let mut batch = collection.make_batch_tx(tx).await?;
            for (k, v, diff) in changes {
                collection.append_to_batch(&mut batch, k, v, *diff);
            }
            batches.push(batch);
            Ok(())
        }

        // The with_transaction fn below requires a Fn that can be cloned,
        // meaning anything it closes over must be Clone. The .pending() here
        // return Vecs. Thus, the Arcs here aren't strictly necessary because
        // Vecs are Clone. However, using an Arc means that we never clone the
        // Vec (which would happen at least one time when the txn starts), and
        // instead only clone the Arc.
        let databases = Arc::new(self.databases.pending());
        let schemas = Arc::new(self.schemas.pending());
        let items = Arc::new(self.items.pending());
        let comments = Arc::new(self.comments.pending());
        let roles = Arc::new(self.roles.pending());
        let clusters = Arc::new(self.clusters.pending());
        let cluster_replicas = Arc::new(self.cluster_replicas.pending());
        let introspection_sources = Arc::new(self.introspection_sources.pending());
        let id_allocator = Arc::new(self.id_allocator.pending());
        let configs = Arc::new(self.configs.pending());
        let settings = Arc::new(self.settings.pending());
        let timestamps = Arc::new(self.timestamps.pending());
        let system_gid_mapping = Arc::new(self.system_gid_mapping.pending());
        let system_configurations = Arc::new(self.system_configurations.pending());
        let default_privileges = Arc::new(self.default_privileges.pending());
        let system_privileges = Arc::new(self.system_privileges.pending());
        let audit_log_updates = Arc::new(self.audit_log_updates);
        let storage_usage_updates = Arc::new(self.storage_usage_updates);

        self.stash
            .with_transaction(move |tx| {
                Box::pin(async move {
                    let mut batches = Vec::new();

                    add_batch(&tx, &mut batches, &DATABASES_COLLECTION, &databases).await?;
                    add_batch(&tx, &mut batches, &SCHEMAS_COLLECTION, &schemas).await?;
                    add_batch(&tx, &mut batches, &ITEM_COLLECTION, &items).await?;
                    add_batch(&tx, &mut batches, &COMMENTS_COLLECTION, &comments).await?;
                    add_batch(&tx, &mut batches, &ROLES_COLLECTION, &roles).await?;
                    add_batch(&tx, &mut batches, &CLUSTER_COLLECTION, &clusters).await?;
                    add_batch(
                        &tx,
                        &mut batches,
                        &CLUSTER_REPLICA_COLLECTION,
                        &cluster_replicas,
                    )
                    .await?;
                    add_batch(
                        &tx,
                        &mut batches,
                        &CLUSTER_INTROSPECTION_SOURCE_INDEX_COLLECTION,
                        &introspection_sources,
                    )
                    .await?;
                    add_batch(&tx, &mut batches, &ID_ALLOCATOR_COLLECTION, &id_allocator).await?;
                    add_batch(&tx, &mut batches, &CONFIG_COLLECTION, &configs).await?;
                    add_batch(&tx, &mut batches, &SETTING_COLLECTION, &settings).await?;
                    add_batch(&tx, &mut batches, &TIMESTAMP_COLLECTION, &timestamps).await?;
                    add_batch(
                        &tx,
                        &mut batches,
                        &SYSTEM_GID_MAPPING_COLLECTION,
                        &system_gid_mapping,
                    )
                    .await?;
                    add_batch(
                        &tx,
                        &mut batches,
                        &SYSTEM_CONFIGURATION_COLLECTION,
                        &system_configurations,
                    )
                    .await?;
                    add_batch(
                        &tx,
                        &mut batches,
                        &DEFAULT_PRIVILEGES_COLLECTION,
                        &default_privileges,
                    )
                    .await?;
                    add_batch(
                        &tx,
                        &mut batches,
                        &SYSTEM_PRIVILEGES_COLLECTION,
                        &system_privileges,
                    )
                    .await?;
                    add_batch(&tx, &mut batches, &AUDIT_LOG_COLLECTION, &audit_log_updates).await?;
                    add_batch(
                        &tx,
                        &mut batches,
                        &STORAGE_USAGE_COLLECTION,
                        &storage_usage_updates,
                    )
                    .await?;
                    tx.append(batches).await?;

                    Ok(())
                })
            })
            .await?;

        Ok(())
    }
}

// Structs used to pass information to outside modules.

#[derive(Debug, Clone)]
pub struct Database {
    pub id: DatabaseId,
    pub name: String,
    pub owner_id: RoleId,
    pub privileges: Vec<MzAclItem>,
}

#[derive(Debug, Clone)]
pub struct Schema {
    pub id: SchemaId,
    pub name: String,
    pub database_id: Option<DatabaseId>,
    pub owner_id: RoleId,
    pub privileges: Vec<MzAclItem>,
}

#[derive(Debug, Clone)]
pub struct Role {
    pub id: RoleId,
    pub name: String,
    pub attributes: RoleAttributes,
    pub membership: RoleMembership,
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

#[derive(Debug, Clone)]
pub struct ClusterReplica {
    pub cluster_id: ClusterId,
    pub replica_id: ReplicaId,
    pub name: String,
    pub config: ReplicaConfig,
    pub owner_id: RoleId,
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
            },
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
    pub name: QualifiedItemName,
    pub create_sql: String,
    pub owner_id: RoleId,
    pub privileges: Vec<MzAclItem>,
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
    pub schema_name: String,
    pub object_type: CatalogItemType,
    pub object_name: String,
    pub id: GlobalId,
    pub fingerprint: String,
}

// Structs used internally to represent on disk-state.

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct SettingKey {
    name: String,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct SettingValue {
    value: String,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct IdAllocKey {
    name: String,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct IdAllocValue {
    next_id: u64,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct GidMappingKey {
    schema_name: String,
    object_type: CatalogItemType,
    object_name: String,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct GidMappingValue {
    id: u64,
    fingerprint: String,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct ClusterKey {
    id: ClusterId,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct ClusterValue {
    name: String,
    linked_object_id: Option<GlobalId>,
    owner_id: RoleId,
    privileges: Vec<MzAclItem>,
    config: ClusterConfig,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct ClusterIntrospectionSourceIndexKey {
    cluster_id: ClusterId,
    name: String,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct ClusterIntrospectionSourceIndexValue {
    index_id: u64,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct ClusterReplicaKey {
    id: ReplicaId,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct ClusterReplicaValue {
    cluster_id: ClusterId,
    name: String,
    config: ReplicaConfig,
    owner_id: RoleId,
}

#[derive(Clone, Copy, Debug, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub struct DatabaseKey {
    id: DatabaseId,
}

#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct DatabaseValue {
    name: String,
    owner_id: RoleId,
    privileges: Vec<MzAclItem>,
}

#[derive(Clone, Copy, Debug, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub struct SchemaKey {
    id: SchemaId,
}

#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct SchemaValue {
    database_id: Option<DatabaseId>,
    name: String,
    owner_id: RoleId,
    privileges: Vec<MzAclItem>,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash, Debug, Arbitrary)]
pub struct ItemKey {
    gid: GlobalId,
}

#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct ItemValue {
    schema_id: SchemaId,
    name: String,
    create_sql: String,
    owner_id: RoleId,
    privileges: Vec<MzAclItem>,
}

#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Ord)]
pub struct CommentKey {
    object_id: CommentObjectId,
    sub_component: Option<usize>,
}

#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct CommentValue {
    comment: String,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash, Debug)]
pub struct RoleKey {
    id: RoleId,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Debug)]
pub struct RoleValue {
    name: String,
    attributes: RoleAttributes,
    membership: RoleMembership,
}

impl From<Role> for RoleValue {
    fn from(role: Role) -> Self {
        RoleValue {
            name: role.name,
            attributes: role.attributes,
            membership: role.membership,
        }
    }
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct ConfigKey {
    key: String,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct ConfigValue {
    value: u64,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct AuditLogKey {
    event: VersionedEvent,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct StorageUsageKey {
    metric: VersionedStorageUsage,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct TimestampKey {
    id: String,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct TimestampValue {
    ts: mz_repr::Timestamp,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct ServerConfigurationKey {
    name: String,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct ServerConfigurationValue {
    value: String,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct DefaultPrivilegesKey {
    role_id: RoleId,
    database_id: Option<DatabaseId>,
    schema_id: Option<SchemaId>,
    object_type: ObjectType,
    grantee: RoleId,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct DefaultPrivilegesValue {
    privileges: AclMode,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct SystemPrivilegesKey {
    grantee: RoleId,
    grantor: RoleId,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct SystemPrivilegesValue {
    acl_mode: AclMode,
}

pub const ALL_COLLECTIONS: &[&str] = &[
    AUDIT_LOG_COLLECTION.name(),
    CLUSTER_COLLECTION.name(),
    CLUSTER_INTROSPECTION_SOURCE_INDEX_COLLECTION.name(),
    CLUSTER_REPLICA_COLLECTION.name(),
    CONFIG_COLLECTION.name(),
    DATABASES_COLLECTION.name(),
    DEFAULT_PRIVILEGES_COLLECTION.name(),
    ID_ALLOCATOR_COLLECTION.name(),
    ITEM_COLLECTION.name(),
    ROLES_COLLECTION.name(),
    SCHEMAS_COLLECTION.name(),
    SETTING_COLLECTION.name(),
    STORAGE_USAGE_COLLECTION.name(),
    SYSTEM_CONFIGURATION_COLLECTION.name(),
    SYSTEM_GID_MAPPING_COLLECTION.name(),
    SYSTEM_PRIVILEGES_COLLECTION.name(),
    TIMESTAMP_COLLECTION.name(),
];

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
