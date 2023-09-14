// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::iter::once;
use std::pin;
use std::time::Duration;

use futures::StreamExt;
use itertools::Itertools;

use mz_audit_log::{VersionedEvent, VersionedStorageUsage};
use mz_controller_types::{ClusterId, ReplicaId};
use mz_ore::collections::CollectionExt;
use mz_ore::now::NowFn;
use mz_ore::retry::Retry;
use mz_proto::{ProtoType, RustType};
use mz_repr::adt::mz_acl_item::MzAclItem;
use mz_repr::role_id::RoleId;
use mz_repr::GlobalId;
use mz_sql::catalog::{
    CatalogError as SqlCatalogError, CatalogItemType, DefaultPrivilegeAclItem,
    DefaultPrivilegeObject,
};
use mz_sql::names::CommentObjectId;
use mz_stash::objects::proto;
use mz_stash::Stash;
use mz_storage_types::sources::Timeline;

use crate::initialize::DEPLOY_GENERATION;
use crate::objects::{
    AuditLogKey, Cluster, ClusterIntrospectionSourceIndexKey, ClusterIntrospectionSourceIndexValue,
    ClusterKey, ClusterReplica, ClusterReplicaKey, ClusterReplicaValue, ClusterValue, CommentKey,
    CommentValue, Database, DatabaseKey, DatabaseValue, DefaultPrivilegesKey,
    DefaultPrivilegesValue, GidMappingKey, GidMappingValue, IdAllocKey, IdAllocValue,
    ReplicaConfig, Role, RoleKey, RoleValue, Schema, SchemaKey, SchemaValue, StorageUsageKey,
    SystemObjectMapping, SystemPrivilegesKey, SystemPrivilegesValue, TimestampKey, TimestampValue,
};
use crate::transaction::{
    add_new_builtin_cluster_replicas_migration, add_new_builtin_clusters_migration, transaction,
    Transaction,
};
use crate::{
    initialize, BootstrapArgs, Error, AUDIT_LOG_COLLECTION, CLUSTER_COLLECTION,
    CLUSTER_INTROSPECTION_SOURCE_INDEX_COLLECTION, CLUSTER_REPLICA_COLLECTION, COMMENTS_COLLECTION,
    CONFIG_COLLECTION, DATABASES_COLLECTION, DEFAULT_PRIVILEGES_COLLECTION,
    ID_ALLOCATOR_COLLECTION, ITEM_COLLECTION, ROLES_COLLECTION, SCHEMAS_COLLECTION,
    SETTING_COLLECTION, STORAGE_USAGE_COLLECTION, SYSTEM_CLUSTER_ID_ALLOC_KEY,
    SYSTEM_CONFIGURATION_COLLECTION, SYSTEM_GID_MAPPING_COLLECTION, SYSTEM_PRIVILEGES_COLLECTION,
    SYSTEM_REPLICA_ID_ALLOC_KEY, TIMESTAMP_COLLECTION, USER_CLUSTER_ID_ALLOC_KEY,
    USER_REPLICA_ID_ALLOC_KEY,
};

/// A [`Connection`] represent an open connection to the stash. It exposes optimized methods for
/// executing a single operation against the stash. If the consumer needs to execute multiple
/// operations atomically, then they should start a transaction via [`Connection::transaction`].
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
                    let should_retry = matches!(&err, Error::Stash(se) if se.should_retry());
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
                        initialize::initialize(&mut tx, &args, boot_ts, deploy_generation).await
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

            match add_new_builtin_clusters_migration(&mut txn, bootstrap_args) {
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

    pub async fn set_connect_timeout(&mut self, connect_timeout: Duration) {
        self.stash.set_connect_timeout(connect_timeout).await;
    }

    pub fn is_read_only(&self) -> bool {
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
                    None => Err(Error::from(SqlCatalogError::IdExhaustion)),
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
