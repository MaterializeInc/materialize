// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic related to opening a [`Catalog`].

mod builtin_item_migration;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::{BoxFuture, FutureExt};
use mz_adapter_types::compaction::CompactionWindow;
use mz_catalog::builtin::{
    Builtin, BuiltinTable, Fingerprint, BUILTINS, BUILTIN_CLUSTERS, BUILTIN_CLUSTER_REPLICAS,
    BUILTIN_PREFIXES, BUILTIN_ROLES, MZ_STORAGE_USAGE_BY_SHARD_DESCRIPTION,
    RUNTIME_ALTERABLE_FINGERPRINT_SENTINEL,
};
use mz_catalog::config::StateConfig;
use mz_catalog::durable::objects::{
    SystemObjectDescription, SystemObjectMapping, SystemObjectUniqueIdentifier,
};
use mz_catalog::durable::{
    ClusterVariant, ClusterVariantManaged, Transaction, SYSTEM_CLUSTER_ID_ALLOC_KEY,
    SYSTEM_REPLICA_ID_ALLOC_KEY,
};
use mz_catalog::memory::error::{Error, ErrorKind};
use mz_catalog::memory::objects::{
    BootstrapStateUpdateKind, CatalogEntry, CatalogItem, CommentsMap, DefaultPrivileges,
    StateUpdate,
};
use mz_catalog::SYSTEM_CONN_ID;
use mz_cluster_client::ReplicaId;
use mz_compute_client::logging::LogVariant;
use mz_controller::clusters::ReplicaLogging;
use mz_controller_types::{is_cluster_size_v2, ClusterId};
use mz_ore::cast::usize_to_u64;
use mz_ore::collections::{CollectionExt, HashSet};
use mz_ore::now::to_datetime;
use mz_ore::{instrument, soft_assert_no_log};
use mz_repr::adt::mz_acl_item::PrivilegeMap;
use mz_repr::namespaces::is_unstable_schema;
use mz_repr::role_id::RoleId;
use mz_repr::{Diff, GlobalId, Timestamp};
use mz_sql::catalog::{
    CatalogError as SqlCatalogError, CatalogItem as SqlCatalogItem, CatalogItemType,
    RoleMembership, RoleVars,
};
use mz_sql::func::OP_IMPLS;
use mz_sql::names::SchemaId;
use mz_sql::rbac;
use mz_sql::session::user::{MZ_SYSTEM_ROLE_ID, SYSTEM_USER};
use mz_sql::session::vars::{SessionVars, SystemVars, VarError, VarInput};
use mz_sql_parser::ast::display::AstDisplay;
use mz_storage_client::controller::StorageController;
use timely::Container;
use tracing::{error, info, warn, Instrument};
use uuid::Uuid;

// DO NOT add any more imports from `crate` outside of `crate::catalog`.
use crate::catalog::open::builtin_item_migration::{
    migrate_builtin_items, BuiltinItemMigrationResult,
};
use crate::catalog::{
    is_reserved_name, migrate, BuiltinTableUpdate, Catalog, CatalogPlans, CatalogState, Config,
};
use crate::AdapterError;

#[derive(Debug)]
pub struct BuiltinMigrationMetadata {
    // Used to drop objects on STORAGE nodes
    pub previous_storage_collection_ids: BTreeSet<GlobalId>,
    // Used to update persisted on disk catalog state
    pub migrated_system_object_mappings: BTreeMap<GlobalId, SystemObjectMapping>,
    pub introspection_source_index_updates:
        BTreeMap<ClusterId, Vec<(LogVariant, String, GlobalId, u32)>>,
    pub user_item_drop_ops: Vec<GlobalId>,
    pub user_item_create_ops: Vec<CreateOp>,
}

#[derive(Debug)]
pub struct CreateOp {
    id: GlobalId,
    oid: u32,
    schema_id: SchemaId,
    name: String,
    owner_id: RoleId,
    privileges: PrivilegeMap,
    item_rebuilder: CatalogItemRebuilder,
}

impl BuiltinMigrationMetadata {
    fn new() -> BuiltinMigrationMetadata {
        BuiltinMigrationMetadata {
            previous_storage_collection_ids: BTreeSet::new(),
            migrated_system_object_mappings: BTreeMap::new(),
            introspection_source_index_updates: BTreeMap::new(),
            user_item_drop_ops: Vec::new(),
            user_item_create_ops: Vec::new(),
        }
    }
}

#[derive(Debug)]
pub enum CatalogItemRebuilder {
    SystemSource(CatalogItem),
    Object {
        sql: String,
        is_retained_metrics_object: bool,
        custom_logical_compaction_window: Option<CompactionWindow>,
    },
}

impl CatalogItemRebuilder {
    fn new(
        entry: &CatalogEntry,
        id: GlobalId,
        ancestor_ids: &BTreeMap<GlobalId, GlobalId>,
    ) -> Self {
        if id.is_system()
            && (entry.is_table() || entry.is_introspection_source() || entry.is_source())
        {
            Self::SystemSource(entry.item().clone())
        } else {
            let create_sql = entry.create_sql().to_string();
            let mut create_stmt = mz_sql::parse::parse(&create_sql)
                .expect("invalid create sql persisted to catalog")
                .into_element()
                .ast;
            mz_sql::ast::transform::create_stmt_replace_ids(&mut create_stmt, ancestor_ids);
            Self::Object {
                sql: create_stmt.to_ast_string_stable(),
                is_retained_metrics_object: entry.item().is_retained_metrics_object(),
                custom_logical_compaction_window: entry.item().custom_logical_compaction_window(),
            }
        }
    }

    fn build(self, state: &CatalogState) -> CatalogItem {
        match self {
            Self::SystemSource(item) => item,
            Self::Object {
                sql,
                is_retained_metrics_object,
                custom_logical_compaction_window,
            } => state
                .parse_item(
                    &sql,
                    is_retained_metrics_object,
                    custom_logical_compaction_window,
                )
                .unwrap_or_else(|error| panic!("invalid persisted create sql ({error:?}): {sql}")),
        }
    }
}

pub struct InitializeStateResult {
    /// An initialized [`CatalogState`].
    pub state: CatalogState,
    /// A set of storage collections to drop (only used by legacy migrations).
    pub storage_collections_to_drop: BTreeSet<GlobalId>,
    /// A set of new shards that may need to be initialized (only used by 0dt migration).
    pub migrated_storage_collections_0dt: BTreeSet<GlobalId>,
    /// A set of new builtin items.
    pub new_builtins: BTreeSet<GlobalId>,
    /// A list of builtin table updates corresponding to the initialized state.
    pub builtin_table_updates: Vec<BuiltinTableUpdate>,
    /// The version of the catalog that existed before initializing the catalog.
    pub last_seen_version: String,
}

pub struct OpenCatalogResult {
    /// An opened [`Catalog`].
    pub catalog: Catalog,
    /// A set of storage collections to drop (only used by legacy migrations).
    pub storage_collections_to_drop: BTreeSet<GlobalId>,
    /// A set of new shards that may need to be initialized (only used by 0dt migration).
    pub migrated_storage_collections_0dt: BTreeSet<GlobalId>,
    /// A set of new builtin items.
    pub new_builtins: BTreeSet<GlobalId>,
    /// A list of builtin table updates corresponding to the initialized state.
    pub builtin_table_updates: Vec<BuiltinTableUpdate>,
}

impl Catalog {
    /// Initializes a CatalogState. Separate from [`Catalog::open`] to avoid depending on state
    /// external to a [mz_catalog::durable::DurableCatalogState]
    /// (for example: no [mz_secrets::SecretsReader]).
    pub async fn initialize_state<'a>(
        config: StateConfig,
        storage: &'a mut Box<dyn mz_catalog::durable::DurableCatalogState>,
    ) -> Result<InitializeStateResult, AdapterError> {
        for builtin_role in BUILTIN_ROLES {
            assert!(
                is_reserved_name(builtin_role.name),
                "builtin role {builtin_role:?} must start with one of the following prefixes {}",
                BUILTIN_PREFIXES.join(", ")
            );
        }
        for builtin_cluster in BUILTIN_CLUSTERS {
            assert!(
                    is_reserved_name(builtin_cluster.name),
                    "builtin cluster {builtin_cluster:?} must start with one of the following prefixes {}",
                    BUILTIN_PREFIXES.join(", ")
                );
        }

        let mut state = CatalogState {
            database_by_name: BTreeMap::new(),
            database_by_id: BTreeMap::new(),
            entry_by_id: BTreeMap::new(),
            ambient_schemas_by_name: BTreeMap::new(),
            ambient_schemas_by_id: BTreeMap::new(),
            clusters_by_name: BTreeMap::new(),
            clusters_by_id: BTreeMap::new(),
            roles_by_name: BTreeMap::new(),
            roles_by_id: BTreeMap::new(),
            system_configuration: {
                let mut s =
                    SystemVars::new(config.active_connection_count).set_unsafe(config.unsafe_mode);
                if config.all_features {
                    s.enable_all_feature_flags_by_default();
                }
                s
            },
            default_privileges: DefaultPrivileges::default(),
            system_privileges: PrivilegeMap::default(),
            comments: CommentsMap::default(),
            storage_metadata: Default::default(),
            temporary_schemas: BTreeMap::new(),
            config: mz_sql::catalog::CatalogConfig {
                start_time: to_datetime((config.now)()),
                start_instant: Instant::now(),
                nonce: rand::random(),
                environment_id: config.environment_id,
                session_id: Uuid::new_v4(),
                build_info: config.build_info,
                timestamp_interval: Duration::from_secs(1),
                now: config.now.clone(),
                connection_context: config.connection_context,
            },
            cluster_replica_sizes: config.cluster_replica_sizes,
            availability_zones: config.availability_zones,
            egress_ips: config.egress_ips,
            aws_principal_context: config.aws_principal_context,
            aws_privatelink_availability_zones: config.aws_privatelink_availability_zones,
            http_host_name: config.http_host_name,
        };

        let mut updates: Vec<_> = storage.sync_to_current_updates().await?;
        assert!(!updates.is_empty(), "initial catalog snapshot is missing");
        let mut txn = storage.transaction().await?;

        // Migrate/update durable data before we start loading the in-memory catalog.
        let (migrated_builtins, new_builtins) = {
            migrate::durable_migrate(&mut txn, config.boot_ts)?;
            // Overwrite and persist selected parameter values in `remote_system_parameters` that
            // was pulled from a remote frontend (e.g. LaunchDarkly) if present.
            if let Some(remote_system_parameters) = config.remote_system_parameters {
                for (name, value) in remote_system_parameters {
                    txn.upsert_system_config(&name, value)?;
                }
                txn.set_system_config_synced_once()?;
            }
            // Add any new builtin objects and remove old ones.
            let (migrated_builtins, new_builtins) =
                add_new_remove_old_builtin_items_migration(&mut txn)?;
            let cluster_sizes = BuiltinBootstrapClusterSizes {
                system_cluster: config.builtin_system_cluster_replica_size,
                catalog_server_cluster: config.builtin_catalog_server_cluster_replica_size,
                probe_cluster: config.builtin_probe_cluster_replica_size,
                support_cluster: config.builtin_support_cluster_replica_size,
                analytics_cluster: config.builtin_analytics_cluster_replica_size,
            };
            // TODO(jkosh44) These functions should clean up old clusters, replicas, and
            // roles like they do for builtin items and introspection sources, but they
            // don't.
            add_new_builtin_clusters_migration(&mut txn, &cluster_sizes)?;
            add_new_remove_old_builtin_introspection_source_migration(&mut txn)?;
            add_new_builtin_cluster_replicas_migration(&mut txn, &cluster_sizes)?;
            add_new_builtin_roles_migration(&mut txn)?;
            remove_invalid_config_param_role_defaults_migration(&mut txn)?;
            (migrated_builtins, new_builtins)
        };
        remove_pending_cluster_replicas_migration(&mut txn)?;

        let op_updates = txn.get_and_commit_op_updates();
        updates.extend(op_updates);

        // Seed the in-memory catalog with values that don't come from the durable catalog.
        {
            // Set defaults from configuration passed in the provided `system_parameter_defaults`
            // map.
            for (name, value) in config.system_parameter_defaults {
                match state.set_system_configuration_default(&name, VarInput::Flat(&value)) {
                    Ok(_) => (),
                    Err(Error {
                        kind: ErrorKind::VarError(VarError::UnknownParameter(name)),
                    }) => {
                        warn!(%name, "cannot load unknown system parameter from catalog storage to set default parameter");
                    }
                    Err(e) => return Err(e.into()),
                };
            }
            state.create_temporary_schema(&SYSTEM_CONN_ID, MZ_SYSTEM_ROLE_ID)?;
        }

        let mut builtin_table_updates = Vec::new();

        // Make life easier by consolidating all updates, so that we end up with only positive
        // diffs.
        let commit_ts = txn.commit_ts();
        let mut updates = into_consolidatable_updates_startup(updates, commit_ts);
        differential_dataflow::consolidation::consolidate_updates(&mut updates);
        soft_assert_no_log!(
            updates.iter().all(|(_, _, diff)| *diff == 1),
            "consolidated updates should be positive during startup: {updates:?}"
        );

        let mut pre_item_updates = Vec::new();
        let mut item_updates = Vec::new();
        let mut post_item_updates = Vec::new();
        for (kind, ts, diff) in updates {
            match kind {
                BootstrapStateUpdateKind::Role(_)
                | BootstrapStateUpdateKind::Database(_)
                | BootstrapStateUpdateKind::Schema(_)
                | BootstrapStateUpdateKind::DefaultPrivilege(_)
                | BootstrapStateUpdateKind::SystemPrivilege(_)
                | BootstrapStateUpdateKind::SystemConfiguration(_)
                | BootstrapStateUpdateKind::Cluster(_)
                | BootstrapStateUpdateKind::IntrospectionSourceIndex(_)
                | BootstrapStateUpdateKind::ClusterReplica(_)
                | BootstrapStateUpdateKind::SystemObjectMapping(_) => {
                    pre_item_updates.push(StateUpdate {
                        kind: kind.into(),
                        ts,
                        diff: diff.try_into().expect("valid diff"),
                    })
                }
                BootstrapStateUpdateKind::Item(_) => item_updates.push(StateUpdate {
                    kind: kind.into(),
                    ts,
                    diff: diff.try_into().expect("valid diff"),
                }),
                BootstrapStateUpdateKind::Comment(_)
                | BootstrapStateUpdateKind::AuditLog(_)
                | BootstrapStateUpdateKind::StorageCollectionMetadata(_)
                | BootstrapStateUpdateKind::SourceReferences(_)
                | BootstrapStateUpdateKind::UnfinalizedShard(_) => {
                    post_item_updates.push(StateUpdate {
                        kind: kind.into(),
                        ts,
                        diff: diff.try_into().expect("valid diff"),
                    })
                }
            }
        }

        let builtin_table_update = state.apply_updates_for_bootstrap(pre_item_updates).await;
        builtin_table_updates.extend(builtin_table_update);

        let last_seen_version = txn
            .get_catalog_content_version()
            .unwrap_or_else(|| "new".to_string());

        // Migrate item ASTs.
        let builtin_table_update = if !config.skip_migrations {
            migrate::migrate(
                &mut state,
                &mut txn,
                item_updates,
                config.now,
                config.boot_ts,
            )
            .await
            .map_err(|e| {
                Error::new(ErrorKind::FailedMigration {
                    last_seen_version: last_seen_version.clone(),
                    this_version: config.build_info.version,
                    cause: e.to_string(),
                })
            })?
        } else {
            state.apply_updates_for_bootstrap(item_updates).await
        };
        builtin_table_updates.extend(builtin_table_update);

        let builtin_table_update = state.apply_updates_for_bootstrap(post_item_updates).await;
        builtin_table_updates.extend(builtin_table_update);

        // Migrate builtin items.
        let BuiltinItemMigrationResult {
            builtin_table_updates: builtin_table_update,
            storage_collections_to_drop,
            migrated_storage_collections_0dt,
            cleanup_action,
        } = migrate_builtin_items(
            &mut state,
            &mut txn,
            migrated_builtins,
            config.builtin_item_migration_config,
        )
        .await?;
        builtin_table_updates.extend(builtin_table_update);
        let builtin_table_updates = state.resolve_builtin_table_updates(builtin_table_updates);

        txn.commit().await?;

        cleanup_action.await;

        Ok(InitializeStateResult {
            state,
            storage_collections_to_drop,
            migrated_storage_collections_0dt,
            new_builtins: new_builtins.into_iter().collect(),
            builtin_table_updates,
            last_seen_version,
        })
    }

    /// Opens or creates a catalog that stores data at `path`.
    ///
    /// Returns the catalog, metadata about builtin objects that have changed
    /// schemas since last restart, a list of updates to builtin tables that
    /// describe the initial state of the catalog, and the version of the
    /// catalog before any migrations were performed.
    ///
    /// BOXED FUTURE: As of Nov 2023 the returned Future from this function was 17KB. This would
    /// get stored on the stack which is bad for runtime performance, and blow up our stack usage.
    /// Because of that we purposefully move this Future onto the heap (i.e. Box it).
    #[instrument(name = "catalog::open")]
    pub fn open(config: Config<'_>) -> BoxFuture<'static, Result<OpenCatalogResult, AdapterError>> {
        async move {
            let mut storage = config.storage;
            let InitializeStateResult {
                state,
                storage_collections_to_drop,
                migrated_storage_collections_0dt,
                new_builtins,
                mut builtin_table_updates,
                last_seen_version: _,
            } =
                // BOXED FUTURE: As of Nov 2023 the returned Future from this function was 7.5KB. This would
                // get stored on the stack which is bad for runtime performance, and blow up our stack usage.
                // Because of that we purposefully move this Future onto the heap (i.e. Box it).
                Self::initialize_state(config.state, &mut storage)
                    .instrument(tracing::info_span!("catalog::initialize_state"))
                    .boxed()
                    .await?;

            let catalog = Catalog {
                state,
                plans: CatalogPlans::default(),
                transient_revision: 1,
                storage: Arc::new(tokio::sync::Mutex::new(storage)),
            };

            // Operators aren't stored in the catalog, but we would like them in
            // introspection views.
            for (op, func) in OP_IMPLS.iter() {
                match func {
                    mz_sql::func::Func::Scalar(impls) => {
                        for imp in impls {
                            builtin_table_updates.push(catalog.state.resolve_builtin_table_update(
                                catalog.state.pack_op_update(op, imp.details(), 1),
                            ));
                        }
                    }
                    _ => unreachable!("all operators must be scalar functions"),
                }
            }

            for ip in &catalog.state.egress_ips {
                builtin_table_updates.push(
                    catalog
                        .state
                        .resolve_builtin_table_update(catalog.state.pack_egress_ip_update(ip)?),
                );
            }

            Ok(OpenCatalogResult {
                catalog,
                storage_collections_to_drop,
                migrated_storage_collections_0dt,
                new_builtins,
                builtin_table_updates,
            })
        }
        .instrument(tracing::info_span!("catalog::open"))
        .boxed()
    }

    /// Initializes the `storage_controller` to understand all shards that
    /// `self` expects to exist.
    ///
    /// Note that this must be done before creating/rendering collections
    /// because the storage controller might not be aware of new system
    /// collections created between versions.
    async fn initialize_storage_controller_state(
        &mut self,
        storage_controller: &mut dyn StorageController<Timestamp = mz_repr::Timestamp>,
        storage_collections_to_drop: BTreeSet<GlobalId>,
    ) -> Result<(), mz_catalog::durable::CatalogError> {
        let collections = self
            .entries()
            .filter(|entry| entry.item().is_storage_collection())
            .map(|entry| entry.id())
            .collect();

        // Clone the state so that any errors that occur do not leak any
        // transformations on error.
        let mut state = self.state.clone();

        let mut storage = self.storage().await;
        let mut txn = storage.transaction().await?;

        storage_controller
            .initialize_state(&mut txn, collections, storage_collections_to_drop)
            .await
            .map_err(mz_catalog::durable::DurableCatalogError::from)?;

        let updates = txn.get_and_commit_op_updates();
        let builtin_updates = state.apply_updates(updates)?;
        assert_eq!(builtin_updates, Vec::new());
        txn.commit().await?;
        drop(storage);

        // Save updated state.
        self.state = state;
        Ok(())
    }

    /// [`mz_controller::Controller`] depends on durable catalog state to boot,
    /// so make it available and initialize the controller.
    pub async fn initialize_controller(
        &mut self,
        config: mz_controller::ControllerConfig,
        envd_epoch: core::num::NonZeroI64,
        read_only: bool,
        storage_collections_to_drop: BTreeSet<GlobalId>,
    ) -> Result<mz_controller::Controller<mz_repr::Timestamp>, mz_catalog::durable::CatalogError>
    {
        let controller_start = Instant::now();
        info!("startup: controller init: beginning");

        let mut controller = {
            let mut storage = self.storage().await;
            let mut tx = storage.transaction().await?;
            mz_controller::prepare_initialization(&mut tx)
                .map_err(mz_catalog::durable::DurableCatalogError::from)?;
            let updates = tx.get_and_commit_op_updates();
            assert!(
                updates.is_empty(),
                "initializing controller should not produce updates: {updates:?}"
            );
            tx.commit().await?;

            let read_only_tx = storage.transaction().await?;

            mz_controller::Controller::new(config, envd_epoch, read_only, &read_only_tx).await
        };

        self.initialize_storage_controller_state(
            &mut *controller.storage,
            storage_collections_to_drop,
        )
        .await?;

        info!(
            "startup: controller init: complete in {:?}",
            controller_start.elapsed()
        );

        Ok(controller)
    }

    /// The objects in the catalog form one or more DAGs (directed acyclic graph) via object
    /// dependencies. To migrate a builtin object we must drop that object along with all of its
    /// descendants, and then recreate that object along with all of its descendants using new
    /// GlobalId`s. To achieve this we perform a DFS (depth first search) on the catalog items
    /// starting with the nodes that correspond to builtin objects that have changed schemas.
    ///
    /// Objects need to be dropped starting from the leafs of the DAG going up towards the roots,
    /// and they need to be recreated starting at the roots of the DAG and going towards the leafs.
    fn generate_builtin_migration_metadata(
        state: &CatalogState,
        txn: &mut Transaction<'_>,
        migrated_ids: Vec<GlobalId>,
        id_fingerprint_map: BTreeMap<GlobalId, String>,
    ) -> Result<BuiltinMigrationMetadata, Error> {
        // First obtain a topological sorting of all migrated objects and their children.
        let mut visited_set = BTreeSet::new();
        let mut sorted_entries = Vec::new();
        for id in migrated_ids {
            if !visited_set.contains(&id) {
                let migrated_topological_sort =
                    Catalog::topological_sort(state, id, &mut visited_set);
                sorted_entries.extend(migrated_topological_sort);
            }
        }
        sorted_entries.reverse();

        // Then process all objects in sorted order.
        let mut migration_metadata = BuiltinMigrationMetadata::new();
        let mut ancestor_ids = BTreeMap::new();
        let mut migrated_log_ids = BTreeMap::new();
        let log_name_map: BTreeMap<_, _> = BUILTINS::logs()
            .map(|log| (log.variant.clone(), log.name))
            .collect();
        for entry in sorted_entries {
            let id = entry.id();

            let new_id = match id {
                GlobalId::System(_) => txn.allocate_system_item_ids(1)?.into_element(),
                GlobalId::User(_) => txn.allocate_user_item_ids(1)?.into_element(),
                _ => unreachable!("can't migrate id: {id}"),
            };

            let name = state.resolve_full_name(entry.name(), None);
            info!("migrating {name} from {id} to {new_id}");

            // Generate value to update fingerprint and global ID persisted mapping for system objects.
            // Not every system object has a fingerprint, like introspection source indexes.
            if let Some(fingerprint) = id_fingerprint_map.get(&id) {
                assert!(
                    id.is_system(),
                    "id_fingerprint_map should only contain builtin objects"
                );
                let schema_name = state
                    .get_schema(
                        &entry.name().qualifiers.database_spec,
                        &entry.name().qualifiers.schema_spec,
                        entry.conn_id().unwrap_or(&SYSTEM_CONN_ID),
                    )
                    .name
                    .schema
                    .as_str();
                migration_metadata.migrated_system_object_mappings.insert(
                    id,
                    SystemObjectMapping {
                        description: SystemObjectDescription {
                            schema_name: schema_name.to_string(),
                            object_type: entry.item_type(),
                            object_name: entry.name().item.clone(),
                        },
                        unique_identifier: SystemObjectUniqueIdentifier {
                            id: new_id,
                            fingerprint: fingerprint.clone(),
                        },
                    },
                );
            }

            ancestor_ids.insert(id, new_id);

            if entry.item().is_storage_collection() {
                migration_metadata
                    .previous_storage_collection_ids
                    .insert(id);
            }

            // Push drop commands.
            match entry.item() {
                CatalogItem::Log(log) => {
                    migrated_log_ids.insert(id, log.variant.clone());
                }
                CatalogItem::Index(index) => {
                    if id.is_system() {
                        if let Some(variant) = migrated_log_ids.get(&index.on) {
                            migration_metadata
                                .introspection_source_index_updates
                                .entry(index.cluster_id)
                                .or_default()
                                .push((
                                    variant.clone(),
                                    log_name_map
                                        .get(variant)
                                        .expect("all variants have a name")
                                        .to_string(),
                                    new_id,
                                    entry.oid(),
                                ));
                        }
                    }
                }
                CatalogItem::Table(_)
                | CatalogItem::Source(_)
                | CatalogItem::MaterializedView(_) => {
                    // Storage objects don't have any external objects to drop.
                }
                CatalogItem::Sink(_) => {
                    // Sinks don't have any external objects to drop--however,
                    // this would change if we add a collections for sinks
                    // #17672.
                }
                CatalogItem::View(_) => {
                    // Views don't have any external objects to drop.
                }
                CatalogItem::Type(_)
                | CatalogItem::Func(_)
                | CatalogItem::Secret(_)
                | CatalogItem::Connection(_) => unreachable!(
                    "impossible to migrate schema for builtin {}",
                    entry.item().typ()
                ),
            }
            if id.is_user() {
                migration_metadata.user_item_drop_ops.push(id);
            }

            // Push create commands.
            let name = entry.name().clone();
            if id.is_user() {
                let schema_id = name.qualifiers.schema_spec.clone().into();
                let item_rebuilder = CatalogItemRebuilder::new(entry, new_id, &ancestor_ids);
                migration_metadata.user_item_create_ops.push(CreateOp {
                    id: new_id,
                    oid: entry.oid(),
                    schema_id,
                    name: name.item.clone(),
                    owner_id: entry.owner_id().clone(),
                    privileges: entry.privileges().clone(),
                    item_rebuilder,
                });
            }
        }

        // Reverse drop commands.
        migration_metadata.user_item_drop_ops.reverse();

        Ok(migration_metadata)
    }

    fn topological_sort<'a, 'b>(
        state: &'a CatalogState,
        id: GlobalId,
        visited_set: &'b mut BTreeSet<GlobalId>,
    ) -> Vec<&'a CatalogEntry> {
        let mut sorted_entries = Vec::new();
        visited_set.insert(id);
        let entry = state.get_entry(&id);
        for dependant in entry.used_by() {
            if !visited_set.contains(dependant) {
                let child_topological_sort =
                    Catalog::topological_sort(state, *dependant, visited_set);
                sorted_entries.extend(child_topological_sort);
            }
        }
        sorted_entries.push(entry);
        sorted_entries
    }

    #[mz_ore::instrument]
    async fn apply_builtin_migration(
        state: &mut CatalogState,
        txn: &mut Transaction<'_>,
        migration_metadata: &mut BuiltinMigrationMetadata,
    ) -> Result<Vec<BuiltinTableUpdate<&'static BuiltinTable>>, Error> {
        for id in &migration_metadata.user_item_drop_ops {
            let entry = state.get_entry(id);
            if entry.is_sink() {
                let full_name = state.resolve_full_name(entry.name(), None);
                error!(
                    "user sink {full_name} will be recreated as part of a builtin migration which \
                    can result in duplicate data being emitted. This is a known issue, \
                    https://github.com/MaterializeInc/materialize/issues/18767. Please inform the \
                    customer that their sink may produce duplicate data."
                )
            }
        }

        let mut builtin_table_updates = Vec::new();
        txn.remove_items(&migration_metadata.user_item_drop_ops.drain(..).collect())?;
        txn.update_system_object_mappings(std::mem::take(
            &mut migration_metadata.migrated_system_object_mappings,
        ))?;
        txn.update_introspection_source_index_gids(
            std::mem::take(&mut migration_metadata.introspection_source_index_updates)
                .into_iter()
                .map(|(cluster_id, updates)| {
                    (
                        cluster_id,
                        updates
                            .into_iter()
                            .map(|(_variant, name, index_id, oid)| (name, index_id, oid)),
                    )
                }),
        )?;
        let updates = txn.get_and_commit_op_updates();
        let builtin_table_update = state.apply_updates_for_bootstrap(updates).await;
        builtin_table_updates.extend(builtin_table_update);
        for CreateOp {
            id,
            oid,
            schema_id,
            name,
            owner_id,
            privileges,
            item_rebuilder,
        } in migration_metadata.user_item_create_ops.drain(..)
        {
            let item = item_rebuilder.build(state);
            let serialized_item = item.to_serialized();
            txn.insert_item(
                id,
                oid,
                schema_id,
                &name,
                serialized_item,
                owner_id.clone(),
                privileges.all_values_owned().collect(),
            )?;
            let updates = txn.get_and_commit_op_updates();
            let builtin_table_update = state.apply_updates_for_bootstrap(updates).await;
            builtin_table_updates.extend(builtin_table_update);
        }
        Ok(builtin_table_updates)
    }

    /// Politely releases all external resources that can only be released in an async context.
    pub async fn expire(self) {
        // If no one else holds a reference to storage, then clean up the storage resources.
        // Otherwise, hopefully the other reference cleans up the resources when it's dropped.
        if let Some(storage) = Arc::into_inner(self.storage) {
            let storage = storage.into_inner();
            storage.expire().await;
        }
    }
}

impl CatalogState {
    /// Set the default value for `name`, which is the value it will be reset to.
    fn set_system_configuration_default(
        &mut self,
        name: &str,
        value: VarInput,
    ) -> Result<(), Error> {
        Ok(self.system_configuration.set_default(name, value)?)
    }
}

/// Updates the catalog with new and removed builtin items.
///
/// Returns the list of builtin [`GlobalId`]s that need to be migrated, and the list of new builtin
/// [`GlobalId`]s.
fn add_new_remove_old_builtin_items_migration(
    txn: &mut mz_catalog::durable::Transaction<'_>,
) -> Result<(Vec<GlobalId>, Vec<GlobalId>), mz_catalog::durable::CatalogError> {
    let mut new_builtins = Vec::new();
    let mut new_builtin_ids = Vec::new();
    let mut migrated_builtin_ids = Vec::new();

    // We compare the builtin items that are compiled into the binary with the builtin items that
    // are persisted in the catalog to discover new, deleted, and migrated builtin items.
    let builtins: Vec<_> = BUILTINS::iter()
        .map(|builtin| {
            (
                SystemObjectDescription {
                    schema_name: builtin.schema().to_string(),
                    object_type: builtin.catalog_item_type(),
                    object_name: builtin.name().to_string(),
                },
                builtin,
            )
        })
        .collect();
    let mut system_object_mappings: BTreeMap<_, _> = txn
        .get_system_object_mappings()
        .map(|system_object_mapping| {
            (
                system_object_mapping.description.clone(),
                system_object_mapping,
            )
        })
        .collect();
    let new_builtin_amount = builtins
        .iter()
        .filter(|(desc, _)| !system_object_mappings.contains_key(desc))
        .count();
    let mut new_ids = txn
        .allocate_system_item_ids(usize_to_u64(new_builtin_amount))?
        .into_iter();

    // Look for new and migrated builtins.
    for (desc, builtin) in builtins {
        let fingerprint = match builtin.runtime_alterable() {
            false => builtin.fingerprint(),
            true => RUNTIME_ALTERABLE_FINGERPRINT_SENTINEL.into(),
        };
        match system_object_mappings.remove(&desc) {
            Some(system_object_mapping) => {
                if system_object_mapping.unique_identifier.fingerprint != fingerprint {
                    // `mz_storage_usage_by_shard` cannot be migrated for multiple reasons. Firstly,
                    // it was cause the table to be truncated because the contents are not also
                    // stored in the durable catalog. Secondly, we prune `mz_storage_usage_by_shard`
                    // of old events in the background on startup. The correctness of that pruning
                    // relies on there being no other retractions to `mz_storage_usage_by_shard`.
                    assert_ne!(
                        *MZ_STORAGE_USAGE_BY_SHARD_DESCRIPTION,
                        system_object_mapping.description,
                        "mz_storage_usage_by_shard cannot be migrated or else the table will be truncated"
                    );
                    assert_ne!(
                        builtin.catalog_item_type(),
                        CatalogItemType::Type,
                        "types cannot be migrated"
                    );
                    assert_ne!(
                        system_object_mapping.unique_identifier.fingerprint,
                        RUNTIME_ALTERABLE_FINGERPRINT_SENTINEL,
                        "clearing the runtime alterable flag on an existing object is not permitted",
                    );
                    assert!(
                        !builtin.runtime_alterable(),
                        "setting the runtime alterable flag on an existing object is not permitted"
                    );
                    migrated_builtin_ids.push(system_object_mapping.unique_identifier.id);
                }
            }
            None => {
                let id = new_ids.next().expect("not enough global IDs");
                new_builtins.push(SystemObjectMapping {
                    description: SystemObjectDescription {
                        schema_name: builtin.schema().to_string(),
                        object_type: builtin.catalog_item_type(),
                        object_name: builtin.name().to_string(),
                    },
                    unique_identifier: SystemObjectUniqueIdentifier { id, fingerprint },
                });
                new_builtin_ids.push(id);

                // Runtime-alterable system objects are durably recorded to the
                // usual items collection, so that they can be later altered at
                // runtime by their owner (i.e., outside of the usual builtin
                // migration framework that requires changes to the binary
                // itself).
                let handled_runtime_alterable = match builtin {
                    Builtin::Connection(c) if c.runtime_alterable => {
                        let mut acl_items = vec![rbac::owner_privilege(
                            mz_sql::catalog::ObjectType::Connection,
                            c.owner_id.clone(),
                        )];
                        acl_items.extend_from_slice(c.access);
                        txn.insert_item(
                            id,
                            c.oid,
                            mz_catalog::durable::initialize::resolve_system_schema(c.schema).id,
                            c.name,
                            c.sql.into(),
                            *c.owner_id,
                            acl_items,
                        )?;
                        true
                    }
                    _ => false,
                };
                assert_eq!(
                    builtin.runtime_alterable(),
                    handled_runtime_alterable,
                    "runtime alterable object was not handled by migration",
                );
            }
        }
    }

    // Add new builtin items to catalog.
    txn.set_system_object_mappings(new_builtins)?;

    // Anything left in `system_object_mappings` must have been deleted and should be removed from
    // the catalog.
    let mut deleted_system_objects = BTreeSet::new();
    let mut deleted_runtime_alterable_system_ids = BTreeSet::new();
    for (_, mapping) in system_object_mappings {
        deleted_system_objects.insert(mapping.description);
        if mapping.unique_identifier.fingerprint == RUNTIME_ALTERABLE_FINGERPRINT_SENTINEL {
            deleted_runtime_alterable_system_ids.insert(mapping.unique_identifier.id);
        }
    }
    // If you are 100% positive that it is safe to delete a system object outside any of the
    // unstable schemas, then add it to this set. Make sure that no prod environments are
    // using this object and that the upgrade checker does not show any issues.
    //
    // Objects can be removed from this set after one release.
    let delete_exceptions: HashSet<SystemObjectDescription> = [].into();
    // TODO(jkosh44) Technically we could support changing the type of a builtin object outside
    // of unstable schemas (i.e. from a table to a view). However, builtin migrations don't currently
    // handle that scenario correctly.
    assert!(
        deleted_system_objects
            .iter()
            // It's okay if Indexes change because they're inherently ephemeral.
            .filter(|object| object.object_type != CatalogItemType::Index)
            .all(
                |deleted_object| is_unstable_schema(&deleted_object.schema_name)
                    || delete_exceptions.contains(deleted_object)
            ),
        "only objects in unstable schemas can be deleted, deleted objects: {:?}",
        deleted_system_objects
    );
    txn.remove_items(&deleted_runtime_alterable_system_ids)?;
    txn.remove_system_object_mappings(deleted_system_objects)?;

    Ok((migrated_builtin_ids, new_builtin_ids))
}

fn add_new_builtin_clusters_migration(
    txn: &mut mz_catalog::durable::Transaction<'_>,
    builtin_cluster_sizes: &BuiltinBootstrapClusterSizes,
) -> Result<(), mz_catalog::durable::CatalogError> {
    let cluster_names: BTreeSet<_> = txn.get_clusters().map(|cluster| cluster.name).collect();
    for builtin_cluster in BUILTIN_CLUSTERS {
        if !cluster_names.contains(builtin_cluster.name) {
            let cluster_size = builtin_cluster_sizes.get_size(builtin_cluster.name)?;
            let id = txn.get_and_increment_id(SYSTEM_CLUSTER_ID_ALLOC_KEY.to_string())?;
            let id = ClusterId::System(id);
            txn.insert_system_cluster(
                id,
                builtin_cluster.name,
                vec![],
                builtin_cluster.privileges.to_vec(),
                builtin_cluster.owner_id.to_owned(),
                mz_catalog::durable::ClusterConfig {
                    variant: mz_catalog::durable::ClusterVariant::Managed(ClusterVariantManaged {
                        size: cluster_size.clone(),
                        availability_zones: vec![],
                        replication_factor: builtin_cluster.replication_factor,
                        disk: is_cluster_size_v2(&cluster_size),
                        logging: default_logging_config(),
                        optimizer_feature_overrides: Default::default(),
                        schedule: Default::default(),
                    }),
                    workload_class: None,
                },
                &HashSet::new(),
            )?;
        }
    }
    Ok(())
}

fn add_new_remove_old_builtin_introspection_source_migration(
    txn: &mut mz_catalog::durable::Transaction<'_>,
) -> Result<(), AdapterError> {
    let mut new_indexes = Vec::new();
    let mut removed_indexes = BTreeSet::new();
    for cluster in txn.get_clusters() {
        let mut introspection_source_index_ids = txn.get_introspection_source_indexes(cluster.id);

        let mut new_logs = Vec::new();

        for log in BUILTINS::logs() {
            if introspection_source_index_ids.remove(log.name).is_none() {
                new_logs.push(log);
            }
        }

        let new_ids = txn.allocate_system_item_ids(usize_to_u64(new_logs.len()))?;
        assert_eq!(new_logs.len(), new_ids.len());
        for (log, index_id) in new_logs.into_iter().zip(new_ids) {
            new_indexes.push((cluster.id, log.name.to_string(), index_id));
        }

        // Anything left in `introspection_source_index_ids` must have been deleted and should be
        // removed from the catalog.
        removed_indexes.extend(
            introspection_source_index_ids
                .into_keys()
                .map(|name| (cluster.id, name)),
        );
    }
    txn.insert_introspection_source_indexes(new_indexes, &HashSet::new())?;
    txn.remove_introspection_source_indexes(removed_indexes)?;
    Ok(())
}

fn add_new_builtin_roles_migration(
    txn: &mut mz_catalog::durable::Transaction<'_>,
) -> Result<(), mz_catalog::durable::CatalogError> {
    let role_names: BTreeSet<_> = txn.get_roles().map(|role| role.name).collect();
    for builtin_role in BUILTIN_ROLES {
        if !role_names.contains(builtin_role.name) {
            txn.insert_builtin_role(
                builtin_role.id,
                builtin_role.name.to_string(),
                builtin_role.attributes.clone(),
                RoleMembership::new(),
                RoleVars::default(),
                builtin_role.oid,
            )?;
        }
    }
    Ok(())
}

fn add_new_builtin_cluster_replicas_migration(
    txn: &mut Transaction<'_>,
    builtin_cluster_sizes: &BuiltinBootstrapClusterSizes,
) -> Result<(), AdapterError> {
    let cluster_lookup: BTreeMap<_, _> = txn
        .get_clusters()
        .map(|cluster| (cluster.name.clone(), cluster.clone()))
        .collect();

    let replicas: BTreeMap<_, _> =
        txn.get_cluster_replicas()
            .fold(BTreeMap::new(), |mut acc, replica| {
                acc.entry(replica.cluster_id)
                    .or_insert_with(BTreeSet::new)
                    .insert(replica.name);
                acc
            });

    for builtin_replica in BUILTIN_CLUSTER_REPLICAS {
        let cluster = cluster_lookup
            .get(builtin_replica.cluster_name)
            .expect("builtin cluster replica references non-existent cluster");
        let replica_names = replicas.get(&cluster.id);
        if matches!(replica_names, None)
            || matches!(replica_names, Some(names) if !names.contains(builtin_replica.name))
        {
            let replica_size = match cluster.config.variant {
                ClusterVariant::Managed(ClusterVariantManaged { ref size, .. }) => size.clone(),
                ClusterVariant::Unmanaged => {
                    builtin_cluster_sizes.get_size(builtin_replica.cluster_name)?
                }
            };

            let replica_id = txn.get_and_increment_id(SYSTEM_REPLICA_ID_ALLOC_KEY.to_string())?;
            let replica_id = ReplicaId::System(replica_id);
            let config = builtin_cluster_replica_config(replica_size);
            txn.insert_cluster_replica(
                cluster.id,
                replica_id,
                builtin_replica.name,
                config,
                MZ_SYSTEM_ROLE_ID,
            )?;
        }
    }

    Ok(())
}

/// Roles can have default values for configuration parameters, e.g. you can set a Role default for
/// the 'cluster' parameter.
///
/// This migration exists to remove the Role default for a configuration parameter, if the persisted
/// input is no longer valid. For example if we remove a configuration parameter or change the
/// accepted set of values.
fn remove_invalid_config_param_role_defaults_migration(
    txn: &mut Transaction<'_>,
) -> Result<(), AdapterError> {
    static BUILD_INFO: mz_build_info::BuildInfo = mz_build_info::build_info!();

    let roles_to_migrate: BTreeMap<_, _> = txn
        .get_roles()
        .filter_map(|mut role| {
            // Create an empty SessionVars just so we can check if a var is valid.
            //
            // TODO(parkmycar): This is a bit hacky, instead we should have a static list of all
            // session variables.
            let session_vars = SessionVars::new_unchecked(&BUILD_INFO, SYSTEM_USER.clone());

            // Iterate over all of the variable defaults for this role.
            let mut invalid_roles_vars = BTreeMap::new();
            for (name, value) in &role.vars.map {
                // If one does not exist or its value is invalid, then mark it for removal.
                let Ok(session_var) = session_vars.inspect(name) else {
                    invalid_roles_vars.insert(name.clone(), value.clone());
                    continue;
                };
                if session_var.check(value.borrow()).is_err() {
                    invalid_roles_vars.insert(name.clone(), value.clone());
                }
            }

            // If the role has no invalid values, nothing to do!
            if invalid_roles_vars.is_empty() {
                return None;
            }

            tracing::warn!(?role, ?invalid_roles_vars, "removing invalid role vars");

            // Otherwise, remove the variables from the role and return it to be updated.
            for (name, _value) in invalid_roles_vars {
                role.vars.map.remove(&name);
            }
            Some(role)
        })
        .map(|role| (role.id, role))
        .collect();

    txn.update_roles(roles_to_migrate)?;

    Ok(())
}

/// Cluster Replicas may be created ephemerally during an alter statement, these replicas
/// are marked as pending and should be cleaned up on catalog opsn.
fn remove_pending_cluster_replicas_migration(tx: &mut Transaction) -> Result<(), anyhow::Error> {
    for replica in tx.get_cluster_replicas() {
        if let mz_catalog::durable::ReplicaLocation::Managed { pending: true, .. } =
            replica.config.location
        {
            tx.remove_cluster_replica(replica.replica_id)?;
        }
    }
    Ok(())
}

pub(crate) fn builtin_cluster_replica_config(
    replica_size: String,
) -> mz_catalog::durable::ReplicaConfig {
    mz_catalog::durable::ReplicaConfig {
        location: mz_catalog::durable::ReplicaLocation::Managed {
            availability_zone: None,
            billed_as: None,
            disk: is_cluster_size_v2(&replica_size),
            pending: false,
            internal: false,
            size: replica_size,
        },
        logging: default_logging_config(),
    }
}

fn default_logging_config() -> ReplicaLogging {
    ReplicaLogging {
        log_logging: false,
        interval: Some(Duration::from_secs(1)),
    }
}
pub struct BuiltinBootstrapClusterSizes {
    /// Size to default system_cluster on bootstrap
    pub system_cluster: String,
    /// Size to default catalog_server_cluster on bootstrap
    pub catalog_server_cluster: String,
    /// Size to default probe_cluster on bootstrap
    pub probe_cluster: String,
    /// Size to default support_cluster on bootstrap
    pub support_cluster: String,
    /// Size to default analytics_cluster on bootstrap
    pub analytics_cluster: String,
}

impl BuiltinBootstrapClusterSizes {
    /// Gets the size of the builtin cluster based on the provided name
    fn get_size(&self, cluster_name: &str) -> Result<String, mz_catalog::durable::CatalogError> {
        if cluster_name == mz_catalog::builtin::MZ_SYSTEM_CLUSTER.name {
            Ok(self.system_cluster.clone())
        } else if cluster_name == mz_catalog::builtin::MZ_CATALOG_SERVER_CLUSTER.name {
            Ok(self.catalog_server_cluster.clone())
        } else if cluster_name == mz_catalog::builtin::MZ_PROBE_CLUSTER.name {
            Ok(self.probe_cluster.clone())
        } else if cluster_name == mz_catalog::builtin::MZ_SUPPORT_CLUSTER.name {
            Ok(self.support_cluster.clone())
        } else if cluster_name == mz_catalog::builtin::MZ_ANALYTICS_CLUSTER.name {
            Ok(self.analytics_cluster.clone())
        } else {
            Err(mz_catalog::durable::CatalogError::Catalog(
                SqlCatalogError::UnexpectedBuiltinCluster(cluster_name.to_owned()),
            ))
        }
    }
}

/// Convert `updates` into a `Vec` that can be consolidated by doing the following:
///
///   - Convert each update into a type that implements [`std::cmp::Ord`].
///   - Update the timestamp of each update to the same value.
///   - Convert the diff of each update to a type that implements
///     [`differential_dataflow::difference::Semigroup`].
///
/// [`mz_catalog::memory::objects::StateUpdateKind`] does not implement [`std::cmp::Ord`] only
/// because it contains a variant for temporary items, which do not implement [`std::cmp::Ord`].
/// However, we know that during bootstrap no temporary items exist, because they are not persisted
/// and are only created after bootstrap is complete. So we forcibly convert each
/// [`mz_catalog::memory::objects::StateUpdateKind`] into an [`BootstrapStateUpdateKind`], which is
/// identical to [`mz_catalog::memory::objects::StateUpdateKind`] except it doesn't have a
/// temporary item variant and does implement [`std::cmp::Ord`].
///
/// WARNING: Do not call outside of startup.
pub(crate) fn into_consolidatable_updates_startup(
    updates: Vec<StateUpdate>,
    ts: Timestamp,
) -> Vec<(BootstrapStateUpdateKind, Timestamp, Diff)> {
    updates
        .into_iter()
        .map(|StateUpdate { kind, ts: _, diff }| {
            let kind: BootstrapStateUpdateKind = kind
                .try_into()
                .unwrap_or_else(|e| panic!("temporary items do not exist during bootstrap: {e:?}"));
            (kind, ts, Diff::from(diff))
        })
        .collect()
}

#[cfg(test)]
mod builtin_migration_tests {
    use std::collections::{BTreeMap, BTreeSet};

    use itertools::Itertools;
    use mz_catalog::memory::objects::{
        CatalogItem, Index, MaterializedView, Table, TableDataSource,
    };
    use mz_catalog::SYSTEM_CONN_ID;
    use mz_controller_types::ClusterId;
    use mz_expr::MirRelationExpr;
    use mz_repr::{GlobalId, RelationDesc, RelationType, ScalarType};
    use mz_sql::catalog::CatalogDatabase;
    use mz_sql::names::{
        ItemQualifiers, QualifiedItemName, ResolvedDatabaseSpecifier, ResolvedIds,
    };
    use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;
    use mz_sql::DEFAULT_SCHEMA;
    use mz_sql_parser::ast::Expr;

    use crate::catalog::{Catalog, Op, OptimizedMirRelationExpr};
    use crate::session::DEFAULT_DATABASE_NAME;

    enum ItemNamespace {
        System,
        User,
    }

    enum SimplifiedItem {
        Table,
        MaterializedView { referenced_names: Vec<String> },
        Index { on: String },
    }

    struct SimplifiedCatalogEntry {
        name: String,
        namespace: ItemNamespace,
        item: SimplifiedItem,
    }

    impl SimplifiedCatalogEntry {
        // A lot of the fields here aren't actually used in the test so we can fill them in with dummy
        // values.
        fn to_catalog_item(
            self,
            id_mapping: &BTreeMap<String, GlobalId>,
        ) -> (String, ItemNamespace, CatalogItem) {
            let item = match self.item {
                SimplifiedItem::Table => CatalogItem::Table(Table {
                    create_sql: Some("CREATE TABLE materialize.public.t (a INT)".to_string()),
                    desc: RelationDesc::builder()
                        .with_column("a", ScalarType::Int32.nullable(true))
                        .with_key(vec![0])
                        .finish(),
                    conn_id: None,
                    resolved_ids: ResolvedIds(BTreeSet::new()),
                    custom_logical_compaction_window: None,
                    is_retained_metrics_object: false,
                    data_source: TableDataSource::TableWrites {
                        defaults: vec![Expr::null(); 1],
                    },
                }),
                SimplifiedItem::MaterializedView { referenced_names } => {
                    let table_list = referenced_names
                        .iter()
                        .map(|table| format!("materialize.public.{table}"))
                        .join(",");
                    let column_list = referenced_names
                        .iter()
                        .enumerate()
                        .map(|(idx, _)| format!("a{idx}"))
                        .join(",");
                    let resolved_ids = convert_names_to_ids(referenced_names, id_mapping);
                    CatalogItem::MaterializedView(MaterializedView {
                        create_sql: format!(
                            "CREATE MATERIALIZED VIEW materialize.public.mv ({column_list}) AS SELECT * FROM {table_list}"
                        ),
                        raw_expr: mz_sql::plan::HirRelationExpr::constant(
                            Vec::new(),
                            RelationType {
                                column_types: Vec::new(),
                                keys: Vec::new(),
                            },
                        ).into(),
                        optimized_expr: OptimizedMirRelationExpr(MirRelationExpr::Constant {
                            rows: Ok(Vec::new()),
                            typ: RelationType {
                                column_types: Vec::new(),
                                keys: Vec::new(),
                            },
                        }).into(),
                        desc: RelationDesc::builder()
                            .with_column("a", ScalarType::Int32.nullable(true))
                            .with_key(vec![0])
                            .finish(),
                        resolved_ids: ResolvedIds(resolved_ids),
                        cluster_id: ClusterId::User(1),
                        non_null_assertions: vec![],
                        custom_logical_compaction_window: None,
                        refresh_schedule: None,
                        initial_as_of: None,
                    })
                }
                SimplifiedItem::Index { on } => {
                    let on_id = id_mapping[&on];
                    CatalogItem::Index(Index {
                        create_sql: format!("CREATE INDEX idx ON materialize.public.{on} (a)"),
                        on: on_id,
                        keys: Default::default(),
                        conn_id: None,
                        resolved_ids: ResolvedIds(BTreeSet::from_iter([on_id])),
                        cluster_id: ClusterId::User(1),
                        custom_logical_compaction_window: None,
                        is_retained_metrics_object: false,
                    })
                }
            };
            (self.name, self.namespace, item)
        }
    }

    struct BuiltinMigrationTestCase {
        test_name: &'static str,
        initial_state: Vec<SimplifiedCatalogEntry>,
        migrated_names: Vec<String>,
        expected_previous_storage_collection_names: Vec<String>,
        expected_migrated_system_object_mappings: Vec<String>,
        expected_user_item_drop_ops: Vec<String>,
        expected_user_item_create_ops: Vec<String>,
    }

    async fn add_item(
        catalog: &mut Catalog,
        name: String,
        item: CatalogItem,
        item_namespace: ItemNamespace,
    ) -> GlobalId {
        let id = match item_namespace {
            ItemNamespace::User => catalog
                .allocate_user_id()
                .await
                .expect("cannot fail to allocate user ids"),
            ItemNamespace::System => catalog
                .allocate_system_id()
                .await
                .expect("cannot fail to allocate system ids"),
        };
        let database_id = catalog
            .resolve_database(DEFAULT_DATABASE_NAME)
            .expect("failed to resolve default database")
            .id();
        let database_spec = ResolvedDatabaseSpecifier::Id(database_id);
        let schema_spec = catalog
            .resolve_schema_in_database(&database_spec, DEFAULT_SCHEMA, &SYSTEM_CONN_ID)
            .expect("failed to resolve default schemas")
            .id
            .clone();
        catalog
            .transact(
                None,
                mz_repr::Timestamp::MIN,
                None,
                vec![Op::CreateItem {
                    id,
                    name: QualifiedItemName {
                        qualifiers: ItemQualifiers {
                            database_spec,
                            schema_spec,
                        },
                        item: name,
                    },
                    item,
                    owner_id: MZ_SYSTEM_ROLE_ID,
                }],
            )
            .await
            .expect("failed to transact");
        id
    }

    fn convert_names_to_ids(
        name_vec: Vec<String>,
        id_lookup: &BTreeMap<String, GlobalId>,
    ) -> BTreeSet<GlobalId> {
        name_vec.into_iter().map(|name| id_lookup[&name]).collect()
    }

    fn convert_ids_to_names<I: IntoIterator<Item = GlobalId>>(
        ids: I,
        name_lookup: &BTreeMap<GlobalId, String>,
    ) -> BTreeSet<String> {
        ids.into_iter().map(|id| name_lookup[&id].clone()).collect()
    }

    async fn run_test_case(test_case: BuiltinMigrationTestCase) {
        Catalog::with_debug(|mut catalog| async move {
            let mut id_mapping = BTreeMap::new();
            let mut name_mapping = BTreeMap::new();
            for entry in test_case.initial_state {
                let (name, namespace, item) = entry.to_catalog_item(&id_mapping);
                let id = add_item(&mut catalog, name.clone(), item, namespace).await;
                id_mapping.insert(name.clone(), id);
                name_mapping.insert(id, name);
            }

            let migrated_ids = test_case
                .migrated_names
                .into_iter()
                .map(|name| id_mapping[&name])
                .collect();
            let id_fingerprint_map: BTreeMap<GlobalId, String> = id_mapping
                .iter()
                .filter(|(_name, id)| id.is_system())
                // We don't use the new fingerprint in this test, so we can just hard code it
                .map(|(_name, id)| (*id, "".to_string()))
                .collect();

            let migration_metadata = {
                // This cloning is a hacky way to appease the borrow checker. It doesn't really
                // matter because we never look at catalog again. We could probably rewrite this
                // test to not even need a `Catalog` which would significantly speed it up.
                let state = catalog.state.clone();
                let mut storage = catalog.storage().await;
                let mut txn = storage
                    .transaction()
                    .await
                    .expect("failed to create transaction");
                Catalog::generate_builtin_migration_metadata(
                    &state,
                    &mut txn,
                    migrated_ids,
                    id_fingerprint_map,
                )
                .expect("failed to generate builtin migration metadata")
            };

            assert_eq!(
                convert_ids_to_names(
                    migration_metadata.previous_storage_collection_ids,
                    &name_mapping
                ),
                test_case
                    .expected_previous_storage_collection_names
                    .into_iter()
                    .collect(),
                "{} test failed with wrong previous collection_names",
                test_case.test_name
            );
            assert_eq!(
                migration_metadata
                    .migrated_system_object_mappings
                    .values()
                    .map(|mapping| mapping.description.object_name.clone())
                    .collect::<BTreeSet<_>>(),
                test_case
                    .expected_migrated_system_object_mappings
                    .into_iter()
                    .collect(),
                "{} test failed with wrong migrated system object mappings",
                test_case.test_name
            );
            assert_eq!(
                convert_ids_to_names(migration_metadata.user_item_drop_ops, &name_mapping),
                test_case.expected_user_item_drop_ops.into_iter().collect(),
                "{} test failed with wrong user drop ops",
                test_case.test_name
            );
            assert_eq!(
                migration_metadata
                    .user_item_create_ops
                    .into_iter()
                    .map(|create_op| create_op.name)
                    .collect::<BTreeSet<_>>(),
                test_case
                    .expected_user_item_create_ops
                    .into_iter()
                    .collect(),
                "{} test failed with wrong user create ops",
                test_case.test_name
            );
            catalog.expire().await;
        })
        .await
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn test_builtin_migration_no_migrations() {
        let test_case = BuiltinMigrationTestCase {
            test_name: "no_migrations",
            initial_state: vec![SimplifiedCatalogEntry {
                name: "s1".to_string(),
                namespace: ItemNamespace::System,
                item: SimplifiedItem::Table,
            }],
            migrated_names: vec![],
            expected_previous_storage_collection_names: vec![],
            expected_migrated_system_object_mappings: vec![],
            expected_user_item_drop_ops: vec![],
            expected_user_item_create_ops: vec![],
        };
        run_test_case(test_case).await;
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn test_builtin_migration_single_migrations() {
        let test_case = BuiltinMigrationTestCase {
            test_name: "single_migrations",
            initial_state: vec![SimplifiedCatalogEntry {
                name: "s1".to_string(),
                namespace: ItemNamespace::System,
                item: SimplifiedItem::Table,
            }],
            migrated_names: vec!["s1".to_string()],
            expected_previous_storage_collection_names: vec!["s1".to_string()],
            expected_migrated_system_object_mappings: vec!["s1".to_string()],
            expected_user_item_drop_ops: vec![],
            expected_user_item_create_ops: vec![],
        };
        run_test_case(test_case).await;
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn test_builtin_migration_child_migrations() {
        let test_case = BuiltinMigrationTestCase {
            test_name: "child_migrations",
            initial_state: vec![
                SimplifiedCatalogEntry {
                    name: "s1".to_string(),
                    namespace: ItemNamespace::System,
                    item: SimplifiedItem::Table,
                },
                SimplifiedCatalogEntry {
                    name: "u1".to_string(),
                    namespace: ItemNamespace::User,
                    item: SimplifiedItem::MaterializedView {
                        referenced_names: vec!["s1".to_string()],
                    },
                },
            ],
            migrated_names: vec!["s1".to_string()],
            expected_previous_storage_collection_names: vec!["u1".to_string(), "s1".to_string()],
            expected_migrated_system_object_mappings: vec!["s1".to_string()],
            expected_user_item_drop_ops: vec!["u1".to_string()],
            expected_user_item_create_ops: vec!["u1".to_string()],
        };
        run_test_case(test_case).await;
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn test_builtin_migration_multi_child_migrations() {
        let test_case = BuiltinMigrationTestCase {
            test_name: "multi_child_migrations",
            initial_state: vec![
                SimplifiedCatalogEntry {
                    name: "s1".to_string(),
                    namespace: ItemNamespace::System,
                    item: SimplifiedItem::Table,
                },
                SimplifiedCatalogEntry {
                    name: "u1".to_string(),
                    namespace: ItemNamespace::User,
                    item: SimplifiedItem::MaterializedView {
                        referenced_names: vec!["s1".to_string()],
                    },
                },
                SimplifiedCatalogEntry {
                    name: "u2".to_string(),
                    namespace: ItemNamespace::User,
                    item: SimplifiedItem::MaterializedView {
                        referenced_names: vec!["s1".to_string()],
                    },
                },
            ],
            migrated_names: vec!["s1".to_string()],
            expected_previous_storage_collection_names: vec![
                "u1".to_string(),
                "u2".to_string(),
                "s1".to_string(),
            ],
            expected_migrated_system_object_mappings: vec!["s1".to_string()],
            expected_user_item_drop_ops: vec!["u1".to_string(), "u2".to_string()],
            expected_user_item_create_ops: vec!["u2".to_string(), "u1".to_string()],
        };
        run_test_case(test_case).await;
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn test_builtin_migration_topological_sort() {
        let test_case = BuiltinMigrationTestCase {
            test_name: "topological_sort",
            initial_state: vec![
                SimplifiedCatalogEntry {
                    name: "s1".to_string(),
                    namespace: ItemNamespace::System,
                    item: SimplifiedItem::Table,
                },
                SimplifiedCatalogEntry {
                    name: "s2".to_string(),
                    namespace: ItemNamespace::System,
                    item: SimplifiedItem::Table,
                },
                SimplifiedCatalogEntry {
                    name: "u1".to_string(),
                    namespace: ItemNamespace::User,
                    item: SimplifiedItem::MaterializedView {
                        referenced_names: vec!["s2".to_string()],
                    },
                },
                SimplifiedCatalogEntry {
                    name: "u2".to_string(),
                    namespace: ItemNamespace::User,
                    item: SimplifiedItem::MaterializedView {
                        referenced_names: vec!["s1".to_string(), "u1".to_string()],
                    },
                },
            ],
            migrated_names: vec!["s1".to_string(), "s2".to_string()],
            expected_previous_storage_collection_names: vec![
                "u2".to_string(),
                "u1".to_string(),
                "s1".to_string(),
                "s2".to_string(),
            ],
            expected_migrated_system_object_mappings: vec!["s1".to_string(), "s2".to_string()],
            expected_user_item_drop_ops: vec!["u2".to_string(), "u1".to_string()],
            expected_user_item_create_ops: vec!["u1".to_string(), "u2".to_string()],
        };
        run_test_case(test_case).await;
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn test_builtin_migration_topological_sort_complex() {
        let test_case = BuiltinMigrationTestCase {
            test_name: "topological_sort_complex",
            initial_state: vec![
                SimplifiedCatalogEntry {
                    name: "s273".to_string(),
                    namespace: ItemNamespace::System,
                    item: SimplifiedItem::Table,
                },
                SimplifiedCatalogEntry {
                    name: "s322".to_string(),
                    namespace: ItemNamespace::System,
                    item: SimplifiedItem::Table,
                },
                SimplifiedCatalogEntry {
                    name: "s317".to_string(),
                    namespace: ItemNamespace::System,
                    item: SimplifiedItem::Table,
                },
                SimplifiedCatalogEntry {
                    name: "s349".to_string(),
                    namespace: ItemNamespace::System,
                    item: SimplifiedItem::MaterializedView {
                        referenced_names: vec!["s273".to_string()],
                    },
                },
                SimplifiedCatalogEntry {
                    name: "s421".to_string(),
                    namespace: ItemNamespace::System,
                    item: SimplifiedItem::MaterializedView {
                        referenced_names: vec!["s273".to_string()],
                    },
                },
                SimplifiedCatalogEntry {
                    name: "s295".to_string(),
                    namespace: ItemNamespace::System,
                    item: SimplifiedItem::MaterializedView {
                        referenced_names: vec!["s273".to_string()],
                    },
                },
                SimplifiedCatalogEntry {
                    name: "s296".to_string(),
                    namespace: ItemNamespace::System,
                    item: SimplifiedItem::MaterializedView {
                        referenced_names: vec!["s295".to_string()],
                    },
                },
                SimplifiedCatalogEntry {
                    name: "s320".to_string(),
                    namespace: ItemNamespace::System,
                    item: SimplifiedItem::MaterializedView {
                        referenced_names: vec!["s295".to_string()],
                    },
                },
                SimplifiedCatalogEntry {
                    name: "s340".to_string(),
                    namespace: ItemNamespace::System,
                    item: SimplifiedItem::MaterializedView {
                        referenced_names: vec!["s295".to_string()],
                    },
                },
                SimplifiedCatalogEntry {
                    name: "s318".to_string(),
                    namespace: ItemNamespace::System,
                    item: SimplifiedItem::MaterializedView {
                        referenced_names: vec!["s295".to_string()],
                    },
                },
                SimplifiedCatalogEntry {
                    name: "s323".to_string(),
                    namespace: ItemNamespace::System,
                    item: SimplifiedItem::MaterializedView {
                        referenced_names: vec!["s295".to_string(), "s322".to_string()],
                    },
                },
                SimplifiedCatalogEntry {
                    name: "s330".to_string(),
                    namespace: ItemNamespace::System,
                    item: SimplifiedItem::MaterializedView {
                        referenced_names: vec!["s318".to_string(), "s317".to_string()],
                    },
                },
                SimplifiedCatalogEntry {
                    name: "s321".to_string(),
                    namespace: ItemNamespace::System,
                    item: SimplifiedItem::MaterializedView {
                        referenced_names: vec!["s318".to_string()],
                    },
                },
                SimplifiedCatalogEntry {
                    name: "s315".to_string(),
                    namespace: ItemNamespace::System,
                    item: SimplifiedItem::MaterializedView {
                        referenced_names: vec!["s296".to_string()],
                    },
                },
                SimplifiedCatalogEntry {
                    name: "s354".to_string(),
                    namespace: ItemNamespace::System,
                    item: SimplifiedItem::MaterializedView {
                        referenced_names: vec!["s296".to_string()],
                    },
                },
                SimplifiedCatalogEntry {
                    name: "s327".to_string(),
                    namespace: ItemNamespace::System,
                    item: SimplifiedItem::MaterializedView {
                        referenced_names: vec!["s296".to_string()],
                    },
                },
                SimplifiedCatalogEntry {
                    name: "s339".to_string(),
                    namespace: ItemNamespace::System,
                    item: SimplifiedItem::MaterializedView {
                        referenced_names: vec!["s296".to_string()],
                    },
                },
                SimplifiedCatalogEntry {
                    name: "s355".to_string(),
                    namespace: ItemNamespace::System,
                    item: SimplifiedItem::MaterializedView {
                        referenced_names: vec!["s315".to_string()],
                    },
                },
            ],
            migrated_names: vec![
                "s273".to_string(),
                "s317".to_string(),
                "s318".to_string(),
                "s320".to_string(),
                "s321".to_string(),
                "s322".to_string(),
                "s323".to_string(),
                "s330".to_string(),
                "s339".to_string(),
                "s340".to_string(),
            ],
            expected_previous_storage_collection_names: vec![
                "s349".to_string(),
                "s421".to_string(),
                "s355".to_string(),
                "s315".to_string(),
                "s354".to_string(),
                "s327".to_string(),
                "s339".to_string(),
                "s296".to_string(),
                "s320".to_string(),
                "s340".to_string(),
                "s330".to_string(),
                "s321".to_string(),
                "s318".to_string(),
                "s323".to_string(),
                "s295".to_string(),
                "s273".to_string(),
                "s317".to_string(),
                "s322".to_string(),
            ],
            expected_migrated_system_object_mappings: vec![
                "s322".to_string(),
                "s317".to_string(),
                "s273".to_string(),
                "s295".to_string(),
                "s323".to_string(),
                "s318".to_string(),
                "s321".to_string(),
                "s330".to_string(),
                "s340".to_string(),
                "s320".to_string(),
                "s296".to_string(),
                "s339".to_string(),
                "s327".to_string(),
                "s354".to_string(),
                "s315".to_string(),
                "s355".to_string(),
                "s421".to_string(),
                "s349".to_string(),
            ],
            expected_user_item_drop_ops: vec![],
            expected_user_item_create_ops: vec![],
        };
        run_test_case(test_case).await;
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn test_builtin_migration_system_child_migrations() {
        let test_case = BuiltinMigrationTestCase {
            test_name: "system_child_migrations",
            initial_state: vec![
                SimplifiedCatalogEntry {
                    name: "s1".to_string(),
                    namespace: ItemNamespace::System,
                    item: SimplifiedItem::Table,
                },
                SimplifiedCatalogEntry {
                    name: "s2".to_string(),
                    namespace: ItemNamespace::System,
                    item: SimplifiedItem::Index {
                        on: "s1".to_string(),
                    },
                },
            ],
            migrated_names: vec!["s1".to_string()],
            expected_previous_storage_collection_names: vec!["s1".to_string()],
            expected_migrated_system_object_mappings: vec!["s1".to_string(), "s2".to_string()],
            expected_user_item_drop_ops: vec![],
            expected_user_item_create_ops: vec![],
        };
        run_test_case(test_case).await;
    }
}
