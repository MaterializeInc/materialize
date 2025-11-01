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
use std::sync::atomic::AtomicU64;
use std::time::{Duration, Instant};

use futures::future::{BoxFuture, FutureExt};
use itertools::{Either, Itertools};
use mz_adapter_types::bootstrap_builtin_cluster_config::BootstrapBuiltinClusterConfig;
use mz_adapter_types::dyncfgs::{ENABLE_CONTINUAL_TASK_BUILTINS, ENABLE_EXPRESSION_CACHE};
use mz_auth::hash::scram256_hash;
use mz_catalog::SYSTEM_CONN_ID;
use mz_catalog::builtin::{
    BUILTIN_CLUSTER_REPLICAS, BUILTIN_CLUSTERS, BUILTIN_PREFIXES, BUILTIN_ROLES, BUILTINS, Builtin,
    Fingerprint, MZ_STORAGE_USAGE_BY_SHARD_DESCRIPTION, RUNTIME_ALTERABLE_FINGERPRINT_SENTINEL,
};
use mz_catalog::config::StateConfig;
use mz_catalog::durable::objects::{
    SystemObjectDescription, SystemObjectMapping, SystemObjectUniqueIdentifier,
};
use mz_catalog::durable::{ClusterReplica, ClusterVariant, ClusterVariantManaged, Transaction};
use mz_catalog::expr_cache::{
    ExpressionCacheConfig, ExpressionCacheHandle, GlobalExpressions, LocalExpressions,
};
use mz_catalog::memory::error::{Error, ErrorKind};
use mz_catalog::memory::objects::{
    BootstrapStateUpdateKind, CommentsMap, DefaultPrivileges, RoleAuth, StateUpdate,
};
use mz_controller::clusters::ReplicaLogging;
use mz_controller_types::ClusterId;
use mz_ore::cast::usize_to_u64;
use mz_ore::collections::HashSet;
use mz_ore::now::{SYSTEM_TIME, to_datetime};
use mz_ore::{instrument, soft_assert_no_log};
use mz_repr::adt::mz_acl_item::PrivilegeMap;
use mz_repr::namespaces::is_unstable_schema;
use mz_repr::{CatalogItemId, Diff, GlobalId, Timestamp};
use mz_sql::catalog::{
    BuiltinsConfig, CatalogError as SqlCatalogError, CatalogItemType, RoleMembership, RoleVars,
};
use mz_sql::func::OP_IMPLS;
use mz_sql::names::CommentObjectId;
use mz_sql::rbac;
use mz_sql::session::user::{MZ_SYSTEM_ROLE_ID, SYSTEM_USER};
use mz_sql::session::vars::{SessionVars, SystemVars, VarError, VarInput};
use mz_storage_client::storage_collections::StorageCollections;
use tracing::{Instrument, info, warn};
use uuid::Uuid;

// DO NOT add any more imports from `crate` outside of `crate::catalog`.
use crate::AdapterError;
use crate::catalog::open::builtin_item_migration::{
    BuiltinItemMigrationResult, migrate_builtin_items,
};
use crate::catalog::state::LocalExpressionCache;
use crate::catalog::{
    BuiltinTableUpdate, Catalog, CatalogPlans, CatalogState, Config, is_reserved_name, migrate,
};

pub struct InitializeStateResult {
    /// An initialized [`CatalogState`].
    pub state: CatalogState,
    /// A set of new shards that may need to be initialized (only used by 0dt migration).
    pub migrated_storage_collections_0dt: BTreeSet<CatalogItemId>,
    /// A set of new builtin items.
    pub new_builtin_collections: BTreeSet<GlobalId>,
    /// A list of builtin table updates corresponding to the initialized state.
    pub builtin_table_updates: Vec<BuiltinTableUpdate>,
    /// The version of the catalog that existed before initializing the catalog.
    pub last_seen_version: String,
    /// A handle to the expression cache if it's enabled.
    pub expr_cache_handle: Option<ExpressionCacheHandle>,
    /// The global expressions that were cached in `expr_cache_handle`.
    pub cached_global_exprs: BTreeMap<GlobalId, GlobalExpressions>,
    /// The local expressions that were NOT cached in `expr_cache_handle`.
    pub uncached_local_exprs: BTreeMap<GlobalId, LocalExpressions>,
}

pub struct OpenCatalogResult {
    /// An opened [`Catalog`].
    pub catalog: Catalog,
    /// A set of new shards that may need to be initialized.
    pub migrated_storage_collections_0dt: BTreeSet<CatalogItemId>,
    /// A set of new builtin items.
    pub new_builtin_collections: BTreeSet<GlobalId>,
    /// A list of builtin table updates corresponding to the initialized state.
    pub builtin_table_updates: Vec<BuiltinTableUpdate>,
    /// The global expressions that were cached in the expression cache.
    pub cached_global_exprs: BTreeMap<GlobalId, GlobalExpressions>,
    /// The local expressions that were NOT cached in the expression cache.
    pub uncached_local_exprs: BTreeMap<GlobalId, LocalExpressions>,
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

        let mut system_configuration = SystemVars::new().set_unsafe(config.unsafe_mode);
        if config.all_features {
            system_configuration.enable_all_feature_flags_by_default();
        }

        let mut state = CatalogState {
            database_by_name: BTreeMap::new(),
            database_by_id: BTreeMap::new(),
            entry_by_id: BTreeMap::new(),
            entry_by_global_id: BTreeMap::new(),
            ambient_schemas_by_name: BTreeMap::new(),
            ambient_schemas_by_id: BTreeMap::new(),
            clusters_by_name: BTreeMap::new(),
            clusters_by_id: BTreeMap::new(),
            roles_by_name: BTreeMap::new(),
            roles_by_id: BTreeMap::new(),
            network_policies_by_id: BTreeMap::new(),
            role_auth_by_id: BTreeMap::new(),
            network_policies_by_name: BTreeMap::new(),
            system_configuration,
            default_privileges: DefaultPrivileges::default(),
            system_privileges: PrivilegeMap::default(),
            comments: CommentsMap::default(),
            source_references: BTreeMap::new(),
            storage_metadata: Default::default(),
            temporary_schemas: BTreeMap::new(),
            mock_authentication_nonce: Default::default(),
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
                builtins_cfg: BuiltinsConfig {
                    // This will fall back to the default in code (false) if we timeout on the
                    // initial LD sync. We're just using this to get some additional testing in on
                    // CTs so a false negative is fine, we're only worried about false positives.
                    include_continual_tasks: get_dyncfg_val_from_defaults_and_remote(
                        &config.system_parameter_defaults,
                        config.remote_system_parameters.as_ref(),
                        &ENABLE_CONTINUAL_TASK_BUILTINS,
                    ),
                },
                helm_chart_version: config.helm_chart_version,
            },
            cluster_replica_sizes: config.cluster_replica_sizes,
            availability_zones: config.availability_zones,
            egress_addresses: config.egress_addresses,
            aws_principal_context: config.aws_principal_context,
            aws_privatelink_availability_zones: config.aws_privatelink_availability_zones,
            http_host_name: config.http_host_name,
            license_key: config.license_key,
        };

        let mut updates: Vec<_> = storage.sync_to_current_updates().await?;
        assert!(!updates.is_empty(), "initial catalog snapshot is missing");
        let mut txn = storage.transaction().await?;

        // Migrate/update durable data before we start loading the in-memory catalog.
        let (migrated_builtins, new_builtin_collections) = {
            migrate::durable_migrate(
                &mut txn,
                state.config.environment_id.organization_id(),
                config.boot_ts,
            )?;
            // Overwrite and persist selected parameter values in `remote_system_parameters` that
            // was pulled from a remote frontend (e.g. LaunchDarkly) if present.
            if let Some(remote_system_parameters) = config.remote_system_parameters {
                for (name, value) in remote_system_parameters {
                    txn.upsert_system_config(&name, value)?;
                }
                txn.set_system_config_synced_once()?;
            }
            // Add any new builtin objects and remove old ones.
            let (migrated_builtins, new_builtin_collections) =
                add_new_remove_old_builtin_items_migration(&state.config().builtins_cfg, &mut txn)?;
            let builtin_bootstrap_cluster_config_map = BuiltinBootstrapClusterConfigMap {
                system_cluster: config.builtin_system_cluster_config,
                catalog_server_cluster: config.builtin_catalog_server_cluster_config,
                probe_cluster: config.builtin_probe_cluster_config,
                support_cluster: config.builtin_support_cluster_config,
                analytics_cluster: config.builtin_analytics_cluster_config,
            };
            add_new_remove_old_builtin_clusters_migration(
                &mut txn,
                &builtin_bootstrap_cluster_config_map,
            )?;
            add_new_remove_old_builtin_introspection_source_migration(&mut txn)?;
            add_new_remove_old_builtin_cluster_replicas_migration(
                &mut txn,
                &builtin_bootstrap_cluster_config_map,
            )?;
            add_new_remove_old_builtin_roles_migration(&mut txn)?;
            remove_invalid_config_param_role_defaults_migration(&mut txn)?;
            (migrated_builtins, new_builtin_collections)
        };
        remove_pending_cluster_replicas_migration(&mut txn)?;

        let op_updates = txn.get_and_commit_op_updates();
        updates.extend(op_updates);

        let mut builtin_table_updates = Vec::new();

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

        // Make life easier by consolidating all updates, so that we end up with only positive
        // diffs.
        let mut updates = into_consolidatable_updates_startup(updates, config.boot_ts);
        differential_dataflow::consolidation::consolidate_updates(&mut updates);
        soft_assert_no_log!(
            updates.iter().all(|(_, _, diff)| *diff == Diff::ONE),
            "consolidated updates should be positive during startup: {updates:?}"
        );

        let mut pre_item_updates = Vec::new();
        let mut system_item_updates = Vec::new();
        let mut item_updates = Vec::new();
        let mut post_item_updates = Vec::new();
        let mut audit_log_updates = Vec::new();
        for (kind, ts, diff) in updates {
            match kind {
                BootstrapStateUpdateKind::Role(_)
                | BootstrapStateUpdateKind::RoleAuth(_)
                | BootstrapStateUpdateKind::Database(_)
                | BootstrapStateUpdateKind::Schema(_)
                | BootstrapStateUpdateKind::DefaultPrivilege(_)
                | BootstrapStateUpdateKind::SystemPrivilege(_)
                | BootstrapStateUpdateKind::SystemConfiguration(_)
                | BootstrapStateUpdateKind::Cluster(_)
                | BootstrapStateUpdateKind::NetworkPolicy(_)
                | BootstrapStateUpdateKind::ClusterReplica(_) => {
                    pre_item_updates.push(StateUpdate {
                        kind: kind.into(),
                        ts,
                        diff: diff.try_into().expect("valid diff"),
                    })
                }
                BootstrapStateUpdateKind::IntrospectionSourceIndex(_)
                | BootstrapStateUpdateKind::SystemObjectMapping(_) => {
                    system_item_updates.push(StateUpdate {
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
                | BootstrapStateUpdateKind::StorageCollectionMetadata(_)
                | BootstrapStateUpdateKind::SourceReferences(_)
                | BootstrapStateUpdateKind::UnfinalizedShard(_) => {
                    post_item_updates.push((kind, ts, diff));
                }
                BootstrapStateUpdateKind::AuditLog(_) => {
                    audit_log_updates.push(StateUpdate {
                        kind: kind.into(),
                        ts,
                        diff: diff.try_into().expect("valid diff"),
                    });
                }
            }
        }

        let builtin_table_update = state
            .apply_updates_for_bootstrap(pre_item_updates, &mut LocalExpressionCache::Closed)
            .await;
        builtin_table_updates.extend(builtin_table_update);

        // Ensure mz_system has a password if configured to have one.
        // It's important we do this after the `pre_item_updates` so that
        // the mz_system role exists in the catalog.
        {
            if let Some(password) = config.external_login_password_mz_system {
                let role_auth = RoleAuth {
                    role_id: MZ_SYSTEM_ROLE_ID,
                    password_hash: Some(scram256_hash(&password).map_err(|_| {
                        AdapterError::Internal("Failed to hash mz_system password.".to_owned())
                    })?),
                    updated_at: SYSTEM_TIME(),
                };
                state
                    .role_auth_by_id
                    .insert(MZ_SYSTEM_ROLE_ID, role_auth.clone());
                let builtin_table_update = state.generate_builtin_table_update(
                    mz_catalog::memory::objects::StateUpdateKind::RoleAuth(role_auth.into()),
                    mz_catalog::memory::objects::StateDiff::Addition,
                );
                builtin_table_updates.extend(builtin_table_update);
            }
        }

        let expr_cache_start = Instant::now();
        info!("startup: coordinator init: catalog open: expr cache open beginning");
        // We wait until after the `pre_item_updates` to open the cache so we can get accurate
        // dyncfgs because the `pre_item_updates` contains `SystemConfiguration` updates.
        let enable_expr_cache_dyncfg = ENABLE_EXPRESSION_CACHE.get(state.system_config().dyncfgs());
        let expr_cache_enabled = config
            .enable_expression_cache_override
            .unwrap_or(enable_expr_cache_dyncfg);
        let (expr_cache_handle, cached_local_exprs, cached_global_exprs) = if expr_cache_enabled {
            info!(
                ?config.enable_expression_cache_override,
                ?enable_expr_cache_dyncfg,
                "using expression cache for startup"
            );
            let current_ids = txn
                .get_items()
                .flat_map(|item| {
                    let gid = item.global_id.clone();
                    let gids: Vec<_> = item.extra_versions.values().cloned().collect();
                    std::iter::once(gid).chain(gids.into_iter())
                })
                .chain(
                    txn.get_system_object_mappings()
                        .map(|som| som.unique_identifier.global_id),
                )
                .collect();
            let dyncfgs = config.persist_client.dyncfgs().clone();
            let build_version = if config.build_info.is_dev() {
                // A single dev version can be used for many different builds, so we need to use
                // the build version that is also enriched with build metadata.
                config
                    .build_info
                    .semver_version_build()
                    .expect("build ID is not available on your platform!")
            } else {
                config.build_info.semver_version()
            };
            let expr_cache_config = ExpressionCacheConfig {
                build_version,
                shard_id: txn
                    .get_expression_cache_shard()
                    .expect("expression cache shard should exist for opened catalogs"),
                persist: config.persist_client,
                current_ids,
                remove_prior_versions: !config.read_only,
                compact_shard: config.read_only,
                dyncfgs,
            };
            let (expr_cache_handle, cached_local_exprs, cached_global_exprs) =
                ExpressionCacheHandle::spawn_expression_cache(expr_cache_config).await;
            (
                Some(expr_cache_handle),
                cached_local_exprs,
                cached_global_exprs,
            )
        } else {
            (None, BTreeMap::new(), BTreeMap::new())
        };
        let mut local_expr_cache = LocalExpressionCache::new(cached_local_exprs);
        info!(
            "startup: coordinator init: catalog open: expr cache open complete in {:?}",
            expr_cache_start.elapsed()
        );

        let builtin_table_update = state
            .apply_updates_for_bootstrap(system_item_updates, &mut local_expr_cache)
            .await;
        builtin_table_updates.extend(builtin_table_update);

        let last_seen_version = txn
            .get_catalog_content_version()
            .unwrap_or("new")
            .to_string();

        let mz_authentication_mock_nonce =
            txn.get_authentication_mock_nonce().ok_or_else(|| {
                Error::new(ErrorKind::SettingError("authentication nonce".to_string()))
            })?;

        state.mock_authentication_nonce = Some(mz_authentication_mock_nonce);

        // Migrate item ASTs.
        let builtin_table_update = if !config.skip_migrations {
            let migrate_result = migrate::migrate(
                &mut state,
                &mut txn,
                &mut local_expr_cache,
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
            })?;
            if !migrate_result.post_item_updates.is_empty() {
                // Include any post-item-updates generated by migrations, and then consolidate
                // them to ensure diffs are all positive.
                post_item_updates.extend(migrate_result.post_item_updates);
                // Push everything to the same timestamp so it consolidates cleanly.
                if let Some(max_ts) = post_item_updates.iter().map(|(_, ts, _)| ts).max().cloned() {
                    for (_, ts, _) in &mut post_item_updates {
                        *ts = max_ts;
                    }
                }
                differential_dataflow::consolidation::consolidate_updates(&mut post_item_updates);
            }

            migrate_result.builtin_table_updates
        } else {
            state
                .apply_updates_for_bootstrap(item_updates, &mut local_expr_cache)
                .await
        };
        builtin_table_updates.extend(builtin_table_update);

        let post_item_updates = post_item_updates
            .into_iter()
            .map(|(kind, ts, diff)| StateUpdate {
                kind: kind.into(),
                ts,
                diff: diff.try_into().expect("valid diff"),
            })
            .collect();
        let builtin_table_update = state
            .apply_updates_for_bootstrap(post_item_updates, &mut local_expr_cache)
            .await;
        builtin_table_updates.extend(builtin_table_update);

        // We don't need to apply the audit logs in memory, yet apply can be expensive when the
        // audit log grows large. Therefore, we skip the apply step and just generate the builtin
        // updates.
        for audit_log_update in audit_log_updates {
            builtin_table_updates.extend(
                state.generate_builtin_table_update(audit_log_update.kind, audit_log_update.diff),
            );
        }

        // Migrate builtin items.
        let BuiltinItemMigrationResult {
            builtin_table_updates: builtin_table_update,
            migrated_storage_collections_0dt,
            cleanup_action,
        } = migrate_builtin_items(
            &mut state,
            &mut txn,
            &mut local_expr_cache,
            migrated_builtins,
            config.builtin_item_migration_config,
        )
        .await?;
        builtin_table_updates.extend(builtin_table_update);
        let builtin_table_updates = state.resolve_builtin_table_updates(builtin_table_updates);

        txn.commit(config.boot_ts).await?;

        cleanup_action.await;

        Ok(InitializeStateResult {
            state,
            migrated_storage_collections_0dt,
            new_builtin_collections: new_builtin_collections.into_iter().collect(),
            builtin_table_updates,
            last_seen_version,
            expr_cache_handle,
            cached_global_exprs,
            uncached_local_exprs: local_expr_cache.into_uncached_exprs(),
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
                migrated_storage_collections_0dt,
                new_builtin_collections,
                mut builtin_table_updates,
                last_seen_version: _,
                expr_cache_handle,
                cached_global_exprs,
                uncached_local_exprs,
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
                expr_cache_handle,
                transient_revision: 1,
                latest_transient_revision: Arc::new(AtomicU64::new(1)),
                storage: Arc::new(tokio::sync::Mutex::new(storage)),
            };

            // Operators aren't stored in the catalog, but we would like them in
            // introspection views.
            for (op, func) in OP_IMPLS.iter() {
                match func {
                    mz_sql::func::Func::Scalar(impls) => {
                        for imp in impls {
                            builtin_table_updates.push(catalog.state.resolve_builtin_table_update(
                                catalog.state.pack_op_update(op, imp.details(), Diff::ONE),
                            ));
                        }
                    }
                    _ => unreachable!("all operators must be scalar functions"),
                }
            }

            for ip in &catalog.state.egress_addresses {
                builtin_table_updates.push(
                    catalog
                        .state
                        .resolve_builtin_table_update(catalog.state.pack_egress_ip_update(ip)?),
                );
            }

            if !catalog.state.license_key.id.is_empty() {
                builtin_table_updates.push(
                    catalog.state.resolve_builtin_table_update(
                        catalog
                            .state
                            .pack_license_key_update(&catalog.state.license_key)?,
                    ),
                );
            }

            catalog.storage().await.mark_bootstrap_complete();

            Ok(OpenCatalogResult {
                catalog,
                migrated_storage_collections_0dt,
                new_builtin_collections,
                builtin_table_updates,
                cached_global_exprs,
                uncached_local_exprs,
            })
        }
        .instrument(tracing::info_span!("catalog::open"))
        .boxed()
    }

    /// Initializes STORAGE to understand all shards that `self` expects to
    /// exist.
    ///
    /// Note that this must be done before creating/rendering collections
    /// because the storage controller might not be aware of new system
    /// collections created between versions.
    async fn initialize_storage_state(
        &mut self,
        storage_collections: &Arc<
            dyn StorageCollections<Timestamp = mz_repr::Timestamp> + Send + Sync,
        >,
    ) -> Result<(), mz_catalog::durable::CatalogError> {
        let collections = self
            .entries()
            .filter(|entry| entry.item().is_storage_collection())
            .flat_map(|entry| entry.global_ids())
            .collect();

        // Clone the state so that any errors that occur do not leak any
        // transformations on error.
        let mut state = self.state.clone();

        let mut storage = self.storage().await;
        let mut txn = storage.transaction().await?;

        storage_collections
            .initialize_state(&mut txn, collections)
            .await
            .map_err(mz_catalog::durable::DurableCatalogError::from)?;

        let updates = txn.get_and_commit_op_updates();
        let builtin_updates = state.apply_updates(updates)?;
        assert!(builtin_updates.is_empty());
        let commit_ts = txn.upper();
        txn.commit(commit_ts).await?;
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
    ) -> Result<mz_controller::Controller<mz_repr::Timestamp>, mz_catalog::durable::CatalogError>
    {
        let controller_start = Instant::now();
        info!("startup: controller init: beginning");

        let controller = {
            let mut storage = self.storage().await;
            let mut tx = storage.transaction().await?;
            mz_controller::prepare_initialization(&mut tx)
                .map_err(mz_catalog::durable::DurableCatalogError::from)?;
            let updates = tx.get_and_commit_op_updates();
            assert!(
                updates.is_empty(),
                "initializing controller should not produce updates: {updates:?}"
            );
            let commit_ts = tx.upper();
            tx.commit(commit_ts).await?;

            let read_only_tx = storage.transaction().await?;

            mz_controller::Controller::new(config, envd_epoch, read_only, &read_only_tx).await
        };

        self.initialize_storage_state(&controller.storage_collections)
            .await?;

        info!(
            "startup: controller init: complete in {:?}",
            controller_start.elapsed()
        );

        Ok(controller)
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
    builtins_cfg: &BuiltinsConfig,
    txn: &mut mz_catalog::durable::Transaction<'_>,
) -> Result<(Vec<CatalogItemId>, Vec<GlobalId>), mz_catalog::durable::CatalogError> {
    let mut new_builtin_mappings = Vec::new();
    let mut migrated_builtin_ids = Vec::new();
    // Used to validate unique descriptions.
    let mut builtin_descs = HashSet::new();

    // We compare the builtin items that are compiled into the binary with the builtin items that
    // are persisted in the catalog to discover new, deleted, and migrated builtin items.
    let mut builtins = Vec::new();
    for builtin in BUILTINS::iter(builtins_cfg) {
        let desc = SystemObjectDescription {
            schema_name: builtin.schema().to_string(),
            object_type: builtin.catalog_item_type(),
            object_name: builtin.name().to_string(),
        };
        // Validate that the description is unique.
        if !builtin_descs.insert(desc.clone()) {
            panic!(
                "duplicate builtin description: {:?}, {:?}",
                SystemObjectDescription {
                    schema_name: builtin.schema().to_string(),
                    object_type: builtin.catalog_item_type(),
                    object_name: builtin.name().to_string(),
                },
                builtin
            );
        }
        builtins.push((desc, builtin));
    }

    let mut system_object_mappings: BTreeMap<_, _> = txn
        .get_system_object_mappings()
        .map(|system_object_mapping| {
            (
                system_object_mapping.description.clone(),
                system_object_mapping,
            )
        })
        .collect();

    let (existing_builtins, new_builtins): (Vec<_>, Vec<_>) =
        builtins.into_iter().partition_map(|(desc, builtin)| {
            let fingerprint = match builtin.runtime_alterable() {
                false => builtin.fingerprint(),
                true => RUNTIME_ALTERABLE_FINGERPRINT_SENTINEL.into(),
            };
            match system_object_mappings.remove(&desc) {
                Some(system_object_mapping) => {
                    Either::Left((builtin, system_object_mapping, fingerprint))
                }
                None => Either::Right((builtin, fingerprint)),
            }
        });
    let new_builtin_ids = txn.allocate_system_item_ids(usize_to_u64(new_builtins.len()))?;
    let new_builtins: Vec<_> = new_builtins
        .into_iter()
        .zip_eq(new_builtin_ids.clone())
        .collect();

    // Look for migrated builtins.
    for (builtin, system_object_mapping, fingerprint) in existing_builtins.iter().cloned() {
        if system_object_mapping.unique_identifier.fingerprint != fingerprint {
            // `mz_storage_usage_by_shard` cannot be migrated for multiple reasons. Firstly,
            // it was cause the table to be truncated because the contents are not also
            // stored in the durable catalog. Secondly, we prune `mz_storage_usage_by_shard`
            // of old events in the background on startup. The correctness of that pruning
            // relies on there being no other retractions to `mz_storage_usage_by_shard`.
            assert_ne!(
                *MZ_STORAGE_USAGE_BY_SHARD_DESCRIPTION, system_object_mapping.description,
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
            migrated_builtin_ids.push(system_object_mapping.unique_identifier.catalog_id);
        }
    }

    // Add new builtin items to catalog.
    for ((builtin, fingerprint), (catalog_id, global_id)) in new_builtins.iter().cloned() {
        new_builtin_mappings.push(SystemObjectMapping {
            description: SystemObjectDescription {
                schema_name: builtin.schema().to_string(),
                object_type: builtin.catalog_item_type(),
                object_name: builtin.name().to_string(),
            },
            unique_identifier: SystemObjectUniqueIdentifier {
                catalog_id,
                global_id,
                fingerprint,
            },
        });

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
                // Builtin Connections cannot be versioned.
                let versions = BTreeMap::new();

                txn.insert_item(
                    catalog_id,
                    c.oid,
                    global_id,
                    mz_catalog::durable::initialize::resolve_system_schema(c.schema).id,
                    c.name,
                    c.sql.into(),
                    *c.owner_id,
                    acl_items,
                    versions,
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
    txn.set_system_object_mappings(new_builtin_mappings)?;

    // Update comments of all builtin objects
    let builtins_with_catalog_ids = existing_builtins
        .iter()
        .map(|(b, m, _)| (*b, m.unique_identifier.catalog_id))
        .chain(
            new_builtins
                .into_iter()
                .map(|((b, _), (catalog_id, _))| (b, catalog_id)),
        );

    for (builtin, id) in builtins_with_catalog_ids {
        let (comment_id, desc, comments) = match builtin {
            Builtin::Source(s) => (CommentObjectId::Source(id), &s.desc, &s.column_comments),
            Builtin::View(v) => (CommentObjectId::View(id), &v.desc, &v.column_comments),
            Builtin::Table(t) => (CommentObjectId::Table(id), &t.desc, &t.column_comments),
            Builtin::Log(_)
            | Builtin::Type(_)
            | Builtin::Func(_)
            | Builtin::ContinualTask(_)
            | Builtin::Index(_)
            | Builtin::Connection(_) => continue,
        };
        txn.drop_comments(&BTreeSet::from_iter([comment_id]))?;

        let mut comments = comments.clone();
        for (col_idx, name) in desc.iter_names().enumerate() {
            if let Some(comment) = comments.remove(name.as_str()) {
                // Comment column indices are 1 based
                txn.update_comment(comment_id, Some(col_idx + 1), Some(comment.to_owned()))?;
            }
        }
        assert!(
            comments.is_empty(),
            "builtin object contains dangling comments that don't correspond to columns {comments:?}"
        );
    }

    // Anything left in `system_object_mappings` must have been deleted and should be removed from
    // the catalog.
    let mut deleted_system_objects = BTreeSet::new();
    let mut deleted_runtime_alterable_system_ids = BTreeSet::new();
    let mut deleted_comments = BTreeSet::new();
    for (desc, mapping) in system_object_mappings {
        deleted_system_objects.insert(mapping.description);
        if mapping.unique_identifier.fingerprint == RUNTIME_ALTERABLE_FINGERPRINT_SENTINEL {
            deleted_runtime_alterable_system_ids.insert(mapping.unique_identifier.catalog_id);
        }

        let id = mapping.unique_identifier.catalog_id;
        let comment_id = match desc.object_type {
            CatalogItemType::Table => CommentObjectId::Table(id),
            CatalogItemType::Source => CommentObjectId::Source(id),
            CatalogItemType::View => CommentObjectId::View(id),
            CatalogItemType::Sink
            | CatalogItemType::MaterializedView
            | CatalogItemType::Index
            | CatalogItemType::Type
            | CatalogItemType::Func
            | CatalogItemType::Secret
            | CatalogItemType::Connection
            | CatalogItemType::ContinualTask => continue,
        };
        deleted_comments.insert(comment_id);
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
    txn.drop_comments(&deleted_comments)?;
    txn.remove_items(&deleted_runtime_alterable_system_ids)?;
    txn.remove_system_object_mappings(deleted_system_objects)?;

    // Filter down to just the GlobalIds which are used to track the underlying collections.
    let new_builtin_collections = new_builtin_ids
        .into_iter()
        .map(|(_catalog_id, global_id)| global_id)
        .collect();

    Ok((migrated_builtin_ids, new_builtin_collections))
}

fn add_new_remove_old_builtin_clusters_migration(
    txn: &mut mz_catalog::durable::Transaction<'_>,
    builtin_cluster_config_map: &BuiltinBootstrapClusterConfigMap,
) -> Result<(), mz_catalog::durable::CatalogError> {
    let mut durable_clusters: BTreeMap<_, _> = txn
        .get_clusters()
        .filter(|cluster| cluster.id.is_system())
        .map(|cluster| (cluster.name.to_string(), cluster))
        .collect();

    // Add new clusters.
    for builtin_cluster in BUILTIN_CLUSTERS {
        if durable_clusters.remove(builtin_cluster.name).is_none() {
            let cluster_config = builtin_cluster_config_map.get_config(builtin_cluster.name)?;

            txn.insert_system_cluster(
                builtin_cluster.name,
                vec![],
                builtin_cluster.privileges.to_vec(),
                builtin_cluster.owner_id.to_owned(),
                mz_catalog::durable::ClusterConfig {
                    variant: mz_catalog::durable::ClusterVariant::Managed(ClusterVariantManaged {
                        size: cluster_config.size,
                        availability_zones: vec![],
                        replication_factor: cluster_config.replication_factor,
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

    // Remove old clusters.
    let old_clusters = durable_clusters
        .values()
        .map(|cluster| cluster.id)
        .collect();
    txn.remove_clusters(&old_clusters)?;

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

        for log in new_logs {
            let (item_id, gid) =
                Transaction::allocate_introspection_source_index_id(&cluster.id, log.variant);
            new_indexes.push((cluster.id, log.name.to_string(), item_id, gid));
        }

        // Anything left in `introspection_source_index_ids` must have been deleted and should be
        // removed from the catalog.
        removed_indexes.extend(
            introspection_source_index_ids
                .into_keys()
                .map(|name| (cluster.id, name.to_string())),
        );
    }
    txn.insert_introspection_source_indexes(new_indexes, &HashSet::new())?;
    txn.remove_introspection_source_indexes(removed_indexes)?;
    Ok(())
}

fn add_new_remove_old_builtin_roles_migration(
    txn: &mut mz_catalog::durable::Transaction<'_>,
) -> Result<(), mz_catalog::durable::CatalogError> {
    let mut durable_roles: BTreeMap<_, _> = txn
        .get_roles()
        .filter(|role| role.id.is_system() || role.id.is_predefined())
        .map(|role| (role.name.to_string(), role))
        .collect();

    // Add new roles.
    for builtin_role in BUILTIN_ROLES {
        if durable_roles.remove(builtin_role.name).is_none() {
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

    // Remove old roles.
    let old_roles = durable_roles.values().map(|role| role.id).collect();
    txn.remove_roles(&old_roles)?;

    Ok(())
}

fn add_new_remove_old_builtin_cluster_replicas_migration(
    txn: &mut Transaction<'_>,
    builtin_cluster_config_map: &BuiltinBootstrapClusterConfigMap,
) -> Result<(), AdapterError> {
    let cluster_lookup: BTreeMap<_, _> = txn
        .get_clusters()
        .map(|cluster| (cluster.name.clone(), cluster.clone()))
        .collect();

    let mut durable_replicas: BTreeMap<ClusterId, BTreeMap<String, ClusterReplica>> = txn
        .get_cluster_replicas()
        .filter(|replica| replica.replica_id.is_system())
        .fold(BTreeMap::new(), |mut acc, replica| {
            acc.entry(replica.cluster_id)
                .or_insert_with(BTreeMap::new)
                .insert(replica.name.to_string(), replica);
            acc
        });

    // Add new replicas.
    for builtin_replica in BUILTIN_CLUSTER_REPLICAS {
        let cluster = cluster_lookup
            .get(builtin_replica.cluster_name)
            .expect("builtin cluster replica references non-existent cluster");
        // `empty_map` is a hack to simplify the if statement below.
        let mut empty_map: BTreeMap<String, ClusterReplica> = BTreeMap::new();
        let replica_names = durable_replicas
            .get_mut(&cluster.id)
            .unwrap_or(&mut empty_map);

        let builtin_cluster_boostrap_config =
            builtin_cluster_config_map.get_config(builtin_replica.cluster_name)?;
        if replica_names.remove(builtin_replica.name).is_none()
            // NOTE(SangJunBak): We need to explicitly check the replication factor because
            // BUILT_IN_CLUSTER_REPLICAS is constant throughout all deployments but the replication
            // factor is configurable on bootstrap.
            && builtin_cluster_boostrap_config.replication_factor > 0
        {
            let replica_size = match cluster.config.variant {
                ClusterVariant::Managed(ClusterVariantManaged { ref size, .. }) => size.clone(),
                ClusterVariant::Unmanaged => builtin_cluster_boostrap_config.size,
            };

            let config = builtin_cluster_replica_config(replica_size);
            txn.insert_cluster_replica(
                cluster.id,
                builtin_replica.name,
                config,
                MZ_SYSTEM_ROLE_ID,
            )?;
        }
    }

    // Remove old replicas.
    let old_replicas = durable_replicas
        .values()
        .flat_map(|replicas| replicas.values().map(|replica| replica.replica_id))
        .collect();
    txn.remove_cluster_replicas(&old_replicas)?;

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
            let session_vars = SessionVars::new_unchecked(&BUILD_INFO, SYSTEM_USER.clone(), None);

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

    txn.update_roles_without_auth(roles_to_migrate)?;

    Ok(())
}

/// Cluster Replicas may be created ephemerally during an alter statement, these replicas
/// are marked as pending and should be cleaned up on catalog opsn.
fn remove_pending_cluster_replicas_migration(tx: &mut Transaction) -> Result<(), anyhow::Error> {
    for replica in tx.get_cluster_replicas().collect::<Vec<_>>() {
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

#[derive(Debug)]
pub struct BuiltinBootstrapClusterConfigMap {
    /// Size and replication factor to default system_cluster on bootstrap
    pub system_cluster: BootstrapBuiltinClusterConfig,
    /// Size and replication factor to default catalog_server_cluster on bootstrap
    pub catalog_server_cluster: BootstrapBuiltinClusterConfig,
    /// Size and replication factor to default probe_cluster on bootstrap
    pub probe_cluster: BootstrapBuiltinClusterConfig,
    /// Size and replication factor to default support_cluster on bootstrap
    pub support_cluster: BootstrapBuiltinClusterConfig,
    /// Size to default analytics_cluster on bootstrap
    pub analytics_cluster: BootstrapBuiltinClusterConfig,
}

impl BuiltinBootstrapClusterConfigMap {
    /// Gets the size of the builtin cluster based on the provided name
    fn get_config(
        &self,
        cluster_name: &str,
    ) -> Result<BootstrapBuiltinClusterConfig, mz_catalog::durable::CatalogError> {
        let cluster_config = if cluster_name == mz_catalog::builtin::MZ_SYSTEM_CLUSTER.name {
            &self.system_cluster
        } else if cluster_name == mz_catalog::builtin::MZ_CATALOG_SERVER_CLUSTER.name {
            &self.catalog_server_cluster
        } else if cluster_name == mz_catalog::builtin::MZ_PROBE_CLUSTER.name {
            &self.probe_cluster
        } else if cluster_name == mz_catalog::builtin::MZ_SUPPORT_CLUSTER.name {
            &self.support_cluster
        } else if cluster_name == mz_catalog::builtin::MZ_ANALYTICS_CLUSTER.name {
            &self.analytics_cluster
        } else {
            return Err(mz_catalog::durable::CatalogError::Catalog(
                SqlCatalogError::UnexpectedBuiltinCluster(cluster_name.to_owned()),
            ));
        };
        Ok(cluster_config.clone())
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

fn get_dyncfg_val_from_defaults_and_remote<T: mz_dyncfg::ConfigDefault>(
    defaults: &BTreeMap<String, String>,
    remote: Option<&BTreeMap<String, String>>,
    cfg: &mz_dyncfg::Config<T>,
) -> T::ConfigType {
    let mut val = T::into_config_type(cfg.default().clone());
    let get_fn = |map: &BTreeMap<String, String>| {
        let val = map.get(cfg.name())?;
        match <T::ConfigType as mz_dyncfg::ConfigType>::parse(val) {
            Ok(x) => Some(x),
            Err(err) => {
                tracing::warn!("could not parse {} value [{}]: {}", cfg.name(), val, err);
                None
            }
        }
    };
    if let Some(x) = get_fn(defaults) {
        val = x;
    }
    if let Some(x) = remote.and_then(get_fn) {
        val = x;
    }
    val
}
