// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic related to opening a [`Catalog`].

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::{BoxFuture, FutureExt};
use mz_adapter_types::compaction::CompactionWindow;
use mz_catalog::builtin::{
    Builtin, Fingerprint, BUILTINS, BUILTIN_CLUSTERS, BUILTIN_CLUSTER_REPLICAS, BUILTIN_PREFIXES,
    BUILTIN_ROLES,
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
    CatalogEntry, CatalogItem, CommentsMap, DataSourceDesc, DefaultPrivileges, Func, Log, Source,
    StateUpdate, StateUpdateKind, Table, Type,
};
use mz_catalog::SYSTEM_CONN_ID;
use mz_cluster_client::ReplicaId;
use mz_compute_client::logging::LogVariant;
use mz_controller::clusters::ReplicaLogging;
use mz_controller_types::{is_cluster_size_v2, ClusterId};
use mz_ore::cast::{usize_to_u64, CastFrom};
use mz_ore::collections::{CollectionExt, HashSet};
use mz_ore::instrument;
use mz_ore::now::to_datetime;
use mz_pgrepr::oid::INVALID_OID;
use mz_repr::adt::mz_acl_item::PrivilegeMap;
use mz_repr::namespaces::MZ_INTERNAL_SCHEMA;
use mz_repr::role_id::RoleId;
use mz_repr::GlobalId;
use mz_sql::catalog::{
    CatalogError as SqlCatalogError, CatalogItem as SqlCatalogItem, CatalogItemType, CatalogType,
    NameReference, RoleMembership, RoleVars,
};
use mz_sql::func::OP_IMPLS;
use mz_sql::names::{
    ItemQualifiers, QualifiedItemName, ResolvedDatabaseSpecifier, ResolvedIds, SchemaId,
    SchemaSpecifier,
};
use mz_sql::rbac;
use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;
use mz_sql::session::vars::{SystemVars, VarError, VarInput};
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::Expr;
use mz_ssh_util::keys::SshKeyPairSet;
use mz_storage_types::sources::Timeline;
use timely::Container;
use tracing::{info, warn, Instrument};
use uuid::Uuid;

// DO NOT add any more imports from `crate` outside of `crate::catalog`.
use crate::catalog::{
    is_reserved_name, migrate, BuiltinTableUpdate, Catalog, CatalogPlans, CatalogState, Config,
};
use crate::AdapterError;

#[derive(Debug)]
pub struct BuiltinMigrationMetadata {
    // Used to drop objects on STORAGE nodes
    pub previous_storage_collection_ids: BTreeSet<GlobalId>,
    // Used to update in memory catalog state
    pub all_drop_ops: Vec<GlobalId>,
    pub all_create_ops: Vec<(
        GlobalId,
        u32,
        QualifiedItemName,
        RoleId,
        PrivilegeMap,
        CatalogItemRebuilder,
    )>,
    pub introspection_source_index_updates:
        BTreeMap<ClusterId, Vec<(LogVariant, String, GlobalId, u32)>>,
    // Used to update persisted on disk catalog state
    pub migrated_system_object_mappings: BTreeMap<GlobalId, SystemObjectMapping>,
    pub user_drop_ops: Vec<GlobalId>,
    pub user_create_ops: Vec<(GlobalId, SchemaId, u32, String)>,
}

impl BuiltinMigrationMetadata {
    fn new() -> BuiltinMigrationMetadata {
        BuiltinMigrationMetadata {
            previous_storage_collection_ids: BTreeSet::new(),
            all_drop_ops: Vec::new(),
            all_create_ops: Vec::new(),
            introspection_source_index_updates: BTreeMap::new(),
            migrated_system_object_mappings: BTreeMap::new(),
            user_drop_ops: Vec::new(),
            user_create_ops: Vec::new(),
        }
    }
}

struct BootstrapSystemIds {
    // Invariant: Sorted by dependency order.
    all_builtins: Vec<(&'static Builtin<NameReference>, GlobalId)>,
    migrated_builtins: Vec<GlobalId>,
}

#[derive(Debug)]
pub enum CatalogItemRebuilder {
    SystemSource(CatalogItem),
    Object {
        id: GlobalId,
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
                id,
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
                id,
                sql,
                is_retained_metrics_object,
                custom_logical_compaction_window,
            } => state
                .parse_item(
                    id,
                    &sql,
                    None,
                    is_retained_metrics_object,
                    custom_logical_compaction_window,
                )
                .unwrap_or_else(|error| panic!("invalid persisted create sql ({error:?}): {sql}")),
        }
    }
}

impl Catalog {
    /// Initializes a CatalogState. Separate from [`Catalog::open`] to avoid depending on state
    /// external to a [mz_catalog::durable::DurableCatalogState] (for example: no [mz_secrets::SecretsReader]).
    ///
    /// The passed in `previous_ts` must be the highest read timestamp for
    /// [Timeline::EpochMilliseconds] known across all timestamp oracles.
    ///
    /// BOXED FUTURE: As of Nov 2023 the returned Future from this function was 7.5KB. This would
    /// get stored on the stack which is bad for runtime performance, and blow up our stack usage.
    /// Because of that we purposefully move this Future onto the heap (i.e. Box it).
    pub fn initialize_state<'a>(
        config: StateConfig,
        storage: &'a mut Box<dyn mz_catalog::durable::DurableCatalogState>,
    ) -> BoxFuture<'a, Result<(CatalogState, BuiltinMigrationMetadata, String), AdapterError>> {
        async move {
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
                temporary_schemas: BTreeMap::new(),
                clusters_by_id: BTreeMap::new(),
                clusters_by_name: BTreeMap::new(),
                roles_by_name: BTreeMap::new(),
                roles_by_id: BTreeMap::new(),
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
                system_configuration: {
                    let mut s = SystemVars::new(config.active_connection_count)
                        .set_unsafe(config.unsafe_mode);
                    if config.all_features {
                        s.enable_all_feature_flags_by_default();
                    }
                    s
                },
                egress_ips: config.egress_ips,
                aws_principal_context: config.aws_principal_context,
                aws_privatelink_availability_zones: config.aws_privatelink_availability_zones,
                http_host_name: config.http_host_name,
                default_privileges: DefaultPrivileges::default(),
                system_privileges: PrivilegeMap::default(),
                comments: CommentsMap::default(),
                storage_metadata: Default::default(),
            };

            let is_read_only = storage.is_read_only();
            let mut txn = storage.transaction().await?;

            // Migrate/update durable data before we start loading the in-memory catalog.
            let builtin_item_ids = {
                migrate::durable_migrate(&mut txn, config.boot_ts)?;
                // Overwrite and persist selected parameter values in `remote_system_parameters` that
                // was pulled from a remote frontend (e.g. LaunchDarkly) if present.
                if let Some(remote_system_parameters) = config.remote_system_parameters {
                    for (name, value) in remote_system_parameters {
                        txn.upsert_system_config(&name, value)?;
                    }
                    txn.set_system_config_synced_once()?;
                    // This mirrors the `persist_txn_tables` "system var" into the catalog
                    // storage "config" collection so that we can toggle the flag with
                    // Launch Darkly, but use it in boot before Launch Darkly is available.
                    txn.set_persist_txn_tables(state.system_config().persist_txn_tables())?;
                }
                // Add any new builtin objects and remove old ones.
                let builtin_item_ids = add_new_remove_old_builtin_items_migration(&mut txn)?;
                if !is_read_only {
                    let cluster_sizes = BuiltinBootstrapClusterSizes {
                        system_cluster: config.builtin_system_cluster_replica_size,
                        introspection_cluster: config.builtin_introspection_cluster_replica_size,
                        probe_cluster: config.builtin_probe_cluster_replica_size,
                        support_cluster: config.builtin_support_cluster_replica_size,
                    };
                    add_new_builtin_clusters_migration(&mut txn, &cluster_sizes)?;
                    add_new_builtin_introspection_source_migration(&mut txn)?;
                    add_new_builtin_cluster_replicas_migration(&mut txn, &cluster_sizes)?;
                    add_new_builtin_roles_migration(&mut txn)?;
                }
                builtin_item_ids
            };

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

            // Durable catalog updates must be partitioned so that we can weave the loading of
            // builtin objects into the right spots.
            let mut pre_cluster_updates = Vec::new();
            let mut cluster_updates = Vec::new();
            let mut item_updates = Vec::new();
            let mut comment_updates = Vec::new();
            for update in txn.get_updates() {
                match &update.kind {
                    StateUpdateKind::Role(_)
                    | StateUpdateKind::Database(_)
                    | StateUpdateKind::Schema(_)
                    | StateUpdateKind::DefaultPrivilege(_)
                    | StateUpdateKind::SystemPrivilege(_)
                    | StateUpdateKind::SystemConfiguration(_) => pre_cluster_updates.push(update),
                    StateUpdateKind::Cluster(_)
                    | StateUpdateKind::IntrospectionSourceIndex(_)
                    | StateUpdateKind::ClusterReplica(_) => cluster_updates.push(update),
                    StateUpdateKind::Item(_) => item_updates.push(update),
                    StateUpdateKind::Comment(_) => comment_updates.push(update),
                }
            }

            state.apply_updates_for_bootstrap(pre_cluster_updates)?;

            // Load all builtin types.
            let (builtin_types, builtin_non_types): (Vec<_>, Vec<_>) = builtin_item_ids
                .all_builtins
                .into_iter()
                .partition(|(builtin, _)| matches!(builtin, Builtin::Type(_)));
            let type_id_map = builtin_types
                .into_iter()
                .map(|(builtin, id)| (builtin.name(), id))
                .collect();
            Catalog::load_builtin_types(&mut state, &type_id_map)?;

            let id_fingerprint_map: BTreeMap<GlobalId, String> = builtin_non_types
                .iter()
                .map(|(builtin, id)| (*id, builtin.fingerprint()))
                .collect();
            let (builtin_indexes, builtin_non_indexes): (Vec<_>, Vec<_>) = builtin_non_types
                .into_iter()
                .partition(|(builtin, _)| matches!(builtin, Builtin::Index(_)));

            // Load all builtins except types and indexes.
            {
                let span = tracing::span!(tracing::Level::DEBUG, "builtin_non_indexes");
                let _enter = span.enter();
                for (builtin, id) in builtin_non_indexes {
                    let schema_id = state.ambient_schemas_by_name[builtin.schema()];
                    let name = QualifiedItemName {
                        qualifiers: ItemQualifiers {
                            database_spec: ResolvedDatabaseSpecifier::Ambient,
                            schema_spec: SchemaSpecifier::Id(schema_id),
                        },
                        item: builtin.name().into(),
                    };
                    match builtin {
                        Builtin::Log(log) => {
                            let mut acl_items = vec![rbac::owner_privilege(
                                mz_sql::catalog::ObjectType::Source,
                                MZ_SYSTEM_ROLE_ID,
                            )];
                            acl_items.extend_from_slice(&log.access);
                            state.insert_item(
                                id,
                                log.oid,
                                name.clone(),
                                CatalogItem::Log(Log {
                                    variant: log.variant.clone(),
                                    has_storage_collection: false,
                                }),
                                MZ_SYSTEM_ROLE_ID,
                                PrivilegeMap::from_mz_acl_items(acl_items),
                            );
                        }

                        Builtin::Table(table) => {
                            let mut acl_items = vec![rbac::owner_privilege(
                                mz_sql::catalog::ObjectType::Table,
                                MZ_SYSTEM_ROLE_ID,
                            )];
                            acl_items.extend_from_slice(&table.access);

                            state.insert_item(
                                id,
                                table.oid,
                                name.clone(),
                                CatalogItem::Table(Table {
                                    create_sql: None,
                                    desc: table.desc.clone(),
                                    defaults: vec![Expr::null(); table.desc.arity()],
                                    conn_id: None,
                                    resolved_ids: ResolvedIds(BTreeSet::new()),
                                    custom_logical_compaction_window: table
                                        .is_retained_metrics_object
                                        .then(|| state.system_config().metrics_retention().try_into().expect("invalid metrics retention")),
                                    is_retained_metrics_object: table.is_retained_metrics_object,
                                }),
                                MZ_SYSTEM_ROLE_ID,
                                PrivilegeMap::from_mz_acl_items(acl_items),
                            );
                        }
                        Builtin::Index(_) => {
                            unreachable!("handled later once clusters have been created")
                        }
                        Builtin::View(view) => {
                            let item = state
                                .parse_item(
                                    id,
                                    &view.create_sql(),
                                    None,
                                    false,
                                    None,
                                )
                                .unwrap_or_else(|e| {
                                    panic!(
                                        "internal error: failed to load bootstrap view:\n\
                                        {}\n\
                                        error:\n\
                                        {:?}\n\n\
                                        make sure that the schema name is specified in the builtin view's create sql statement.",
                                        view.name, e
                                    )
                                });
                            let mut acl_items = vec![rbac::owner_privilege(
                                mz_sql::catalog::ObjectType::View,
                                MZ_SYSTEM_ROLE_ID,
                            )];
                            acl_items.extend_from_slice(&view.access);

                            state.insert_item(
                                id,
                                view.oid,
                                name,
                                item,
                                MZ_SYSTEM_ROLE_ID,
                                PrivilegeMap::from_mz_acl_items(acl_items),
                            );
                        }

                        Builtin::Type(_) => unreachable!("loaded separately"),

                        Builtin::Func(func) => {
                            // This OID is never used. `func` has a `Vec` of implementations and
                            // each implementation has its own OID. Those are the OIDs that are
                            // actually used by the system.
                            let oid = INVALID_OID;
                            state.insert_item(
                                id,
                                oid,
                                name.clone(),
                                CatalogItem::Func(Func { inner: func.inner }),
                                MZ_SYSTEM_ROLE_ID,
                                PrivilegeMap::default(),
                            );
                        }

                        Builtin::Source(coll) => {
                            let mut acl_items = vec![rbac::owner_privilege(
                                mz_sql::catalog::ObjectType::Source,
                                MZ_SYSTEM_ROLE_ID,
                            )];
                            acl_items.extend_from_slice(&coll.access);

                            state.insert_item(
                                id,
                                coll.oid,
                                name.clone(),
                                CatalogItem::Source(Source {
                                    create_sql: None,
                                    data_source: DataSourceDesc::Introspection(coll.data_source),
                                    desc: coll.desc.clone(),
                                    timeline: Timeline::EpochMilliseconds,
                                    resolved_ids: ResolvedIds(BTreeSet::new()),
                                    custom_logical_compaction_window: coll
                                        .is_retained_metrics_object
                                        .then(|| state.system_config().metrics_retention().try_into().expect("invalid metrics retention")),
                                    is_retained_metrics_object: coll.is_retained_metrics_object,
                                }),
                                MZ_SYSTEM_ROLE_ID,
                                PrivilegeMap::from_mz_acl_items(acl_items),
                            );
                        }
                    }
                }
            }

            state.apply_updates_for_bootstrap(cluster_updates)?;

            // Load all builtin indexes.
            for (builtin, id) in builtin_indexes {
                let schema_id = state.ambient_schemas_by_name[builtin.schema()];
                let name = QualifiedItemName {
                    qualifiers: ItemQualifiers {
                        database_spec: ResolvedDatabaseSpecifier::Ambient,
                        schema_spec: SchemaSpecifier::Id(schema_id),
                    },
                    item: builtin.name().into(),
                };
                match builtin {
                    Builtin::Index(index) => {
                        let mut item = state
                            .parse_item(
                                id,
                                &index.create_sql(),
                                None,
                                index.is_retained_metrics_object,
                                if index.is_retained_metrics_object { Some(state.system_config().metrics_retention().try_into().expect("invalid metrics retention")) } else { None },
                            )
                            .unwrap_or_else(|e| {
                                panic!(
                                    "internal error: failed to load bootstrap index:\n\
                                    {}\n\
                                    error:\n\
                                    {:?}\n\n\
                                    make sure that the schema name is specified in the builtin index's create sql statement.",
                                    index.name, e
                                )
                            });
                        let CatalogItem::Index(_) = &mut item else {
                            panic!("internal error: builtin index {}'s SQL does not begin with \"CREATE INDEX\".", index.name);
                        };

                        state.insert_item(
                            id,
                            index.oid,
                            name,
                            item,
                            MZ_SYSTEM_ROLE_ID,
                            PrivilegeMap::default(),
                        );
                    }
                    Builtin::Log(_)
                    | Builtin::Table(_)
                    | Builtin::View(_)
                    | Builtin::Type(_)
                    | Builtin::Func(_)
                    | Builtin::Source(_) => {
                        unreachable!("handled above")
                    }
                }
            }

            let last_seen_version = txn
                .get_catalog_content_version()
                .unwrap_or_else(|| "new".to_string());

            // Migrate item ASTs.
            if !config.skip_migrations {
                migrate::migrate(&state, &mut txn, config.now, config.boot_ts, &state.config.connection_context)
                    .await
                    .map_err(|e| {
                        Error::new(ErrorKind::FailedMigration {
                            last_seen_version: last_seen_version.clone(),
                            this_version: config.build_info.version,
                            cause: e.to_string(),
                        })
                    })?;
                txn.set_catalog_content_version(config.build_info.version.to_string())?;
                // Throw the existing item updates away because they may have been re-written in
                // the migration.
                let item_updates = txn.get_items().map(|item| StateUpdate{kind: StateUpdateKind::Item(item), diff: 1}).collect();
                state.apply_updates_for_bootstrap(item_updates)?;
            } else {
                state.apply_updates_for_bootstrap(item_updates)?;
            }

            state.apply_updates_for_bootstrap(comment_updates)?;

            // Migrate builtin items.
            let mut builtin_migration_metadata = Catalog::generate_builtin_migration_metadata(
                &state,
                &mut txn,
                builtin_item_ids.migrated_builtins,
                id_fingerprint_map,
            )?;
            Catalog::apply_in_memory_builtin_migration(
                &mut state,
                &mut builtin_migration_metadata,
            )?;
            Catalog::apply_persisted_builtin_migration(
                &state,
                &mut txn,
                &mut builtin_migration_metadata,
            )?;

            state.update_storage_metadata(&txn);

            txn.commit().await?;
            Ok((
                state,
                builtin_migration_metadata,
                last_seen_version,
            ))
        }
            .instrument(tracing::info_span!("catalog::initialize_state"))
            .boxed()
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
    pub fn open(
        config: Config<'_>,
        boot_ts: mz_repr::Timestamp,
    ) -> BoxFuture<
        'static,
        Result<
            (
                Catalog,
                BuiltinMigrationMetadata,
                Vec<BuiltinTableUpdate>,
                String,
            ),
            AdapterError,
        >,
    > {
        async move {
            let mut storage = config.storage;
            let (state, builtin_migration_metadata, last_seen_version) =
                Self::initialize_state(config.state, &mut storage).await?;

            let mut catalog = Catalog {
                state,
                plans: CatalogPlans::default(),
                transient_revision: 1,
                storage: Arc::new(tokio::sync::Mutex::new(storage)),
            };
            let secrets_reader = &catalog.state.config.connection_context.secrets_reader;

            // Load public keys for SSH connections from the secrets store to the catalog
            for (id, entry) in catalog.state.entry_by_id.iter_mut() {
                if let CatalogItem::Connection(ref mut connection) = entry.item {
                    if let mz_storage_types::connections::Connection::Ssh(ref mut ssh) =
                        connection.connection
                    {
                        let secret = secrets_reader.read(*id).await?;
                        let keyset = SshKeyPairSet::from_bytes(&secret)?;
                        let public_key_pair = keyset.public_keys();
                        ssh.public_keys = Some(public_key_pair);
                    }
                }
            }

            let mut builtin_table_updates = vec![];
            for (schema_id, schema) in &catalog.state.ambient_schemas_by_id {
                let db_spec = ResolvedDatabaseSpecifier::Ambient;
                builtin_table_updates
                    .push(catalog.state.pack_schema_update(&db_spec, schema_id, 1));
                for (_item_name, item_id) in &schema.items {
                    builtin_table_updates.extend(catalog.state.pack_item_update(*item_id, 1));
                }
                for (_item_name, function_id) in &schema.functions {
                    builtin_table_updates.extend(catalog.state.pack_item_update(*function_id, 1));
                }
                for (_item_name, type_id) in &schema.types {
                    builtin_table_updates.extend(catalog.state.pack_item_update(*type_id, 1));
                }
            }
            for (_id, db) in &catalog.state.database_by_id {
                builtin_table_updates.push(catalog.state.pack_database_update(db, 1));
                let db_spec = ResolvedDatabaseSpecifier::Id(db.id.clone());
                for (schema_id, schema) in &db.schemas_by_id {
                    builtin_table_updates
                        .push(catalog.state.pack_schema_update(&db_spec, schema_id, 1));
                    for (_item_name, item_id) in &schema.items {
                        builtin_table_updates.extend(catalog.state.pack_item_update(*item_id, 1));
                    }
                    for (_item_name, function_id) in &schema.functions {
                        builtin_table_updates
                            .extend(catalog.state.pack_item_update(*function_id, 1));
                    }
                    for (_item_name, type_id) in &schema.types {
                        builtin_table_updates.extend(catalog.state.pack_item_update(*type_id, 1));
                    }
                }
            }
            for (id, sub_component, comment) in catalog.state.comments.iter() {
                builtin_table_updates.push(catalog.state.pack_comment_update(
                    id,
                    sub_component,
                    comment,
                    1,
                ));
            }
            for (_id, role) in &catalog.state.roles_by_id {
                if let Some(builtin_update) = catalog.state.pack_role_update(role.id, 1) {
                    builtin_table_updates.push(builtin_update);
                }
                for group_id in role.membership.map.keys() {
                    builtin_table_updates.push(
                        catalog
                            .state
                            .pack_role_members_update(*group_id, role.id, 1),
                    )
                }
            }
            for (default_privilege_object, default_privilege_acl_items) in
                catalog.state.default_privileges.iter()
            {
                for default_privilege_acl_item in default_privilege_acl_items {
                    builtin_table_updates.push(catalog.state.pack_default_privileges_update(
                        default_privilege_object,
                        &default_privilege_acl_item.grantee,
                        &default_privilege_acl_item.acl_mode,
                        1,
                    ));
                }
            }
            for system_privilege in catalog.state.system_privileges.all_values_owned() {
                builtin_table_updates.push(
                    catalog
                        .state
                        .pack_system_privileges_update(system_privilege, 1),
                );
            }
            for (id, cluster) in &catalog.state.clusters_by_id {
                builtin_table_updates.push(catalog.state.pack_cluster_update(&cluster.name, 1));
                for (replica_name, replica_id) in
                    cluster.replicas().map(|r| (&r.name, r.replica_id))
                {
                    builtin_table_updates.extend(catalog.state.pack_cluster_replica_update(
                        *id,
                        replica_name,
                        1,
                    ));
                    let replica = catalog.state.get_cluster_replica(*id, replica_id);
                    for process_id in 0..replica.config.location.num_processes() {
                        let update = catalog.state.pack_cluster_replica_status_update(
                            *id,
                            replica_id,
                            u64::cast_from(process_id),
                            1,
                        );
                        builtin_table_updates.push(update);
                    }
                }
            }
            // Operators aren't stored in the catalog, but we would like them in
            // introspection views.
            for (op, func) in OP_IMPLS.iter() {
                match func {
                    mz_sql::func::Func::Scalar(impls) => {
                        for imp in impls {
                            builtin_table_updates.push(catalog.state.pack_op_update(
                                op,
                                imp.details(),
                                1,
                            ));
                        }
                    }
                    _ => unreachable!("all operators must be scalar functions"),
                }
            }
            let audit_logs = catalog.storage().await.get_audit_logs().await?;
            for event in audit_logs {
                builtin_table_updates.push(catalog.state.pack_audit_log_update(&event)?);
            }

            // To avoid reading over storage_usage events multiple times, do both
            // the table updates and delete calculations in a single read over the
            // data.
            let wait_for_consolidation = catalog
                .system_config()
                .wait_catalog_consolidation_on_startup();
            let storage_usage_events = catalog
                .storage()
                .await
                .get_and_prune_storage_usage(
                    config.storage_usage_retention_period,
                    boot_ts,
                    wait_for_consolidation,
                )
                .await?;
            for event in storage_usage_events {
                builtin_table_updates.push(catalog.state.pack_storage_usage_update(&event)?);
            }

            for ip in &catalog.state.egress_ips {
                builtin_table_updates.push(catalog.state.pack_egress_ip_update(ip)?);
            }

            Ok((
                catalog,
                builtin_migration_metadata,
                builtin_table_updates,
                last_seen_version,
            ))
        }
        .instrument(tracing::info_span!("catalog::open"))
        .boxed()
    }

    /// Loads built-in system types into the catalog.
    ///
    /// Built-in types sometimes have references to other built-in types, and sometimes these
    /// references are circular. This makes loading built-in types more complicated than other
    /// built-in objects, and requires us to make multiple passes over the types to correctly
    /// resolve all references.
    #[mz_ore::instrument]
    fn load_builtin_types(
        state: &mut CatalogState,
        type_id_map: &BTreeMap<&str, GlobalId>,
    ) -> Result<(), Error> {
        // Replace named references with id references
        let mut builtin_types: Vec<_> = BUILTINS::types()
            .map(|typ| Catalog::resolve_builtin_type(typ, type_id_map))
            .collect();

        // Resolve array_id for types
        let mut element_id_to_array_id = BTreeMap::new();
        for typ in &builtin_types {
            match &typ.details.typ {
                CatalogType::Array { element_reference } => {
                    let array_id = type_id_map[typ.name];
                    element_id_to_array_id.insert(*element_reference, array_id);
                }
                _ => {}
            }
        }
        for typ in &mut builtin_types {
            let element_id = type_id_map[typ.name];
            typ.details.array_id = element_id_to_array_id.get(&element_id).map(|id| id.clone());
        }

        // Insert into catalog
        for typ in builtin_types {
            let element_id = type_id_map[typ.name];

            // Assert that no built-in types are record types so that we don't
            // need to bother to build a description. Only record types need
            // descriptions.
            let desc = None;
            assert!(!matches!(typ.details.typ, CatalogType::Record { .. }));

            let schema_id = state.resolve_system_schema(typ.schema);

            state.insert_item(
                element_id,
                typ.oid,
                QualifiedItemName {
                    qualifiers: ItemQualifiers {
                        database_spec: ResolvedDatabaseSpecifier::Ambient,
                        schema_spec: SchemaSpecifier::Id(schema_id),
                    },
                    item: typ.name.to_owned(),
                },
                CatalogItem::Type(Type {
                    create_sql: None,
                    details: typ.details.clone(),
                    desc,
                    resolved_ids: ResolvedIds(BTreeSet::new()),
                }),
                MZ_SYSTEM_ROLE_ID,
                PrivilegeMap::from_mz_acl_items(vec![
                    rbac::default_builtin_object_privilege(mz_sql::catalog::ObjectType::Type),
                    rbac::owner_privilege(mz_sql::catalog::ObjectType::Type, MZ_SYSTEM_ROLE_ID),
                ]),
            );
        }

        Ok(())
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
                migration_metadata.user_drop_ops.push(id);
            }
            migration_metadata.all_drop_ops.push(id);

            // Push create commands.
            let name = entry.name().clone();
            if id.is_user() {
                let schema_id = name.qualifiers.schema_spec.clone().into();
                migration_metadata.user_create_ops.push((
                    new_id,
                    schema_id,
                    entry.oid(),
                    name.item.clone(),
                ));
            }
            let item_rebuilder = CatalogItemRebuilder::new(entry, new_id, &ancestor_ids);
            migration_metadata.all_create_ops.push((
                new_id,
                entry.oid(),
                name,
                entry.owner_id().clone(),
                entry.privileges().clone(),
                item_rebuilder,
            ));
        }

        // Reverse drop commands.
        migration_metadata.all_drop_ops.reverse();
        migration_metadata.user_drop_ops.reverse();

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

    fn apply_in_memory_builtin_migration(
        state: &mut CatalogState,
        migration_metadata: &mut BuiltinMigrationMetadata,
    ) -> Result<(), Error> {
        assert_eq!(
            migration_metadata.all_drop_ops.len(),
            migration_metadata.all_create_ops.len(),
            "we should be re-creating every dropped object"
        );
        for id in migration_metadata.all_drop_ops.drain(..) {
            state.drop_item(id);
        }
        for (id, oid, name, owner_id, privileges, item_rebuilder) in
            migration_metadata.all_create_ops.drain(..)
        {
            let item = item_rebuilder.build(state);
            state.insert_item(id, oid, name, item, owner_id, privileges);
        }
        for (cluster_id, updates) in &migration_metadata.introspection_source_index_updates {
            let log_indexes = &mut state
                .clusters_by_id
                .get_mut(cluster_id)
                .unwrap_or_else(|| panic!("invalid cluster {cluster_id}"))
                .log_indexes;
            for (variant, _name, new_id, _oid) in updates {
                log_indexes.remove(variant);
                log_indexes.insert(variant.clone(), new_id.clone());
            }
        }

        Ok(())
    }

    #[mz_ore::instrument]
    fn apply_persisted_builtin_migration(
        state: &CatalogState,
        txn: &mut Transaction<'_>,
        migration_metadata: &mut BuiltinMigrationMetadata,
    ) -> Result<(), Error> {
        txn.remove_items(migration_metadata.user_drop_ops.drain(..).collect())?;
        for (id, schema_id, oid, name) in migration_metadata.user_create_ops.drain(..) {
            let entry = state.get_entry(&id);
            let item = entry.item();
            let serialized_item = item.to_serialized();
            txn.insert_item(
                id,
                oid,
                schema_id,
                &name,
                serialized_item,
                entry.owner_id().clone(),
                entry.privileges().all_values_owned().collect(),
            )?;
        }
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

        Ok(())
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

fn add_new_remove_old_builtin_items_migration(
    txn: &mut mz_catalog::durable::Transaction<'_>,
) -> Result<BootstrapSystemIds, mz_catalog::durable::CatalogError> {
    let mut new_builtins = Vec::new();
    let mut all_builtins = Vec::new();
    let mut migrated_builtins = Vec::new();

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
        match system_object_mappings.remove(&desc) {
            Some(system_object_mapping) => {
                all_builtins.push((builtin, system_object_mapping.unique_identifier.id));
                if system_object_mapping.unique_identifier.fingerprint != builtin.fingerprint() {
                    assert_ne!(
                        builtin.catalog_item_type(),
                        CatalogItemType::Type,
                        "types cannot be migrated"
                    );
                    migrated_builtins.push(system_object_mapping.unique_identifier.id);
                }
            }
            None => {
                let id = new_ids.next().expect("not enough global IDs");
                all_builtins.push((builtin, id));
                new_builtins.push(SystemObjectMapping {
                    description: SystemObjectDescription {
                        schema_name: builtin.schema().to_string(),
                        object_type: builtin.catalog_item_type(),
                        object_name: builtin.name().to_string(),
                    },
                    unique_identifier: SystemObjectUniqueIdentifier {
                        id,
                        fingerprint: builtin.fingerprint(),
                    },
                });
            }
        }
    }

    // Add new builtin items to catalog.
    txn.set_system_object_mappings(new_builtins)?;

    // Anything left in `system_object_mappings` must have been deleted and should be removed from
    // the catalog.
    let deleted_system_objects: BTreeSet<_> = system_object_mappings
        .into_iter()
        .map(|(_, system_object_mapping)| system_object_mapping.description)
        .collect();
    // If you are 100% positive that it is safe to delete a system object outside the
    // `mz_internal` schema, then add it to this set. Make sure that no prod environments are
    // using this object and that the upgrade checker does not show any issues.
    //
    // Objects can be removed from this set after one release.
    let delete_exceptions: HashSet<SystemObjectDescription> = [].into();
    // TODO(jkosh44) Technically we could support changing the type of a builtin object outside
    // of `mz_internal` (i.e. from a table to a view). However, builtin migrations don't currently
    // handle that scenario correctly.
    assert!(
        deleted_system_objects
            .iter()
            .all(
                |deleted_object| deleted_object.schema_name == MZ_INTERNAL_SCHEMA
                    || delete_exceptions.contains(deleted_object)
            ),
        "only mz_internal objects can be deleted, deleted objects: {:?}",
        deleted_system_objects
    );
    txn.remove_system_object_mappings(deleted_system_objects)?;

    Ok(BootstrapSystemIds {
        all_builtins,
        migrated_builtins,
    })
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
                        replication_factor: 1,
                        disk: is_cluster_size_v2(&cluster_size),
                        logging: default_logging_config(),
                        optimizer_feature_overrides: Default::default(),
                        schedule: Default::default(),
                    }),
                },
            )?;
        }
    }
    Ok(())
}

fn add_new_builtin_introspection_source_migration(
    txn: &mut mz_catalog::durable::Transaction<'_>,
) -> Result<(), AdapterError> {
    let mut new_indexes = Vec::new();
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

        // TODO(jkosh44) We probably want to delete these.
        if !introspection_source_index_ids.is_empty() {
            warn!("catalog contains leaked introspection source indexes: {introspection_source_index_ids:?}");
        }
    }
    txn.insert_introspection_source_indexes(new_indexes)?;
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
                    builtin_cluster_sizes.get_size(builtin_replica.name)?
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

pub(crate) fn builtin_cluster_replica_config(
    replica_size: String,
) -> mz_catalog::durable::ReplicaConfig {
    mz_catalog::durable::ReplicaConfig {
        location: mz_catalog::durable::ReplicaLocation::Managed {
            availability_zone: None,
            billed_as: None,
            disk: is_cluster_size_v2(&replica_size),
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
    /// Size to default introspection_cluster on bootstrap
    pub introspection_cluster: String,
    /// Size to default probe_cluster on bootstrap
    pub probe_cluster: String,
    /// Size to default support_cluster on bootstrap
    pub support_cluster: String,
}

impl BuiltinBootstrapClusterSizes {
    /// Gets the size of the builtin cluster based on the provided name
    fn get_size(&self, cluster_name: &str) -> Result<String, mz_catalog::durable::CatalogError> {
        if cluster_name == mz_catalog::builtin::MZ_SYSTEM_CLUSTER.name {
            Ok(self.system_cluster.clone())
        } else if cluster_name == mz_catalog::builtin::MZ_INTROSPECTION_CLUSTER.name {
            Ok(self.introspection_cluster.clone())
        } else if cluster_name == mz_catalog::builtin::MZ_PROBE_CLUSTER.name {
            Ok(self.probe_cluster.clone())
        } else if cluster_name == mz_catalog::builtin::MZ_SUPPORT_CLUSTER.name {
            Ok(self.support_cluster.clone())
        } else {
            Err(mz_catalog::durable::CatalogError::Catalog(
                SqlCatalogError::UnexpectedBuiltinCluster(cluster_name.to_owned()),
            ))
        }
    }
}

#[cfg(test)]
mod builtin_migration_tests {
    use std::collections::{BTreeMap, BTreeSet};

    use itertools::Itertools;
    use mz_catalog::memory::objects::{Index, MaterializedView, Table};
    use mz_controller_types::ClusterId;
    use mz_expr::MirRelationExpr;
    use mz_ore::now::NOW_ZERO;
    use mz_repr::{GlobalId, RelationDesc, RelationType, ScalarType};
    use mz_sql::catalog::CatalogDatabase;
    use mz_sql::names::{
        ItemQualifiers, QualifiedItemName, ResolvedDatabaseSpecifier, ResolvedIds,
    };
    use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;
    use mz_sql_parser::ast::Expr;

    use crate::catalog::{
        Catalog, CatalogItem, Op, OptimizedMirRelationExpr, DEFAULT_SCHEMA, SYSTEM_CONN_ID,
    };
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
                    create_sql: Some("CREATE TABLE t ()".to_string()),
                    desc: RelationDesc::empty()
                        .with_column("a", ScalarType::Int32.nullable(true))
                        .with_key(vec![0]),
                    defaults: vec![Expr::null(); 1],
                    conn_id: None,
                    resolved_ids: ResolvedIds(BTreeSet::new()),
                    custom_logical_compaction_window: None,
                    is_retained_metrics_object: false,
                }),
                SimplifiedItem::MaterializedView { referenced_names } => {
                    let table_list = referenced_names.iter().join(",");
                    let resolved_ids = convert_names_to_ids(referenced_names, id_mapping);
                    CatalogItem::MaterializedView(MaterializedView {
                        create_sql: format!(
                            "CREATE MATERIALIZED VIEW mv AS SELECT * FROM {table_list}"
                        ),
                        raw_expr: mz_sql::plan::HirRelationExpr::constant(
                            Vec::new(),
                            RelationType {
                                column_types: Vec::new(),
                                keys: Vec::new(),
                            },
                        ),
                        optimized_expr: OptimizedMirRelationExpr(MirRelationExpr::Constant {
                            rows: Ok(Vec::new()),
                            typ: RelationType {
                                column_types: Vec::new(),
                                keys: Vec::new(),
                            },
                        }),
                        desc: RelationDesc::empty()
                            .with_column("a", ScalarType::Int32.nullable(true))
                            .with_key(vec![0]),
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
                        create_sql: format!("CREATE INDEX idx ON {on} (a)"),
                        on: on_id,
                        keys: Vec::new(),
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
        expected_all_drop_ops: Vec<String>,
        expected_user_drop_ops: Vec<String>,
        expected_all_create_ops: Vec<String>,
        expected_user_create_ops: Vec<String>,
        expected_migrated_system_object_mappings: Vec<String>,
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
        Catalog::with_debug(NOW_ZERO.clone(), |mut catalog| async move {
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
                convert_ids_to_names(migration_metadata.all_drop_ops, &name_mapping),
                test_case.expected_all_drop_ops.into_iter().collect(),
                "{} test failed with wrong all drop ops",
                test_case.test_name
            );
            assert_eq!(
                convert_ids_to_names(migration_metadata.user_drop_ops, &name_mapping),
                test_case.expected_user_drop_ops.into_iter().collect(),
                "{} test failed with wrong user drop ops",
                test_case.test_name
            );
            assert_eq!(
                migration_metadata
                    .all_create_ops
                    .into_iter()
                    .map(|(_, _, name, _, _, _)| name.item)
                    .collect::<BTreeSet<_>>(),
                test_case.expected_all_create_ops.into_iter().collect(),
                "{} test failed with wrong all create ops",
                test_case.test_name
            );
            assert_eq!(
                migration_metadata
                    .user_create_ops
                    .into_iter()
                    .map(|(_, _, _, name)| name)
                    .collect::<BTreeSet<_>>(),
                test_case.expected_user_create_ops.into_iter().collect(),
                "{} test failed with wrong user create ops",
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
            expected_all_drop_ops: vec![],
            expected_user_drop_ops: vec![],
            expected_all_create_ops: vec![],
            expected_user_create_ops: vec![],
            expected_migrated_system_object_mappings: vec![],
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
            expected_all_drop_ops: vec!["s1".to_string()],
            expected_user_drop_ops: vec![],
            expected_all_create_ops: vec!["s1".to_string()],
            expected_user_create_ops: vec![],
            expected_migrated_system_object_mappings: vec!["s1".to_string()],
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
            expected_all_drop_ops: vec!["u1".to_string(), "s1".to_string()],
            expected_user_drop_ops: vec!["u1".to_string()],
            expected_all_create_ops: vec!["s1".to_string(), "u1".to_string()],
            expected_user_create_ops: vec!["u1".to_string()],
            expected_migrated_system_object_mappings: vec!["s1".to_string()],
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
            expected_all_drop_ops: vec!["u1".to_string(), "u2".to_string(), "s1".to_string()],
            expected_user_drop_ops: vec!["u1".to_string(), "u2".to_string()],
            expected_all_create_ops: vec!["s1".to_string(), "u2".to_string(), "u1".to_string()],
            expected_user_create_ops: vec!["u2".to_string(), "u1".to_string()],
            expected_migrated_system_object_mappings: vec!["s1".to_string()],
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
            expected_all_drop_ops: vec![
                "u2".to_string(),
                "s1".to_string(),
                "u1".to_string(),
                "s2".to_string(),
            ],
            expected_user_drop_ops: vec!["u2".to_string(), "u1".to_string()],
            expected_all_create_ops: vec![
                "s2".to_string(),
                "u1".to_string(),
                "s1".to_string(),
                "u2".to_string(),
            ],
            expected_user_create_ops: vec!["u1".to_string(), "u2".to_string()],
            expected_migrated_system_object_mappings: vec!["s1".to_string(), "s2".to_string()],
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
            expected_all_drop_ops: vec![
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
            expected_user_drop_ops: vec![],
            expected_all_create_ops: vec![
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
            expected_user_create_ops: vec![],
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
            expected_all_drop_ops: vec!["s2".to_string(), "s1".to_string()],
            expected_user_drop_ops: vec![],
            expected_all_create_ops: vec!["s1".to_string(), "s2".to_string()],
            expected_user_create_ops: vec![],
            expected_migrated_system_object_mappings: vec!["s1".to_string(), "s2".to_string()],
        };
        run_test_case(test_case).await;
    }
}
