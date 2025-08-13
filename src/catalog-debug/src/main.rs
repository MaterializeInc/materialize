// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Debug utility for Catalog storage.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs::File;
use std::io::{self, Write};
use std::path::PathBuf;
use std::process;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Instant;

use anyhow::Context;
use clap::Parser;
use futures::future::FutureExt;
use mz_adapter::catalog::{Catalog, InitializeStateResult};
use mz_adapter_types::bootstrap_builtin_cluster_config::{
    ANALYTICS_CLUSTER_DEFAULT_REPLICATION_FACTOR, BootstrapBuiltinClusterConfig,
    CATALOG_SERVER_CLUSTER_DEFAULT_REPLICATION_FACTOR, PROBE_CLUSTER_DEFAULT_REPLICATION_FACTOR,
    SUPPORT_CLUSTER_DEFAULT_REPLICATION_FACTOR, SYSTEM_CLUSTER_DEFAULT_REPLICATION_FACTOR,
};
use mz_build_info::{BuildInfo, build_info};
use mz_catalog::config::{BuiltinItemMigrationConfig, ClusterReplicaSizeMap, StateConfig};
use mz_catalog::durable::debug::{
    AuditLogCollection, ClusterCollection, ClusterIntrospectionSourceIndexCollection,
    ClusterReplicaCollection, Collection, CollectionTrace, CollectionType, CommentCollection,
    ConfigCollection, DatabaseCollection, DebugCatalogState, DefaultPrivilegeCollection,
    IdAllocatorCollection, ItemCollection, NetworkPolicyCollection, RoleAuthCollection,
    RoleCollection, SchemaCollection, SettingCollection, SourceReferencesCollection,
    StorageCollectionMetadataCollection, SystemConfigurationCollection,
    SystemItemMappingCollection, SystemPrivilegeCollection, Trace, TxnWalShardCollection,
    UnfinalizedShardsCollection,
};
use mz_catalog::durable::{
    BootstrapArgs, OpenableDurableCatalogState, persist_backed_catalog_state,
};
use mz_catalog::memory::objects::CatalogItem;
use mz_cloud_resources::AwsExternalIdPrefix;
use mz_license_keys::ValidatedLicenseKey;
use mz_orchestrator_tracing::{StaticTracingConfig, TracingCliArgs};
use mz_ore::cli::{self, CliConfig};
use mz_ore::collections::HashSet;
use mz_ore::error::ErrorExt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_ore::url::SensitiveUrl;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::cfg::PersistConfig;
use mz_persist_client::rpc::PubSubClientConnection;
use mz_persist_client::{Diagnostics, PersistClient, PersistLocation};
use mz_repr::{Diff, Timestamp};
use mz_service::secrets::SecretsReaderCliArgs;
use mz_sql::catalog::EnvironmentId;
use mz_storage_types::StorageDiff;
use mz_storage_types::connections::ConnectionContext;
use mz_storage_types::controller::StorageError;
use mz_storage_types::sources::SourceData;
use serde::{Deserialize, Serialize};
use tracing::{Instrument, error};

pub const BUILD_INFO: BuildInfo = build_info!();
pub static VERSION: LazyLock<String> = LazyLock::new(|| BUILD_INFO.human_version(None));

#[derive(Parser, Debug)]
#[clap(name = "catalog", next_line_help = true, version = VERSION.as_str())]
pub struct Args {
    // === Persist options. ===
    /// An opaque identifier for the environment in which this process is
    /// running.
    #[clap(long, env = "ENVIRONMENT_ID")]
    environment_id: EnvironmentId,
    /// Where the persist library should store its blob data.
    #[clap(long, env = "PERSIST_BLOB_URL")]
    persist_blob_url: SensitiveUrl,
    /// Where the persist library should perform consensus.
    #[clap(long, env = "PERSIST_CONSENSUS_URL")]
    persist_consensus_url: SensitiveUrl,
    // === Cloud options. ===
    /// An external ID to be supplied to all AWS AssumeRole operations.
    ///
    /// Details: <https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html>
    #[clap(long, env = "AWS_EXTERNAL_ID", value_name = "ID", value_parser = AwsExternalIdPrefix::new_from_cli_argument_or_environment_variable)]
    aws_external_id_prefix: Option<AwsExternalIdPrefix>,

    /// The ARN for a Materialize-controlled role to assume before assuming
    /// a customer's requested role for an AWS connection.
    #[clap(long, env = "AWS_CONNECTION_ROLE_ARN")]
    aws_connection_role_arn: Option<String>,
    // === Tracing options. ===
    #[clap(flatten)]
    tracing: TracingCliArgs,
    // === Other options. ===
    #[clap(long)]
    deploy_generation: Option<u64>,

    #[clap(subcommand)]
    action: Action,
}

#[derive(Debug, Clone, clap::Subcommand)]
enum Action {
    /// Dumps the catalog contents to stdout in a human-readable format.
    /// Includes JSON for each key and value that can be hand edited and
    /// then passed to the `edit` or `delete` commands. Also includes statistics
    /// for each collection.
    Dump {
        /// Ignores the `audit_log` and `storage_usage` usage collections, which are often
        /// extremely large and not that useful for debugging.
        #[clap(long)]
        ignore_large_collections: bool,
        /// A list of collections to ignore.
        #[clap(long, short = 'i', action = clap::ArgAction::Append)]
        ignore: Vec<CollectionType>,
        /// Only dumps the statistics of each collection and not the contents.
        #[clap(long)]
        stats_only: bool,
        /// Consolidates the catalog contents.
        #[clap(long, short = 'c')]
        consolidate: bool,
        /// Write output to specified path. Default stdout.
        target: Option<PathBuf>,
    },
    /// Prints the current epoch.
    Epoch {
        /// Write output to specified path. Default stdout.
        target: Option<PathBuf>,
    },
    /// Edits a single item in a collection in the catalog.
    Edit {
        /// The name of the catalog collection to edit.
        collection: String,
        /// The JSON-encoded key that identifies the item to edit.
        key: serde_json::Value,
        /// The new JSON-encoded value for the item.
        value: serde_json::Value,
    },
    /// Deletes a single item in a collection in the catalog
    Delete {
        /// The name of the catalog collection to edit.
        collection: String,
        /// The JSON-encoded key that identifies the item to delete.
        key: serde_json::Value,
    },
    /// Checks if the specified catalog could be upgraded from its state to the
    /// adapter catalog at the version of this binary. Prints a success message
    /// or error message. Exits with 0 if the upgrade would succeed, otherwise
    /// non-zero. Can be used on a running environmentd. Operates without
    /// interfering with it or committing any data to that catalog.
    UpgradeCheck {
        #[clap(flatten)]
        secrets: SecretsReaderCliArgs,
        /// Map of cluster name to resource specification. Check the README for latest values.
        cluster_replica_sizes: String,
    },
}

#[tokio::main]
async fn main() {
    let args: Args = cli::parse_args(CliConfig {
        env_prefix: Some("MZ_CATALOG_DEBUG_"),
        enable_version_flag: true,
    });

    let (_, _tracing_guard) = args
        .tracing
        .configure_tracing(
            StaticTracingConfig {
                service_name: "catalog-debug",
                build_info: BUILD_INFO,
            },
            MetricsRegistry::new(),
        )
        .await
        .expect("failed to init tracing");

    if let Err(err) = run(args).await {
        eprintln!(
            "catalog-debug: fatal: {}\nbacktrace: {}",
            err.display_with_causes(),
            err.backtrace()
        );
        process::exit(1);
    }
}

async fn run(args: Args) -> Result<(), anyhow::Error> {
    let metrics_registry = MetricsRegistry::new();
    let start = Instant::now();
    // It's important that the version in this `BUILD_INFO` is kept in sync with the build
    // info used to write data to the persist catalog.
    let persist_config = PersistConfig::new_default_configs(&BUILD_INFO, SYSTEM_TIME.clone());
    let persist_clients = PersistClientCache::new(persist_config, &metrics_registry, |_, _| {
        PubSubClientConnection::noop()
    });
    let persist_location = PersistLocation {
        blob_uri: args.persist_blob_url.clone(),
        consensus_uri: args.persist_consensus_url.clone(),
    };
    let persist_client = persist_clients.open(persist_location).await?;
    let organization_id = args.environment_id.organization_id();
    let metrics = Arc::new(mz_catalog::durable::Metrics::new(&metrics_registry));
    let openable_state = persist_backed_catalog_state(
        persist_client.clone(),
        organization_id,
        BUILD_INFO.semver_version(),
        args.deploy_generation,
        metrics,
    )
    .await?;

    match args.action.clone() {
        Action::Dump {
            ignore_large_collections,
            ignore,
            stats_only,
            consolidate,
            target,
        } => {
            let ignore: HashSet<_> = ignore.into_iter().collect();
            let target: Box<dyn Write> = if let Some(path) = target {
                Box::new(File::create(path)?)
            } else {
                Box::new(io::stdout().lock())
            };
            dump(
                openable_state,
                ignore_large_collections,
                ignore,
                stats_only,
                consolidate,
                target,
            )
            .await
        }
        Action::Epoch { target } => {
            let target: Box<dyn Write> = if let Some(path) = target {
                Box::new(File::create(path)?)
            } else {
                Box::new(io::stdout().lock())
            };
            epoch(openable_state, target).await
        }
        Action::Edit {
            collection,
            key,
            value,
        } => edit(openable_state, collection, key, value).await,
        Action::Delete { collection, key } => delete(openable_state, collection, key).await,
        Action::UpgradeCheck {
            secrets,
            cluster_replica_sizes,
        } => {
            let cluster_replica_sizes =
                ClusterReplicaSizeMap::parse_from_str(&cluster_replica_sizes, false)
                    .context("parsing replica size map")?;
            upgrade_check(
                args,
                openable_state,
                persist_client,
                secrets,
                cluster_replica_sizes,
                start,
            )
            .await
        }
    }
}

/// Macro to help call function `$fn` with the correct generic parameter that matches
/// `$collection_type`.
macro_rules! for_collection {
    ($collection_type:expr, $fn:ident $(, $arg:expr)*) => {
        match $collection_type {
            CollectionType::AuditLog => $fn::<AuditLogCollection>($($arg),*).await?,
            CollectionType::ComputeInstance => $fn::<ClusterCollection>($($arg),*).await?,
            CollectionType::ComputeIntrospectionSourceIndex => $fn::<ClusterIntrospectionSourceIndexCollection>($($arg),*).await?,
            CollectionType::ComputeReplicas => $fn::<ClusterReplicaCollection>($($arg),*).await?,
            CollectionType::Comments => $fn::<CommentCollection>($($arg),*).await?,
            CollectionType::Config => $fn::<ConfigCollection>($($arg),*).await?,
            CollectionType::Database => $fn::<DatabaseCollection>($($arg),*).await?,
            CollectionType::DefaultPrivileges => $fn::<DefaultPrivilegeCollection>($($arg),*).await?,
            CollectionType::IdAlloc => $fn::<IdAllocatorCollection>($($arg),*).await?,
            CollectionType::Item => $fn::<ItemCollection>($($arg),*).await?,
            CollectionType::NetworkPolicy => $fn::<NetworkPolicyCollection>($($arg),*).await?,
            CollectionType::Role => $fn::<RoleCollection>($($arg),*).await?,
            CollectionType::RoleAuth => $fn::<RoleAuthCollection>($($arg),*).await?,
            CollectionType::Schema => $fn::<SchemaCollection>($($arg),*).await?,
            CollectionType::Setting => $fn::<SettingCollection>($($arg),*).await?,
            CollectionType::SourceReferences => $fn::<SourceReferencesCollection>($($arg),*).await?,
            CollectionType::SystemConfiguration => $fn::<SystemConfigurationCollection>($($arg),*).await?,
            CollectionType::SystemGidMapping => $fn::<SystemItemMappingCollection>($($arg),*).await?,
            CollectionType::SystemPrivileges => $fn::<SystemPrivilegeCollection>($($arg),*).await?,
            CollectionType::StorageCollectionMetadata => $fn::<StorageCollectionMetadataCollection>($($arg),*).await?,
            CollectionType::UnfinalizedShard => $fn::<UnfinalizedShardsCollection>($($arg),*).await?,
            CollectionType::TxnWalShard => $fn::<TxnWalShardCollection>($($arg),*).await?,
        }
    };
}

async fn edit(
    openable_state: Box<dyn OpenableDurableCatalogState>,
    collection: String,
    key: serde_json::Value,
    value: serde_json::Value,
) -> Result<(), anyhow::Error> {
    async fn edit_col<T: Collection>(
        mut debug_state: DebugCatalogState,
        key: serde_json::Value,
        value: serde_json::Value,
    ) -> Result<serde_json::Value, anyhow::Error>
    where
        for<'a> T::Key: PartialEq + Eq + Debug + Clone + Deserialize<'a>,
        for<'a> T::Value: Debug + Clone + Serialize + Deserialize<'a>,
    {
        let key: T::Key = serde_json::from_value(key)?;
        let value: T::Value = serde_json::from_value(value)?;
        let prev = debug_state.edit::<T>(key.clone(), value.clone()).await?;
        Ok(serde_json::to_value(prev)?)
    }

    let collection_type: CollectionType = collection.parse()?;
    let debug_state = openable_state.open_debug().await?;
    let prev = for_collection!(collection_type, edit_col, debug_state, key, value);
    println!("previous value: {prev:?}");
    Ok(())
}

async fn delete(
    openable_state: Box<dyn OpenableDurableCatalogState>,
    collection: String,
    key: serde_json::Value,
) -> Result<(), anyhow::Error> {
    async fn delete_col<T: Collection>(
        mut debug_state: DebugCatalogState,
        key: serde_json::Value,
    ) -> Result<(), anyhow::Error>
    where
        for<'a> T::Key: PartialEq + Eq + Debug + Clone + Deserialize<'a>,
        T::Value: Debug,
    {
        let key: T::Key = serde_json::from_value(key)?;
        debug_state.delete::<T>(key.clone()).await?;
        Ok(())
    }

    let collection_type: CollectionType = collection.parse()?;
    let debug_state = openable_state.open_debug().await?;
    for_collection!(collection_type, delete_col, debug_state, key);
    Ok(())
}

async fn dump(
    mut openable_state: Box<dyn OpenableDurableCatalogState>,
    ignore_large_collections: bool,
    ignore: HashSet<CollectionType>,
    stats_only: bool,
    consolidate: bool,
    mut target: impl Write,
) -> Result<(), anyhow::Error> {
    fn dump_col<T: Collection>(
        data: &mut BTreeMap<String, DumpedCollection>,
        trace: CollectionTrace<T>,
        ignore: &HashSet<CollectionType>,
        stats_only: bool,
        consolidate: bool,
    ) where
        T::Key: Serialize + Debug + 'static,
        T::Value: Serialize + Debug + 'static,
    {
        if ignore.contains(&T::collection_type()) {
            return;
        }

        let entries: Vec<_> = trace
            .values
            .into_iter()
            .map(|((k, v), timestamp, diff)| {
                let key_json = serde_json::to_string(&k).expect("must serialize");
                let value_json = serde_json::to_string(&v).expect("must serialize");
                DumpedEntry {
                    key: Box::new(k),
                    value: Box::new(v),
                    key_json: UnescapedDebug(key_json),
                    value_json: UnescapedDebug(value_json),
                    timestamp,
                    diff,
                }
            })
            .collect();

        let total_count = entries.len();
        let addition_count = entries
            .iter()
            .filter(|entry| entry.diff == Diff::ONE)
            .count();
        let retraction_count = entries
            .iter()
            .filter(|entry| entry.diff == Diff::MINUS_ONE)
            .count();
        let entries = if stats_only { None } else { Some(entries) };
        let dumped_col = DumpedCollection {
            total_count,
            addition_count,
            retraction_count,
            entries,
        };
        let name = T::name();

        if consolidate && retraction_count != 0 {
            error!(
                "{name} catalog collection has corrupt entries, there should be no retractions in a consolidated catalog, but there are {retraction_count} retractions"
            );
        }

        data.insert(name, dumped_col);
    }

    let mut data = BTreeMap::new();
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
        role_auth,
        schemas,
        settings,
        source_references,
        system_object_mappings,
        system_configurations,
        system_privileges,
        storage_collection_metadata,
        unfinalized_shards,
        txn_wal_shard,
    } = if consolidate {
        openable_state.trace_consolidated().await?
    } else {
        openable_state.trace_unconsolidated().await?
    };

    if !ignore_large_collections {
        dump_col(&mut data, audit_log, &ignore, stats_only, consolidate);
    }
    dump_col(&mut data, clusters, &ignore, stats_only, consolidate);
    dump_col(
        &mut data,
        introspection_sources,
        &ignore,
        stats_only,
        consolidate,
    );
    dump_col(
        &mut data,
        cluster_replicas,
        &ignore,
        stats_only,
        consolidate,
    );
    dump_col(&mut data, comments, &ignore, stats_only, consolidate);
    dump_col(&mut data, configs, &ignore, stats_only, consolidate);
    dump_col(&mut data, databases, &ignore, stats_only, consolidate);
    dump_col(
        &mut data,
        default_privileges,
        &ignore,
        stats_only,
        consolidate,
    );
    dump_col(&mut data, id_allocator, &ignore, stats_only, consolidate);
    dump_col(&mut data, items, &ignore, stats_only, consolidate);
    dump_col(
        &mut data,
        network_policies,
        &ignore,
        stats_only,
        consolidate,
    );
    dump_col(&mut data, roles, &ignore, stats_only, consolidate);
    dump_col(&mut data, role_auth, &ignore, stats_only, consolidate);
    dump_col(&mut data, schemas, &ignore, stats_only, consolidate);
    dump_col(&mut data, settings, &ignore, stats_only, consolidate);
    dump_col(
        &mut data,
        source_references,
        &ignore,
        stats_only,
        consolidate,
    );
    dump_col(
        &mut data,
        system_configurations,
        &ignore,
        stats_only,
        consolidate,
    );
    dump_col(
        &mut data,
        system_object_mappings,
        &ignore,
        stats_only,
        consolidate,
    );
    dump_col(
        &mut data,
        system_privileges,
        &ignore,
        stats_only,
        consolidate,
    );
    dump_col(
        &mut data,
        storage_collection_metadata,
        &ignore,
        stats_only,
        consolidate,
    );
    dump_col(
        &mut data,
        unfinalized_shards,
        &ignore,
        stats_only,
        consolidate,
    );
    dump_col(&mut data, txn_wal_shard, &ignore, stats_only, consolidate);

    writeln!(&mut target, "{data:#?}")?;
    Ok(())
}

async fn epoch(
    mut openable_state: Box<dyn OpenableDurableCatalogState>,
    mut target: impl Write,
) -> Result<(), anyhow::Error> {
    let epoch = openable_state.epoch().await?;
    writeln!(&mut target, "Epoch: {epoch:#?}")?;
    Ok(())
}

async fn upgrade_check(
    args: Args,
    openable_state: Box<dyn OpenableDurableCatalogState>,
    persist_client: PersistClient,
    secrets: SecretsReaderCliArgs,
    cluster_replica_sizes: ClusterReplicaSizeMap,
    start: Instant,
) -> Result<(), anyhow::Error> {
    let secrets_reader = secrets.load().await.context("loading secrets reader")?;

    let now = SYSTEM_TIME.clone();
    let mut storage = openable_state
        .open_savepoint(
            now().into(),
            &BootstrapArgs {
                default_cluster_replica_size:
                    "DEFAULT CLUSTER REPLICA SIZE IS ONLY USED FOR NEW ENVIRONMENTS".into(),
                default_cluster_replication_factor: 1,
                bootstrap_role: None,
                cluster_replica_size_map: cluster_replica_sizes.clone(),
            },
        )
        .await?
        .0;

    // If this upgrade has new builtin replicas, then we need to assign some size to it. It doesn't
    // really matter what size since it's not persisted, so we pick a random valid one.
    let builtin_clusters_replica_size = cluster_replica_sizes
        .0
        .first_key_value()
        .expect("we must have at least a single valid replica size")
        .0
        .clone();

    let boot_ts = now().into();
    let read_only = true;
    // BOXED FUTURE: As of Nov 2023 the returned Future from this function was 7.5KB. This would
    // get stored on the stack which is bad for runtime performance, and blow up our stack usage.
    // Because of that we purposefully move this Future onto the heap (i.e. Box it).
    let InitializeStateResult {
        state,
        migrated_storage_collections_0dt: _,
        new_builtin_collections: _,
        builtin_table_updates: _,
        last_seen_version,
        expr_cache_handle: _,
        cached_global_exprs: _,
        uncached_local_exprs: _,
    } = Catalog::initialize_state(
        StateConfig {
            unsafe_mode: true,
            all_features: false,
            build_info: &BUILD_INFO,
            environment_id: args.environment_id.clone(),
            read_only,
            now,
            boot_ts,
            skip_migrations: false,
            cluster_replica_sizes,
            builtin_system_cluster_config: BootstrapBuiltinClusterConfig {
                size: builtin_clusters_replica_size.clone(),
                replication_factor: SYSTEM_CLUSTER_DEFAULT_REPLICATION_FACTOR,
            },
            builtin_catalog_server_cluster_config: BootstrapBuiltinClusterConfig {
                size: builtin_clusters_replica_size.clone(),
                replication_factor: CATALOG_SERVER_CLUSTER_DEFAULT_REPLICATION_FACTOR,
            },
            builtin_probe_cluster_config: BootstrapBuiltinClusterConfig {
                size: builtin_clusters_replica_size.clone(),
                replication_factor: PROBE_CLUSTER_DEFAULT_REPLICATION_FACTOR,
            },
            builtin_support_cluster_config: BootstrapBuiltinClusterConfig {
                size: builtin_clusters_replica_size.clone(),
                replication_factor: SUPPORT_CLUSTER_DEFAULT_REPLICATION_FACTOR,
            },
            builtin_analytics_cluster_config: BootstrapBuiltinClusterConfig {
                size: builtin_clusters_replica_size.clone(),
                replication_factor: ANALYTICS_CLUSTER_DEFAULT_REPLICATION_FACTOR,
            },
            system_parameter_defaults: Default::default(),
            remote_system_parameters: None,
            availability_zones: vec![],
            egress_addresses: vec![],
            aws_principal_context: None,
            aws_privatelink_availability_zones: None,
            http_host_name: None,
            connection_context: ConnectionContext::from_cli_args(
                args.environment_id.to_string(),
                &args.tracing.startup_log_filter,
                args.aws_external_id_prefix,
                args.aws_connection_role_arn,
                secrets_reader,
                None,
            ),
            builtin_item_migration_config: BuiltinItemMigrationConfig {
                // We don't actually want to write anything down, so use an in-memory persist
                // client.
                persist_client: PersistClient::new_for_tests().await,
                read_only,
            },
            persist_client: persist_client.clone(),
            enable_expression_cache_override: None,
            helm_chart_version: None,
            external_login_password_mz_system: None,
            license_key: ValidatedLicenseKey::for_tests(),
        },
        &mut storage,
    )
    .instrument(tracing::info_span!("catalog::initialize_state"))
    .boxed()
    .await?;
    let dur = start.elapsed();

    let msg = format!(
        "catalog upgrade from {} to {} would succeed in about {} ms",
        last_seen_version,
        &BUILD_INFO.human_version(None),
        dur.as_millis(),
    );
    println!("{msg}");

    // Check that we can evolve the schema for all Persist shards.
    let storage_entries = state
        .get_entries()
        .filter_map(|(_item_id, entry)| match entry.item() {
            // TODO(alter_table): Handle multiple versions of tables.
            CatalogItem::Table(table) => Some((table.global_id_writes(), table.desc.latest())),
            CatalogItem::Source(source) => Some((source.global_id(), source.desc.clone())),
            CatalogItem::ContinualTask(ct) => Some((ct.global_id(), ct.desc.clone())),
            CatalogItem::MaterializedView(mv) => Some((mv.global_id(), mv.desc.clone())),
            CatalogItem::Log(_)
            | CatalogItem::View(_)
            | CatalogItem::Sink(_)
            | CatalogItem::Index(_)
            | CatalogItem::Type(_)
            | CatalogItem::Func(_)
            | CatalogItem::Secret(_)
            | CatalogItem::Connection(_) => None,
        });

    let mut storage_errors = BTreeMap::default();
    for (gid, item_desc) in storage_entries {
        // If a new version adds a BuiltinTable or BuiltinSource, we won't have created the shard
        // yet so there isn't anything to check.
        let maybe_shard_id = state
            .storage_metadata()
            .get_collection_shard::<Timestamp>(gid);
        let shard_id = match maybe_shard_id {
            Ok(shard_id) => shard_id,
            Err(StorageError::IdentifierMissing(_)) => {
                println!("no shard_id found for {gid}, continuing...");
                continue;
            }
            Err(err) => {
                // Collect errors instead of bailing on the first one.
                storage_errors.insert(gid, err.to_string());
                continue;
            }
        };
        println!("checking Persist schema info for {gid}: {shard_id}");

        let diagnostics = Diagnostics {
            shard_name: gid.to_string(),
            handle_purpose: "catalog upgrade check".to_string(),
        };
        let persisted_schema = persist_client
            .latest_schema::<SourceData, (), Timestamp, StorageDiff>(shard_id, diagnostics)
            .await
            .expect("invalid persist usage");
        // If in the new version a BuiltinTable or BuiltinSource is changed (e.g. a new
        // column is added) then we'll potentially have a new shard, but no writes will
        // have occurred so no schema will be registered.
        let Some((_schema_id, persisted_relation_desc, _)) = persisted_schema else {
            println!("no schema found for {gid} '{shard_id}', continuing...");
            continue;
        };

        let persisted_data_type =
            mz_persist_types::columnar::data_type::<SourceData>(&persisted_relation_desc)?;
        let new_data_type = mz_persist_types::columnar::data_type::<SourceData>(&item_desc)?;

        let migration =
            mz_persist_types::schema::backward_compatible(&persisted_data_type, &new_data_type);
        if migration.is_none() {
            let msg = format!(
                "invalid Persist schema migration!\nshard_id: {}\npersisted: {:?}\n{:?}\nnew: {:?}\n{:?}",
                shard_id, persisted_relation_desc, persisted_data_type, item_desc, new_data_type,
            );
            storage_errors.insert(gid, msg);
        }
    }

    if !storage_errors.is_empty() {
        anyhow::bail!("validation of storage objects failed! errors: {storage_errors:?}")
    } else {
        Ok(())
    }
}

struct DumpedCollection {
    total_count: usize,
    addition_count: usize,
    retraction_count: usize,
    entries: Option<Vec<DumpedEntry>>,
}

impl std::fmt::Debug for DumpedCollection {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut debug = f.debug_struct("");
        let debug = debug.field("total_count", &self.total_count);
        let debug = debug.field("addition_count", &self.addition_count);
        let debug = debug.field("retraction_count", &self.retraction_count);
        let debug = match &self.entries {
            Some(entries) => debug.field("entries", entries),
            None => debug,
        };
        debug.finish()
    }
}

struct DumpedEntry {
    key: Box<dyn std::fmt::Debug>,
    value: Box<dyn std::fmt::Debug>,
    key_json: UnescapedDebug,
    value_json: UnescapedDebug,
    timestamp: Timestamp,
    diff: Diff,
}

impl std::fmt::Debug for DumpedEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("")
            .field("key", &self.key)
            .field("value", &self.value)
            .field("key_json", &self.key_json)
            .field("value_json", &self.value_json)
            .field("timestamp", &self.timestamp)
            .field("diff", &self.diff)
            .finish()
    }
}

// We want to auto format things with debug, but also not print \ before the " in JSON values, so
// implement our own debug that doesn't escape.
struct UnescapedDebug(String);

impl std::fmt::Debug for UnescapedDebug {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "'{}'", &self.0)
    }
}
