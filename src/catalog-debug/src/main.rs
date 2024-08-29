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
use std::sync::LazyLock;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::Context;
use clap::Parser;
use futures::future::FutureExt;
use mz_adapter::catalog::{Catalog, InitializeStateResult};
use mz_build_info::{build_info, BuildInfo};
use mz_catalog::config::{BuiltinItemMigrationConfig, ClusterReplicaSizeMap, StateConfig};
use mz_catalog::durable::debug::{
    AuditLogCollection, ClusterCollection, ClusterIntrospectionSourceIndexCollection,
    ClusterReplicaCollection, Collection, CollectionTrace, CollectionType, CommentCollection,
    ConfigCollection, DatabaseCollection, DebugCatalogState, DefaultPrivilegeCollection,
    IdAllocatorCollection, ItemCollection, RoleCollection, SchemaCollection, SettingCollection,
    SourceReferencesCollection, StorageCollectionMetadataCollection, SystemConfigurationCollection,
    SystemItemMappingCollection, SystemPrivilegeCollection, Trace, TxnWalShardCollection,
    UnfinalizedShardsCollection,
};
use mz_catalog::durable::{
    persist_backed_catalog_state, BootstrapArgs, OpenableDurableCatalogState,
};
use mz_orchestrator_tracing::{StaticTracingConfig, TracingCliArgs};
use mz_ore::cli::{self, CliConfig};
use mz_ore::collections::HashSet;
use mz_ore::error::ErrorExt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::cfg::PersistConfig;
use mz_persist_client::rpc::PubSubClientConnection;
use mz_persist_client::PersistLocation;
use mz_repr::{Diff, Timestamp};
use mz_secrets::InMemorySecretsController;
use mz_sql::catalog::EnvironmentId;
use mz_sql::session::vars::ConnectionCounter;
use mz_storage_types::connections::ConnectionContext;
use serde::{Deserialize, Serialize};
use tracing::{error, Instrument};
use url::Url;
use uuid::Uuid;

pub const BUILD_INFO: BuildInfo = build_info!();
pub static VERSION: LazyLock<String> = LazyLock::new(|| BUILD_INFO.human_version());

#[derive(Parser, Debug)]
#[clap(name = "catalog", next_line_help = true, version = VERSION.as_str())]
pub struct Args {
    // === Persist options. ===
    /// The organization ID of the environment.
    #[clap(long, env = "ORG_ID")]
    organization_id: Uuid,
    /// Where the persist library should store its blob data.
    #[clap(long, env = "PERSIST_BLOB_URL")]
    persist_blob_url: Url,
    /// Where the persist library should perform consensus.
    #[clap(long, env = "PERSIST_CONSENSUS_URL")]
    persist_consensus_url: Url,

    #[clap(subcommand)]
    action: Action,

    #[clap(flatten)]
    tracing: TracingCliArgs,
}

#[derive(Debug, clap::Subcommand)]
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
        /// Map of cluster name to resource specification. Check the README for latest values.
        cluster_replica_sizes: Option<String>,
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
        blob_uri: args.persist_blob_url.to_string(),
        consensus_uri: args.persist_consensus_url.to_string(),
    };
    let persist_client = persist_clients.open(persist_location).await?;
    let organization_id = args.organization_id;
    let metrics = Arc::new(mz_catalog::durable::Metrics::new(&metrics_registry));
    let openable_state = persist_backed_catalog_state(
        persist_client,
        organization_id,
        BUILD_INFO.semver_version(),
        None,
        metrics,
    )
    .await?;

    match args.action {
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
            cluster_replica_sizes,
        } => {
            let cluster_replica_sizes: ClusterReplicaSizeMap = match cluster_replica_sizes {
                None => Default::default(),
                Some(json) => serde_json::from_str(&json).context("parsing replica size map")?,
            };
            upgrade_check(openable_state, cluster_replica_sizes, start).await
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
            CollectionType::Role => $fn::<RoleCollection>($($arg),*).await?,
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
        let addition_count = entries.iter().filter(|entry| entry.diff == 1).count();
        let retraction_count = entries.iter().filter(|entry| entry.diff == -1).count();
        let entries = if stats_only { None } else { Some(entries) };
        let dumped_col = DumpedCollection {
            total_count,
            addition_count,
            retraction_count,
            entries,
        };
        let name = T::name();

        if consolidate && retraction_count != 0 {
            error!("{name} catalog collection has corrupt entries, there should be no retractions in a consolidated catalog, but there are {retraction_count} retractions");
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
    dump_col(&mut data, roles, &ignore, stats_only, consolidate);
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
    openable_state: Box<dyn OpenableDurableCatalogState>,
    cluster_replica_sizes: ClusterReplicaSizeMap,
    start: Instant,
) -> Result<(), anyhow::Error> {
    let now = SYSTEM_TIME.clone();
    let mut storage = openable_state
        .open_savepoint(
            now(),
            &BootstrapArgs {
                default_cluster_replica_size:
                    "DEFAULT CLUSTER REPLICA SIZE IS ONLY USED FOR NEW ENVIRONMENTS".into(),
                bootstrap_role: None,
            },
        )
        .await?;

    // If this upgrade has new builtin replicas, then we need to assign some size to it. It doesn't
    // really matter what size since it's not persisted, so we pick a random valid one.
    let builtin_clusters_replica_size = cluster_replica_sizes
        .0
        .first_key_value()
        .expect("we must have at least a single valid replica size")
        .0
        .clone();

    let boot_ts = now().into();
    // BOXED FUTURE: As of Nov 2023 the returned Future from this function was 7.5KB. This would
    // get stored on the stack which is bad for runtime performance, and blow up our stack usage.
    // Because of that we purposefully move this Future onto the heap (i.e. Box it).
    let InitializeStateResult {
        state: _state,
        storage_collections_to_drop: _,
        migrated_storage_collections_0dt: _,
        builtin_table_updates: _,
        last_seen_version,
    } = Catalog::initialize_state(
        StateConfig {
            unsafe_mode: true,
            all_features: false,
            build_info: &BUILD_INFO,
            environment_id: EnvironmentId::for_tests(),
            now,
            boot_ts,
            skip_migrations: false,
            cluster_replica_sizes,
            builtin_system_cluster_replica_size: builtin_clusters_replica_size.clone(),
            builtin_catalog_server_cluster_replica_size: builtin_clusters_replica_size.clone(),
            builtin_probe_cluster_replica_size: builtin_clusters_replica_size.clone(),
            builtin_support_cluster_replica_size: builtin_clusters_replica_size.clone(),
            builtin_analytics_cluster_replica_size: builtin_clusters_replica_size,
            system_parameter_defaults: Default::default(),
            remote_system_parameters: None,
            availability_zones: vec![],
            egress_ips: vec![],
            aws_principal_context: None,
            aws_privatelink_availability_zones: None,
            http_host_name: None,
            connection_context: ConnectionContext::for_tests(Arc::new(
                InMemorySecretsController::new(),
            )),
            active_connection_count: Arc::new(Mutex::new(ConnectionCounter::new(0, 0))),
            builtin_item_migration_config: BuiltinItemMigrationConfig::Legacy,
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
        &BUILD_INFO.human_version(),
        dur.as_millis(),
    );
    println!("{msg}");
    Ok(())
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
