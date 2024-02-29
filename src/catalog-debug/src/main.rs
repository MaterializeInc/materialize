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
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::Context;
use clap::Parser;
use mz_adapter::catalog::Catalog;
use mz_build_info::{build_info, BuildInfo};
use mz_catalog::config::{ClusterReplicaSizeMap, StateConfig};
use mz_catalog::durable::debug::{
    AuditLogCollection, ClusterCollection, ClusterIntrospectionSourceIndexCollection,
    ClusterReplicaCollection, Collection, CollectionTrace, CollectionType, CommentCollection,
    ConfigCollection, DatabaseCollection, DebugCatalogState, DefaultPrivilegeCollection,
    IdAllocatorCollection, ItemCollection, RoleCollection, SchemaCollection, SettingCollection,
    StorageMetadataCollection, StorageUsageCollection, SystemConfigurationCollection,
    SystemItemMappingCollection, SystemPrivilegeCollection, TimestampCollection, Trace,
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
use mz_repr::Diff;
use mz_secrets::InMemorySecretsController;
use mz_sql::catalog::EnvironmentId;
use mz_sql::session::vars::ConnectionCounter;
use mz_storage_types::connections::ConnectionContext;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

pub const BUILD_INFO: BuildInfo = build_info!();
pub static VERSION: Lazy<String> = Lazy::new(|| BUILD_INFO.human_version());

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
    /// Dumps the catalog contents to stdout in a human readable format.
    /// Includes JSON for each key and value that can be hand edited and
    /// then passed to the `edit` or `delete` commands.
    Dump {
        /// Ignores the `audit_log` and `storage_usage` usage collections, which are often
        /// extremely large and not that useful for debugging.
        #[clap(long)]
        ignore_large_collections: bool,
        /// A list of collections to ignore.
        #[clap(long, short = 'i', action = clap::ArgAction::Append)]
        ignore: Vec<CollectionType>,
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
    let openable_state: Box<dyn OpenableDurableCatalogState> = Box::new(
        persist_backed_catalog_state(
            persist_client,
            organization_id,
            BUILD_INFO.semver_version(),
            metrics,
        )
        .await?,
    );

    match args.action {
        Action::Dump {
            ignore_large_collections,
            ignore,
            target,
        } => {
            let ignore: HashSet<_> = ignore.into_iter().collect();
            let target: Box<dyn Write> = if let Some(path) = target {
                Box::new(File::create(path)?)
            } else {
                Box::new(io::stdout().lock())
            };
            dump(openable_state, ignore_large_collections, ignore, target).await
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
            CollectionType::StorageUsage => $fn::<StorageUsageCollection>($($arg),*).await?,
            CollectionType::SystemConfiguration => $fn::<SystemConfigurationCollection>($($arg),*).await?,
            CollectionType::SystemGidMapping => $fn::<SystemItemMappingCollection>($($arg),*).await?,
            CollectionType::SystemPrivileges => $fn::<SystemPrivilegeCollection>($($arg),*).await?,
            CollectionType::Timestamp => $fn::<TimestampCollection>($($arg),*).await?,
            CollectionType::StorageMetadata => $fn::<StorageMetadataCollection>($($arg),*).await?,
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
    mut target: impl Write,
) -> Result<(), anyhow::Error> {
    fn dump_col<T: Collection>(
        data: &mut BTreeMap<String, Vec<Dumped>>,
        trace: CollectionTrace<T>,
        ignore: &HashSet<CollectionType>,
    ) where
        T::Key: Serialize + Debug + 'static,
        T::Value: Serialize + Debug + 'static,
    {
        if ignore.contains(&T::collection_type()) {
            return;
        }

        let dumped = trace
            .values
            .into_iter()
            .map(|((k, v), timestamp, diff)| {
                let key_json = serde_json::to_string(&k).expect("must serialize");
                let value_json = serde_json::to_string(&v).expect("must serialize");
                Dumped {
                    key: Box::new(k),
                    value: Box::new(v),
                    key_json: UnescapedDebug(key_json),
                    value_json: UnescapedDebug(value_json),
                    timestamp,
                    diff,
                }
            })
            .collect();
        data.insert(T::name(), dumped);
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
        storage_usage,
        system_object_mappings,
        system_configurations,
        system_privileges,
        timestamps,
        storage_metadata,
    } = openable_state.trace().await?;

    if !ignore_large_collections {
        dump_col(&mut data, audit_log, &ignore);
    }
    dump_col(&mut data, clusters, &ignore);
    dump_col(&mut data, introspection_sources, &ignore);
    dump_col(&mut data, cluster_replicas, &ignore);
    dump_col(&mut data, comments, &ignore);
    dump_col(&mut data, configs, &ignore);
    dump_col(&mut data, databases, &ignore);
    dump_col(&mut data, default_privileges, &ignore);
    dump_col(&mut data, id_allocator, &ignore);
    dump_col(&mut data, items, &ignore);
    dump_col(&mut data, roles, &ignore);
    dump_col(&mut data, schemas, &ignore);
    dump_col(&mut data, settings, &ignore);
    if !ignore_large_collections {
        dump_col(&mut data, storage_usage, &ignore);
    }
    dump_col(&mut data, system_configurations, &ignore);
    dump_col(&mut data, system_object_mappings, &ignore);
    dump_col(&mut data, system_privileges, &ignore);
    dump_col(&mut data, timestamps, &ignore);
    dump_col(&mut data, storage_metadata, &ignore);

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
            None,
            None,
        )
        .await?;

    // If this upgrade has new builtin replicas, then we need to assign some size to it. It doesn't
    // really matter what size since it's not persisted, so we pick a random valid one.
    let builtin_cluster_replica_size = cluster_replica_sizes
        .0
        .first_key_value()
        .expect("we must have at least a single valid replica size")
        .0
        .clone();

    let (_catalog, _, last_catalog_version) = Catalog::initialize_state(
        StateConfig {
            unsafe_mode: true,
            all_features: false,
            build_info: &BUILD_INFO,
            environment_id: EnvironmentId::for_tests(),
            now,
            skip_migrations: false,
            cluster_replica_sizes,
            builtin_cluster_replica_size,
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
        },
        &mut storage,
    )
    .await?;
    let dur = start.elapsed();

    let msg = format!(
        "catalog upgrade from {} to {} would succeed in about {} ms",
        last_catalog_version,
        &BUILD_INFO.human_version(),
        dur.as_millis(),
    );
    println!("{msg}");
    Ok(())
}

struct Dumped {
    key: Box<dyn std::fmt::Debug>,
    value: Box<dyn std::fmt::Debug>,
    key_json: UnescapedDebug,
    value_json: UnescapedDebug,
    timestamp: String,
    diff: Diff,
}

impl std::fmt::Debug for Dumped {
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
