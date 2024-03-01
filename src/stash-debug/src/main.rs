// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Debug utility for stashes.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{self, Write};
use std::path::PathBuf;
use std::pin;
use std::process;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use futures::StreamExt;
use once_cell::sync::Lazy;
use tracing_subscriber::filter::EnvFilter;

use mz_adapter::catalog::Catalog;
use mz_build_info::{build_info, BuildInfo};
use mz_catalog::config::{ClusterReplicaSizeMap, StateConfig};
use mz_catalog::durable::{
    self as catalog, BootstrapArgs, OpenableDurableCatalogState, StashConfig,
};
use mz_ore::cli::{self, CliConfig};
use mz_ore::error::ErrorExt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_ore::retry::Retry;
use mz_secrets::InMemorySecretsController;
use mz_sql::catalog::EnvironmentId;
use mz_sql::session::vars::ConnectionCounter;
use mz_stash::{Stash, StashFactory};
use mz_storage_controller as storage;
use mz_storage_types::connections::ConnectionContext;

pub const BUILD_INFO: BuildInfo = build_info!();
pub static VERSION: Lazy<String> = Lazy::new(|| BUILD_INFO.human_version());

#[derive(Parser, Debug)]
#[clap(name = "stash", next_line_help = true, version = VERSION.as_str())]
pub struct Args {
    #[clap(long, env = "POSTGRES_URL")]
    postgres_url: String,

    #[clap(subcommand)]
    action: Action,

    /// Which log messages to emit.
    ///
    /// See environmentd's `--log-filter` option for details.
    #[clap(long, env = "LOG_FILTER", value_name = "FILTER", default_value = "off")]
    log_filter: EnvFilter,
}

#[derive(Debug, clap::Subcommand)]
enum Action {
    /// Dumps the stash contents to stdout in a human readable format.
    /// Includes JSON for each key and value that can be hand edited and
    /// then passed to the `edit` or `delete` commands.
    Dump {
        /// Write output to specified path. Default stdout.
        target: Option<PathBuf>,
    },
    /// Edits a single item in a collection in the stash.
    Edit {
        /// The name of the stash collection to edit.
        collection: String,
        /// The JSON-encoded key that identifies the item to edit.
        key: serde_json::Value,
        /// The new JSON-encoded value for the item.
        value: serde_json::Value,
    },
    /// Deletes a single item in a collection in the stash
    Delete {
        /// The name of the stash collection to edit.
        collection: String,
        /// The JSON-encoded key that identifies the item to delete.
        key: serde_json::Value,
    },
    /// Checks if the specified stash could be upgraded from its state to the
    /// adapter catalog at the version of this binary. Prints a success message
    /// or error message. Exits with 0 if the upgrade would succeed, otherwise
    /// non-zero. Can be used on a running environmentd. Operates without
    /// interfering with it or committing any data to that stash.
    UpgradeCheck {
        /// Map of cluster name to resource specification. Check the README for latest values.
        cluster_replica_sizes: Option<String>,
    },
}

#[tokio::main]
async fn main() {
    let args = cli::parse_args(CliConfig {
        env_prefix: Some("MZ_STASH_DEBUG_"),
        enable_version_flag: true,
    });
    if let Err(err) = run(args).await {
        eprintln!("stash: fatal: {}", err.display_with_causes());
        process::exit(1);
    }
}

async fn run(args: Args) -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt()
        .with_env_filter(args.log_filter)
        .with_writer(io::stdout)
        .init();

    let tls = mz_tls_util::make_tls(&tokio_postgres::config::Config::from_str(
        &args.postgres_url,
    )?)?;
    let factory = StashFactory::new(&MetricsRegistry::new());
    let mut stash = factory
        .open_readonly(args.postgres_url.clone(), None, tls.clone())
        .await?;
    let usage = Usage::from_stash(&mut stash).await?;
    let schema = None;

    match args.action {
        Action::Dump { target } => {
            let target: Box<dyn Write> = if let Some(path) = target {
                Box::new(File::create(path)?)
            } else {
                Box::new(io::stdout().lock())
            };
            dump(stash, usage, target).await
        }
        Action::Edit {
            collection,
            key,
            value,
        } => {
            // edit needs a mutable stash, so reconnect.
            let stash = factory.open(args.postgres_url, schema, tls, None).await?;
            edit(stash, usage, collection, key, value).await
        }
        Action::Delete { collection, key } => {
            // delete needs a mutable stash, so reconnect.
            let stash = factory.open(args.postgres_url, schema, tls, None).await?;
            delete(stash, usage, collection, key).await
        }
        Action::UpgradeCheck {
            cluster_replica_sizes,
        } => {
            let cluster_replica_sizes: ClusterReplicaSizeMap = match cluster_replica_sizes {
                None => Default::default(),
                Some(json) => serde_json::from_str(&json).context("parsing replica size map")?,
            };
            upgrade_check(
                StashConfig {
                    stash_factory: factory,
                    stash_url: args.postgres_url,
                    schema,
                    tls,
                },
                usage,
                cluster_replica_sizes,
            )
            .await
        }
    }
}

async fn edit(
    mut stash: Stash,
    usage: Usage,
    collection: String,
    key: serde_json::Value,
    value: serde_json::Value,
) -> Result<(), anyhow::Error> {
    let prev = usage.edit(&mut stash, collection, key, value).await?;
    println!("previous value: {:?}", prev);
    Ok(())
}

async fn delete(
    mut stash: Stash,
    usage: Usage,
    collection: String,
    key: serde_json::Value,
) -> Result<(), anyhow::Error> {
    usage.delete(&mut stash, collection, key).await?;
    Ok(())
}

async fn dump(mut stash: Stash, usage: Usage, mut target: impl Write) -> Result<(), anyhow::Error> {
    let data = usage.dump(&mut stash).await?;
    writeln!(&mut target, "{data:#?}")?;
    Ok(())
}
async fn upgrade_check(
    stash_config: StashConfig,
    usage: Usage,
    cluster_replica_sizes: ClusterReplicaSizeMap,
) -> Result<(), anyhow::Error> {
    let msg = usage
        .upgrade_check(stash_config, cluster_replica_sizes)
        .await?;
    println!("{msg}");
    Ok(())
}

macro_rules! for_collections {
    ($usage:expr, $macro:ident) => {
        match $usage {
            Usage::Catalog => {
                $macro!(catalog::AUDIT_LOG_COLLECTION);
                $macro!(catalog::CLUSTER_COLLECTION);
                $macro!(catalog::CLUSTER_INTROSPECTION_SOURCE_INDEX_COLLECTION);
                $macro!(catalog::CLUSTER_REPLICA_COLLECTION);
                $macro!(catalog::COMMENTS_COLLECTION);
                $macro!(catalog::CONFIG_COLLECTION);
                $macro!(catalog::DATABASES_COLLECTION);
                $macro!(catalog::DEFAULT_PRIVILEGES_COLLECTION);
                $macro!(catalog::ID_ALLOCATOR_COLLECTION);
                $macro!(catalog::ITEM_COLLECTION);
                $macro!(catalog::ROLES_COLLECTION);
                $macro!(catalog::SCHEMAS_COLLECTION);
                $macro!(catalog::SETTING_COLLECTION);
                $macro!(catalog::STORAGE_USAGE_COLLECTION);
                $macro!(catalog::SYSTEM_CONFIGURATION_COLLECTION);
                $macro!(catalog::SYSTEM_GID_MAPPING_COLLECTION);
                $macro!(catalog::SYSTEM_PRIVILEGES_COLLECTION);
                $macro!(catalog::TIMESTAMP_COLLECTION);
            }
            Usage::Storage => {
                $macro!(storage::METADATA_COLLECTION);
            }
        }
    };
}

struct Dumped {
    key: Box<dyn std::fmt::Debug>,
    value: Box<dyn std::fmt::Debug>,
    key_json: UnescapedDebug,
    value_json: UnescapedDebug,
    timestamp: mz_stash::Timestamp,
    diff: mz_stash::Diff,
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

#[derive(Debug)]
enum Usage {
    Catalog,
    Storage,
}

impl Usage {
    fn all_usages() -> Vec<Usage> {
        vec![Self::Catalog, Self::Storage]
    }

    /// Returns an error if there is any overlap of collection names from all
    /// Usages.
    fn verify_all_usages() -> Result<(), anyhow::Error> {
        let mut all_names = BTreeSet::new();
        for usage in Self::all_usages() {
            let mut names = usage.names();
            if names.is_subset(&all_names) {
                anyhow::bail!(
                    "duplicate names; cannot determine usage: {:?}",
                    all_names.intersection(&names)
                );
            }
            all_names.append(&mut names);
        }
        Ok(())
    }

    async fn from_stash(stash: &mut Stash) -> Result<Self, anyhow::Error> {
        // Determine which usage we are on by any collection matching any
        // expected name of a usage. To do that safely, we need to verify that
        // there is no overlap between expected names.
        Self::verify_all_usages()?;

        let names = BTreeSet::from_iter(stash.collections().await?.into_values());
        for usage in Self::all_usages() {
            // Some TypedCollections exist before any entries have been written
            // to a collection, so `stash.collections()` won't return it, and we
            // have to look for any overlap to indicate which stash we are on.
            if usage.names().intersection(&names).next().is_some() {
                return Ok(usage);
            }
        }
        anyhow::bail!("could not determine usage: unknown names: {:?}", names);
    }

    fn names(&self) -> BTreeSet<String> {
        BTreeSet::from_iter(
            match self {
                Self::Catalog => mz_catalog::durable::ALL_COLLECTIONS,
                Self::Storage => storage::ALL_COLLECTIONS,
            }
            .iter()
            .map(|s| s.to_string()),
        )
    }

    async fn dump(&self, stash: &mut Stash) -> Result<BTreeMap<&str, Vec<Dumped>>, anyhow::Error> {
        let mut collections = Vec::new();
        let collection_names = BTreeSet::from_iter(stash.collections().await?.into_values());
        macro_rules! dump_col {
            ($col:expr) => {
                // Collections might not yet exist.
                if collection_names.contains($col.name()) {
                    let values = $col.iter(stash).await?;
                    let values = values
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
                        .collect::<Vec<_>>();
                    collections.push(($col.name(), values));
                }
            };
        }

        for_collections!(self, dump_col);

        let data = BTreeMap::from_iter(collections);
        let data_names = BTreeSet::from_iter(data.keys().map(|k| k.to_string()));
        if data_names != self.names() {
            // This is useful to know because it can either be fine (collection
            // not yet created) or a programming error where this file was not
            // updated after adding a collection.
            eprintln!(
                "unexpected names, verify this program knows about all collections: got {:?}, expected {:?}",
                data_names,
                self.names()
            );
        }
        Ok(data)
    }

    async fn edit(
        &self,
        stash: &mut Stash,
        collection: String,
        key: serde_json::Value,
        value: serde_json::Value,
    ) -> Result<serde_json::Value, anyhow::Error> {
        macro_rules! edit_col {
            ($col:expr) => {
                if collection == $col.name() {
                    let key = serde_json::from_value(key)?;
                    let value = serde_json::from_value(value)?;
                    let (prev, _next) = $col
                        .upsert_key(stash, key, move |_| {
                            Ok::<_, std::convert::Infallible>(value)
                        })
                        .await??;
                    let prev = serde_json::to_value(&prev)?;
                    return Ok(prev);
                }
            };
        }
        for_collections!(self, edit_col);
        anyhow::bail!("unknown collection {} for stash {:?}", collection, self)
    }

    async fn delete(
        &self,
        stash: &mut Stash,
        collection: String,
        key: serde_json::Value,
    ) -> Result<(), anyhow::Error> {
        macro_rules! delete_col {
            ($col:expr) => {
                if collection == $col.name() {
                    let key = serde_json::from_value(key)?;
                    let keys = BTreeSet::from([key]);
                    $col.delete_keys(stash, keys).await?;
                    return Ok(());
                }
            };
        }
        for_collections!(self, delete_col);
        anyhow::bail!("unknown collection {} for stash {:?}", collection, self)
    }

    async fn upgrade_check(
        &self,
        stash_config: StashConfig,
        cluster_replica_sizes: ClusterReplicaSizeMap,
    ) -> Result<String, anyhow::Error> {
        if !matches!(self, Self::Catalog) {
            anyhow::bail!("upgrade_check expected Catalog stash, found {:?}", self);
        }

        let retry = Retry::default()
            .clamp_backoff(Duration::from_secs(1))
            .max_duration(Duration::from_secs(30))
            .into_retry_stream();
        let mut retry = pin::pin!(retry);

        loop {
            let now = SYSTEM_TIME.clone();
            let openable_storage = Box::new(mz_catalog::durable::stash_backed_catalog_state(
                stash_config.clone(),
            ));
            let mut storage = openable_storage
                .open_savepoint(
                    now(),
                    &BootstrapArgs {
                        default_cluster_replica_size: "1".into(),
                        bootstrap_role: None,
                    },
                    None,
                    None,
                )
                .await?;

            match Catalog::initialize_state(
                StateConfig {
                    unsafe_mode: true,
                    all_features: false,
                    build_info: &BUILD_INFO,
                    environment_id: EnvironmentId::for_tests(),
                    now,
                    skip_migrations: false,
                    cluster_replica_sizes: cluster_replica_sizes.clone(),
                    builtin_cluster_replica_size: "1".into(),
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
            .await
            {
                Ok((_, _, last_catalog_version)) => {
                    storage.expire().await;
                    return Ok(format!(
                        "catalog upgrade from {} to {} would succeed",
                        last_catalog_version,
                        BUILD_INFO.human_version(),
                    ));
                }
                Err(e) => {
                    if retry.next().await.is_none() {
                        tracing::error!(?e, "Could not open catalog, and out of retries.");
                        return Err(e.into());
                    }
                    tracing::warn!(?e, "Could not open catalog. Retrying...");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn test_verify_all_usages() {
        Usage::verify_all_usages().unwrap();
    }
}
