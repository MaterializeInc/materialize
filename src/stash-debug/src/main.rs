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
use std::process;
use std::str::FromStr;

use clap::Parser;
use once_cell::sync::Lazy;
use tracing_subscriber::filter::EnvFilter;

use mz_build_info::{build_info, BuildInfo};
use mz_ore::cli::{self, CliConfig};
use mz_ore::error::ErrorExt;
use mz_ore::metrics::MetricsRegistry;
use mz_stash::{Stash, StashFactory};
use mz_storage_controller as storage;

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

macro_rules! for_collections {
    ($usage:expr, $macro:ident) => {
        match $usage {
            Usage::Storage => {
                $macro!(storage::METADATA_COLLECTION);
                $macro!(storage::PERSIST_TXNS_SHARD);
                $macro!(storage::command_wals::SHARD_FINALIZATION);
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
    Storage,
}

impl Usage {
    fn all_usages() -> Vec<Usage> {
        vec![Self::Storage]
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn test_verify_all_usages() {
        Usage::verify_all_usages().unwrap();
    }
}
