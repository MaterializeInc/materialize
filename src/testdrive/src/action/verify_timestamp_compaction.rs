// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::bail;
use async_trait::async_trait;

use mz_dataflow_types::sources::MzOffset;
use mz_expr::PartitionId;
use mz_ore::retry::Retry;
use mz_sql::catalog::SessionCatalog;
use mz_sql::names::PartialObjectName;
use mz_stash::Stash;
use tokio_postgres::NoTls;

use crate::action::{Action, ControlFlow, State};
use crate::parser::BuiltinCommand;
use crate::util::mz_data::catalog_copy;

pub struct VerifyTimestampCompactionAction {
    source: String,
    max_size: usize,
    permit_progress: bool,
}

pub fn build_verify_timestamp_compaction_action(
    mut cmd: BuiltinCommand,
) -> Result<VerifyTimestampCompactionAction, anyhow::Error> {
    let source = cmd.args.string("source")?;
    let max_size = cmd.args.opt_parse("max-size")?.unwrap_or(3);
    let permit_progress = cmd.args.opt_bool("permit-progress")?.unwrap_or(false);
    cmd.args.done()?;
    Ok(VerifyTimestampCompactionAction {
        source,
        max_size,
        permit_progress,
    })
}

#[async_trait]
impl Action for VerifyTimestampCompactionAction {
    async fn undo(&self, _: &mut State) -> Result<(), anyhow::Error> {
        // Can't undo a verification.
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<ControlFlow, anyhow::Error> {
        let item_id = state
            .with_catalog_copy(|catalog| {
                catalog
                    .resolve_item(&PartialObjectName {
                        database: None,
                        schema: None,
                        item: self.source.clone(),
                    })
                    .map(|item| item.id())
            })
            .await?;
        // Skip if we don't know where the timestamp stash is or the catalog is.
        if item_id.is_none() || state.materialized_data_path.is_none() {
            println!(
                "Skipping timestamp binding compaction verification for {:?}.",
                self.source
            );
            Ok(ControlFlow::Continue)
        } else {
            // Unwrap is safe because this is known to be Some.
            let item_id = item_id.unwrap()?;
            let copy_schema = format!("mz_storage_copy_{}", state.seed);
            let initial_highest_base = Arc::new(AtomicU64::new(u64::MAX));
            let materialized_catalog_postgres_stash =
                state.materialized_catalog_postgres_stash.clone();
            let materialized_data_path = state.materialized_data_path.clone();
            Retry::default()
                .initial_backoff(Duration::from_secs(1))
                .max_duration(Duration::from_secs(30))
                .retry_async_canceling(|retry_state| {
                    let initial_highest = Arc::clone(&initial_highest_base);
                    let copy_schema = copy_schema.clone();
                    let materialized_catalog_postgres_stash =materialized_catalog_postgres_stash.clone();
                    let materialized_data_path = materialized_data_path.clone();
                    // We would like to use a wrapper like state.with_catalog_copy, but we actually
                    // need the specific stash impl here because we have to access it via a
                    // collection, which means it's not object safe and so can't be a Box dyn. For
                    // now just duplicate the stash copy code until we do something like remove
                    // sqlite completely.
                    async move {
                        let bindings: Vec<(PartitionId, u64, MzOffset)> =
                            if let Some(url) = &materialized_catalog_postgres_stash {

                                let (client, connection) = tokio_postgres::connect(url, NoTls).await?;
                                mz_ore::task::spawn(|| "tokio-postgres testdrive connection", async move {
                                    if let Err(e) = connection.await {
                                        panic!("postgres stash connection error: {}", e);
                                    }
                                });

                                client
                                    .execute(format!("CREATE SCHEMA {copy_schema}").as_str(), &[])
                                    .await?;
                                client
                                    .execute(format!("SET search_path TO {copy_schema}").as_str(), &[])
                                    .await?;
                                client
                                    .batch_execute(
                                        "
                                            CREATE TABLE fence AS SELECT * FROM storage.fence;
                                            CREATE TABLE collections AS SELECT * FROM storage.collections;
                                            CREATE TABLE data AS SELECT * FROM storage.data;
                                            CREATE TABLE sinces AS SELECT * FROM storage.sinces;
                                            CREATE TABLE uppers AS SELECT * FROM storage.uppers;
                                        ",
                                    )
                                    .await?;

                                let tls = mz_postgres_util::make_tls(&tokio_postgres::Config::new()).unwrap();
                                let mut stash = mz_stash::Postgres::new(url.clone(), Some(copy_schema.clone()), tls).await?;
                                let collection = stash
                                    .collection::<PartitionId, ()>(&format!("timestamp-bindings-{item_id}"))
                                    .await?;
                                let bindings: Vec<(PartitionId, u64, MzOffset)> = stash
                                    .iter(collection)
                                    .await?
                                    .into_iter()
                                    .map(|((pid, _), ts, offset)| {
                                        (
                                            pid,
                                            ts.try_into().unwrap_or_else(|_| panic!()),
                                            MzOffset { offset },
                                        )
                                    })
                                    .collect();
                                client
                                    .execute(format!("DROP SCHEMA {copy_schema} CASCADE").as_str(), &[])
                                    .await?;
                                bindings
                            } else if let Some(path) = &materialized_data_path {
                                let temp_mzdata = catalog_copy(path)?;
                                let path = temp_mzdata.path();
                                let mut stash = mz_stash::Sqlite::open(path)?;
                                let collection = stash
                                    .collection::<PartitionId, ()>(&format!("timestamp-bindings-{item_id}"))
                                    .await?;
                                let bindings: Vec<(PartitionId, u64, MzOffset)> = stash
                                    .iter(collection)
                                    .await?
                                    .into_iter()
                                    .map(|((pid, _), ts, offset)| {
                                        (
                                            pid,
                                            ts.try_into().unwrap_or_else(|_| panic!()),
                                            MzOffset { offset },
                                        )
                                    })
                                    .collect();
                                bindings
                            } else {
                                unreachable!()
                            };

                        // We consider progress to be eventually compacting at least up to the original highest
                        // timestamp binding.
                        let lo_binding = bindings.iter().map(|(_, ts, _)| *ts).min();
                        let progress = if retry_state.i == 0 {
                            initial_highest.store(
                                bindings.iter().map(|(_, ts, _)| *ts).max().unwrap_or(u64::MIN),
                                Ordering::SeqCst,
                            );
                            false
                        } else {
                            self.permit_progress &&
                                (lo_binding.unwrap_or(u64::MAX) >= initial_highest.load(Ordering::SeqCst))
                        };

                        println!(
                            "Verifying timestamp binding compaction for {:?}.  Found {:?} vs expected {:?}.  Progress: {:?} vs {:?}",
                            self.source,
                            bindings.len(),
                            self.max_size,
                            lo_binding,
                            initial_highest.load(Ordering::SeqCst),
                        );

                        if bindings.is_empty() {
                            bail!("There are unexpectedly no bindings")
                        } else if bindings.len() <= self.max_size || progress {
                            Ok(())
                        } else {
                            bail!(
                                "There are {:?} bindings compared to max size {:?}",
                                bindings.len(),
                                self.max_size,
                            );
                        }
                    }
                }).await?;
            Ok(ControlFlow::Continue)
        }
    }
}
