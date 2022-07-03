// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// NOTE(benesch): this is presently disabled because it needs to be rewritten
// for the persist-backed timestamp bindings.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::bail;
use async_trait::async_trait;

use mz_compute_client::sources::MzOffset;
use mz_expr::PartitionId;
use mz_ore::retry::Retry;
use mz_sql::catalog::SessionCatalog;
use mz_sql::names::PartialObjectName;
use mz_stash::Stash;

use crate::action::{Action, ControlFlow, State};
use crate::parser::BuiltinCommand;
use crate::util::mz_data::mzdata_copy;

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
        if item_id.is_none() || state.materialize_data_path.is_none() {
            println!(
                "Skipping timestamp binding compaction verification for {:?}.",
                self.source
            );
            Ok(ControlFlow::Continue)
        } else {
            // Unwrap is safe because this is known to be Some.
            let item_id = item_id.unwrap()?;
            let path = state.materialize_data_path.as_ref().unwrap();
            let temp_mzdata = mzdata_copy(path)?;
            let path = temp_mzdata.path();
            let initial_highest_base = Arc::new(AtomicU64::new(u64::MAX));
            Retry::default()
                .initial_backoff(Duration::from_secs(1))
                .max_duration(Duration::from_secs(30))
                .retry_async_canceling(|retry_state| {
                    let initial_highest = Arc::clone(&initial_highest_base);
                    async move {
                        let mut stash = mz_stash::Sqlite::open(&path.join("storage"))?;
                        let collection = stash
                            .collection::<PartitionId, ()>(&format!("timestamp-bindings-{item_id}")).await?;
                        let bindings: Vec<(PartitionId, u64, MzOffset)> = stash.iter(collection).await?
                            .into_iter()
                            .map(|((pid, _), ts, offset)| {
                                (
                                    pid,
                                    ts.try_into().unwrap_or_else(|_| panic!()),
                                    MzOffset { offset },
                                )
                            })
                            .collect();

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
