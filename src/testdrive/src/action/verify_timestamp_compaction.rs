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

use async_trait::async_trait;

use coord::catalog::Catalog;
use coord::session::Session;
use ore::now::NOW_ZERO;
use ore::result::ResultExt;
use ore::retry::Retry;
use sql::catalog::SessionCatalog;
use sql::names::PartialName;

use crate::action::{Action, State};
use crate::parser::BuiltinCommand;

pub struct VerifyTimestampsAction {
    source: String,
    max_size: usize,
    permit_progress: bool,
}

pub fn build_verify_timestamp_compaction(
    mut cmd: BuiltinCommand,
) -> Result<VerifyTimestampsAction, String> {
    let source = cmd.args.string("source")?;
    let max_size = cmd
        .args
        .opt_string("max-size")
        .map(|s| s.parse::<usize>().expect("unable to parse usize"))
        .unwrap_or(3);
    let permit_progress = cmd
        .args
        .opt_bool("permit-progress")
        .expect("require valid bool if specified")
        .unwrap_or(false);
    cmd.args.done()?;
    Ok(VerifyTimestampsAction {
        source,
        max_size,
        permit_progress,
    })
}

#[async_trait]
impl Action for VerifyTimestampsAction {
    async fn undo(&self, _: &mut State) -> Result<(), String> {
        // Can't undo a verification.
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<(), String> {
        if let Some(path) = &state.materialized_catalog_path {
            let initial_highest_base = Arc::new(AtomicU64::new(u64::MAX));
            Retry::default()
                .initial_backoff(Duration::from_secs(1))
                .max_duration(Duration::from_secs(10))
                .retry(|retry_state| {
                    let initial_highest = initial_highest_base.clone();
                    async move {
                        let mut catalog = Catalog::open_debug(path, NOW_ZERO.clone())
                            .await
                            .map_err_to_string()?;
                        let item_id = catalog
                            .for_session(&Session::dummy())
                            .resolve_item(&PartialName {
                                database: None,
                                schema: None,
                                item: self.source.clone(),
                            })
                            .map_err_to_string()?
                            .id();
                        let bindings = catalog
                            .load_timestamp_bindings(item_id)
                            .map_err_to_string()?;

                        // We consider progress to be eventually compacting at least up to the original highest
                        // timestamp binding.
                        let progress = if retry_state.i == 0 {
                            initial_highest.store(
                                bindings.iter().map(|(_, ts, _)| ts).fold(u64::MIN, |a, &b| a.max(b)),
                                Ordering::SeqCst,
                            );
                            false
                        } else {
                            self.permit_progress &&
                                (bindings.iter().map(|(_, ts, _)| ts).fold(u64::MAX, |a, &b| a.min(b))
                                    >= initial_highest.load(Ordering::SeqCst))
                        };

                        println!(
                            "Verifying timestamp binding compaction for {:?}.  Found {:?} vs expected {:?}.  Progress: {:?}",
                            self.source,
                            bindings.len(),
                            self.max_size,
                            progress,
                        );

                        if bindings.len() < self.max_size || progress {
                            Ok(())
                        } else {
                            Err(format!(
                                "There are {:?} bindings compared to max size {:?}",
                                bindings.len(),
                                self.max_size,
                            ))
                        }
                    }
                }).await
        } else {
            println!(
                "Skipping timestamp binding compaction verification for {:?}.",
                self.source
            );
            Ok(())
        }
    }
}
