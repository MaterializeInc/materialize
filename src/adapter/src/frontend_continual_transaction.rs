// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Session-scoped repeated mutations over a common input snapshot.

use std::collections::BTreeMap;

use differential_dataflow::consolidation;
use itertools::Itertools;
use mz_adapter_types::connection::ConnectionId;
use mz_repr::{CatalogItemId, Diff, GlobalId, RelationDesc, Row, Timestamp};
use mz_sql::plan::{self, QueryWhen};
use mz_sql::session::metadata::SessionMetadata;
use timely::progress::Antichain;
use tokio_util::sync::CancellationToken;

use crate::active_compute_sink::ActiveSubscribeOwner;
use crate::command::Command;
use crate::coord::TargetCluster;
use crate::coord::appends::WriteResult;
use crate::coord::id_bundle::CollectionIdBundle;
use crate::coord::read_then_write::DependencyPolicy;
use crate::error::AdapterError;
use crate::frontend_read_then_write::{SubscribeHandle, validate_read_then_write};
use crate::optimize::Optimize;
use crate::session::Session;
use crate::{PeekClient, PeekResponseUnary};

#[derive(Debug)]
pub struct ContinualStatement {
    pub plan: plan::ReadThenWritePlan,
    pub target_global_id: GlobalId,
}

struct SubscribeState {
    handle: SubscribeHandle,
    target_id: CatalogItemId,
    target_global_id: GlobalId,
    table_desc: RelationDesc,
    // The consolidated historical prefix stays here so feedback can cancel it.
    all_diffs: Vec<(Row, Timestamp, Diff)>,
    current_upper: Option<Timestamp>,
    closed: bool,
}

impl PeekClient {
    pub(crate) async fn start_continual_transaction(
        &mut self,
        session: &Session,
        statements: Vec<ContinualStatement>,
        commit_mode: plan::ContinualCommitMode,
        cancel_token: CancellationToken,
    ) -> Result<(), AdapterError> {
        let catalog = self.catalog_snapshot("continual_transaction").await;
        let cluster_id = catalog.resolve_cluster(session.vars().cluster())?.id;
        let mut bundle = CollectionIdBundle::default();
        let mut prepared = Vec::new();
        for statement in &statements {
            let stmt = &statement.plan;
            if catalog
                .try_get_entry(&stmt.id)
                .map(|entry| entry.latest_global_id())
                != Some(statement.target_global_id)
            {
                return Err(AdapterError::ConcurrentDependencyMutation {
                    dependency_id: stmt.id.to_string(),
                });
            }
            let validated = validate_read_then_write(
                &catalog,
                session,
                stmt,
                TargetCluster::Active,
                DependencyPolicy::UserDml,
            )?;
            let (optimizer, mir) =
                self.optimize_mir_read_then_write(&catalog, session, stmt, cluster_id)?;
            bundle.extend(&mir.id_bundle(cluster_id));
            bundle.storage_ids.insert(statement.target_global_id);
            prepared.push((optimizer, mir, validated));
        }
        let timeline = catalog.validate_timeline_context(bundle.iter())?;
        let oracle_read_ts = self.oracle_read_ts(&timeline).await?;
        let (determination, read_holds) = self
            .determine_timestamp(
                session,
                &bundle,
                &QueryWhen::FreshestTableWrite,
                cluster_id,
                &timeline,
                oracle_read_ts,
                None,
            )
            .await?;
        let as_of = determination.timestamp_context.timestamp_or_default();
        self.ensure_read_linearized(&timeline, as_of).await?;
        let start_time = (self.statement_logging_frontend.now)();
        let mut subscribe_states = Vec::new();
        for (statement, (mut optimizer, mir, validated)) in statements.iter().zip_eq(prepared) {
            let stmt = &statement.plan;
            let lir = optimizer.catch_unwind_optimize(mir.resolve(Antichain::from_elem(as_of)))?;
            let sink_id = lir.sink_id();
            let (df, _) = lir.unapply();
            let arity = df
                .sink_exports
                .values()
                .next()
                .expect("has sink")
                .from_desc
                .arity();
            let handle = self
                .create_internal_subscribe(
                    Box::new(df),
                    cluster_id,
                    validated.replica_id,
                    validated.depends_on,
                    as_of,
                    arity,
                    sink_id,
                    ActiveSubscribeOwner::Background,
                    start_time,
                    read_holds.clone(),
                )
                .await?;
            subscribe_states.push(SubscribeState {
                handle,
                target_id: stmt.id,
                target_global_id: catalog.get_entry(&stmt.id).latest_global_id(),
                table_desc: validated.table_desc,
                all_diffs: Vec::new(),
                current_upper: None,
                closed: false,
            });
        }
        let conn_id = session.conn_id().clone();
        let mut client = self.clone_for_continual_transaction();
        mz_ore::task::spawn(|| "continual-transaction", async move {
            let run = client.run_continual_transaction(
                conn_id,
                as_of,
                subscribe_states,
                commit_mode,
                cancel_token.clone(),
            );
            let result = tokio::select! {
                _ = cancel_token.cancelled() => Ok(()),
                result = run => result,
            };
            if let Err(error) = result {
                tracing::warn!(%error, "continual transaction stopped");
            }
        });
        Ok(())
    }

    async fn run_continual_transaction(
        &mut self,
        conn_id: ConnectionId,
        as_of: Timestamp,
        mut subscribe_states: Vec<SubscribeState>,
        commit_mode: plan::ContinualCommitMode,
        cancel_token: CancellationToken,
    ) -> Result<(), AdapterError> {
        let mut last_commit_ts = as_of;
        let mut write_target = None;
        loop {
            let commit_ts = match write_target {
                Some(timestamp) => timestamp,
                None => {
                    let oracle = self
                        .ensure_oracle(mz_storage_types::sources::Timeline::EpochMilliseconds)
                        .await?;
                    let timestamp = oracle.read_ts().await.step_forward();
                    if timestamp <= last_commit_ts {
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                        continue;
                    }
                    write_target = Some(timestamp);
                    timestamp
                }
            };
            for state in &mut subscribe_states {
                for _ in 0..128 {
                    match state.handle.try_recv() {
                        Ok(message) => process_subscribe_message(state, message)?,
                        Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                        Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                            state.closed = true;
                            state.current_upper = Some(Timestamp::MAX);
                            break;
                        }
                    }
                }
            }
            // A retry may already be certified by every input, including a sealed
            // input that will never send another progress message.
            if !subscribe_states
                .iter()
                .all(|state| state.current_upper.is_some_and(|upper| upper >= commit_ts))
            {
                if let Some((index, message)) = recv_from_any(&mut subscribe_states).await {
                    let state = &mut subscribe_states[index];
                    if let Some(message) = message {
                        process_subscribe_message(state, message)?;
                    } else {
                        state.closed = true;
                        state.current_upper = Some(Timestamp::MAX);
                    }
                }
                continue;
            }

            let mut writes: BTreeMap<CatalogItemId, (GlobalId, Vec<(Row, Diff)>)> = BTreeMap::new();

            for sub in &mut subscribe_states {
                let mut filtered: Vec<(Row, Timestamp, Diff)> = sub
                    .all_diffs
                    .iter()
                    .filter(|(_, ts, _)| *ts < commit_ts)
                    .cloned()
                    .collect();

                for (_, ts, _) in &mut filtered {
                    *ts = Timestamp::MIN;
                }
                consolidation::consolidate_updates(&mut filtered);

                sub.all_diffs.retain(|(_, ts, _)| *ts >= commit_ts);
                sub.all_diffs.extend(filtered.iter().cloned());
                if !filtered.is_empty() {
                    let diffs: Vec<(Row, Diff)> =
                        filtered.iter().map(|(r, _, d)| (r.clone(), *d)).collect();
                    writes
                        .entry(sub.target_id)
                        .or_insert_with(|| (sub.target_global_id, Vec::new()))
                        .1
                        .extend(diffs);
                }
            }

            if writes.is_empty() {
                last_commit_ts = commit_ts;
                write_target = None;
                continue;
            }

            tracing::debug!(
                commit_ts = %commit_ts,
                last_commit_ts = ?last_commit_ts,
                num_tables = writes.len(),
                "continual transaction: committing"
            );

            let result = self
                .call_coordinator(|tx| Command::AttemptMultiTableTimestampedWrite {
                    conn_id: conn_id.clone(),
                    writes,
                    write_ts: commit_ts,
                    tx,
                })
                .await?;

            match result {
                WriteResult::Success { .. } => {
                    tracing::debug!(
                        commit_ts = %commit_ts,
                        "continual transaction write succeeded"
                    );
                    last_commit_ts = commit_ts;
                    write_target = None;
                }
                WriteResult::TimestampPassed {
                    target_timestamp,
                    next_eligible_timestamp,
                } => {
                    tracing::debug!(
                        commit_ts = %commit_ts,
                        target_timestamp = %target_timestamp,
                        current_write_ts = %next_eligible_timestamp,
                        "continual transaction write conflict (TimestampPassed), \
                         subscribe frontier is behind write frontier"
                    );
                    write_target = Some(next_eligible_timestamp);
                    continue;
                }
                WriteResult::Canceled => return Err(AdapterError::Canceled),
                WriteResult::ReadOnly => return Err(AdapterError::ReadOnly),
                other => {
                    return Err(AdapterError::Internal(format!(
                        "continual write failed: {other:?}"
                    )));
                }
            }

            match &commit_mode {
                plan::ContinualCommitMode::Every(duration) => {
                    tokio::select! {
                        _ = cancel_token.cancelled() => {
                            return Ok(());
                        }
                        _ = tokio::time::sleep(*duration) => {}
                    }
                }
                plan::ContinualCommitMode::Asap => {}
            }
        }
    }
}

async fn recv_from_any(
    states: &mut [SubscribeState],
) -> Option<(usize, Option<PeekResponseUnary>)> {
    let receivers: Vec<_> = states
        .iter_mut()
        .enumerate()
        .filter(|(_, state)| !state.closed)
        .map(|(index, state)| Box::pin(async move { (index, state.handle.recv().await) }))
        .collect();
    if receivers.is_empty() {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        None
    } else {
        Some(futures::future::select_all(receivers).await.0)
    }
}

fn process_subscribe_message(
    state: &mut SubscribeState,
    response: PeekResponseUnary,
) -> Result<(), AdapterError> {
    match response {
        PeekResponseUnary::Rows(mut rows) => {
            while let Some(row) = rows.next() {
                let mut datums = row.iter();

                let ts_datum = datums.next().expect("missing mz_timestamp");
                let ts = match ts_datum {
                    mz_repr::Datum::Numeric(n) => match n.0.try_into() {
                        Ok(ts_u64) => Timestamp::new(ts_u64),
                        Err(_) => {
                            return Err(AdapterError::Internal(format!(
                                "mz_timestamp is not a valid u64: {n}"
                            )));
                        }
                    },
                    other => {
                        return Err(AdapterError::Internal(format!(
                            "unexpected mz_timestamp datum: {other:?}"
                        )));
                    }
                };

                let progressed_datum = datums.next().expect("missing mz_progressed");
                let is_progress = matches!(progressed_datum, mz_repr::Datum::True);

                if is_progress {
                    state.current_upper = Some(ts);
                } else {
                    let diff_datum = datums.next().expect("missing mz_diff");
                    let diff = match diff_datum {
                        mz_repr::Datum::Int64(d) => Diff::from(d),
                        _ => continue,
                    };

                    let data_row = Row::pack(datums);
                    if diff.is_positive() {
                        for (idx, datum) in data_row.iter().enumerate() {
                            state.table_desc.constraints_met(idx, &datum)?;
                        }
                    }
                    state.all_diffs.push((data_row, ts, diff));
                }
            }
            Ok(())
        }
        PeekResponseUnary::Error(e) => Err(e),
        PeekResponseUnary::DependencyDropped(dep) => Err(dep.to_concurrent_dependency_drop()),
        PeekResponseUnary::Canceled => Err(AdapterError::Canceled),
    }
}
