// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Renders the table snapshot side of the [`MySqlSourceConnection`] dataflow.
//!
//! # Snapshot reading
//!
//! Depending on the `source_outputs resume_upper` parameters this dataflow decides which tables to
//! snapshot and performs a simple `SELECT * FROM table` on them in order to get a snapshot.
//! There are a few subtle points about this operation, described below.
//!
//! It is crucial for correctness that we always perform the snapshot of all tables at a specific
//! point in time. This must be true even in the presence of restarts or partially committed
//! snapshots. The consistent point that the snapshot must happen at is discovered and durably
//! recorded during planning of the source and is exposed to this ingestion dataflow via the
//! `initial_gtid_set` field in `MySqlSourceDetails`.
//!
//! Unfortunately MySQL does not provide an API to perform a transaction at a specific point in
//! time. Instead, MySQL allows us to perform a snapshot of a table and let us know at which point
//! in time the snapshot was taken. Using this information we can take a snapshot at an arbitrary
//! point in time and then rewind it to the desired `initial_gtid_set` by "rewinding" it. These two
//! phases are described in the following section.
//!
//! ## Producing a snapshot at a known point in time.
//!
//! A designated leader worker acquires table locks on ALL tables that need snapshotting, reads the
//! current GTID frontier, discovers PK bounds for parallel range splitting, and broadcasts a
//! `SnapshotInfo` to all workers via a timely feedback loop. All workers then start CONSISTENT
//! SNAPSHOT transactions while the leader holds the locks. Workers signal completion by dropping
//! their snapshot capability. The leader drains the feedback input to confirm all workers have
//! started, then unlocks the tables.
//!
//! ## Parallel PK-range snapshots
//!
//! For tables with a single-column integer primary key, the leader queries `MIN(pk)` and `MAX(pk)`
//! and broadcasts the bounds. Each worker computes its assigned PK range and reads only that
//! portion of the table. Tables without a suitable PK fall back to single-worker-per-table mode.
//!
//! ## Rewinding the snapshot to a specific point in time.
//!
//! Having obtained a snapshot of a table at some `snapshot_upper` we are now tasked with
//! transforming this snapshot into one at `initial_gtid_set`. In other words we have produced a
//! snapshot containing all updates that happened at `t: !(snapshot_upper <= t)` but what we
//! actually want is a snapshot containing all updates that happened at `t: !(initial_gtid <= t)`.
//!
//! If we assume that `initial_gtid_set <= snapshot_upper`, which is a fair assumption since the
//! former is obtained before the latter, then we can observe that the snapshot we produced
//! contains all updates at `t: !(initial_gtid <= t)` (i.e the snapshot we want) and some additional
//! unwanted updates at `t: initial_gtid <= t && !(snapshot_upper <= t)`. We happen to know exactly
//! what those additional unwanted updates are because those will be obtained by reading the
//! replication stream in the replication operator and so all we need to do to "rewind" our
//! `snapshot_upper` snapshot to `initial_gtid` is to ask the replication operator to "undo" any
//! updates that falls in the undesirable region.
//!
//! This is exactly what `RewindRequest` is about. It informs the replication operator that a
//! particular table has been snapshotted at `snapshot_upper` and would like all the updates
//! discovered during replication that happen at `t: initial_gtid <= t && !(snapshot_upper <= t)`.
//! to be cancelled. In Differential Dataflow this is as simple as flipping the sign of the diff
//! field.
//!
//! The snapshot reader emits updates at the minimum timestamp (by convention) to allow the
//! updates to be potentially negated by the replication operator, which will emit negated
//! updates at the minimum timestamp (by convention) when it encounters rows from a table that
//! occur before the GTID frontier in the Rewind Request for that table.
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::sync::Arc;

use differential_dataflow::AsCollection;
use futures::{StreamExt as _, TryStreamExt};
use itertools::Itertools;
use mysql_async::prelude::Queryable;
use mysql_async::{IsolationLevel, Row as MySqlRow, TxOpts};
use mz_mysql_util::{
    ER_NO_SUCH_TABLE, MySqlError, pack_mysql_row, query_sys_var, quote_identifier,
};
use mz_ore::cast::CastFrom;
use mz_ore::future::InTask;
use mz_ore::iter::IteratorExt;
use mz_ore::metrics::MetricsFutureExt;
use mz_repr::{Diff, Row, SqlScalarType};
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sources::MySqlSourceConnection;
use mz_storage_types::sources::mysql::{GtidPartition, gtid_set_frontier};
use mz_timely_util::antichain::AntichainExt;
use mz_timely_util::builder_async::{
    Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};
use mz_timely_util::containers::stack::AccountedStackBuilder;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::core::Map;
use timely::dataflow::operators::vec::Broadcast;
use timely::dataflow::operators::{CapabilitySet, Concat, ConnectLoop, Feedback};
use timely::dataflow::{Scope, StreamVec};
use timely::progress::Timestamp;
use tracing::trace;

use crate::metrics::source::mysql::MySqlSnapshotMetrics;
use crate::source::RawSourceCreationConfig;
use crate::source::types::{SignaledFuture, SourceMessage, StackedCollection};
use crate::statistics::SourceStatistics;

use super::schemas::verify_schemas;
use super::{
    DefiniteError, MySqlTableName, ReplicationError, RewindRequest, SourceOutputInfo,
    TransientError, return_definite_error, validate_mysql_repl_settings,
};

/// If `desc` has a single-column integer PK, return the column name.
fn integer_pk_col(desc: &mz_mysql_util::MySqlTableDesc) -> Option<&str> {
    let pk = desc.keys.iter().find(|k| k.is_primary)?;
    if pk.columns.len() != 1 {
        return None;
    }
    let pk_col_name = &pk.columns[0];
    let col = desc.columns.iter().find(|c| &c.name == pk_col_name)?;
    let scalar_type = &col.column_type.as_ref()?.scalar_type;
    match scalar_type {
        SqlScalarType::Int16
        | SqlScalarType::Int32
        | SqlScalarType::Int64
        | SqlScalarType::UInt16
        | SqlScalarType::UInt32 => Some(pk_col_name.as_str()),
        // Exclude UInt64 since MIN/MAX are queried as i64
        _ => None,
    }
}

/// PK bounds for a table, discovered by the leader.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PkBounds {
    pk_col: String,
    min_val: i64,
    max_val: i64,
}

/// Snapshot info broadcast from leader to all workers.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct SnapshotInfo {
    gtid_set: String,
    /// PK bounds per table. None = no integer PK, use single-worker fallback.
    pk_bounds: BTreeMap<MySqlTableName, Option<PkBounds>>,
}

/// A worker's assigned PK range for a table.
struct PkRange {
    pk_col: String,
    lower: i64,
    upper: Option<i64>, // None = open-ended (last worker)
}

/// Compute this worker's PK range. Returns None if this worker has no work.
///
/// Uses `i128` intermediate arithmetic to avoid overflow when the PK range
/// spans a large portion of the `i64` domain (e.g. `min_val = 0, max_val = 2^62`).
fn worker_pk_range(bounds: &PkBounds, worker_id: usize, worker_count: usize) -> Option<PkRange> {
    // Use i128 throughout to avoid overflow on large Int64 PK ranges.
    let min = i128::from(bounds.min_val);
    let max = i128::from(bounds.max_val);
    let range_size = max - min + 1;
    if range_size <= 0 {
        return None;
    }
    let effective = std::cmp::min(i128::cast_from(worker_count), range_size);
    if i128::cast_from(worker_id) >= effective {
        return None;
    }
    let wid = i128::cast_from(worker_id);
    let start_128 = min + wid * range_size / effective;
    let start = i64::try_from(start_128).expect("PK range start fits in i64");
    let is_last = wid == effective - 1;
    if is_last {
        Some(PkRange {
            pk_col: bounds.pk_col.clone(),
            lower: start,
            upper: None,
        })
    } else {
        let end_128 = min + (wid + 1) * range_size / effective;
        let end = i64::try_from(end_128).expect("PK range end fits in i64");
        Some(PkRange {
            pk_col: bounds.pk_col.clone(),
            lower: start,
            upper: Some(end),
        })
    }
}

/// Error from the snapshot leader's lock-acquisition / GTID-reading phase.
/// Allows the leader to broadcast a failure sentinel before returning, so
/// that non-leader workers do not deadlock waiting for `SnapshotInfo`.
enum LeaderError {
    /// A definite error (e.g. table dropped) — emitted for every output.
    Definite(DefiniteError),
    /// A transient error — causes the source to restart.
    Transient(TransientError),
}

impl From<mysql_async::Error> for LeaderError {
    fn from(e: mysql_async::Error) -> Self {
        LeaderError::Transient(e.into())
    }
}

impl From<MySqlError> for LeaderError {
    fn from(e: MySqlError) -> Self {
        LeaderError::Transient(e.into())
    }
}

impl From<anyhow::Error> for LeaderError {
    fn from(e: anyhow::Error) -> Self {
        LeaderError::Transient(e.into())
    }
}

/// Renders the snapshot dataflow. See the module documentation for more information.
pub(crate) fn render<'scope>(
    scope: Scope<'scope, GtidPartition>,
    config: RawSourceCreationConfig,
    connection: MySqlSourceConnection,
    source_outputs: Vec<SourceOutputInfo>,
    metrics: MySqlSnapshotMetrics,
) -> (
    StackedCollection<'scope, GtidPartition, (usize, Result<SourceMessage, DataflowError>)>,
    StreamVec<'scope, GtidPartition, RewindRequest>,
    StreamVec<'scope, GtidPartition, ReplicationError>,
    PressOnDropButton,
) {
    let mut builder =
        AsyncOperatorBuilder::new(format!("MySqlSnapshotReader({})", config.id), scope.clone());

    let (feedback_handle, feedback_data) = scope.feedback(Default::default());

    let (raw_handle, raw_data) = builder.new_output::<AccountedStackBuilder<_>>();
    let (rewinds_handle, rewinds) = builder.new_output::<CapacityContainerBuilder<Vec<_>>>();
    // Captures DefiniteErrors that affect the entire source, including all outputs
    let (definite_error_handle, definite_errors) =
        builder.new_output::<CapacityContainerBuilder<Vec<_>>>();
    let (snapshot_handle, snapshot) = builder.new_output::<CapacityContainerBuilder<Vec<_>>>();

    // This operator needs to broadcast data to itself in order to synchronize the transaction
    // snapshot. However, none of the feedback capabilities result in output messages and for the
    // feedback edge specifically having a default connection would result in a loop.
    let mut snapshot_input = builder.new_disconnected_input(feedback_data, Pipeline);

    // The snapshot info must be sent to all workers, so we broadcast the feedback connection
    snapshot.broadcast().connect_loop(feedback_handle);

    let is_snapshot_leader = config.responsible_for("mysql_snapshot_leader");

    // A global view of all outputs that will be snapshot by all workers.
    let mut all_outputs = vec![];
    // ALL workers get ALL tables that need snapshotting (for parallel PK-range reads).
    let mut tables_to_snapshot: BTreeMap<MySqlTableName, Vec<SourceOutputInfo>> = BTreeMap::new();
    // Maps MySQL table name to export `SourceStatistics`.
    // Every worker keeps handles so `snapshot_records_staged` can sum local contributions.
    let mut export_statistics: BTreeMap<MySqlTableName, Vec<SourceStatistics>> = BTreeMap::new();
    for output in source_outputs.into_iter() {
        // Determine which outputs need to be snapshot and which already have been.
        if *output.resume_upper != [GtidPartition::minimum()] {
            // Already has been snapshotted.
            continue;
        }
        all_outputs.push(output.output_index);
        let export_stats = config
            .statistics
            .get(&output.export_id)
            .expect("statistics have been intialized")
            .clone();
        export_statistics
            .entry(output.table_name.clone())
            .or_insert_with(Vec::new)
            .push(export_stats);
        tables_to_snapshot
            .entry(output.table_name.clone())
            .or_insert_with(Vec::new)
            .push(output);
    }

    let (button, transient_errors): (_, StreamVec<'scope, GtidPartition, Rc<TransientError>>) =
        builder.build_fallible(move |caps| {
            let busy_signal = Arc::clone(&config.busy_signal);
            Box::pin(SignaledFuture::new(busy_signal, async move {
                let [
                    data_cap_set,
                    rewind_cap_set,
                    definite_error_cap_set,
                    snapshot_cap_set,
                ]: &mut [_; 4] = caps.try_into().unwrap();

                let id = config.id;
                let worker_id = config.worker_id;

                // Initialize statistics for all exports (required even
                // if this worker won't snapshot anything).
                if !all_outputs.is_empty() {
                    for statistics in config.statistics.values() {
                        statistics.set_snapshot_records_known(0);
                        statistics.set_snapshot_records_staged(0);
                    }
                }

                if tables_to_snapshot.is_empty() {
                    trace!(%id, "timely-{worker_id} no tables to snapshot, exiting");
                    return Ok(());
                }

                trace!(%id, "timely-{worker_id} snapshotting {} tables",
                       tables_to_snapshot.len());

                let connection_config = connection
                    .connection
                    .config(
                        &config.config.connection_context.secrets_reader,
                        &config.config,
                        InTask::Yes,
                    )
                    .await?;
                let task_name = format!("timely-{worker_id} MySQL snapshotter");

                // Phase B: Leader acquires locks, reads GTID, queries PK bounds, broadcasts.
                //
                // All fallible leader work is wrapped in an inner async block so that
                // we ALWAYS broadcast a result (success or failure) before returning.
                // Without this, a leader error would drop `snapshot_cap_set` without
                // broadcasting, causing non-leader workers to deadlock waiting for
                // `SnapshotInfo` on the feedback loop.
                let mut lock_conn = if is_snapshot_leader {
                    let leader_result: Result<_, LeaderError> = async {
                        let lock_clauses = tables_to_snapshot
                            .keys()
                            .map(|t| format!("{} READ", t))
                            .collect::<Vec<String>>()
                            .join(", ");
                        let mut lock_conn = connection_config
                            .connect(
                                &task_name,
                                &config.config.connection_context.ssh_tunnel_manager,
                            )
                            .await?;
                        if let Some(timeout) = config
                            .config
                            .parameters
                            .mysql_source_timeouts
                            .snapshot_lock_wait_timeout
                        {
                            lock_conn
                                .query_drop(format!(
                                    "SET @@session.lock_wait_timeout = {}",
                                    timeout.as_secs()
                                ))
                                .await?;
                        }

                        trace!(%id, "timely-{worker_id} acquiring table locks: {lock_clauses}");
                        match lock_conn
                            .query_drop(format!("LOCK TABLES {lock_clauses}"))
                            .await
                        {
                            Err(mysql_async::Error::Server(mysql_async::ServerError {
                                code,
                                message,
                                ..
                            })) if code == ER_NO_SUCH_TABLE => {
                                trace!(%id, "timely-{worker_id} received unknown table error from \
                                             lock query");
                                return Err(LeaderError::Definite(DefiniteError::TableDropped(
                                    message,
                                )));
                            }
                            e => e?,
                        };

                        // Record the frontier of future GTIDs based on the executed GTID set
                        // at the start of the snapshot
                        let snapshot_gtid_set =
                            query_sys_var(&mut lock_conn, "global.gtid_executed").await?;

                        trace!(%id, "timely-{worker_id} acquired table locks");

                        // Query PK bounds for each table
                        let mut pk_bounds_map = BTreeMap::new();
                        for (table, outputs) in &tables_to_snapshot {
                            let desc = &outputs[0].desc;
                            if let Some(pk_col_name) = integer_pk_col(desc) {
                                let quoted = quote_identifier(pk_col_name);
                                let query = format!(
                                    "SELECT MIN({}) AS pk_min, MAX({}) AS pk_max FROM {}",
                                    quoted, quoted, table
                                );
                                let row: Option<(Option<i64>, Option<i64>)> =
                                    lock_conn.query_first(query).await?;
                                match row {
                                    Some((Some(min_val), Some(max_val)))
                                        if i128::from(max_val) - i128::from(min_val) + 1
                                            >= i128::cast_from(config.worker_count) =>
                                    {
                                        pk_bounds_map.insert(
                                            table.clone(),
                                            Some(PkBounds {
                                                pk_col: quoted,
                                                min_val,
                                                max_val,
                                            }),
                                        );
                                    }
                                    _ => {
                                        // Table is empty, too small to
                                        // parallelize, or has NULL PKs.
                                        pk_bounds_map.insert(table.clone(), None);
                                    }
                                }
                            } else {
                                pk_bounds_map.insert(table.clone(), None);
                            }
                        }

                        let snapshot_info = SnapshotInfo {
                            gtid_set: snapshot_gtid_set,
                            pk_bounds: pk_bounds_map,
                        };
                        Ok((snapshot_info, lock_conn))
                    }
                    .await;

                    match leader_result {
                        Ok((info, conn)) => {
                            trace!(%id, "timely-{worker_id} broadcasting snapshot info: {info:?}");
                            snapshot_handle.give(&snapshot_cap_set[0], Some(info));
                            Some(conn)
                        }
                        Err(err) => {
                            // CRITICAL: broadcast None so non-leaders exit cleanly
                            // instead of deadlocking on the feedback loop.
                            trace!(%id, "timely-{worker_id} leader failed, broadcasting \
                                         error sentinel");
                            snapshot_handle.give(&snapshot_cap_set[0], None);
                            match err {
                                LeaderError::Definite(e) => {
                                    return Ok(return_definite_error(
                                        e,
                                        &all_outputs,
                                        &raw_handle,
                                        data_cap_set,
                                        &definite_error_handle,
                                        definite_error_cap_set,
                                    )
                                    .await);
                                }
                                LeaderError::Transient(e) => {
                                    return Err(e);
                                }
                            }
                        }
                    }
                } else {
                    None
                };

                // Phase C: All workers receive broadcast.
                // The payload is `Option<SnapshotInfo>`: `Some` on success,
                // `None` if the leader encountered an error.
                let snapshot_info: Option<SnapshotInfo> = 'recv: loop {
                    match snapshot_input.next().await {
                        Some(AsyncEvent::Data(_, mut data)) => {
                            if let Some(msg) = data.pop() {
                                break 'recv msg;
                            }
                        }
                        Some(AsyncEvent::Progress(_)) => continue,
                        None => {
                            // Feedback stream closed without data — the leader
                            // must have failed. Return cleanly; the leader's
                            // operator instance handles error propagation.
                            break 'recv None;
                        }
                    }
                };
                let snapshot_info = match snapshot_info {
                    Some(info) => info,
                    None => {
                        // Leader signaled failure. Bail out — errors are
                        // already propagated by the leader's worker.
                        return Ok(());
                    }
                };

                // Parse GTID frontier from snapshot_info.gtid_set
                let snapshot_gtid_frontier = match gtid_set_frontier(&snapshot_info.gtid_set) {
                    Ok(frontier) => frontier,
                    Err(err) => {
                        let err = DefiniteError::UnsupportedGtidState(err.to_string());
                        return Ok(return_definite_error(
                            err,
                            &all_outputs,
                            &raw_handle,
                            data_cap_set,
                            &definite_error_handle,
                            definite_error_cap_set,
                        )
                        .await);
                    }
                };

                trace!(%id, "timely-{worker_id} received snapshot info at: {}",
                       snapshot_gtid_frontier.pretty());

                // Precompute each table's read plan for this worker.
                // Tables with PK bounds get a range; others use
                // single-worker fallback via responsible_for.
                let table_ranges: BTreeMap<_, _> = tables_to_snapshot
                    .keys()
                    .filter_map(|table| {
                        let range = match snapshot_info.pk_bounds.get(table) {
                            Some(Some(bounds)) => {
                                worker_pk_range(bounds, config.worker_id, config.worker_count)
                            }
                            _ => None,
                        };
                        let should_read = range.is_some() || config.responsible_for(table);
                        should_read.then(|| (table.clone(), range))
                    })
                    .collect();
                let has_work = !table_ranges.is_empty();

                // Non-leader workers that have nothing to read skip
                // connecting — avoids exhausting MySQL's connection
                // pool in many-sources scenarios (limits test).
                let mut conn = if is_snapshot_leader || has_work {
                    let mut c = connection_config
                        .connect(
                            &task_name,
                            &config.config.connection_context.ssh_tunnel_manager,
                        )
                        .await?;
                    match validate_mysql_repl_settings(&mut c).await {
                        Err(err @ MySqlError::InvalidSystemSetting { .. }) => {
                            return Ok(return_definite_error(
                                DefiniteError::ServerConfigurationError(err.to_string()),
                                &all_outputs,
                                &raw_handle,
                                data_cap_set,
                                &definite_error_handle,
                                definite_error_cap_set,
                            )
                            .await);
                        }
                        Err(err) => Err(err)?,
                        Ok(()) => (),
                    };
                    Some(c)
                } else {
                    trace!(%id, "timely-{worker_id} has no tables to read, \
                                 skipping MySQL connection");
                    None
                };

                let mut tx = if let Some(ref mut conn) = conn {
                    trace!(%id, "timely-{worker_id} starting transaction with \
                                 consistent snapshot at: {}", snapshot_gtid_frontier.pretty());
                    let mut tx_opts = TxOpts::default();
                    tx_opts
                        .with_isolation_level(IsolationLevel::RepeatableRead)
                        .with_consistent_snapshot(true)
                        .with_readonly(true);
                    let mut tx = conn.start_transaction(tx_opts).await?;
                    tx.query_drop("set @@session.time_zone = '+00:00'").await?;
                    if let Some(timeout) = config
                        .config
                        .parameters
                        .mysql_source_timeouts
                        .snapshot_max_execution_time
                    {
                        tx.query_drop(format!(
                            "SET @@session.max_execution_time = {}",
                            timeout.as_millis()
                        ))
                        .await?;
                    }
                    Some(tx)
                } else {
                    None
                };

                // Phase E: All workers signal, leader unlocks
                *snapshot_cap_set = CapabilitySet::new();
                if is_snapshot_leader {
                    while snapshot_input.next().await.is_some() {}
                    if let Some(mut lc) = lock_conn.take() {
                        lc.query_drop("UNLOCK TABLES").await?;
                        lc.disconnect().await?;
                    }
                }
                drop(lock_conn);

                trace!(%id, "timely-{worker_id} started transaction (has_work={has_work})");

                // Workers without a transaction have nothing to read.
                let Some(ref mut tx) = tx else {
                    return Ok(());
                };

                // Phase F: Verify schemas — only for tables this worker reads,
                // to avoid redundant queries across workers.
                let errored_outputs = verify_schemas(
                    tx,
                    tables_to_snapshot
                        .iter()
                        .filter(|(t, _)| table_ranges.contains_key(t))
                        .map(|(k, v)| (k, v.as_slice()))
                        .collect(),
                )
                .await?;
                let mut removed_outputs = BTreeSet::new();
                for (output, err) in errored_outputs {
                    raw_handle
                        .give_fueled(
                            &data_cap_set[0],
                            (
                                (output.output_index, Err(err.clone().into())),
                                GtidPartition::minimum(),
                                Diff::ONE,
                            ),
                        )
                        .await;
                    tracing::warn!(%id, "timely-{worker_id} stopping snapshot of output {output:?} \
                                due to schema mismatch");
                    removed_outputs.insert(output.output_index);
                }
                for (_, outputs) in tables_to_snapshot.iter_mut() {
                    outputs.retain(|output| !removed_outputs.contains(&output.output_index));
                }
                tables_to_snapshot.retain(|_, outputs| !outputs.is_empty());

                // Phase G: The leader publishes the full snapshot size so the summed
                // worker-local gauges reflect the upstream total without double-counting.
                if is_snapshot_leader {
                    fetch_snapshot_size(
                        tx,
                        tables_to_snapshot
                            .iter()
                            .map(|(name, outputs)| {
                                let stats = export_statistics
                                    .get(name)
                                    .expect("statistics are initialized for each output");
                                (name.clone(), outputs.len(), stats)
                            })
                            .collect(),
                        metrics,
                    )
                    .await?;
                }

                if tables_to_snapshot.is_empty() {
                    return Ok(());
                }

                // Phase H: Read snapshot data
                let mut final_row = Row::default();

                // Yield more frequently when multiple workers are
                // active to keep total in-flight memory bounded.
                // ~130 bytes/row × 10K rows ≈ 1.3 MiB per yield.
                let yield_interval = 10_000 / u64::cast_from(config.worker_count).max(1);

                let mut snapshot_staged_total = 0;
                for (table, outputs) in &tables_to_snapshot {
                    let pk_range = match table_ranges.get(table) {
                        Some(range) => range.as_ref(),
                        None => continue, // This worker has no work for this table
                    };

                    let mut snapshot_staged = 0;
                    let query = build_snapshot_query(outputs, pk_range);
                    trace!(%id, "timely-{worker_id} reading snapshot query='{}'", query);
                    let mut results = tx.exec_stream(query, ()).await?;
                    while let Some(row) = results.try_next().await? {
                        let row: MySqlRow = row;
                        snapshot_staged += 1;
                        for (output, row_val) in outputs.iter().repeat_clone(row) {
                            let event = match pack_mysql_row(&mut final_row, row_val, &output.desc)
                            {
                                Ok(row) => Ok(SourceMessage {
                                    key: Row::default(),
                                    value: row,
                                    metadata: Row::default(),
                                }),
                                // Produce a DefiniteError in the stream for any rows that fail to decode
                                Err(err @ MySqlError::ValueDecodeError { .. }) => {
                                    Err(DataflowError::from(DefiniteError::ValueDecodeError(
                                        err.to_string(),
                                    )))
                                }
                                Err(err) => Err(err)?,
                            };
                            raw_handle
                                .give_fueled(
                                    &data_cap_set[0],
                                    (
                                        (output.output_index, event),
                                        GtidPartition::minimum(),
                                        Diff::ONE,
                                    ),
                                )
                                .await;
                        }
                        snapshot_staged_total += u64::cast_from(outputs.len());
                        if snapshot_staged_total % yield_interval == 0 {
                            tokio::task::yield_now().await;
                        }
                        if snapshot_staged_total % 1000 == 0 {
                            if let Some(stats_list) = export_statistics.get(table) {
                                for statistics in stats_list {
                                    statistics.set_snapshot_records_staged(snapshot_staged);
                                }
                            }
                        }
                    }
                    if let Some(stats_list) = export_statistics.get(table) {
                        for statistics in stats_list {
                            statistics.set_snapshot_records_staged(snapshot_staged);
                        }
                    }
                    trace!(%id, "timely-{worker_id} snapshotted {} records from \
                                 table '{table}'", snapshot_staged * u64::cast_from(outputs.len()));
                }

                // Phase I: Emit rewind requests
                // We are done with the snapshot so now we will emit rewind requests. It is
                // important that this happens after the snapshot has finished because this is what
                // unblocks the replication operator and we want this to happen serially.
                //
                // Only the responsible worker emits rewind requests
                // for each table. This worker is guaranteed to have
                // a transaction (responsible_for → has_work = true).
                for (table, outputs) in &tables_to_snapshot {
                    if !config.responsible_for(table) {
                        continue;
                    }
                    for output in outputs {
                        trace!(%id, "timely-{worker_id} producing rewind request for {table}\
                                     output {}", output.output_index);
                        let req = RewindRequest {
                            output_index: output.output_index,
                            snapshot_upper: snapshot_gtid_frontier.clone(),
                        };
                        rewinds_handle.give(&rewind_cap_set[0], req);
                    }
                }
                *rewind_cap_set = CapabilitySet::new();

                Ok(())
            }))
        });

    // TODO: Split row decoding into a separate operator that can be distributed across all workers

    let errors = definite_errors.concat(transient_errors.map(ReplicationError::from));

    (
        raw_data.as_collection(),
        rewinds,
        errors,
        button.press_on_drop(),
    )
}

/// Fetch the size of the snapshot on this worker and emits the appropriate emtrics and statistics
/// for each table.
async fn fetch_snapshot_size<Q>(
    conn: &mut Q,
    tables: Vec<(MySqlTableName, usize, &Vec<SourceStatistics>)>,
    metrics: MySqlSnapshotMetrics,
) -> Result<u64, anyhow::Error>
where
    Q: Queryable,
{
    let mut total = 0;
    for (table, num_outputs, export_statistics) in tables {
        let stats = collect_table_statistics(conn, &table).await?;
        metrics.record_table_count_latency(table.1, table.0, stats.count_latency);
        for export_stat in export_statistics {
            export_stat.set_snapshot_records_known(stats.count);
            export_stat.set_snapshot_records_staged(0);
        }
        total += stats.count * u64::cast_from(num_outputs);
    }
    Ok(total)
}

/// Builds the SQL query to be used for creating the snapshot using the first entry in outputs.
///
/// Expect `outputs` to contain entries for a single table, and to have at least 1 entry.
/// Expect that each MySqlTableDesc entry contains all columns described in information_schema.columns.
///
/// When `pk_range` is provided, a WHERE clause is appended to filter rows by PK range.
#[must_use]
fn build_snapshot_query(outputs: &[SourceOutputInfo], pk_range: Option<&PkRange>) -> String {
    let info = outputs.first().expect("MySQL table info");
    for output in &outputs[1..] {
        // the columns are decoded solely based on position, so we just need to ensure that
        // all columns are accounted for.
        assert!(
            info.desc.columns.len() == output.desc.columns.len(),
            "Mismatch in table descriptions for {}",
            info.table_name
        );
    }
    let columns = info
        .desc
        .columns
        .iter()
        .map(|col| quote_identifier(&col.name))
        .join(", ");
    let mut query = format!("SELECT {} FROM {}", columns, info.table_name);
    if let Some(range) = pk_range {
        query.push_str(&format!(" WHERE {} >= {}", range.pk_col, range.lower));
        if let Some(upper) = range.upper {
            query.push_str(&format!(" AND {} < {}", range.pk_col, upper));
        }
    }
    query
}

#[derive(Default)]
struct TableStatistics {
    count_latency: f64,
    count: u64,
}

async fn collect_table_statistics<Q>(
    conn: &mut Q,
    table: &MySqlTableName,
) -> Result<TableStatistics, anyhow::Error>
where
    Q: Queryable,
{
    let mut stats = TableStatistics::default();

    let count_row: Option<u64> = conn
        .query_first(format!("SELECT COUNT(*) FROM {}", table))
        .wall_time()
        .set_at(&mut stats.count_latency)
        .await?;
    stats.count = count_row.ok_or_else(|| anyhow::anyhow!("failed to COUNT(*) {table}"))?;

    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_mysql_util::{MySqlColumnDesc, MySqlTableDesc};
    use timely::progress::Antichain;

    #[mz_ore::test]
    fn snapshot_query_duplicate_table() {
        let schema_name = "myschema".to_string();
        let table_name = "mytable".to_string();
        let table = MySqlTableName(schema_name.clone(), table_name.clone());
        let columns = ["c1", "c2", "c3"]
            .iter()
            .map(|col| MySqlColumnDesc {
                name: col.to_string(),
                column_type: None,
                meta: None,
            })
            .collect::<Vec<_>>();
        let desc = MySqlTableDesc {
            schema_name: schema_name.clone(),
            name: table_name.clone(),
            columns,
            keys: BTreeSet::default(),
        };
        let info = SourceOutputInfo {
            output_index: 1, // ignored
            table_name: table.clone(),
            desc,
            text_columns: vec![],
            exclude_columns: vec![],
            initial_gtid_set: Antichain::default(),
            resume_upper: Antichain::default(),
            export_id: mz_repr::GlobalId::User(1),
        };
        let query = build_snapshot_query(&[info.clone(), info], None);
        assert_eq!(
            format!(
                "SELECT `c1`, `c2`, `c3` FROM `{}`.`{}`",
                &schema_name, &table_name
            ),
            query
        );
    }

    #[mz_ore::test]
    fn snapshot_query_with_pk_range() {
        let schema_name = "myschema".to_string();
        let table_name = "mytable".to_string();
        let table = MySqlTableName(schema_name.clone(), table_name.clone());
        let columns = ["id", "name"]
            .iter()
            .map(|col| MySqlColumnDesc {
                name: col.to_string(),
                column_type: None,
                meta: None,
            })
            .collect::<Vec<_>>();
        let desc = MySqlTableDesc {
            schema_name: schema_name.clone(),
            name: table_name.clone(),
            columns,
            keys: BTreeSet::default(),
        };
        let info = SourceOutputInfo {
            output_index: 1,
            table_name: table.clone(),
            desc,
            text_columns: vec![],
            exclude_columns: vec![],
            initial_gtid_set: Antichain::default(),
            resume_upper: Antichain::default(),
            export_id: mz_repr::GlobalId::User(1),
        };

        // Bounded range
        let range = PkRange {
            pk_col: "`id`".to_string(),
            lower: 100,
            upper: Some(200),
        };
        let query = build_snapshot_query(std::slice::from_ref(&info), Some(&range));
        assert_eq!(
            format!(
                "SELECT `id`, `name` FROM `{}`.`{}` WHERE `id` >= 100 AND `id` < 200",
                &schema_name, &table_name
            ),
            query
        );

        // Open-ended range (last worker)
        let range = PkRange {
            pk_col: "`id`".to_string(),
            lower: 200,
            upper: None,
        };
        let query = build_snapshot_query(std::slice::from_ref(&info), Some(&range));
        assert_eq!(
            format!(
                "SELECT `id`, `name` FROM `{}`.`{}` WHERE `id` >= 200",
                &schema_name, &table_name
            ),
            query
        );
    }

    #[mz_ore::test]
    fn test_worker_pk_range() {
        let bounds = PkBounds {
            pk_col: "`id`".to_string(),
            min_val: 1,
            max_val: 100,
        };

        // 2 workers, range_size = 100
        let r0 = worker_pk_range(&bounds, 0, 2).expect("worker 0 should have range");
        assert_eq!(r0.lower, 1);
        assert_eq!(r0.upper, Some(51));

        let r1 = worker_pk_range(&bounds, 1, 2).expect("worker 1 should have range");
        assert_eq!(r1.lower, 51);
        assert!(r1.upper.is_none()); // last worker

        // More workers than range
        let small_bounds = PkBounds {
            pk_col: "`id`".to_string(),
            min_val: 1,
            max_val: 2,
        };
        // range_size = 2, effective = 2
        let r0 = worker_pk_range(&small_bounds, 0, 10).expect("worker 0 should have range");
        assert_eq!(r0.lower, 1);
        assert_eq!(r0.upper, Some(2));
        let r1 = worker_pk_range(&small_bounds, 1, 10).expect("worker 1 should have range");
        assert_eq!(r1.lower, 2);
        assert!(r1.upper.is_none());
        // Workers beyond effective count get nothing
        assert!(worker_pk_range(&small_bounds, 2, 10).is_none());

        // Large Int64 range — would overflow with i64 arithmetic
        let large_bounds = PkBounds {
            pk_col: "`id`".to_string(),
            min_val: 0,
            max_val: i64::MAX,
        };
        // With 4 workers this should not panic or wrap
        let r0 = worker_pk_range(&large_bounds, 0, 4).expect("worker 0");
        assert_eq!(r0.lower, 0);
        let r3 = worker_pk_range(&large_bounds, 3, 4).expect("worker 3");
        assert!(r3.upper.is_none()); // last worker, open-ended
        // Ranges should be contiguous: each worker's start == previous worker's end
        let r1 = worker_pk_range(&large_bounds, 1, 4).expect("worker 1");
        let r2 = worker_pk_range(&large_bounds, 2, 4).expect("worker 2");
        assert_eq!(r0.upper, Some(r1.lower));
        assert_eq!(r1.upper, Some(r2.lower));
        assert_eq!(r2.upper, Some(r3.lower));
    }

    #[mz_ore::test]
    fn test_integer_pk_col() {
        use mz_mysql_util::MySqlKeyDesc;
        use mz_repr::SqlColumnType;

        // Single-column INT32 PK
        let desc = MySqlTableDesc {
            schema_name: "s".to_string(),
            name: "t".to_string(),
            columns: vec![MySqlColumnDesc {
                name: "id".to_string(),
                column_type: Some(SqlColumnType {
                    scalar_type: SqlScalarType::Int32,
                    nullable: false,
                }),
                meta: None,
            }],
            keys: BTreeSet::from([MySqlKeyDesc {
                name: "PRIMARY".to_string(),
                is_primary: true,
                columns: vec!["id".to_string()],
            }]),
        };
        assert_eq!(integer_pk_col(&desc), Some("id"));

        // UInt64 PK should be excluded
        let desc_u64 = MySqlTableDesc {
            schema_name: "s".to_string(),
            name: "t".to_string(),
            columns: vec![MySqlColumnDesc {
                name: "id".to_string(),
                column_type: Some(SqlColumnType {
                    scalar_type: SqlScalarType::UInt64,
                    nullable: false,
                }),
                meta: None,
            }],
            keys: BTreeSet::from([MySqlKeyDesc {
                name: "PRIMARY".to_string(),
                is_primary: true,
                columns: vec!["id".to_string()],
            }]),
        };
        assert_eq!(integer_pk_col(&desc_u64), None);

        // Multi-column PK should be excluded
        let desc_multi = MySqlTableDesc {
            schema_name: "s".to_string(),
            name: "t".to_string(),
            columns: vec![
                MySqlColumnDesc {
                    name: "a".to_string(),
                    column_type: Some(SqlColumnType {
                        scalar_type: SqlScalarType::Int32,
                        nullable: false,
                    }),
                    meta: None,
                },
                MySqlColumnDesc {
                    name: "b".to_string(),
                    column_type: Some(SqlColumnType {
                        scalar_type: SqlScalarType::Int32,
                        nullable: false,
                    }),
                    meta: None,
                },
            ],
            keys: BTreeSet::from([MySqlKeyDesc {
                name: "PRIMARY".to_string(),
                is_primary: true,
                columns: vec!["a".to_string(), "b".to_string()],
            }]),
        };
        assert_eq!(integer_pk_col(&desc_multi), None);

        // No PK
        let desc_no_pk = MySqlTableDesc {
            schema_name: "s".to_string(),
            name: "t".to_string(),
            columns: vec![MySqlColumnDesc {
                name: "id".to_string(),
                column_type: Some(SqlColumnType {
                    scalar_type: SqlScalarType::Int32,
                    nullable: false,
                }),
                meta: None,
            }],
            keys: BTreeSet::default(),
        };
        assert_eq!(integer_pk_col(&desc_no_pk), None);
    }
}
