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
//! For tables with a suitable primary key, the leader computes `worker_count - 1` boundary keys
//! that split the key domain into disjoint half-open ranges, and broadcasts them. Each worker
//! reads only its assigned range. A single-column integer PK uses a cheap `MIN(pk)`/`MAX(pk)`
//! split; other supported PKs (non-integer and/or composite) sample evenly spaced boundary keys
//! with one primary-key-index scan. Tables without a suitable PK fall back to
//! single-worker-per-table mode.
//!
//! ## Resource considerations
//!
//! Parallel range reads open one MySQL connection per worker that has a range to read, plus the
//! leader's lock connection, so a single source can hold up to `worker_count + 1` concurrent
//! upstream connections while snapshotting. On clusters with many workers, or when many sources
//! snapshot concurrently, this can approach the server's `max_connections` limit (which defaults
//! to 151 on stock MySQL, far below typical PostgreSQL defaults). A failure to connect surfaces as
//! a transient error that restarts the snapshot, so connection pressure causes retries rather than
//! incorrect data. Workers with no range to read skip connecting to avoid amplifying this.
//!
//! The leader holds the global `LOCK TABLES ... READ` only until every worker has *started* its
//! `CONSISTENT SNAPSHOT` transaction (not until reads finish), since each transaction pins its read
//! view at start. The lock window therefore scales with the time for the slowest worker to connect
//! and begin its transaction, during which upstream writes to the locked tables are blocked
//! (bounded by `snapshot_lock_wait_timeout`).
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
use mz_timely_util::containers::stack::FueledBuilder;
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
use crate::source::types::{FuelSize, SignaledFuture, SourceMessage, StackedCollection};
use crate::statistics::SourceStatistics;

use super::schemas::verify_schemas;
use super::{
    DefiniteError, MySqlTableName, ReplicationError, RewindRequest, SourceOutputInfo,
    TransientError, return_definite_error, validate_mysql_repl_settings,
};

/// If `desc` has a single-column integer PK, return the column name. Such PKs
/// take the cheap `MIN`/`MAX` split path in [`compute_integer_splits`].
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

/// How a PK column's values are rendered as SQL literals in range predicates.
#[derive(Debug, Clone, Copy)]
enum PkColKind {
    /// Integer type, rendered and compared as a bare numeric literal.
    Numeric,
    /// Character type, rendered as a quoted string literal via MySQL `QUOTE()`
    /// and compared under the column's own collation.
    Text,
}

/// Classify a scalar type for range splitting, or `None` if unsupported.
fn pk_col_kind(scalar_type: &SqlScalarType) -> Option<PkColKind> {
    match scalar_type {
        SqlScalarType::Int16
        | SqlScalarType::Int32
        | SqlScalarType::Int64
        | SqlScalarType::UInt16
        | SqlScalarType::UInt32
        | SqlScalarType::UInt64 => Some(PkColKind::Numeric),
        SqlScalarType::Char { .. } | SqlScalarType::VarChar { .. } | SqlScalarType::String => {
            Some(PkColKind::Text)
        }
        _ => None,
    }
}

/// If `desc` has a primary key whose every column is a supported type, return
/// the quoted PK columns with their kinds, in key order. Used for the general
/// (non-integer and/or composite) sampling path in [`compute_sampled_splits`].
fn formattable_pk(desc: &mz_mysql_util::MySqlTableDesc) -> Option<Vec<(String, PkColKind)>> {
    let pk = desc.keys.iter().find(|k| k.is_primary)?;
    let mut cols = Vec::with_capacity(pk.columns.len());
    for name in &pk.columns {
        let col = desc.columns.iter().find(|c| &c.name == name)?;
        let kind = pk_col_kind(&col.column_type.as_ref()?.scalar_type)?;
        cols.push((quote_identifier(name), kind));
    }
    Some(cols)
}

/// PK-range partition boundaries for a table, computed by the leader.
///
/// `pk_cols` are the quoted PK column identifiers in key order. `boundaries`
/// holds `partition_count - 1` boundary keys; each is one SQL literal per PK
/// column. Boundary `i` is the lower bound of partition `i + 1`, so the
/// partitions are the half-open ranges `[boundaries[i-1], boundaries[i])`.
///
/// INVARIANT: boundaries are non-decreasing under the same comparison the range
/// predicates use (per-column collation for text, numeric otherwise). That makes
/// the partitions disjoint and exhaustive over the entire key domain, so every
/// row is read by exactly one worker regardless of the actual data distribution.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PkSplits {
    pk_cols: Vec<String>,
    boundaries: Vec<Vec<String>>,
}

/// Snapshot info broadcast from leader to all workers.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct SnapshotInfo {
    gtid_set: String,
    /// PK splits per table. None = no suitable PK, use single-worker fallback.
    pk_bounds: BTreeMap<MySqlTableName, Option<PkSplits>>,
}

/// A worker's assigned PK range for a table, as ready-to-splice SQL fragments.
struct PkRange {
    /// Comma-separated quoted PK columns, e.g. `` `a`, `b` ``.
    pk_cols: String,
    /// Inclusive lower bound (comma-separated literals), or `None` for the first
    /// partition (open start).
    lower: Option<String>,
    /// Exclusive upper bound, or `None` for the last partition (open end).
    upper: Option<String>,
}

/// What a worker does for one table during the snapshot.
enum ReadPlan {
    /// Partitioned table: read this worker's assigned PK range.
    Range(PkRange),
    /// Unpartitioned table: this worker is responsible for it and reads it whole.
    WholeTable,
    /// Partitioned table this worker is responsible for but owns no range (more
    /// workers than partitions). It reads nothing, but still opens a transaction
    /// so it can emit the table's rewind request. Without this carve-out the
    /// responsible surplus worker would read the whole table and duplicate the
    /// rows already read by the range-owning workers.
    RewindOnly,
}

/// Compute this worker's PK range from the broadcast splits, or `None` if this
/// worker's id is beyond the partition count (fewer partitions than workers).
fn worker_pk_range(splits: &PkSplits, worker_id: usize) -> Option<PkRange> {
    let partitions = splits.boundaries.len() + 1;
    if worker_id >= partitions {
        return None;
    }
    let tuple = |b: &[String]| b.iter().join(", ");
    Some(PkRange {
        pk_cols: splits.pk_cols.iter().join(", "),
        lower: (worker_id > 0).then(|| tuple(&splits.boundaries[worker_id - 1])),
        upper: (worker_id < partitions - 1).then(|| tuple(&splits.boundaries[worker_id])),
    })
}

/// Split a single-column integer PK into evenly spaced ranges using `MIN`/`MAX`,
/// without scanning the table. Returns `None` if the key domain is too small to
/// split across `worker_count` workers, or the table is empty.
///
/// Uses `i128` intermediate arithmetic to avoid overflow when the range spans a
/// large portion of the `i64` domain (e.g. `min = 0, max = 2^62`).
async fn compute_integer_splits<Q>(
    conn: &mut Q,
    table: &MySqlTableName,
    pk_col: &str,
    worker_count: usize,
) -> Result<Option<PkSplits>, LeaderError>
where
    Q: Queryable,
{
    let quoted = quote_identifier(pk_col);
    // `quoted` and `table` are escaped via `quote_identifier` / `MySqlTableName::Display`,
    // so this interpolation is safe; not parameterizable.
    #[allow(clippy::disallowed_methods)]
    let row: Option<(Option<i64>, Option<i64>)> = conn
        .query_first(format!(
            "SELECT MIN({0}) AS pk_min, MAX({0}) AS pk_max FROM {1}",
            quoted, table
        ))
        .await
        .map_err(classify_query_error)?;
    let Some((Some(min_val), Some(max_val))) = row else {
        return Ok(None);
    };
    let min = i128::from(min_val);
    let range_size = i128::from(max_val) - min + 1;
    let partitions = std::cmp::min(i128::cast_from(worker_count), range_size);
    if partitions < 2 {
        return Ok(None);
    }
    let boundaries = (1..partitions)
        .map(|p| vec![(min + p * range_size / partitions).to_string()])
        .collect();
    Ok(Some(PkSplits {
        pk_cols: vec![quoted],
        boundaries,
    }))
}

/// Split an arbitrary (non-integer and/or composite) PK into evenly sized
/// partitions by sampling `partition_count - 1` boundary keys via keyset
/// pagination: each probe reads the first key one chunk past the previous
/// boundary (`WHERE pk > prev ORDER BY pk LIMIT 1 OFFSET chunk-1`), so together
/// they make a single forward pass of the PK index rather than a full sort.
///
/// Boundaries come back already rendered as SQL literals (`QUOTE()` for text,
/// `CAST(.. AS CHAR)` for numeric) so the `ORDER BY` here and the range
/// predicates compare identically (per-column collation for text). Because each
/// boundary is found with a strict `pk > prev`, boundaries strictly increase.
///
/// Returns `None` if the table has fewer rows than `worker_count` or is empty,
/// or a boundary fails to decode (e.g. a non-UTF-8 collation).
async fn compute_sampled_splits<Q>(
    conn: &mut Q,
    table: &MySqlTableName,
    pk_cols: &[(String, PkColKind)],
    worker_count: usize,
) -> Result<Option<PkSplits>, LeaderError>
where
    Q: Queryable,
{
    // `table` is escaped via `MySqlTableName::Display`; not parameterizable.
    #[allow(clippy::disallowed_methods)]
    let total: Option<u64> = conn
        .query_first(format!("SELECT COUNT(*) FROM {}", table))
        .await
        .map_err(classify_query_error)?;
    let total = total.unwrap_or(0);
    let partitions = std::cmp::min(u64::cast_from(worker_count), total);
    if partitions < 2 {
        return Ok(None);
    }
    let chunk = total / partitions;
    let cols_ident = pk_cols.iter().map(|(c, _)| c.as_str()).join(", ");
    let cols_literal = pk_cols
        .iter()
        .map(|(c, kind)| match kind {
            PkColKind::Numeric => format!("CAST({c} AS CHAR)"),
            PkColKind::Text => format!("QUOTE({c})"),
        })
        .join(", ");

    let mut boundaries: Vec<Vec<String>> = Vec::with_capacity(usize::cast_from(partitions) - 1);
    for _ in 1..partitions {
        // Skip a chunk of rows past the previous boundary and take the next key.
        // The first probe has no lower bound and skips a full chunk; later probes
        // start just after the previous boundary, so `OFFSET chunk - 1`.
        let (predicate, offset) = match boundaries.last() {
            Some(prev) => (
                format!(" WHERE ({}) > ({})", cols_ident, prev.iter().join(", ")),
                chunk - 1,
            ),
            None => (String::new(), chunk),
        };
        // Identifiers are quoted via `quote_identifier`, the previous boundary is
        // itself a value MySQL rendered as a literal, `table` via Display, and the
        // offset is an integer, so this interpolation is safe; not parameterizable.
        #[allow(clippy::disallowed_methods)]
        let row: Option<MySqlRow> = conn
            .query_first(format!(
                "SELECT {cols_literal} FROM {table}{predicate} \
                 ORDER BY {cols_ident} LIMIT 1 OFFSET {offset}"
            ))
            .await
            .map_err(classify_query_error)?;
        // Ran off the end (table smaller than COUNT implied): stop, and use the
        // boundaries found so far. Fewer partitions is still correct.
        let Some(mut row) = row else { break };
        let mut tuple = Vec::with_capacity(pk_cols.len());
        for i in 0..pk_cols.len() {
            // Every column is CAST/QUOTE-ed to text, so it decodes as a String
            // that is already a valid SQL literal. A decode failure (e.g. a
            // non-UTF-8 collation) means we can't safely partition: fall back.
            match row.take_opt::<String, usize>(i) {
                Some(Ok(lit)) => tuple.push(lit),
                _ => return Ok(None),
            }
        }
        boundaries.push(tuple);
    }
    if boundaries.is_empty() {
        return Ok(None);
    }
    Ok(Some(PkSplits {
        pk_cols: pk_cols.iter().map(|(c, _)| c.clone()).collect(),
        boundaries,
    }))
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

/// Classify an error from a leader query that touches the upstream tables.
///
/// A table dropped upstream after source planning surfaces as
/// `ER_NO_SUCH_TABLE`, which is a definite `TableDropped` error, not a transient
/// one. The PK-split probes and `LOCK TABLES` are the first statements to touch
/// the tables, so a dropped table fails here. The default `From<mysql_async::Error>`
/// conversion classifies everything as transient, which would restart the
/// dataflow into the same error forever. Every leader query against the tables
/// must route its error through this instead of the `?` conversion.
fn classify_query_error(e: mysql_async::Error) -> LeaderError {
    match e {
        mysql_async::Error::Server(mysql_async::ServerError { code, message, .. })
            if code == ER_NO_SUCH_TABLE =>
        {
            LeaderError::Definite(DefiniteError::TableDropped(message))
        }
        e => LeaderError::Transient(e.into()),
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

    let (raw_handle, raw_data) = builder.new_output::<FueledBuilder<_>>();
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

                        // Compute PK-range split boundaries for each table BEFORE
                        // acquiring the lock. Single-column integer PKs use the
                        // cheap MIN/MAX split; other supported PKs sample boundaries
                        // with one PK-index scan, which is O(rows). Doing this work
                        // before `LOCK TABLES` keeps the lock-hold window O(1) rather
                        // than scaling with table size. Boundaries need not match the
                        // exact snapshot point: they only partition the key domain and
                        // stay correct for any data distribution (see `PkSplits`).
                        // `None` means single-worker fallback.
                        let mut pk_bounds_map = BTreeMap::new();
                        for (table, outputs) in &tables_to_snapshot {
                            let desc = &outputs[0].desc;
                            // With a single worker there is only one reader, so
                            // there is nothing to split. Skip the split probes
                            // (an O(rows) COUNT(*)/index scan for sampled PKs, a
                            // MIN/MAX probe for integer PKs) and fall back to
                            // single-worker mode.
                            let splits = if config.worker_count < 2 {
                                None
                            } else if let Some(pk_col) = integer_pk_col(desc) {
                                compute_integer_splits(
                                    &mut *lock_conn,
                                    table,
                                    pk_col,
                                    config.worker_count,
                                )
                                .await?
                            } else if let Some(pk_cols) = formattable_pk(desc) {
                                compute_sampled_splits(
                                    &mut *lock_conn,
                                    table,
                                    &pk_cols,
                                    config.worker_count,
                                )
                                .await?
                            } else {
                                None
                            };
                            pk_bounds_map.insert(table.clone(), splits);
                        }

                        if let Some(timeout) = config
                            .config
                            .parameters
                            .mysql_source_timeouts
                            .snapshot_lock_wait_timeout
                        {
                            // Interpolating a `Duration` integer; not parameterizable in MySQL `SET`.
                            #[allow(clippy::disallowed_methods)]
                            lock_conn
                                .query_drop(format!(
                                    "SET @@session.lock_wait_timeout = {}",
                                    timeout.as_secs()
                                ))
                                .await?;
                        }

                        trace!(%id, "timely-{worker_id} acquiring table locks: {lock_clauses}");
                        // `lock_clauses` is built from `MySqlTableName::Display`, which
                        // escapes both schema and table via `quote_identifier`.
                        #[allow(clippy::disallowed_methods)]
                        lock_conn
                            .query_drop(format!("LOCK TABLES {lock_clauses}"))
                            .await
                            .map_err(classify_query_error)?;

                        // Record the frontier of future GTIDs based on the executed GTID set
                        // at the start of the snapshot
                        let snapshot_gtid_set =
                            query_sys_var(&mut lock_conn, "global.gtid_executed").await?;

                        trace!(%id, "timely-{worker_id} acquired table locks");

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

                // Precompute each table's read plan for this worker. A
                // partitioned table is read only by the workers that own a
                // range (plus a rewind-only entry for the responsible worker if
                // it owns none), so no row is read twice. An unpartitioned table
                // is read whole by its single responsible worker.
                let table_ranges: BTreeMap<_, ReadPlan> = tables_to_snapshot
                    .keys()
                    .filter_map(|table| {
                        let plan = match snapshot_info.pk_bounds.get(table) {
                            Some(Some(splits)) => match worker_pk_range(splits, config.worker_id) {
                                Some(range) => Some(ReadPlan::Range(range)),
                                None => config
                                    .responsible_for(table)
                                    .then_some(ReadPlan::RewindOnly),
                            },
                            _ => config
                                .responsible_for(table)
                                .then_some(ReadPlan::WholeTable),
                        };
                        plan.map(|plan| (table.clone(), plan))
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
                    // Set the session time zone to UTC so we read TIMESTAMP columns as UTC.
                    #[allow(clippy::disallowed_methods)] // static SQL string
                    tx.query_drop("set @@session.time_zone = '+00:00'").await?;
                    if let Some(timeout) = config
                        .config
                        .parameters
                        .mysql_source_timeouts
                        .snapshot_max_execution_time
                    {
                        // Interpolating an integer millis value; not parameterizable in MySQL `SET`.
                        #[allow(clippy::disallowed_methods)]
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
                        #[allow(clippy::disallowed_methods)] // static SQL string
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

                // Phase F: Verify schemas for the tables this worker reads. In
                // PK-range mode several workers read the same table, so each
                // verifies independently to learn which outputs to skip; only
                // the responsible worker publishes any resulting error (below).
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
                    // Every worker reading this table must stop ingesting the output, but
                    // only the responsible worker publishes the error so it lands with
                    // multiplicity one — other workers read disjoint PK ranges of the same
                    // table and would otherwise each emit the same error.
                    if config.responsible_for(&output.table_name) {
                        let update = (
                            (output.output_index, Err(err.clone().into())),
                            GtidPartition::minimum(),
                            Diff::ONE,
                        );
                        let size = update.fuel_size();
                        raw_handle.give_fueled(&data_cap_set[0], update, size).await;
                        tracing::warn!(%id, "timely-{worker_id} stopping snapshot of output \
                                    {output:?} due to schema mismatch");
                    }
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
                // The trailing `.max(1)` keeps the interval positive even for
                // pathological worker counts (> 10K), avoiding a `% 0` panic.
                let yield_interval = (10_000 / u64::cast_from(config.worker_count).max(1)).max(1);

                let mut snapshot_staged_total = 0;
                for (table, outputs) in &tables_to_snapshot {
                    let pk_range = match table_ranges.get(table) {
                        Some(ReadPlan::Range(range)) => Some(range),
                        Some(ReadPlan::WholeTable) => None,
                        // RewindOnly reads nothing; its rewind is emitted below.
                        // None: this worker has no work for this table.
                        Some(ReadPlan::RewindOnly) | None => continue,
                    };

                    let mut snapshot_staged = 0;
                    let query = build_snapshot_query(outputs, pk_range);
                    trace!(%id, "timely-{worker_id} reading snapshot query='{}'", query);
                    let mut results = tx.exec_stream(query, ()).await?;
                    while let Some(row) = results.try_next().await? {
                        let row: MySqlRow = row;
                        snapshot_staged += 1;
                        for (output, row_val) in outputs.iter().repeat_clone(row) {
                            // We don't need to verify if binlog_row_metadata matches the expected when snapshotting as
                            // the snapshot query always returns rows with full metadata. If the output is configured
                            // with binlog_full_metadata = false, then we will just ignore the metadata when decoding.
                            let event = match pack_mysql_row(
                                &mut final_row,
                                row_val,
                                &output.desc,
                                None,
                                output.binlog_full_metadata,
                            ) {
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
                            let update = (
                                (output.output_index, event),
                                GtidPartition::minimum(),
                                Diff::ONE,
                            );
                            let size = update.fuel_size();
                            raw_handle.give_fueled(&data_cap_set[0], update, size).await;
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
        // the columns may be decoded based on position, and different outputs may replicate
        // different columns, so we need to ensure that all columns are accounted for.
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
        // Row-value comparison so composite keys use lexicographic ordering that
        // matches the boundary `ORDER BY`. A single-column tuple is just the
        // column. The first/last partition omits its open bound.
        let cols = &range.pk_cols;
        if let Some(lower) = &range.lower {
            query.push_str(&format!(" WHERE ({cols}) >= ({lower})"));
        }
        if let Some(upper) = &range.upper {
            let kw = if range.lower.is_some() {
                "AND"
            } else {
                "WHERE"
            };
            query.push_str(&format!(" {kw} ({cols}) < ({upper})"));
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

    // `MySqlTableName::Display` escapes both identifier components via
    // `quote_identifier`, so this interpolation is safe; not parameterizable.
    #[allow(clippy::disallowed_methods)]
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
            binlog_full_metadata: false,
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
            binlog_full_metadata: false,
        };

        // Middle worker: both bounds, row-value form.
        let range = PkRange {
            pk_cols: "`id`".to_string(),
            lower: Some("100".to_string()),
            upper: Some("200".to_string()),
        };
        let query = build_snapshot_query(std::slice::from_ref(&info), Some(&range));
        assert_eq!(
            format!(
                "SELECT `id`, `name` FROM `{}`.`{}` WHERE (`id`) >= (100) AND (`id`) < (200)",
                &schema_name, &table_name
            ),
            query
        );

        // First worker: open start.
        let range = PkRange {
            pk_cols: "`id`".to_string(),
            lower: None,
            upper: Some("200".to_string()),
        };
        let query = build_snapshot_query(std::slice::from_ref(&info), Some(&range));
        assert_eq!(
            format!(
                "SELECT `id`, `name` FROM `{}`.`{}` WHERE (`id`) < (200)",
                &schema_name, &table_name
            ),
            query
        );

        // Last worker: open end.
        let range = PkRange {
            pk_cols: "`id`".to_string(),
            lower: Some("200".to_string()),
            upper: None,
        };
        let query = build_snapshot_query(std::slice::from_ref(&info), Some(&range));
        assert_eq!(
            format!(
                "SELECT `id`, `name` FROM `{}`.`{}` WHERE (`id`) >= (200)",
                &schema_name, &table_name
            ),
            query
        );

        // Composite text PK: row-value comparison over quoted columns.
        let range = PkRange {
            pk_cols: "`a`, `b`".to_string(),
            lower: Some("'m', 5".to_string()),
            upper: None,
        };
        let query = build_snapshot_query(std::slice::from_ref(&info), Some(&range));
        assert_eq!(
            format!(
                "SELECT `id`, `name` FROM `{}`.`{}` WHERE (`a`, `b`) >= ('m', 5)",
                &schema_name, &table_name
            ),
            query
        );
    }

    #[mz_ore::test]
    fn test_worker_pk_range() {
        // Two partitions, single-column boundary at 51.
        let splits = PkSplits {
            pk_cols: vec!["`id`".to_string()],
            boundaries: vec![vec!["51".to_string()]],
        };
        let r0 = worker_pk_range(&splits, 0).expect("worker 0");
        assert_eq!(r0.pk_cols, "`id`");
        assert_eq!(r0.lower, None); // open start
        assert_eq!(r0.upper.as_deref(), Some("51"));
        let r1 = worker_pk_range(&splits, 1).expect("worker 1");
        assert_eq!(r1.lower.as_deref(), Some("51"));
        assert_eq!(r1.upper, None); // open end
        // Beyond the partition count → no work.
        assert!(worker_pk_range(&splits, 2).is_none());

        // Three partitions: the middle worker has both bounds.
        let splits = PkSplits {
            pk_cols: vec!["`id`".to_string()],
            boundaries: vec![vec!["34".to_string()], vec!["67".to_string()]],
        };
        let r1 = worker_pk_range(&splits, 1).expect("worker 1");
        assert_eq!(r1.lower.as_deref(), Some("34"));
        assert_eq!(r1.upper.as_deref(), Some("67"));

        // Composite PK: a boundary is a comma-joined tuple of per-column literals.
        let splits = PkSplits {
            pk_cols: vec!["`a`".to_string(), "`b`".to_string()],
            boundaries: vec![vec!["'m'".to_string(), "5".to_string()]],
        };
        let r0 = worker_pk_range(&splits, 0).expect("worker 0");
        assert_eq!(r0.pk_cols, "`a`, `b`");
        assert_eq!(r0.upper.as_deref(), Some("'m', 5"));
    }

    #[mz_ore::test]
    fn test_formattable_pk() {
        use mz_mysql_util::MySqlKeyDesc;
        use mz_repr::SqlColumnType;

        let col = |name: &str, ty: SqlScalarType| MySqlColumnDesc {
            name: name.to_string(),
            column_type: Some(SqlColumnType {
                scalar_type: ty,
                nullable: false,
            }),
            meta: None,
        };
        let pk = |cols: &[&str]| {
            BTreeSet::from([MySqlKeyDesc {
                name: "PRIMARY".to_string(),
                is_primary: true,
                columns: cols.iter().map(|c| c.to_string()).collect(),
            }])
        };
        let desc = |columns, keys| MySqlTableDesc {
            schema_name: "s".to_string(),
            name: "t".to_string(),
            columns,
            keys,
        };

        // Single char PK.
        let cols = formattable_pk(&desc(
            vec![col("id", SqlScalarType::Char { length: None })],
            pk(&["id"]),
        ))
        .expect("char pk supported");
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].0, "`id`");
        assert!(matches!(cols[0].1, PkColKind::Text));

        // Composite (char, int) PK.
        let cols = formattable_pk(&desc(
            vec![
                col("a", SqlScalarType::Char { length: None }),
                col("b", SqlScalarType::Int64),
            ],
            pk(&["a", "b"]),
        ))
        .expect("composite pk supported");
        assert_eq!(
            cols.iter().map(|(c, _)| c.as_str()).collect::<Vec<_>>(),
            ["`a`", "`b`"]
        );

        // Unsupported column type in the PK → fall back.
        assert!(
            formattable_pk(&desc(vec![col("id", SqlScalarType::Bytes)], pk(&["id"]))).is_none()
        );

        // No primary key → fall back.
        assert!(
            formattable_pk(&desc(
                vec![col("id", SqlScalarType::Int64)],
                BTreeSet::default()
            ))
            .is_none()
        );
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
