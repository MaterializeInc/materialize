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
//! Ideally we would like to start a transaction and ask MySQL to tell us the point in time this
//! transaction is running at. As far as we know there isn't such API so we achieve this using
//! table locks instead.
//!
//! A designated leader worker acquires table locks on all the tables to be snapshotted. By doing
//! so we establish a moment in time where we know no writes are happening to the tables we are
//! interested in. The leader then reads the current upper frontier (`snapshot_upper`) using the
//! `@@gtid_executed` system variable and broadcasts it, along with PK-range bounds (see below), to
//! all workers via a timely feedback loop. This frontier establishes an upper bound on any
//! possible write to the tables of interest until the lock is released.
//!
//! Each worker now starts a transaction via a new connection with 'REPEATABLE READ' and
//! 'CONSISTENT SNAPSHOT' semantics. Due to linearizability we know that this transaction's view of
//! the database must some time `t_snapshot` such that `snapshot_upper <= t_snapshot`. We don't
//! actually know the exact value of `t_snapshot` and it might be strictly greater than
//! `snapshot_upper`. However, because this transaction will only be used to read the locked tables
//! and we know that `snapshot_upper` is an upper bound on all the writes that have happened to
//! them we can safely pretend that the transaction's `t_snapshot` is *equal* to `snapshot_upper`.
//! We have therefore succeeded in starting a transaction at a known point in time!
//!
//! The leader verifies each output's schema against the planning-time desc before locking, and
//! each worker re-verifies in its transaction, retrying transiently if the schema drifted since.
//!
//! Once all workers have started their transactions the leader unlocks the tables. Each worker
//! then reads the snapshot of the tables (or PK ranges) it is responsible for and publishes it
//! downstream.
//!
//! TODO: Other software products hold the table lock for the duration of the snapshot, and some do
//! not. We should figure out why and if we need to hold the lock longer. This may be because of a
//! difference in how REPEATABLE READ works in some MySQL-compatible systems (e.g. Aurora MySQL).
//!
//! ## Parallel PK-range snapshots
//!
//! For tables with a suitable primary key, the leader computes `worker_count - 1` boundary keys
//! that split the key domain into disjoint half-open ranges, and broadcasts them. Each worker
//! reads only its assigned range. Ranges are assigned round-robin starting from each table's
//! legacy single-worker owner, so the open-ended ranges (which absorb any rows written past the
//! last sampled boundary) land on a different worker per table rather than always the last worker.
//! Tables without a suitable PK fall back to single-worker-per-table mode. The
//! `mysql_source_snapshot_parallelism` dyncfg disables splitting entirely, putting every table in
//! that fallback mode. Workers open their connections while the leader samples and locks, so
//! setup briefly holds up to `2 * worker_count + 1` upstream connections per source, settling to
//! one per ranged worker plus the leader's lock connection. To handle various charsets and
//! collation gracefully we rely on MySQL's sort order and never attempt to compare or order
//! strings in Rust. To handle possible races with
//! changes to collation each worker validates in its read transaction that the boundaries are
//! strictly increasing under the table's current collation, retrying transiently if not. The
//! repeatable read snapshots should then succeed if they start reading from the table before DDL
//! runs, or if DDL does run before one of the workers reads the table, that worker's transaction
//! should fail with an ER_TABLE_DEF_CHANGED.
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
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use differential_dataflow::AsCollection;
use futures::{StreamExt as _, TryStreamExt};
use itertools::Itertools;
use mysql_async::prelude::Queryable;
use mysql_async::{IsolationLevel, Row as MySqlRow, TxOpts};
use mz_mysql_util::{
    ER_NO_SUCH_TABLE, MySqlConn, MySqlError, pack_mysql_row, query_sys_var, quote_identifier,
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

/// The raw (unquoted) name and scalar type of a table's primary key, when it is a
/// single column. Callers quote the name for SQL predicates and use the raw name for
/// `information_schema` lookups.
fn try_extract_single_column_pk(
    desc: &mz_mysql_util::MySqlTableDesc,
) -> Option<(String, SqlScalarType)> {
    let pk = desc.keys.iter().find(|k| k.is_primary)?;
    let [name] = &pk.columns[..] else {
        return None;
    };
    let col = desc.columns.iter().find(|c| &c.name == name)?;
    if col.meta.is_some() {
        return None;
    }
    let scalar_type = col.column_type.as_ref()?.scalar_type.clone();
    Some((name.clone(), scalar_type))
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PkBoundaries {
    pk_col: String,
    // Ordered primary key values that partition the table space.
    boundaries: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct SnapshotInfo {
    gtid_set: String,
    /// PK splits per table. None = no suitable PK, use single-worker fallback.
    pk_bounds: BTreeMap<MySqlTableName, Option<PkBoundaries>>,
    errored_outputs: Vec<(usize, DefiniteError)>,
}

struct PkRange {
    /// Quoted PK column, e.g. `` `id` ``.
    pk_col: String,
    /// Inclusive lower bound literal, or `None` for the first partition (open start).
    lower: Option<String>,
    /// Exclusive upper bound literal, or `None` for the last partition (open end).
    upper: Option<String>,
}

/// What a worker does for one table during the snapshot.
enum ReadPlan {
    /// Partitioned table: read this worker's assigned PK range.
    Range(PkRange),
    /// Unpartitioned table: this worker is responsible for it and reads it whole.
    WholeTable,
}

/// This worker's PK range, or `None` if it owns no partition. Rotating the partition
/// by the table's `owner` keeps the open-ended ranges from always landing on the same workers.
fn worker_pk_range(
    splits: &PkBoundaries,
    worker_id: usize,
    owner_worker_id: usize,
    worker_count: usize,
) -> Option<PkRange> {
    let partition = (worker_id + worker_count - owner_worker_id) % worker_count;
    let partitions = splits.boundaries.len() + 1;
    if partition >= partitions {
        return None;
    }
    Some(PkRange {
        pk_col: splits.pk_col.clone(),
        lower: (partition > 0).then(|| splits.boundaries[partition - 1].clone()),
        upper: (partition < partitions - 1).then(|| splits.boundaries[partition].clone()),
    })
}

/// Walks the primary key index in steps of about `total / worker_count`, taking the key
/// at each step's `OFFSET`. The per-step OFFSET scans sum to a full index pass, so this
/// is O(total). Worker count is small, so the OFFSET scans dominate. `total` is the exact
/// row count supplied by the caller, read on the same `READ ONLY` transaction as this
/// walk, so the partitions are exact. Returns None if the primary key column type is not
/// supported or the table is too small to split.
async fn compute_sampled_splits<Q>(
    conn: &mut Q,
    table: &MySqlTableName,
    pk_col: &(String, SqlScalarType),
    worker_count: usize,
    total: u64,
) -> Result<Option<PkBoundaries>, SnapshotSetupError>
where
    Q: Queryable,
{
    let (col, scalar_type) = pk_col;
    // Render the PK column as text that sorts and compares the same way the range
    // predicates do: `QUOTE()` under the column's collation for character types,
    // `CAST(.. AS CHAR)` for integers. Any other type can't be split safely.
    let (col_literal, integer_path) = match scalar_type {
        SqlScalarType::Int16
        | SqlScalarType::Int32
        | SqlScalarType::Int64
        | SqlScalarType::UInt16
        | SqlScalarType::UInt32
        | SqlScalarType::UInt64 => (format!("CAST({col} AS CHAR)"), true),
        SqlScalarType::Char { .. } | SqlScalarType::VarChar { .. } | SqlScalarType::String => {
            (format!("QUOTE({col})"), false)
        }
        _ => return Ok(None),
    };

    let partitions = std::cmp::min(u64::cast_from(worker_count), total);
    if partitions < 2 {
        return Ok(None);
    }
    let chunk = total / partitions;

    let mut boundaries: Vec<String> = Vec::with_capacity(usize::cast_from(partitions) - 1);
    for _ in 1..partitions {
        let (predicate, offset) = match boundaries.last() {
            Some(prev) => (format!(" WHERE {col} > {prev}"), chunk - 1),
            None => (String::new(), chunk),
        };
        // The identifier is quoted via `quote_identifier`, the previous boundary is
        // itself a value MySQL rendered as a literal, `table` via Display, and the
        // offset is an integer, so this interpolation is safe; not parameterizable.
        #[allow(clippy::disallowed_methods)]
        let row: Option<MySqlRow> = conn
            .query_first(format!(
                "SELECT {col_literal} FROM {table}{predicate} \
                 ORDER BY {col} LIMIT 1 OFFSET {offset}"
            ))
            .await
            .map_err(classify_query_error)?;
        // Defensive: if a concurrent write shrank the range out from under us, stop and
        // use the boundaries found so far. Fewer partitions is still correct.
        let Some(mut row) = row else { break };
        // The column is CAST/QUOTE-ed to text, so it decodes as a String that is
        // already a valid SQL literal. A decode failure (e.g. a non-UTF-8
        // collation) means we can't safely partition: fall back.
        match row.take_opt::<String, usize>(0) {
            Some(Ok(lit)) if !integer_path || is_decimal_literal(&lit) => boundaries.push(lit),
            _ => return Ok(None),
        }
    }
    if boundaries.is_empty() {
        return Ok(None);
    }
    Ok(Some(PkBoundaries {
        pk_col: col.clone(),
        boundaries,
    }))
}

/// For every table, read the exact row count and, for a supported single-column primary
/// key, compute the PK-range split boundaries, concurrently over at most `worker_count`
/// connections. `None` bounds means single-worker fallback for that table. The counts are
/// reused for both the sampling stride and the snapshot size gauge.
async fn sample_pk_bounds(
    config: &RawSourceCreationConfig,
    connection_config: &mz_mysql_util::Config,
    task_name: &str,
    tables: &BTreeMap<MySqlTableName, Vec<SourceOutputInfo>>,
    metrics: &MySqlSnapshotMetrics,
) -> Result<
    (
        BTreeMap<MySqlTableName, Option<PkBoundaries>>,
        BTreeMap<MySqlTableName, u64>,
    ),
    SnapshotSetupError,
> {
    let ssh_tunnel_manager = &config.config.connection_context.ssh_tunnel_manager;
    let worker_count = config.worker_count;
    let max_execution_time = config
        .config
        .parameters
        .mysql_source_timeouts
        .snapshot_max_execution_time;
    // Kill switch for PK-range splitting. When disabled every table gets `None`
    // bounds, i.e. the single-worker-per-table fallback. The counts still run,
    // they feed the snapshot size gauge.
    let parallelism_enabled = mz_storage_types::dyncfgs::MYSQL_SOURCE_SNAPSHOT_PARALLELISM
        .get(config.config.config_set());

    let pooled_conns: Rc<RefCell<Vec<MySqlConn>>> = Rc::new(RefCell::new(Vec::new()));
    // Counting and boundary-sampling each walk a table's index (O(rows)), so run tables
    // concurrently, capped at worker_count, to keep the leader's setup from scaling with
    // table count.
    let per_table: Vec<(MySqlTableName, u64, Option<PkBoundaries>)> = futures::stream::iter(tables)
        .map(|(table, outputs)| {
            let pool = Rc::clone(&pooled_conns);
            async move {
                // Grab the connection before the match to ensure the borrow is dropped
                // before any awaits are called.
                let pooled = pool.borrow_mut().pop();
                let mut conn = match pooled {
                    Some(conn) => conn,
                    None => {
                        let mut conn = connection_config
                            .connect(task_name, ssh_tunnel_manager)
                            .await?;
                        if let Some(timeout) = max_execution_time {
                            #[allow(clippy::disallowed_methods)]
                            conn.query_drop(format!(
                                "SET @@session.max_execution_time = {}",
                                timeout.as_millis()
                            ))
                            .await?;
                        }
                        #[allow(clippy::disallowed_methods)]
                        conn.query_drop("START TRANSACTION READ ONLY").await?;
                        conn
                    }
                };
                // Exact row count, reused for the sampling stride and the size gauge.
                // It runs on the same `READ ONLY` transaction as the boundary walk in
                // `compute_sampled_splits`, so both see one consistent snapshot.
                let stats = collect_table_statistics(&mut *conn, table).await?;
                metrics.record_table_count_latency(
                    table.1.clone(),
                    table.0.clone(),
                    stats.count_latency,
                );
                let count = stats.count;
                // Compute split boundaries only for a supported single-column PK.
                let splits = match parallelism_enabled
                    .then(|| try_extract_single_column_pk(&outputs[0].desc))
                    .flatten()
                {
                    Some((raw_col, scalar_type)) => {
                        let pk_col = (quote_identifier(&raw_col), scalar_type);
                        compute_sampled_splits(&mut *conn, table, &pk_col, worker_count, count)
                            .await?
                    }
                    None => None,
                };
                pool.borrow_mut().push(conn);
                Ok::<_, SnapshotSetupError>((table.clone(), count, splits))
            }
        })
        // At most `worker_count` connections are checked out at once, so the pool
        // opens at most `min(worker_count, num_tables)` in total.
        .buffer_unordered(worker_count)
        .try_collect()
        .await?;

    let mut pk_bounds: BTreeMap<MySqlTableName, Option<PkBoundaries>> = BTreeMap::new();
    let mut counts: BTreeMap<MySqlTableName, u64> = BTreeMap::new();
    for (table, count, splits) in per_table {
        pk_bounds.insert(table.clone(), splits);
        counts.insert(table, count);
    }

    // Every future has completed and dropped its `Rc` clone, so `pool` is the sole
    // owner. Release the probe connections now, ending their `READ ONLY` transactions
    // and the shared metadata locks they hold, before the caller takes `LOCK TABLES`.
    let probe_conns = Rc::into_inner(pooled_conns)
        .expect("all sampling futures completed, so no Rc clones remain")
        .into_inner();
    for conn in probe_conns {
        conn.disconnect().await?;
    }
    Ok((pk_bounds, counts))
}

/// Leader-only snapshot setup: sample PK bounds, lock the tables `READ`, and read
/// the snapshot GTID frontier. All fallible work happens here so the caller can
/// always broadcast a result. A dropped `snapshot_cap_set` with no broadcast
/// deadlocks the other workers waiting on the feedback loop.
async fn lock_and_prepare_snapshot(
    config: &RawSourceCreationConfig,
    connection_config: &mz_mysql_util::Config,
    task_name: &str,
    tables: &BTreeMap<MySqlTableName, Vec<SourceOutputInfo>>,
    metrics: &MySqlSnapshotMetrics,
) -> Result<(SnapshotInfo, BTreeMap<MySqlTableName, u64>, MySqlConn), SnapshotSetupError> {
    let mut lock_conn = connection_config
        .connect(
            task_name,
            &config.config.connection_context.ssh_tunnel_manager,
        )
        .await?;

    let errored_outputs = verify_output_schemas(&mut *lock_conn, tables).await?;
    let errored: BTreeSet<usize> = errored_outputs.iter().map(|(idx, _)| *idx).collect();
    let sample_tables: BTreeMap<MySqlTableName, Vec<SourceOutputInfo>> = tables
        .iter()
        .map(|(table, outputs)| {
            let outputs = outputs
                .iter()
                .filter(|o| !errored.contains(&o.output_index))
                .cloned()
                .collect::<Vec<_>>();
            (table.clone(), outputs)
        })
        .filter(|(_, outputs)| !outputs.is_empty())
        .collect();

    // Sampling is expensive, so run it before locking writes.
    let (pk_bounds, counts) = sample_pk_bounds(
        config,
        connection_config,
        task_name,
        &sample_tables,
        metrics,
    )
    .await?;

    let lock_clauses = tables
        .keys()
        .map(|t| format!("{} READ", t))
        .collect::<Vec<String>>()
        .join(", ");

    // TODO(roshan): Insert metric for how long it took to acquire the locks
    let snapshot_gtid_set = lock_tables_and_read_gtid_set(
        &mut lock_conn,
        &lock_clauses,
        config
            .config
            .parameters
            .mysql_source_timeouts
            .snapshot_lock_wait_timeout,
    )
    .await?;

    Ok((
        SnapshotInfo {
            gtid_set: snapshot_gtid_set,
            pk_bounds,
            errored_outputs,
        },
        counts,
        lock_conn,
    ))
}

async fn verify_output_schemas<Q>(
    conn: &mut Q,
    tables: &BTreeMap<MySqlTableName, Vec<SourceOutputInfo>>,
) -> Result<Vec<(usize, DefiniteError)>, SnapshotSetupError>
where
    Q: Queryable,
{
    let errored = verify_schemas(
        conn,
        tables.iter().map(|(k, v)| (k, v.as_slice())).collect(),
    )
    .await?;
    Ok(errored
        .into_iter()
        .map(|(output, err)| (output.output_index, err))
        .collect())
}

/// Character set and collation of `column` in `table`, or `None` if the column has no
/// collation (numeric/temporal types sort independently of collation) or is absent.
async fn fetch_column_collation<Q>(
    conn: &mut Q,
    table: &MySqlTableName,
    column: &str,
) -> Result<Option<(String, String)>, TransientError>
where
    Q: Queryable,
{
    let row: Option<(Option<String>, Option<String>)> = conn
        .exec_first(
            "SELECT character_set_name, collation_name \
             FROM information_schema.columns \
             WHERE table_schema = ? AND table_name = ? AND column_name = ?",
            (&table.0, &table.1, column),
        )
        .await?;
    // The two are NULL together for a non-character column.
    Ok(row.and_then(|(charset, collation)| Some((charset?, collation?))))
}

/// Whether `boundaries` are strictly increasing under `collation`. The half-open PK
/// ranges only partition the table without gaps or overlaps when this holds. The
/// boundaries are already SQL literals (from `QUOTE()`); each is coerced to
/// `charset`/`collation` so the comparison uses the same collation as the column,
/// matching the read predicates. Fewer than two boundaries are trivially monotonic.
async fn boundaries_strictly_monotonic<Q>(
    conn: &mut Q,
    boundaries: &[String],
    charset: &str,
    collation: &str,
) -> Result<bool, TransientError>
where
    Q: Queryable,
{
    if boundaries.len() < 2 {
        return Ok(true);
    }
    // `charset`/`collation` come from `information_schema` and can't be bound as parameters,
    // so only interpolate the plain-identifier shape we expect. Anything else is unvalidated.
    if !is_plain_ident(charset) || !is_plain_ident(collation) {
        return Ok(false);
    }
    let terms = boundaries
        .iter()
        .map(|b| format!("CONVERT({b} USING {charset}) COLLATE {collation}"))
        .collect::<Vec<_>>();
    let predicate = terms
        .windows(2)
        .map(|w| format!("{} < {}", w[0], w[1]))
        .collect::<Vec<_>>()
        .join(" AND ");
    // Boundaries are MySQL-rendered literals and the identifiers are validated above,
    // so this interpolation is safe; not parameterizable.
    #[allow(clippy::disallowed_methods)]
    let ok: Option<i64> = conn.query_first(format!("SELECT {predicate}")).await?;
    Ok(ok == Some(1))
}

async fn verify_pk_bounds_monotonic<Q>(
    tx: &mut Q,
    tables: &BTreeMap<MySqlTableName, Vec<SourceOutputInfo>>,
    table_ranges: &BTreeMap<MySqlTableName, ReadPlan>,
    pk_bounds: &BTreeMap<MySqlTableName, Option<PkBoundaries>>,
) -> Result<(), TransientError>
where
    Q: Queryable,
{
    for (table, plan) in table_ranges {
        if !matches!(plan, ReadPlan::Range(_)) {
            continue;
        }
        let Some(Some(splits)) = pk_bounds.get(table) else {
            continue;
        };
        let Some((raw_col, _)) = tables
            .get(table)
            .and_then(|outputs| try_extract_single_column_pk(&outputs[0].desc))
        else {
            continue;
        };
        let Some((charset, collation)) = fetch_column_collation(tx, table, &raw_col).await? else {
            continue;
        };
        if !boundaries_strictly_monotonic(tx, &splits.boundaries, &charset, &collation).await? {
            return Err(TransientError::Generic(anyhow::anyhow!(
                "collation of {table} changed during snapshot setup"
            )));
        }
    }
    Ok(())
}

/// A plain SQL identifier: non-empty, only ASCII alphanumerics and underscores. Used to
/// gate charset/collation names before interpolating them (they can't be parameters).
fn is_plain_ident(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn is_decimal_literal(s: &str) -> bool {
    let digits = s.strip_prefix('-').unwrap_or(s);
    !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())
}

/// Returns the set of full tables/sections of tables to read.
fn plan_worker_reads(
    config: &RawSourceCreationConfig,
    tables: &BTreeMap<MySqlTableName, Vec<SourceOutputInfo>>,
    pk_bounds: &BTreeMap<MySqlTableName, Option<PkBoundaries>>,
) -> BTreeMap<MySqlTableName, ReadPlan> {
    tables
        .keys()
        .filter_map(|table| {
            let plan = match pk_bounds.get(table) {
                Some(Some(splits)) => worker_pk_range(
                    splits,
                    config.worker_id,
                    config.responsible_worker(table),
                    config.worker_count,
                )
                .map(ReadPlan::Range),
                Some(None) => config
                    .responsible_for(table)
                    .then_some(ReadPlan::WholeTable),
                None => panic!(
                    "Programmer error: tables absent from pk_bounds failed schema \
                     verification and are dropped before planning."
                ),
            };
            plan.map(|plan| (table.clone(), plan))
        })
        .collect()
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
    // The table infos to snapshot. Every worker holds all of them, since parallel
    // PK-range reads split each table across workers.
    let mut reader_snapshot_table_info = BTreeMap::new();
    // Maps MySQL table name to export `SourceStatistics`. Same info exists in reader_snapshot_table_info,
    // but this avoids having to iterate + map each time the statistics are needed.
    let mut export_statistics = BTreeMap::new();
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

        reader_snapshot_table_info
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

                if !all_outputs.is_empty() {
                    // A worker *must* emit a count even if not responsible for snapshotting a table
                    // as statistic summarization will return null if any worker hasn't set a value.
                    // This will also reset snapshot stats for any exports not snapshotting.
                    for statistics in config.statistics.values() {
                        statistics.set_snapshot_records_known(0);
                        statistics.set_snapshot_records_staged(0);
                    }
                }

                // If this worker has no tables to snapshot then there is nothing to do.
                if reader_snapshot_table_info.is_empty() {
                    trace!(%id, "timely-{worker_id} initializing table reader \
                                 with no tables to snapshot, exiting");
                    return Ok(());
                } else {
                    trace!(%id, "timely-{worker_id} initializing table reader \
                                 with {} tables to snapshot",
                           reader_snapshot_table_info.len());
                }

                let connection_config = connection
                    .connection
                    .config(
                        &config.config.connection_context.secrets_reader,
                        &config.config,
                        InTask::Yes,
                    )
                    .await?;
                let task_name = format!("timely-{worker_id} MySQL snapshotter");

                // Exact per-table row counts, computed once during PK sampling and reused
                // by the leader to publish the snapshot size gauge.
                let mut snapshot_counts: BTreeMap<MySqlTableName, u64> = BTreeMap::new();

                let mut conn = connection_config
                    .connect(
                        &task_name,
                        &config.config.connection_context.ssh_tunnel_manager,
                    )
                    .await?;

                // Verify the MySQL system settings are correct for consistent row-based replication using GTIDs
                match validate_mysql_repl_settings(&mut conn).await {
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

                let mut lock_conn = if is_snapshot_leader {
                    match lock_and_prepare_snapshot(
                        &config,
                        &connection_config,
                        &task_name,
                        &reader_snapshot_table_info,
                        &metrics,
                    )
                    .await
                    {
                        Ok((info, counts, conn)) => {
                            snapshot_counts = counts;
                            trace!(%id, "timely-{worker_id} broadcasting snapshot info: {info:?}");
                            snapshot_handle.give(&snapshot_cap_set[0], Some(info));
                            Some(conn)
                        }
                        Err(err) => {
                            // Broadcast the failure sentinel so non-leaders exit cleanly instead
                            // of deadlocking on the feedback loop.
                            snapshot_handle.give(&snapshot_cap_set[0], None);
                            match err {
                                SnapshotSetupError::Definite(e) => {
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
                                SnapshotSetupError::Transient(e) => return Err(e),
                            }
                        }
                    }
                } else {
                    None
                };

                // Receive the leader's broadcast: `Some` on success, `None` on leader failure.
                let snapshot_info: Option<SnapshotInfo> = loop {
                    match snapshot_input.next().await {
                        Some(AsyncEvent::Data(_, mut data)) => {
                            if let Some(msg) = data.pop() {
                                break msg;
                            }
                        }
                        Some(AsyncEvent::Progress(_)) => continue,
                        // Feedback closed without data: the leader failed and already
                        // propagated the error.
                        None => break None,
                    }
                };
                let snapshot_info = match snapshot_info {
                    Some(info) => info,
                    None => return Ok(()),
                };

                let errored: BTreeMap<usize, DefiniteError> =
                    snapshot_info.errored_outputs.iter().cloned().collect();
                let errored_outputs: Vec<_> = reader_snapshot_table_info
                    .values()
                    .flatten()
                    .filter_map(|output| {
                        errored.get(&output.output_index).map(|err| (output, err))
                    })
                    .collect();
                let mut removed_outputs = BTreeSet::new();
                for (output, err) in errored_outputs {
                    removed_outputs.insert(output.output_index);
                    // Only the responsible worker publishes any error,
                    // so it lands once instead of once per worker reading the table.
                    if !config.responsible_for(&output.table_name) {
                        continue;
                    }
                    let update = (
                        (output.output_index, Err(err.clone().into())),
                        GtidPartition::minimum(),
                        Diff::ONE,
                    );
                    let size = update.fuel_size();
                    raw_handle.give_fueled(&data_cap_set[0], update, size).await;
                    tracing::warn!(%id, "timely-{worker_id} stopping snapshot of output {output:?} \
                                due to schema mismatch");
                }
                for (_, outputs) in reader_snapshot_table_info.iter_mut() {
                    outputs.retain(|output| !removed_outputs.contains(&output.output_index));
                }
                reader_snapshot_table_info.retain(|_, outputs| !outputs.is_empty());

                let snapshot_gtid_frontier = match gtid_set_frontier(&snapshot_info.gtid_set) {
                    Ok(frontier) => frontier,
                    Err(err) => {
                        // If we received a GTID Set with non-consecutive intervals this breaks all
                        // our assumptions, so there is nothing else we can do.
                        return Ok(return_definite_error(
                            DefiniteError::UnsupportedGtidState(err.to_string()),
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

                let table_ranges = plan_worker_reads(
                    &config,
                    &reader_snapshot_table_info,
                    &snapshot_info.pk_bounds,
                );
                let has_work = !table_ranges.is_empty();

                // Returning will release the snapshot capabilities unblocking the leader from dropping the lock connection.
                if !has_work && !is_snapshot_leader {
                    trace!(%id, "timely-{worker_id} has no tables to snapshot.");
                    return Ok(());
                }

                trace!(%id, "timely-{worker_id} starting transaction with \
                             consistent snapshot at: {}", snapshot_gtid_frontier.pretty());

                // Start a transaction with REPEATABLE READ and 'CONSISTENT SNAPSHOT' semantics
                // so we can read a consistent snapshot of the table at the specific GTID we read.
                let mut tx_opts = TxOpts::default();
                tx_opts
                    .with_isolation_level(IsolationLevel::RepeatableRead)
                    .with_consistent_snapshot(true)
                    .with_readonly(true);
                let mut tx = conn.start_transaction(tx_opts).await?;
                // Set the session time zone to UTC so that we can read TIMESTAMP columns as UTC
                // From https://dev.mysql.com/doc/refman/8.0/en/datetime.html: "MySQL converts TIMESTAMP values
                // from the current time zone to UTC for storage, and back from UTC to the current time zone
                // for retrieval. (This does not occur for other types such as DATETIME.)"
                #[allow(clippy::disallowed_methods)] // static SQL string
                tx.query_drop("set @@session.time_zone = '+00:00'").await?;

                // Configure query execution time based on param. We want to be able to
                // override the server value here in case it's set too low,
                // respective to the size of the data we need to copy.
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

                // Signal readiness by dropping the snapshot capability, then the leader
                // waits for every worker to signal before unlocking.
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

                trace!(%id, "timely-{worker_id} started transaction (has_work={has_work}, is_snapshot_leader={is_snapshot_leader})");

                // Verify the schemas of the tables we are snapshotting
                let errored_outputs = verify_schemas(
                    &mut tx,
                    reader_snapshot_table_info
                        .iter()
                        .filter(|(t, _)| table_ranges.contains_key(t))
                        .map(|(k, v)| (k, v.as_slice()))
                        .collect(),
                )
                .await?;
                if let Some((output, err)) = errored_outputs.into_iter().next() {
                    return Err(TransientError::Generic(anyhow::anyhow!(
                        "schema of {} changed during snapshot setup: {err}",
                        output.table_name
                    )));
                }
                verify_pk_bounds_monotonic(
                    &mut tx,
                    &reader_snapshot_table_info,
                    &table_ranges,
                    &snapshot_info.pk_bounds,
                )
                .await?;

                // Only the leader publishes the full snapshot size, so the summed
                // worker-local gauges reflect the upstream total without double-counting.
                // The counts were computed once during PK sampling and are reused here.
                if is_snapshot_leader {
                    publish_snapshot_size(
                        &snapshot_counts,
                        &reader_snapshot_table_info,
                        &export_statistics,
                    );
                }

                // This worker has nothing else to do
                if reader_snapshot_table_info.is_empty() {
                    return Ok(());
                }

                // Read the snapshot data from the tables
                let mut final_row = Row::default();

                let mut snapshot_staged_total = 0;
                for (table, outputs) in &reader_snapshot_table_info {
                    let pk_range = match table_ranges.get(table) {
                        Some(ReadPlan::Range(range)) => Some(range),
                        Some(ReadPlan::WholeTable) => None,
                        // This worker has no work for this table.
                        None => continue,
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
                        // This overcounting maintains existing behavior but will be removed once readers no longer rely on the value.
                        snapshot_staged_total += u64::cast_from(outputs.len());
                        if snapshot_staged_total % 1000 == 0 {
                            for statistics in export_statistics.get(table).unwrap() {
                                statistics.set_snapshot_records_staged(snapshot_staged);
                            }
                        }
                    }
                    for statistics in export_statistics.get(table).unwrap() {
                        statistics.set_snapshot_records_staged(snapshot_staged);
                    }
                    trace!(%id, "timely-{worker_id} snapshotted {} records from \
                                 table '{table}'", snapshot_staged * u64::cast_from(outputs.len()));
                }

                // We are done with the snapshot so now we will emit rewind requests. It is
                // important that this happens after the snapshot has finished because this is what
                // unblocks the replication operator and we want this to happen serially. It might
                // seem like a good idea to read the replication stream concurrently with the
                // snapshot but it actually leads to a lot of data being staged for the future,
                // which needlesly consumed memory in the cluster.
                for (table, outputs) in &reader_snapshot_table_info {
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

/// Publish the exact snapshot size to each table's statistics gauges, using the counts
/// computed once during PK sampling. Called leader-only so the summed worker-local gauges
/// reflect the upstream total without double-counting.
fn publish_snapshot_size(
    counts: &BTreeMap<MySqlTableName, u64>,
    tables: &BTreeMap<MySqlTableName, Vec<SourceOutputInfo>>,
    export_statistics: &BTreeMap<MySqlTableName, Vec<SourceStatistics>>,
) {
    for name in tables.keys() {
        let count = counts.get(name).copied().unwrap_or(0);
        let stats = export_statistics
            .get(name)
            .expect("statistics are initialized for each output");
        for export_stat in stats {
            export_stat.set_snapshot_records_known(count);
            export_stat.set_snapshot_records_staged(0);
        }
    }
}

enum SnapshotSetupError {
    Definite(DefiniteError),
    Transient(TransientError),
}

impl From<mysql_async::Error> for SnapshotSetupError {
    fn from(e: mysql_async::Error) -> Self {
        SnapshotSetupError::Transient(e.into())
    }
}

impl From<MySqlError> for SnapshotSetupError {
    fn from(e: MySqlError) -> Self {
        SnapshotSetupError::Transient(e.into())
    }
}

fn classify_query_error(e: mysql_async::Error) -> SnapshotSetupError {
    match e {
        mysql_async::Error::Server(mysql_async::ServerError { code, message, .. })
            if code == ER_NO_SUCH_TABLE =>
        {
            SnapshotSetupError::Definite(DefiniteError::TableDropped(message))
        }
        e => SnapshotSetupError::Transient(e.into()),
    }
}

async fn lock_tables_and_read_gtid_set(
    lock_conn: &mut MySqlConn,
    lock_clauses: &str,
    lock_wait_timeout: Option<Duration>,
) -> Result<String, SnapshotSetupError> {
    if let Some(timeout) = lock_wait_timeout {
        // Interpolating a `Duration` integer; not parameterizable in MySQL `SET`.
        #[allow(clippy::disallowed_methods)]
        lock_conn
            .query_drop(format!(
                "SET @@session.lock_wait_timeout = {}",
                timeout.as_secs()
            ))
            .await?;
    }

    // `lock_clauses` is built from `MySqlTableName::Display`, which escapes both
    // schema and table via `quote_identifier`.
    #[allow(clippy::disallowed_methods)]
    lock_conn
        .query_drop(format!("LOCK TABLES {lock_clauses}"))
        .await
        .map_err(classify_query_error)?;

    let snapshot_gtid_set = query_sys_var(lock_conn, "global.gtid_executed").await?;
    Ok(snapshot_gtid_set)
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
        // Half-open range on the PK column. The first/last partition omits its
        // open bound.
        let col = &range.pk_col;
        if let Some(lower) = &range.lower {
            query.push_str(&format!(" WHERE {col} >= {lower}"));
        }
        if let Some(upper) = &range.upper {
            let kw = if range.lower.is_some() {
                "AND"
            } else {
                "WHERE"
            };
            query.push_str(&format!(" {kw} {col} < {upper}"));
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
) -> Result<TableStatistics, SnapshotSetupError>
where
    Q: Queryable,
{
    let mut stats = TableStatistics::default();

    // `MySqlTableName::Display` escapes both identifier components via
    // `quote_identifier`, so this interpolation is safe; not parameterizable.
    // `classify_query_error` turns a dropped table into a definite error, matching
    // the boundary-sampling query rather than retrying forever.
    #[allow(clippy::disallowed_methods)]
    let count_row: Option<u64> = conn
        .query_first(format!("SELECT COUNT(*) FROM {}", table))
        .wall_time()
        .set_at(&mut stats.count_latency)
        .await
        .map_err(classify_query_error)?;
    // `COUNT(*)` returns exactly one row, so `None` should be impossible. Default to 0
    // defensively rather than failing the snapshot on a protocol quirk.
    stats.count = count_row.unwrap_or(0);

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

        // Middle worker: both bounds.
        let range = PkRange {
            pk_col: "`id`".to_string(),
            lower: Some("100".to_string()),
            upper: Some("200".to_string()),
        };
        let query = build_snapshot_query(std::slice::from_ref(&info), Some(&range));
        assert_eq!(
            format!(
                "SELECT `id`, `name` FROM `{}`.`{}` WHERE `id` >= 100 AND `id` < 200",
                &schema_name, &table_name
            ),
            query
        );

        // First worker: open start.
        let range = PkRange {
            pk_col: "`id`".to_string(),
            lower: None,
            upper: Some("200".to_string()),
        };
        let query = build_snapshot_query(std::slice::from_ref(&info), Some(&range));
        assert_eq!(
            format!(
                "SELECT `id`, `name` FROM `{}`.`{}` WHERE `id` < 200",
                &schema_name, &table_name
            ),
            query
        );

        // Last worker: open end.
        let range = PkRange {
            pk_col: "`id`".to_string(),
            lower: Some("200".to_string()),
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
        // Two partitions, boundary at 51. Owner 0 is the identity mapping
        // (partition == worker_id).
        let splits = PkBoundaries {
            pk_col: "`id`".to_string(),
            boundaries: vec!["51".to_string()],
        };
        let r0 = worker_pk_range(&splits, 0, 0, 4).expect("worker 0");
        assert_eq!(r0.pk_col, "`id`");
        assert_eq!(r0.lower, None); // open start
        assert_eq!(r0.upper.as_deref(), Some("51"));
        let r1 = worker_pk_range(&splits, 1, 0, 4).expect("worker 1");
        assert_eq!(r1.lower.as_deref(), Some("51"));
        assert_eq!(r1.upper, None); // open end
        // A surplus worker beyond the partition count has no work.
        assert!(worker_pk_range(&splits, 2, 0, 4).is_none());

        // Three partitions: the middle worker has both bounds.
        let splits = PkBoundaries {
            pk_col: "`id`".to_string(),
            boundaries: vec!["34".to_string(), "67".to_string()],
        };
        let r1 = worker_pk_range(&splits, 1, 0, 3).expect("worker 1");
        assert_eq!(r1.lower.as_deref(), Some("34"));
        assert_eq!(r1.upper.as_deref(), Some("67"));

        // Offsetting by the owner still assigns every worker a distinct partition,
        // and the owner reads the open-started first partition.
        let owner = 2;
        let owned = worker_pk_range(&splits, owner, owner, 3).expect("owner has work");
        assert_eq!(owned.lower, None);
        let mut ranges: Vec<_> = (0..3)
            .map(|w| {
                let r = worker_pk_range(&splits, w, owner, 3).expect("worker has work");
                (r.lower, r.upper)
            })
            .collect();
        ranges.sort();
        assert_eq!(
            ranges,
            vec![
                (None, Some("34".to_string())),
                (Some("34".to_string()), Some("67".to_string())),
                (Some("67".to_string()), None),
            ]
        );
    }

    #[mz_ore::test]
    fn test_single_column_pk() {
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

        // Single-column PK: returns the raw column name and its type.
        let (name, ty) = try_extract_single_column_pk(&desc(
            vec![col("id", SqlScalarType::Char { length: None })],
            pk(&["id"]),
        ))
        .expect("single-column pk");
        assert_eq!(name, "id");
        assert!(matches!(ty, SqlScalarType::Char { .. }));

        let (name, ty) =
            try_extract_single_column_pk(&desc(vec![col("id", SqlScalarType::Bytes)], pk(&["id"])))
                .expect("single-column pk");
        assert_eq!(name, "id");
        assert!(matches!(ty, SqlScalarType::Bytes));

        // Composite PK → not a single column, fall back.
        assert!(
            try_extract_single_column_pk(&desc(
                vec![
                    col("a", SqlScalarType::Char { length: None }),
                    col("b", SqlScalarType::Int64),
                ],
                pk(&["a", "b"]),
            ))
            .is_none()
        );

        // No primary key → fall back.
        assert!(
            try_extract_single_column_pk(&desc(
                vec![col("id", SqlScalarType::Int64)],
                BTreeSet::default()
            ))
            .is_none()
        );
    }
}
