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
//! Depending on the `subsource_resume_uppers` parameter this dataflow decides which tables to
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
//! The full set of tables that are meant to be snapshotted are partitioned among the workers. Each
//! worker initiates a connection to the server and acquires a table lock on all the tables that
//! have been assigned to it. By doing so we establish a moment in time where we know no writes are
//! happening to the tables we are interested in. After the locks are taken each worker reads the
//! current upper frontier (`snapshot_upper`) using the `@@gtid_executed` system variable. This
//! frontier establishes an upper bound on any possible write to the tables of interest until the
//! lock is released.
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
//! At this point it is safe for each worker to unlock the tables, since the transaction has
//! established a point in time, and close the initial connection. Each worker can then read the
//! snapshot of the tables it is responsible for and publish it downstream.
//!
//! TODO: Other software products hold the table lock for the duration of the snapshot, and some do
//! not. We should figure out why and if we need to hold the lock longer. This may be because of a
//! difference in how REPEATABLE READ works in some MySQL-compatible systems (e.g. Aurora MySQL).
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

use differential_dataflow::{AsCollection, Collection};
use futures::TryStreamExt;
use mysql_async::prelude::Queryable;
use mysql_async::{IsolationLevel, Row as MySqlRow, TxOpts};
use mz_timely_util::antichain::AntichainExt;
use timely::dataflow::operators::{CapabilitySet, Concat, Map};
use timely::dataflow::{Scope, Stream};
use timely::progress::{Antichain, Timestamp};
use tracing::trace;

use mz_mysql_util::{pack_mysql_row, query_sys_var, MySqlTableDesc, ER_NO_SUCH_TABLE};
use mz_ore::metrics::MetricsFutureExt;
use mz_ore::result::ResultExt;
use mz_repr::{Diff, GlobalId, Row};
use mz_sql_parser::ast::UnresolvedItemName;
use mz_sql_parser::ast::{display::AstDisplay, Ident};
use mz_storage_types::sources::mysql::{gtid_set_frontier, GtidPartition};
use mz_storage_types::sources::MySqlSourceConnection;
use mz_timely_util::builder_async::{OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton};

use crate::metrics::source::mysql::MySqlSnapshotMetrics;
use crate::source::{RawSourceCreationConfig, SourceReaderError};

use super::schemas::verify_schemas;
use super::{
    return_definite_error, validate_mysql_repl_settings, DefiniteError, ReplicationError,
    RewindRequest, TransientError,
};

/// Renders the snapshot dataflow. See the module documentation for more information.
pub(crate) fn render<G: Scope<Timestamp = GtidPartition>>(
    scope: G,
    config: RawSourceCreationConfig,
    connection: MySqlSourceConnection,
    subsource_resume_uppers: BTreeMap<GlobalId, Antichain<GtidPartition>>,
    table_info: BTreeMap<UnresolvedItemName, (usize, MySqlTableDesc)>,
    metrics: MySqlSnapshotMetrics,
) -> (
    Collection<G, (usize, Result<Row, SourceReaderError>), Diff>,
    Stream<G, RewindRequest>,
    Stream<G, ReplicationError>,
    PressOnDropButton,
) {
    let mut builder =
        AsyncOperatorBuilder::new(format!("MySqlSnapshotReader({})", config.id), scope.clone());

    let (mut raw_handle, raw_data) = builder.new_output();
    let (mut rewinds_handle, rewinds) = builder.new_output();
    // Captures DefiniteErrors that affect the entire source, including all subsources
    let (mut definite_error_handle, definite_errors) = builder.new_output();

    // A global view of all exports that need to be snapshot by all workers. Note that this affects
    // `reader_snapshot_table_info` but must be kept separate from it because each worker needs to
    // understand if any worker is snapshotting any subsource.
    let export_indexes_to_snapshot: BTreeSet<_> = subsource_resume_uppers
        .into_iter()
        .filter_map(|(id, upper)| {
            // Determined which collections need to be snapshot and which already have been.
            if id != config.id && *upper == [GtidPartition::minimum()] {
                // Convert from `GlobalId` to output index.
                Some(config.source_exports[&id].output_index)
            } else {
                None
            }
        })
        .collect();

    let mut all_outputs = vec![];
    // A map containing only the table infos that this worker should snapshot.
    let mut reader_snapshot_table_info = BTreeMap::new();

    for (table, val) in table_info {
        mz_ore::soft_assert_or_log!(
            val.0 != 0,
            "primary collection should not be represented in table info"
        );
        if !export_indexes_to_snapshot.contains(&val.0) {
            continue;
        }
        all_outputs.push(val.0);
        if config.responsible_for(&table) {
            reader_snapshot_table_info.insert(table, val);
        }
    }

    let (button, transient_errors): (_, Stream<G, Rc<TransientError>>) =
        builder.build_fallible(move |caps| {
            Box::pin(async move {
                let [data_cap_set, rewind_cap_set, definite_error_cap_set]: &mut [_; 3] =
                    caps.try_into().unwrap();

                let id = config.id;
                let worker_id = config.worker_id;

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
                        &*config.config.connection_context.secrets_reader,
                        &config.config,
                    )
                    .await?;
                let task_name = format!("timely-{worker_id} MySQL snapshotter");

                let lock_clauses = reader_snapshot_table_info
                    .keys()
                    .map(|t| format!("{} READ", t.to_ast_string()))
                    .collect::<Vec<String>>()
                    .join(", ");
                let mut lock_conn = connection_config
                    .connect(
                        &task_name,
                        &config.config.connection_context.ssh_tunnel_manager,
                    )
                    .await?;

                trace!(%id, "timely-{worker_id} acquiring table locks: {lock_clauses}");
                match lock_conn
                    .query_drop(format!("LOCK TABLES {lock_clauses}"))
                    .await
                {
                    // Handle the case where a table we are snapshotting has been dropped or renamed.
                    Err(mysql_async::Error::Server(mysql_async::ServerError {
                        code,
                        message,
                        ..
                    })) if code == ER_NO_SUCH_TABLE => {
                        let err = DefiniteError::TableDropped(message);
                        return Ok(return_definite_error(
                            err,
                            &all_outputs,
                            &mut raw_handle,
                            data_cap_set,
                            &mut definite_error_handle,
                            definite_error_cap_set,
                        )
                        .await);
                    }
                    e => e?,
                };

                // Record the frontier of future GTIDs based on the executed GTID set at the start
                // of the snapshot
                let snapshot_gtid_set =
                    query_sys_var(&mut lock_conn, "global.gtid_executed").await?;
                let snapshot_gtid_frontier = match gtid_set_frontier(&snapshot_gtid_set) {
                    Ok(frontier) => frontier,
                    Err(err) => {
                        let err = DefiniteError::UnsupportedGtidState(err.to_string());
                        // If we received a GTID Set with non-consecutive intervals this breaks all
                        // our assumptions, so there is nothing else we can do.
                        return Ok(return_definite_error(
                            err,
                            &all_outputs,
                            &mut raw_handle,
                            data_cap_set,
                            &mut definite_error_handle,
                            definite_error_cap_set,
                        )
                        .await);
                    }
                };

                // TODO(roshan): Insert metric for how long it took to acquire the locks
                trace!(%id, "timely-{worker_id} acquired table locks at: {}",
                       snapshot_gtid_frontier.pretty());

                let mut conn = connection_config
                    .connect(
                        &task_name,
                        &config.config.connection_context.ssh_tunnel_manager,
                    )
                    .await?;

                // Verify the MySQL system settings are correct for consistent row-based replication using GTIDs
                if let Err(err) = validate_mysql_repl_settings(&mut conn).await {
                    return Ok(return_definite_error(
                        DefiniteError::ServerConfigurationError(err.to_string()),
                        &all_outputs,
                        &mut raw_handle,
                        data_cap_set,
                        &mut definite_error_handle,
                        definite_error_cap_set,
                    )
                    .await);
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
                tx.query_drop("set @@session.time_zone = '+00:00'").await?;

                // We have started our transaction so we can unlock the tables.
                lock_conn.query_drop("UNLOCK TABLES").await?;
                lock_conn.disconnect().await?;

                trace!(%id, "timely-{worker_id} started transaction");

                // Verify the schemas of the tables we are snapshotting
                let errored_tables = verify_schemas(
                    &mut tx,
                    &reader_snapshot_table_info
                        .iter()
                        .map(|(table, (_, desc))| (table, desc))
                        .collect::<Vec<_>>(),
                )
                .await?;
                let mut removed_tables = vec![];
                for (table, err) in errored_tables {
                    let (output_index, _) = reader_snapshot_table_info.get(table).unwrap();
                    // Publish the error for this table and stop ingesting it
                    raw_handle
                        .give(
                            &data_cap_set[0],
                            (
                                (*output_index, Err(err.clone())),
                                GtidPartition::minimum(),
                                1,
                            ),
                        )
                        .await;
                    trace!(%id, "timely-{worker_id} stopping snapshot of table {table} \
                                    due to schema mismatch");
                    removed_tables.push(table.clone());
                }
                for table in removed_tables {
                    reader_snapshot_table_info.remove(&table);
                }
                // This worker has nothing else to do
                if reader_snapshot_table_info.is_empty() {
                    return Ok(());
                }

                // We have established a snapshot frontier so we can broadcast the rewind requests
                for table in reader_snapshot_table_info.keys() {
                    trace!(%id, "timely-{worker_id} producing rewind request for {table}");
                    let req = RewindRequest {
                        table: table.clone(),
                        snapshot_upper: snapshot_gtid_frontier.clone(),
                    };
                    rewinds_handle.give(&rewind_cap_set[0], req).await;
                }
                *rewind_cap_set = CapabilitySet::new();

                record_table_sizes(
                    &mut tx,
                    metrics,
                    reader_snapshot_table_info
                        .values()
                        .map(|(_, table_desc)| {
                            (
                                Ident::new_unchecked(table_desc.name.clone()).to_ast_string(),
                                Ident::new_unchecked(table_desc.schema_name.clone())
                                    .to_ast_string(),
                            )
                        })
                        .collect(),
                )
                .await?;

                // Read the snapshot data from the tables
                let mut final_row = Row::default();
                for (table, (output_index, table_desc)) in reader_snapshot_table_info {
                    let query = format!("SELECT * FROM {}", table.to_ast_string());
                    trace!(%id, "timely-{worker_id} reading snapshot from \
                                 table '{table}':\n{table_desc:?}");
                    let mut results = tx.exec_stream(query, ()).await?;
                    while let Some(row) = results.try_next().await? {
                        let row: MySqlRow = row;
                        let packed_row = pack_mysql_row(&mut final_row, row, &table_desc)?;
                        raw_handle
                            .give(
                                &data_cap_set[0],
                                ((output_index, Ok(packed_row)), GtidPartition::minimum(), 1),
                            )
                            .await;
                    }
                }
                Ok(())
            })
        });

    // TODO: Split row decoding into a separate operator that can be distributed across all workers
    let snapshot_updates = raw_data
        .as_collection()
        .map(move |(output_index, event)| (output_index, event.err_into()));

    let errors = definite_errors.concat(&transient_errors.map(ReplicationError::from));

    (snapshot_updates, rewinds, errors, button.press_on_drop())
}

/// Record the sizes of the tables being snapshotted in `MySqlSnapshotMetrics`.
async fn record_table_sizes<Q>(
    conn: &mut Q,
    metrics: MySqlSnapshotMetrics,
    // Tables and their schemas.
    tables: Vec<(String, String)>,
) -> Result<(), anyhow::Error>
where
    Q: Queryable,
{
    for (table, schema) in tables {
        let stats = collect_table_statistics(conn, &table, &schema).await?;
        if let Some(estimate) = stats.estimate_count {
            metrics.record_table_count(
                table.clone(),
                schema.clone(),
                estimate,
                stats.estimate_latency,
            );
        }
        if let Some(count) = stats.count {
            metrics.record_table_estimate(
                table.clone(),
                schema.clone(),
                count,
                stats.count_latency,
            );
        }
    }
    Ok(())
}

#[derive(Default)]
struct TableStatistics {
    count_latency: f64,
    count: Option<u64>,
    estimate_latency: f64,
    estimate_count: Option<u64>,
}

async fn collect_table_statistics<Q>(
    conn: &mut Q,
    table: &str,
    schema: &str,
) -> Result<TableStatistics, anyhow::Error>
where
    Q: Queryable,
{
    let mut stats = TableStatistics::default();

    let estimate_row: Option<u64> = conn
        .query_first(format!(
            "SELECT TABLE_ROWS FROM INFORMATION_SCHEMA.TABLES \
            WHERE TABLE_NAME = '{table}' AND TABLE_SCHEMA = '{schema}'"
        ))
        .wall_time()
        .set_at(&mut stats.estimate_latency)
        .await?;
    stats.estimate_count = estimate_row;

    let count_row: Option<u64> = conn
        .query_first(format!("SELECT COUNT(*) FROM {schema}.{table}"))
        .wall_time()
        .set_at(&mut stats.count_latency)
        .await?;
    stats.count = count_row;

    Ok(stats)
}
