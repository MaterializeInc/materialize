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
use std::sync::Arc;

use differential_dataflow::AsCollection;
use futures::TryStreamExt;
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
use mz_repr::{Diff, Row};
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sources::MySqlSourceConnection;
use mz_storage_types::sources::mysql::{GtidPartition, gtid_set_frontier};
use mz_timely_util::antichain::AntichainExt;
use mz_timely_util::builder_async::{OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton};
use mz_timely_util::containers::stack::AccountedStackBuilder;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::operators::core::Map;
use timely::dataflow::operators::{CapabilitySet, Concat};
use timely::dataflow::{Scope, Stream};
use timely::progress::Timestamp;
use tracing::{error, trace};

use crate::metrics::source::mysql::MySqlSnapshotMetrics;
use crate::source::RawSourceCreationConfig;
use crate::source::types::{SignaledFuture, SourceMessage, StackedCollection};
use crate::statistics::SourceStatistics;

use super::schemas::verify_schemas;
use super::{
    DefiniteError, MySqlTableName, ReplicationError, RewindRequest, SourceOutputInfo,
    TransientError, return_definite_error, validate_mysql_repl_settings,
};

/// Renders the snapshot dataflow. See the module documentation for more information.
pub(crate) fn render<G: Scope<Timestamp = GtidPartition>>(
    scope: G,
    config: RawSourceCreationConfig,
    connection: MySqlSourceConnection,
    source_outputs: Vec<SourceOutputInfo>,
    metrics: MySqlSnapshotMetrics,
) -> (
    StackedCollection<G, (usize, Result<SourceMessage, DataflowError>)>,
    Stream<G, RewindRequest>,
    Stream<G, ReplicationError>,
    PressOnDropButton,
) {
    let mut builder =
        AsyncOperatorBuilder::new(format!("MySqlSnapshotReader({})", config.id), scope.clone());

    let (raw_handle, raw_data) = builder.new_output::<AccountedStackBuilder<_>>();
    let (rewinds_handle, rewinds) = builder.new_output::<CapacityContainerBuilder<_>>();
    // Captures DefiniteErrors that affect the entire source, including all outputs
    let (definite_error_handle, definite_errors) =
        builder.new_output::<CapacityContainerBuilder<_>>();

    // A global view of all outputs that will be snapshot by all workers.
    let mut all_outputs = vec![];
    // A map containing only the table infos that this worker should snapshot.
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
        if config.responsible_for(&output.table_name) {
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
    }

    let (button, transient_errors): (_, Stream<G, Rc<TransientError>>) =
        builder.build_fallible(move |caps| {
            let busy_signal = Arc::clone(&config.busy_signal);
            Box::pin(SignaledFuture::new(busy_signal, async move {
                let [data_cap_set, rewind_cap_set, definite_error_cap_set]: &mut [_; 3] =
                    caps.try_into().unwrap();

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

                let lock_clauses = reader_snapshot_table_info
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
                    // Handle the case where a table we are snapshotting has been dropped or renamed.
                    Err(mysql_async::Error::Server(mysql_async::ServerError {
                        code,
                        message,
                        ..
                    })) if code == ER_NO_SUCH_TABLE => {
                        trace!(%id, "timely-{worker_id} received unknown table error from \
                                     lock query");
                        let err = DefiniteError::TableDropped(message);
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
                            &raw_handle,
                            data_cap_set,
                            &definite_error_handle,
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

                // Configure query execution time based on param. We want to be able to
                // override the server value here in case it's set too low,
                // respective to the size of the data we need to copy.
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

                // We have started our transaction so we can unlock the tables.
                lock_conn.query_drop("UNLOCK TABLES").await?;
                lock_conn.disconnect().await?;

                trace!(%id, "timely-{worker_id} started transaction");

                // Verify the schemas of the tables we are snapshotting
                let errored_outputs =
                    verify_schemas(&mut tx, reader_snapshot_table_info.iter().collect()).await?;
                let mut removed_outputs = BTreeSet::new();
                for (output, err) in errored_outputs {
                    // Publish the error for this table and stop ingesting it
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
                    trace!(%id, "timely-{worker_id} stopping snapshot of output {output:?} \
                                due to schema mismatch");
                    removed_outputs.insert(output.output_index);
                }
                for (_, outputs) in reader_snapshot_table_info.iter_mut() {
                    outputs.retain(|output| !removed_outputs.contains(&output.output_index));
                }
                reader_snapshot_table_info.retain(|_, outputs| !outputs.is_empty());

                let snapshot_total = fetch_snapshot_size(
                    &mut tx,
                    reader_snapshot_table_info
                        .iter()
                        .map(|(name, outputs)| {
                            (
                                name.clone(),
                                outputs.len(),
                                export_statistics.get(name).unwrap(),
                            )
                        })
                        .collect(),
                    metrics,
                )
                .await?;

                // This worker has nothing else to do
                if reader_snapshot_table_info.is_empty() {
                    return Ok(());
                }

                // Read the snapshot data from the tables
                let mut final_row = Row::default();

                let mut snapshot_staged_total = 0;
                for (table, outputs) in &reader_snapshot_table_info {
                    let mut snapshot_staged = 0;
                    let query = build_snapshot_query(outputs);
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
                        // This overcounting maintains existing behavior but will be removed one readers no longer rely on the value.
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
                for (table, outputs) in reader_snapshot_table_info {
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

                // TODO (maz): Should we remove this to match Postgres?
                if snapshot_staged_total < snapshot_total {
                    error!(%id, "timely-{worker_id} snapshot size {snapshot_total} is somehow \
                                 bigger than records staged {snapshot_staged_total}");
                }

                Ok(())
            }))
        });

    // TODO: Split row decoding into a separate operator that can be distributed across all workers

    let errors = definite_errors.concat(&transient_errors.map(ReplicationError::from));

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
#[must_use]
fn build_snapshot_query(outputs: &[SourceOutputInfo]) -> String {
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
    format!("SELECT {} FROM {}", columns, info.table_name)
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
        let query = build_snapshot_query(&[info.clone(), info]);
        assert_eq!(
            format!(
                "SELECT `c1`, `c2`, `c3` FROM `{}`.`{}`",
                &schema_name, &table_name
            ),
            query
        );
    }
}
