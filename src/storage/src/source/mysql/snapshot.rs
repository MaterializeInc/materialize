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
//! ## Locking for a consistent GTID Set at the snapshot point
//!
//! Given that all our ingestion is based on correctly timestamping updates with the GTID of the
//! transaction that produced them, it is important that we snapshot the tables at a consistent
//! "GTID Set" (the full set of all transactions committed on the MySQL server at that point) that
//! is relatable to the GTID Set frontier we track when reading the replication stream.
//!
//! To achieve this we must ensure that all workers snapshot the tables at the same GTID Set. We
//! designate a snapshot leader worker that starts by acquiring a table lock on all tables to be
//! snapshot and then reading the @@gtid_executed "GTID Set" at that point. Once the GTID Set is
//! read, the leader sends a signal to all workers to start their transactions.
//!
//! Each worker starts a new transaction with 'REPEATABLE READ' and 'CONSISTENT SNAPSHOT' semantics
//! so that they can read a consistent snapshot of the table at the specific GTID Set the
//! transaction was started from.
//!
//! Once all workers have started their transactions, they send a signal back to the leader to
//! release the table locks. This ensures that all workers see a consistent view of the tables
//! starting from the same GTID. The workers then read the snapshot data and publish it downstream.
//!
//! TODO: Other software products hold the table lock for the duration of the snapshot, and some do
//! not. We should figure out why and if we need to hold the lock longer. This may be because of a
//! difference in how REPEATABLE READ works in some MySQL-compatible systems (e.g. Aurora MySQL).
//!
//! ## Snapshot rewinding
//!
//! The snapshot reader also produces a stream of `RewindRequest` messages that are used to rewind
//! the replication stream to the point in time of the snapshot. This is necessary because the point
//! in time of the snapshot is not necessarily the same as the point in time of the start of the
//! replication stream. The replication stream may be started from an earlier point in time, and
//! the updates that occurred between the start of the replication stream and the snapshot must be
//! negated to avoid double-counting them.
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
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{Broadcast, CapabilitySet, Concat, ConnectLoop, Feedback, Map};
use timely::dataflow::{Scope, Stream};
use timely::progress::{Antichain, Timestamp};
use tracing::{trace, warn};

use mz_mysql_util::{query_sys_var, SchemaRequest};
use mz_mysql_util::{schema_info, MySqlTableDesc};
use mz_ore::cast::CastFrom;
use mz_ore::result::ResultExt;
use mz_repr::{Diff, GlobalId, Row};
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::UnresolvedItemName;
use mz_storage_types::sources::mysql::{gtid_set_frontier, GtidPartition};
use mz_storage_types::sources::MySqlSourceConnection;
use mz_timely_util::builder_async::{
    Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};

use crate::source::{RawSourceCreationConfig, SourceReaderError};

use super::{
    pack_mysql_row, return_definite_error, validate_mysql_repl_settings, DefiniteError,
    ReplicationError, RewindRequest, TransientError,
};

/// Renders the snapshot dataflow. See the module documentation for more information.
pub(crate) fn render<G: Scope<Timestamp = GtidPartition>>(
    mut scope: G,
    config: RawSourceCreationConfig,
    connection: MySqlSourceConnection,
    subsource_resume_uppers: BTreeMap<GlobalId, Antichain<GtidPartition>>,
    table_info: BTreeMap<UnresolvedItemName, (usize, MySqlTableDesc)>,
) -> (
    Collection<G, (usize, Result<Row, SourceReaderError>), Diff>,
    Stream<G, RewindRequest>,
    Stream<G, ReplicationError>,
    PressOnDropButton,
) {
    let mut builder =
        AsyncOperatorBuilder::new(format!("MySqlSnapshotReader({})", config.id), scope.clone());

    let snapshot_leader_id = config.responsible_worker("snapshot_leader");
    let is_snapshot_leader = config.worker_id == snapshot_leader_id;
    let snapshot_leader_id = u64::cast_from(snapshot_leader_id);

    let (mut raw_handle, raw_data) = builder.new_output();
    let (mut rewinds_handle, rewinds) = builder.new_output();
    let (mut definite_error_handle, definite_errors) = builder.new_output();

    // Broadcast a signal from the snapshot leader worker to the other workers when the table
    // lock is in place. Upon receiving the first message the workers should start a transaction
    // that guarantees they all see a consistent view from the same GTID.
    let (mut lock_start_handle, lock_start) = builder.new_output();
    let (ls_feedback_handle, ls_feedback_data) = scope.feedback(Default::default());
    let mut lock_start_input = builder.new_disconnected_input(&ls_feedback_data, Pipeline);
    lock_start.broadcast().connect_loop(ls_feedback_handle);

    // Broadcast a signal from the all workers that they have begun a transaction and that the
    // table lock held by the snapshot leader can be released
    let (_, transaction_start) = builder.new_output();
    let (ts_feedback_handle, ts_feedback_data) = scope.feedback(Default::default());
    let mut transaction_start_input = builder.new_disconnected_input(
        &ts_feedback_data,
        Exchange::new(move |_: &()| snapshot_leader_id),
    );
    transaction_start
        .broadcast()
        .connect_loop(ts_feedback_handle);

    // A global view of all exports that need to be snapshot by all workers. Note that this affects
    // `reader_snapshot_table_info` but must be kept separate from it because each worker needs to
    // understand if any worker is snapshotting any subsource.
    let export_indexes_to_snapshot: BTreeSet<_> = config
        .source_exports
        .iter()
        .enumerate()
        .filter_map(|(output_idx, (id, export))| {
            if export.input_index.is_some()
                && *subsource_resume_uppers[id] == [GtidPartition::minimum()]
            {
                Some(output_idx)
            } else {
                None
            }
        })
        .collect();

    let mut all_table_names = vec![];
    let mut all_outputs = vec![];
    // A map containing only the table infos that this worker should snapshot.
    let mut reader_snapshot_table_info = BTreeMap::new();

    for (table, val) in table_info {
        if !export_indexes_to_snapshot.contains(&val.0) {
            continue;
        }
        all_table_names.push(table.clone());
        all_outputs.push(val.0);
        if config.responsible_for(&table) {
            reader_snapshot_table_info.insert(table, val);
        }
    }

    let (button, transient_errors): (_, Stream<G, Rc<TransientError>>) =
        builder.build_fallible(move |caps| {
            Box::pin(async move {
                let id = config.id;
                let worker_id = config.worker_id;

                let [
                    data_cap_set,
                    rewind_cap_set,
                    definite_error_cap_set,
                    lock_start_cap_set,
                    transaction_start_cap_set
                ]: &mut [_; 5] = caps.try_into().unwrap();

                trace!(%id, "timely-{worker_id} initializing table reader \
                             with {} tables to snapshot",
                       reader_snapshot_table_info.len());

                // Nothing needs to be snapshot.
                if all_table_names.is_empty() {
                    trace!(%id, "no exports to snapshot");
                    return Ok(());
                }

                let connection_config = connection
                    .connection
                    .config(
                        &*config.config.connection_context.secrets_reader,
                        &config.config,
                    )
                    .await?;
                let task_name = format!("timely-{worker_id} MySQL snapshotter");

                // The snapshot leader is responsible for ensuring a consistent snapshot of
                // all tables that need to be snapshot. The leader will create a new 'lock connection'
                // that takes a table lock on all tables to be snapshot and then read the active GTID
                // at that time.
                //
                // Once this lock is acquired, the leader is responsible for sending a signal to all
                // workers that they should start their own transactions. The workers will then start a
                // transaction with REPEATABLE READ and 'CONSISTENT SNAPSHOT' semantics.
                // Once each worker has started their transaction, they will send a signal to the
                // leader that the table lock can be released. This will allow further writes to the
                // tables by other clients to occur, while ensuring that the snapshot workers all see
                // a consistent view of the tables starting from the same GTID.
                let mut lock_conn = None;
                if is_snapshot_leader {
                    let lock_clauses = all_table_names
                        .iter()
                        .map(|t| format!("{} READ", t.to_ast_string()))
                        .collect::<Vec<String>>()
                        .join(", ");
                    let leader_task_name = format!("{} leader", task_name);
                    lock_conn = Some(Box::new(
                        connection_config
                            .connect(
                                &leader_task_name,
                                &config.config.connection_context.ssh_tunnel_manager,
                            )
                            .await?,
                    ));

                    trace!(%id, "timely-{worker_id} acquiring table locks: {lock_clauses}");
                    lock_conn
                        .as_mut()
                        .expect("lock_conn just created")
                        .query_drop(format!("LOCK TABLES {lock_clauses}"))
                        .await?;

                    // Record the frontier of future GTIDs based on the executed GTID set at the start of the snapshot
                    let snapshot_gtid_set = query_sys_var(
                        lock_conn.as_mut().expect("lock_conn just created"),
                        "global.gtid_executed",
                    )
                    .await?;
                    let snapshot_gtid_frontier = match gtid_set_frontier(snapshot_gtid_set.as_str())
                    {
                        Ok(frontier) => frontier,
                        Err(err) => {
                            let err = DefiniteError::UnsupportedGtidState(err.to_string());
                            // If we received a GTID Set with non-consecutive intervals this breaks all our assumptions, so there is nothing else we can do.
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

                    trace!(%id, "timely-{worker_id} acquired table locks at \
                                 start gtid set: {snapshot_gtid_set:?}");

                    // TODO(roshan): Insert metric for how long it took to acquire the locks

                    // Send a signal to all workers that they should start their transactions
                    lock_start_handle
                        .give(&lock_start_cap_set[0], snapshot_gtid_frontier)
                        .await;
                }
                *lock_start_cap_set = CapabilitySet::new();

                // This non-leader worker has no tables to snapshot.
                if !is_snapshot_leader && reader_snapshot_table_info.is_empty() {
                    // Drop the capability to indicate to the leader that we are okay with releasing the table locks
                    *transaction_start_cap_set = CapabilitySet::new();
                    return Ok(());
                }

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

                // Wait for the start_gtids from the leader, which indicate that the table locks have
                // been acquired and we should start a transaction.
                let snapshot_gtid_frontier = loop {
                    match lock_start_input.next().await {
                        Some(AsyncEvent::Data(_, mut data)) => {
                            break data.pop().expect("Sent above")
                        }
                        Some(AsyncEvent::Progress(_)) => (),
                        None => panic!("lock_start_input closed unexpectedly"),
                    }
                };

                trace!(%id, "timely-{worker_id} starting transaction with \
                             consistent snapshot at frontier: {snapshot_gtid_frontier:?}");

                // Start a transaction with REPEATABLE READ and 'CONSISTENT SNAPSHOT' semantics
                // so we can read a consistent snapshot of the table at the specific GTID we read.
                let mut tx_opts = TxOpts::default();
                tx_opts
                    .with_isolation_level(IsolationLevel::RepeatableRead)
                    .with_consistent_snapshot(true)
                    .with_readonly(true);
                conn.start_transaction(tx_opts).await?;
                // Set the session time zone to UTC so that we can read TIMESTAMP columns as UTC
                // From https://dev.mysql.com/doc/refman/8.0/en/datetime.html: "MySQL converts TIMESTAMP values
                // from the current time zone to UTC for storage, and back from UTC to the current time zone
                // for retrieval. (This does not occur for other types such as DATETIME.)"
                conn.query_drop("set @@session.time_zone = '+00:00'")
                    .await?;

                // Drop the capability to indicate to the leader that we have started our transaction
                *transaction_start_cap_set = CapabilitySet::new();

                trace!(%id, "timely-{worker_id} started transaction");

                if is_snapshot_leader {
                    // Wait for all workers to start their transactions so we can release the table locks
                    loop {
                        match transaction_start_input.next().await {
                            Some(AsyncEvent::Data(_, _)) => (),
                            Some(AsyncEvent::Progress(frontier)) => {
                                if frontier.is_empty() {
                                    break;
                                }
                            }
                            None => panic!("transaction_start_input closed unexpectedly"),
                        }
                    }

                    // TODO(roshan): Figure out how to add a test-case that ensures we do release this lock
                    trace!(%id, "timely-{worker_id} releasing table locks");
                    let mut lock_conn = lock_conn
                        .expect("lock_conn should have been created for the snapshot leader");
                    lock_conn.query_drop("UNLOCK TABLES").await?;
                    lock_conn.disconnect().await?;
                    // This worker has nothing else to do
                    if reader_snapshot_table_info.is_empty() {
                        return Ok(());
                    }
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

                // Read the schemas of the tables we are snapshotting
                // TODO: verify the schema matches the expected schema
                let _ = schema_info(
                    &mut conn,
                    &SchemaRequest::Tables(
                        reader_snapshot_table_info
                            .keys()
                            .map(|f| (f.0[0].as_str(), f.0[1].as_str()))
                            .collect(),
                    ),
                )
                .await?;

                // Read the snapshot data from the tables
                let mut final_row = Row::default();
                'outer: for (table, (output_index, table_desc)) in reader_snapshot_table_info {
                    let query = format!("SELECT * FROM {}", table.to_ast_string());
                    trace!(%id, "timely-{worker_id} reading snapshot from \
                                 table '{table}':\n{table_desc:?}");
                    let mut results = conn.exec_stream(query, ()).await?;
                    while let Some(row) = results.try_next().await? {
                        let row: MySqlRow = row;
                        match pack_mysql_row(&mut final_row, row, &table_desc)? {
                            Ok(packed_row) => {
                                raw_handle
                                    .give(
                                        &data_cap_set[0],
                                        (
                                            (output_index, Ok(packed_row)),
                                            GtidPartition::minimum(),
                                            1,
                                        ),
                                    )
                                    .await;
                            }
                            Err(err) => {
                                // A definite error for this table means we should stop ingesting it
                                raw_handle
                                    .give(
                                        &data_cap_set[0],
                                        (
                                            (output_index, Err(err.clone())),
                                            GtidPartition::minimum(),
                                            1,
                                        ),
                                    )
                                    .await;
                                warn!(%id, "timely-{worker_id} stopping snapshot of \
                                            table {table} due to encoding error: {err:?}");
                                continue 'outer;
                            }
                        }
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
