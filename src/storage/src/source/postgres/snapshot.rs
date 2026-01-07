// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Renders the table snapshot side of the [`PostgresSourceConnection`] ingestion dataflow.
//!
//! # Snapshot reading
//!
//! Depending on the resumption LSNs the table reader decides which tables need to be snapshotted.
//! Each table is partitioned across all workers using PostgreSQL's `ctid` (tuple identifier)
//! column, which identifies the physical location of each row. This allows parallel snapshotting
//! of large tables across all available workers.
//!
//! There are a few subtle points about this operation, described in the following sections.
//!
//! ## Consistent LSN point for snapshot transactions
//!
//! Given that all our ingestion is based on correctly timestamping updates with the LSN they
//! happened at it is important that we run the `COPY` query at a specific LSN point that is
//! relatable with the LSN numbers we receive from the replication stream. Such point does not
//! necessarily exist for a normal SQL transaction. To achieve this we must force postgres to
//! produce a consistent point and let us know of the LSN number of that by creating a replication
//! slot as the first statement in a transaction.
//!
//! This is a temporary dummy slot that is only used to put our snapshot transaction on a
//! consistent LSN point. Unfortunately no lighterweight method exists for doing this. See this
//! [postgres thread] for more details.
//!
//! One might wonder why we don't use the actual real slot to provide us with the snapshot point
//! which would automatically be at the correct LSN. The answer is that it's possible that we crash
//! and restart after having already created the slot but before having finished the snapshot. In
//! that case the restarting process will have lost its opportunity to run queries at the slot's
//! consistent point as that opportunity only exists in the ephemeral transaction that created the
//! slot and that is long gone. Additionally there are good reasons of why we'd like to move the
//! slot creation much earlier, e.g during purification, in which case the slot will always be
//! pre-created.
//!
//! [postgres thread]: https://www.postgresql.org/message-id/flat/CAMN0T-vzzNy6TV1Jvh4xzNQdAvCLBQK_kh6_U7kAXgGU3ZFg-Q%40mail.gmail.com
//!
//! ## Reusing the consistent point among all workers
//!
//! Creating replication slots is potentially expensive so the code makes is such that all workers
//! cooperate and reuse one consistent snapshot among them. In order to do so we make use the
//! "export transaction" feature of postgres. This feature allows one SQL session to create an
//! identifier for the transaction (a string identifier) it is currently in, which can be used by
//! other sessions to enter the same "snapshot".
//!
//! We accomplish this by picking one worker at random to function as the transaction leader. The
//! transaction leader is responsible for starting a SQL session, creating a temporary replication
//! slot in a transaction, exporting the transaction id, and broadcasting the transaction
//! information to all other workers via a broadcasted feedback edge.
//!
//! During this phase the follower workers are simply waiting to hear on the feedback edge,
//! effectively synchronizing with the leader. Once all workers have received the snapshot
//! information they can all start to perform their assigned COPY queries.
//!
//! The leader and follower steps described above are accomplished by the [`export_snapshot`] and
//! [`use_snapshot`] functions respectively.
//!
//! ## Coordinated transaction COMMIT
//!
//! When follower workers are done with snapshotting they commit their transaction, close their
//! session, and then drop their snapshot feedback capability. When the leader worker is done with
//! snapshotting it drops its snapshot feedback capability and waits until it observes the
//! snapshot input advancing to the empty frontier. This allows the leader to COMMIT its
//! transaction last, which is the transaction that exported the snapshot.
//!
//! It's unclear if this is strictly necessary, but having the frontiers made it easy enough that I
//! added the synchronization.
//!
//! ## Snapshot rewinding
//!
//! Ingestion dataflows must produce definite data, including the snapshot. What this means
//! practically is that whenever we deem it necessary to snapshot a table we must do so at the same
//! LSN. However, the method for running a transaction described above doesn't let us choose the
//! LSN, it could be an LSN in the future chosen by PostgresSQL while it creates the temporary
//! replication slot.
//!
//! The definition of differential collections states that a collection at some time `t_snapshot`
//! is defined to be the accumulation of all updates that happen at `t <= t_snapshot`, where `<=`
//! is the partial order. In this case we are faced with the problem of knowing the state of a
//! table at `t_snapshot` but actually wanting to know the snapshot at `t_slot <= t_snapshot`.
//!
//! From the definition we can see that the snapshot at `t_slot` is related to the snapshot at
//! `t_snapshot` with the following equations:
//!
//!```text
//! sum(update: t <= t_snapshot) = sum(update: t <= t_slot) + sum(update: t_slot <= t <= t_snapshot)
//!                                         |
//!                                         V
//! sum(update: t <= t_slot) = sum(update: t <= snapshot) - sum(update: t_slot <= t <= t_snapshot)
//! ```
//!
//! Therefore, if we manage to recover the `sum(update: t_slot <= t <= t_snapshot)` term we will be
//! able to "rewind" the snapshot we obtained at `t_snapshot` to `t_slot` by emitting all updates
//! that happen between these two points with their diffs negated.
//!
//! It turns out that this term is exactly what the main replication slot provides us with and we
//! can rewind snapshot at arbitrary points! In order to do this the snapshot dataflow emits rewind
//! requests to the replication reader which informs it that a certain range of updates must be
//! emitted at LSN 0 (by convention) with their diffs negated. These negated diffs are consolidated
//! with the diffs taken at `t_snapshot` that were also emitted at LSN 0 (by convention) and we end
//! up with a TVC that at LSN 0 contains the snapshot at `t_slot`.
//!
//! # Parallel table snapshotting with ctid ranges
//!
//! Each table is partitioned across workers using PostgreSQL's `ctid` column. The `ctid` is a
//! tuple identifier of the form `(block_number, tuple_index)` that represents the physical
//! location of a row on disk. By partitioning the ctid range, each worker can independently
//! fetch a portion of the table.
//!
//! The partitioning works as follows:
//! 1. The snapshot leader queries `pg_class.relpages` to estimate the number of blocks for each
//!    table. This is much faster than querying `max(ctid)` which would require a sequential scan.
//! 2. The leader broadcasts the block count estimates along with the snapshot transaction ID
//!    to all workers, ensuring all workers use consistent estimates for partitioning.
//! 3. Each worker calculates its assigned block range and fetches rows using a `COPY` query
//!    with a `SELECT` that filters by `ctid >= start AND ctid < end`.
//! 4. The last worker uses an open-ended range (`ctid >= start`) to capture any rows beyond
//!    the estimated block count (handles cases where statistics are stale or table has grown).
//!
//! This approach efficiently parallelizes large table snapshots while maintaining the benefits
//! of the `COPY` protocol for bulk data transfer.
//!
//! ## PostgreSQL version requirements
//!
//! Ctid range scans are only efficient on PostgreSQL >= 14 due to TID range scan optimizations
//! introduced in that version. For older PostgreSQL versions, the snapshot falls back to the
//! single-worker-per-table mode where each table is assigned to one worker based on consistent
//! hashing. This is implemented by having the leader broadcast all-zero block counts when
//! PostgreSQL version < 14.
//!
//! # Snapshot decoding
//!
//! Each worker fetches its ctid range directly and decodes the COPY stream locally. The raw
//! COPY data is then distributed to all workers for decoding using round-robin distribution.
//!
//! ```text
//!                 ╭──────────────────╮
//!    ┏━━━━━━━━━━━━v━┓                │ exported
//!    ┃    table     ┃   ╭─────────╮  │ snapshot id
//!    ┃   readers    ┠─>─┤broadcast├──╯
//!    ┃  (parallel)  ┃   ╰─────────╯
//!    ┗━┯━━━━━━━━━━┯━┛
//!   raw│          │
//!  COPY│          │
//!  data│          │
//! ╭────┴─────╮    │
//! │distribute│    │
//! ╰────┬─────╯    │
//! ┏━━━━┷━━━━┓     │
//! ┃  COPY   ┃     │
//! ┃ decoder ┃     │
//! ┗━━━━┯━━━━┛     │
//!      │ snapshot │rewind
//!      │ updates  │requests
//!      v          v
//! ```

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::pin::pin;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use anyhow::bail;
use differential_dataflow::AsCollection;
use futures::{StreamExt as _, TryStreamExt};
use itertools::Itertools;
use mz_ore::cast::CastFrom;
use mz_ore::future::InTask;
use mz_postgres_util::desc::PostgresTableDesc;
use mz_postgres_util::schemas::get_pg_major_version;
use mz_postgres_util::{Client, Config, PostgresError, simple_query_opt};
use mz_repr::{Datum, DatumVec, Diff, Row};
use mz_sql_parser::ast::{Ident, display::AstDisplay};
use mz_storage_types::connections::ConnectionContext;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::parameters::PgSourceSnapshotConfig;
use mz_storage_types::sources::{MzOffset, PostgresSourceConnection};
use mz_timely_util::builder_async::{
    Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::core::Map;
use timely::dataflow::operators::{
    Broadcast, CapabilitySet, Concat, ConnectLoop, Feedback, Operator,
};
use timely::dataflow::{Scope, Stream};
use timely::progress::Timestamp;
use tokio_postgres::error::SqlState;
use tokio_postgres::types::{Oid, PgLsn};
use tracing::trace;

use crate::metrics::source::postgres::PgSnapshotMetrics;
use crate::source::RawSourceCreationConfig;
use crate::source::postgres::replication::RewindRequest;
use crate::source::postgres::{
    DefiniteError, ReplicationError, SourceOutputInfo, TransientError, verify_schema,
};
use crate::source::types::{SignaledFuture, SourceMessage, StackedCollection};
use crate::statistics::SourceStatistics;

/// Information broadcasted from the snapshot leader to all workers.
/// This includes the transaction snapshot ID, LSN, and estimated block counts for each table.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct SnapshotInfo {
    /// The exported transaction snapshot identifier.
    snapshot_id: String,
    /// The LSN at which the snapshot was taken.
    snapshot_lsn: MzOffset,
    /// Estimated number of blocks (pages) for each table, keyed by OID.
    /// This is derived from `pg_class.relpages` and used to partition ctid ranges.
    table_block_counts: BTreeMap<u32, u64>,
}

/// Represents a ctid range that a worker should snapshot.
/// The range is [start_block, end_block) where end_block is optional (None means unbounded).
#[derive(Debug)]
struct CtidRange {
    /// The starting block number (inclusive).
    start_block: u64,
    /// The ending block number (exclusive). None means unbounded (open-ended range).
    end_block: Option<u64>,
}

/// Calculate the ctid range for a given worker based on estimated block count.
///
/// The table is partitioned by block number across all workers. Each worker gets a contiguous
/// range of blocks. The last worker gets an open-ended range to handle any rows beyond the
/// estimated block count.
///
/// When `estimated_blocks` is 0 (either because statistics are unavailable, the table appears
/// empty, or PostgreSQL version < 14 doesn't support ctid range scans), the table is assigned
/// to a single worker determined by `config.responsible_for(oid)` and that worker scans the
/// full table.
///
/// Returns None if this worker has no work to do.
fn worker_ctid_range(
    config: &RawSourceCreationConfig,
    estimated_blocks: u64,
    oid: u32,
) -> Option<CtidRange> {
    // If estimated_blocks is 0, fall back to single-worker mode for this table.
    // This handles:
    // - PostgreSQL < 14 (ctid range scans not supported)
    // - Tables that appear empty in statistics
    // - Tables with stale/missing statistics
    // The responsible worker scans the full table with an open-ended range.
    if estimated_blocks == 0 {
        let fallback = if config.responsible_for(oid) {
            Some(CtidRange {
                start_block: 0,
                end_block: None,
            })
        } else {
            None
        };
        return fallback;
    }

    let worker_id = u64::cast_from(config.worker_id);
    let worker_count = u64::cast_from(config.worker_count);

    // If there are more workers than blocks, only assign work to workers with id < estimated_blocks
    // The last assigned worker still gets an open range.
    let effective_worker_count = std::cmp::min(worker_count, estimated_blocks);

    if worker_id >= effective_worker_count {
        // This worker has no work to do
        return None;
    }

    // Calculate start block for this worker (integer division distributes blocks evenly)
    let start_block = worker_id * estimated_blocks / effective_worker_count;

    // The last effective worker gets an open-ended range
    let is_last_effective_worker = worker_id == effective_worker_count - 1;
    if is_last_effective_worker {
        Some(CtidRange {
            start_block,
            end_block: None,
        })
    } else {
        let end_block = (worker_id + 1) * estimated_blocks / effective_worker_count;
        Some(CtidRange {
            start_block,
            end_block: Some(end_block),
        })
    }
}

/// Estimate the number of blocks for each table from pg_class statistics.
/// This is used to partition ctid ranges across workers.
async fn estimate_table_block_counts(
    client: &Client,
    table_oids: &[u32],
) -> Result<BTreeMap<u32, u64>, TransientError> {
    if table_oids.is_empty() {
        return Ok(BTreeMap::new());
    }

    // Query relpages for all tables at once
    let oid_list = table_oids
        .iter()
        .map(|oid| oid.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let query = format!(
        "SELECT oid, relpages FROM pg_class WHERE oid IN ({})",
        oid_list
    );

    let mut block_counts = BTreeMap::new();
    // Initialize all tables with 0 blocks (in case they're not in pg_class)
    for &oid in table_oids {
        block_counts.insert(oid, 0);
    }

    // Execute the query and collect results
    let rows = client.simple_query(&query).await?;
    for msg in rows {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            let oid: u32 = row.get("oid").unwrap().parse().unwrap();
            let relpages: i64 = row.get("relpages").unwrap().parse().unwrap_or(0);
            // relpages can be -1 if never analyzed, treat as 0
            let relpages = std::cmp::max(0, relpages).try_into().unwrap();
            block_counts.insert(oid, relpages);
        }
    }

    Ok(block_counts)
}

/// Renders the snapshot dataflow. See the module documentation for more information.
pub(crate) fn render<G: Scope<Timestamp = MzOffset>>(
    mut scope: G,
    config: RawSourceCreationConfig,
    connection: PostgresSourceConnection,
    table_info: BTreeMap<u32, BTreeMap<usize, SourceOutputInfo>>,
    metrics: PgSnapshotMetrics,
) -> (
    StackedCollection<G, (usize, Result<SourceMessage, DataflowError>)>,
    Stream<G, RewindRequest>,
    Stream<G, Infallible>,
    Stream<G, ReplicationError>,
    PressOnDropButton,
) {
    let op_name = format!("TableReader({})", config.id);
    let mut builder = AsyncOperatorBuilder::new(op_name, scope.clone());

    let (feedback_handle, feedback_data) = scope.feedback(Default::default());

    let (raw_handle, raw_data) = builder.new_output();
    let (rewinds_handle, rewinds) = builder.new_output::<CapacityContainerBuilder<_>>();
    // This output is used to signal to the replication operator that the replication slot has been
    // created. With the current state of execution serialization there isn't a lot of benefit
    // of splitting the snapshot and replication phases into two operators.
    // TODO(petrosagg): merge the two operators in one (while still maintaining separation as
    // functions/modules)
    let (_, slot_ready) = builder.new_output::<CapacityContainerBuilder<_>>();
    let (snapshot_handle, snapshot) = builder.new_output::<CapacityContainerBuilder<_>>();
    let (definite_error_handle, definite_errors) =
        builder.new_output::<CapacityContainerBuilder<_>>();

    // This operator needs to broadcast data to itself in order to synchronize the transaction
    // snapshot. However, none of the feedback capabilities result in output messages and for the
    // feedback edge specifically having a default conncetion would result in a loop.
    let mut snapshot_input = builder.new_disconnected_input(&feedback_data, Pipeline);

    // The export id must be sent to all workers, so we broadcast the feedback connection
    snapshot.broadcast().connect_loop(feedback_handle);

    let is_snapshot_leader = config.responsible_for("snapshot_leader");

    // A global view of all outputs that will be snapshot by all workers.
    let mut all_outputs = vec![];
    // Table info for tables that need snapshotting. All workers will snapshot all tables,
    // but each worker will handle a different ctid range within each table.
    let mut tables_to_snapshot = BTreeMap::new();
    // A collection of `SourceStatistics` to update for a given Oid. Same info exists in table_info,
    // but this avoids having to iterate + map each time the statistics are needed.
    let mut export_statistics = BTreeMap::new();
    for (table, outputs) in table_info.iter() {
        for (&output_index, output) in outputs {
            if *output.resume_upper != [MzOffset::minimum()] {
                // Already has been snapshotted.
                continue;
            }
            all_outputs.push(output_index);
            tables_to_snapshot
                .entry(*table)
                .or_insert_with(BTreeMap::new)
                .insert(output_index, output.clone());
            let statistics = config
                .statistics
                .get(&output.export_id)
                .expect("statistics are initialized")
                .clone();
            export_statistics.insert((*table, output_index), statistics);
        }
    }

    let (button, transient_errors) = builder.build_fallible(move |caps| {
        let busy_signal = Arc::clone(&config.busy_signal);
        Box::pin(SignaledFuture::new(busy_signal, async move {
            let id = config.id;
            let worker_id = config.worker_id;
            let [
                data_cap_set,
                rewind_cap_set,
                slot_ready_cap_set,
                snapshot_cap_set,
                definite_error_cap_set,
            ]: &mut [_; 5] = caps.try_into().unwrap();

            trace!(
                %id,
                "timely-{worker_id} initializing table reader \
                    with {} tables to snapshot",
                    tables_to_snapshot.len()
            );

            let connection_config = connection
                .connection
                .config(
                    &config.config.connection_context.secrets_reader,
                    &config.config,
                    InTask::Yes,
                )
                .await?;


            // The snapshot operator is responsible for creating the replication slot(s).
            // This first slot is the permanent slot that will be used for reading the replication
            // stream.  A temporary slot is created further on to capture table snapshots.
            let replication_client = if is_snapshot_leader {
                let client = connection_config
                    .connect_replication(&config.config.connection_context.ssh_tunnel_manager)
                    .await?;
                let main_slot = &connection.publication_details.slot;

                tracing::info!(%id, "ensuring replication slot {main_slot} exists");
                super::ensure_replication_slot(&client, main_slot).await?;
                Some(client)
            } else {
                None
            };
            *slot_ready_cap_set = CapabilitySet::new();

            // Nothing needs to be snapshot.
            if all_outputs.is_empty() {
                trace!(%id, "no exports to snapshot");
                // Note we do not emit a `ProgressStatisticsUpdate::Snapshot` update here,
                // as we do not want to attempt to override the current value with 0. We
                // just leave it null.
                return Ok(());
            }

            // A worker *must* emit a count even if not responsible for snapshotting a table
            // as statistic summarization will return null if any worker hasn't set a value.
            // This will also reset snapshot stats for any exports not snapshotting.
            // If no workers need to snapshot, then avoid emitting these as they will clear
            // previous stats.
            for statistics in config.statistics.values() {
                statistics.set_snapshot_records_known(0);
                statistics.set_snapshot_records_staged(0);
            }

            // Collect table OIDs for block count estimation
            let table_oids: Vec<u32> = tables_to_snapshot.keys().copied().collect();

            // replication client is only set if this worker is the snapshot leader
            let client = match replication_client {
                Some(client) => {
                    let tmp_slot = format!("mzsnapshot_{}", uuid::Uuid::new_v4()).replace('-', "");
                    let (snapshot_id, snapshot_lsn) = export_snapshot(&client, &tmp_slot, true).await?;

                    // Check PostgreSQL version. Ctid range scans are only efficient on PG >= 14
                    // due to improvements in TID range scan support.
                    let pg_version = get_pg_major_version(&client).await?;

                    // Estimate block counts for all tables from pg_class statistics.
                    // This must be done by the leader and broadcasted to ensure all workers
                    // use the same estimates for ctid range partitioning.
                    //
                    // For PostgreSQL < 14, we set all block counts to 0 to fall back to
                    // single-worker-per-table mode, as ctid range scans are not well supported.
                    let table_block_counts = if pg_version >= 14 {
                        estimate_table_block_counts(&client, &table_oids).await?
                    } else {
                        trace!(
                            %id,
                            "timely-{worker_id} PostgreSQL version {pg_version} < 14, \
                             falling back to single-worker-per-table snapshot mode"
                        );
                        // Return all zeros to trigger fallback mode
                        table_oids.iter().map(|&oid| (oid, 0u64)).collect()
                    };

                    let snapshot_info = SnapshotInfo {
                        snapshot_id,
                        snapshot_lsn,
                        table_block_counts,
                    };
                    trace!(
                        %id,
                        "timely-{worker_id} exporting snapshot info {snapshot_info:?}");
                    snapshot_handle.give(&snapshot_cap_set[0], snapshot_info);

                    client
                }
                None => {
                    // Only the snapshot leader needs a replication connection.
                    let task_name = format!("timely-{worker_id} PG snapshotter");
                    connection_config
                        .connect(
                            &task_name,
                            &config.config.connection_context.ssh_tunnel_manager,
                        )
                        .await?
                }
            };

            // Configure statement_timeout based on param. We want to be able to
            // override the server value here in case it's set too low,
            // respective to the size of the data we need to copy.
            set_statement_timeout(
                &client,
                config
                    .config
                    .parameters
                    .pg_source_snapshot_statement_timeout,
            )
            .await?;

            let snapshot_info = loop {
                match snapshot_input.next().await {
                    Some(AsyncEvent::Data(_, mut data)) => {
                        break data.pop().expect("snapshot sent above")
                    }
                    Some(AsyncEvent::Progress(_)) => continue,
                    None => panic!(
                        "feedback closed \
                    before sending snapshot info"
                    ),
                }
            };
            let SnapshotInfo {
                snapshot_id,
                snapshot_lsn,
                table_block_counts,
            } = snapshot_info;

            // Snapshot leader is already in identified transaction but all other workers need to enter it.
            if !is_snapshot_leader {
                trace!(%id, "timely-{worker_id} using snapshot id {snapshot_id:?}");
                use_snapshot(&client, &snapshot_id).await?;
            }


            let upstream_info = {
                let table_oids = tables_to_snapshot.keys().copied().collect::<Vec<_>>();
                // As part of retrieving the schema info, RLS policies are checked to ensure the
                // snapshot can successfully read the tables. RLS policy errors are treated as
                // transient, as the customer can simply add the BYPASSRLS to the PG account
                // used by MZ.
                match retrieve_schema_info(
                    &connection_config,
                    &config.config.connection_context,
                    &connection.publication,
                    &table_oids)
                    .await
                {
                    // If the replication stream cannot be obtained in a definite way there is
                    // nothing else to do. These errors are not retractable.
                    Err(PostgresError::PublicationMissing(publication)) => {
                        let err = DefiniteError::PublicationDropped(publication);
                        for (oid, outputs) in tables_to_snapshot.iter() {
                            // Produce a definite error here and then exit to ensure
                            // a missing publication doesn't generate a transient
                            // error and restart this dataflow indefinitely.
                            //
                            // We pick `u64::MAX` as the LSN which will (in
                            // practice) never conflict any previously revealed
                            // portions of the TVC.
                            for output_index in outputs.keys() {
                                let update = (
                                    (*oid, *output_index, Err(err.clone().into())),
                                    MzOffset::from(u64::MAX),
                                    Diff::ONE,
                                );
                                raw_handle.give_fueled(&data_cap_set[0], update).await;
                            }
                        }

                        definite_error_handle.give(
                            &definite_error_cap_set[0],
                            ReplicationError::Definite(Rc::new(err)),
                        );
                        return Ok(());
                    },
                    Err(e) => Err(TransientError::from(e))?,
                    Ok(i) => i,
                }
            };

            // Only the snapshot leader reports table sizes to avoid duplicate statistics.
            if is_snapshot_leader {
                report_snapshot_size(&client, &tables_to_snapshot, metrics, &config, &export_statistics).await?;
            }

            for (&oid, outputs) in tables_to_snapshot.iter() {
                for (&output_index, info) in outputs.iter() {
                    if let Err(err) = verify_schema(oid, info, &upstream_info) {
                        raw_handle
                            .give_fueled(
                                &data_cap_set[0],
                                (
                                    (oid, output_index, Err(err.into())),
                                    MzOffset::minimum(),
                                    Diff::ONE,
                                ),
                            )
                            .await;
                        continue;
                    }

                    // Get estimated block count from the broadcasted table statistics
                    let block_count = table_block_counts.get(&oid).copied().unwrap_or(0);

                    // Calculate this worker's ctid range based on estimated blocks.
                    // When estimated_blocks is 0 (PG < 14 or empty table), fall back to
                    // single-worker mode using responsible_for to pick the worker.
                    let Some(ctid_range) = worker_ctid_range(&config, block_count, oid) else {
                        // This worker has no work for this table (more workers than blocks)
                        trace!(
                            %id,
                            "timely-{worker_id} no ctid range assigned for table {:?}({oid})",
                            info.desc.name
                        );
                        continue;
                    };

                    trace!(
                        %id,
                        "timely-{worker_id} snapshotting table {:?}({oid}) output {output_index} \
                         @ {snapshot_lsn} with ctid range {:?}",
                        info.desc.name,
                        ctid_range
                    );

                    // To handle quoted/keyword names, we can use `Ident`'s AST printing, which
                    // emulate's PG's rules for name formatting.
                    let namespace = Ident::new_unchecked(&info.desc.namespace).to_ast_string_stable();
                    let table = Ident::new_unchecked(&info.desc.name).to_ast_string_stable();
                    let column_list = info
                        .desc
                        .columns
                        .iter()
                        .map(|c| Ident::new_unchecked(&c.name).to_ast_string_stable())
                        .join(",");


                    let ctid_filter = match ctid_range.end_block {
                        Some(end) => format!(
                            "WHERE ctid >= '({},0)'::tid AND ctid < '({},0)'::tid",
                            ctid_range.start_block, end
                        ),
                        None => format!("WHERE ctid >= '({},0)'::tid", ctid_range.start_block),
                    };
                    let query = format!(
                        "COPY (SELECT {column_list} FROM {namespace}.{table} {ctid_filter}) \
                         TO STDOUT (FORMAT TEXT, DELIMITER '\t')"
                    );
                    let mut stream = pin!(client.copy_out_simple(&query).await?);

                    let mut snapshot_staged = 0;
                    let mut update = ((oid, output_index, Ok(vec![])), MzOffset::minimum(), Diff::ONE);
                    while let Some(bytes) = stream.try_next().await? {
                        let data = update.0 .2.as_mut().unwrap();
                        data.clear();
                        data.extend_from_slice(&bytes);
                        raw_handle.give_fueled(&data_cap_set[0], &update).await;
                        snapshot_staged += 1;
                        if snapshot_staged % 1000 == 0 {
                            export_statistics[&(oid, output_index)].set_snapshot_records_staged(snapshot_staged);
                        }
                    }
                    // final update for snapshot_staged, using the staged values as the total is an estimate
                    export_statistics[&(oid, output_index)].set_snapshot_records_staged(snapshot_staged);
                    export_statistics[&(oid, output_index)].set_snapshot_records_known(snapshot_staged);
                }
            }

            // We are done with the snapshot so now we will emit rewind requests. It is important
            // that this happens after the snapshot has finished because this is what unblocks the
            // replication operator and we want this to happen serially. It might seem like a good
            // idea to read the replication stream concurrently with the snapshot but it actually
            // leads to a lot of data being staged for the future, which needlessly consumed memory
            // in the cluster.
            //
            // Since all workers now snapshot all tables (each with different ctid ranges), we only
            // emit rewind requests from the worker responsible for each output to avoid duplicates.
            for (&oid, output) in tables_to_snapshot.iter() {
                for (output_index, info) in output {
                    // Only emit rewind request from one worker per output
                    if !config.responsible_for((oid, *output_index)) {
                        continue;
                    }
                    trace!(%id, "timely-{worker_id} producing rewind request for table {} output {output_index}", info.desc.name);
                    let req = RewindRequest { output_index: *output_index, snapshot_lsn };
                    rewinds_handle.give(&rewind_cap_set[0], req);
                }
            }
            *rewind_cap_set = CapabilitySet::new();

            // Failure scenario after we have produced the snapshot, but before a successful COMMIT
            fail::fail_point!("pg_snapshot_failure", |_| Err(
                TransientError::SyntheticError
            ));

            // The exporting worker should wait for all the other workers to commit before dropping
            // its client since this is what holds the exported transaction alive.
            if is_snapshot_leader {
                trace!(%id, "timely-{worker_id} waiting for all workers to finish");
                *snapshot_cap_set = CapabilitySet::new();
                while snapshot_input.next().await.is_some() {}
                trace!(%id, "timely-{worker_id} (leader) comitting COPY transaction");
                client.simple_query("COMMIT").await?;
            } else {
                trace!(%id, "timely-{worker_id} comitting COPY transaction");
                client.simple_query("COMMIT").await?;
                *snapshot_cap_set = CapabilitySet::new();
            }
            drop(client);
            Ok(())
        }))
    });

    // We now decode the COPY protocol and apply the cast expressions
    let mut text_row = Row::default();
    let mut final_row = Row::default();
    let mut datum_vec = DatumVec::new();
    let mut next_worker = (0..u64::cast_from(scope.peers()))
        // Round robin on 1000-records basis to avoid creating tiny containers when there are a
        // small number of updates and a large number of workers.
        .flat_map(|w| std::iter::repeat_n(w, 1000))
        .cycle();
    let round_robin = Exchange::new(move |_| next_worker.next().unwrap());
    let snapshot_updates = raw_data
        .map::<Vec<_>, _, _>(Clone::clone)
        .unary(round_robin, "PgCastSnapshotRows", |_, _| {
            move |input, output| {
                input.for_each_time(|time, data| {
                    let mut session = output.session(&time);
                    for ((oid, output_index, event), time, diff) in
                        data.flat_map(|data| data.drain(..))
                    {
                        let output = &table_info
                            .get(&oid)
                            .and_then(|outputs| outputs.get(&output_index))
                            .expect("table_info contains all outputs");

                        let event = event
                            .as_ref()
                            .map_err(|e: &DataflowError| e.clone())
                            .and_then(|bytes| {
                                decode_copy_row(bytes, output.casts.len(), &mut text_row)?;
                                let datums = datum_vec.borrow_with(&text_row);
                                super::cast_row(&output.casts, &datums, &mut final_row)?;
                                Ok(SourceMessage {
                                    key: Row::default(),
                                    value: final_row.clone(),
                                    metadata: Row::default(),
                                })
                            });

                        session.give(((output_index, event), time, diff));
                    }
                });
            }
        })
        .as_collection();

    let errors = definite_errors.concat(&transient_errors.map(ReplicationError::from));

    (
        snapshot_updates,
        rewinds,
        slot_ready,
        errors,
        button.press_on_drop(),
    )
}

/// Starts a read-only transaction on the SQL session of `client` at a consistent LSN point by
/// creating a replication slot. Returns a snapshot identifier that can be imported in
/// other SQL session and the LSN of the consistent point.
async fn export_snapshot(
    client: &Client,
    slot: &str,
    temporary: bool,
) -> Result<(String, MzOffset), TransientError> {
    match export_snapshot_inner(client, slot, temporary).await {
        Ok(ok) => Ok(ok),
        Err(err) => {
            // We don't want to leave the client inside a failed tx
            client.simple_query("ROLLBACK;").await?;
            Err(err)
        }
    }
}

async fn export_snapshot_inner(
    client: &Client,
    slot: &str,
    temporary: bool,
) -> Result<(String, MzOffset), TransientError> {
    client
        .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
        .await?;

    // Note: Using unchecked here is okay because we're using it in a SQL query.
    let slot = Ident::new_unchecked(slot).to_ast_string_simple();
    let temporary_str = if temporary { " TEMPORARY" } else { "" };
    let query =
        format!("CREATE_REPLICATION_SLOT {slot}{temporary_str} LOGICAL \"pgoutput\" USE_SNAPSHOT");
    let row = match simple_query_opt(client, &query).await {
        Ok(row) => Ok(row.unwrap()),
        Err(PostgresError::Postgres(err)) if err.code() == Some(&SqlState::DUPLICATE_OBJECT) => {
            return Err(TransientError::ReplicationSlotAlreadyExists);
        }
        Err(err) => Err(err),
    }?;

    // When creating a replication slot postgres returns the LSN of its consistent point, which is
    // the LSN that must be passed to `START_REPLICATION` to cleanly transition from the snapshot
    // phase to the replication phase. `START_REPLICATION` includes all transactions that commit at
    // LSNs *greater than or equal* to the passed LSN. Therefore the snapshot phase must happen at
    // the greatest LSN that is not beyond the consistent point. That LSN is `consistent_point - 1`
    let consistent_point: PgLsn = row.get("consistent_point").unwrap().parse().unwrap();
    let consistent_point = u64::from(consistent_point)
        .checked_sub(1)
        .expect("consistent point is always non-zero");

    let row = simple_query_opt(client, "SELECT pg_export_snapshot();")
        .await?
        .unwrap();
    let snapshot = row.get("pg_export_snapshot").unwrap().to_owned();

    Ok((snapshot, MzOffset::from(consistent_point)))
}

/// Starts a read-only transaction on the SQL session of `client` at a the consistent LSN point of
/// `snapshot`.
async fn use_snapshot(client: &Client, snapshot: &str) -> Result<(), TransientError> {
    client
        .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
        .await?;
    let query = format!("SET TRANSACTION SNAPSHOT '{snapshot}';");
    client.simple_query(&query).await?;
    Ok(())
}

async fn set_statement_timeout(client: &Client, timeout: Duration) -> Result<(), TransientError> {
    // Value is known to accept milliseconds w/o units.
    // https://www.postgresql.org/docs/current/runtime-config-client.html
    client
        .simple_query(&format!("SET statement_timeout = {}", timeout.as_millis()))
        .await?;
    Ok(())
}

/// Decodes a row of `col_len` columns obtained from a text encoded COPY query into `row`.
fn decode_copy_row(data: &[u8], col_len: usize, row: &mut Row) -> Result<(), DefiniteError> {
    let mut packer = row.packer();
    let row_parser = mz_pgcopy::CopyTextFormatParser::new(data, b'\t', "\\N");
    let mut column_iter = row_parser.iter_raw_truncating(col_len);
    for _ in 0..col_len {
        let value = match column_iter.next() {
            Some(Ok(value)) => value,
            Some(Err(_)) => return Err(DefiniteError::InvalidCopyInput),
            None => return Err(DefiniteError::MissingColumn),
        };
        let datum = value.map(super::decode_utf8_text).transpose()?;
        packer.push(datum.unwrap_or(Datum::Null));
    }
    Ok(())
}

/// Record the sizes of the tables being snapshotted in `PgSnapshotMetrics` and emit snapshot statistics for each export.
async fn report_snapshot_size(
    client: &Client,
    tables_to_snapshot: &BTreeMap<u32, BTreeMap<usize, SourceOutputInfo>>,
    metrics: PgSnapshotMetrics,
    config: &RawSourceCreationConfig,
    export_statistics: &BTreeMap<(u32, usize), SourceStatistics>,
) -> Result<(), anyhow::Error> {
    // TODO(guswynn): delete unused configs
    let snapshot_config = config.config.parameters.pg_snapshot_config;

    for (&oid, outputs) in tables_to_snapshot {
        // Use the first output's desc to make the table name since it is the same for all outputs
        let Some((_, info)) = outputs.first_key_value() else {
            continue;
        };
        let table = format!(
            "{}.{}",
            Ident::new_unchecked(info.desc.namespace.clone()).to_ast_string_simple(),
            Ident::new_unchecked(info.desc.name.clone()).to_ast_string_simple()
        );
        let stats =
            collect_table_statistics(client, snapshot_config, &table, info.desc.oid).await?;
        metrics.record_table_count_latency(table, stats.count_latency);
        for &output_index in outputs.keys() {
            export_statistics[&(oid, output_index)].set_snapshot_records_known(stats.count);
            export_statistics[&(oid, output_index)].set_snapshot_records_staged(0);
        }
    }
    Ok(())
}

#[derive(Default)]
struct TableStatistics {
    count: u64,
    count_latency: f64,
}

async fn collect_table_statistics(
    client: &Client,
    config: PgSourceSnapshotConfig,
    table: &str,
    oid: u32,
) -> Result<TableStatistics, anyhow::Error> {
    use mz_ore::metrics::MetricsFutureExt;
    let mut stats = TableStatistics::default();

    let estimate_row = simple_query_opt(
        client,
        &format!("SELECT reltuples::bigint AS estimate_count FROM pg_class WHERE oid = '{oid}'"),
    )
    .wall_time()
    .set_at(&mut stats.count_latency)
    .await?;
    stats.count = match estimate_row {
        Some(row) => row.get("estimate_count").unwrap().parse().unwrap_or(0),
        None => bail!("failed to get estimate count for {table}"),
    };

    // If the estimate is low enough we can attempt to get an exact count. Note that not yet
    // vacuumed tables will report zero rows here and there is a possibility that they are very
    // large. We accept this risk and we offer the feature flag as an escape hatch if it becomes
    // problematic.
    if config.collect_strict_count && stats.count < 1_000_000 {
        let count_row = simple_query_opt(client, &format!("SELECT count(*) as count from {table}"))
            .wall_time()
            .set_at(&mut stats.count_latency)
            .await?;
        stats.count = match count_row {
            Some(row) => row.get("count").unwrap().parse().unwrap(),
            None => bail!("failed to get count for {table}"),
        }
    }

    Ok(stats)
}

/// Validates that there are no blocking RLS polcicies on the tables and retrieves table schemas
/// for the given publication.
async fn retrieve_schema_info(
    connection_config: &Config,
    connection_context: &ConnectionContext,
    publication: &str,
    table_oids: &[Oid],
) -> Result<BTreeMap<u32, PostgresTableDesc>, PostgresError> {
    let schema_client = connection_config
        .connect(
            "snapshot schema info",
            &connection_context.ssh_tunnel_manager,
        )
        .await?;
    mz_postgres_util::validate_no_rls_policies(&schema_client, table_oids).await?;
    mz_postgres_util::publication_info(&schema_client, publication, Some(table_oids)).await
}
