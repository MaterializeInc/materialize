// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Renders the replication side of the [`MySqlSourceConnection`] ingestion dataflow.
//!
//! # Progress tracking using Partitioned Timestamps
//!
//! This dataflow uses a Partitioned Timestamp implementation to represent the GTID Set that
//! comprises the full set of committed transactions from the MySQL Server. The frontier
//! representing progress for this dataflow represents the full range of possible UUIDs +
//! Transaction IDs of future GTIDs that could be added to the GTID Set.
//!
//! See the [`mz_storage_types::sources::mysql::GtidPartition`] type for more information.
//!
//! To maintain a complete frontier of the full UUID GTID range, we use a
//! [`partitions::GtidReplicationPartitions`] struct to store the GTID Set as a set of partitions.
//! This allows us to easily advance the frontier each time we see a new GTID on the replication
//! stream.
//!
//! # Resumption
//!
//! When the dataflow is resumed, the MySQL replication stream is started from the GTID frontier
//! of the minimum frontier across all source outputs. This is compared against the GTID set that
//! may still be obtained from the MySQL server, using the @@GTID_PURGED value in MySQL to
//! determine GTIDs that are no longer available in the binlog and to put the source in an error
//! state if we cannot resume from the GTID frontier.
//!
//! # Rewinds
//!
//! The replication stream may be resumed from a point before the snapshot for a specific output
//! occurs. To avoid double-counting updates that were present in the snapshot, we store a map
//! of pending rewinds that we've received from the snapshot operator, and when we see updates
//! for an output that were present in the snapshot, we negate the snapshot update
//! (at the minimum timestamp) and send it again at the correct GTID.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::num::NonZeroU64;
use std::pin::pin;
use std::sync::Arc;

use differential_dataflow::AsCollection;
use futures::StreamExt;
use itertools::Itertools;
use mysql_async::prelude::Queryable;
use mysql_async::{BinlogStream, BinlogStreamRequest, GnoInterval, Sid};
use mz_ore::future::InTask;
use mz_ssh_util::tunnel_manager::ManagedSshTunnelHandle;
use mz_timely_util::containers::stack::AccountedStackBuilder;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::core::Map;
use timely::dataflow::operators::Concat;
use timely::dataflow::{Scope, Stream};
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use tracing::trace;
use uuid::Uuid;

use mz_mysql_util::{
    query_sys_var, MySqlConn, MySqlError, ER_SOURCE_FATAL_ERROR_READING_BINLOG_CODE,
};
use mz_ore::cast::CastFrom;
use mz_repr::GlobalId;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sources::mysql::{gtid_set_frontier, GtidPartition, GtidState};
use mz_storage_types::sources::MySqlSourceConnection;
use mz_timely_util::builder_async::{
    Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};

use crate::metrics::source::mysql::MySqlSourceMetrics;
use crate::source::types::{SignaledFuture, SourceMessage, StackedCollection};
use crate::source::RawSourceCreationConfig;

use super::{
    return_definite_error, validate_mysql_repl_settings, DefiniteError, ReplicationError,
    RewindRequest, SourceOutputInfo, TransientError,
};

mod context;
mod events;
mod partitions;

/// Used as a partition id to determine if the worker is
/// responsible for reading from the MySQL replication stream
static REPL_READER: &str = "reader";

/// A constant arbitrary offset to add to the source-id to
/// produce a deterministic server-id for identifying Materialize
/// as a replica on the upstream MySQL server.
/// TODO(roshan): Add user-facing documentation for this
static REPLICATION_SERVER_ID_OFFSET: u32 = 524000;

/// Renders the replication dataflow. See the module documentation for more
/// information.
pub(crate) fn render<G: Scope<Timestamp = GtidPartition>>(
    scope: G,
    config: RawSourceCreationConfig,
    connection: MySqlSourceConnection,
    source_outputs: Vec<SourceOutputInfo>,
    rewind_stream: &Stream<G, RewindRequest>,
    metrics: MySqlSourceMetrics,
) -> (
    StackedCollection<G, (usize, Result<SourceMessage, DataflowError>)>,
    Stream<G, Infallible>,
    Stream<G, ReplicationError>,
    PressOnDropButton,
) {
    let op_name = format!("MySqlReplicationReader({})", config.id);
    let mut builder = AsyncOperatorBuilder::new(op_name, scope);

    let repl_reader_id = u64::cast_from(config.responsible_worker(REPL_READER));
    let (mut data_output, data_stream) = builder.new_output::<AccountedStackBuilder<_>>();
    let (_upper_output, upper_stream) = builder.new_output::<CapacityContainerBuilder<_>>();
    // Captures DefiniteErrors that affect the entire source, including all outputs
    let (definite_error_handle, definite_errors) =
        builder.new_output::<CapacityContainerBuilder<_>>();
    let mut rewind_input = builder.new_input_for(
        rewind_stream,
        Exchange::new(move |_| repl_reader_id),
        &data_output,
    );

    let output_indexes = source_outputs
        .iter()
        .map(|output| output.output_index)
        .collect_vec();

    metrics.tables.set(u64::cast_from(source_outputs.len()));

    let (button, transient_errors) = builder.build_fallible(move |caps| {
        let busy_signal = Arc::clone(&config.busy_signal);
        Box::pin(SignaledFuture::new(busy_signal, async move {
            let (id, worker_id) = (config.id, config.worker_id);
            let [data_cap_set, upper_cap_set, definite_error_cap_set]: &mut [_; 3] =
                caps.try_into().unwrap();

            // Only run the replication reader on the worker responsible for it.
            if !config.responsible_for(REPL_READER) {
                return Ok(());
            }

            let connection_config = connection
                .connection
                .config(
                    &config.config.connection_context.secrets_reader,
                    &config.config,
                    InTask::Yes,
                )
                .await?;

            let mut conn = connection_config
                .connect(
                    &format!("timely-{worker_id} MySQL replication reader"),
                    &config.config.connection_context.ssh_tunnel_manager,
                )
                .await?;

            // Get the set of GTIDs that have been purged from the binlogs. The assumption is that this
            // represents the frontier of possible GTIDs that exist in the binlog, that we can start
            // replicating from.
            let binlog_purged_set = query_sys_var(&mut conn, "global.gtid_purged").await?;
            let binlog_frontier = match gtid_set_frontier(&binlog_purged_set) {
                Ok(frontier) => frontier,
                Err(err) => {
                    let err = DefiniteError::UnsupportedGtidState(err.to_string());
                    return Ok(
                        // If GTID intervals in the binlog are not available in a monotonic consecutive
                        // order this breaks all of our assumptions and there is nothing else we can do.
                        // This can occur if the mysql server is restored to a previous point-in-time
                        // or if a user manually adds transactions to the @@gtid_purged system var.
                        return_definite_error(
                            err,
                            &output_indexes,
                            &data_output,
                            data_cap_set,
                            &definite_error_handle,
                            definite_error_cap_set,
                        )
                        .await,
                    );
                }
            };

            trace!(%id, "timely-{worker_id} replication binlog frontier: {binlog_frontier:?}");

            // upstream-table-name: Vec<SourceOutputInfo> since multiple
            // outputs can refer to the same table
            let mut table_info = BTreeMap::new();
            let mut output_uppers = Vec::new();

            // Calculate the lowest frontier across all outputs, which represents the point which
            // we should start replication from.
            let min_frontier = Antichain::from_elem(GtidPartition::minimum());
            for output in source_outputs.into_iter() {
                // If an output is resuming at the minimum frontier then its snapshot
                // has not yet been committed.
                // We need to resume from a frontier before the output's snapshot frontier
                // to ensure we don't miss updates that happen after the snapshot was taken.
                //
                // This also ensures that tables created as part of the same CREATE SOURCE
                // statement are 'effectively' snapshot at the same GTID Set, even if their
                // actual snapshot frontiers are different due to a restart.
                //
                // We've chosen the frontier beyond the GTID Set recorded
                // during purification as this resume point.
                if &output.resume_upper == &min_frontier {
                    output_uppers.push(output.initial_gtid_set.clone());
                } else {
                    output_uppers.push(output.resume_upper.clone());
                }

                table_info
                    .entry(output.table_name.clone())
                    .or_insert_with(Vec::new)
                    .push(output);
            }
            let resume_upper = match output_uppers.len() {
                0 => {
                    // If there are no outputs to replicate then we will just be updating the
                    // source progress collection. In this case we can just start from the head of
                    // the binlog to avoid wasting time on old events.
                    trace!(%id, "timely-{worker_id} replication reader found no outputs \
                                 to replicate, using latest gtid_executed as resume_upper");
                    let executed_gtid_set =
                        query_sys_var(&mut conn, "global.gtid_executed").await?;

                    gtid_set_frontier(&executed_gtid_set)?
                }
                _ => Antichain::from_iter(output_uppers.into_iter().flatten()),
            };

            // Validate that we can actually resume from this upper.
            if !PartialOrder::less_equal(&binlog_frontier, &resume_upper) {
                let err = DefiniteError::BinlogMissingResumePoint(
                    format!("{:?}", binlog_frontier),
                    format!("{:?}", resume_upper),
                );
                return Ok(return_definite_error(
                    err,
                    &output_indexes,
                    &data_output,
                    data_cap_set,
                    &definite_error_handle,
                    definite_error_cap_set,
                )
                .await);
            };

            data_cap_set.downgrade(&*resume_upper);
            upper_cap_set.downgrade(&*resume_upper);
            trace!(%id, "timely-{worker_id} replication reader started at {:?}", resume_upper);

            let mut rewinds = BTreeMap::new();
            while let Some(event) = rewind_input.next().await {
                if let AsyncEvent::Data(caps, data) = event {
                    for req in data {
                        // Check that the replication stream will be resumed from the snapshot point or before.
                        if !PartialOrder::less_equal(&resume_upper, &req.snapshot_upper) {
                            let err = DefiniteError::BinlogMissingResumePoint(
                                format!("{:?}", resume_upper),
                                format!("{:?}", req.snapshot_upper),
                            );
                            return Ok(return_definite_error(
                                err,
                                &output_indexes,
                                &data_output,
                                data_cap_set,
                                &definite_error_handle,
                                definite_error_cap_set,
                            )
                            .await);
                        };
                        // If the snapshot point is the same as the resume point then we don't need to rewind
                        if resume_upper != req.snapshot_upper {
                            rewinds.insert(req.output_index.clone(), (caps.clone(), req));
                        }
                    }
                }
            }
            trace!(%id, "timely-{worker_id} pending rewinds {rewinds:?}");

            // We don't use _conn_tunnel_handle here, but need to keep it around to ensure that the
            // SSH tunnel is not dropped until the replication stream is dropped.
            let (binlog_stream, _conn_tunnel_handle) =
                match raw_stream(&config, conn, &resume_upper).await? {
                    Ok(stream) => stream,
                    // If the replication stream cannot be obtained in a definite way there is
                    // nothing else to do. These errors are not retractable.
                    Err(err) => {
                        return Ok(return_definite_error(
                            err,
                            &output_indexes,
                            &data_output,
                            data_cap_set,
                            &definite_error_handle,
                            definite_error_cap_set,
                        )
                        .await)
                    }
                };
            let mut stream = pin!(binlog_stream.peekable());

            // Store all partitions from the resume_upper so we can create a frontier that comprises
            // timestamps for partitions representing the full range of UUIDs to advance our main
            // capabilities.
            let mut data_partitions =
                partitions::GtidReplicationPartitions::from(resume_upper.clone());
            let mut progress_partitions = partitions::GtidReplicationPartitions::from(resume_upper);

            let mut repl_context = context::ReplContext::new(
                &config,
                &connection_config,
                stream.as_mut(),
                &table_info,
                &metrics,
                &mut data_output,
                data_cap_set,
                upper_cap_set,
                rewinds,
            );

            let mut active_tx: Option<(Uuid, NonZeroU64)> = None;

            let mut row_event_buffer = Vec::new();

            while let Some(event) = repl_context.stream.next().await {
                use mysql_async::binlog::events::*;
                let event = event?;
                let event_data = event.read_data()?;
                metrics.total.inc();

                match event_data {
                    Some(EventData::XidEvent(_)) => {
                        // We've received a transaction commit event, which means that we've seen
                        // all events for the current GTID and we can advance the frontier beyond.
                        let (source_id, tx_id) = active_tx.take().expect("unexpected xid event");

                        // Increment the transaction-id to the next GTID we should see from this source-id
                        let next_tx_id = tx_id.checked_add(1).unwrap();
                        let next_gtid =
                            GtidPartition::new_singleton(source_id, GtidState::Active(next_tx_id));

                        if let Err(err) = data_partitions.advance_frontier(next_gtid) {
                            return Ok(return_definite_error(
                                err,
                                &output_indexes,
                                &data_output,
                                data_cap_set,
                                &definite_error_handle,
                                definite_error_cap_set,
                            )
                            .await);
                        }
                        let new_upper = data_partitions.frontier();
                        repl_context.downgrade_data_cap_set("xid_event", new_upper);
                    }
                    // We receive a GtidEvent that tells us the GTID of the incoming RowsEvents (and other events)
                    Some(EventData::GtidEvent(event)) => {
                        let source_id = Uuid::from_bytes(event.sid());
                        let tx_id = NonZeroU64::new(event.gno()).unwrap();

                        // We are potentially about to ingest a big transaction that we don't want
                        // to store in memory. For this reason we are immediately downgrading our
                        // progress frontier to one that includes the upcoming transaction. This
                        // will cause a remap binding to be minted right away and so the data of
                        // the transaction will not accumulate in the reclock operator.
                        let next_tx_id = tx_id.checked_add(1).unwrap();
                        let next_gtid =
                            GtidPartition::new_singleton(source_id, GtidState::Active(next_tx_id));

                        if let Err(err) = progress_partitions.advance_frontier(next_gtid) {
                            return Ok(return_definite_error(
                                err,
                                &output_indexes,
                                &data_output,
                                data_cap_set,
                                &definite_error_handle,
                                definite_error_cap_set,
                            )
                            .await);
                        }
                        let new_upper = progress_partitions.frontier();
                        repl_context.downgrade_progress_cap_set("xid_event", new_upper);

                        // Store the information of the active transaction for the subsequent events
                        active_tx = Some((source_id, tx_id));
                    }
                    Some(EventData::RowsEvent(data)) => {
                        let (source_id, tx_id) = active_tx
                            .clone()
                            .expect("gtid cap should be set by previous GtidEvent");
                        let cur_gtid =
                            GtidPartition::new_singleton(source_id, GtidState::Active(tx_id));

                        events::handle_rows_event(
                            data,
                            &repl_context,
                            &cur_gtid,
                            &mut row_event_buffer,
                        )
                        .await?;

                        // Advance the frontier up to the point right before this GTID, since we
                        // might still see other events that are part of this same GTID, such as
                        // row events for multiple tables or large row events split into multiple.
                        if let Err(err) = data_partitions.advance_frontier(cur_gtid) {
                            return Ok(return_definite_error(
                                err,
                                &output_indexes,
                                &data_output,
                                data_cap_set,
                                &definite_error_handle,
                                definite_error_cap_set,
                            )
                            .await);
                        }
                        let new_upper = data_partitions.frontier();
                        repl_context.downgrade_data_cap_set("rows_event", new_upper);
                    }
                    Some(EventData::QueryEvent(event)) => {
                        let (source_id, tx_id) = active_tx
                            .clone()
                            .expect("gtid cap should be set by previous GtidEvent");
                        let cur_gtid =
                            GtidPartition::new_singleton(source_id, GtidState::Active(tx_id));

                        let should_advance =
                            events::handle_query_event(event, &mut repl_context, &cur_gtid).await?;

                        if should_advance {
                            active_tx = None;
                            // Increment the transaction-id to the next GTID we should see from this source-id
                            let next_tx_id = tx_id.checked_add(1).unwrap();
                            let next_gtid = GtidPartition::new_singleton(
                                source_id,
                                GtidState::Active(next_tx_id),
                            );

                            if let Err(err) = data_partitions.advance_frontier(next_gtid) {
                                return Ok(return_definite_error(
                                    err,
                                    &output_indexes,
                                    &data_output,
                                    data_cap_set,
                                    &definite_error_handle,
                                    definite_error_cap_set,
                                )
                                .await);
                            }
                            let new_upper = data_partitions.frontier();
                            repl_context.downgrade_data_cap_set("query_event", new_upper);
                        }
                    }
                    _ => {
                        // TODO: Handle other event types
                        metrics.ignored.inc();
                    }
                }
            }
            // We never expect the replication stream to gracefully end
            Err(TransientError::ReplicationEOF)
        }))
    });

    // TODO: Split row decoding into a separate operator that can be distributed across all workers

    let errors = definite_errors.concat(&transient_errors.map(ReplicationError::from));

    (
        data_stream.as_collection(),
        upper_stream,
        errors,
        button.press_on_drop(),
    )
}

/// Produces the replication stream from the MySQL server. This will return all transactions
/// whose GTIDs were not present in the GTID UUIDs referenced in the `resume_uppper` partitions.
async fn raw_stream<'a>(
    config: &RawSourceCreationConfig,
    mut conn: MySqlConn,
    resume_upper: &Antichain<GtidPartition>,
) -> Result<Result<(BinlogStream, Option<ManagedSshTunnelHandle>), DefiniteError>, TransientError> {
    // Verify the MySQL system settings are correct for consistent row-based replication using GTIDs
    match validate_mysql_repl_settings(&mut conn).await {
        Err(err @ MySqlError::InvalidSystemSetting { .. }) => {
            return Ok(Err(DefiniteError::ServerConfigurationError(
                err.to_string(),
            )));
        }
        Err(err) => Err(err)?,
        Ok(()) => (),
    };

    // To start the stream we need to provide a GTID set of the transactions that we've 'seen'
    // and the server will send us all transactions that have been committed after that point.
    // NOTE: The 'Gno' intervals in this transaction-set use an open set [start, end)
    // interval, which is different than the closed-set [start, end] form returned by the
    // @gtid_executed system variable. So the intervals we construct in this GTID set
    // end with the value of the transaction-id that we want to start replication at,
    // which happens to be the same as the definition of a frontier value.
    // https://dev.mysql.com/doc/refman/8.0/en/replication-options-gtids.html#sysvar_gtid_executed
    // https://dev.mysql.com/doc/dev/mysql-server/latest/classGtid__set.html#ab46da5ceeae0198b90f209b0a8be2a24
    let seen_gtids = resume_upper
        .iter()
        .flat_map(|partition| match partition.timestamp() {
            GtidState::Absent => None,
            GtidState::Active(frontier_time) => {
                let part_uuid = partition
                    .interval()
                    .singleton()
                    .expect("Non-absent paritions will be singletons");
                // NOTE: Since we enforce replica_preserve_commit_order=ON we can start the interval at 1
                // since we know that all transactions with a lower transaction id were monotonic
                Some(
                    Sid::new(*part_uuid.as_bytes())
                        .with_interval(GnoInterval::new(1, frontier_time.get())),
                )
            }
        })
        .collect::<Vec<_>>();

    // Request that the stream provide us with a heartbeat message when no other messages have
    // been sent. This isn't strictly necessary, but is a lightweight additional general
    // health-check for the replication stream
    conn.query_drop(format!(
        "SET @master_heartbeat_period = {};",
        mz_storage_types::dyncfgs::MYSQL_REPLICATION_HEARTBEAT_INTERVAL
            .get(config.config.config_set())
            .as_nanos()
    ))
    .await?;

    // Generate a deterministic server-id for identifying us as a replica on the upstream mysql server.
    // The value does not actually matter since it's irrelevant for GTID-based replication and won't
    // cause errors if it happens to be the same as another replica in the mysql cluster (based on testing),
    // but by setting it to a constant value we can make it easier for users to identify Materialize connections
    let server_id = match config.id {
        GlobalId::System(id) => id,
        GlobalId::User(id) => id,
        GlobalId::Transient(id) => id,
        _ => unreachable!(),
    };
    let server_id = match u32::try_from(server_id) {
        Ok(id) if id + REPLICATION_SERVER_ID_OFFSET < u32::MAX => id + REPLICATION_SERVER_ID_OFFSET,
        _ => REPLICATION_SERVER_ID_OFFSET,
    };

    trace!(
        "requesting replication stream with seen_gtids: {seen_gtids:?} \
         and server_id: {server_id:?}"
    );

    // We need to transform the connection into a BinlogStream (which takes the `Conn` by value),
    // but to avoid dropping any active SSH tunnel used by the connection we need to preserve the
    // tunnel handle and return it
    let (inner_conn, conn_tunnel_handle) = conn.take();

    let repl_stream = match inner_conn
        .get_binlog_stream(
            BinlogStreamRequest::new(server_id)
                .with_gtid()
                .with_gtid_set(seen_gtids),
        )
        .await
    {
        Ok(stream) => stream,
        Err(mysql_async::Error::Server(ref server_err))
            if server_err.code == ER_SOURCE_FATAL_ERROR_READING_BINLOG_CODE =>
        {
            // The GTID set we requested is no longer available
            return Ok(Err(DefiniteError::BinlogNotAvailable));
        }
        // TODO: handle other error types. Some may require a re-snapshot and some may be transient
        Err(err) => return Err(err.into()),
    };

    Ok(Ok((repl_stream, conn_tunnel_handle)))
}
