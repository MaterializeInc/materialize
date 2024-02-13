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
//! [`GtidReplicationPartitions`] struct to store the GTID Set as a set of partitions.
//! This allows us to easily advance the frontier each time we see a new GTID on the replication
//! stream.
//!
//! # Resumption
//!
//! When the dataflow is resumed, the MySQL replication stream is started from the GTID frontier
//! of the minimum frontier across all subsources. This is compared against the GTID set that may
//! still be obtained from the MySQL server, using the @@GTID_PURGED value in MySQL to determine
//! GTIDs that are no longer available in the binlog and to put the source in an error state if
//! we cannot resume from the GTID frontier.
//!
//! # Rewinds
//!
//! The replication stream may be resumed from a point before the snapshot for a specific table
//! occurs. To avoid double-counting updates that were present in the snapshot, we store a map
//! of pending rewinds that we've received from the snapshot operator, and when we see updates
//! for a table that were present in the snapshot, we negate the snapshot update
//! (at the minimum timestamp) and send it again at the correct GTID.

use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::num::NonZeroU64;
use std::pin::pin;
use std::time::Duration;

use differential_dataflow::{AsCollection, Collection};
use futures::StreamExt;
use itertools::Itertools;
use mysql_async::prelude::Queryable;
use mysql_async::{BinlogStream, BinlogStreamRequest, GnoInterval, Sid};
use mz_ssh_util::tunnel_manager::ManagedSshTunnelHandle;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Concat, Map};
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use timely::progress::Timestamp;
use timely::PartialOrder;
use tracing::trace;
use uuid::Uuid;

use mz_mysql_util::{
    query_sys_var, MySqlConn, MySqlTableDesc, ER_SOURCE_FATAL_ERROR_READING_BINLOG_CODE,
};
use mz_ore::cast::CastFrom;
use mz_ore::result::ResultExt;
use mz_repr::{Diff, GlobalId, Row};
use mz_sql_parser::ast::UnresolvedItemName;
use mz_storage_types::sources::mysql::{gtid_set_frontier, GtidPartition, GtidState};
use mz_storage_types::sources::MySqlSourceConnection;
use mz_timely_util::builder_async::{
    Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};

use crate::metrics::mysql::MySqlSourceMetrics;
use crate::source::mysql::GtidReplicationPartitions;
use crate::source::types::SourceReaderError;
use crate::source::RawSourceCreationConfig;

use super::{
    pack_mysql_row, return_definite_error, table_name, validate_mysql_repl_settings, DefiniteError,
    ReplicationError, RewindRequest, TransientError,
};

mod events;

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
    subsource_resume_uppers: BTreeMap<GlobalId, Antichain<GtidPartition>>,
    table_info: BTreeMap<UnresolvedItemName, (usize, MySqlTableDesc)>,
    rewind_stream: &Stream<G, RewindRequest>,
    metrics: MySqlSourceMetrics,
) -> (
    Collection<G, (usize, Result<Row, SourceReaderError>), Diff>,
    Stream<G, Infallible>,
    Stream<G, ReplicationError>,
    PressOnDropButton,
) {
    let op_name = format!("MySqlReplicationReader({})", config.id);
    let mut builder = AsyncOperatorBuilder::new(op_name, scope);

    let repl_reader_id = u64::cast_from(config.responsible_worker(REPL_READER));
    let (mut data_output, data_stream) = builder.new_output();
    let (upper_output, upper_stream) = builder.new_output();
    // Captures DefiniteErrors that affect the entire source, including all subsources
    let (mut definite_error_handle, definite_errors) = builder.new_output();
    let mut rewind_input = builder.new_input_for_many(
        rewind_stream,
        Exchange::new(move |_| repl_reader_id),
        [&data_output, &upper_output],
    );

    let output_indexes = table_info
        .values()
        .map(|(output_index, _)| *output_index)
        .collect_vec();

    // TODO: Add additional metrics

    let (button, transient_errors) = builder.build_fallible(move |caps| {
        Box::pin(async move {
            // Keep the metrics alive during replication.
            let _metrics = metrics;

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
                    &*config.config.connection_context.secrets_reader,
                    &config.config,
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
                            &mut data_output,
                            data_cap_set,
                            &mut definite_error_handle,
                            definite_error_cap_set,
                        )
                        .await,
                    );
                }
            };

            trace!(%id, "timely-{worker_id} replication binlog frontier: {binlog_frontier:?}");

            // Calculate the lowest frontier across all subsources, which represents the point which
            // we should start replication from.
            let resume_upper =
                match Antichain::from_iter(subsource_resume_uppers.into_values().flatten()) {
                    upper if upper == Antichain::from_elem(GtidPartition::minimum()) => {
                        // If any subsource is at the minimum frontier then we are either starting
                        // from scratch or at least one table has to complete its initial snapshot.
                        //
                        // In either case, we need to ensure that the resumption point is
                        // deterministic and consistent across all tables, so that a user can't
                        // observe an MZ timestamp that may contain different snapshots for
                        // different tables.
                        // We've chosen the frontier beyond the GTID Set recorded
                        // during purification as this consistent resume point.
                        //
                        // Tables may still be snapshot at a future GTID Set but as long as we can
                        // resume the replication stream from this consistent point all updates
                        // will be rewound appropriately, such that the effective snapshot point
                        // is consistent across all tables.
                        gtid_set_frontier(&connection.details.initial_gtid_set)?
                    }
                    upper => upper,
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
                    &mut data_output,
                    data_cap_set,
                    &mut definite_error_handle,
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
                                &mut data_output,
                                data_cap_set,
                                &mut definite_error_handle,
                                definite_error_cap_set,
                            )
                            .await);
                        };
                        // If the snapshot point is the same as the resume point then we don't need to rewind
                        if resume_upper != req.snapshot_upper {
                            rewinds.insert(req.table.clone(), (caps.clone(), req));
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
                            &mut data_output,
                            data_cap_set,
                            &mut definite_error_handle,
                            definite_error_cap_set,
                        )
                        .await)
                    }
                };
            let mut stream = pin!(binlog_stream.peekable());

            // Store all partitions from the resume_upper so we can create a frontier that comprises
            // timestamps for partitions representing the full range of UUIDs to advance our main
            // capabilities.
            let mut repl_partitions: GtidReplicationPartitions = resume_upper.into();

            // Binlog Table Id -> Table Name (its key in the `table_info` map)
            let mut table_id_map = BTreeMap::<u64, UnresolvedItemName>::new();
            let mut skipped_table_ids = BTreeSet::<u64>::new();

            let mut errored_tables: BTreeSet<UnresolvedItemName> = BTreeSet::new();

            let mut final_row = Row::default();

            let mut next_gtid: Option<GtidPartition> = None;

            while let Some(event) = stream.as_mut().next().await {
                use mysql_async::binlog::events::*;
                let event = event?;
                let event_data = event.read_data()?;

                match event_data {
                    Some(EventData::HeartbeatEvent) => {
                        // The upstream MySQL source only emits a heartbeat when there are no other events
                        // sent within the heartbeat interval. This means that we can safely advance the
                        // frontier once since we know there were no more events for the last sent GTID.
                        // See: https://dev.mysql.com/doc/refman/8.0/en/replication-administration-status.html
                        if let Some(mut new_gtid) = next_gtid.take() {
                            // Increment the transaction-id to the next GTID we should see from this source-id
                            match new_gtid.timestamp_mut() {
                                GtidState::Active(time) => {
                                    *time = time.checked_add(1).unwrap();
                                }
                                _ => unreachable!(),
                            }

                            if let Err(err) = repl_partitions.update(new_gtid) {
                                return Ok(return_definite_error(
                                    err,
                                    &output_indexes,
                                    &mut data_output,
                                    data_cap_set,
                                    &mut definite_error_handle,
                                    definite_error_cap_set,
                                )
                                .await);
                            }
                            let new_upper = repl_partitions.frontier();

                            trace!(%id, "timely-{worker_id} advancing frontier from \
                                         heartbeat to {new_upper:?}");
                            data_cap_set.downgrade(&*new_upper);
                            upper_cap_set.downgrade(&*new_upper);

                            rewinds.retain(|_, (_, req)| {
                                // We need to retain the rewind requests whose snapshot_upper has
                                // at least one timestamp such that new_upper is less than that
                                // timestamp
                                req.snapshot_upper.iter().any(|ts| new_upper.less_than(ts))
                            });

                            trace!(%id, "timely-{worker_id} pending rewinds after \
                                         filtering: {rewinds:?}");
                        }
                    }
                    // We receive a GtidEvent that tells us the GTID of the incoming RowsEvents (and other events)
                    Some(EventData::GtidEvent(event)) => {
                        let source_id = Uuid::from_bytes(event.sid());
                        let received_tx_id =
                            GtidState::Active(NonZeroU64::new(event.gno()).unwrap());

                        // Store this GTID as a partition timestamp for ease of use in publishing data
                        next_gtid = Some(GtidPartition::new_singleton(source_id, received_tx_id));
                    }
                    // A row event is a write/update/delete event
                    Some(EventData::RowsEvent(data)) => {
                        // Find the relevant table
                        let binlog_table_id = data.table_id();
                        let table_map_event =
                            stream.get_ref().get_tme(binlog_table_id).ok_or_else(|| {
                                TransientError::Generic(anyhow::anyhow!(
                                    "Table map event not found"
                                ))
                            })?;
                        let table = match (
                            table_id_map.get(&binlog_table_id),
                            skipped_table_ids.get(&binlog_table_id),
                        ) {
                            (Some(table), None) => table,
                            (None, Some(_)) => {
                                // We've seen this table ID before and it was skipped
                                continue;
                            }
                            (None, None) => {
                                let table = table_name(
                                    &*table_map_event.database_name(),
                                    &*table_map_event.table_name(),
                                )?;
                                if table_info.contains_key(&table) {
                                    table_id_map.insert(binlog_table_id, table);
                                    &table_id_map[&binlog_table_id]
                                } else {
                                    skipped_table_ids.insert(binlog_table_id);
                                    trace!(
                                        "timely-{worker_id} skipping table {table:?} \
                                         with id {binlog_table_id}"
                                    );
                                    // We don't know about this table, so skip this event
                                    continue;
                                }
                            }
                            _ => unreachable!(),
                        };
                        if errored_tables.contains(table) {
                            continue;
                        }

                        let (output_index, table_desc) = &table_info[table];
                        let new_gtid = next_gtid
                            .as_ref()
                            .expect("gtid cap should be set by previous GtidEvent");

                        // Iterate over the rows in this RowsEvent. Each row is a pair of 'before_row', 'after_row',
                        // to accomodate for updates and deletes (which include a before_row),
                        // and updates and inserts (which inclued an after row).
                        let mut container = Vec::new();
                        let mut rows_iter = data.rows(table_map_event);
                        while let Some(Ok((before_row, after_row))) = rows_iter.next() {
                            let updates = [before_row.map(|r| (r, -1)), after_row.map(|r| (r, 1))];
                            for (binlog_row, diff) in updates.into_iter().flatten() {
                                let row = mysql_async::Row::try_from(binlog_row)?;
                                let packed_row = pack_mysql_row(&mut final_row, row, table_desc)?;
                                let data = (*output_index, packed_row);

                                // Rewind this update if it was already present in the snapshot
                                if let Some(([data_cap, _upper_cap], rewind_req)) =
                                    rewinds.get(table)
                                {
                                    if !rewind_req.snapshot_upper.less_equal(new_gtid) {
                                        trace!(%id, "timely-{worker_id} rewinding update \
                                                     {data:?} for {new_gtid:?}");
                                        data_output
                                            .give(
                                                data_cap,
                                                (data.clone(), GtidPartition::minimum(), -diff),
                                            )
                                            .await;
                                    }
                                }
                                trace!(%id, "timely-{worker_id} sending update {data:?}
                                             for {new_gtid:?}");
                                container.push((data, new_gtid.clone(), diff));
                            }
                        }

                        // Flush this data
                        let gtid_cap = data_cap_set.delayed(new_gtid);
                        trace!(%id, "timely-{worker_id} sending container for {new_gtid:?} \
                                     with {container:?} updates");
                        data_output.give_container(&gtid_cap, &mut container).await;

                        // Advance the frontier up to the point right before this GTID
                        // We are being careful here and not advancing beyond this GTID since we aren't
                        // sure that other mysql event-types may need to be supported that present other
                        // data to commit at this timestamp. In the future if we are sure that this RowsEvent
                        // represents all the data that we need to commit at this timestamp and that there is no
                        // then we can do so (like we currently do when handling Heartbeat Events).
                        // This is still useful to allow data committed at previous timestamps to become available
                        // without waiting for the next break in data at which point we should receive a Heartbeat
                        // Event.
                        if let Err(err) = repl_partitions.update(new_gtid.clone()) {
                            return Ok(return_definite_error(
                                err,
                                &output_indexes,
                                &mut data_output,
                                data_cap_set,
                                &mut definite_error_handle,
                                definite_error_cap_set,
                            )
                            .await);
                        }
                        let new_upper = repl_partitions.frontier();

                        trace!(%id, "timely-{worker_id} advancing frontier to {new_upper:?}");
                        data_cap_set.downgrade(&*new_upper);
                        upper_cap_set.downgrade(&*new_upper);

                        rewinds.retain(|_, (_, req)| {
                            // We need to retain the rewind requests whose snapshot_upper has
                            // at least one timestamp such that new_upper is less than that
                            // timestamp
                            req.snapshot_upper.iter().any(|ts| new_upper.less_than(ts))
                        });
                        trace!(%id, "timely-{worker_id} pending rewinds \
                                     after filtering: {rewinds:?}");
                    }

                    Some(EventData::QueryEvent(event)) => {
                        trace!(%id, "timely-{worker_id} received query event {event:?}");
                        events::handle_query_event(
                            event,
                            &config,
                            &connection_config,
                            &table_info,
                            &next_gtid,
                            &mut errored_tables,
                            &mut data_output,
                            data_cap_set,
                        )
                        .await?;
                    }
                    _ => {
                        // TODO: Handle other event types
                    }
                }
            }
            // We never expect the replication stream to gracefully end
            Err(TransientError::ReplicationEOF)
        })
    });

    // TODO: Split row decoding into a separate operator that can be distributed across all workers
    let replication_updates = data_stream
        .as_collection()
        .map(|(output_index, row)| (output_index, row.err_into()));

    let errors = definite_errors.concat(&transient_errors.map(ReplicationError::from));

    (
        replication_updates,
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
    if let Err(err) = validate_mysql_repl_settings(&mut conn).await {
        return Ok(Err(DefiniteError::ServerConfigurationError(
            err.to_string(),
        )));
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

    // Request that the stream provide us with a heartbeat message when no other messages have been sent
    let heartbeat = Duration::from_secs(3);
    conn.query_drop(format!(
        "SET @master_heartbeat_period = {};",
        heartbeat.as_nanos()
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
