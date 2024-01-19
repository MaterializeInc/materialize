// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::pin::pin;
use std::rc::Rc;
use std::str::FromStr;
use std::time::Duration;

use differential_dataflow::{AsCollection, Collection};
use futures::StreamExt;
use itertools::Itertools;
use mysql_async::prelude::Queryable;
use mysql_async::{BinlogStream, BinlogStreamRequest, Conn, GnoInterval, Sid};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::channels::pushers::TeeCore;
use timely::dataflow::operators::{CapabilitySet, Concat, Map};
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use timely::progress::Timestamp;
use tracing::trace;
use uuid::Uuid;

use mz_mysql_util::GtidSet;
use mz_mysql_util::{
    ensure_full_row_binlog_format, ensure_gtid_consistency, ensure_replication_commit_order,
    MySqlTableDesc, ER_SOURCE_FATAL_ERROR_READING_BINLOG_CODE,
};
use mz_ore::cast::CastFrom;
use mz_ore::result::ResultExt;
use mz_repr::{Diff, GlobalId, Row};
use mz_sql_parser::ast::UnresolvedItemName;
use mz_storage_types::sources::mysql::{GtidPartition, TransactionId};
use mz_storage_types::sources::MySqlSourceConnection;
use mz_timely_util::builder_async::{
    AsyncOutputHandle, Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder,
    PressOnDropButton,
};
use mz_timely_util::order::{Extrema, Sequence};

use crate::source::types::SourceReaderError;
use crate::source::RawSourceCreationConfig;

use super::{
    gtid_singletons, pack_mysql_row, table_name, DefiniteError, GtidPartitionCapability,
    ReplicationError, RewindRequest, TransientError,
};

// Used as a partition id to determine if the worker is responsible for reading from the
// MySQL replication stream
static REPL_READER: &str = "reader";

// A constant arbitrary offset to add to the source-id to produce a deterministic server-id for
// identifying Materialize as a replica on the upstream MySQL server.
// TODO(roshan): Add documentation for this
static REPLICATION_SERVER_ID_OFFSET: u32 = 524000;

/// Renders the replication dataflow. See the module documentation for more information.
pub(crate) fn render<G: Scope<Timestamp = GtidPartition>>(
    scope: G,
    config: RawSourceCreationConfig,
    connection: MySqlSourceConnection,
    subsource_resume_uppers: BTreeMap<GlobalId, Antichain<GtidPartition>>,
    table_info: BTreeMap<UnresolvedItemName, (usize, MySqlTableDesc)>,
    rewind_stream: &Stream<G, RewindRequest>,
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

    // TODO: Add metrics

    let (button, transient_errors) = builder.build_fallible(move |caps| {
        Box::pin(async move {
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

            // Get the set of GTIDs currently available in the binlogs (GTIDs that we can safely start replication from)
            let binlog_gtid_set: String = conn.query_first("SELECT GTID_SUBTRACT(@@global.gtid_executed, @@global.gtid_purged)").await?.unwrap();
            // For each GTID UUID in the binlog set, get a partitioned timestamp representing the transaction-id of the first transaction that is available
            let binlog_start_gtids: BTreeMap<Uuid, _> = match gtid_singletons(GtidSet::from_str(&binlog_gtid_set)?, false) {
                Ok(res) => res,
                // If GTID intervals in the binlog are not available in a monotonic consecutive order this breaks all of our assumptions
                // and there is nothing else we can do. This can occur if the mysql server is restored to a previous point-in-time
                // or if a user manually adds transactions to the @@gtid_purged system var.
                Err(err) => return Ok(
                    return_definite_error(err, &output_indexes, &mut data_output, data_cap_set, &mut definite_error_handle, definite_error_cap_set).await
                )
            }.into_iter().map(|part| (part.interval().singleton().unwrap().clone(), part)).collect();
            trace!(%id, "timely-{worker_id} replication binlog contains gtids starting at: {binlog_start_gtids:?}");

            // Calculate the resume upper that represents the start transaction-id for each GTID UUID
            // that we should resume from, across all sub-sources.
            let resume_upper = Antichain::from_iter(
                subsource_resume_uppers
                    .values()
                    .flat_map(|f| f.elements())
                    // Remove any partitions representing UUIDs that do not yet exist
                    .filter(|t| t.timestamp() != &TransactionId::Absent)
                    // Advance each partition's upper as far as the start transaction-id of the corresponding GTID UUID available in the binlog
                    .map(|t| {
                        let part = t.interval().singleton().expect("filtered non-singletons");
                        let other = match binlog_start_gtids.get(part) {
                            Some(binlog_start_gtid) => binlog_start_gtid.clone(),
                            None => GtidPartition::new_singleton(part.clone(), TransactionId::Absent),
                        };
                        if t > &other {
                            // TODO: Can we avoid this clone? Unfortunately there doesn't seem to be a way to take the element from an Antichain
                            t.clone()
                        } else {
                            other
                        }
                    })
            );

            // Create capabilities for each active partition that we might see events for, and generate a frontier of all
            // future partitions that may exist (the UUID gaps between the active partitions).
            let mut active_partition_caps = BTreeMap::new();
            let mut future_partitions = Antichain::new();
            let mut prev = Uuid::nil();
            for ts in resume_upper.iter().sorted_by(|a, b| a.interval().singleton().unwrap().cmp(b.interval().singleton().unwrap())) {
                let part = ts.interval().singleton().unwrap();
                assert!(prev < *part, "duplicate or unsorted partitions");

                // Create a partition representing all the UUIDs in the gap between the previous one and this one
                let gap = GtidPartition::new_range(prev, part.before(), TransactionId::Absent);
                future_partitions.insert(gap);

                // Set prev to the UUID + 1 so the next gap starts after this one
                prev = part.after();

                // Mint capabilities for this partition at the resume transaction-id
                let part_ts = GtidPartition::new_singleton(part.clone(), ts.timestamp().clone());
                trace!(%id, "timely-{worker_id} creating partition capability for {part_ts:?}");
                let cap = GtidPartitionCapability::new(&part_ts, data_cap_set, upper_cap_set);
                active_partition_caps.insert(part.clone(), cap);
            }
            future_partitions.insert(GtidPartition::new_range(prev, Uuid::maximum(), TransactionId::Absent));

            data_cap_set.downgrade(&*future_partitions);
            upper_cap_set.downgrade(&*future_partitions);
            trace!(%id, "timely-{worker_id} replication reader started at {:?}", resume_upper);

            let mut rewinds = BTreeMap::new();
            while let Some(event) = rewind_input.next().await {
                if let AsyncEvent::Data(caps, data) = event {
                    for req in data {
                        let mut rewind_partition_map = BTreeMap::new();
                        for snapshot_partition in req.snapshot_gtid_set {
                            let part = snapshot_partition.interval().singleton().unwrap();
                            let snapshot_time = snapshot_partition.timestamp();

                            // Check that the replication stream can be resumed from the snapshot point or before.
                            if !binlog_start_gtids.get(part).is_some_and(|binlog_start_gtid| binlog_start_gtid.timestamp() <= &(*snapshot_time + TransactionId::Active(1))) {
                                let err = DefiniteError::BinlogMissingResumePoint(snapshot_time.into(), part.to_string());
                                return Ok(
                                    return_definite_error(err, &output_indexes, &mut data_output, data_cap_set, &mut definite_error_handle, definite_error_cap_set).await
                                )
                            }
                            rewind_partition_map.insert(part.clone(), snapshot_partition);
                        }
                        rewinds.insert(req.table.clone(), (caps.clone(), rewind_partition_map));
                    }
                }
            }
            trace!(%id, "timely-{worker_id} pending rewinds {rewinds:?}");

            let binlog_stream = match raw_stream(&config, conn, &resume_upper).await? {
                Ok(stream) => stream,
                // If the replication stream cannot be obtained in a definite way there is
                // nothing else to do. These errors are not retractable.
                Err(err) => return Ok(
                    return_definite_error(err, &output_indexes, &mut data_output, data_cap_set, &mut definite_error_handle, definite_error_cap_set).await
                )
            };
            let mut stream = pin!(binlog_stream.peekable());

            // Binlog Table Id -> Table Name (its key in the `table_info` map)
            let mut table_id_map = BTreeMap::<u64, UnresolvedItemName>::new();
            let mut skipped_table_ids = BTreeSet::<u64>::new();

            let mut final_row = Row::default();

            let mut new_gtid_upper: Option<GtidPartition> = None;

            trace!(%id, "timely-{worker_id} starting replication at {resume_upper:?}");
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
                        if let Some(new_gtid) = new_gtid_upper {
                            let new_upper = GtidPartition::new_singleton(
                                new_gtid.interval().singleton().unwrap().clone(),
                                *new_gtid.timestamp() + TransactionId::Active(1),
                            );

                            trace!(%id, "heartbeat: timely-{worker_id} advancing partition frontier to {new_upper:?}");
                            active_partition_caps.get_mut(new_gtid.interval().singleton().unwrap()).unwrap()
                                .downgrade(&new_upper);
                            data_cap_set.downgrade(&*future_partitions);
                            upper_cap_set.downgrade(&*future_partitions);

                            // Retain any rewinds whose snapshot still contains partitions with transaction-ids greater
                            // than their corresponding frontiers
                            rewinds.retain(|_, (_, partitions)|
                                partitions.iter().any(|(uuid, rewind_part)|
                                    active_partition_caps.get(uuid).is_some_and(|active_cap| active_cap.tx_id() <= &rewind_part.timestamp())
                            ));
                            trace!(%id, "timely-{worker_id} pending rewinds after filtering: {rewinds:?}");

                            new_gtid_upper = None;
                        }
                    }
                    // We receive a GtidEvent that tells us the GTID of the incoming RowsEvents (and other events)
                    Some(EventData::GtidEvent(event)) => {
                        let source_id = Uuid::from_bytes(event.sid());
                        let received_tx_id = TransactionId::Active(event.gno());

                        // Validate that we have an active partition capability for the GTID UUID
                        match active_partition_caps.get_mut(&source_id) {
                            Some(active_cap) => {
                                if active_cap.tx_id() > &received_tx_id {
                                    let err = DefiniteError::BinlogGtidMonotonicityViolation(source_id.to_string(), received_tx_id.into());
                                    return Ok(
                                        return_definite_error(err, &output_indexes, &mut data_output, data_cap_set, &mut definite_error_handle, definite_error_cap_set).await
                                    )
                                }
                            }
                            // We've received a GTID for a UUID we don't yet know about
                            None => {
                                // Find the corresponding partition that holds this UUID in `future_partitions`
                                let (contained_part, other_parts): (Vec<_>, Vec<_>) = future_partitions
                                    .into_iter()
                                    .partition(|part| part.interval().contains(&source_id));
                                assert_eq!(contained_part.len(), 1, "expected exactly one partition to contain the UUID");

                                // Split the future partition into partitions for before and after this UUID, and recreate the future_partitions antichain
                                let new_ranges = contained_part[0].split(&source_id);
                                future_partitions = Antichain::from_iter(other_parts.into_iter().chain(new_ranges.into_iter()));

                                // Mint capabilities for this new GTID UUID partition
                                let new_cap = GtidPartitionCapability::new(&GtidPartition::new_singleton(source_id, TransactionId::Absent), data_cap_set, upper_cap_set);
                                active_partition_caps.insert(source_id, new_cap);
                            }
                        };

                        new_gtid_upper = Some(GtidPartition::new_singleton(source_id, received_tx_id));
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
                                    trace!("timely-{worker_id} skipping table {table:?} with id {binlog_table_id}");
                                    // We don't know about this table, so skip this event
                                    continue;
                                }
                            }
                            _ => unreachable!(),
                        };

                        trace!(%id, "timely-{worker_id} received rows event: {data:?}");

                        let (output_index, table_desc) = &table_info[table];
                        let new_gtid = new_gtid_upper.as_ref().expect("gtid cap should be set by previous GtidEvent");
                        let gtid_uuid = new_gtid.interval().singleton().unwrap();

                        // If there is a rewind event for this table, get the transaction-id of the current GTID UUID if its in the rewind snapshot
                        let rewind_tx = rewinds.get(table).and_then(|(caps, partitions)| match partitions.get(gtid_uuid) {
                            Some(partition) => Some((caps, partition.timestamp())),
                            None => None,
                        });

                        // Iterate over the rows in this RowsEvent. Each row is a pair of 'before_row', 'after_row',
                        // to accomodate for updates and deletes (which include a before_row),
                        // and updates and inserts (which inclued an after row).
                        let mut container = Vec::new();
                        let mut rows_iter = data.rows(table_map_event);
                        while let Some(Ok((before_row, after_row))) = rows_iter.next() {
                            let updates = [before_row.map(|r| (r, -1)), after_row.map(|r| (r, 1))];
                            for (binlog_row, diff) in updates.into_iter().flatten() {
                                // TODO: Map columns from table schema to indexes in each row
                                let row = mysql_async::Row::try_from(binlog_row)?;
                                let packed_row = pack_mysql_row(&mut final_row, row, table_desc)?;
                                let data = (*output_index, packed_row);

                                // Rewind this update if it was already present in the snapshot
                                if let Some(([data_cap, _upper_cap], rewind_tx_id)) = rewind_tx {
                                    if new_gtid.timestamp() <= rewind_tx_id {
                                        data_output.give(data_cap, (data.clone(), GtidPartition::minimum(), -diff)).await;
                                    }
                                }
                                trace!(%id, "timely-{worker_id} sending update {data:?} for {new_gtid:?}");
                                container.push((data, new_gtid.clone(), diff));
                            }
                        }

                        // Flush and advance the frontier for this partition
                        let gtid_cap = active_partition_caps.get_mut(gtid_uuid).unwrap();
                        data_output.give_container(&gtid_cap.data, &mut container).await;
                        trace!(%id, "timely-{worker_id} advancing partition frontier to {new_gtid:?}");
                        gtid_cap.downgrade(new_gtid);
                        data_cap_set.downgrade(&*future_partitions);
                        upper_cap_set.downgrade(&*future_partitions);
                        // Retain any rewinds whose snapshot still contains partitions with transaction-ids greater
                        // than their corresponding frontiers
                        rewinds.retain(|_, (_, partitions)|
                            partitions.iter().any(|(uuid, rewind_part)|
                                active_partition_caps.get(uuid).is_some_and(|active_cap| active_cap.tx_id() <= &rewind_part.timestamp())
                        ));
                        trace!(%id, "timely-{worker_id} pending rewinds after filtering: {rewinds:?}");
                    }
                    _ => {
                        // TODO: Handle other event types
                        // We definitely need to handle 'query' events and parse them for DDL changes
                        // that might affect the schema of the tables we care about.
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

async fn return_definite_error(
    err: DefiniteError,
    outputs: &[usize],
    data_handle: &mut AsyncOutputHandle<
        GtidPartition,
        Vec<((usize, Result<Row, DefiniteError>), GtidPartition, i64)>,
        TeeCore<GtidPartition, Vec<((usize, Result<Row, DefiniteError>), GtidPartition, i64)>>,
    >,
    data_cap_set: &CapabilitySet<GtidPartition>,
    definite_error_handle: &mut AsyncOutputHandle<
        GtidPartition,
        Vec<ReplicationError>,
        TeeCore<GtidPartition, Vec<ReplicationError>>,
    >,
    definite_error_cap_set: &CapabilitySet<GtidPartition>,
) -> () {
    for output_index in outputs {
        let update = (
            (*output_index, Err(err.clone())),
            GtidPartition::new_range(Uuid::minimum(), Uuid::maximum(), TransactionId::MAX),
            1,
        );
        data_handle.give(&data_cap_set[0], update).await;
    }
    definite_error_handle
        .give(
            &definite_error_cap_set[0],
            ReplicationError::Definite(Rc::new(err)),
        )
        .await;
    ()
}

/// Produces the replication stream from the MySQL server. This will return all transactions whose GTIDs were not
/// present in the GTID UUIDs referenced in the `resume_uppper` partitions.
async fn raw_stream<'a>(
    config: &RawSourceCreationConfig,
    mut conn: Conn,
    resume_upper: &Antichain<GtidPartition>,
) -> Result<Result<BinlogStream, DefiniteError>, TransientError> {
    // Verify the MySQL system settings are correct for consistent row-based replication using GTIDs
    // TODO: Should these return DefiniteError instead of TransientError?
    ensure_gtid_consistency(&mut conn).await?;
    ensure_full_row_binlog_format(&mut conn).await?;
    ensure_replication_commit_order(&mut conn).await?;

    // To start the stream we need to provide a GTID set of the transactions that we've 'seen'
    // and the server will send us all transactions that have been committed after that point.
    // NOTE: The 'Gno' intervals in this transaction-set use an open set [start, end)
    // interval, which is different than the closed-set [start, end] form returned by the
    // @gtid_executed system variable. So the intervals we construct in this GTID set
    // end with the value of the transaction-id that we want to start replication at,
    // which happens to be our `resume_transaction_id`
    // https://dev.mysql.com/doc/refman/8.0/en/replication-options-gtids.html#sysvar_gtid_executed
    // https://dev.mysql.com/doc/dev/mysql-server/latest/classGtid__set.html#ab46da5ceeae0198b90f209b0a8be2a24
    let seen_gtids = resume_upper
        .iter()
        // If we're starting from the beginning of the binlog or the first transaction id
        // we haven't seen 'anything'
        .filter(|partition| Into::<u64>::into(partition.timestamp()) > 1)
        .map(|partition| {
            let part_uuid = partition.interval().singleton().unwrap();
            // NOTE: Since we enforce replica_preserve_commit_order=ON we can start the interval at 1
            // since we know that all transactions with a lower transaction id were monotonic
            Sid::new(*part_uuid.as_bytes())
                .with_interval(GnoInterval::new(1, partition.timestamp().into()))
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
    // The value does not actually matter since it's irrelevant for GTID-based replication and won't cause errors
    // if it happens to be the same as another replica in the mysql cluster (based on testing),
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

    let repl_stream = match conn
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

    Ok(Ok(repl_stream))
}
