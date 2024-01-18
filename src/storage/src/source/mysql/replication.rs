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

use bytes::Bytes;
use differential_dataflow::{AsCollection, Collection};
use futures::StreamExt;
use mysql_async::prelude::Queryable;
use mysql_async::{BinlogStream, BinlogStreamRequest, Conn, GnoInterval, Sid};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Concat, Map};
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
use mz_storage_types::sources::MySqlSourceConnection;
use mz_timely_util::builder_async::{
    Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};

use crate::source::types::SourceReaderError;
use crate::source::RawSourceCreationConfig;

use super::{
    pack_mysql_row, table_name, DefiniteError, ReplicationError, RewindRequest, TransactionId,
    TransientError,
};

// Used as a partition id to determine if the worker is responsible for reading from the
// MySQL replication stream
static REPL_READER: &str = "reader";

// A constant arbitrary offset to add to the source-id to produce a deterministic server-id for
// identifying Materialize as a replica on the upstream MySQL server.
// TODO(roshan): Add documentation for this
static REPLICATION_SERVER_ID_OFFSET: u32 = 524000;

/// Renders the replication dataflow. See the module documentation for more information.
pub(crate) fn render<G: Scope<Timestamp = TransactionId>>(
    scope: G,
    config: RawSourceCreationConfig,
    connection: MySqlSourceConnection,
    subsource_resume_uppers: BTreeMap<GlobalId, Antichain<TransactionId>>,
    table_info: BTreeMap<UnresolvedItemName, (usize, MySqlTableDesc)>,
    rewind_stream: &Stream<G, RewindRequest>,
    _committed_uppers: impl futures::Stream<Item = Antichain<TransactionId>> + 'static,
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

    // TODO: Add metrics

    let (button, transient_errors) = builder.build_fallible(move |caps| {
        Box::pin(async move {
            let (id, worker_id) = (config.id, config.worker_id);
            let [data_cap_set, upper_cap_set, definite_error_cap_set]: &mut [_; 3] = caps.try_into().unwrap();

            // Only run the replication reader on the worker responsible for it.
            if !config.responsible_for(REPL_READER) {
                return Ok(());
            }

            let connection_config = connection
                .connection
                .config(&*config.config.connection_context.secrets_reader, &config.config)
                .await?;

            let mut conn = connection_config.connect(
                &format!("timely-{worker_id} MySQL replication reader"),
                &config.config.connection_context.ssh_tunnel_manager,
            ).await?;

            // Get the set of GTIDs currently available in the binlogs (GTIDs that we can safely start replication from)
            let binlog_gtid_set: String = conn.query_first("SELECT GTID_SUBTRACT(@@global.gtid_executed, @@global.gtid_purged)").await?.unwrap();
            // NOTE: The assumption here for the MVP is that there is only one source-id in the GTID set, so we can just
            // take the transaction_id from the first one. Fix this once we move to a partitioned timestamp.
            let binlog_start_gtid = GtidSet::from_str(&binlog_gtid_set)?.first().expect("At least one gtid").to_owned();
            let binlog_start_transaction_id = TransactionId::new(binlog_start_gtid.earliest_transaction_id());

            let resume_upper = Antichain::from_iter(
                subsource_resume_uppers
                    .values()
                    .flat_map(|f| f.elements())
                    // Advance any upper as far as the start of the binlog.
                    .map(|t| std::cmp::max(*t, binlog_start_transaction_id))
            );

            let Some(resume_transaction_id) = resume_upper.into_option() else {
                return Ok(());
            };
            data_cap_set.downgrade([&resume_transaction_id]);
            upper_cap_set.downgrade([&resume_transaction_id]);
            trace!(%id, "timely-{worker_id} replication reader started transaction_id={}", resume_transaction_id);

            let mut rewinds = BTreeMap::new();
            while let Some(event) = rewind_input.next().await {
                if let AsyncEvent::Data(caps, data) = event {
                    for req in data {
                        let snapshot_transaction_id = TransactionId::new(req.snapshot_gtid_set.first().expect("at least one gtid").latest_transaction_id());
                        if resume_transaction_id > snapshot_transaction_id + TransactionId::new(1) {
                            let err = DefiniteError::BinlogCompactedPastResumePoint(resume_transaction_id.to_string(), (snapshot_transaction_id + TransactionId::new(1)).to_string());
                            // If the replication stream cannot be obtained from the resume point there is
                            // nothing else to do. These errors are not retractable.
                            for (output_index, _) in table_info.values() {
                                let update = ((*output_index, Err(err.clone())), TransactionId::MAX, 1);
                                data_output.give(&data_cap_set[0], update).await;
                            }
                            definite_error_handle.give(&definite_error_cap_set[0], ReplicationError::Definite(Rc::new(err))).await;
                            return Ok(());
                        }
                        rewinds.insert(req.table.clone(), (caps.clone(), req));
                    }
                }
            }
            trace!(%id, "timely-{worker_id} pending rewinds {rewinds:?}");

            let stream_result = raw_stream(
                &config,
                conn,
                binlog_start_gtid.uuid,
                *data_cap_set[0].time(),
            )
            .await?;

            let binlog_stream = match stream_result {
                Ok(stream) => stream,
                Err(err) => {
                    // If the replication stream cannot be obtained in a definite way there is
                    // nothing else to do. These errors are not retractable.
                    for (output_index, _) in table_info.values() {
                        let update = ((*output_index, Err(err.clone())), TransactionId::MAX, 1);
                        data_output.give(&data_cap_set[0], update).await;
                    }
                    definite_error_handle.give(&definite_error_cap_set[0], ReplicationError::Definite(Rc::new(err))).await;
                    return Ok(());
                }
            };
            let mut stream = pin!(binlog_stream.peekable());

            let mut container = Vec::new();
            let max_capacity = timely::container::buffer::default_capacity::<((u32, Result<Vec<Option<Bytes>>, DefiniteError>), TransactionId, Diff)>();

            // Binlog Table Id -> Table Name (its key in the `table_info` map)
            let mut table_id_map = BTreeMap::<u64, UnresolvedItemName>::new();
            let mut skipped_table_ids = BTreeSet::<u64>::new();

            let mut final_row = Row::default();

            let mut new_upper = *data_cap_set[0].time();
            let mut advance_on_heartbeat = false;

            trace!(%id, "timely-{worker_id} starting replication at {new_upper:?}");
            while let Some(event) = stream.as_mut().next().await {
                use mysql_async::binlog::events::*;
                let event = event?;
                let event_data = event.read_data()?;

                match event_data {
                    Some(EventData::HeartbeatEvent) =>  {
                        // The upstream MySQL source only emits a heartbeat when there are no other events
                        // sent within the heartbeat interval. This means that we can safely advance the
                        // frontier once since we know there were no more events for the last sent GTID.
                        // See: https://dev.mysql.com/doc/refman/8.0/en/replication-administration-status.html
                        if advance_on_heartbeat {
                            data_output.give_container(&data_cap_set[0], &mut container).await;

                            new_upper = new_upper + TransactionId::new(1);
                            trace!(%id, "heartbeat: timely-{worker_id} advancing frontier to {new_upper:?}");
                            upper_cap_set.downgrade([&new_upper]);
                            data_cap_set.downgrade([&new_upper]);
                            rewinds.retain(|_, (_, req)| data_cap_set[0].time() <= &TransactionId::new(req.snapshot_gtid_set.first().expect("at least one gtid").latest_transaction_id()));
                            advance_on_heartbeat = false;
                        }
                    }
                    // We receive a GtidEvent that tells us the transaction-id of proceeding
                    // RowsEvents (and other events)
                    Some(EventData::GtidEvent(event)) => {
                        // TODO: Use this when we use a partitioned timestamp
                        let _source_id = Uuid::from_bytes(event.sid());
                        let received_upper = TransactionId::new(event.gno());
                        assert!(new_upper <= received_upper, "GTID went backwards {} -> {}", new_upper, received_upper);
                        new_upper = received_upper;

                        // Indicate that we should advance the frontier if we receive a heartbeat since that would indicate
                        // that there are no more records for this GTID.
                        advance_on_heartbeat = true;
                    }
                    // A row event is a write/update/delete event
                    Some(EventData::RowsEvent(data)) => {
                        // Find the relevant table
                        let binlog_table_id = data.table_id();
                        let table_map_event = stream.get_ref().get_tme(binlog_table_id).ok_or_else(|| {
                            TransientError::Generic(anyhow::anyhow!("Table map event not found"))
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
                                let table = table_name(&*table_map_event.database_name(), &*table_map_event.table_name())?;
                                if table_info.contains_key(&table) {
                                    table_id_map.insert(binlog_table_id, table);
                                    &table_id_map[&binlog_table_id]
                                } else {
                                    skipped_table_ids.insert(binlog_table_id);
                                    // We don't know about this table, so skip this event
                                    continue;
                                }
                            }
                            _ => unreachable!(),
                        };

                        let (output_index, table_desc) = &table_info[table];

                        // Iterate over the rows in this RowsEvent. Each row is a pair of 'before_row', 'after_row',
                        // to accomodate for updates and deletes (which include a before_row),
                        // and updates and inserts (which inclued an after row).
                        let mut rows_iter = data.rows(table_map_event);
                        while let Some(Ok((before_row, after_row))) = rows_iter.next() {
                            let updates = [before_row.map(|r| (r, -1)), after_row.map(|r| (r, 1))];
                            for (binlog_row, diff) in updates.into_iter().flatten() {
                                // TODO: Map columns from table schema to indexes in each row
                                let row = mysql_async::Row::try_from(binlog_row)?;
                                let packed_row = pack_mysql_row(&mut final_row, row, table_desc)?;
                                let data = (*output_index, packed_row);

                                // Rewind this update if it was already present in the snapshot
                                if let Some((rewind_caps, req)) = rewinds.get(table) {
                                    let latest_transaction_id = req.snapshot_gtid_set.first().expect("at least one gtid").latest_transaction_id();
                                    let [data_cap, _upper_cap] = rewind_caps;
                                    if new_upper <= TransactionId::new(latest_transaction_id) {
                                        data_output.give(data_cap, (data.clone(), TransactionId::minimum(), -diff)).await;
                                    }
                                }
                                trace!(%id, "timely-{worker_id} sending update {data:?} at {new_upper:?}");
                                container.push((data, new_upper, diff));
                            }
                        }

                        // flush any pending records and advance our frontier
                        if container.len() > max_capacity {
                            data_output.give_container(&data_cap_set[0], &mut container).await;
                            trace!(%id, "timely-{worker_id} advancing frontier to {new_upper:?}");
                            upper_cap_set.downgrade([&new_upper]);
                            data_cap_set.downgrade([&new_upper]);
                            rewinds.retain(|_, (_, req)| data_cap_set[0].time() <= &TransactionId::new(req.snapshot_gtid_set.first().expect("at least one gtid").latest_transaction_id()));
                        }
                    }
                    _ => {
                        // TODO: Handle other event types
                        // We definitely need to handle 'query' events and parse them for DDL changes
                        // that might affect the schema of the tables we care about.
                    }
                }

                // TODO: Do we need to downgrade the frontier if the stream yields? Or is that handled by the heartbeat?
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

/// Produces the logical replication stream while taking care of regularly sending standby
/// keepalive messages with the provided `uppers` stream.
///
/// The returned stream will contain all transactions that whose commit LSN is beyond `resume_lsn`.
async fn raw_stream<'a>(
    config: &RawSourceCreationConfig,
    mut conn: Conn,
    // TODO: this is only needed while we use a single source-id for the timestamp; remove once we move to a partitioned timestamp
    gtid_source_id: Uuid,
    resume_transaction_id: TransactionId,
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
    let seen_gtids = match resume_transaction_id.into() {
        0 | 1 => {
            // If we're starting from the beginning of the binlog or the first transaction id
            // we haven't seen 'anything'
            Vec::new()
        }
        ref a => {
            // NOTE: Since we enforce replica_preserve_commit_order=ON we can start the interval at 1
            // since we know that all transactions with a lower transaction id were monotonic
            vec![Sid::new(*gtid_source_id.as_bytes()).with_interval(GnoInterval::new(1, *a))]
        }
    };

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
