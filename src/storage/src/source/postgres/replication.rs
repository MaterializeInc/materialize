// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Renders the logical replication side of the [`PostgresSourceConnection`] ingestion dataflow.
//!
//! ```text
//!              o
//!              │rewind
//!              │requests
//!          ╭───┴────╮
//!          │exchange│ (collect all requests to one worker)
//!          ╰───┬────╯
//!           ┏━━v━━━━━━━━━━┓
//!           ┃ replication ┃ (single worker)
//!           ┃   reader    ┃
//!           ┗━┯━━━━━━━━┯━━┛
//!             │raw     │
//!             │data    │
//!        ╭────┴─────╮  │
//!        │distribute│  │ (distribute to all workers)
//!        ╰────┬─────╯  │
//! ┏━━━━━━━━━━━┷━┓      │
//! ┃ replication ┃      │ (parallel decode)
//! ┃   decoder   ┃      │
//! ┗━━━━━┯━━━━━━━┛      │
//!       │ replication  │ progress
//!       │ updates      │ output
//!       v              v
//! ```
//!
//! # Progress tracking
//!
//! In order to avoid causing excessive resource usage in the upstream server it's important to
//! track the LSN that we have successfully committed to persist and communicate that back to
//! PostgreSQL. Under normal operation this gauge of progress is provided by the presence of
//! transactions themselves. Since at a given LSN offset there can be only a single message, when a
//! transaction is received and processed we can infer that we have seen all the messages that are
//! not beyond `commit_lsn + 1`.
//!
//! Things are a bit more complicated in the absence of transactions though because even though we
//! don't receive any the server might very well be generating WAL records. This can happen if
//! there is a separate logical database performing writes (which is the case for RDS databases),
//! or, in servers running PostgreSQL version 15 or greater, the logical replication process
//! includes an optimization that omits empty transactions, which can happen if you're only
//! replicating a subset of the tables and there writes going to the other ones.
//!
//! If we fail to detect this situation and don't send LSN feedback in a timely manner the server
//! will be forced to keep around WAL data that can eventually lead to disk space exhaustion.
//!
//! In the absence of transactions the only available piece of information in the replication
//! stream are keepalive messages. Keepalive messages are documented[1] to contain the current end
//! of WAL on the server. That is a useless number when it comes to progress tracking because there
//! might be pending messages at LSNs between the last received commit_lsn and the current end of
//! WAL.
//!
//! Fortunately for us, the documentation for PrimaryKeepalive messages is wrong and it actually
//! contains the last *sent* LSN[2]. Here sent doesn't necessarily mean sent over the wire, but
//! sent to the upstream process that is handling producing the logical stream. Therefore, if we
//! receive a keepalive with a particular LSN we can be certain that there are no other replication
//! messages at previous LSNs, because they would have been already generated and received. We
//! therefore connect the keepalive messages directly to our capability.
//!
//! [1]: https://www.postgresql.org/docs/15/protocol-replication.html#PROTOCOL-REPLICATION-START-REPLICATION
//! [2]: https://www.postgresql.org/message-id/CAFPTHDZS9O9WG02EfayBd6oONzK%2BqfUxS6AbVLJ7W%2BKECza2gg%40mail.gmail.com

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::pin::pin;
use std::rc::Rc;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Instant;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use differential_dataflow::AsCollection;
use futures::{FutureExt, Stream as AsyncStream, StreamExt, TryStreamExt};
use mz_ore::cast::CastFrom;
use mz_ore::collections::HashSet;
use mz_ore::future::InTask;
use mz_ore::iter::IteratorExt;
use mz_postgres_util::tunnel::PostgresFlavor;
use mz_postgres_util::PostgresError;
use mz_postgres_util::{simple_query_opt, Client};
use mz_repr::{Datum, DatumVec, Diff, Row};
use mz_sql_parser::ast::{display::AstDisplay, Ident};
use mz_storage_types::dyncfgs::PG_OFFSET_KNOWN_INTERVAL;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sources::SourceTimestamp;
use mz_storage_types::sources::{MzOffset, PostgresSourceConnection};
use mz_timely_util::builder_async::{
    AsyncOutputHandle, Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder,
    PressOnDropButton,
};
use mz_timely_util::operator::StreamExt as TimelyStreamExt;
use postgres_replication::protocol::{LogicalReplicationMessage, ReplicationMessage, TupleData};
use postgres_replication::LogicalReplicationStream;
use serde::{Deserialize, Serialize};
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::operators::core::Map;
use timely::dataflow::operators::Capability;
use timely::dataflow::operators::Concat;
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use tokio::sync::watch;
use tokio_postgres::error::SqlState;
use tokio_postgres::types::PgLsn;
use tracing::{error, trace};

use crate::metrics::source::postgres::PgSourceMetrics;
use crate::source::postgres::verify_schema;
use crate::source::postgres::{DefiniteError, ReplicationError, SourceOutputInfo, TransientError};
use crate::source::probe;
use crate::source::types::{
    Probe, ProgressStatisticsUpdate, SignaledFuture, SourceMessage, StackedCollection,
};
use crate::source::RawSourceCreationConfig;

/// Postgres epoch is 2000-01-01T00:00:00Z
static PG_EPOCH: LazyLock<SystemTime> =
    LazyLock::new(|| UNIX_EPOCH + Duration::from_secs(946_684_800));

// A request to rewind a snapshot taken at `snapshot_lsn` to the initial LSN of the replication
// slot. This is accomplished by emitting `(data, 0, -diff)` for all updates `(data, lsn, diff)`
// whose `lsn <= snapshot_lsn`. By convention the snapshot is always emitted at LSN 0.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RewindRequest {
    /// The output index that should be rewound.
    pub(crate) output_index: usize,
    /// The LSN that the snapshot was taken at.
    pub(crate) snapshot_lsn: MzOffset,
}

/// Renders the replication dataflow. See the module documentation for more information.
pub(crate) fn render<G: Scope<Timestamp = MzOffset>>(
    scope: G,
    config: RawSourceCreationConfig,
    connection: PostgresSourceConnection,
    table_info: BTreeMap<u32, BTreeMap<usize, SourceOutputInfo>>,
    rewind_stream: &Stream<G, RewindRequest>,
    slot_ready_stream: &Stream<G, Infallible>,
    committed_uppers: impl futures::Stream<Item = Antichain<MzOffset>> + 'static,
    metrics: PgSourceMetrics,
) -> (
    StackedCollection<G, (usize, Result<SourceMessage, DataflowError>)>,
    Stream<G, Infallible>,
    Stream<G, ProgressStatisticsUpdate>,
    Option<Stream<G, Probe<MzOffset>>>,
    Stream<G, ReplicationError>,
    PressOnDropButton,
) {
    let op_name = format!("ReplicationReader({})", config.id);
    let mut builder = AsyncOperatorBuilder::new(op_name, scope.clone());

    let slot_reader = u64::cast_from(config.responsible_worker("slot"));
    let (data_output, data_stream) = builder.new_output();
    let (_upper_output, upper_stream) = builder.new_output::<CapacityContainerBuilder<_>>();
    let (definite_error_handle, definite_errors) = builder.new_output();

    let (stats_output, stats_stream) = builder.new_output::<CapacityContainerBuilder<_>>();
    let (probe_output, probe_stream) = builder.new_output::<CapacityContainerBuilder<_>>();

    // Yugabyte doesn't support LSN probing currently.
    let probe_stream = match connection.connection.flavor {
        PostgresFlavor::Vanilla => Some(probe_stream),
        PostgresFlavor::Yugabyte => None,
    };

    let mut rewind_input =
        builder.new_disconnected_input(rewind_stream, Exchange::new(move |_| slot_reader));
    let mut slot_ready_input = builder.new_disconnected_input(slot_ready_stream, Pipeline);
    let mut output_uppers = table_info
        .iter()
        .flat_map(|(_, outputs)| outputs.values().map(|o| o.resume_upper.clone()))
        .collect::<Vec<_>>();
    metrics.tables.set(u64::cast_from(output_uppers.len()));

    // Include the upper of the main source output for use in calculating the initial
    // resume upper.
    output_uppers.push(Antichain::from_iter(
        config
            .source_resume_uppers
            .get(&config.id)
            .expect("id exists")
            .iter()
            .map(MzOffset::decode_row),
    ));

    let reader_table_info = table_info.clone();
    let (button, transient_errors) = builder.build_fallible(move |caps| {
        let table_info = reader_table_info;
        let busy_signal = Arc::clone(&config.busy_signal);
        Box::pin(SignaledFuture::new(busy_signal, async move {
            let (id, worker_id) = (config.id, config.worker_id);
            let [data_cap_set, upper_cap_set, definite_error_cap_set, stats_cap, probe_cap]: &mut [_; 5] =
                caps.try_into().unwrap();

            if !config.responsible_for("slot") {
                // Emit 0, to mark this worker as having started up correctly.
                stats_output.give(
                    &stats_cap[0],
                    ProgressStatisticsUpdate::SteadyState {
                        offset_known: 0,
                        offset_committed: 0,
                    },
                );
                return Ok(());
            }

            // Determine the slot lsn.
            let connection_config = connection
                .connection
                .config(
                    &config.config.connection_context.secrets_reader,
                    &config.config,
                    InTask::Yes,
                )
                .await?;

            let slot = &connection.publication_details.slot;
            let replication_client = connection_config
                .connect_replication(&config.config.connection_context.ssh_tunnel_manager)
                .await?;

            let metadata_client = connection_config
                .connect(
                    "replication metadata",
                    &config.config.connection_context.ssh_tunnel_manager,
                )
                .await?;
            let metadata_client = Arc::new(metadata_client);

            while let Some(_) = slot_ready_input.next().await {
                // Wait for the slot to be created
            }
            tracing::info!(%id, "ensuring replication slot {slot} exists");
            super::ensure_replication_slot(&replication_client, slot).await?;
            let slot_metadata = super::fetch_slot_metadata(
                &*metadata_client,
                slot,
                mz_storage_types::dyncfgs::PG_FETCH_SLOT_RESUME_LSN_INTERVAL
                    .get(config.config.config_set()),
            )
            .await?;

            // We're the only application that should be using this replication
            // slot. The only way that there can be another connection using
            // this slot under normal operation is if there's a stale TCP
            // connection from a prior incarnation of the source holding on to
            // the slot. We don't want to wait for the WAL sender timeout and/or
            // TCP keepalives to time out that connection, because these values
            // are generally under the control of the DBA and may not time out
            // the connection for multiple minutes, or at all. Instead we just
            // force kill the connection that's using the slot.
            //
            // Note that there's a small risk that *we're* the zombie cluster
            // that should not be using the replication slot. Kubernetes cannot
            // 100% guarantee that only one cluster is alive at a time. However,
            // this situation should not last long, and the worst that can
            // happen is a bit of transient thrashing over ownership of the
            // replication slot.
            if let Some(active_pid) = slot_metadata.active_pid {
                tracing::warn!(
                    %id, %active_pid,
                    "replication slot already in use; will attempt to kill existing connection",
                );

                match metadata_client
                    .execute("SELECT pg_terminate_backend($1)", &[&active_pid])
                    .await
                {
                    Ok(_) => {
                        tracing::info!(
                            "successfully killed existing connection; \
                            starting replication is likely to succeed"
                        );
                        // Note that `pg_terminate_backend` does not wait for
                        // the termination of the targeted connection to
                        // complete. We may try to start replication before the
                        // targeted connection has cleaned up its state. That's
                        // okay. If that happens we'll just try again from the
                        // top via the suspend-and-restart flow.
                    }
                    Err(e) => {
                        tracing::warn!(
                            %e,
                            "failed to kill existing replication connection; \
                            replication will likely fail to start"
                        );
                        // Continue on anyway, just in case the replication slot
                        // is actually available. Maybe PostgreSQL has some
                        // staleness when it reports `active_pid`, for example.
                    }
                }
            }

            // The overall resumption point for this source is the minimum of the resumption points
            // contributed by each of the outputs.
            let resume_lsn = output_uppers.iter().flat_map(|f| f.elements()).map(|&lsn| {
                // An output is either an output that has never had data committed to it or one
                // that has and needs to resume. We differentiate between the two by checking
                // whether an output wishes to "resume" from the minimum timestamp. In that case
                // its contribution to the overal resumption point is the earliest point available
                // in the slot. This information would normally be something that the storage
                // controller figures out in the form of an as-of frontier, but at the moment the
                // storage controller does not have visibility into what the replication slot is
                // doing.
                if lsn == MzOffset::from(0) {
                    slot_metadata.confirmed_flush_lsn
                } else {
                    lsn
                }
            }).min();
            let Some(resume_lsn) = resume_lsn else {
                return Ok(())
            };
            upper_cap_set.downgrade([&resume_lsn]);
            trace!(%id, "timely-{worker_id} replication reader started lsn={resume_lsn}");

            let mut rewinds = BTreeMap::new();
            while let Some(event) = rewind_input.next().await {
                if let AsyncEvent::Data(_, data) = event {
                    for req in data {
                        if resume_lsn > req.snapshot_lsn + 1 {
                            let err = DefiniteError::SlotCompactedPastResumePoint(
                                req.snapshot_lsn + 1,
                                resume_lsn,
                            );
                            // If the replication stream cannot be obtained from the resume point there is nothing
                            // else to do. These errors are not retractable.
                            for (oid, outputs) in table_info.iter() {
                                for output_index in outputs.keys() {
                                    // We pick `u64::MAX` as the LSN which will (in practice) never conflict
                                    // any previously revealed portions of the TVC.
                                    let update = (
                                        (
                                            *oid,
                                            *output_index,
                                            Err(DataflowError::from(err.clone())),
                                        ),
                                        MzOffset::from(u64::MAX),
                                        1,
                                    );
                                    data_output.give_fueled(&data_cap_set[0], update).await;
                                }
                            }
                            definite_error_handle.give(
                                &definite_error_cap_set[0],
                                ReplicationError::Definite(Rc::new(err)),
                            );
                            return Ok(());
                        }
                        rewinds.insert(req.output_index, req);
                    }
                }
            }
            trace!(%id, "timely-{worker_id} pending rewinds {rewinds:?}");

            let mut committed_uppers = pin!(committed_uppers);

            let stream_result = raw_stream(
                &config,
                replication_client,
                Arc::clone(&metadata_client),
                &connection.publication_details.slot,
                &connection.publication_details.timeline_id,
                &connection.publication,
                resume_lsn,
                committed_uppers.as_mut(),
                &stats_output,
                &stats_cap[0],
                &probe_output,
                &probe_cap[0],
            )
            .await?;

            let stream = match stream_result {
                Ok(stream) => stream,
                Err(err) => {
                    // If the replication stream cannot be obtained in a definite way there is
                    // nothing else to do. These errors are not retractable.
                    for (oid, outputs) in table_info.iter() {
                        for output_index in outputs.keys() {
                            // We pick `u64::MAX` as the LSN which will (in practice) never conflict
                            // any previously revealed portions of the TVC.
                            let update = (
                                (*oid, *output_index, Err(DataflowError::from(err.clone()))),
                                MzOffset::from(u64::MAX),
                                1,
                            );
                            data_output.give_fueled(&data_cap_set[0], update).await;
                        }
                    }

                    definite_error_handle.give(
                        &definite_error_cap_set[0],
                        ReplicationError::Definite(Rc::new(err)),
                    );
                    return Ok(());
                }
            };

            let mut stream = pin!(stream.peekable());

            let mut errored = HashSet::new();
            // Instead of downgrading the capability for every transaction we process we only do it
            // if we're about to yield, which is checked at the bottom of the loop. This avoids
            // creating excessive progress tracking traffic when there are multiple small
            // transactions ready to go.
            let mut data_upper = resume_lsn;
            // A stash of reusable vectors to convert from bytes::Bytes based data, which is not
            // compatible with `columnation`, to Vec<u8> data that is.
            let mut col_temp: Vec<Vec<u8>> = vec![];
            let mut row_temp = vec![];
            let mut last_schema_validation = Instant::now();
            while let Some(event) = stream.as_mut().next().await {
                use LogicalReplicationMessage::*;
                use ReplicationMessage::*;
                match event {
                    Ok(XLogData(data)) => match data.data() {
                        Begin(begin) => {
                            let commit_lsn = MzOffset::from(begin.final_lsn());

                            let mut tx = pin!(extract_transaction(
                                stream.by_ref(),
                                &*metadata_client,
                                commit_lsn,
                                &table_info,
                                &metrics,
                                &connection.publication,
                                &mut errored
                            ));

                            trace!(
                                %id,
                                "timely-{worker_id} extracting transaction \
                                    at {commit_lsn}"
                            );
                            assert!(
                                data_upper <= commit_lsn,
                                "new_upper={data_upper} tx_lsn={commit_lsn}",
                            );
                            data_upper = commit_lsn + 1;
                            // We are about to ingest a transaction which has the possiblity to be
                            // very big and we certainly don't want to hold the data in memory. For
                            // this reason we eagerly downgrade the upper capability in order for
                            // the reclocking machinery to mint a binding that includes
                            // this transaction and therefore be able to pass the data of the
                            // transaction through as we stream it.
                            upper_cap_set.downgrade([&data_upper]);
                            while let Some((oid, output_index, event, diff)) = tx.try_next().await?
                            {
                                let event = match event {
                                    Ok(cols) => {
                                        row_temp.clear();
                                        for c in cols {
                                            let c = c.map(|c| {
                                                let mut col_vec =
                                                    col_temp.pop().unwrap_or_default();
                                                col_vec.clear();
                                                col_vec.extend_from_slice(&c);
                                                col_vec
                                            });
                                            row_temp.push(c);
                                        }
                                        Ok(std::mem::take(&mut row_temp))
                                    }
                                    Err(err) => Err(err.into()),
                                };
                                let mut data = (oid, output_index, event);
                                if let Some(req) = rewinds.get(&output_index) {
                                    if commit_lsn <= req.snapshot_lsn {
                                        let update = (data, MzOffset::from(0), -diff);
                                        data_output.give_fueled(&data_cap_set[0], &update).await;
                                        data = update.0;
                                    }
                                }
                                let update = (data, commit_lsn, diff);
                                data_output.give_fueled(&data_cap_set[0], &update).await;
                                // Store buffers for reuse
                                if let Ok(mut row) = update.0 .2 {
                                    col_temp.extend(row.drain(..).flatten());
                                    row_temp = row;
                                }
                            }
                        }
                        _ => return Err(TransientError::BareTransactionEvent),
                    },
                    Ok(PrimaryKeepAlive(keepalive)) => {
                        trace!( %id,
                            "timely-{worker_id} received keepalive lsn={}",
                            keepalive.wal_end()
                        );
                        let validation_interval = mz_storage_types::dyncfgs::PG_SCHEMA_VALIDATION_INTERVAL.get(config.config.config_set());
                        if last_schema_validation.elapsed() > validation_interval {
                            trace!(%id, "timely-{worker_id} validating schemas");
                            let upstream_info = {
                                match mz_postgres_util::publication_info(&*metadata_client, &connection.publication)
                                    .await
                                {
                                    Ok(info) => info.into_iter().map(|t| (t.oid, t)).collect(),
                                    // If the replication stream cannot be obtained in a definite way there is
                                    // nothing else to do. These errors are not retractable.
                                    Err(PostgresError::PublicationMissing(publication)) => {
                                        let err = DefiniteError::PublicationDropped(publication);
                                        for (oid, outputs) in table_info.iter() {
                                            for output_index in outputs.keys() {
                                                let update = (
                                                    (
                                                        *oid,
                                                        *output_index,
                                                        Err(DataflowError::from(err.clone())),
                                                    ),
                                                    data_cap_set[0].time().clone(),
                                                    1,
                                                );
                                                data_output.give_fueled(&data_cap_set[0], update).await;
                                            }
                                        }
                                        definite_error_handle.give(
                                            &definite_error_cap_set[0],
                                            ReplicationError::Definite(Rc::new(err)),
                                        );
                                        return Ok(());
                                    }
                                    Err(e) => Err(TransientError::from(e))?,
                                }
                            };
                            for (&oid, outputs) in table_info.iter() {
                                for (output_index, info) in outputs {
                                    if errored.contains(output_index) {
                                        trace!(%id, "timely-{worker_id} output index {output_index} \
                                            for oid {oid} skipped");
                                        continue;
                                    }
                                    match verify_schema(oid, &info.desc, &upstream_info, &*info.casts) {
                                        Ok(()) => {
                                            trace!(%id, "timely-{worker_id} schema of output \
                                                index {output_index} for oid {oid} valid");
                                        }
                                        Err(err) => {
                                            trace!(%id, "timely-{worker_id} schema of output \
                                                index {output_index} for oid {oid} invalid");
                                            let update = (
                                                (oid, *output_index, Err(err.into())),
                                                data_cap_set[0].time().clone(),
                                                1,
                                            );
                                            data_output.give_fueled(&data_cap_set[0], update).await;
                                            errored.insert(*output_index);
                                        }
                                    }
                                }
                            }
                            last_schema_validation = Instant::now();
                        }
                        data_upper = std::cmp::max(data_upper, keepalive.wal_end().into());
                    }
                    Ok(_) => return Err(TransientError::UnknownReplicationMessage),
                    Err(err) => return Err(err),
                }

                let will_yield = stream.as_mut().peek().now_or_never().is_none();
                if will_yield {
                    trace!(%id, "timely-{worker_id} yielding at lsn={data_upper}");
                    rewinds.retain(|_, req| data_upper <= req.snapshot_lsn);
                    // As long as there are pending rewinds we can't downgrade our data capability
                    // since we must be able to produce data at offset 0.
                    if rewinds.is_empty() {
                        data_cap_set.downgrade([&data_upper]);
                    }
                    upper_cap_set.downgrade([&data_upper]);
                }
            }
            // We never expect the replication stream to gracefully end
            Err(TransientError::ReplicationEOF)
        }))
    });

    // Distribute the raw slot data to all workers.
    let data_stream = data_stream.distribute();

    // We now process the slot updates and apply the cast expressions
    let mut final_row = Row::default();
    let mut datum_vec = DatumVec::new();
    let replication_updates = data_stream
        .map(move |((oid, output_index, event), time, diff)| {
            let output = &table_info
                .get(oid)
                .and_then(|outputs| outputs.get(output_index))
                .expect("table_info contains all outputs");
            let event = event.as_ref().map_err(|e| e.clone()).and_then(|row| {
                let mut datums = datum_vec.borrow();
                for col in row.iter() {
                    let datum = col.as_deref().map(super::decode_utf8_text).transpose()?;
                    datums.push(datum.unwrap_or(Datum::Null));
                }
                super::cast_row(&output.casts, &datums, &mut final_row)?;
                Ok(SourceMessage {
                    key: Row::default(),
                    value: final_row.clone(),
                    metadata: Row::default(),
                })
            });

            ((*output_index, event), *time, *diff)
        })
        .as_collection();

    let errors = definite_errors.concat(&transient_errors.map(ReplicationError::from));

    (
        replication_updates,
        upper_stream,
        stats_stream,
        probe_stream,
        errors,
        button.press_on_drop(),
    )
}

/// Produces the logical replication stream while taking care of regularly sending standby
/// keepalive messages with the provided `uppers` stream.
///
/// The returned stream will contain all transactions that whose commit LSN is beyond `resume_lsn`.
async fn raw_stream<'a>(
    config: &'a RawSourceCreationConfig,
    replication_client: Client,
    metadata_client: Arc<Client>,
    slot: &'a str,
    timeline_id: &'a Option<u64>,
    publication: &'a str,
    resume_lsn: MzOffset,
    uppers: impl futures::Stream<Item = Antichain<MzOffset>> + 'a,
    stats_output: &'a AsyncOutputHandle<
        MzOffset,
        CapacityContainerBuilder<Vec<ProgressStatisticsUpdate>>,
        Tee<MzOffset, Vec<ProgressStatisticsUpdate>>,
    >,
    stats_cap: &'a Capability<MzOffset>,
    probe_output: &'a AsyncOutputHandle<
        MzOffset,
        CapacityContainerBuilder<Vec<Probe<MzOffset>>>,
        Tee<MzOffset, Vec<Probe<MzOffset>>>,
    >,
    probe_cap: &'a Capability<MzOffset>,
) -> Result<
    Result<
        impl AsyncStream<
                Item = Result<ReplicationMessage<LogicalReplicationMessage>, TransientError>,
            > + 'a,
        DefiniteError,
    >,
    TransientError,
> {
    if let Err(err) = ensure_publication_exists(&*metadata_client, publication).await? {
        // If the publication gets deleted there is nothing else to do. These errors
        // are not retractable.
        return Ok(Err(err));
    }

    // Skip the timeline ID check for sources without a known timeline ID
    // (sources created before the timeline ID was added to the source details)
    if let Some(expected_timeline_id) = timeline_id {
        if let Err(err) =
            ensure_replication_timeline_id(&replication_client, expected_timeline_id).await?
        {
            return Ok(Err(err));
        }
    }

    // How often a proactive standby status update message should be sent to the server.
    //
    // The upstream will periodically request status updates by setting the keepalive's reply field
    // value to 1. However, we cannot rely on these messages arriving on time. For example, when
    // the upstream is sending a big transaction its keepalive messages are queued and can be
    // delayed arbitrarily.
    //
    // See: <https://www.postgresql.org/message-id/CAMsr+YE2dSfHVr7iEv1GSPZihitWX-PMkD9QALEGcTYa+sdsgg@mail.gmail.com>
    //
    // For this reason we query the server's timeout value and proactively send a keepalive at
    // twice the frequency to have a healthy margin from the deadline.
    //
    // Note: We must use the metadata client here which is NOT in replication mode. Some Aurora
    // Postgres versions disallow SHOW commands from within replication connection.
    // See: https://github.com/readysettech/readyset/discussions/28#discussioncomment-4405671
    let row = simple_query_opt(&*metadata_client, "SHOW wal_sender_timeout;")
        .await?
        .unwrap();
    let wal_sender_timeout = match row.get("wal_sender_timeout") {
        // When this parameter is zero the timeout mechanism is disabled
        Some("0") => None,
        Some(value) => Some(
            mz_repr::adt::interval::Interval::from_str(value)
                .unwrap()
                .duration()
                .unwrap(),
        ),
        None => panic!("ubiquitous parameter missing"),
    };

    // This interval controls the cadence at which we send back status updates and, crucially,
    // request PrimaryKeepAlive messages. PrimaryKeepAlive messages drive the frontier forward in
    // the absence of data updates and we don't want a large `wal_sender_timeout` value to slow us
    // down. For this reason the feedback interval is set to one second, or less if the
    // wal_sender_timeout is less than 2 seconds.
    let feedback_interval = match wal_sender_timeout {
        Some(t) => std::cmp::min(Duration::from_secs(1), t.checked_div(2).unwrap()),
        None => Duration::from_secs(1),
    };

    let mut feedback_timer = tokio::time::interval(feedback_interval);
    // 'Delay' ensures we always tick at least 'feedback_interval'.
    feedback_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    // Postgres will return all transactions that commit *at or after* after the provided LSN,
    // following the timely upper semantics.
    let lsn = PgLsn::from(resume_lsn.offset);
    let query = format!(
        r#"START_REPLICATION SLOT "{}" LOGICAL {} ("proto_version" '1', "publication_names" '{}')"#,
        Ident::new_unchecked(slot).to_ast_string(),
        lsn,
        publication,
    );
    let copy_stream = match replication_client.copy_both_simple(&query).await {
        Ok(copy_stream) => copy_stream,
        Err(err) if err.code() == Some(&SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE) => {
            return Ok(Err(DefiniteError::InvalidReplicationSlot));
        }
        Err(err) => return Err(err.into()),
    };

    // According to the documentation [1] we must check that the slot LSN matches our
    // expectations otherwise we risk getting silently fast-forwarded to a future LSN. In order
    // to avoid a TOCTOU issue we must do this check after starting the replication stream. We
    // cannot use the replication client to do that because it's already in CopyBoth mode.
    // [1] https://www.postgresql.org/docs/15/protocol-replication.html#PROTOCOL-REPLICATION-START-REPLICATION-SLOT-LOGICAL
    let slot_metadata = super::fetch_slot_metadata(
        &*metadata_client,
        slot,
        mz_storage_types::dyncfgs::PG_FETCH_SLOT_RESUME_LSN_INTERVAL
            .get(config.config.config_set()),
    )
    .await?;
    let min_resume_lsn = slot_metadata.confirmed_flush_lsn;
    tracing::info!(
        %config.id,
        "started replication using backend PID={:?}. wal_sender_timeout={:?}",
        slot_metadata.active_pid, wal_sender_timeout
    );

    let (probe_tx, mut probe_rx) = watch::channel(None);
    let config_set = Arc::clone(config.config.config_set());
    let now_fn = config.now_fn.clone();
    let max_lsn_task_handle =
        mz_ore::task::spawn(|| format!("pg_current_wal_lsn:{}", config.id), async move {
            let mut probe_ticker =
                probe::Ticker::new(|| PG_OFFSET_KNOWN_INTERVAL.get(&config_set), now_fn);

            while !probe_tx.is_closed() {
                let probe_ts = probe_ticker.tick().await;
                let probe_or_err = super::fetch_max_lsn(&*metadata_client)
                    .await
                    .map(|lsn| Probe {
                        probe_ts,
                        upstream_frontier: Antichain::from_elem(lsn),
                    });
                let _ = probe_tx.send(Some(probe_or_err));
            }
        })
        .abort_on_drop();

    let stream = async_stream::try_stream!({
        // Ensure we don't pre-drop the task
        let _max_lsn_task_handle = max_lsn_task_handle;

        let mut uppers = pin!(uppers);
        let mut last_committed_upper = resume_lsn;

        let mut stream = pin!(LogicalReplicationStream::new(copy_stream));

        if !(resume_lsn == MzOffset::from(0) || min_resume_lsn <= resume_lsn) {
            let err = TransientError::OvercompactedReplicationSlot {
                available_lsn: min_resume_lsn,
                requested_lsn: resume_lsn,
            };
            error!("timely-{} ({}) {err}", config.worker_id, config.id);
            Err(err)?;
        }

        loop {
            tokio::select! {
                Some(next_message) = stream.next() => match next_message {
                    Ok(ReplicationMessage::XLogData(data)) => {
                        yield ReplicationMessage::XLogData(data);
                        Ok(())
                    }
                    Ok(ReplicationMessage::PrimaryKeepAlive(keepalive)) => {
                        yield ReplicationMessage::PrimaryKeepAlive(keepalive);
                        Ok(())
                    }
                    Err(err) => Err(err.into()),
                    _ => Err(TransientError::UnknownReplicationMessage),
                },
                _ = feedback_timer.tick() => {
                    let ts: i64 = PG_EPOCH.elapsed().unwrap().as_micros().try_into().unwrap();
                    let lsn = PgLsn::from(last_committed_upper.offset);
                    trace!("timely-{} ({}) sending keepalive {lsn:?}", config.worker_id, config.id);
                    // Postgres only sends PrimaryKeepAlive messages when *it* wants a reply, which
                    // happens when out status update is late. Since we send them proactively this
                    // may never happen. It is therefore *crucial* that we set the last parameter
                    // (the reply flag) to 1 here. This will cause the upstream server to send us a
                    // PrimaryKeepAlive message promptly which will give us frontier advancement
                    // information in the absence of data updates.
                    let res = stream.as_mut().standby_status_update(lsn, lsn, lsn, ts, 1).await;
                    res.map_err(|e| e.into())
                },
                Some(upper) = uppers.next() => match upper.into_option() {
                    Some(lsn) => {
                        last_committed_upper = std::cmp::max(last_committed_upper, lsn);
                        Ok(())
                    }
                    None => Ok(()),
                },
                Ok(()) = probe_rx.changed() => match &*probe_rx.borrow() {
                    Some(Ok(probe)) => {
                        if let Some(offset_known) = probe.upstream_frontier.as_option() {
                            stats_output.give(
                                stats_cap,
                                ProgressStatisticsUpdate::SteadyState {
                                    // Similar to the kafka source, we don't subtract 1 from the
                                    // upper as we want to report the _number of bytes_ we have
                                    // processed/in upstream.
                                    offset_known: offset_known.offset,
                                    offset_committed: last_committed_upper.offset,
                                },
                            );
                        }
                        probe_output.give(probe_cap, probe.clone());
                        Ok(())
                    },
                    Some(Err(err)) => Err(anyhow::anyhow!("{err}").into()),
                    None => Ok(()),
                },
                else => return
            }?;
        }
    });
    Ok(Ok(stream))
}

/// Extracts a single transaction from the replication stream delimited by a BEGIN and COMMIT
/// message. The BEGIN message must have already been consumed from the stream before calling this
/// function.
fn extract_transaction<'a>(
    stream: impl AsyncStream<Item = Result<ReplicationMessage<LogicalReplicationMessage>, TransientError>>
        + 'a,
    metadata_client: &'a Client,
    commit_lsn: MzOffset,
    table_info: &'a BTreeMap<u32, BTreeMap<usize, SourceOutputInfo>>,
    metrics: &'a PgSourceMetrics,
    publication: &'a str,
    errored_outputs: &'a mut HashSet<usize>,
) -> impl AsyncStream<
    Item = Result<(u32, usize, Result<Vec<Option<Bytes>>, DefiniteError>, Diff), TransientError>,
> + 'a {
    use LogicalReplicationMessage::*;
    async_stream::try_stream!({
        let mut stream = pin!(stream);
        metrics.transactions.inc();
        metrics.lsn.set(commit_lsn.offset);
        while let Some(event) = stream.try_next().await? {
            // We can ignore keepalive messages while processing a transaction because the
            // commit_lsn will drive progress.
            let message = match event {
                ReplicationMessage::XLogData(data) => data.into_data(),
                ReplicationMessage::PrimaryKeepAlive(_) => continue,
                _ => Err(TransientError::UnknownReplicationMessage)?,
            };
            metrics.total.inc();
            match message {
                Insert(body) if !table_info.contains_key(&body.rel_id()) => continue,
                Update(body) if !table_info.contains_key(&body.rel_id()) => continue,
                Delete(body) if !table_info.contains_key(&body.rel_id()) => continue,
                Relation(body) if !table_info.contains_key(&body.rel_id()) => continue,
                Insert(body) => {
                    metrics.inserts.inc();
                    let row = unpack_tuple(body.tuple().tuple_data());
                    let rel = body.rel_id();
                    for ((output, _), row) in table_info
                        .get(&rel)
                        .map(|o| o.iter().filter(|(o, _)| !errored_outputs.contains(o)))
                        .into_iter()
                        .flatten()
                        .repeat_clone(row)
                    {
                        yield (rel, *output, row, 1);
                    }
                }
                Update(body) => match body.old_tuple() {
                    Some(old_tuple) => {
                        metrics.updates.inc();
                        // If the new tuple contains unchanged toast values we reference the old ones
                        let new_tuple =
                            std::iter::zip(body.new_tuple().tuple_data(), old_tuple.tuple_data())
                                .map(|(new, old)| match new {
                                    TupleData::UnchangedToast => old,
                                    _ => new,
                                });
                        let old_row = unpack_tuple(old_tuple.tuple_data());
                        let new_row = unpack_tuple(new_tuple);
                        let rel = body.rel_id();
                        for ((output, _), (old_row, new_row)) in table_info
                            .get(&rel)
                            .map(|o| o.iter().filter(|(o, _)| !errored_outputs.contains(o)))
                            .into_iter()
                            .flatten()
                            .repeat_clone((old_row, new_row))
                        {
                            yield (rel, *output, old_row, -1);
                            yield (rel, *output, new_row, 1);
                        }
                    }
                    None => {
                        let rel = body.rel_id();
                        for (output, _) in table_info
                            .get(&rel)
                            .map(|o| o.iter().filter(|(o, _)| !errored_outputs.contains(o)))
                            .into_iter()
                            .flatten()
                        {
                            yield (rel, *output, Err(DefiniteError::DefaultReplicaIdentity), 1);
                        }
                    }
                },
                Delete(body) => match body.old_tuple() {
                    Some(old_tuple) => {
                        metrics.deletes.inc();
                        let row = unpack_tuple(old_tuple.tuple_data());
                        let rel = body.rel_id();
                        for ((output, _), row) in table_info
                            .get(&rel)
                            .map(|o| o.iter().filter(|(o, _)| !errored_outputs.contains(o)))
                            .into_iter()
                            .flatten()
                            .repeat_clone(row)
                        {
                            yield (rel, *output, row, -1);
                        }
                    }
                    None => {
                        let rel = body.rel_id();
                        for (output, _) in table_info
                            .get(&rel)
                            .map(|o| o.iter().filter(|(o, _)| !errored_outputs.contains(o)))
                            .into_iter()
                            .flatten()
                        {
                            yield (rel, *output, Err(DefiniteError::DefaultReplicaIdentity), 1);
                        }
                    }
                },
                Relation(body) => {
                    let rel_id = body.rel_id();
                    let valid_outputs = table_info
                        .get(&rel_id)
                        .map(|o| o.iter().filter(|(o, _)| !errored_outputs.contains(o)))
                        .into_iter()
                        .flatten()
                        .collect::<Vec<_>>();
                    if valid_outputs.len() > 0 {
                        // Because the replication stream doesn't include columns' attnums, we need
                        // to check the current local schema against the current remote schema to
                        // ensure e.g. we haven't received a schema update with the same terminal
                        // column name which is actually a different column.
                        let upstream_info =
                            mz_postgres_util::publication_info(metadata_client, publication)
                                .await?;
                        let upstream_info = upstream_info.into_iter().map(|t| (t.oid, t)).collect();

                        for (output_index, output) in valid_outputs {
                            if let Err(err) =
                                verify_schema(rel_id, &output.desc, &upstream_info, &output.casts)
                            {
                                errored_outputs.insert(*output_index);
                                yield (rel_id, *output_index, Err(err), 1);
                            }

                            // Error any dropped tables.
                            for (oid, outputs) in table_info {
                                if !upstream_info.contains_key(oid) {
                                    for output in outputs.keys() {
                                        if errored_outputs.insert(*output) {
                                            // Minimize the number of excessive errors
                                            // this will generate.
                                            yield (
                                                *oid,
                                                *output,
                                                Err(DefiniteError::TableDropped),
                                                1,
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Truncate(body) => {
                    for &rel_id in body.rel_ids() {
                        if let Some(outputs) = table_info.get(&rel_id) {
                            for (output, _) in outputs {
                                if errored_outputs.insert(*output) {
                                    yield (rel_id, *output, Err(DefiniteError::TableTruncated), 1);
                                }
                            }
                        }
                    }
                }
                Commit(body) => {
                    if commit_lsn != body.commit_lsn().into() {
                        Err(TransientError::InvalidTransaction)?
                    }
                    return;
                }
                // TODO: We should handle origin messages and emit an error as they indicate that
                // the upstream performed a point in time restore so all bets are off about the
                // continuity of the stream.
                Origin(_) | Type(_) => metrics.ignored.inc(),
                Begin(_) => Err(TransientError::NestedTransaction)?,
                // The enum is marked as non_exhaustive. Better to be conservative
                _ => Err(TransientError::UnknownLogicalReplicationMessage)?,
            }
        }
        Err(TransientError::ReplicationEOF)?;
    })
}

/// Unpacks an iterator of TupleData into a list of nullable bytes or an error if this can't be
/// done.
fn unpack_tuple<'a, I>(tuple_data: I) -> Result<Vec<Option<Bytes>>, DefiniteError>
where
    I: IntoIterator<Item = &'a TupleData>,
    I::IntoIter: ExactSizeIterator,
{
    let iter = tuple_data.into_iter();
    let mut row = Vec::with_capacity(iter.len());
    for data in iter {
        let datum = match data {
            TupleData::Text(bytes) => Some(bytes.clone()),
            TupleData::Null => None,
            TupleData::UnchangedToast => return Err(DefiniteError::MissingToast),
            TupleData::Binary(_) => return Err(DefiniteError::UnexpectedBinaryData),
        };
        row.push(datum);
    }
    Ok(row)
}

/// Ensures the publication exists on the server. It returns an outer transient error in case of
/// connection issues and an inner definite error if the publication is dropped.
async fn ensure_publication_exists(
    client: &Client,
    publication: &str,
) -> Result<Result<(), DefiniteError>, TransientError> {
    // Figure out the last written LSN and then add one to convert it into an upper.
    let result = client
        .query_opt(
            "SELECT 1 FROM pg_publication WHERE pubname = $1;",
            &[&publication],
        )
        .await?;
    match result {
        Some(_) => Ok(Ok(())),
        None => Ok(Err(DefiniteError::PublicationDropped(
            publication.to_owned(),
        ))),
    }
}

/// Ensure the active replication timeline_id matches the one we expect such that we can safely
/// resume replication. It returns an outer transient error in case of
/// connection issues and an inner definite error if the timeline id does not match.
async fn ensure_replication_timeline_id(
    replication_client: &Client,
    expected_timeline_id: &u64,
) -> Result<Result<(), DefiniteError>, TransientError> {
    let timeline_id = mz_postgres_util::get_timeline_id(replication_client).await?;
    if timeline_id == *expected_timeline_id {
        Ok(Ok(()))
    } else {
        Ok(Err(DefiniteError::InvalidTimelineId {
            expected: *expected_timeline_id,
            actual: timeline_id,
        }))
    }
}
