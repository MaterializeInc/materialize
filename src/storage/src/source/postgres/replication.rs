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

use std::cell::Cell;
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::pin::pin;
use std::rc::Rc;
use std::sync::Arc;

use differential_dataflow::AsCollection;
use futures::{FutureExt, StreamExt, TryStreamExt};
use mz_ore::cast::CastFrom;
use mz_ore::future::InTask;
use mz_postgres_util::PostgresError;
use mz_postgres_util::{execute, sql};
use mz_repr::{DatumVec, Diff, GlobalId, Row};
use mz_storage_types::dyncfgs::STORAGE_PERSIST_SINK_DESCRIPTION_LOOKAHEAD;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sources::SourceTimestamp;
use mz_storage_types::sources::{MzOffset, PostgresSourceConnection};
use mz_timely_util::builder_async::{
    Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};
use postgres_replication::protocol::{LogicalReplicationMessage, ReplicationMessage};
use serde::{Deserialize, Serialize};
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::Concat;
use timely::dataflow::operators::Operator;
use timely::dataflow::operators::core::Map;
use timely::dataflow::{Scope, StreamVec};
use timely::progress::Antichain;
use tracing::trace;

use crate::metrics::source::postgres::PgSourceMetrics;
use crate::source::RawSourceCreationConfig;
use crate::source::postgres::{DefiniteError, ReplicationError, SourceOutputInfo, TransientError};
use crate::source::types::{
    Probe, ResumeUppers, SharedFuel, SignaledFuture, SourceMessage, StackedCollection,
};

mod decode;
mod schema;
mod stream;

pub(super) use stream::ensure_replication_prerequisites;

use decode::extract_transaction;
use schema::{SchemaValidationError, spawn_schema_validator};
use stream::raw_stream;

/// A logical replication message from the server.
type LogicalReplMsg = ReplicationMessage<LogicalReplicationMessage>;

// A request to rewind a snapshot taken at `snapshot_lsn` to the initial LSN of the replication
// slot. This is accomplished by emitting `(data, 0, -diff)` for all updates `(data, lsn, diff)`
// whose `lsn <= snapshot_lsn` and that the output does not ignore (see
// `SourceOutputInfo::ignores`). By convention the snapshot is always emitted at LSN 0.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RewindRequest {
    /// The output index that should be rewound.
    pub(crate) output_index: usize,
    /// The LSN that the snapshot was taken at.
    pub(crate) snapshot_lsn: MzOffset,
}

/// Renders the replication dataflow. See the module documentation for more information.
pub(crate) fn render<'scope>(
    scope: Scope<'scope, MzOffset>,
    config: RawSourceCreationConfig,
    connection: PostgresSourceConnection,
    table_info: BTreeMap<u32, BTreeMap<usize, SourceOutputInfo>>,
    rewind_stream: StreamVec<'scope, MzOffset, RewindRequest>,
    slot_ready_stream: StreamVec<'scope, MzOffset, Infallible>,
    committed_uppers: impl futures::Stream<Item = ResumeUppers<MzOffset>> + 'static,
    metrics: PgSourceMetrics,
) -> (
    Vec<StackedCollection<'scope, MzOffset, Result<SourceMessage, DataflowError>>>,
    StreamVec<'scope, MzOffset, Probe<MzOffset>>,
    StreamVec<'scope, MzOffset, ReplicationError>,
    PressOnDropButton,
) {
    let op_name = format!("ReplicationReader({})", config.id);
    let mut builder = AsyncOperatorBuilder::new(op_name, scope.clone());

    // Read at render time, matching the value the persist sink reads in the same dataflow build.
    // A read in the async body can observe an `UpdateConfiguration` sequenced after this dataflow's
    // creation and disagree with the sink because the `ConfigSet` is updated in place.
    // A nonzero lookahead is the switch, see `STORAGE_PERSIST_SINK_DESCRIPTION_LOOKAHEAD`.
    let concurrent_replication = !STORAGE_PERSIST_SINK_DESCRIPTION_LOOKAHEAD
        .get(config.config.config_set())
        .is_zero();

    let slot_reader = u64::cast_from(config.responsible_worker("slot"));
    // One data output port per source export, in output index order. With concurrent
    // replication enabled a port holds the minimum capability only while its export has a
    // pending rewind and advances with the stream otherwise. When disabled all ports are
    // managed in lockstep and hold the minimum until every rewind resolves, so every export
    // observes the same frontier.
    let export_count = config.source_exports.len();
    let mut data_outputs = Vec::with_capacity(export_count);
    let mut data_streams = Vec::with_capacity(export_count);
    for _ in 0..export_count {
        let (output, stream) = builder.new_output::<CapacityContainerBuilder<Vec<_>>>();
        data_outputs.push(output);
        data_streams.push(stream);
    }
    let (definite_error_handle, definite_errors) =
        builder.new_output::<CapacityContainerBuilder<_>>();
    let (probe_output, probe_stream) = builder.new_output::<CapacityContainerBuilder<_>>();

    let mut rewind_input =
        builder.new_disconnected_input(rewind_stream, Exchange::new(move |_| slot_reader));
    let mut slot_ready_input = builder.new_disconnected_input(slot_ready_stream, Pipeline);
    let output_uppers = table_info
        .iter()
        .flat_map(|(_, outputs)| outputs.values().map(|o| o.resume_upper.clone()))
        .collect::<Vec<_>>();
    metrics.tables.set(u64::cast_from(output_uppers.len()));
    // Same values as output_uppers above (output_uppers is derived from source_resume_uppers), but it contains an additional
    // entry for the top-level source when legacy syntax is used.
    let resume_lsns: BTreeMap<GlobalId, MzOffset> = config
        .source_resume_uppers
        .iter()
        .filter_map(|(id, upper)| {
            let upper = Antichain::from_iter(upper.iter().map(MzOffset::decode_row));
            Some((*id, upper.into_option()?))
        })
        .collect();

    let reader_table_info = table_info.clone();
    let (button, transient_errors) = builder.build_fallible(move |caps| {
        let mut table_info = reader_table_info;
        let busy_signal = Arc::clone(&config.busy_signal);
        Box::pin(SignaledFuture::new(busy_signal, async move {
            let (id, worker_id) = (config.id, config.worker_id);
            let (data_cap_sets, caps) = caps.split_at_mut(export_count);
            let [definite_error_cap_set, probe_cap]: &mut [_; 2] = caps.try_into().unwrap();
            let mut data_outputs = SharedFuel::new(data_outputs);

            if !config.responsible_for("slot") {
                // Emit 0, to mark this worker as having started up correctly.
                for stat in config.statistics.values() {
                    stat.set_offset_known(0);
                    stat.set_offset_committed(0);
                }
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

            // The slot is always created by the snapshot operator. If the slot doesn't exist,
            // when this check runs, this operator will return an error.
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

                match execute(
                    &**metadata_client,
                    sql!("SELECT pg_terminate_backend($1)"),
                    &[&active_pid],
                )
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
            let resume_lsn = output_uppers
                .iter()
                .flat_map(|f| f.elements())
                .map(|&lsn| {
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
                })
                .min();
            let Some(resume_lsn) = resume_lsn else {
                std::future::pending::<()>().await;
                return Ok(());
            };
            // If we don't set "offset_committed" now, it'll be stuck at 0 (the default value)
            // until we finish processing the table snapshot. If the snapshot is large, that could be a long time.
            // This confuses the ingestion lag calculation in the UI, causing it to yield erroneously high values.
            for (id, stat) in config.statistics.iter() {
                let lsn = resume_lsns
                    .get(id)
                    .map_or(resume_lsn, |lsn| (*lsn).max(resume_lsn));
                stat.set_offset_committed(lsn.offset);
            }
            tracing::info!(
                %id,
                "timely-{worker_id} replication reader started lsn={resume_lsn}, \
                    waiting for snapshot to complete"
            );

            // Emitting an initial probe before we start waiting for rewinds ensures that we will
            // have a timestamp binding in the remap collection while the snapshot is processed.
            // This is important because otherwise the snapshot updates would need to be buffered
            // in the reclock operator, instead of being spilled to S3 in the persist sink.
            //
            // Note that we need to fetch the probe LSN _after_ having created the replication
            // slot, to make sure the fetched LSN will be included in the replication stream.
            let probe_ts = (config.now_fn)().into();
            let max_lsn = mz_postgres_util::fetch_max_lsn(
                &*metadata_client,
                connection.publication_details.get_is_physical_replica(),
            )
            .await?;
            let probe = Probe {
                probe_ts,
                upstream_frontier: Antichain::from_elem(MzOffset::from(max_lsn)),
            };
            probe_output.give(&probe_cap[0], probe);

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
                            for outputs in table_info.values() {
                                for output_index in outputs.keys() {
                                    // We pick `u64::MAX` as the LSN which will (in practice) never conflict
                                    // any previously revealed portions of the TVC.
                                    let update = (
                                        Err(DataflowError::from(err.clone())),
                                        MzOffset::from(u64::MAX),
                                        Diff::ONE,
                                    );
                                    data_outputs
                                        .give(
                                            *output_index,
                                            &data_cap_sets[*output_index][0],
                                            update,
                                        )
                                        .await;
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
                &connection,
                replication_client,
                Arc::clone(&metadata_client),
                resume_lsn,
                committed_uppers.as_mut(),
                &probe_output,
                &probe_cap[0],
            )
            .await?;

            let stream = match stream_result {
                Ok(stream) => stream,
                Err(err) => {
                    // If the replication stream cannot be obtained in a definite way there is
                    // nothing else to do. These errors are not retractable.
                    for outputs in table_info.values() {
                        for output_index in outputs.keys() {
                            // We pick `u64::MAX` as the LSN which will (in practice) never conflict
                            // any previously revealed portions of the TVC.
                            let update = (
                                Err(DataflowError::from(err.clone())),
                                MzOffset::from(u64::MAX),
                                Diff::ONE,
                            );
                            data_outputs
                                .give(*output_index, &data_cap_sets[*output_index][0], update)
                                .await;
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

            // Run the periodic schema validation on a separate task using a separate client,
            // to prevent it from blocking the replication reading progress.
            let ssh_tunnel_manager = &config.config.connection_context.ssh_tunnel_manager;
            let client = connection_config
                .connect("schema validation", ssh_tunnel_manager)
                .await?;
            let mut schema_errors = spawn_schema_validator(
                client,
                &config,
                connection.publication.clone(),
                table_info.clone(),
                connection.publication_details.get_is_physical_replica(),
            );

            // Instead of downgrading the capability for every transaction we process we only do it
            // if we're about to yield, which is checked at the bottom of the loop. This avoids
            // creating excessive progress tracking traffic when there are multiple small
            // transactions ready to go.
            let mut data_upper = resume_lsn;
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
                                &mut table_info,
                                &metrics,
                                &connection.publication,
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
                            while let Some((_oid, output_index, event, diff)) =
                                tx.try_next().await?
                            {
                                let event: Result<_, DataflowError> = event.map_err(Into::into);
                                if let Some(req) = rewinds.get(&output_index) {
                                    if commit_lsn <= req.snapshot_lsn {
                                        let update = (event.clone(), MzOffset::from(0), -diff);
                                        data_outputs
                                            .give(
                                                output_index,
                                                &data_cap_sets[output_index][0],
                                                update,
                                            )
                                            .await;
                                    }
                                }
                                let update = (event, commit_lsn, diff);
                                data_outputs
                                    .give(output_index, &data_cap_sets[output_index][0], update)
                                    .await;
                            }
                        }
                        _ => return Err(TransientError::BareTransactionEvent),
                    },
                    Ok(PrimaryKeepAlive(keepalive)) => {
                        trace!( %id,
                            "timely-{worker_id} received keepalive lsn={}",
                            keepalive.wal_end()
                        );

                        // Take the opportunity to report any schema validation errors.
                        while let Ok(error) = schema_errors.try_recv() {
                            use SchemaValidationError::*;
                            match error {
                                Postgres(PostgresError::PublicationMissing(publication)) => {
                                    let err = DefiniteError::PublicationDropped(publication);
                                    for outputs in table_info.values() {
                                        for output_index in outputs.keys() {
                                            let update = (
                                                Err(DataflowError::from(err.clone())),
                                                data_cap_sets[*output_index][0].time().clone(),
                                                Diff::ONE,
                                            );
                                            data_outputs
                                                .give(
                                                    *output_index,
                                                    &data_cap_sets[*output_index][0],
                                                    update,
                                                )
                                                .await;
                                        }
                                    }
                                    definite_error_handle.give(
                                        &definite_error_cap_set[0],
                                        ReplicationError::Definite(Rc::new(err)),
                                    );
                                    return Ok(());
                                }
                                Postgres(pg_error) => Err(TransientError::from(pg_error))?,
                                PhysicalReplicaChanged { expected, actual } => {
                                    // The upstream is no longer the kind of node the
                                    // source was created against (e.g. a physical
                                    // replica was promoted to a primary). Logical
                                    // decoding cannot safely continue, so stall with a
                                    // definite, non-retryable error.
                                    let err =
                                        DefiniteError::InvalidPhysicalReplica { expected, actual };
                                    for outputs in table_info.values() {
                                        for output_index in outputs.keys() {
                                            let update = (
                                                Err(DataflowError::from(err.clone())),
                                                // We don't have a clean way to align on when the replica changed so jump straight to u64::MAX to avoid conflicts.
                                                MzOffset::from(u64::MAX),
                                                Diff::ONE,
                                            );
                                            data_outputs
                                                .give(
                                                    *output_index,
                                                    &data_cap_sets[*output_index][0],
                                                    update,
                                                )
                                                .await;
                                        }
                                    }
                                    definite_error_handle.give(
                                        &definite_error_cap_set[0],
                                        ReplicationError::Definite(Rc::new(err)),
                                    );
                                    return Ok(());
                                }
                                Schema {
                                    oid,
                                    output_index,
                                    error,
                                } => {
                                    let table = table_info.get_mut(&oid).unwrap();
                                    if table.remove(&output_index).is_none() {
                                        continue;
                                    }

                                    let update = (
                                        Err(error.into()),
                                        data_cap_sets[output_index][0].time().clone(),
                                        Diff::ONE,
                                    );
                                    data_outputs
                                        .give(output_index, &data_cap_sets[output_index][0], update)
                                        .await;
                                }
                            }
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
                    // A port with a pending rewind must stay at the minimum capability since
                    // negated rewind data for its export is emitted at offset 0.
                    if concurrent_replication {
                        // Every other port advances with the stream, so exports that need no
                        // rewind make progress while a snapshot runs.
                        for (output_index, cap_set) in data_cap_sets.iter_mut().enumerate() {
                            if !rewinds.contains_key(&output_index) {
                                cap_set.downgrade([&data_upper]);
                            }
                        }
                    } else if rewinds.is_empty() {
                        for cap_set in data_cap_sets.iter_mut() {
                            cap_set.downgrade([&data_upper]);
                        }
                    }
                }
            }
            // We never expect the replication stream to gracefully end
            Err(TransientError::ReplicationEOF)
        }))
    });

    // We now process the slot updates and apply the cast expressions, with one decode operator
    // per export.
    let mut output_info = BTreeMap::new();
    for outputs in table_info.into_values() {
        for (output_index, info) in outputs {
            output_info.insert(output_index, info);
        }
    }
    let mut replication_updates = Vec::with_capacity(data_streams.len());
    let peers = u64::cast_from(scope.peers());
    let record = Rc::new(Cell::new(0_u64));
    for (output_index, data_stream) in data_streams.into_iter().enumerate() {
        let info = output_info.get(&output_index).cloned();
        let mut final_row = Row::default();
        let mut datum_vec = DatumVec::new();

        // Round robin on 1000-records basis to avoid creating tiny containers when there are a
        // small number of updates and a large number of workers. The counter is shared by every
        // export's exchange so rotation follows total volume.
        let record = Rc::clone(&record);
        let round_robin = Exchange::new(move |_| {
            let n = record.get();
            record.set(n.wrapping_add(1));
            (n / 1000) % peers
        });
        let updates = data_stream
            .unary(
                round_robin,
                &format!("PgCastReplicationRows({output_index})"),
                |_, _| {
                    move |input, output| {
                        input.for_each_time(|time, data| {
                            let mut session = output.session(&time);
                            for (event, time, diff) in data.flat_map(|data| data.drain(..)) {
                                let info = info
                                    .as_ref()
                                    .expect("only exports with output info receive data");
                                let event = event.and_then(|row| {
                                    let datums = datum_vec.borrow_with(&row);
                                    super::cast_row(&info.casts, &datums, &mut final_row)?;
                                    Ok(SourceMessage {
                                        key: Row::default(),
                                        value: final_row.clone(),
                                        metadata: Row::default(),
                                    })
                                });
                                session.give((event, time, diff));
                            }
                        });
                    }
                },
            )
            .as_collection();
        replication_updates.push(updates);
    }

    let errors = definite_errors.concat(transient_errors.map(ReplicationError::from));

    (
        replication_updates,
        probe_stream,
        errors,
        button.press_on_drop(),
    )
}
