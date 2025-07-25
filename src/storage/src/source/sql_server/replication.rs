// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Code to render the ingestion dataflow of a [`SqlServerSource`].

use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use differential_dataflow::AsCollection;
use differential_dataflow::containers::TimelyStack;
use futures::StreamExt;
use mz_ore::cast::CastFrom;
use mz_ore::future::InTask;
use mz_repr::{Diff, GlobalId, Row, RowArena};
use mz_sql_server_util::cdc::{CdcEvent, Lsn, Operation as CdcOperation};
use mz_storage_types::errors::{DataflowError, DecodeError, DecodeErrorKind};
use mz_storage_types::sources::SqlServerSource;
use mz_storage_types::sources::sql_server::{
    CDC_POLL_INTERVAL, MAX_LSN_WAIT, SNAPSHOT_PROGRESS_REPORT_INTERVAL,
};
use mz_timely_util::builder_async::{
    AsyncOutputHandle, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};
use mz_timely_util::containers::stack::AccountedStackBuilder;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::operators::{CapabilitySet, Concat, Map};
use timely::dataflow::{Scope, Stream as TimelyStream};
use timely::progress::{Antichain, Timestamp};

use crate::source::RawSourceCreationConfig;
use crate::source::sql_server::{
    DefiniteError, ReplicationError, SourceOutputInfo, TransientError,
};
use crate::source::types::{
    ProgressStatisticsUpdate, SignaledFuture, SourceMessage, StackedCollection,
};

/// Used as a partition ID to determine the worker that is responsible for
/// reading data from SQL Server.
///
/// TODO(sql_server2): It's possible we could have different workers
/// replicate different tables, if we're using SQL Server's CDC features.
static REPL_READER: &str = "reader";

pub(crate) fn render<G: Scope<Timestamp = Lsn>>(
    scope: G,
    config: RawSourceCreationConfig,
    outputs: BTreeMap<GlobalId, SourceOutputInfo>,
    source: SqlServerSource,
) -> (
    StackedCollection<G, (u64, Result<SourceMessage, DataflowError>)>,
    TimelyStream<G, Infallible>,
    TimelyStream<G, ReplicationError>,
    TimelyStream<G, ProgressStatisticsUpdate>,
    PressOnDropButton,
) {
    let op_name = format!("SqlServerReplicationReader({})", config.id);
    let mut builder = AsyncOperatorBuilder::new(op_name, scope);

    let (data_output, data_stream) = builder.new_output::<AccountedStackBuilder<_>>();
    let (_upper_output, upper_stream) = builder.new_output::<CapacityContainerBuilder<_>>();
    let (stats_output, stats_stream) = builder.new_output::<CapacityContainerBuilder<_>>();

    // Captures DefiniteErrors that affect the entire source, including all outputs
    let (definite_error_handle, definite_errors) =
        builder.new_output::<CapacityContainerBuilder<_>>();

    let (button, transient_errors) = builder.build_fallible(move |caps| {
        let busy_signal = Arc::clone(&config.busy_signal);
        Box::pin(SignaledFuture::new(busy_signal, async move {
            let [
                data_cap_set,
                upper_cap_set,
                stats_cap,
                definite_error_cap_set,
            ]: &mut [_; 4] = caps.try_into().unwrap();

            // TODO(sql_server2): Run ingestions across multiple workers.
            if !config.responsible_for(REPL_READER) {
                return Ok::<_, TransientError>(());
            }

            let connection_config = source
                .connection
                .resolve_config(
                    &config.config.connection_context.secrets_reader,
                    &config.config,
                    InTask::Yes,
                )
                .await?;
            let mut client = mz_sql_server_util::Client::connect(connection_config).await?;

            // The decoder is specific to the export, and each export pulls data from a specific capture instance.
            let mut decoder_map: BTreeMap<_, _> = BTreeMap::new();
            // Maps the 'capture instance' to the output index for only those outputs that this worker will snapshot
            let mut export_ids_to_snapshot: BTreeMap<_, Vec<_>> = BTreeMap::new();
            // Maps the 'capture instance' to the output index for all outputs of this worker
            let mut capture_instances: BTreeMap<_, Vec<_>> = BTreeMap::new();

            for output in outputs.values() {
                if decoder_map.insert(output.partition_index, Arc::clone(&output.decoder)).is_some() {
                    panic!("Multiple decoders for output index {}", output.partition_index);
                }
                capture_instances
                    .entry(Arc::clone(&output.capture_instance))
                    .or_default()
                    .push(output.partition_index);

                if *output.resume_upper == [Lsn::minimum()] {
                    export_ids_to_snapshot
                        .entry(Arc::clone(&output.capture_instance))
                        .or_default()
                        .push((output.partition_index, output.initial_lsn));
                }
            }

            let mut cdc_handle = client
                .cdc(capture_instances.keys().cloned())
                .max_lsn_wait(MAX_LSN_WAIT.get(config.config.config_set()));

            // Snapshot any instance that requires it.
            // The LSN returned here will be the max LSN for any capture instance on the SQL server.
            let snapshot_lsn = {
                // Small helper closure.
                let emit_stats = |cap, known: usize, total: usize| {
                    let update = ProgressStatisticsUpdate::Snapshot {
                        records_known: u64::cast_from(known),
                        records_staged: u64::cast_from(total),
                    };
                    tracing::debug!(?config.id, %known, %total, "snapshot progress");
                    stats_output.give(cap, update);
                };

                let capture_instances_to_snapshot: BTreeSet<_> = export_ids_to_snapshot.keys().cloned().collect();
                tracing::debug!(?config.id, ?capture_instances_to_snapshot, "starting snapshot");
                // Eagerly emit an event if we have tables to snapshot.
                if !capture_instances_to_snapshot.is_empty() {
                    emit_stats(&stats_cap[0], 0, 0);
                }

                let (snapshot_lsn, snapshot_stats, snapshot_streams) = cdc_handle
                    .snapshot(Some(capture_instances_to_snapshot), config.worker_id, config.id)
                    .await?;
                let snapshot_cap = data_cap_set.delayed(&snapshot_lsn);

                // As we stream rows for the snapshot we'll track the total we've seen.
                let mut records_total: usize = 0;
                let records_known = snapshot_stats.values().sum();
                let report_interval =
                    SNAPSHOT_PROGRESS_REPORT_INTERVAL.handle(config.config.config_set());
                let mut last_report = Instant::now();
                if !snapshot_stats.is_empty() {
                    emit_stats(&stats_cap[0], records_known, 0);
                }

                // Begin streaming our snapshots!
                let mut snapshot_streams = std::pin::pin!(snapshot_streams);
                while let Some((capture_instance, data)) = snapshot_streams.next().await {
                    let sql_server_row = data.map_err(TransientError::from)?;
                    records_total = records_total.saturating_add(1);

                    if last_report.elapsed() > report_interval.get() {
                        last_report = Instant::now();
                        emit_stats(&stats_cap[0], records_known, records_total);
                    }

                    // Decode the SQL Server row into an MZ one.
                    let mut mz_row = Row::default();
                    let arena = RowArena::default();

                    let partition_indexes = export_ids_to_snapshot.get(&capture_instance)
                        .ok_or_else(|| {
                            let msg = format!("no snapshot outputs for capture instance: '{capture_instance}'");
                            TransientError::ProgrammingError(msg)
                        })?;

                    for (partition_idx, _) in partition_indexes {
                        let decoder = decoder_map.get(partition_idx).expect("decoder for output");
                        // Try to decode a row, returning a SourceError if it fails.
                        let message = match decoder.decode(&sql_server_row, &mut mz_row, &arena) {
                            Ok(()) => Ok(SourceMessage {
                                key: Row::default(),
                                value: mz_row.clone(),
                                metadata: Row::default(),
                            }),
                            Err(e) => {
                                let kind = DecodeErrorKind::Text(e.to_string().into());
                                // TODO(sql_server2): Get the raw bytes from `tiberius`.
                                let raw = format!("{sql_server_row:?}");
                                Err(DataflowError::DecodeError(Box::new(DecodeError {
                                    kind,
                                    raw: raw.as_bytes().to_vec(),
                                })))
                            }
                        };
                        data_output
                            .give_fueled(
                                &snapshot_cap,
                                ((*partition_idx, message), snapshot_lsn, Diff::ONE),
                            )
                            .await;
                    }
                }

                mz_ore::soft_assert_eq_or_log!(
                    records_known,
                    records_total,
                    "snapshot size did not match total records received",
                );
                emit_stats(&stats_cap[0], records_known, records_total);

                snapshot_lsn
            };

            // Rewinds need to keep track of 2 timestamps to ensure that
            // all replicas emit the same set of updates for any given timestamp.
            // These are the initial_lsn and snapshot_lsn, where initial_lsn must be
            // less than or equal to snapshot_lsn.
            //
            // - events at an LSN less than or equal to initial_lsn are ignored
            // - events at an LSN greater than initial_lsn and less than or equal to
            //   snapshot_lsn are retracted at Lsn::minimum(), and emitted at the commit_lsn
            // - events at an LSN greater than snapshot_lsn are emitted at the commit_lsn
            //
            // where the commit_lsn is the upstream LSN that the event was committed at
            //
            // If initial_lsn == snapshot_lsn, all CDC events at LSNs up to and including the
            // snapshot_lsn are ignored, and no rewinds are issued.
            let mut rewinds: BTreeMap<_, _> = export_ids_to_snapshot
                .values()
                .flat_map(|export_ids|
                    export_ids
                        .iter()
                        .map(|(idx, initial_lsn)| (*idx, (*initial_lsn, snapshot_lsn)))
                ).collect();

            // For now, we assert that initial_lsn captured during purification is less
            // than or equal to snapshot_lsn. If that was not true, it would mean that
            // we observed a SQL server DB that appeared to go back in time.
            // TODO (maz): not ideal to do this after snapshot, move this into
            // CdcStream::snapshot after https://github.com/MaterializeInc/materialize/pull/32979 is merged.
            for (initial_lsn, snapshot_lsn) in rewinds.values() {
                assert!(
                    initial_lsn <= snapshot_lsn,
                    "initial_lsn={initial_lsn} snapshot_lsn={snapshot_lsn}"
                );
            }

            tracing::debug!("rewinds to process: {rewinds:?}");

            export_ids_to_snapshot.clear();


            // Resumption point is the minimum LSN that has been observed.
            let resume_lsn = outputs
                .values()
                .flat_map(|src_info| {
                    // initial_lsn is the max lsn observed, but the resume lsn
                    // is the next lsn that should be read.  After a snapshot, initial_lsn
                    // has been read, so replication will start at the next available lsn.
                    let start_lsn = src_info.initial_lsn.increment();
                    src_info.resume_upper
                        .elements()
                        .iter()
                        .map(move |lsn| {
                            if *lsn == Lsn::minimum() {
                                start_lsn
                            } else {
                                *lsn
                            }
                        })
                })
                .min();

            // If a source is created with no subsources, there will be no inputs
            // as each capture instance collects data for a table. Without a cdc stream
            // to consume, this source will have no outputs either.
            let Some(resume_lsn) = resume_lsn else {
                // TODO: this is only in place until we implement RLU
                // https://github.com/MaterializeInc/database-issues/issues/9212
                tracing::warn!(%config.id, "timely-{} no resume_lsn, waiting", config.worker_id);
                std::future::pending::<()>().await;
                unreachable!();
            };
            tracing::info!(%config.id, "timely-{} replication starting with resume_lsn = {}", config.worker_id, resume_lsn);

            for instance in capture_instances.keys() {
                cdc_handle = cdc_handle.start_lsn(instance, resume_lsn);
            }

            // Off to the races! Replicate data from SQL Server.
            let cdc_stream = cdc_handle
                .poll_interval(CDC_POLL_INTERVAL.get(config.config.config_set()))
                .into_stream();
            let mut cdc_stream = std::pin::pin!(cdc_stream);

            // TODO(sql_server2): We should emit `ProgressStatisticsUpdate::SteadyState` messages
            // here, when we receive progress events. What stops us from doing this now is our
            // 10-byte LSN doesn't fit into the 8-byte integer that the progress event uses.
            let mut log_rewinds_complete = true;
            while let Some(event) = cdc_stream.next().await {
                let event = event.map_err(TransientError::from)?;
                tracing::trace!(?config.id, ?event, "got replication event");

                let (capture_instance, commit_lsn, changes) = match event {
                    // We've received all of the changes up-to this LSN, so
                    // downgrade our capability.
                    CdcEvent::Progress { next_lsn } => {
                        tracing::debug!(?config.id, ?next_lsn, "got a closed lsn");
                        // cannot downgrade capability until rewinds have been processed,
                        // we must be able to produce data at the minimum offset.
                        rewinds.retain(|_, (_, snapshot_lsn)| next_lsn <= *snapshot_lsn);
                        if rewinds.is_empty() {
                            if log_rewinds_complete {
                                tracing::debug!("rewinds complete");
                                log_rewinds_complete = false;
                            }
                            data_cap_set.downgrade(Antichain::from_elem(next_lsn));
                        } else {
                            tracing::debug!("rewinds remaining: {:?}", rewinds);
                        }
                        upper_cap_set.downgrade(Antichain::from_elem(next_lsn));
                        continue;
                    }
                    // We've got new data! Let's process it.
                    CdcEvent::Data {
                        capture_instance,
                        lsn,
                        changes,
                    } => (capture_instance, lsn, changes),
                };

                let Some(partition_indexes) = capture_instances.get(&capture_instance) else {
                    let definite_error = DefiniteError::ProgrammingError(format!(
                        "capture instance didn't exist: '{capture_instance}'"
                    ));
                    let () = return_definite_error(
                        definite_error.clone(),
                        capture_instances.values().flat_map(|indexes| indexes.iter().copied()),
                        data_output,
                        data_cap_set,
                        definite_error_handle,
                        definite_error_cap_set,
                    )
                    .await;
                    return Ok(());
                };


                for change in changes {
                    let (sql_server_row, diff): (_, _) = match change {
                        CdcOperation::Insert(sql_server_row)
                        | CdcOperation::UpdateNew(sql_server_row) => (sql_server_row, Diff::ONE),
                        CdcOperation::Delete(sql_server_row)
                        | CdcOperation::UpdateOld(sql_server_row) => {
                            (sql_server_row, Diff::MINUS_ONE)
                        }
                    };

                    // Try to decode a row, returning a SourceError if it fails.
                    let mut mz_row = Row::default();
                    let arena = RowArena::default();

                    for partition_idx in partition_indexes {
                        let decoder = decoder_map.get(partition_idx).unwrap();

                        let rewind = rewinds.get(partition_idx);
                        // We must continue here to avoid decoding and emitting. We don't have to compare with
                        // snapshot_lsn as we are guaranteed that initial_lsn <= snapshot_lsn.
                        if rewind.is_some_and(|(initial_lsn, _)| commit_lsn <= *initial_lsn) {
                            continue;
                        }

                        // Try to decode a row, returning a SourceError if it fails.
                        let message = match decoder.decode(&sql_server_row, &mut mz_row, &arena) {
                            Ok(()) => Ok(SourceMessage {
                                key: Row::default(),
                                value: mz_row.clone(),
                                metadata: Row::default(),
                            }),
                            Err(e) => {
                                let kind = DecodeErrorKind::Text(e.to_string().into());
                                // TODO(sql_server2): Get the raw bytes from `tiberius`.
                                let raw = format!("{sql_server_row:?}");
                                Err(DataflowError::DecodeError(Box::new(DecodeError {
                                    kind,
                                    raw: raw.as_bytes().to_vec(),
                                })))
                            }
                        };

                        if rewind.is_some_and(|(_, snapshot_lsn)| commit_lsn <= *snapshot_lsn) {
                            data_output
                                .give_fueled(
                                    &data_cap_set[0],
                                    ((*partition_idx, message.clone()), Lsn::minimum(), -diff),
                                )
                                .await;
                        }
                        data_output
                            .give_fueled(
                                &data_cap_set[0],
                                ((*partition_idx, message), commit_lsn, diff),
                            )
                            .await;
                    }
                }
            }
            Err(TransientError::ReplicationEOF)
        }))
    });

    let error_stream = definite_errors.concat(&transient_errors.map(ReplicationError::Transient));

    (
        data_stream.as_collection(),
        upper_stream,
        error_stream,
        stats_stream,
        button.press_on_drop(),
    )
}

type StackedAsyncOutputHandle<T, D> = AsyncOutputHandle<
    T,
    AccountedStackBuilder<CapacityContainerBuilder<TimelyStack<(D, T, Diff)>>>,
    Tee<T, TimelyStack<(D, T, Diff)>>,
>;

/// Helper method to return a "definite" error upstream.
async fn return_definite_error(
    err: DefiniteError,
    outputs: impl Iterator<Item = u64>,
    data_handle: StackedAsyncOutputHandle<Lsn, (u64, Result<SourceMessage, DataflowError>)>,
    data_capset: &CapabilitySet<Lsn>,
    errs_handle: AsyncOutputHandle<
        Lsn,
        CapacityContainerBuilder<Vec<ReplicationError>>,
        Tee<Lsn, Vec<ReplicationError>>,
    >,
    errs_capset: &CapabilitySet<Lsn>,
) {
    for output_idx in outputs {
        let update = (
            (output_idx, Err(err.clone().into())),
            // TODO(sql_server1): Provide the correct LSN.
            Lsn::minimum(),
            Diff::ONE,
        );
        data_handle.give_fueled(&data_capset[0], update).await;
    }
    errs_handle.give(
        &errs_capset[0],
        ReplicationError::DefiniteError(Rc::new(err)),
    );
}
