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
use mz_ore::columnar::Boxed;
use mz_ore::future::InTask;
use mz_repr::{Diff, GlobalId, Row, RowArena};
use mz_sql_server_util::cdc::{CdcEvent, Lsn, Operation as CdcOperation};
use mz_storage_types::errors::{DataflowError, DecodeError, DecodeErrorKind};
use mz_storage_types::sources::SqlServerSource;
use mz_storage_types::sources::sql_server::{
    CDC_POLL_INTERVAL, SNAPSHOT_MAX_LSN_WAIT, SNAPSHOT_PROGRESS_REPORT_INTERVAL,
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

            let output_indexes: Vec<_> = outputs
                .values()
                .map(|v| usize::cast_from(v.partition_index))
                .collect();

            // Instances that have already made progress do not need to be snapshotted.
            let needs_snapshot: BTreeSet<_> = outputs
                .values()
                .filter_map(|output| {
                    if *output.resume_upper == [Lsn::minimum()] {
                        Some(Arc::clone(&output.capture_instance))
                    } else {
                        None
                    }
                })
                .collect();

            // Map from a SQL Server 'capture instance' to Materialize collection.
            let capture_instances: BTreeMap<_, _> = outputs
                .values()
                .map(|output| {
                    (
                        Arc::clone(&output.capture_instance),
                        (output.partition_index, Arc::clone(&output.decoder)),
                    )
                })
                .collect();
            let mut cdc_handle = client
                .cdc(capture_instances.keys().cloned())
                .max_lsn_wait(SNAPSHOT_MAX_LSN_WAIT.get(config.config.config_set()));

            // Snapshot any instances that require it.
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

                tracing::debug!(?config.id, ?needs_snapshot, "starting snapshot");
                // Eagerly emit an event if we have tables to snapshot.
                if !needs_snapshot.is_empty() {
                    emit_stats(&stats_cap[0], 0, 0);
                }

                let (snapshot_lsn, snapshot_stats, snapshot_streams) =
                    cdc_handle.snapshot(Some(needs_snapshot)).await?;
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
                    let (partition_idx, decoder) =
                        capture_instances.get(&capture_instance).ok_or_else(|| {
                            let msg =
                                format!("capture instance didn't exist: '{capture_instance}'");
                            TransientError::ProgrammingError(msg)
                        })?;

                    // Try to decode a row, returning a SourceError if it fails.
                    let mut mz_row = Row::default();
                    let arena = RowArena::default();
                    let message = match decoder.decode(&sql_server_row, &mut mz_row, &arena) {
                        Ok(()) => Ok(SourceMessage {
                            key: Row::default(),
                            value: mz_row,
                            metadata: Row::default(),
                        }),
                        Err(e) => {
                            let kind = DecodeErrorKind::Text(e.to_string().into());
                            // TODO(sql_server2): Get the raw bytes from `tiberius`.
                            let raw = format!("{sql_server_row:?}");
                            Err(DataflowError::DecodeError(Boxed::new(DecodeError {
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

                mz_ore::soft_assert_eq_or_log!(
                    records_known,
                    records_total,
                    "snapshot size did not match total records received",
                );
                emit_stats(&stats_cap[0], records_known, records_total);

                snapshot_lsn
            };

            // Start replicating from the LSN __after__ we took the snapshot.
            let replication_start_lsn = snapshot_lsn.increment();

            // Set all of the LSNs to start replicating from.
            for output_info in outputs.values() {
                match output_info.resume_upper.as_option() {
                    // We just snapshotted this instance, so use the snapshot LSN.
                    Some(lsn) => {
                        let initial_lsn = if *lsn == Lsn::minimum() {
                            replication_start_lsn
                        } else {
                            *lsn
                        };
                        cdc_handle =
                            cdc_handle.start_lsn(&output_info.capture_instance, initial_lsn);
                    }
                    None => unreachable!("empty resume upper?"),
                }
            }

            // Off to the races! Replicate data from SQL Server.
            let cdc_stream = cdc_handle
                .poll_interval(CDC_POLL_INTERVAL.get(config.config.config_set()))
                .into_stream();
            let mut cdc_stream = std::pin::pin!(cdc_stream);

            // TODO(sql_server2): We should emit `ProgressStatisticsUpdate::SteadyState` messages
            // here, when we receive progress events. What stops us from doing this now is our
            // 10-byte LSN doesn't fit into the 8-byte integer that the progress event uses.
            while let Some(event) = cdc_stream.next().await {
                let event = event.map_err(TransientError::from)?;
                tracing::trace!(?config.id, ?event, "got replication event");

                let (capture_instance, commit_lsn, changes) = match event {
                    // We've received all of the changes up-to this LSN, so
                    // downgrade our capability.
                    CdcEvent::Progress { next_lsn } => {
                        tracing::debug!(?config.id, ?next_lsn, "got a closed lsn");
                        data_cap_set.downgrade(Antichain::from_elem(next_lsn));
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

                // Decode the SQL Server row into an MZ one.
                let Some((partition_idx, decoder)) = capture_instances.get(&capture_instance)
                else {
                    let definite_error = DefiniteError::ProgrammingError(format!(
                        "capture instance didn't exist: '{capture_instance}'"
                    ));
                    let () = return_definite_error(
                        definite_error,
                        &output_indexes[..],
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
                    let message = match decoder.decode(&sql_server_row, &mut mz_row, &arena) {
                        Ok(()) => Ok(SourceMessage {
                            key: Row::default(),
                            value: mz_row,
                            metadata: Row::default(),
                        }),
                        Err(e) => {
                            let kind = DecodeErrorKind::Text(e.to_string().into());
                            // TODO(sql_server2): Get the raw bytes from `tiberius`.
                            let raw = format!("{sql_server_row:?}");
                            Err(DataflowError::DecodeError(Boxed::new(DecodeError {
                                kind,
                                raw: raw.as_bytes().to_vec(),
                            })))
                        }
                    };
                    data_output
                        .give_fueled(
                            &data_cap_set[0],
                            ((*partition_idx, message), commit_lsn, diff),
                        )
                        .await;
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
    outputs: &[usize],
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
            (u64::cast_from(*output_idx), Err(err.clone().into())),
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
