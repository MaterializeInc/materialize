// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Code to render the ingestion dataflow of a [`SqlServerSourceConnection`].

use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use differential_dataflow::AsCollection;
use differential_dataflow::containers::TimelyStack;
use futures::StreamExt;
use itertools::Itertools;
use mz_ore::cast::CastFrom;
use mz_ore::future::InTask;
use mz_repr::{Diff, GlobalId, Row, RowArena};
use mz_sql_server_util::SqlServerCdcMetrics;
use mz_sql_server_util::cdc::{CdcEvent, Lsn, Operation as CdcOperation};
use mz_sql_server_util::desc::SqlServerRowDecoder;
use mz_sql_server_util::inspect::get_latest_restore_history_id;
use mz_storage_types::errors::{DataflowError, DecodeError, DecodeErrorKind};
use mz_storage_types::sources::SqlServerSourceConnection;
use mz_storage_types::sources::sql_server::{
    CDC_POLL_INTERVAL, MAX_LSN_WAIT, SNAPSHOT_PROGRESS_REPORT_INTERVAL,
};
use mz_timely_util::builder_async::{
    AsyncOutputHandle, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};
use mz_timely_util::containers::stack::AccountedStackBuilder;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::operators::{CapabilitySet, Concat, Map};
use timely::dataflow::{Scope, Stream as TimelyStream};
use timely::progress::{Antichain, Timestamp};

use crate::metrics::source::sql_server::SqlServerSourceMetrics;
use crate::source::RawSourceCreationConfig;
use crate::source::sql_server::{
    DefiniteError, ReplicationError, SourceOutputInfo, TransientError,
};
use crate::source::types::{SignaledFuture, SourceMessage, StackedCollection};

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
    source: SqlServerSourceConnection,
    metrics: SqlServerSourceMetrics,
) -> (
    StackedCollection<G, (u64, Result<SourceMessage, DataflowError>)>,
    TimelyStream<G, Infallible>,
    TimelyStream<G, ReplicationError>,
    PressOnDropButton,
) {
    let op_name = format!("SqlServerReplicationReader({})", config.id);
    let mut builder = AsyncOperatorBuilder::new(op_name, scope);

    let (data_output, data_stream) = builder.new_output::<AccountedStackBuilder<_>>();
    let (_upper_output, upper_stream) = builder.new_output::<CapacityContainerBuilder<_>>();

    // Captures DefiniteErrors that affect the entire source, including all outputs
    let (definite_error_handle, definite_errors) =
        builder.new_output::<CapacityContainerBuilder<_>>();

    let (button, transient_errors) = builder.build_fallible(move |caps| {
        let busy_signal = Arc::clone(&config.busy_signal);
        Box::pin(SignaledFuture::new(busy_signal, async move {
            let [
                data_cap_set,
                upper_cap_set,
                definite_error_cap_set,
            ]: &mut [_; 3] = caps.try_into().unwrap();

            let connection_config = source
                .connection
                .resolve_config(
                    &config.config.connection_context.secrets_reader,
                    &config.config,
                    InTask::Yes,
                )
                .await?;
            let mut client = mz_sql_server_util::Client::connect(connection_config).await?;

            let worker_id = config.worker_id;

            // The decoder is specific to the export, and each export pulls data from a specific capture instance.
            let mut decoder_map: BTreeMap<_, _> = BTreeMap::new();
            // Maps the 'capture instance' to the output index for only those outputs that this worker will snapshot
            let mut capture_instance_to_snapshot: BTreeMap<Arc<str>, Vec<_>> = BTreeMap::new();
            // Maps the 'capture instance' to the output index for all outputs of this worker
            let mut capture_instances: BTreeMap<Arc<str>, Vec<_>> = BTreeMap::new();
            // Export statistics for a given capture instance
            let mut export_statistics: BTreeMap<_, Vec<_>> = BTreeMap::new();

            for (export_id, output) in outputs.iter() {
                if decoder_map.insert(output.partition_index, Arc::clone(&output.decoder)).is_some() {
                    panic!("Multiple decoders for output index {}", output.partition_index);
                }
                capture_instances
                    .entry(Arc::clone(&output.capture_instance))
                    .or_default()
                    .push(output.partition_index);

                if *output.resume_upper == [Lsn::minimum()] {
                    capture_instance_to_snapshot
                        .entry(Arc::clone(&output.capture_instance))
                        .or_default()
                        .push((output.partition_index, output.initial_lsn));
                }
                export_statistics.entry(Arc::clone(&output.capture_instance))
                    .or_default()
                    .push(
                        config
                            .statistics
                            .get(export_id)
                            .expect("statistics have been intialized")
                            .clone(),
                    );
            }

            // Eagerly emit an event if we have tables to snapshot.
            // A worker *must* emit a count even if not responsible for snapshotting a table
            // as statistic summarization will return null if any worker hasn't set a value.
            // This will also reset snapshot stats for any exports not snapshotting.
            metrics.snapshot_table_count.set(u64::cast_from(capture_instance_to_snapshot.len()));
            if !capture_instance_to_snapshot.is_empty() {
                for stats in config.statistics.values() {
                    stats.set_snapshot_records_known(0);
                    stats.set_snapshot_records_staged(0);
                }
            }
            // We need to emit statistics before we exit
            // TODO(sql_server2): Run ingestions across multiple workers.
            if !config.responsible_for(REPL_READER) {
                return Ok::<_, TransientError>(());
            }

            let snapshot_instances = capture_instance_to_snapshot
                    .keys()
                    .map(|i| i.as_ref());

            // TODO (maz): we can avoid this query by using SourceOutputInfo
            let snapshot_tables = mz_sql_server_util::inspect::get_tables_for_capture_instance(&mut client, snapshot_instances).await?;

            // validate that the restore_history_id hasn't changed
            let current_restore_history_id = get_latest_restore_history_id(&mut client).await?;
            if current_restore_history_id != source.extras.restore_history_id {
                let definite_error = DefiniteError::RestoreHistoryChanged(
                    source.extras.restore_history_id.clone(),
                    current_restore_history_id.clone()
                );
                tracing::warn!(?definite_error, "Restore detected, exiting");

                return_definite_error(
                        definite_error,
                        capture_instances.values().flat_map(|indexes| indexes.iter().copied()),
                        data_output,
                        data_cap_set,
                        definite_error_handle,
                        definite_error_cap_set,
                    ).await;
                return Ok(());
            }

            // We first calculate all the total rows we need to fetch across all tables. Since this
            // happens outside the snapshot transaction the totals might be off, so we won't assert
            // that we get exactly this many rows later.
            for table in &snapshot_tables {
                let qualified_table_name = format!("{schema_name}.{table_name}",
                    schema_name = &table.schema_name,
                    table_name = &table.name);
                let size_calc_start = Instant::now();
                let table_total = mz_sql_server_util::inspect::snapshot_size(&mut client, &table.schema_name, &table.name).await?;
                metrics.set_snapshot_table_size_latency(
                    &qualified_table_name,
                    size_calc_start.elapsed().as_secs_f64()
                );
                for export_stat in export_statistics.get(&table.capture_instance.name).unwrap() {
                    export_stat.set_snapshot_records_known(u64::cast_from(table_total));
                    export_stat.set_snapshot_records_staged(0);
                }
            }
            let cdc_metrics = PrometheusSqlServerCdcMetrics{inner: &metrics};
            let mut cdc_handle = client
                .cdc(capture_instances.keys().cloned(), cdc_metrics)
                .max_lsn_wait(MAX_LSN_WAIT.get(config.config.config_set()));

            // Snapshot any instance that requires it.
            // Each table snapshot will have its own LSN captured at the moment of snapshotting.
            let snapshot_lsns: BTreeMap<Arc<str>, Lsn> = {
                // Before starting a transaction where the LSN will not advance, ensure
                // the upstream DB is ready for CDC.
                cdc_handle.wait_for_ready().await?;

                // Intentionally logging this at info for debugging. This section won't get entered
                // often, but if there are problems here, it will be much easier to troubleshoot
                // knowing where stall/hang might be happening.
                tracing::info!(%config.worker_id, "timely-{worker_id} upstream is ready");

                let report_interval =
                    SNAPSHOT_PROGRESS_REPORT_INTERVAL.handle(config.config.config_set());
                let mut last_report = Instant::now();
                let mut snapshot_lsns = BTreeMap::new();
                let arena = RowArena::default();

                for table in snapshot_tables {
                    // TODO(sql_server3): filter columns to only select columns required for Source.
                    let (snapshot_lsn, snapshot) = cdc_handle
                        .snapshot(&table, config.worker_id, config.id)
                        .await?;

                    tracing::info!(%config.id, %table.name, %table.schema_name, %snapshot_lsn, "timely-{worker_id} snapshot start");

                    let mut snapshot = std::pin::pin!(snapshot);

                    snapshot_lsns.insert(Arc::clone(&table.capture_instance.name), snapshot_lsn);

                    let partition_indexes = capture_instance_to_snapshot.get(&table.capture_instance.name)
                        .unwrap_or_else(|| {
                            panic!("no snapshot outputs in known capture instances [{}] for capture instance: '{}'", capture_instance_to_snapshot.keys().join(","), table.capture_instance.name);
                        });

                    let mut snapshot_staged = 0;
                    while let Some(result) = snapshot.next().await {
                        let sql_server_row = result.map_err(TransientError::from)?;

                        if last_report.elapsed() > report_interval.get() {
                            last_report = Instant::now();
                            for export_stat in export_statistics.get(&table.capture_instance.name).unwrap() {
                                export_stat.set_snapshot_records_staged(snapshot_staged);
                            }
                        }

                        for (partition_idx, _) in partition_indexes {
                            // Decode the SQL Server row into an MZ one.
                            let mut mz_row = Row::default();

                            let decoder = decoder_map.get(partition_idx).expect("decoder for output");
                            // Try to decode a row, returning a SourceError if it fails.
                            let message = decode(decoder, &sql_server_row, &mut mz_row, &arena, None);
                            data_output
                                .give_fueled(
                                    &data_cap_set[0],
                                    ((*partition_idx, message), Lsn::minimum(), Diff::ONE),
                                )
                                .await;
                        }
                        snapshot_staged += 1;
                    }

                    tracing::info!(%config.id, %table.name, %table.schema_name, %snapshot_lsn, "timely-{worker_id} snapshot complete");
                    metrics.snapshot_table_count.dec();
                    // final update for snapshot_staged, using the staged values as the total is an estimate
                    for export_stat in export_statistics.get(&table.capture_instance.name).unwrap() {
                        export_stat.set_snapshot_records_staged(snapshot_staged);
                        export_stat.set_snapshot_records_known(snapshot_staged);
                    }
                }

                snapshot_lsns
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
            let mut rewinds: BTreeMap<_, _> = capture_instance_to_snapshot
                .iter()
                .flat_map(|(capture_instance, export_ids)|{
                    let snapshot_lsn = snapshot_lsns.get(capture_instance).expect("snapshot lsn must be collected for capture instance");
                    export_ids
                        .iter()
                        .map(|(idx, initial_lsn)| (*idx, (*initial_lsn, *snapshot_lsn)))
                }).collect();

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

            capture_instance_to_snapshot.clear();

            // Resumption point is the minimum LSN that has been observed per capture instance.
            let mut resume_lsns = BTreeMap::new();
            for src_info in outputs.values() {
                let resume_lsn = match src_info.resume_upper.as_option() {
                    Some(lsn) if *lsn != Lsn::minimum() => *lsn,
                    // initial_lsn is the max lsn observed, but the resume lsn
                    // is the next lsn that should be read.  After a snapshot, initial_lsn
                    // has been read, so replication will start at the next available lsn.
                    Some(_) => src_info.initial_lsn.increment(),
                    None => panic!("resume_upper has at least one value"),
                };
                resume_lsns.entry(Arc::clone(&src_info.capture_instance))
                    .and_modify(|existing| *existing = std::cmp::min(*existing, resume_lsn))
                    .or_insert(resume_lsn);
            }

            tracing::info!(%config.id, ?resume_lsns, "timely-{} replication starting", config.worker_id);
            for instance in capture_instances.keys() {
                let resume_lsn = resume_lsns
                    .get(instance)
                    .expect("resume_lsn exists for capture instance");
                cdc_handle = cdc_handle.start_lsn(instance, *resume_lsn);
            }

            // Off to the races! Replicate data from SQL Server.
            let cdc_stream = cdc_handle
                .poll_interval(CDC_POLL_INTERVAL.get(config.config.config_set()))
                .into_stream();
            let mut cdc_stream = std::pin::pin!(cdc_stream);

            let mut errored_instances = BTreeSet::new();

            // TODO(sql_server2): We should emit `ProgressStatisticsUpdate::SteadyState` messages
            // here, when we receive progress events. What stops us from doing this now is our
            // 10-byte LSN doesn't fit into the 8-byte integer that the progress event uses.
            let mut log_rewinds_complete = true;

            // deferred_updates temporarily stores rows for UPDATE operation to support Large Object
            // Data (LOD) types (i.e. varchar(max), nvarchar(max)). The value of a
            // LOD column will be NULL for the old row (operation = 3) if the value of the
            // field did not change. The field data will be available in the new row
            // (operation = 4).
            // The CDC stream implementation emits a [`CdcEvent::Data`] event, which contains a
            // batch of operations.  There is no guarantee that both old and new rows will
            // exist in a single batch, so deferred updates must be tracked across multiple data
            // events.
            //
            // In the current implementation schema change events won't be emitted between old
            // and new rows.
            //
            // See <https://learn.microsoft.com/en-us/sql/relational-databases/system-tables/cdc-capture-instance-ct-transact-sql?view=sql-server-ver17#large-object-data-types>
            let mut deferred_updates = BTreeMap::new();

            while let Some(event) = cdc_stream.next().await {
                let event = event.map_err(TransientError::from)?;
                tracing::trace!(?config.id, ?event, "got replication event");

                tracing::trace!("deferred_updates = {deferred_updates:?}");
                match event {
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
                    }
                    // We've got new data! Let's process it.
                    CdcEvent::Data {
                        capture_instance,
                        lsn,
                        changes,
                    } => {
                        if errored_instances.contains(&capture_instance) {
                            // outputs for this captured instance are in an errored state, so they are not
                            // emitted
                            metrics.ignored.inc_by(u64::cast_from(changes.len()));
                        }

                        let Some(partition_indexes) = capture_instances.get(&capture_instance) else {
                            let definite_error = DefiniteError::ProgrammingError(format!(
                                "capture instance didn't exist: '{capture_instance}'"
                            ));
                            return_definite_error(
                                definite_error,
                                capture_instances.values().flat_map(|indexes| indexes.iter().copied()),
                                data_output,
                                data_cap_set,
                                definite_error_handle,
                                definite_error_cap_set,
                            )
                            .await;
                            return Ok(());
                        };


                        handle_data_event(
                            changes,
                            partition_indexes,
                            &decoder_map,
                            lsn,
                            &rewinds,
                            &data_output,
                            data_cap_set,
                            &metrics,
                            &mut deferred_updates,
                        ).await?
                    },
                    CdcEvent::SchemaUpdate { capture_instance, table, ddl_event } => {
                        if !errored_instances.contains(&capture_instance)
                            && !ddl_event.is_compatible() {
                            let Some(partition_indexes) = capture_instances.get(&capture_instance) else {
                                let definite_error = DefiniteError::ProgrammingError(format!(
                                    "capture instance didn't exist: '{capture_instance}'"
                                ));
                                return_definite_error(
                                    definite_error,
                                    capture_instances.values().flat_map(|indexes| indexes.iter().copied()),
                                    data_output,
                                    data_cap_set,
                                    definite_error_handle,
                                    definite_error_cap_set,
                                )
                                .await;
                                return Ok(());
                            };
                            let error = DefiniteError::IncompatibleSchemaChange(
                                capture_instance.to_string(),
                                table.to_string()
                            );
                            for partition_idx in partition_indexes {
                                data_output
                                    .give_fueled(
                                        &data_cap_set[0],
                                        ((*partition_idx, Err(error.clone().into())), ddl_event.lsn, Diff::ONE),
                                    )
                                    .await;
                            }
                            errored_instances.insert(capture_instance);
                        }
                    }
                };
            }
            Err(TransientError::ReplicationEOF)
        }))
    });

    let error_stream = definite_errors.concat(&transient_errors.map(ReplicationError::Transient));

    (
        data_stream.as_collection(),
        upper_stream,
        error_stream,
        button.press_on_drop(),
    )
}

async fn handle_data_event(
    changes: Vec<CdcOperation>,
    partition_indexes: &[u64],
    decoder_map: &BTreeMap<u64, Arc<SqlServerRowDecoder>>,
    commit_lsn: Lsn,
    rewinds: &BTreeMap<u64, (Lsn, Lsn)>,
    data_output: &StackedAsyncOutputHandle<Lsn, (u64, Result<SourceMessage, DataflowError>)>,
    data_cap_set: &CapabilitySet<Lsn>,
    metrics: &SqlServerSourceMetrics,
    deferred_updates: &mut BTreeMap<(Lsn, Lsn), CdcOperation>,
) -> Result<(), TransientError> {
    // Events are emitted in LSN order for a given capture instance. If deferred_updates contains
    // LSNs that are less than the commit_lsn, it is a bug.
    if let Some(((deferred_lsn, _seqval), _row)) = deferred_updates.first_key_value()
        && *deferred_lsn < commit_lsn
    {
        panic!(
            "deferred update lsn {deferred_lsn} < commit lsn {commit_lsn}: {:?}",
            deferred_updates.keys()
        );
    }

    let mut mz_row = Row::default();
    let arena = RowArena::default();

    for change in changes {
        // deferred_update is only valid for single iteration of the loop.  It is set once both
        // old and new update rows are seen. It will be decoded and emitted to appropriate outputs.
        // Its life now fullfilled, it will return to whence it came.
        let mut deferred_update: Option<_> = None;
        let (sql_server_row, diff): (_, _) = match change {
            CdcOperation::Insert(sql_server_row) => {
                metrics.inserts.inc();
                (sql_server_row, Diff::ONE)
            }
            CdcOperation::Delete(sql_server_row) => {
                metrics.deletes.inc();
                (sql_server_row, Diff::MINUS_ONE)
            }

            // Updates are not ordered by seqval, so either old or new row could be observed first.
            // The first update row is stashed, when the second arrives, both are processed.
            CdcOperation::UpdateNew(seqval, sql_server_row) => {
                // arbitrarily choosing to update metrics on the the new row
                metrics.updates.inc();
                deferred_update = deferred_updates.remove(&(commit_lsn, seqval));
                if deferred_update.is_none() {
                    tracing::trace!("capture deferred UpdateNew ({commit_lsn}, {seqval})");
                    deferred_updates.insert(
                        (commit_lsn, seqval),
                        CdcOperation::UpdateNew(seqval, sql_server_row),
                    );
                    continue;
                }
                (sql_server_row, Diff::ONE)
            }
            CdcOperation::UpdateOld(seqval, sql_server_row) => {
                deferred_update = deferred_updates.remove(&(commit_lsn, seqval));
                if deferred_update.is_none() {
                    tracing::trace!("capture deferred UpdateOld ({commit_lsn}, {seqval})");
                    deferred_updates.insert(
                        (commit_lsn, seqval),
                        CdcOperation::UpdateOld(seqval, sql_server_row),
                    );
                    continue;
                }
                // The old row is emitted conditionally. This [`Diff`] is for the new row.
                (sql_server_row, Diff::ONE)
            }
        };

        // Try to decode the input row for each output.
        for partition_idx in partition_indexes {
            let decoder = decoder_map.get(partition_idx).unwrap();

            let rewind = rewinds.get(partition_idx);
            // We must continue here to avoid decoding and emitting. We don't have to compare with
            // snapshot_lsn as we are guaranteed that initial_lsn <= snapshot_lsn.
            if rewind.is_some_and(|(initial_lsn, _)| commit_lsn <= *initial_lsn) {
                continue;
            }

            let message = if let Some(ref deferred_update) = deferred_update {
                let (old_row, new_row) = match deferred_update {
                    CdcOperation::UpdateOld(_seqval, row) => (row, &sql_server_row),
                    CdcOperation::UpdateNew(_seqval, row) => (&sql_server_row, row),
                    CdcOperation::Insert(_) | CdcOperation::Delete(_) => unreachable!(),
                };

                let update_old = decode(decoder, old_row, &mut mz_row, &arena, Some(new_row));
                if rewind.is_some_and(|(_, snapshot_lsn)| commit_lsn <= *snapshot_lsn) {
                    data_output
                        .give_fueled(
                            &data_cap_set[0],
                            (
                                (*partition_idx, update_old.clone()),
                                Lsn::minimum(),
                                Diff::ONE,
                            ),
                        )
                        .await;
                }
                data_output
                    .give_fueled(
                        &data_cap_set[0],
                        ((*partition_idx, update_old), commit_lsn, Diff::MINUS_ONE),
                    )
                    .await;

                decode(decoder, new_row, &mut mz_row, &arena, None)
            } else {
                decode(decoder, &sql_server_row, &mut mz_row, &arena, None)
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
    Ok(())
}

type StackedAsyncOutputHandle<T, D> = AsyncOutputHandle<
    T,
    AccountedStackBuilder<CapacityContainerBuilder<TimelyStack<(D, T, Diff)>>>,
>;

/// Helper method to decode a row from a [`tiberius::Row`] (or 2 of them in the case of update)
/// to a [`Row`]. This centralizes the decode and mapping to result.
fn decode(
    decoder: &SqlServerRowDecoder,
    row: &tiberius::Row,
    mz_row: &mut Row,
    arena: &RowArena,
    new_row: Option<&tiberius::Row>,
) -> Result<SourceMessage, DataflowError> {
    match decoder.decode(row, mz_row, arena, new_row) {
        Ok(()) => Ok(SourceMessage {
            key: Row::default(),
            value: mz_row.clone(),
            metadata: Row::default(),
        }),
        Err(e) => {
            let kind = DecodeErrorKind::Text(e.to_string().into());
            // TODO(sql_server2): Get the raw bytes from `tiberius`.
            let raw = format!("{row:?}");
            Err(DataflowError::DecodeError(Box::new(DecodeError {
                kind,
                raw: raw.as_bytes().to_vec(),
            })))
        }
    }
}

/// Helper method to return a "definite" error upstream.
async fn return_definite_error(
    err: DefiniteError,
    outputs: impl Iterator<Item = u64>,
    data_handle: StackedAsyncOutputHandle<Lsn, (u64, Result<SourceMessage, DataflowError>)>,
    data_capset: &CapabilitySet<Lsn>,
    errs_handle: AsyncOutputHandle<Lsn, CapacityContainerBuilder<Vec<ReplicationError>>>,
    errs_capset: &CapabilitySet<Lsn>,
) {
    for output_idx in outputs {
        let update = (
            (output_idx, Err(err.clone().into())),
            // Select an LSN that should not conflict with a previously observed LSN.  Ideally
            // we could identify the LSN that resulted in the definite error so that all replicas
            // would emit the same updates for the same times.
            Lsn {
                vlf_id: u32::MAX,
                block_id: u32::MAX,
                record_id: u16::MAX,
            },
            Diff::ONE,
        );
        data_handle.give_fueled(&data_capset[0], update).await;
    }
    errs_handle.give(
        &errs_capset[0],
        ReplicationError::DefiniteError(Rc::new(err)),
    );
}

/// Provides an implemntation of [`SqlServerCdcMetrics`] that will update [`SqlServerSourceMetrics`]`
struct PrometheusSqlServerCdcMetrics<'a> {
    inner: &'a SqlServerSourceMetrics,
}

impl<'a> SqlServerCdcMetrics for PrometheusSqlServerCdcMetrics<'a> {
    fn snapshot_table_lock_start(&self, table_name: &str) {
        self.inner.update_snapshot_table_lock_count(table_name, 1);
    }

    fn snapshot_table_lock_end(&self, table_name: &str) {
        self.inner.update_snapshot_table_lock_count(table_name, -1);
    }
}
