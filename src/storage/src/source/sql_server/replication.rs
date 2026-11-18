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
use std::sync::Arc;

use differential_dataflow::AsCollection;
use futures::StreamExt;
use mz_ore::cast::CastFrom;
use mz_ore::collections::HashMap;
use mz_ore::future::InTask;
use mz_repr::{Diff, GlobalId, Row, RowArena};
use mz_sql_server_util::cdc::{CdcEvent, Lsn, Operation as CdcOperation};
use mz_sql_server_util::desc::SqlServerRowDecoder;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sources::SqlServerSourceConnection;
use mz_storage_types::sources::sql_server::MAX_LSN_WAIT;
use mz_timely_util::builder_async::{
    Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};
use serde::{Deserialize, Serialize};
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::vec::Map;
use timely::dataflow::operators::{CapabilitySet, Concat};
use timely::dataflow::{Scope, StreamVec};
use timely::progress::Timestamp;

use crate::metrics::source::sql_server::SqlServerSourceMetrics;
use crate::source::RawSourceCreationConfig;
use crate::source::sql_server::{
    DataOutputs, DefiniteError, PrometheusSqlServerCdcMetrics, ReplicationError, SourceOutputInfo,
    TransientError, decode, return_definite_error, validate_upstream,
};
use crate::source::types::{SharedFuel, SignaledFuture, SourceMessage, StackedCollection};

/// Used as a partition ID to determine the worker that is responsible for
/// reading data from SQL Server.
///
/// TODO(sql_server2): It's possible we could have different workers
/// replicate different tables, if we're using SQL Server's CDC features.
static REPL_READER: &str = "reader";

/// Asks the replication operator to retract the changes of an export that its snapshot already
/// contains.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RewindRequest {
    /// The output index that should be rewound.
    pub(crate) output_index: usize,
    /// The LSN that the snapshot was taken at.
    pub(crate) snapshot_lsn: Lsn,
}

pub(crate) fn render<'scope>(
    scope: Scope<'scope, Lsn>,
    config: RawSourceCreationConfig,
    outputs: BTreeMap<GlobalId, SourceOutputInfo>,
    source: SqlServerSourceConnection,
    rewind_stream: StreamVec<'scope, Lsn, RewindRequest>,
    metrics: SqlServerSourceMetrics,
) -> (
    Vec<StackedCollection<'scope, Lsn, Result<SourceMessage, DataflowError>>>,
    StreamVec<'scope, Lsn, ReplicationError>,
    PressOnDropButton,
) {
    let op_name = format!("SqlServerReplicationReader({})", config.id);
    let mut builder = AsyncOperatorBuilder::new(op_name, scope);

    // One data output port per source export, in output index order. All ports advance in
    // lockstep. Every export observes the same frontier.
    let export_count = config.source_exports.len();
    let mut data_outputs = Vec::with_capacity(export_count);
    let mut data_streams = Vec::with_capacity(export_count);
    for _ in 0..export_count {
        let (output, stream) = builder.new_output::<CapacityContainerBuilder<Vec<_>>>();
        data_outputs.push(output);
        data_streams.push(stream);
    }

    // Captures DefiniteErrors that affect the entire source, including all outputs
    let (definite_error_handle, definite_errors) =
        builder.new_output::<CapacityContainerBuilder<_>>();

    // The operator collects every rewind request before it streams any change, so the rewind
    // input needs no connection to the data ports.
    let repl_reader_id = u64::cast_from(config.responsible_worker(REPL_READER));
    let mut rewind_input =
        builder.new_disconnected_input(rewind_stream, Exchange::new(move |_| repl_reader_id));

    let (button, transient_errors) = builder.build_fallible(move |caps| {
        let busy_signal = Arc::clone(&config.busy_signal);
        Box::pin(SignaledFuture::new(busy_signal, async move {
            let (data_cap_sets, caps) = caps.split_at_mut(export_count);
            let [definite_error_cap_set]: &mut [_; 1] = caps.try_into().unwrap();
            let mut data_outputs = SharedFuel::new(data_outputs);

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
            // Maps the 'capture instance' to the output index for all outputs of this worker
            let mut capture_instances: BTreeMap<Arc<str>, Vec<_>> = BTreeMap::new();
            // Maps the included columns for each output index so we can check
            // whether schema updates are valid on a per-output basis
            let mut included_columns: HashMap<usize, Vec<Arc<str>>> = HashMap::new();
            let mut initial_lsns = BTreeMap::new();

            for output in outputs.values() {
                let key = output.output_index;
                if decoder_map.insert(key, Arc::clone(&output.decoder)).is_some() {
                    panic!("Multiple decoders for output index {}", output.output_index);
                }
                // Collect the included columns from decoder for schema
                // change validation. The decoder serves as an effective
                // source of truth for which columns are "included", as we
                // only care about the columns that are being decoded and
                // replicated
                let included_cols = output.decoder.included_column_names();
                included_columns.insert(output.output_index, included_cols);

                capture_instances
                    .entry(Arc::clone(&output.capture_instance))
                    .or_default()
                    .push(output.output_index);
                initial_lsns.insert(output.output_index, output.initial_lsn);
            }

            if let Some(definite_error) = validate_upstream(&mut client, &source, &config).await? {
                tracing::warn!(?definite_error, "Restore detected, exiting");
                return_definite_error(
                    definite_error,
                    capture_instances.values().flat_map(|indexes| indexes.iter().copied()),
                    &mut data_outputs,
                    data_cap_sets,
                    &definite_error_handle,
                    definite_error_cap_set,
                )
                .await;
                return Ok(());
            }

            let cdc_metrics = PrometheusSqlServerCdcMetrics{inner: &metrics};
            let mut cdc_handle = client
                .cdc(capture_instances.keys().cloned(), cdc_metrics)
                .max_lsn_wait(MAX_LSN_WAIT.get(config.config.config_set()));
            cdc_handle.wait_for_ready().await?;

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
            let mut rewinds = BTreeMap::new();
            while let Some(event) = rewind_input.next().await {
                if let AsyncEvent::Data(_, data) = event {
                    for RewindRequest { output_index, snapshot_lsn } in data {
                        let initial_lsn = initial_lsns[&output_index];
                        rewinds.insert(output_index, (initial_lsn, snapshot_lsn));
                    }
                }
            }
            tracing::debug!("rewinds to process: {rewinds:?}");

            // Resumption point is the minimum LSN that has been observed per capture instance.
            let mut resume_lsns = BTreeMap::new();
            for src_info in outputs.values() {
                // initial_lsn is the max lsn observed, but the resume lsn
                // is the next lsn that should be read.  After a snapshot, initial_lsn
                // has been read, so replication will start at the next available lsn.
                let resume_lsn = src_info.resume_lsn_or(src_info.initial_lsn.increment());
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
                .poll_interval(config.timestamp_interval)
                .into_stream();
            let mut cdc_stream = std::pin::pin!(cdc_stream);

            let mut errored_outputs = BTreeSet::new();

            // TODO(sql_server2): We should emit `ProgressStatisticsUpdate::SteadyState` messages
            // here, when we receive progress events. What stops us from doing this now is our
            // 10-byte LSN doesn't fit into the 8-byte integer that the progress event uses.
            let mut log_rewinds_complete = true;

            // deferred_updates temporarily stores rows for UPDATE operation to support Large Object
            // (LOB) types (i.e. varchar(max), nvarchar(max)). The value of a
            // LOB column will be NULL for the old row (operation = 3) if the value of the
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
                            for cap_set in data_cap_sets.iter_mut() {
                                cap_set.downgrade([next_lsn]);
                            }
                        } else {
                            tracing::debug!("rewinds remaining: {:?}", rewinds);
                        }

                        // Events are emitted in LSN order for a given capture instance, if any
                        // deferred updates remain when the LSN progresses, it is a bug.
                        if let Some(((deferred_lsn, _seqval), _row)) =
                            deferred_updates.first_key_value()
                            && *deferred_lsn < next_lsn
                        {
                            panic!(
                                "deferred update lsn {deferred_lsn} \
                                 < progress lsn {next_lsn}: {:?}",
                                deferred_updates.keys()
                            );
                        }

                    }
                    // We've got new data! Let's process it.
                    CdcEvent::Data {
                        capture_instance,
                        lsn,
                        changes,
                    } => {
                        let Some(output_indexes) =
                            capture_instances.get(&capture_instance)
                        else {
                            let definite_error =
                                DefiniteError::ProgrammingError(format!(
                                    "capture instance didn't exist: \
                                     '{capture_instance}'"
                                ));
                            return_definite_error(
                                definite_error,
                                capture_instances
                                    .values()
                                    .flat_map(|indexes| {
                                        indexes.iter().copied()
                                    }),
                                &mut data_outputs,
                                data_cap_sets,
                                &definite_error_handle,
                                definite_error_cap_set,
                            )
                            .await;
                            return Ok(());
                        };

                        let (valid_outputs, err_outputs) =
                            output_indexes
                                .iter()
                                .partition::<Vec<usize>, _>(
                                    |&output_index| {
                                        !errored_outputs
                                            .contains(output_index)
                                    },
                                );

                        if err_outputs.len() > 0 {
                            metrics.ignored.inc_by(u64::cast_from(changes.len()));
                        }

                        handle_data_event(
                            changes,
                            &valid_outputs,
                            &decoder_map,
                            lsn,
                            &rewinds,
                            &mut data_outputs,
                            data_cap_sets,
                            &metrics,
                            &mut deferred_updates,
                        ).await?
                    },
                    CdcEvent::SchemaUpdate {
                        capture_instance,
                        table,
                        ddl_event,
                    } => {
                        let Some(output_indexes) =
                            capture_instances.get(&capture_instance)
                        else {
                            let definite_error =
                                DefiniteError::ProgrammingError(format!(
                                    "capture instance didn't exist: \
                                     '{capture_instance}'"
                                ));
                            return_definite_error(
                                definite_error,
                                capture_instances
                                    .values()
                                    .flat_map(|indexes| {
                                        indexes.iter().copied()
                                    }),
                                &mut data_outputs,
                                data_cap_sets,
                                &definite_error_handle,
                                definite_error_cap_set,
                            )
                            .await;
                            return Ok(());
                        };
                        let error =
                            DefiniteError::IncompatibleSchemaChange(
                                capture_instance.to_string(),
                                table.to_string(),
                            );
                        for output_index in output_indexes {
                            let cols = included_columns
                                .get(output_index)
                                .unwrap_or_else(|| {
                                    panic!(
                                        "Output index didn't \
                                         exist: '{output_index}'"
                                    )
                                });
                            if !errored_outputs
                                .contains(output_index)
                                && !ddl_event.is_compatible(cols)
                            {
                                let update = (
                                    Err(error.clone().into()),
                                    ddl_event.lsn,
                                    Diff::ONE,
                                );
                                data_outputs
                                    .give(
                                        *output_index,
                                        &data_cap_sets[*output_index][0],
                                        update,
                                    )
                                    .await;
                                errored_outputs.insert(*output_index);
                            }
                        }
                    }
                };
            }
            Err(TransientError::ReplicationEOF)
        }))
    });

    let error_stream = definite_errors.concat(transient_errors.map(ReplicationError::Transient));

    let data_collections = data_streams
        .into_iter()
        .map(|stream| stream.as_collection())
        .collect();

    (data_collections, error_stream, button.press_on_drop())
}

async fn handle_data_event(
    changes: Vec<CdcOperation>,
    output_indexes: &[usize],
    decoder_map: &BTreeMap<usize, Arc<SqlServerRowDecoder>>,
    commit_lsn: Lsn,
    rewinds: &BTreeMap<usize, (Lsn, Lsn)>,
    data_outputs: &mut DataOutputs,
    data_cap_sets: &[CapabilitySet<Lsn>],
    metrics: &SqlServerSourceMetrics,
    deferred_updates: &mut BTreeMap<(Lsn, Lsn), CdcOperation>,
) -> Result<(), TransientError> {
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
                // this is overriden below when the updates are ordered
                (sql_server_row, Diff::ZERO)
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
                // this is overriden below when the updates are ordered
                (sql_server_row, Diff::ZERO)
            }
        };

        // Try to decode the input row for each output.
        for &output_index in output_indexes {
            let decoder = decoder_map.get(&output_index).unwrap();
            let cap = &data_cap_sets[output_index][0];

            let rewind = rewinds.get(&output_index);
            // We must continue here to avoid decoding and emitting. We don't have to compare with
            // snapshot_lsn as we are guaranteed that initial_lsn <= snapshot_lsn.
            if rewind.is_some_and(|(initial_lsn, _)| commit_lsn <= *initial_lsn) {
                continue;
            }

            let (message, diff) = if let Some(ref deferred_update) = deferred_update {
                let (old_row, new_row) = match deferred_update {
                    CdcOperation::UpdateOld(_seqval, row) => (row, &sql_server_row),
                    CdcOperation::UpdateNew(_seqval, row) => (&sql_server_row, row),
                    CdcOperation::Insert(_) | CdcOperation::Delete(_) => unreachable!(),
                };

                let update_old = decode(decoder, old_row, &mut mz_row, &arena, Some(new_row));
                if rewind.is_some_and(|(_, snapshot_lsn)| commit_lsn <= *snapshot_lsn) {
                    let update = (update_old.clone(), Lsn::minimum(), Diff::ONE);
                    data_outputs.give(output_index, cap, update).await;
                }
                let update = (update_old, commit_lsn, Diff::MINUS_ONE);
                data_outputs.give(output_index, cap, update).await;

                (
                    decode(decoder, new_row, &mut mz_row, &arena, None),
                    Diff::ONE,
                )
            } else {
                (
                    decode(decoder, &sql_server_row, &mut mz_row, &arena, None),
                    diff,
                )
            };
            assert_ne!(Diff::ZERO, diff);
            if rewind.is_some_and(|(_, snapshot_lsn)| commit_lsn <= *snapshot_lsn) {
                let update = (message.clone(), Lsn::minimum(), -diff);
                data_outputs.give(output_index, cap, update).await;
            }
            let update = (message, commit_lsn, diff);
            data_outputs.give(output_index, cap, update).await;
        }
    }
    Ok(())
}
