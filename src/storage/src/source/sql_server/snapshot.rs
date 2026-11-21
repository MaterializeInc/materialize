// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Renders the operator that copies the initial snapshot of SQL Server tables.
//!
//! The operator locks every table it snapshots at once, so that all of them are read at one
//! snapshot [`Lsn`]. It emits the rows at [`Lsn::minimum`] and then sends a [`RewindRequest`] per
//! export, which tells the replication operator to retract the changes the snapshot already
//! contains.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use differential_dataflow::AsCollection;
use futures::StreamExt;
use itertools::Itertools;
use mz_ore::cast::CastFrom;
use mz_ore::future::InTask;
use mz_repr::{Diff, GlobalId, Row, RowArena};
use mz_sql_server_util::cdc::{Lsn, SnapshotFence, SnapshotTransaction};
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sources::SqlServerSourceConnection;
use mz_storage_types::sources::sql_server::{
    MAX_LSN_WAIT, SNAPSHOT_LOCK_WAIT_TIMEOUT, SNAPSHOT_PROGRESS_REPORT_INTERVAL,
};
use mz_timely_util::builder_async::{OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton};
use timely::container::CapacityContainerBuilder;
use timely::dataflow::operators::Concat;
use timely::dataflow::operators::vec::Map;
use timely::dataflow::{Scope, StreamVec};
use timely::progress::Timestamp;

use crate::metrics::source::sql_server::SqlServerSourceMetrics;
use crate::source::RawSourceCreationConfig;
use crate::source::sql_server::replication::RewindRequest;
use crate::source::sql_server::{
    PrometheusSqlServerCdcMetrics, ReplicationError, SourceOutputInfo, TransientError, decode,
    return_definite_error, validate_upstream,
};
use crate::source::types::{SharedFuel, SignaledFuture, SourceMessage, StackedCollection};

/// Used as a partition ID to determine the worker that is responsible for snapshotting.
static SNAPSHOT_READER: &str = "snapshot_reader";

pub(crate) fn render<'scope>(
    scope: Scope<'scope, Lsn>,
    config: RawSourceCreationConfig,
    outputs: BTreeMap<GlobalId, SourceOutputInfo>,
    source: SqlServerSourceConnection,
    metrics: SqlServerSourceMetrics,
) -> (
    Vec<StackedCollection<'scope, Lsn, Result<SourceMessage, DataflowError>>>,
    StreamVec<'scope, Lsn, RewindRequest>,
    StreamVec<'scope, Lsn, ReplicationError>,
    PressOnDropButton,
) {
    let op_name = format!("SqlServerSnapshotReader({})", config.id);
    let mut builder = AsyncOperatorBuilder::new(op_name, scope);

    // One data output port per source export, in output index order. Every port holds the
    // minimum capability until the entire snapshot completes.
    let export_count = config.source_exports.len();
    let mut data_outputs = Vec::with_capacity(export_count);
    let mut data_streams = Vec::with_capacity(export_count);
    for _ in 0..export_count {
        let (output, stream) = builder.new_output::<CapacityContainerBuilder<Vec<_>>>();
        data_outputs.push(output);
        data_streams.push(stream);
    }
    let (rewinds_handle, rewinds) = builder.new_output::<CapacityContainerBuilder<Vec<_>>>();
    let (definite_error_handle, definite_errors) =
        builder.new_output::<CapacityContainerBuilder<Vec<_>>>();

    let (button, transient_errors) = builder.build_fallible(move |caps| {
        let busy_signal = Arc::clone(&config.busy_signal);
        Box::pin(SignaledFuture::new(busy_signal, async move {
            let (data_cap_sets, caps) = caps.split_at_mut(export_count);
            let [rewind_cap_set, definite_error_cap_set]: &mut [_; 2] = caps.try_into().unwrap();
            let mut data_outputs = SharedFuel::new(data_outputs);
            let worker_id = config.worker_id;

            // The decoder is specific to the export, and each export pulls data from a specific
            // capture instance.
            let mut decoder_map: BTreeMap<_, _> = BTreeMap::new();
            // Maps the 'capture instance' to the outputs that need a snapshot, with the
            // initial LSN of each.
            let mut capture_instance_to_snapshot: BTreeMap<Arc<str>, Vec<_>> = BTreeMap::new();
            // Export statistics for a given capture instance
            let mut export_statistics: BTreeMap<_, Vec<_>> = BTreeMap::new();
            for (export_id, output) in outputs.iter() {
                if *output.resume_upper == [Lsn::minimum()] {
                    decoder_map.insert(output.output_index, Arc::clone(&output.decoder));
                    capture_instance_to_snapshot
                        .entry(Arc::clone(&output.capture_instance))
                        .or_default()
                        .push((output.output_index, output.initial_lsn));
                }
                export_statistics
                    .entry(Arc::clone(&output.capture_instance))
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
            metrics
                .snapshot_table_count
                .set(u64::cast_from(capture_instance_to_snapshot.len()));
            if capture_instance_to_snapshot.is_empty() {
                return Ok::<_, TransientError>(());
            }
            for stats in config.statistics.values() {
                stats.set_snapshot_records_known(0);
                stats.set_snapshot_records_staged(0);
            }
            if !config.responsible_for(SNAPSHOT_READER) {
                return Ok(());
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

            if let Some(definite_error) = validate_upstream(&mut client, &source, &config).await? {
                return_definite_error(
                    definite_error,
                    capture_instance_to_snapshot
                        .values()
                        .flat_map(|outputs| outputs.iter().map(|(output_index, _)| *output_index)),
                    &mut data_outputs,
                    data_cap_sets,
                    &definite_error_handle,
                    definite_error_cap_set,
                )
                .await;
                return Ok(());
            }

            // TODO (maz): we can avoid this query by using SourceOutputInfo
            let snapshot_tables = mz_sql_server_util::inspect::get_tables_for_capture_instance(
                &mut client,
                capture_instance_to_snapshot.keys().map(|i| i.as_ref()),
            )
            .await?;

            // We first calculate all the total rows we need to fetch across all tables. Since this
            // happens outside the snapshot transaction the totals might be off, so we won't assert
            // that we get exactly this many rows later.
            for table in &snapshot_tables {
                let qualified_table_name = format!(
                    "{schema_name}.{table_name}",
                    schema_name = table.schema_name,
                    table_name = table.name
                );
                let size_calc_start = Instant::now();
                let table_total = mz_sql_server_util::inspect::snapshot_size(
                    &mut client,
                    &table.schema_name,
                    &table.name,
                )
                .await?;
                metrics.set_snapshot_table_size_latency(
                    &qualified_table_name,
                    size_calc_start.elapsed().as_secs_f64(),
                );
                for export_stat in export_statistics.get(&table.capture_instance.name).unwrap() {
                    export_stat.set_snapshot_records_known(u64::cast_from(table_total));
                    export_stat.set_snapshot_records_staged(0);
                }
            }

            // Before locking the tables, ensure the upstream DB is ready for CDC.
            client
                .cdc(capture_instance_to_snapshot.keys().cloned())
                .max_lsn_wait(MAX_LSN_WAIT.get(config.config.config_set()))
                .wait_for_ready()
                .await?;

            // Intentionally logging this at info for debugging. This section won't get entered
            // often, but if there are problems here, it will be much easier to troubleshoot
            // knowing where stall/hang might be happening.
            tracing::info!(%config.worker_id, "timely-{worker_id} upstream is ready");

            let mut fence_client = client.new_connection().await?;
            let fence = SnapshotFence::acquire(
                &mut fence_client,
                &snapshot_tables,
                SNAPSHOT_LOCK_WAIT_TIMEOUT.get(config.config.config_set()),
                PrometheusSqlServerCdcMetrics { inner: &metrics },
            )
            .await?;
            let snapshot_lsn = fence.lsn();
            tracing::info!(%config.id, %snapshot_lsn, "timely-{worker_id} locked tables");

            let mut rewind_requests = Vec::new();
            for (output_index, initial_lsn) in capture_instance_to_snapshot.values().flatten() {
                // An initial_lsn ahead of the snapshot_lsn would mean that we observed a
                // SQL Server DB that appeared to go back in time.
                assert!(
                    *initial_lsn <= snapshot_lsn,
                    "initial_lsn={initial_lsn} snapshot_lsn={snapshot_lsn}"
                );
                rewind_requests.push(RewindRequest {
                    output_index: *output_index,
                    snapshot_lsn,
                });
            }

            let mut snapshot_txn = SnapshotTransaction::begin(&mut client).await?;
            fence.release().await?;

            let report_interval =
                SNAPSHOT_PROGRESS_REPORT_INTERVAL.handle(config.config.config_set());
            let mut last_report = Instant::now();

            for table in &snapshot_tables {
                tracing::info!(
                    %config.id,
                    %table.name,
                    %table.schema_name,
                    %snapshot_lsn,
                    "timely-{worker_id} snapshot start",
                );

                let ci_name = &table.capture_instance.name;
                let output_indexes =
                    capture_instance_to_snapshot
                        .get(ci_name)
                        .unwrap_or_else(|| {
                            panic!(
                                "no snapshot outputs in known capture \
                             instances [{}] for capture instance: \
                             '{}'",
                                capture_instance_to_snapshot.keys().join(","),
                                ci_name,
                            );
                        });

                // TODO(sql_server3): filter columns to only select columns required for Source.
                let mut snapshot = std::pin::pin!(snapshot_txn.read_table(table));
                let mut snapshot_staged = 0;
                while let Some(result) = snapshot.next().await {
                    let sql_server_row = result.map_err(TransientError::from)?;

                    if last_report.elapsed() > report_interval.get() {
                        last_report = Instant::now();
                        let stats = export_statistics.get(ci_name).unwrap();
                        for export_stat in stats {
                            export_stat.set_snapshot_records_staged(snapshot_staged);
                        }
                    }

                    for (output_index, _) in output_indexes {
                        // Decode the SQL Server row into an MZ one.
                        let mut mz_row = Row::default();
                        let arena = RowArena::default();

                        let decoder = decoder_map.get(output_index).expect("decoder for output");
                        // Try to decode a row, returning a SourceError
                        // if it fails.
                        let message = decode(decoder, &sql_server_row, &mut mz_row, &arena, None);
                        data_outputs
                            .give(
                                *output_index,
                                &data_cap_sets[*output_index][0],
                                (message, Lsn::minimum(), Diff::ONE),
                            )
                            .await;
                    }
                    snapshot_staged += 1;
                }

                tracing::info!(
                    %config.id,
                    %table.name,
                    %table.schema_name,
                    %snapshot_lsn,
                    "timely-{worker_id} snapshot complete",
                );
                metrics.snapshot_table_count.dec();
                // final update for snapshot_staged, using the staged
                // values as the total is an estimate
                let stats = export_statistics.get(ci_name).unwrap();
                for export_stat in stats {
                    export_stat.set_snapshot_records_staged(snapshot_staged);
                    export_stat.set_snapshot_records_known(snapshot_staged);
                }
            }
            snapshot_txn.rollback().await?;

            for request in rewind_requests {
                rewinds_handle.give(&rewind_cap_set[0], request);
            }

            Ok(())
        }))
    });

    let data_collections = data_streams
        .into_iter()
        .map(|stream| stream.as_collection())
        .collect();

    (
        data_collections,
        rewinds,
        definite_errors.concat(transient_errors.map(ReplicationError::Transient)),
        button.press_on_drop(),
    )
}
