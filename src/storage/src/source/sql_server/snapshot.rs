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
//! A leader worker locks every table to snapshot at once and broadcasts the snapshot [`Lsn`]
//! over a feedback edge. Each worker reads the tables of the capture instances it is responsible
//! for, in a transaction it begins while the leader holds the locks, so that every table is read
//! as of that one [`Lsn`]. The leader releases the locks once every worker has begun its
//! transaction. Each worker emits its rows at [`Lsn::minimum`] and sends a [`RewindRequest`] per
//! export it reads, which tells the replication operator to retract the changes the snapshot
//! already contains.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use differential_dataflow::AsCollection;
use futures::StreamExt;
use itertools::Itertools;
use mz_ore::cast::CastFrom;
use mz_ore::future::InTask;
use mz_repr::{Diff, GlobalId, Row, RowArena};
use mz_sql_server_util::cdc::{Lsn, SnapshotFence, SnapshotTransaction};
use mz_storage_types::dyncfgs::STORAGE_PERSIST_SINK_DESCRIPTION_LOOKAHEAD;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sources::SqlServerSourceConnection;
use mz_storage_types::sources::sql_server::{
    MAX_LSN_WAIT, SNAPSHOT_LOCK_WAIT_TIMEOUT, SNAPSHOT_PROGRESS_REPORT_INTERVAL,
};
use mz_timely_util::builder_async::{
    Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::vec::{Broadcast, Map};
use timely::dataflow::operators::{CapabilitySet, Concat, ConnectLoop, Feedback};
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

/// Used as a partition ID to determine the worker that locks the tables and takes the snapshot LSN.
static SNAPSHOT_LEADER: &str = "snapshot_leader";

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
    let mut builder = AsyncOperatorBuilder::new(op_name, scope.clone());

    let (feedback_handle, feedback_data) = scope.feedback(Default::default());

    // One data output port per source export, in output index order. With concurrent
    // replication each port is held at the minimum only while this operator still has snapshot
    // data to emit for that export. Without it all ports are held until the entire snapshot
    // completes. Every export then observes the same frontier.
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
    let (snapshot_handle, snapshot) = builder.new_output::<CapacityContainerBuilder<Vec<_>>>();

    // The leader broadcasts the snapshot LSN to this operator on every worker. The feedback input
    // has no connection to the outputs, which would otherwise close a loop.
    let mut snapshot_input = builder.new_disconnected_input(feedback_data, Pipeline);
    snapshot.broadcast().connect_loop(feedback_handle);

    let is_snapshot_leader = config.responsible_for(SNAPSHOT_LEADER);

    // Read at render time, matching the value the persist sink reads in the same dataflow build.
    // A read in the async body can observe an `UpdateConfiguration` sequenced after this dataflow's
    // creation and disagree with the sink because the `ConfigSet` is updated in place.
    // A nonzero lookahead is the switch, see `STORAGE_PERSIST_SINK_DESCRIPTION_LOOKAHEAD`.
    let concurrent_replication = !STORAGE_PERSIST_SINK_DESCRIPTION_LOOKAHEAD
        .get(config.config.config_set())
        .is_zero();

    let (button, transient_errors) = builder.build_fallible(move |caps| {
        let busy_signal = Arc::clone(&config.busy_signal);
        Box::pin(SignaledFuture::new(busy_signal, async move {
            let (data_cap_sets, caps) = caps.split_at_mut(export_count);
            let [rewind_cap_set, definite_error_cap_set, snapshot_cap_set]: &mut [_; 3] =
                caps.try_into().unwrap();
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

            // A worker *must* emit a count even if not responsible for snapshotting a table
            // as statistic summarization will return null if any worker hasn't set a value.
            // This will also reset snapshot stats for any exports not snapshotting.
            if capture_instance_to_snapshot.is_empty() {
                metrics.snapshot_table_count.set(0);
                return Ok::<_, TransientError>(());
            }
            for stats in config.statistics.values() {
                stats.set_snapshot_records_known(0);
                stats.set_snapshot_records_staged(0);
            }

            // Each worker reads the tables of the capture instances it is responsible for.
            let reader_instances: BTreeSet<Arc<str>> = capture_instance_to_snapshot
                .keys()
                .filter(|instance| config.responsible_for(&***instance))
                .cloned()
                .collect();
            let reader_outputs: BTreeSet<usize> = reader_instances
                .iter()
                .flat_map(|instance| &capture_instance_to_snapshot[instance])
                .map(|(output_index, _)| *output_index)
                .collect();
            metrics
                .snapshot_table_count
                .set(u64::cast_from(reader_instances.len()));

            // This operator only emits snapshot data for the exports it snapshots. With
            // concurrent replication the ports of all other exports close up front. Their
            // frontiers then follow the replication operator alone. The leader keeps the port of
            // every snapshotted export until it has broadcast the snapshot LSN, since it reports
            // a definite error on all of them.
            if concurrent_replication {
                let snapshotting: BTreeSet<usize> = capture_instance_to_snapshot
                    .values()
                    .flatten()
                    .map(|(output_index, _)| *output_index)
                    .collect();
                let keep = if is_snapshot_leader {
                    &snapshotting
                } else {
                    &reader_outputs
                };
                release_data_ports(data_cap_sets, keep);
            }

            // Returning releases this worker's snapshot capability, which the leader waits on
            // before releasing the table locks.
            if reader_instances.is_empty() && !is_snapshot_leader {
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

            // These queries run before the leader locks the tables, which keeps them out of the
            // window in which upstream writers are blocked.
            // TODO (maz): we can avoid this query by using SourceOutputInfo
            let reader_tables = mz_sql_server_util::inspect::get_tables_for_capture_instance(
                &mut client,
                reader_instances.iter().map(|i| i.as_ref()),
            )
            .await?;

            // We first calculate all the total rows we need to fetch across all tables. Since this
            // happens outside the snapshot transaction the totals might be off, so we won't assert
            // that we get exactly this many rows later.
            for table in &reader_tables {
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

            // The leader locks every table that any worker snapshots and broadcasts the
            // snapshot LSN. A transient error on the leader leaves the other workers waiting for
            // the broadcast, with their capabilities held, until the dataflow restarts.
            let mut fence_client = None;
            let mut fence = None;
            if is_snapshot_leader {
                if let Some(definite_error) =
                    validate_upstream(&mut client, &source, &config).await?
                {
                    return_definite_error(
                        definite_error,
                        capture_instance_to_snapshot
                            .values()
                            .flatten()
                            .map(|(output_index, _)| *output_index),
                        &mut data_outputs,
                        data_cap_sets,
                        &definite_error_handle,
                        definite_error_cap_set,
                    )
                    .await;
                    snapshot_handle.give(&snapshot_cap_set[0], None);
                    return Ok(());
                }

                let snapshot_tables = mz_sql_server_util::inspect::get_tables_for_capture_instance(
                    &mut client,
                    capture_instance_to_snapshot.keys().map(|i| i.as_ref()),
                )
                .await?;

                // Before locking the tables, ensure the upstream DB is ready for CDC.
                client
                    .cdc(capture_instance_to_snapshot.keys().cloned())
                    .max_lsn_wait(MAX_LSN_WAIT.get(config.config.config_set()))
                    .wait_for_ready()
                    .await?;

                // Intentionally logging this at info for debugging. This section won't get
                // entered often, but if there are problems here, it will be much easier to
                // troubleshoot knowing where stall/hang might be happening.
                tracing::info!(%config.worker_id, "timely-{worker_id} upstream is ready");

                let fence_client = fence_client.insert(client.new_connection().await?);
                let locked = fence.insert(
                    SnapshotFence::acquire(
                        fence_client,
                        &snapshot_tables,
                        SNAPSHOT_LOCK_WAIT_TIMEOUT.get(config.config.config_set()),
                        PrometheusSqlServerCdcMetrics { inner: &metrics },
                    )
                    .await?,
                );
                let snapshot_lsn = locked.lsn();
                for (_, initial_lsn) in capture_instance_to_snapshot.values().flatten() {
                    // An initial_lsn ahead of the snapshot_lsn would mean that we observed a
                    // SQL Server DB that appeared to go back in time.
                    assert!(
                        *initial_lsn <= snapshot_lsn,
                        "initial_lsn={initial_lsn} snapshot_lsn={snapshot_lsn}"
                    );
                }
                tracing::info!(%config.id, %snapshot_lsn, "timely-{worker_id} locked tables");
                snapshot_handle.give(&snapshot_cap_set[0], Some(snapshot_lsn));
            }

            // `None` means that the leader reported a definite error.
            let snapshot_lsn = loop {
                match snapshot_input.next().await {
                    Some(AsyncEvent::Data(_, mut data)) => {
                        if let Some(msg) = data.pop() {
                            break msg;
                        }
                    }
                    Some(AsyncEvent::Progress(_)) => continue,
                    None => break None,
                }
            };
            let Some(snapshot_lsn) = snapshot_lsn else {
                return Ok(());
            };
            if concurrent_replication {
                release_data_ports(data_cap_sets, &reader_outputs);
            }

            let snapshot_txn = if reader_tables.is_empty() {
                None
            } else {
                Some(SnapshotTransaction::begin(&mut client).await?)
            };
            // Dropping the snapshot capability tells the leader that this worker's view is
            // fixed. The leader releases the locks once every worker has done so.
            *snapshot_cap_set = CapabilitySet::new();
            if let Some(fence) = fence.take() {
                while snapshot_input.next().await.is_some() {}
                fence.release().await?;
            }
            drop(fence);
            drop(fence_client);

            let mut rewind_requests: Vec<_> = reader_outputs
                .iter()
                .map(|output_index| RewindRequest {
                    output_index: *output_index,
                    snapshot_lsn,
                })
                .collect();

            let emit_rewinds =
                |requests: Vec<RewindRequest>, rewind_cap_set: &mut CapabilitySet<Lsn>| {
                    for request in requests {
                        rewinds_handle.give(&rewind_cap_set[0], request);
                    }
                    *rewind_cap_set = CapabilitySet::new();
                };

            // The rewind requests unblock the replication operator. With concurrent replication
            // they go out now, before any table is read. The replication operator then reads the
            // CDC stream while the snapshot runs and stages its data in the dataflow until the
            // snapshot completes. Without concurrent replication they go out after the snapshot.
            // That keeps the two phases serial and avoids the staging cost.
            if concurrent_replication {
                emit_rewinds(std::mem::take(&mut rewind_requests), rewind_cap_set);
            }

            let Some(mut snapshot_txn) = snapshot_txn else {
                return Ok(());
            };

            let report_interval =
                SNAPSHOT_PROGRESS_REPORT_INTERVAL.handle(config.config.config_set());
            let mut last_report = Instant::now();

            for table in &reader_tables {
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

                sql_server_snapshot_failpoint_pause(&table.name).await;

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
                // A transient error above returns with the capabilities still held. That keeps a
                // partially copied export from advancing.
                if concurrent_replication {
                    for (output_index, _) in output_indexes {
                        data_cap_sets[*output_index] = CapabilitySet::new();
                    }
                }
            }
            snapshot_txn.rollback().await?;

            if !concurrent_replication {
                emit_rewinds(rewind_requests, rewind_cap_set);
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

/// Test-only pause point, run before the operator reads any rows of `table`. Configuring the
/// failpoint as `sql_server_snapshot_pause=return(<table name>)` holds the snapshot of that table
/// until the failpoint is deactivated. `return(<table name>:<millis>)` holds it for that many
/// milliseconds instead. The snapshot then completes in the same dataflow incarnation.
async fn sql_server_snapshot_failpoint_pause(table: &str) {
    let held_since = Instant::now();
    loop {
        let held = fail::eval("sql_server_snapshot_pause", |payload| {
            let (paused_table, millis) = payload
                .as_deref()
                .map(|p| p.split_once(':').unwrap_or((p, "")))
                .unwrap_or_default();
            paused_table == table
                && millis.parse::<u64>().map_or(true, |millis| {
                    held_since.elapsed() < Duration::from_millis(millis)
                })
        });
        match held {
            Some(true) => tokio::time::sleep(Duration::from_millis(100)).await,
            _ => break,
        }
    }
}

/// Drops the capabilities of every data port whose output index is not in `keep`.
fn release_data_ports(data_cap_sets: &mut [CapabilitySet<Lsn>], keep: &BTreeSet<usize>) {
    for (output_index, cap_set) in data_cap_sets.iter_mut().enumerate() {
        if !keep.contains(&output_index) {
            *cap_set = CapabilitySet::new();
        }
    }
}
