// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Code to render the ingestion dataflow of a [`PostgresSourceConnection`]. The dataflow consists
//! of multiple operators in order to take advantage of all the available workers.
//!
//! # Snapshot
//!
//! One part of the dataflow deals with snapshotting the tables involved in the ingestion. Each
//! table is partitioned across all workers using PostgreSQL's `ctid` column to identify row
//! ranges. Each worker fetches its assigned range using a `COPY` query with ctid filtering,
//! enabling parallel snapshotting of large tables.
//!
//! For all tables that ended up being snapshotted the snapshot reader also emits a rewind request
//! to the replication reader which will ensure that the requested portion of the replication
//! stream is subtracted from the snapshot.
//!
//! See the [snapshot] module for more information on the snapshot strategy.
//!
//! # Replication
//!
//! The other part of the dataflow deals with reading the logical replication slot, which must
//! happen from a single worker. The minimum amount of processing is performed from that worker
//! and the data is then distributed among all workers for decoding.
//!
//! See the [replication] module for more information on the replication strategy.
//!
//! # Error handling
//!
//! There are two kinds of errors that can happen during ingestion that are represented as two
//! separate error types:
//!
//! [`DefiniteError`]s are errors that happen during processing of a specific
//! collection record at a specific LSN. These are the only errors that can ever end up in the
//! error collection of a subsource.
//!
//! Transient errors are any errors that can happen for reasons that are unrelated to the data
//! itself. This could be authentication failures, connection failures, etc. The only operators
//! that can emit such errors are the `TableReader` and the `ReplicationReader` operators, which
//! are the ones that talk to the external world. Both of these operators are built with the
//! `AsyncOperatorBuilder::build_fallible` method which allows transient errors to be propagated
//! upwards with the standard `?` operator without risking downgrading the capability and producing
//! bogus frontiers.
//!
//! The error streams from both of those operators are published to the source status and also
//! trigger a restart of the dataflow.
//!
//! ```text
//!    ┏━━━━━━━━━━━━━━┓
//!    ┃    table     ┃
//!    ┃    reader    ┃
//!    ┗━┯━━━━━━━━━━┯━┛
//!      │          │rewind
//!      │          │requests
//!      │          ╰────╮
//!      │             ┏━v━━━━━━━━━━━┓
//!      │             ┃ replication ┃
//!      │             ┃   reader    ┃
//!      │             ┗━┯━━━━━━━━━┯━┛
//!  COPY│           slot│         │
//!  data│           data│         │
//! ┏━━━━v━━━━━┓ ┏━━━━━━━v━━━━━┓   │
//! ┃  COPY    ┃ ┃ replication ┃   │
//! ┃ decoder  ┃ ┃   decoder   ┃   │
//! ┗━━━━┯━━━━━┛ ┗━━━━━┯━━━━━━━┛   │
//!      │snapshot     │replication│
//!      │updates      │updates    │
//!      ╰────╮    ╭───╯           │
//!          ╭┴────┴╮              │
//!          │concat│              │
//!          ╰──┬───╯              │
//!             │ data             │progress
//!             │ output           │output
//!             v                  v
//! ```

use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::Duration;

use differential_dataflow::AsCollection;
use itertools::Itertools as _;
use mz_expr::{EvalError, MirScalarExpr};
use mz_ore::cast::CastFrom;
use mz_ore::error::ErrorExt;
use mz_postgres_util::desc::PostgresTableDesc;
use mz_postgres_util::{Client, PostgresError, simple_query_opt};
use mz_repr::{Datum, Diff, GlobalId, Row};
use mz_sql_parser::ast::Ident;
use mz_sql_parser::ast::display::AstDisplay;
use mz_storage_types::errors::{DataflowError, SourceError, SourceErrorDetails};
use mz_storage_types::sources::postgres::CastType;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::sources::{
    MzOffset, PostgresSourceConnection, SourceExport, SourceExportDetails, SourceTimestamp,
};
use mz_timely_util::builder_async::PressOnDropButton;
use serde::{Deserialize, Serialize};
use timely::container::CapacityContainerBuilder;
use timely::dataflow::operators::Concat;
use timely::dataflow::operators::core::Partition;
use timely::dataflow::operators::vec::{Map, ToStream};
use timely::dataflow::{Scope, StreamVec};
use timely::progress::Antichain;
use tokio_postgres::error::SqlState;
use tokio_postgres::types::PgLsn;

use crate::healthcheck::{HealthStatusMessage, HealthStatusUpdate, StatusNamespace};
use crate::source::channel_reclock::SourceBatch;
use crate::source::types::{
    Probe, SourceRender, SourceTask, SourceTaskOutputs, SourceTaskUpdate, StackedCollection,
};
use crate::source::{RawSourceCreationConfig, SourceMessage};

mod replication;
mod snapshot;

impl SourceRender for PostgresSourceConnection {
    type Time = MzOffset;

    const STATUS_NAMESPACE: StatusNamespace = StatusNamespace::Postgres;

    /// Render the ingestion dataflow. This function only connects things together and contains no
    /// actual processing logic.
    fn render<G: Scope<Timestamp = MzOffset>>(
        self,
        scope: &mut G,
        config: &RawSourceCreationConfig,
        resume_uppers: impl futures::Stream<Item = Antichain<MzOffset>> + 'static,
        _start_signal: impl std::future::Future<Output = ()> + 'static,
    ) -> (
        BTreeMap<GlobalId, StackedCollection<G, Result<SourceMessage, DataflowError>>>,
        StreamVec<G, HealthStatusMessage>,
        StreamVec<G, Probe<MzOffset>>,
        Vec<PressOnDropButton>,
    ) {
        // Collect the source outputs that we will be exporting into a per-table map.
        let mut table_info = BTreeMap::new();
        for (idx, (id, export)) in config.source_exports.iter().enumerate() {
            let SourceExport {
                details,
                storage_metadata: _,
                data_config: _,
            } = export;
            let details = match details {
                SourceExportDetails::Postgres(details) => details,
                // This is an export that doesn't need any data output to it.
                SourceExportDetails::None => continue,
                _ => panic!("unexpected source export details: {:?}", details),
            };
            let desc = details.table.clone();
            let casts = details.column_casts.clone();
            let resume_upper = Antichain::from_iter(
                config
                    .source_resume_uppers
                    .get(id)
                    .expect("all source exports must be present in source resume uppers")
                    .iter()
                    .map(MzOffset::decode_row),
            );
            let output = SourceOutputInfo {
                desc,
                projection: None,
                casts,
                resume_upper,
                export_id: id.clone(),
            };
            table_info
                .entry(output.desc.oid)
                .or_insert_with(BTreeMap::new)
                .insert(idx, output);
        }

        let metrics = config.metrics.get_postgres_source_metrics(config.id);

        let (snapshot_updates, rewinds, slot_ready, snapshot_err, snapshot_token) =
            snapshot::render(
                scope.clone(),
                config.clone(),
                self.clone(),
                table_info.clone(),
                metrics.snapshot_metrics.clone(),
            );

        let (repl_updates, probe_stream, repl_err, repl_token) = replication::render(
            scope.clone(),
            config.clone(),
            self,
            table_info,
            rewinds,
            slot_ready,
            resume_uppers,
            metrics,
        );

        let updates = snapshot_updates.concat(repl_updates);
        let partition_count = u64::cast_from(config.source_exports.len());
        let data_streams: Vec<_> = updates
            .inner
            .partition::<CapacityContainerBuilder<_>, _, _>(
                partition_count,
                |((output, data), time, diff): &(
                    (usize, Result<SourceMessage, DataflowError>),
                    MzOffset,
                    Diff,
                )| {
                    let output = u64::cast_from(*output);
                    (output, (data.clone(), time.clone(), diff.clone()))
                },
            );
        let mut data_collections = BTreeMap::new();
        for (id, data_stream) in config.source_exports.keys().zip_eq(data_streams) {
            data_collections.insert(*id, data_stream.as_collection());
        }

        let export_ids = config.source_exports.keys().copied();
        let health_init = export_ids
            .map(Some)
            .chain(std::iter::once(None))
            .map(|id| HealthStatusMessage {
                id,
                namespace: <Self as SourceRender>::STATUS_NAMESPACE,
                update: HealthStatusUpdate::Running,
            })
            .collect::<Vec<_>>()
            .to_stream(scope);

        // N.B. Note that we don't check ssh tunnel statuses here. We could, but immediately on
        // restart we are going to set the status to an ssh error correctly, so we don't do this
        // extra work.
        let errs = snapshot_err.concat(repl_err).map(move |err| {
            // This update will cause the dataflow to restart
            let err_string = err.display_with_causes().to_string();
            let update = HealthStatusUpdate::halting(err_string.clone(), None);

            let namespace = match err {
                ReplicationError::Transient(err)
                    if matches!(
                        &*err,
                        TransientError::PostgresError(PostgresError::Ssh(_))
                            | TransientError::PostgresError(PostgresError::SshIo(_))
                    ) =>
                {
                    StatusNamespace::Ssh
                }
                _ => <Self as SourceRender>::STATUS_NAMESPACE,
            };

            HealthStatusMessage {
                id: None,
                namespace: namespace.clone(),
                update,
            }
        });

        let health = health_init.concat(errs);

        (
            data_collections,
            health,
            probe_stream,
            vec![snapshot_token, repl_token],
        )
    }
}

impl SourceTask for PostgresSourceConnection {
    type Time = MzOffset;
    const STATUS_NAMESPACE: StatusNamespace = StatusNamespace::Postgres;

    fn spawn(
        self,
        config: RawSourceCreationConfig,
        resume_rx: tokio::sync::watch::Receiver<Antichain<MzOffset>>,
    ) -> (SourceTaskOutputs<MzOffset>, mz_ore::task::AbortOnDropHandle<()>) {
        let (data_tx, data_rx) = tokio::sync::mpsc::unbounded_channel();
        let (probe_tx, probe_rx) = tokio::sync::watch::channel(None);
        let (health_tx, health_rx) = tokio::sync::mpsc::unbounded_channel();

        let id = config.id;
        let worker_id = config.worker_id;

        // Only worker 0 runs the actual PG task. Other workers get a no-op task
        // whose channels close immediately, producing no data.
        let is_active_worker = worker_id == 0;

        let task_handle = if is_active_worker {
            // Extract the Send-safe fields from config. RawSourceCreationConfig contains Rc
            // fields (shared_remap_upper, statistics) that are not Send, so we can't move the
            // whole config into the spawned task.
            let source_exports = config.source_exports.clone();
            let source_resume_uppers = config.source_resume_uppers.clone();
            let now_fn = config.now_fn.clone();
            let storage_config = config.config.clone();
            let timestamp_interval = config.timestamp_interval;

            mz_ore::task::spawn(
                || format!("pg_source_task:{id}"),
                pg_source_task(
                    self,
                    id,
                    worker_id,
                    source_exports,
                    source_resume_uppers,
                    now_fn,
                    storage_config,
                    timestamp_interval,
                    data_tx,
                    probe_tx,
                    health_tx,
                    resume_rx,
                ),
            )
        } else {
            // Non-active workers: drop the senders so receivers see closed channels,
            // then park forever (task will be cancelled on dataflow drop).
            drop(data_tx);
            drop(probe_tx);
            drop(health_tx);
            mz_ore::task::spawn(|| format!("pg_source_task_idle:{id}"), async {
                std::future::pending::<()>().await;
            })
        };

        let outputs = SourceTaskOutputs {
            data_rx,
            probe_rx,
            health_rx,
        };
        (outputs, task_handle.abort_on_drop())
    }
}

/// The main async task for a PostgreSQL source.
///
/// This task handles the full lifecycle: connect → snapshot → replicate.
/// Data is sent via channels rather than through timely operators.
///
/// Note: This function takes individual fields from `RawSourceCreationConfig` rather than the
/// whole config because `RawSourceCreationConfig` contains `Rc` fields that are not `Send`.
async fn pg_source_task(
    connection: PostgresSourceConnection,
    id: GlobalId,
    worker_id: usize,
    source_exports: BTreeMap<GlobalId, SourceExport<CollectionMetadata>>,
    source_resume_uppers: BTreeMap<GlobalId, Vec<Row>>,
    now_fn: mz_ore::now::NowFn,
    config: mz_storage_types::configuration::StorageConfiguration,
    timestamp_interval: Duration,
    data_tx: tokio::sync::mpsc::UnboundedSender<
        SourceBatch<SourceTaskUpdate<MzOffset>, MzOffset, Diff>,
    >,
    probe_tx: tokio::sync::watch::Sender<Option<Probe<MzOffset>>>,
    health_tx: tokio::sync::mpsc::UnboundedSender<HealthStatusMessage>,
    mut resume_rx: tokio::sync::watch::Receiver<Antichain<MzOffset>>,
) {
    // Build table_info, export_id mapping, and resume uppers.
    let mut table_info: BTreeMap<u32, BTreeMap<usize, SourceOutputInfo>> = BTreeMap::new();
    let mut output_to_export_id: Vec<GlobalId> = Vec::new();
    for (idx, (export_id, export)) in source_exports.iter().enumerate() {
        output_to_export_id.push(*export_id);
        let SourceExport {
            details,
            storage_metadata: _,
            data_config: _,
        } = export;
        let details = match details {
            SourceExportDetails::Postgres(details) => details,
            SourceExportDetails::None => continue,
            _ => panic!("unexpected source export details: {:?}", details),
        };
        let resume_upper = Antichain::from_iter(
            source_resume_uppers
                .get(export_id)
                .expect("all source exports must be present in source resume uppers")
                .iter()
                .map(MzOffset::decode_row),
        );
        let output = SourceOutputInfo {
            desc: details.table.clone(),
            projection: None,
            casts: details.column_casts.clone(),
            resume_upper,
            export_id: *export_id,
        };
        table_info
            .entry(output.desc.oid)
            .or_insert_with(BTreeMap::new)
            .insert(idx, output);
    }

    // Emit initial health.
    for export_id in source_exports.keys().copied().map(Some).chain(Some(None)) {
        let _ = health_tx.send(HealthStatusMessage {
            id: export_id,
            namespace: StatusNamespace::Postgres,
            update: HealthStatusUpdate::Running,
        });
    }

    // Run the inner logic. On transient error the task returns and the dataflow
    // restart machinery will recreate us.
    let result = pg_source_task_inner(
        &connection,
        id,
        worker_id,
        &source_exports,
        &config,
        timestamp_interval,
        &now_fn,
        &mut table_info,
        &output_to_export_id,
        &data_tx,
        &probe_tx,
        &health_tx,
        &mut resume_rx,
    )
    .await;

    match result {
        Ok(()) => tracing::info!(%id, "pg_source_task completed"),
        Err(err) => {
            let err_string = err.to_string();
            tracing::warn!(%id, "pg_source_task error: {err_string}");
            let _ = health_tx.send(HealthStatusMessage {
                id: None,
                namespace: StatusNamespace::Postgres,
                update: HealthStatusUpdate::halting(err_string, None),
            });
        }
    }
}

/// Inner implementation of pg_source_task, factored out for `?` ergonomics.
async fn pg_source_task_inner(
    connection: &PostgresSourceConnection,
    id: GlobalId,
    worker_id: usize,
    _source_exports: &BTreeMap<GlobalId, SourceExport<CollectionMetadata>>,
    config: &mz_storage_types::configuration::StorageConfiguration,
    timestamp_interval: Duration,
    now_fn: &mz_ore::now::NowFn,
    table_info: &mut BTreeMap<u32, BTreeMap<usize, SourceOutputInfo>>,
    output_to_export_id: &[GlobalId],
    data_tx: &tokio::sync::mpsc::UnboundedSender<
        SourceBatch<SourceTaskUpdate<MzOffset>, MzOffset, Diff>,
    >,
    probe_tx: &tokio::sync::watch::Sender<Option<Probe<MzOffset>>>,
    _health_tx: &tokio::sync::mpsc::UnboundedSender<HealthStatusMessage>,
    resume_rx: &mut tokio::sync::watch::Receiver<Antichain<MzOffset>>,
) -> Result<(), TransientError> {
    use std::pin::pin;
    use std::sync::Arc;

    use futures::{StreamExt, TryStreamExt};
    use mz_ore::future::InTask;
    use postgres_replication::LogicalReplicationStream;
    use postgres_replication::protocol::ReplicationMessage;
    use tokio_postgres::types::PgLsn;

    tracing::info!(%id, "timely-{worker_id} pg_source_task started");

    // --- Connect ---
    let connection_config = connection
        .connection
        .config(
            &config.connection_context.secrets_reader,
            config,
            InTask::Yes,
        )
        .await?;

    let slot = &connection.publication_details.slot;

    let replication_client = connection_config
        .connect_replication(&config.connection_context.ssh_tunnel_manager)
        .await?;
    let metadata_client = Arc::new(
        connection_config
            .connect(
                "task metadata",
                &config.connection_context.ssh_tunnel_manager,
            )
            .await?,
    );

    // Ensure the replication slot exists.
    ensure_replication_slot(&replication_client, slot).await?;

    let slot_metadata = fetch_slot_metadata(
        &*metadata_client,
        slot,
        mz_storage_types::dyncfgs::PG_FETCH_SLOT_RESUME_LSN_INTERVAL.get(config.config_set()),
    )
    .await?;

    // Kill stale connection on the slot if any.
    if let Some(active_pid) = slot_metadata.active_pid {
        tracing::warn!(%id, %active_pid, "replication slot in use; killing existing connection");
        let _ = metadata_client
            .execute("SELECT pg_terminate_backend($1)", &[&active_pid])
            .await;
    }

    // --- Compute resume LSN ---
    let output_uppers: Vec<Antichain<MzOffset>> = table_info
        .iter()
        .flat_map(|(_, outputs)| outputs.values().map(|o| o.resume_upper.clone()))
        .collect();
    let resume_lsn = output_uppers
        .iter()
        .flat_map(|f| f.elements())
        .map(|&lsn| {
            if lsn == MzOffset::from(0) {
                slot_metadata.confirmed_flush_lsn
            } else {
                lsn
            }
        })
        .min();
    let Some(resume_lsn) = resume_lsn else {
        // All outputs closed.
        std::future::pending::<()>().await;
        return Ok(());
    };
    tracing::info!(%id, "timely-{worker_id} resume_lsn={resume_lsn}");

    // --- Emit initial probe ---
    let max_lsn = fetch_max_lsn(&*metadata_client).await?;
    let _ = probe_tx.send(Some(Probe {
        probe_ts: now_fn().into(),
        upstream_frontier: Antichain::from_elem(max_lsn),
    }));

    // --- Snapshot ---
    // Determine which tables need snapshotting (resume_upper == [minimum]).
    let mut tables_to_snapshot: BTreeMap<u32, BTreeMap<usize, SourceOutputInfo>> = BTreeMap::new();
    let mut rewinds: BTreeMap<usize, replication::RewindRequest> = BTreeMap::new();

    for (&oid, outputs) in table_info.iter() {
        for (&output_index, info) in outputs {
            if *info.resume_upper == [MzOffset::from(0u64)] {
                tables_to_snapshot
                    .entry(oid)
                    .or_default()
                    .insert(output_index, info.clone());
            }
        }
    }

    if !tables_to_snapshot.is_empty() {
        // Create a temporary replication slot for a consistent snapshot point.
        let tmp_slot = format!("mzsnapshot_{}", uuid::Uuid::new_v4()).replace('-', "");
        let (snapshot_lsn, snapshot_id) = {
            let query = format!(
                "CREATE_REPLICATION_SLOT {tmp_slot} TEMPORARY LOGICAL \"pgoutput\" USE_SNAPSHOT"
            );
            let row = mz_postgres_util::simple_query_opt(&replication_client, &query)
                .await?
                .unwrap();
            let lsn: PgLsn = row.get("consistent_point").unwrap().parse().unwrap();
            let snapshot_id = row.get("snapshot_name").unwrap().to_string();
            (MzOffset::from(u64::from(lsn)), snapshot_id)
        };
        tracing::info!(%id, "snapshot at lsn={snapshot_lsn} snapshot_id={snapshot_id}");

        // Snapshot each table sequentially using the exported snapshot.
        let snap_client = connection_config
            .connect("snapshot", &config.connection_context.ssh_tunnel_manager)
            .await?;
        snap_client
            .simple_query("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .await?;
        let set_snap = format!("SET TRANSACTION SNAPSHOT '{snapshot_id}'");
        snap_client.simple_query(&set_snap).await?;

        for (_oid, outputs) in &tables_to_snapshot {
            for (&output_index, info) in outputs {
                let namespace =
                    mz_sql_parser::ast::Ident::new_unchecked(&info.desc.namespace)
                        .to_ast_string_stable();
                let table =
                    mz_sql_parser::ast::Ident::new_unchecked(&info.desc.name)
                        .to_ast_string_stable();
                let column_list = info
                    .desc
                    .columns
                    .iter()
                    .map(|c| mz_sql_parser::ast::Ident::new_unchecked(&c.name).to_ast_string_stable())
                    .collect::<Vec<_>>()
                    .join(",");
                let query = format!(
                    "COPY (SELECT {column_list} FROM {namespace}.{table}) TO STDOUT (FORMAT TEXT, DELIMITER '\t')"
                );

                let mut copy_stream = pin!(snap_client.copy_out_simple(&query).await?);
                let export_id = output_to_export_id[output_index];
                let mut batch_updates = Vec::new();

                while let Some(bytes) = copy_stream.try_next().await? {
                    // The COPY data comes as raw tab-delimited text rows.
                    // Each row is a single Bytes chunk. We pack it into a Row.
                    let mut row = Row::default();
                    let mut packer = row.packer();
                    for field in bytes.split(|&b| b == b'\t') {
                        if field == b"\\N" {
                            packer.push(Datum::Null);
                        } else {
                            match decode_utf8_text(field) {
                                Ok(s) => packer.push(s),
                                Err(err) => {
                                    // Definite errors get emitted into the data stream.
                                    let msg = Err(DataflowError::from(err));
                                    batch_updates.push(((export_id, msg, MzOffset::from(0u64)), MzOffset::from(0u64), Diff::ONE));
                                    break; // skip rest of this row
                                }
                            }
                        }
                    }

                    let mut datum_vec = mz_repr::DatumVec::new();
                    let datums = datum_vec.borrow_with(&row);
                    let mut final_row = Row::default();
                    let msg = match cast_row(&info.casts, &datums, &mut final_row) {
                        Ok(()) => Ok(SourceMessage {
                            key: Row::default(),
                            value: final_row,
                            metadata: Row::default(),
                        }),
                        Err(err) => Err(DataflowError::from(err)),
                    };

                    batch_updates.push(((export_id, msg, MzOffset::from(0u64)), MzOffset::from(0u64), Diff::ONE));

                    // Flush in batches of 1024 to bound latency.
                    if batch_updates.len() >= 1024 {
                        let batch = SourceBatch {
                            updates: std::mem::take(&mut batch_updates),
                            // During snapshot, frontier stays at minimum — we can't advance
                            // until rewinds are applied by replication.
                            frontier: Antichain::from_elem(MzOffset::from(0u64)),
                        };
                        if data_tx.send(batch).is_err() {
                            return Ok(());
                        }
                    }
                }
                // Flush remaining.
                if !batch_updates.is_empty() {
                    let batch = SourceBatch {
                        updates: batch_updates,
                        frontier: Antichain::from_elem(MzOffset::from(0u64)),
                    };
                    if data_tx.send(batch).is_err() {
                        return Ok(());
                    }
                }

                // Record rewind request for this output.
                rewinds.insert(
                    output_index,
                    replication::RewindRequest {
                        output_index,
                        snapshot_lsn,
                    },
                );
            }
        }
        snap_client.simple_query("COMMIT").await?;
    }

    // --- Start replication ---
    let lsn = PgLsn::from(resume_lsn.offset);
    let query = format!(
        r#"START_REPLICATION SLOT "{}" LOGICAL {} ("proto_version" '1', "publication_names" {})"#,
        mz_sql_parser::ast::Ident::new_unchecked(slot).to_ast_string_simple(),
        lsn,
        mz_sql_parser::ast::display::escaped_string_literal(&connection.publication),
    );
    let copy_stream = replication_client.copy_both_simple(&query).await?;
    let mut stream = pin!(LogicalReplicationStream::new(copy_stream));

    // Spawn periodic probe ticker.
    let probe_metadata_client = Arc::clone(&metadata_client);
    let probe_source_id = id;
    let probe_now_fn = now_fn.clone();
    let (probe_inner_tx, mut probe_inner_rx) = tokio::sync::watch::channel(None);
    let _probe_task = mz_ore::task::spawn(
        || format!("pg_probe_ticker:{id}"),
        async move {
            let mut ticker =
                crate::source::probe::Ticker::new(move || timestamp_interval, probe_now_fn);
            while !probe_inner_tx.is_closed() {
                let probe_ts = ticker.tick().await;
                match fetch_max_lsn(&*probe_metadata_client).await {
                    Ok(lsn) => {
                        let _ = probe_inner_tx.send(Some(Probe {
                            probe_ts,
                            upstream_frontier: Antichain::from_elem(lsn),
                        }));
                    }
                    Err(err) => {
                        tracing::warn!(%probe_source_id, "probe error: {err}");
                    }
                }
            }
        },
    )
    .abort_on_drop();

    // Feedback timer for standby status updates.
    let mut feedback_timer = tokio::time::interval(Duration::from_secs(1));
    feedback_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_committed_upper = resume_lsn;

    // --- Replication loop ---
    let mut data_upper = resume_lsn;
    let mut batch_updates: Vec<(SourceTaskUpdate<MzOffset>, MzOffset, Diff)> = Vec::new();

    loop {
        tokio::select! {
            biased;

            // Forward probes to the remap operator.
            Ok(()) = probe_inner_rx.changed() => {
                if let Some(probe) = probe_inner_rx.borrow().clone() {
                    let _ = probe_tx.send(Some(probe));
                }
            }
            // Standby keepalive timer.
            _ = feedback_timer.tick() => {
                let lsn = PgLsn::from(last_committed_upper.offset);
                let ts: i64 = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH + Duration::from_secs(946_684_800))
                    .unwrap()
                    .as_micros()
                    .try_into()
                    .unwrap();
                let _ = stream.as_mut().standby_status_update(lsn, lsn, lsn, ts, 1).await;
            }
            // Resume upper feedback for committed offset tracking.
            Ok(()) = resume_rx.changed() => {
                let upper = resume_rx.borrow().clone();
                if let Some(lsn) = upper.into_option() {
                    if last_committed_upper < lsn {
                        last_committed_upper = lsn;
                    }
                }
            }
            // Replication stream messages.
            msg = StreamExt::next(&mut stream) => {
                let Some(event) = msg else {
                    return Err(TransientError::ReplicationEOF);
                };
                let event = event.map_err(TransientError::SQLClient)?;
                match event {
                    ReplicationMessage::XLogData(data) => {
                        use postgres_replication::protocol::LogicalReplicationMessage::*;
                        match data.data() {
                            Begin(begin) => {
                                let commit_lsn = MzOffset::from(begin.final_lsn());
                                assert!(
                                    data_upper <= commit_lsn,
                                    "data_upper={data_upper} commit_lsn={commit_lsn}"
                                );
                                data_upper = commit_lsn + 1;

                                // Process the transaction inline until Commit.
                                loop {
                                    let Some(tx_event) = stream.as_mut().next().await else {
                                        return Err(TransientError::ReplicationEOF);
                                    };
                                    let tx_event = tx_event.map_err(TransientError::SQLClient)?;
                                    let tx_data = match tx_event {
                                        ReplicationMessage::XLogData(d) => d.into_data(),
                                        ReplicationMessage::PrimaryKeepAlive(_) => continue,
                                        _ => return Err(TransientError::UnknownReplicationMessage),
                                    };
                                    match tx_data {
                                        Commit(body) => {
                                            if commit_lsn != body.commit_lsn().into() {
                                                return Err(TransientError::InvalidTransaction);
                                            }
                                            break;
                                        }
                                        Insert(body) => {
                                            let rel = body.rel_id();
                                            if let Some(outputs) = table_info.get(&rel) {
                                                for (&out_idx, info) in outputs {
                                                    let export_id = output_to_export_id[out_idx];
                                                    let msg = unpack_and_cast(body.tuple().tuple_data(), info);
                                                    emit_row(&mut batch_updates, &rewinds, export_id, out_idx, commit_lsn, msg, Diff::ONE);
                                                }
                                            }
                                        }
                                        Delete(body) => {
                                            if let Some(old) = body.old_tuple() {
                                                let rel = body.rel_id();
                                                if let Some(outputs) = table_info.get(&rel) {
                                                    for (&out_idx, info) in outputs {
                                                        let export_id = output_to_export_id[out_idx];
                                                        let msg = unpack_and_cast(old.tuple_data(), info);
                                                        emit_row(&mut batch_updates, &rewinds, export_id, out_idx, commit_lsn, msg, Diff::MINUS_ONE);
                                                    }
                                                }
                                            }
                                        }
                                        Update(body) => {
                                            if let Some(old) = body.old_tuple() {
                                                let rel = body.rel_id();
                                                if let Some(outputs) = table_info.get(&rel) {
                                                    for (&out_idx, info) in outputs {
                                                        let export_id = output_to_export_id[out_idx];
                                                        let old_msg = unpack_and_cast(old.tuple_data(), info);
                                                        let new_msg = unpack_and_cast(body.new_tuple().tuple_data(), info);
                                                        emit_row(&mut batch_updates, &rewinds, export_id, out_idx, commit_lsn, old_msg, Diff::MINUS_ONE);
                                                        emit_row(&mut batch_updates, &rewinds, export_id, out_idx, commit_lsn, new_msg, Diff::ONE);
                                                    }
                                                }
                                            }
                                        }
                                        Relation(_) | Truncate(_) | Origin(_) | Type(_) => {}
                                        Begin(_) => return Err(TransientError::NestedTransaction),
                                        _ => {}
                                    }
                                }
                            }
                            _ => return Err(TransientError::BareTransactionEvent),
                        }
                    }
                    ReplicationMessage::PrimaryKeepAlive(keepalive) => {
                        data_upper = std::cmp::max(data_upper, keepalive.wal_end().into());
                    }
                    _ => return Err(TransientError::UnknownReplicationMessage),
                }

                // Flush batch after every message group.
                {
                    rewinds.retain(|_, req| data_upper <= req.snapshot_lsn);
                    let frontier = if rewinds.is_empty() {
                        Antichain::from_elem(data_upper)
                    } else {
                        // Can't advance past 0 while rewinds are pending.
                        Antichain::from_elem(MzOffset::from(0u64))
                    };
                    if !batch_updates.is_empty() {
                        let batch = SourceBatch {
                            updates: std::mem::take(&mut batch_updates),
                            frontier: frontier.clone(),
                        };
                        if data_tx.send(batch).is_err() {
                            return Ok(());
                        }
                    } else if frontier != Antichain::from_elem(MzOffset::from(0u64)) {
                        // Even without data, send a frontier-only batch to advance progress.
                        let batch = SourceBatch {
                            updates: Vec::new(),
                            frontier,
                        };
                        if data_tx.send(batch).is_err() {
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
}

/// Unpack a tuple from the replication stream and apply cast expressions.
fn unpack_and_cast(
    tuple_data: &[postgres_replication::protocol::TupleData],
    info: &SourceOutputInfo,
) -> Result<SourceMessage, DataflowError> {
    let mut row = Row::default();
    let mut packer = row.packer();
    for data in tuple_data {
        let datum = match data {
            postgres_replication::protocol::TupleData::Text(bytes) => {
                match decode_utf8_text(bytes) {
                    Ok(d) => d,
                    Err(err) => return Err(DataflowError::from(err)),
                }
            }
            postgres_replication::protocol::TupleData::Null => Datum::Null,
            postgres_replication::protocol::TupleData::UnchangedToast => {
                return Err(DataflowError::from(DefiniteError::MissingToast));
            }
            postgres_replication::protocol::TupleData::Binary(_) => {
                return Err(DataflowError::from(DefiniteError::UnexpectedBinaryData));
            }
        };
        packer.push(datum);
    }

    let mut datum_vec = mz_repr::DatumVec::new();
    let datums = datum_vec.borrow_with(&row);
    let mut final_row = Row::default();
    match cast_row(&info.casts, &datums, &mut final_row) {
        Ok(()) => Ok(SourceMessage {
            key: Row::default(),
            value: final_row,
            metadata: Row::default(),
        }),
        Err(err) => Err(DataflowError::from(err)),
    }
}

/// Emit a data row (and its rewind if applicable) into the batch.
fn emit_row(
    batch: &mut Vec<(SourceTaskUpdate<MzOffset>, MzOffset, Diff)>,
    rewinds: &BTreeMap<usize, replication::RewindRequest>,
    export_id: GlobalId,
    output_index: usize,
    commit_lsn: MzOffset,
    row: Result<SourceMessage, DataflowError>,
    diff: Diff,
) {
    let mz_offset_zero = MzOffset::from(0u64);
    if let Some(req) = rewinds.get(&output_index) {
        if commit_lsn <= req.snapshot_lsn {
            batch.push(((export_id, row.clone(), mz_offset_zero), mz_offset_zero, -diff));
        }
    }
    batch.push(((export_id, row, commit_lsn), commit_lsn, diff));
}

#[derive(Clone, Debug)]
struct SourceOutputInfo {
    /// The expected upstream schema of this output.
    desc: PostgresTableDesc,
    /// A projection of the upstream columns into the columns expected by this output. This field
    /// is recalculated every time we observe an upstream schema change. On dataflow initialization
    /// this field is None since we haven't yet observed any schemas.
    projection: Option<Vec<usize>>,
    casts: Vec<(CastType, MirScalarExpr)>,
    resume_upper: Antichain<MzOffset>,
    export_id: GlobalId,
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum ReplicationError {
    #[error(transparent)]
    Transient(#[from] Rc<TransientError>),
    #[error(transparent)]
    Definite(#[from] Rc<DefiniteError>),
}

/// A transient error that never ends up in the collection of a specific table.
#[derive(Debug, thiserror::Error)]
pub enum TransientError {
    #[error("replication slot mysteriously missing")]
    MissingReplicationSlot,
    #[error(
        "slot overcompacted. Requested LSN {requested_lsn} but only LSNs >= {available_lsn} are available"
    )]
    OvercompactedReplicationSlot {
        requested_lsn: MzOffset,
        available_lsn: MzOffset,
    },
    #[error("replication slot already exists")]
    ReplicationSlotAlreadyExists,
    #[error("stream ended prematurely")]
    ReplicationEOF,
    #[error("unexpected replication message")]
    UnknownReplicationMessage,
    #[error("unexpected logical replication message")]
    UnknownLogicalReplicationMessage,
    #[error("received replication event outside of transaction")]
    BareTransactionEvent,
    #[error("lsn mismatch between BEGIN and COMMIT")]
    InvalidTransaction,
    #[error("BEGIN within existing BEGIN stream")]
    NestedTransaction,
    #[error("recoverable errors should crash the process during snapshots")]
    SyntheticError,
    #[error("sql client error")]
    SQLClient(#[from] tokio_postgres::Error),
    #[error(transparent)]
    PostgresError(#[from] PostgresError),
    #[error(transparent)]
    Generic(#[from] anyhow::Error),
}

/// A definite error that always ends up in the collection of a specific table.
#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
pub enum DefiniteError {
    #[error("slot compacted past snapshot point. snapshot consistent point={0} resume_lsn={1}")]
    SlotCompactedPastResumePoint(MzOffset, MzOffset),
    #[error("table was truncated")]
    TableTruncated,
    #[error("table was dropped")]
    TableDropped,
    #[error("publication {0:?} does not exist")]
    PublicationDropped(String),
    #[error("replication slot has been invalidated because it exceeded the maximum reserved size")]
    InvalidReplicationSlot,
    #[error("unexpected number of columns while parsing COPY output")]
    MissingColumn,
    #[error("failed to parse COPY protocol")]
    InvalidCopyInput,
    #[error(
        "unsupported action: database restored from point-in-time backup. Expected timeline ID {expected} but got {actual}"
    )]
    InvalidTimelineId { expected: u64, actual: u64 },
    #[error(
        "TOASTed value missing from old row. Did you forget to set REPLICA IDENTITY to FULL for your table?"
    )]
    MissingToast,
    #[error(
        "old row missing from replication stream. Did you forget to set REPLICA IDENTITY to FULL for your table?"
    )]
    DefaultReplicaIdentity,
    #[error("incompatible schema change: {0}")]
    // TODO: proper error variants for all the expected schema violations
    IncompatibleSchema(String),
    #[error("invalid UTF8 string: {0:?}")]
    InvalidUTF8(Vec<u8>),
    #[error("failed to cast raw column: {0}")]
    CastError(#[source] EvalError),
    #[error("unexpected binary data in replication stream")]
    UnexpectedBinaryData,
}

impl From<DefiniteError> for DataflowError {
    fn from(err: DefiniteError) -> Self {
        let m = err.to_string().into();
        DataflowError::SourceError(Box::new(SourceError {
            error: match &err {
                DefiniteError::SlotCompactedPastResumePoint(_, _) => SourceErrorDetails::Other(m),
                DefiniteError::TableTruncated => SourceErrorDetails::Other(m),
                DefiniteError::TableDropped => SourceErrorDetails::Other(m),
                DefiniteError::PublicationDropped(_) => SourceErrorDetails::Initialization(m),
                DefiniteError::InvalidReplicationSlot => SourceErrorDetails::Initialization(m),
                DefiniteError::MissingColumn => SourceErrorDetails::Other(m),
                DefiniteError::InvalidCopyInput => SourceErrorDetails::Other(m),
                DefiniteError::InvalidTimelineId { .. } => SourceErrorDetails::Initialization(m),
                DefiniteError::MissingToast => SourceErrorDetails::Other(m),
                DefiniteError::DefaultReplicaIdentity => SourceErrorDetails::Other(m),
                DefiniteError::IncompatibleSchema(_) => SourceErrorDetails::Other(m),
                DefiniteError::InvalidUTF8(_) => SourceErrorDetails::Other(m),
                DefiniteError::CastError(_) => SourceErrorDetails::Other(m),
                DefiniteError::UnexpectedBinaryData => SourceErrorDetails::Other(m),
            },
        }))
    }
}

async fn ensure_replication_slot(client: &Client, slot: &str) -> Result<(), TransientError> {
    // Note: Using unchecked here is okay because we're using it in a SQL query.
    let slot = Ident::new_unchecked(slot).to_ast_string_simple();
    let query = format!("CREATE_REPLICATION_SLOT {slot} LOGICAL \"pgoutput\" NOEXPORT_SNAPSHOT");
    match simple_query_opt(client, &query).await {
        Ok(_) => Ok(()),
        // If the slot already exists that's still ok
        Err(PostgresError::Postgres(err)) if err.code() == Some(&SqlState::DUPLICATE_OBJECT) => {
            tracing::trace!("replication slot {slot} already existed");
            Ok(())
        }
        Err(err) => Err(TransientError::PostgresError(err)),
    }
}

/// The state of a replication slot.
struct SlotMetadata {
    /// The process ID of the session using this slot if the slot is currently actively being used.
    /// None if inactive.
    active_pid: Option<i32>,
    /// The address (LSN) up to which the logical slot's consumer has confirmed receiving data.
    /// Data corresponding to the transactions committed before this LSN is not available anymore.
    confirmed_flush_lsn: MzOffset,
}

/// Fetches the minimum LSN at which this slot can safely resume.
async fn fetch_slot_metadata(
    client: &Client,
    slot: &str,
    interval: Duration,
) -> Result<SlotMetadata, TransientError> {
    loop {
        let query = "SELECT active_pid, confirmed_flush_lsn
                FROM pg_replication_slots WHERE slot_name = $1";
        let Some(row) = client.query_opt(query, &[&slot]).await? else {
            return Err(TransientError::MissingReplicationSlot);
        };

        match row.get::<_, Option<PgLsn>>("confirmed_flush_lsn") {
            // For postgres, `confirmed_flush_lsn` means that the slot is able to produce
            // all transactions that happen at tx_lsn >= confirmed_flush_lsn. Therefore this value
            // already has "upper" semantics.
            Some(lsn) => {
                return Ok(SlotMetadata {
                    confirmed_flush_lsn: MzOffset::from(lsn),
                    active_pid: row.get("active_pid"),
                });
            }
            // It can happen that confirmed_flush_lsn is NULL as the slot initializes
            // This could probably be a `tokio::time::interval`, but its only is called twice,
            // so its fine like this.
            None => tokio::time::sleep(interval).await,
        };
    }
}

/// Fetch the `pg_current_wal_lsn`, used to report metrics.
async fn fetch_max_lsn(client: &Client) -> Result<MzOffset, TransientError> {
    let query = "SELECT pg_current_wal_lsn()";
    let row = simple_query_opt(client, query).await?;

    match row.and_then(|row| {
        row.get("pg_current_wal_lsn")
            .map(|lsn| lsn.parse::<PgLsn>().unwrap())
    }) {
        // Based on the documentation, it appears that `pg_current_wal_lsn` has
        // the same "upper" semantics of `confirmed_flush_lsn`:
        // <https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-ADMIN-BACKUP>
        // We may need to revisit this and use `pg_current_wal_flush_lsn`.
        Some(lsn) => Ok(MzOffset::from(lsn)),
        None => Err(TransientError::Generic(anyhow::anyhow!(
            "pg_current_wal_lsn() mysteriously has no value"
        ))),
    }
}

// Ensures that the table with oid `oid` and expected schema `expected_schema` is still compatible
// with the current upstream schema `upstream_info`.
fn verify_schema(
    oid: u32,
    info: &SourceOutputInfo,
    upstream_info: &BTreeMap<u32, PostgresTableDesc>,
) -> Result<(), DefiniteError> {
    let current_desc = upstream_info.get(&oid).ok_or(DefiniteError::TableDropped)?;

    let allow_oids_to_change_by_col_num = info
        .desc
        .columns
        .iter()
        .zip_eq(info.casts.iter())
        .flat_map(|(col, (cast_type, _))| match cast_type {
            CastType::Text => Some(col.col_num),
            CastType::Natural => None,
        })
        .collect();

    match info
        .desc
        .determine_compatibility(current_desc, &allow_oids_to_change_by_col_num)
    {
        Ok(()) => Ok(()),
        Err(err) => Err(DefiniteError::IncompatibleSchema(err.to_string())),
    }
}

/// Casts a text row into the target types
fn cast_row(
    casts: &[(CastType, MirScalarExpr)],
    datums: &[Datum<'_>],
    row: &mut Row,
) -> Result<(), DefiniteError> {
    let arena = mz_repr::RowArena::new();
    let mut packer = row.packer();
    for (_, column_cast) in casts {
        let datum = column_cast
            .eval(datums, &arena)
            .map_err(DefiniteError::CastError)?;
        packer.push(datum);
    }
    Ok(())
}

/// Converts raw bytes that are expected to be UTF8 encoded into a `Datum::String`
fn decode_utf8_text(bytes: &[u8]) -> Result<Datum<'_>, DefiniteError> {
    match std::str::from_utf8(bytes) {
        Ok(text) => Ok(Datum::String(text)),
        Err(_) => Err(DefiniteError::InvalidUTF8(bytes.to_vec())),
    }
}
