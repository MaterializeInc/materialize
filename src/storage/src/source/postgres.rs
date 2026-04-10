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
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::{DataflowError, SourceError, SourceErrorDetails};
use mz_storage_types::sources::postgres::CastType;
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

/// Type alias for the data channel sender.
type DataBatchSender =
    tokio::sync::mpsc::UnboundedSender<SourceBatch<SourceTaskUpdate<MzOffset>, MzOffset, Diff>>;

/// Type alias for a batch of updates being accumulated before sending.
type UpdateBatch = Vec<(SourceTaskUpdate<MzOffset>, MzOffset, Diff)>;

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
    ) -> (
        SourceTaskOutputs<MzOffset>,
        mz_ore::task::AbortOnDropHandle<()>,
    ) {
        let (data_tx, data_rx) = tokio::sync::mpsc::unbounded_channel();
        let (probe_tx, probe_rx) = tokio::sync::watch::channel(None);
        let (health_tx, health_rx) = tokio::sync::mpsc::unbounded_channel();

        let id = config.id;
        let worker_id = config.worker_id;

        // Only worker 0 runs the actual PG task. Other workers get a no-op task
        // whose channels close immediately, producing no data.
        let is_active_worker = worker_id == 0;

        let task_handle = if is_active_worker {
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
/// Handles the full lifecycle: connect → snapshot → replicate. Data is sent via channels rather
/// than through timely operators.
async fn pg_source_task(
    connection: PostgresSourceConnection,
    id: GlobalId,
    worker_id: usize,
    source_exports: BTreeMap<GlobalId, SourceExport<CollectionMetadata>>,
    source_resume_uppers: BTreeMap<GlobalId, Vec<Row>>,
    now_fn: mz_ore::now::NowFn,
    config: mz_storage_types::configuration::StorageConfiguration,
    timestamp_interval: Duration,
    data_tx: DataBatchSender,
    probe_tx: tokio::sync::watch::Sender<Option<Probe<MzOffset>>>,
    health_tx: tokio::sync::mpsc::UnboundedSender<HealthStatusMessage>,
    mut resume_rx: tokio::sync::watch::Receiver<Antichain<MzOffset>>,
) {
    // Build table_info and export_id mapping.
    let (mut table_info, output_to_export_id) =
        build_table_info(&source_exports, &source_resume_uppers);

    // Emit initial health.
    for export_id in source_exports.keys().copied().map(Some).chain(Some(None)) {
        let _ = health_tx.send(HealthStatusMessage {
            id: export_id,
            namespace: StatusNamespace::Postgres,
            update: HealthStatusUpdate::Running,
        });
    }

    let result = pg_source_task_inner(
        &connection,
        id,
        worker_id,
        &config,
        timestamp_interval,
        &now_fn,
        &mut table_info,
        &output_to_export_id,
        data_tx,
        probe_tx,
        &mut resume_rx,
    )
    .await;

    match result {
        Ok(()) => tracing::info!(%id, "pg_source_task completed"),
        Err(err) => {
            tracing::warn!(%id, "pg_source_task error: {err:?}");
            let err_string = err.to_string();
            let _ = health_tx.send(HealthStatusMessage {
                id: None,
                namespace: StatusNamespace::Postgres,
                update: HealthStatusUpdate::halting(err_string, None),
            });
        }
    }
}

/// Build table_info and output-index-to-GlobalId mapping from source exports.
fn build_table_info(
    source_exports: &BTreeMap<GlobalId, SourceExport<CollectionMetadata>>,
    source_resume_uppers: &BTreeMap<GlobalId, Vec<Row>>,
) -> (
    BTreeMap<u32, BTreeMap<usize, SourceOutputInfo>>,
    Vec<GlobalId>,
) {
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
    (table_info, output_to_export_id)
}

/// Inner implementation of pg_source_task, factored out for `?` ergonomics.
async fn pg_source_task_inner(
    connection: &PostgresSourceConnection,
    id: GlobalId,
    worker_id: usize,
    config: &mz_storage_types::configuration::StorageConfiguration,
    timestamp_interval: Duration,
    now_fn: &mz_ore::now::NowFn,
    table_info: &mut BTreeMap<u32, BTreeMap<usize, SourceOutputInfo>>,
    output_to_export_id: &[GlobalId],
    data_tx: DataBatchSender,
    probe_tx: tokio::sync::watch::Sender<Option<Probe<MzOffset>>>,
    resume_rx: &mut tokio::sync::watch::Receiver<Antichain<MzOffset>>,
) -> Result<(), TransientError> {
    use std::sync::Arc;

    use mz_ore::future::InTask;

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

    ensure_replication_slot(&replication_client, slot).await?;

    let slot_metadata = fetch_slot_metadata(
        &*metadata_client,
        slot,
        mz_storage_types::dyncfgs::PG_FETCH_SLOT_RESUME_LSN_INTERVAL.get(config.config_set()),
    )
    .await?;

    if let Some(active_pid) = slot_metadata.active_pid {
        tracing::warn!(%id, %active_pid, "replication slot in use; killing existing connection");
        let _ = metadata_client
            .execute("SELECT pg_terminate_backend($1)", &[&active_pid])
            .await;
    }

    // --- Compute resume LSN ---
    let resume_lsn = compute_resume_lsn(table_info, &slot_metadata);
    let Some(resume_lsn) = resume_lsn else {
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
    let rewinds = run_snapshot(
        &replication_client,
        table_info,
        output_to_export_id,
        &data_tx,
        id,
    )
    .await?;

    // --- Replication ---
    run_replication(
        &replication_client,
        Arc::clone(&metadata_client),
        connection,
        slot,
        resume_lsn,
        timestamp_interval,
        now_fn,
        id,
        table_info,
        output_to_export_id,
        &data_tx,
        probe_tx,
        resume_rx,
        rewinds,
    )
    .await
}

/// Compute the overall resume LSN from per-table resume uppers.
fn compute_resume_lsn(
    table_info: &BTreeMap<u32, BTreeMap<usize, SourceOutputInfo>>,
    slot_metadata: &SlotMetadata,
) -> Option<MzOffset> {
    table_info
        .iter()
        .flat_map(|(_, outputs)| outputs.values().map(|o| &o.resume_upper))
        .flat_map(|f| f.elements())
        .map(|&lsn| {
            if lsn == MzOffset::from(0u64) {
                slot_metadata.confirmed_flush_lsn
            } else {
                lsn
            }
        })
        .min()
}

/// Run the snapshot phase: export a consistent snapshot, COPY each table, and build rewind
/// requests. Returns the rewind map for use by the replication phase.
async fn run_snapshot(
    replication_client: &Client,
    table_info: &BTreeMap<u32, BTreeMap<usize, SourceOutputInfo>>,
    output_to_export_id: &[GlobalId],
    data_tx: &DataBatchSender,
    id: GlobalId,
) -> Result<BTreeMap<usize, replication::RewindRequest>, TransientError> {
    let mut rewinds: BTreeMap<usize, replication::RewindRequest> = BTreeMap::new();

    // Determine which tables need snapshotting (resume_upper at minimum).
    let tables_to_snapshot: BTreeMap<u32, BTreeMap<usize, SourceOutputInfo>> = table_info
        .iter()
        .filter_map(|(&oid, outputs)| {
            let need_snapshot: BTreeMap<_, _> = outputs
                .iter()
                .filter(|(_, info)| *info.resume_upper == [MzOffset::from(0u64)])
                .map(|(&idx, info)| (idx, info.clone()))
                .collect();
            if need_snapshot.is_empty() {
                None
            } else {
                Some((oid, need_snapshot))
            }
        })
        .collect();

    if tables_to_snapshot.is_empty() {
        return Ok(rewinds);
    }

    // Export a consistent snapshot point following the protocol in `export_snapshot_inner`.
    let (snapshot_lsn, snapshot_id) = export_snapshot_for_copy(replication_client).await?;
    tracing::info!(%id, "snapshot at lsn={snapshot_lsn} snapshot_id={snapshot_id}");

    // COPY each table using the exported snapshot.
    for (_oid, outputs) in &tables_to_snapshot {
        for (&output_index, info) in outputs {
            let export_id = output_to_export_id[output_index];
            snapshot_table(replication_client, info, export_id, data_tx).await?;
            rewinds.insert(
                output_index,
                replication::RewindRequest {
                    output_index,
                    snapshot_lsn,
                },
            );
        }
    }

    // Close the snapshot transaction so the replication client can run START_REPLICATION.
    replication_client.simple_query("COMMIT;").await?;

    Ok(rewinds)
}

/// Create a temporary replication slot and export a transaction snapshot.
/// Returns (snapshot_lsn, snapshot_id).
async fn export_snapshot_for_copy(
    replication_client: &Client,
) -> Result<(MzOffset, String), TransientError> {
    replication_client
        .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
        .await?;

    let tmp_slot = format!("mzsnapshot_{}", uuid::Uuid::new_v4()).replace('-', "");
    let slot_name = Ident::new_unchecked(&tmp_slot).to_ast_string_simple();
    let query = format!(
        "CREATE_REPLICATION_SLOT {slot_name} TEMPORARY LOGICAL \"pgoutput\" USE_SNAPSHOT"
    );
    let row = match simple_query_opt(replication_client, &query).await {
        Ok(row) => row.unwrap(),
        Err(err) => {
            replication_client.simple_query("ROLLBACK;").await?;
            return Err(err.into());
        }
    };

    // consistent_point - 1: START_REPLICATION includes txns at LSNs >= consistent_point,
    // so the snapshot must be at the LSN just before.
    let consistent_point: PgLsn = row.get("consistent_point").unwrap().parse().unwrap();
    let snapshot_lsn = u64::from(consistent_point)
        .checked_sub(1)
        .expect("consistent point is always non-zero");

    let row = match simple_query_opt(replication_client, "SELECT pg_export_snapshot();").await {
        Ok(row) => row.unwrap(),
        Err(err) => {
            replication_client.simple_query("ROLLBACK;").await?;
            return Err(err.into());
        }
    };
    let snapshot_id = row.get("pg_export_snapshot").unwrap().to_owned();

    Ok((MzOffset::from(snapshot_lsn), snapshot_id))
}

/// Snapshot a single table via COPY, sending batches through the data channel.
async fn snapshot_table(
    client: &Client,
    info: &SourceOutputInfo,
    export_id: GlobalId,
    data_tx: &DataBatchSender,
) -> Result<(), TransientError> {
    use std::pin::pin;

    use futures::TryStreamExt;

    let namespace =
        mz_sql_parser::ast::Ident::new_unchecked(&info.desc.namespace).to_ast_string_stable();
    let table =
        mz_sql_parser::ast::Ident::new_unchecked(&info.desc.name).to_ast_string_stable();
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

    let mut copy_stream = pin!(client.copy_out_simple(&query).await?);
    let col_len = info.casts.len();
    let mut text_row = Row::default();
    let mut batch_updates: UpdateBatch = Vec::new();
    let mz_zero = MzOffset::from(0u64);

    while let Some(bytes) = copy_stream.try_next().await? {
        let msg = match snapshot::decode_copy_row(&bytes, col_len, &mut text_row) {
            Ok(()) => {
                let mut datum_vec = mz_repr::DatumVec::new();
                let datums = datum_vec.borrow_with(&text_row);
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
            Err(err) => Err(DataflowError::from(err)),
        };

        batch_updates.push(((export_id, msg, mz_zero), mz_zero, Diff::ONE));

        if batch_updates.len() >= 1024 {
            send_batch(&data_tx, &mut batch_updates, Antichain::from_elem(mz_zero))?;
        }
    }
    if !batch_updates.is_empty() {
        send_batch(&data_tx, &mut batch_updates, Antichain::from_elem(mz_zero))?;
    }
    Ok(())
}

/// Run the replication phase: start the logical replication stream, process transactions, and
/// send data + frontier updates through the data channel.
async fn run_replication(
    replication_client: &Client,
    metadata_client: std::sync::Arc<Client>,
    connection: &PostgresSourceConnection,
    slot: &str,
    resume_lsn: MzOffset,
    timestamp_interval: Duration,
    now_fn: &mz_ore::now::NowFn,
    id: GlobalId,
    table_info: &mut BTreeMap<u32, BTreeMap<usize, SourceOutputInfo>>,
    output_to_export_id: &[GlobalId],
    data_tx: &DataBatchSender,
    probe_tx: tokio::sync::watch::Sender<Option<Probe<MzOffset>>>,
    resume_rx: &mut tokio::sync::watch::Receiver<Antichain<MzOffset>>,
    mut rewinds: BTreeMap<usize, replication::RewindRequest>,
) -> Result<(), TransientError> {
    use std::pin::pin;

    use futures::StreamExt;
    use postgres_replication::LogicalReplicationStream;
    use postgres_replication::protocol::ReplicationMessage;

    let lsn = PgLsn::from(resume_lsn.offset);
    let query = format!(
        r#"START_REPLICATION SLOT "{}" LOGICAL {} ("proto_version" '1', "publication_names" {})"#,
        Ident::new_unchecked(slot).to_ast_string_simple(),
        lsn,
        mz_sql_parser::ast::display::escaped_string_literal(&connection.publication),
    );
    let copy_stream = replication_client.copy_both_simple(&query).await?;
    let mut stream = pin!(LogicalReplicationStream::new(copy_stream));

    // Spawn periodic probe ticker.
    let probe_metadata_client = std::sync::Arc::clone(&metadata_client);
    let probe_now_fn = now_fn.clone();
    let _probe_task = mz_ore::task::spawn(|| format!("pg_probe_ticker:{id}"), async move {
        let mut ticker =
            crate::source::probe::Ticker::new(move || timestamp_interval, probe_now_fn);
        while !probe_tx.is_closed() {
            let probe_ts = ticker.tick().await;
            match fetch_max_lsn(&*probe_metadata_client).await {
                Ok(lsn) => {
                    let _ = probe_tx.send(Some(Probe {
                        probe_ts,
                        upstream_frontier: Antichain::from_elem(lsn),
                    }));
                }
                Err(err) => {
                    tracing::warn!(%id, "probe error: {err}");
                }
            }
        }
    })
    .abort_on_drop();

    // Standby keepalive timer.
    let mut feedback_timer = tokio::time::interval(Duration::from_secs(1));
    feedback_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_committed_upper = resume_lsn;

    // PG epoch for standby timestamps.
    let pg_epoch = std::time::UNIX_EPOCH + Duration::from_secs(946_684_800);

    let mut data_upper = resume_lsn;
    let mut batch_updates: UpdateBatch = Vec::new();

    loop {
        tokio::select! {
            biased;
            _ = feedback_timer.tick() => {
                send_standby_keepalive(&mut stream, last_committed_upper, pg_epoch).await;
            }
            Ok(()) = resume_rx.changed() => {
                let upper = resume_rx.borrow().clone();
                if let Some(lsn) = upper.into_option() {
                    if last_committed_upper < lsn {
                        last_committed_upper = lsn;
                    }
                }
            }
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

                                process_transaction(
                                    &mut stream,
                                    &metadata_client,
                                    commit_lsn,
                                    table_info,
                                    output_to_export_id,
                                    &rewinds,
                                    &mut batch_updates,
                                ).await?;
                            }
                            _ => return Err(TransientError::BareTransactionEvent),
                        }
                    }
                    ReplicationMessage::PrimaryKeepAlive(keepalive) => {
                        data_upper = std::cmp::max(data_upper, keepalive.wal_end().into());
                    }
                    _ => return Err(TransientError::UnknownReplicationMessage),
                }

                // Flush batch and advance frontier.
                flush_replication_batch(
                    data_tx,
                    &mut batch_updates,
                    &mut rewinds,
                    data_upper,
                )?;
            }
        }
    }
}

/// Process a single transaction from the replication stream (between Begin and Commit).
async fn process_transaction(
    stream: &mut std::pin::Pin<&mut postgres_replication::LogicalReplicationStream>,
    _metadata_client: &std::sync::Arc<Client>,
    commit_lsn: MzOffset,
    table_info: &mut BTreeMap<u32, BTreeMap<usize, SourceOutputInfo>>,
    output_to_export_id: &[GlobalId],
    rewinds: &BTreeMap<usize, replication::RewindRequest>,
    batch_updates: &mut UpdateBatch,
) -> Result<(), TransientError> {
    use futures::StreamExt;
    use postgres_replication::protocol::LogicalReplicationMessage::*;
    use postgres_replication::protocol::ReplicationMessage;

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
                return Ok(());
            }
            Insert(body) => {
                let rel = body.rel_id();
                if let Some(outputs) = table_info.get(&rel) {
                    for (&out_idx, info) in outputs {
                        let export_id = output_to_export_id[out_idx];
                        let msg = unpack_and_cast(body.tuple().tuple_data(), info);
                        emit_row(batch_updates, rewinds, export_id, out_idx, commit_lsn, msg, Diff::ONE);
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
                            emit_row(batch_updates, rewinds, export_id, out_idx, commit_lsn, msg, Diff::MINUS_ONE);
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
                            emit_row(batch_updates, rewinds, export_id, out_idx, commit_lsn, old_msg, Diff::MINUS_ONE);
                            emit_row(batch_updates, rewinds, export_id, out_idx, commit_lsn, new_msg, Diff::ONE);
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

/// Send a standby status update to PostgreSQL.
async fn send_standby_keepalive(
    stream: &mut std::pin::Pin<&mut postgres_replication::LogicalReplicationStream>,
    last_committed_upper: MzOffset,
    pg_epoch: std::time::SystemTime,
) {
    let lsn = PgLsn::from(last_committed_upper.offset);
    let ts: i64 = std::time::SystemTime::now()
        .duration_since(pg_epoch)
        .unwrap()
        .as_micros()
        .try_into()
        .unwrap();
    let _ = stream
        .as_mut()
        .standby_status_update(lsn, lsn, lsn, ts, 1)
        .await;
}

/// Flush accumulated replication batch updates and advance the frontier.
fn flush_replication_batch(
    data_tx: &DataBatchSender,
    batch_updates: &mut UpdateBatch,
    rewinds: &mut BTreeMap<usize, replication::RewindRequest>,
    data_upper: MzOffset,
) -> Result<(), TransientError> {
    rewinds.retain(|_, req| data_upper <= req.snapshot_lsn);
    let frontier = if rewinds.is_empty() {
        Antichain::from_elem(data_upper)
    } else {
        Antichain::from_elem(MzOffset::from(0u64))
    };
    if !batch_updates.is_empty() {
        let batch = SourceBatch {
            updates: std::mem::take(batch_updates),
            frontier,
        };
        if data_tx.send(batch).is_err() {
            return Ok(()); // receiver dropped, shut down gracefully
        }
    } else if frontier != Antichain::from_elem(MzOffset::from(0u64)) {
        let batch = SourceBatch {
            updates: Vec::new(),
            frontier,
        };
        if data_tx.send(batch).is_err() {
            return Ok(());
        }
    }
    Ok(())
}

/// Send a batch of snapshot/replication updates through the data channel.
fn send_batch(
    data_tx: &DataBatchSender,
    batch_updates: &mut UpdateBatch,
    frontier: Antichain<MzOffset>,
) -> Result<(), TransientError> {
    let batch = SourceBatch {
        updates: std::mem::take(batch_updates),
        frontier,
    };
    if data_tx.send(batch).is_err() {
        // Receiver dropped — shut down gracefully.
    }
    Ok(())
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
    batch: &mut UpdateBatch,
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
            batch.push((
                (export_id, row.clone(), mz_offset_zero),
                mz_offset_zero,
                -diff,
            ));
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
    let slot = Ident::new_unchecked(slot).to_ast_string_simple();
    loop {
        let query = format!(
            "SELECT active_pid, confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = {slot}"
        );
        let row = simple_query_opt(client, &query).await?;
        match row {
            Some(row) => {
                let active_pid = row.get("active_pid").and_then(|s| s.parse().ok());
                let confirmed_flush_lsn: PgLsn =
                    row.get("confirmed_flush_lsn").unwrap().parse().unwrap();
                return Ok(SlotMetadata {
                    active_pid,
                    confirmed_flush_lsn: MzOffset::from(u64::from(confirmed_flush_lsn)),
                });
            }
            None => {
                tokio::time::sleep(interval).await;
            }
        }
    }
}

async fn fetch_max_lsn(client: &Client) -> Result<MzOffset, TransientError> {
    let row = simple_query_opt(client, "SELECT pg_current_wal_lsn()")
        .await?
        .unwrap();
    let lsn: PgLsn = row
        .get("pg_current_wal_lsn")
        .unwrap()
        .parse()
        .unwrap();
    Ok(MzOffset::from(u64::from(lsn)))
}

fn verify_schema(
    oid: u32,
    info: &SourceOutputInfo,
    upstream_info: &BTreeMap<u32, PostgresTableDesc>,
) -> Result<(), DefiniteError> {
    let upstream = match upstream_info.get(&oid) {
        Some(desc) => desc,
        None => return Err(DefiniteError::TableDropped),
    };
    let local = &info.desc;
    if upstream.columns.len() < local.columns.len() {
        return Err(DefiniteError::IncompatibleSchema(format!(
            "upstream table has {} columns but we expect at least {}",
            upstream.columns.len(),
            local.columns.len()
        )));
    }
    for (local_col, upstream_col) in local.columns.iter().zip(upstream.columns.iter()) {
        if local_col.type_oid != upstream_col.type_oid {
            return Err(DefiniteError::IncompatibleSchema(format!(
                "column {} type changed from {} to {}",
                local_col.name, local_col.type_oid, upstream_col.type_oid
            )));
        }
    }
    Ok(())
}

fn cast_row(
    table_cast: &[(CastType, MirScalarExpr)],
    datums: &[Datum<'_>],
    row: &mut Row,
) -> Result<(), DefiniteError> {
    let arena = mz_repr::RowArena::new();
    let mut packer = row.packer();
    for (_, column_cast) in table_cast {
        let datum = column_cast
            .eval(datums, &arena)
            .map_err(DefiniteError::CastError)?;
        packer.push(datum);
    }
    Ok(())
}

fn decode_utf8_text(bytes: &[u8]) -> Result<Datum<'_>, DefiniteError> {
    match std::str::from_utf8(bytes) {
        Ok(s) => Ok(Datum::String(s)),
        Err(_) => Err(DefiniteError::InvalidUTF8(bytes.to_vec())),
    }
}
