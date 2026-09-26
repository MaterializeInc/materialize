// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Postgres sink.
//!
//! The write path has three stages. The setup operator, on a single worker,
//! creates the target table, the staging table and the shared progress table,
//! and truncates the staging table. Once it releases its barrier every worker
//! streams its share of updates into the staging table with `COPY ... FROM
//! STDIN`, tagging each row with the Materialize timestamp and a diff of `1`
//! or `-1`. Rows are copied as soon as they arrive, including rows beyond the
//! input frontier. Finally the apply operator, again on a single worker, moves
//! each completed timestamp window from the staging table into the target table
//! in one transaction.
//!
//! Correctness rests on two rules: only completed timestamps are ever moved
//! from staging into the target table, and the staging table is truncated on
//! every dataflow start. A COPY that fails on any worker halts the sink, which
//! restarts the whole dataflow and therefore re-runs setup, because after a
//! failed COPY there is no way to know which rows landed.
//!
//! `materialize.sink_progress` records how far the sink has got. It is the
//! source of truth, because it is updated in the same transaction that writes
//! the rows. The persist progress shard mirrors it and is reconciled forward at
//! startup.

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::{cell::RefCell, fmt::Write as _};

use anyhow::{Context, bail};
use bytes::Bytes;
use differential_dataflow::trace::{Cursor, Navigable, TraceReader};
use differential_dataflow::{Hashable, VecCollection};
use futures::{SinkExt, StreamExt};
use mz_ore::cast::CastFrom;
use mz_ore::error::ErrorExt;
use mz_ore::future::InTask;
use mz_persist_client::Diagnostics;
use mz_persist_client::write::WriteHandle;
use mz_persist_types::codec_impls::UnitSchema;
use mz_pgcopy::{CopyFormatParams, encode_copy_format};
use mz_pgrepr::TextEncodeSettings;
use mz_postgres_util::{Client, Sql, batch_execute, execute, query_one, sql};
use mz_repr::{
    Datum, Diff, GlobalId, RelationDesc, Row, SqlColumnType, SqlRelationType, SqlScalarType,
    Timestamp,
};
use mz_storage_types::StorageDiff;
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sinks::{PostgresSinkConnection, StorageSinkDesc};
use mz_storage_types::sources::SourceData;
use mz_timely_util::antichain::AntichainExt;
use mz_timely_util::builder_async::{Event, OperatorBuilder, PressOnDropButton};
use timely::PartialOrder;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::vec::{Map, ToStream};
use timely::dataflow::operators::{CapabilitySet, Concatenate};
use timely::dataflow::{Scope, StreamVec};
use timely::progress::{Antichain, Timestamp as _};
use tokio_postgres::CopyInSink;

use crate::healthcheck::{HealthStatusMessage, HealthStatusUpdate, StatusNamespace};
use crate::render::sinks::{SinkBatchStream, SinkRender, SinkTrace};
use crate::statistics::SinkStatistics;
use crate::storage_state::StorageState;

type SinkBatch = <SinkTrace as TraceReader>::Batch;
type SinkCursor = <SinkBatch as Navigable>::Cursor;

/// Staging table column holding the Materialize timestamp of a row.
const TIMESTAMP_COLUMN: &str = "mz_timestamp";
/// Staging table column holding `1` for an insertion or `-1` for a retraction.
const DIFF_COLUMN: &str = "mz_diff";

/// Encoded bytes accumulated before a chunk is handed to the COPY stream.
const COPY_CHUNK_BYTES: usize = 1 << 20;

/// COPY BINARY stream header: the 11 byte signature, an i32 of flags (none
/// set) and an i32 header extension length (zero). See the "Binary Format"
/// section of the PostgreSQL `COPY` documentation.
const COPY_BINARY_HEADER: &[u8] = b"PGCOPY\n\xff\r\n\0\0\0\0\0\0\0\0\0";
/// COPY BINARY stream trailer: an i16 of `-1` in place of a tuple field count.
const COPY_BINARY_TRAILER: &[u8] = &(-1i16).to_be_bytes();

impl<'scope> SinkRender<'scope> for PostgresSinkConnection {
    fn get_key_indices(&self) -> Option<&[usize]> {
        self.key_desc_and_indices
            .as_ref()
            .map(|(_, indices)| indices.as_slice())
    }

    fn get_relation_key_indices(&self) -> Option<&[usize]> {
        self.relation_key_indices.as_deref()
    }

    fn render_sink(
        &self,
        storage_state: &mut StorageState,
        sink: &StorageSinkDesc<CollectionMetadata, Timestamp>,
        sink_id: GlobalId,
        batches: SinkBatchStream<'scope>,
        _key_is_synthetic: bool,
        _err_collection: VecCollection<'scope, Timestamp, DataflowError, Diff>,
    ) -> (
        StreamVec<'scope, Timestamp, HealthStatusMessage>,
        Vec<PressOnDropButton>,
    ) {
        let scope = batches.scope();

        let write_handle = {
            let persist = Arc::clone(&storage_state.persist_clients);
            let shard_meta = sink.to_storage_metadata.clone();
            async move {
                let client = persist.open(shard_meta.persist_location).await?;
                let handle: WriteHandle<SourceData, (), Timestamp, i64> = client
                    .open_writer(
                        shard_meta.data_shard,
                        Arc::new(shard_meta.relation_desc),
                        Arc::new(UnitSchema),
                        Diagnostics::from_purpose("sink handle"),
                    )
                    .await?;
                Ok::<_, anyhow::Error>(handle)
            }
        };

        let write_frontier = Rc::new(RefCell::new(Antichain::from_elem(Timestamp::minimum())));
        storage_state
            .sink_write_frontiers
            .insert(sink_id, Rc::clone(&write_frontier));

        let statistics = storage_state
            .aggregated_statistics
            .get_sink(&sink_id)
            .expect("statistics initialized")
            .clone();

        let (tables_ready, setup_status, setup_token) = setup_postgres_tables(
            format!("postgres-{sink_id}-setup"),
            scope.clone(),
            self.clone(),
            storage_state.storage_configuration.clone(),
            sink.from_desc.clone(),
            sink_id,
        );

        let (staged, copy_status, copy_token) = encode_and_stage_input(
            format!("postgres-{sink_id}-copy-staging"),
            batches,
            tables_ready.clone(),
            self.clone(),
            storage_state.storage_configuration.clone(),
            sink.from_desc.clone(),
            sink_id,
            statistics.clone(),
        );

        let (apply_status, apply_token) = insert_into_target_table(
            format!("postgres-{sink_id}-insert-target"),
            staged,
            tables_ready,
            self.clone(),
            storage_state.storage_configuration.clone(),
            sink.from_desc.clone(),
            sink.as_of.clone(),
            sink_id,
            statistics,
            write_handle,
            write_frontier,
        );

        let running_status = Some(HealthStatusMessage {
            id: None,
            update: HealthStatusUpdate::Running,
            namespace: StatusNamespace::Postgres,
        })
        .to_stream(scope);

        let status = scope.concatenate([running_status, setup_status, copy_status, apply_status]);

        (status, vec![setup_token, copy_token, apply_token])
    }
}

/// Creates the sink's tables on a single worker and releases a barrier once
/// they exist and the staging table is empty.
///
/// The returned stream carries no data. Its frontier becomes empty only after
/// the active worker's DDL and `TRUNCATE` have committed, so operators that
/// wait for it can rely on the tables being present and the staging table
/// being clean.
fn setup_postgres_tables<'scope>(
    name: String,
    scope: Scope<'scope, Timestamp>,
    connection: PostgresSinkConnection,
    storage_configuration: StorageConfiguration,
    from_desc: RelationDesc,
    sink_id: GlobalId,
) -> (
    StreamVec<'scope, Timestamp, Infallible>,
    StreamVec<'scope, Timestamp, HealthStatusMessage>,
    PressOnDropButton,
) {
    let is_active_worker = usize::cast_from(sink_id.hashed()) % scope.peers() == scope.index();
    let mut builder = OperatorBuilder::new(name, scope);
    let (_output, tables_ready) = builder.new_output::<CapacityContainerBuilder<Vec<Infallible>>>();

    let (button, errors) = builder.build_fallible(move |caps| {
        Box::pin(async move {
            let [capset]: &mut [_; 1] = caps.try_into().unwrap();

            if !is_active_worker {
                return Ok(());
            }

            // Reject unsupported schemas before opening a connection.
            staging_relation_type(&from_desc)?;

            let client = connect(
                &connection,
                &storage_configuration,
                sink_id,
                &format!("postgres-sink-{sink_id}-setup"),
            )
            .await?;
            create_postgres_tables(&client, &connection, &from_desc, sink_id).await?;

            *capset = CapabilitySet::new();
            Ok::<(), anyhow::Error>(())
        })
    });

    (
        tables_ready,
        health_statuses(errors),
        button.press_on_drop(),
    )
}

/// Streams every update in `batches` into the staging table with `COPY ...
/// FROM STDIN`.
///
/// Runs on every worker over that worker's share of the input. The returned
/// stream carries no data. Its frontier passes a time `t` only once every row
/// with time `< t` seen by this worker is committed in the staging table.
/// Timely combines that across workers, so a downstream operator observing the
/// frontier knows when a timestamp is complete in staging.
fn encode_and_stage_input<'scope>(
    name: String,
    batches: SinkBatchStream<'scope>,
    tables_ready: StreamVec<'scope, Timestamp, Infallible>,
    connection: PostgresSinkConnection,
    storage_configuration: StorageConfiguration,
    from_desc: RelationDesc,
    sink_id: GlobalId,
    statistics: SinkStatistics,
) -> (
    StreamVec<'scope, Timestamp, Infallible>,
    StreamVec<'scope, Timestamp, HealthStatusMessage>,
    PressOnDropButton,
) {
    let scope = batches.scope();
    let worker_index = scope.index();
    let mut builder = OperatorBuilder::new(name, scope);
    let (output, progress) = builder.new_output::<CapacityContainerBuilder<Vec<Infallible>>>();
    let mut input = builder.new_input_for(batches, Pipeline, &output);
    let mut tables_ready = builder.new_disconnected_input(tables_ready, Pipeline);

    let (button, errors) = builder.build_fallible(move |caps| {
        Box::pin(async move {
            let [capset]: &mut [_; 1] = caps.try_into().unwrap();

            while let Some(_) = tables_ready.next().await {
                // Wait for the setup operator to release its barrier.
            }

            let staging_typ = staging_relation_type(&from_desc)?;
            let client = connect(
                &connection,
                &storage_configuration,
                sink_id,
                &format!("postgres-sink-{sink_id}-copy-{worker_index}"),
            )
            .await?;

            let columns = Sql::join(
                from_desc
                    .iter_names()
                    .map(|name| Sql::ident(name.as_str()))
                    .chain([Sql::ident(TIMESTAMP_COLUMN), Sql::ident(DIFF_COLUMN)]),
                ", ",
            );
            let copy_stmt = sql!(
                "COPY {}.{} ({}) FROM STDIN (FORMAT BINARY)",
                Sql::ident(&connection.schema),
                Sql::ident(&connection.staging_table_name(sink_id)),
                columns,
            );

            // NOTE: fallible work happens below while capabilities are held.
            // On an error `build_fallible` parks this future with the
            // capabilities still held, the health stream reports halting, and
            // the whole sink dataflow restarts. That restart is required: a
            // failed COPY leaves no way to tell which rows landed, and the
            // setup operator truncates the staging table on the way back up.
            let mut copy: Option<Pin<Box<CopyInSink<Bytes>>>> = None;
            let mut buf: Vec<u8> = Vec::new();
            let mut scratch: Vec<u8> = Vec::new();
            let mut row_buf = Row::default();
            let mut times: Vec<(Timestamp, Diff)> = Vec::new();

            while let Some(event) = input.next().await {
                match event {
                    Event::Data(_cap, batches) => {
                        for batch in &batches {
                            let mut rows = 0u64;
                            let mut bytes = 0u64;
                            let mut cursor = batch.cursor();
                            while cursor.key_valid(batch) {
                                while cursor.val_valid(batch) {
                                    times.clear();
                                    cursor.map_times(batch, |time, diff| {
                                        times.push((
                                            SinkCursor::owned_time(time),
                                            SinkCursor::owned_diff(diff),
                                        ));
                                    });
                                    let val = cursor.val(batch);
                                    for &(time, diff) in &times {
                                        let ts = i64::try_from(u64::from(time))
                                            .context("timestamp does not fit in a bigint")?;
                                        let sign: i16 = if diff.is_negative() { -1 } else { 1 };
                                        let mut packer = row_buf.packer();
                                        packer.extend(val);
                                        packer.push(Datum::Int64(ts));
                                        packer.push(Datum::Int16(sign));

                                        scratch.clear();
                                        encode_copy_format(
                                            &CopyFormatParams::Binary,
                                            &row_buf,
                                            &staging_typ,
                                            &mut scratch,
                                            TextEncodeSettings::STABLE,
                                        )?;
                                        // An update with multiplicity `n` becomes
                                        // `n` staging rows carrying `mz_diff = ±1`.
                                        let n = diff.unsigned_abs();
                                        for _ in 0..n {
                                            buf.extend_from_slice(&scratch);
                                        }
                                        rows += n;
                                        bytes += u64::cast_from(scratch.len()) * n;
                                    }
                                    cursor.step_val(batch);
                                }
                                cursor.step_key(batch);
                            }
                            statistics.inc_messages_staged_by(rows);
                            statistics.inc_bytes_staged_by(bytes);

                            if buf.len() >= COPY_CHUNK_BYTES {
                                send_buffered(&client, &copy_stmt, &mut copy, &mut buf).await?;
                            }
                        }
                    }
                    Event::Progress(frontier) => {
                        // Every row with a time below `frontier` has been
                        // handed to the COPY, so finishing it commits them.
                        // Rows at or beyond the frontier that arrived early
                        // commit along with them, which is fine because only
                        // completed timestamps are moved to the target table.
                        send_buffered(&client, &copy_stmt, &mut copy, &mut buf).await?;
                        if let Some(mut sink) = copy.take() {
                            sink.send(Bytes::from_static(COPY_BINARY_TRAILER)).await?;
                            sink.as_mut().finish().await?;
                        }
                        // Downgrading the held capability is the progress
                        // signal to downstream operators.
                        capset.downgrade(frontier.iter());
                    }
                }
            }

            Ok::<(), anyhow::Error>(())
        })
    });

    (progress, health_statuses(errors), button.press_on_drop())
}

/// Moves each completed timestamp window from the staging table into the target
/// table, advancing the sink's recorded frontier in the same transaction.
///
/// Runs on one worker, because the windows have to be applied in order and a
/// window is applied by a single transaction. Reads nothing from `staged`: that
/// stream carries no data, and its frontier is what says a timestamp is
/// complete in staging across every worker.
fn insert_into_target_table<'scope>(
    name: String,
    staged: StreamVec<'scope, Timestamp, Infallible>,
    tables_ready: StreamVec<'scope, Timestamp, Infallible>,
    connection: PostgresSinkConnection,
    storage_configuration: StorageConfiguration,
    from_desc: RelationDesc,
    as_of: Antichain<Timestamp>,
    sink_id: GlobalId,
    statistics: SinkStatistics,
    write_handle: impl Future<
        Output = Result<WriteHandle<SourceData, (), Timestamp, StorageDiff>, anyhow::Error>,
    > + 'static,
    write_frontier: Rc<RefCell<Antichain<Timestamp>>>,
) -> (
    StreamVec<'scope, Timestamp, HealthStatusMessage>,
    PressOnDropButton,
) {
    let scope = staged.scope();
    let is_active_worker = usize::cast_from(sink_id.hashed()) % scope.peers() == scope.index();
    let mut builder = OperatorBuilder::new(name.clone(), scope);
    let mut staged = builder.new_disconnected_input(staged, Pipeline);
    let mut tables_ready = builder.new_disconnected_input(tables_ready, Pipeline);

    let (button, errors) = builder.build_fallible(move |_caps| {
        Box::pin(async move {
            if !is_active_worker {
                // Leaving a frontier here would hold back what the controller
                // sees for the whole sink.
                write_frontier.borrow_mut().clear();
                return Ok(());
            }

            while let Some(_) = tables_ready.next().await {
                // Wait for the setup operator to release its barrier.
            }

            let mut write_handle = write_handle.await?;
            let mut client = connect(
                &connection,
                &storage_configuration,
                sink_id,
                &format!("postgres-sink-{sink_id}-apply"),
            )
            .await?;

            let statements = ApplyStatements::new(&connection, &from_desc, sink_id)?;
            let resume_upper =
                reconcile_frontiers(&client, &statements, &mut write_handle, &write_frontier)
                    .await?;

            // The input has overcompacted if we have made progress in the past
            // but the since frontier is now beyond it, in which case the rows
            // between the two will never be replayed.
            let overcompacted = *resume_upper != [Timestamp::minimum()]
                && !PartialOrder::less_equal(&as_of, &resume_upper);
            if overcompacted {
                bail!(
                    "{name}: input compacted past resume upper: as_of {}, resume_upper: {}",
                    as_of.pretty(),
                    resume_upper.pretty()
                );
            }

            let Some(mut lower) = resume_upper.clone().into_option() else {
                write_frontier.borrow_mut().clear();
                return Ok(());
            };

            while let Some(event) = staged.next().await {
                let Event::Progress(progress) = event else {
                    // The stream is `Infallible`, so it carries no data.
                    continue;
                };
                // Ignore progress below where we resumed.
                if !PartialOrder::less_equal(&resume_upper, &progress) {
                    continue;
                }
                // Only start applying once strictly beyond the as_of. A sink
                // restarted with an earlier as_of replays its snapshot at the
                // earlier time, and applying before then would record progress
                // that skips it.
                if !as_of.iter().all(|t| !progress.less_equal(t)) {
                    continue;
                }

                let upper = frontier_bound(&progress)?;
                apply_window(
                    &mut client,
                    &statements,
                    timestamp_to_i64(lower)?,
                    upper,
                    &statistics,
                )
                .await?;

                // Only now that the rows are durable in Postgres does the
                // progress shard move. See `reconcile_frontiers` for why this
                // order and not the reverse.
                advance_persist(&mut write_handle, &progress).await;
                write_frontier.borrow_mut().clone_from(&progress);

                match progress.into_option() {
                    Some(new_lower) => lower = new_lower,
                    None => break,
                }
            }

            Ok::<(), anyhow::Error>(())
        })
    });

    (health_statuses(errors), button.press_on_drop())
}

/// Brings the persist progress shard in line with the sink's recorded frontier
/// and returns the frontier to resume from.
///
/// `materialize.sink_progress` is the source of truth, because it is written in
/// the same transaction as the rows themselves. The shard is appended to only
/// after that transaction commits, so a process that dies in between leaves the
/// progress row ahead, and this pulls the shard forward to match. The reverse
/// order would leave the shard ahead of the target table with no way to tell
/// whether the window had been applied.
async fn reconcile_frontiers(
    client: &Client,
    statements: &ApplyStatements,
    write_handle: &mut WriteHandle<SourceData, (), Timestamp, StorageDiff>,
    write_frontier: &Rc<RefCell<Antichain<Timestamp>>>,
) -> Result<Antichain<Timestamp>, anyhow::Error> {
    let row = query_one(&**client, statements.read_progress.clone(), &[]).await?;
    let recorded: i64 = row.get(0);
    let recorded = Antichain::from_elem(Timestamp::from(u64::try_from(recorded).context(
        "sink_progress.mz_frontier is negative, which no frontier this sink writes can be",
    )?));

    let persisted = write_handle.shared_upper();
    if persisted.is_empty() {
        // The shard is closed, so there is nothing left to write.
        return Ok(persisted);
    }

    if PartialOrder::less_than(&persisted, &recorded) {
        advance_persist(write_handle, &recorded).await;
        write_frontier.borrow_mut().clone_from(&recorded);
    } else if PartialOrder::less_than(&recorded, &persisted) {
        // The target database lost progress the shard says was committed, for
        // instance because it was restored from a backup. Those rows cannot be
        // replayed: a persist upper only moves forward, and the source may
        // already have compacted past them.
        bail!(
            "Postgres sink progress ({}) is behind its progress shard ({}). \
             The target database has lost writes this sink already committed, \
             so the sink has to be dropped and recreated.",
            recorded.pretty(),
            persisted.pretty(),
        );
    }

    Ok(recorded)
}

/// Appends empty batches until the progress shard reaches `target`.
async fn advance_persist(
    write_handle: &mut WriteHandle<SourceData, (), Timestamp, StorageDiff>,
    target: &Antichain<Timestamp>,
) {
    const EMPTY: &[((SourceData, ()), Timestamp, StorageDiff)] = &[];
    let mut expect_upper = write_handle.shared_upper();
    loop {
        if PartialOrder::less_equal(target, &expect_upper) {
            return;
        }
        match write_handle
            .compare_and_append(EMPTY, expect_upper, target.clone())
            .await
            .expect("valid usage")
        {
            Ok(()) => return,
            Err(mismatch) => expect_upper = mismatch.current,
        }
    }
}

/// Applies every staged row with a timestamp in `[lower, upper)` to the target
/// table and records `upper` as the sink's new frontier, in one transaction.
async fn apply_window(
    client: &mut Client,
    statements: &ApplyStatements,
    lower: i64,
    upper: i64,
    statistics: &SinkStatistics,
) -> Result<(), anyhow::Error> {
    let txn = client.transaction().await?;

    // Rows below `lower` were applied by an earlier incarnation that died
    // before its progress reached the shard, so the copy operator staged them
    // again. Applying them twice would duplicate them in the target.
    execute(&txn, statements.discard.clone(), &[&lower]).await?;

    let applied = match &statements.apply {
        ApplyKind::Keyed(statement) => {
            let row = query_one(&txn, statement.clone(), &[&upper]).await?;
            let (deleted, touched, inserted): (i64, i64, i64) =
                (row.get(0), row.get(1), row.get(2));
            if deleted > touched {
                bail!(
                    "Postgres sink deleted {deleted} rows for {touched} keys, so the \
                     target table holds more than one row per key"
                );
            }
            u64::try_from(deleted + inserted).expect("counts are non-negative")
        }
        ApplyKind::Keyless { insert, retract } => {
            let inserted = execute(&txn, insert.clone(), &[&upper]).await?;
            let row = query_one(&txn, retract.clone(), &[&upper]).await?;
            let (retracted, deleted): (i64, i64) = (row.get(0), row.get(1));
            if retracted != deleted {
                bail!(
                    "Postgres sink retracted {retracted} rows but found {deleted} to delete, \
                     so the target table is missing rows this sink expected to remove"
                );
            }
            inserted + u64::try_from(deleted).expect("counts are non-negative")
        }
    };

    let updated = execute(&txn, statements.record_progress.clone(), &[&upper]).await?;
    if updated != 1 {
        bail!("Postgres sink progress row is missing from materialize.sink_progress");
    }

    txn.commit().await?;
    statistics.inc_messages_committed_by(applied);
    Ok(())
}

/// The statements `apply_window` runs, built once per sink.
struct ApplyStatements {
    read_progress: Sql,
    record_progress: Sql,
    discard: Sql,
    apply: ApplyKind,
}

/// How a window is applied, which depends on whether the sink has a key.
enum ApplyKind {
    /// One statement that replaces every key the window touches.
    Keyed(Sql),
    /// Insertions, then retractions matched on every column.
    Keyless { insert: Sql, retract: Sql },
}

impl ApplyStatements {
    fn new(
        connection: &PostgresSinkConnection,
        from_desc: &RelationDesc,
        sink_id: GlobalId,
    ) -> Result<Self, anyhow::Error> {
        let schema = Sql::ident(&connection.schema);
        let target = Sql::ident(&connection.table);
        let staging = Sql::ident(&connection.staging_table_name(sink_id));
        let mz_schema = Sql::ident(PostgresSinkConnection::MZ_SCHEMA);
        let progress = Sql::ident(PostgresSinkConnection::PROGRESS_TABLE);
        let sink_id = Sql::literal(&sink_id.to_string());
        let timestamp = Sql::ident(TIMESTAMP_COLUMN);
        let diff = Sql::ident(DIFF_COLUMN);
        let column_names: Vec<String> = from_desc
            .iter_names()
            .map(|name| name.to_string())
            .collect();
        let columns = Sql::join(column_names.iter().map(|name| Sql::ident(name)), ", ");

        let apply = match key_columns(connection, from_desc) {
            Some(keys) => ApplyKind::Keyed(Self::keyed_statement(
                &schema, &target, &staging, &columns, &keys, &timestamp, &diff,
            )),
            None => ApplyKind::Keyless {
                insert: sql!(
                    "WITH moved AS (\
                       DELETE FROM {}.{} WHERE {} < {} AND {} > 0 RETURNING {}\
                     ) \
                     INSERT INTO {}.{} ({}) SELECT {} FROM moved",
                    schema.clone(),
                    staging.clone(),
                    timestamp.clone(),
                    Sql::param(1),
                    diff.clone(),
                    columns.clone(),
                    schema.clone(),
                    target.clone(),
                    columns.clone(),
                    columns.clone(),
                ),
                retract: Self::keyless_retract_statement(
                    &schema,
                    &target,
                    &staging,
                    &columns,
                    &column_names,
                    &timestamp,
                    &diff,
                ),
            },
        };

        Ok(ApplyStatements {
            read_progress: sql!(
                "SELECT mz_frontier FROM {}.{} WHERE sink_id = {}",
                mz_schema.clone(),
                progress.clone(),
                sink_id.clone(),
            ),
            record_progress: sql!(
                "UPDATE {}.{} SET mz_frontier = {} WHERE sink_id = {}",
                mz_schema,
                progress,
                Sql::param(1),
                sink_id,
            ),
            discard: sql!(
                "DELETE FROM {}.{} WHERE {} < {}",
                schema,
                staging,
                timestamp,
                Sql::param(1),
            ),
            apply,
        })
    }

    /// Replaces every key the window touches with that key's final state.
    ///
    /// Postgres sinks are always upsert, so a window is applied as one: delete
    /// the rows for every key it mentions, then reinsert the keys whose last
    /// event is an insertion. Replaying the individual diffs instead would not
    /// work, because an update leaves two rows for the key in flight and a
    /// key-matched retraction cannot tell which of them to remove.
    ///
    /// The data-modifying branches all read the same snapshot, so the delete
    /// never sees the rows the insert adds.
    fn keyed_statement(
        schema: &Sql,
        target: &Sql,
        staging: &Sql,
        columns: &Sql,
        keys: &[String],
        timestamp: &Sql,
        diff: &Sql,
    ) -> Sql {
        let key_columns = Sql::join(keys.iter().map(|key| Sql::ident(key)), ", ");
        let key_match = Sql::join(
            keys.iter().map(|key| {
                sql!(
                    "t.{} IS NOT DISTINCT FROM l.{}",
                    Sql::ident(key),
                    Sql::ident(key)
                )
            }),
            " AND ",
        );
        sql!(
            "WITH win AS (\
               DELETE FROM {}.{} WHERE {} < {} RETURNING {}, {}, {}\
             ), \
             latest AS (\
               SELECT DISTINCT ON ({}) {}, {} FROM win ORDER BY {}, {} DESC, {} DESC\
             ), \
             deleted AS (\
               DELETE FROM {}.{} t USING latest l WHERE {} RETURNING 1\
             ), \
             inserted AS (\
               INSERT INTO {}.{} ({}) SELECT {} FROM latest WHERE {} > 0 RETURNING 1\
             ) \
             SELECT (SELECT count(*) FROM deleted), (SELECT count(*) FROM latest), \
                    (SELECT count(*) FROM inserted)",
            schema.clone(),
            staging.clone(),
            timestamp.clone(),
            Sql::param(1),
            columns.clone(),
            timestamp.clone(),
            diff.clone(),
            key_columns.clone(),
            columns.clone(),
            diff.clone(),
            key_columns,
            timestamp.clone(),
            diff.clone(),
            schema.clone(),
            target.clone(),
            key_match,
            schema.clone(),
            target.clone(),
            columns.clone(),
            columns.clone(),
            diff.clone(),
        )
    }

    /// Deletes exactly as many copies of each retracted row as were retracted.
    ///
    /// A keyless target may legitimately hold duplicates, so a plain
    /// `DELETE ... WHERE` would remove every copy instead of the retracted
    /// count. `ctid` picks out individual rows for that reason.
    ///
    /// NOTE: this join matches on every column and so cannot use an index. That
    /// is the cost of a sink without a key.
    fn keyless_retract_statement(
        schema: &Sql,
        target: &Sql,
        staging: &Sql,
        columns: &Sql,
        column_names: &[String],
        timestamp: &Sql,
        diff: &Sql,
    ) -> Sql {
        let column_match = Sql::join(
            column_names.iter().map(|column| {
                sql!(
                    "t.{} IS NOT DISTINCT FROM c.{}",
                    Sql::ident(column),
                    Sql::ident(column),
                )
            }),
            " AND ",
        );
        let target_columns = Sql::join(
            column_names
                .iter()
                .map(|column| sql!("t.{}", Sql::ident(column))),
            ", ",
        );
        sql!(
            "WITH retracted AS (\
               DELETE FROM {}.{} WHERE {} < {} AND {} < 0 RETURNING {}\
             ), \
             counts AS (SELECT {}, count(*) AS n FROM retracted GROUP BY {}), \
             ranked AS (\
               SELECT t.ctid, c.n, row_number() OVER (PARTITION BY {} ORDER BY t.ctid) AS rn \
               FROM {}.{} t JOIN counts c ON {}\
             ), \
             deleted AS (\
               DELETE FROM {}.{} WHERE ctid IN (SELECT ctid FROM ranked WHERE rn <= n) RETURNING 1\
             ) \
             SELECT (SELECT count(*) FROM retracted), (SELECT count(*) FROM deleted)",
            schema.clone(),
            staging.clone(),
            timestamp.clone(),
            Sql::param(1),
            diff.clone(),
            columns.clone(),
            columns.clone(),
            columns.clone(),
            target_columns,
            schema.clone(),
            target.clone(),
            column_match,
            schema.clone(),
            target.clone(),
        )
    }
}

/// The columns that identify a row in the target table, if the sink has any.
///
/// Prefers the user's `KEY`, then a natural key of the relation, matching the
/// order the sink's own arrangement uses.
fn key_columns(
    connection: &PostgresSinkConnection,
    from_desc: &RelationDesc,
) -> Option<Vec<String>> {
    let indices = connection
        .key_desc_and_indices
        .as_ref()
        .map(|(_, indices)| indices)
        .or(connection.relation_key_indices.as_ref())?;
    let names: Vec<_> = from_desc
        .iter_names()
        .map(|name| name.to_string())
        .collect();
    Some(indices.iter().map(|&index| names[index].clone()).collect())
}

/// The exclusive upper bound of the window a frontier closes.
///
/// An empty frontier closes every remaining timestamp.
fn frontier_bound(frontier: &Antichain<Timestamp>) -> Result<i64, anyhow::Error> {
    match frontier.as_option() {
        Some(time) => timestamp_to_i64(*time),
        None => Ok(i64::MAX),
    }
}

fn timestamp_to_i64(time: Timestamp) -> Result<i64, anyhow::Error> {
    i64::try_from(u64::from(time)).context("timestamp does not fit in a bigint")
}

/// Creates the schema, progress table, target table and staging table if they
/// do not exist, fences off connections left behind by an earlier incarnation
/// of the sink, truncates the staging table, and registers the sink in the
/// progress table.
///
/// Runs in one transaction. Only the active worker calls this, so the
/// `CREATE ... IF NOT EXISTS` statements never race. The `TRUNCATE` runs
/// unconditionally: the staging table must be empty whenever the copy
/// operators start, both on first creation and after a restart.
async fn create_postgres_tables(
    client: &Client,
    connection: &PostgresSinkConnection,
    from_desc: &RelationDesc,
    sink_id: GlobalId,
) -> Result<(), anyhow::Error> {
    let staging_name = connection.staging_table_name(sink_id);
    let mz_schema = Sql::ident(PostgresSinkConnection::MZ_SCHEMA);
    let progress = Sql::ident(PostgresSinkConnection::PROGRESS_TABLE);
    let schema = Sql::ident(&connection.schema);
    let target = Sql::ident(&connection.table);
    let staging = Sql::ident(&staging_name);

    let source_columns = || {
        from_desc
            .iter()
            .map(|(name, typ)| column_ddl(name.as_str(), &typ.scalar_type, typ.nullable))
    };
    let target_columns = Sql::join(source_columns(), ", ");
    let staging_columns = Sql::join(
        source_columns().chain([
            column_ddl(TIMESTAMP_COLUMN, &SqlScalarType::Int64, false),
            column_ddl(DIFF_COLUMN, &SqlScalarType::Int16, false),
        ]),
        ", ",
    );

    let statements = [
        sql!("BEGIN"),
        sql!("CREATE SCHEMA IF NOT EXISTS {}", mz_schema.clone()),
        sql!(
            "CREATE TABLE IF NOT EXISTS {}.{} (\
                sink_id text PRIMARY KEY, \
                sink_schema text NOT NULL, \
                sink_table text NOT NULL, \
                mz_frontier bigint NOT NULL\
            )",
            mz_schema.clone(),
            progress.clone(),
        ),
        sql!(
            "CREATE TABLE IF NOT EXISTS {}.{} ({})",
            schema.clone(),
            target,
            target_columns,
        ),
        sql!(
            "CREATE TABLE IF NOT EXISTS {}.{} ({})",
            schema.clone(),
            staging.clone(),
            staging_columns,
        ),
        sql!(
            "CREATE INDEX IF NOT EXISTS {} ON {}.{} ({})",
            Sql::ident(&format!("{staging_name}_{TIMESTAMP_COLUMN}_idx")),
            schema.clone(),
            staging.clone(),
            Sql::ident(TIMESTAMP_COLUMN),
        ),
        sql!(
            "CREATE INDEX IF NOT EXISTS {} ON {}.{} ({})",
            Sql::ident(&format!("{staging_name}_{DIFF_COLUMN}_idx")),
            schema.clone(),
            staging.clone(),
            Sql::ident(DIFF_COLUMN),
        ),
        // Connections of an earlier incarnation of this sink may still be
        // alive, either mid-COPY on the staging table or merely not yet reaped
        // by TCP keepalives. Either blocks the TRUNCATE below until the server
        // gives up on them, and a COPY that finished after the TRUNCATE would
        // leave stale rows in a table that must start empty.
        sql!(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
             WHERE application_name = {} AND pid <> pg_backend_pid()",
            Sql::literal(&application_name(sink_id)),
        ),
        sql!("TRUNCATE {}.{}", schema, staging),
        sql!(
            "INSERT INTO {}.{} (sink_id, sink_schema, sink_table, mz_frontier) \
             VALUES ({}, {}, {}, {}) ON CONFLICT (sink_id) DO NOTHING",
            mz_schema,
            progress,
            Sql::literal(&sink_id.to_string()),
            Sql::literal(&connection.schema),
            Sql::literal(&connection.table),
            Sql::from(u64::from(Timestamp::minimum())),
        ),
        sql!("COMMIT"),
    ];

    batch_execute(&**client, Sql::join(statements, ";\n")).await?;
    Ok(())
}

/// `application_name` carried by every connection a sink opens, so that a
/// later incarnation of the sink can find and terminate the connections of an
/// earlier one.
fn application_name(sink_id: GlobalId) -> String {
    format!("materialize_sink_{sink_id}")
}

async fn connect(
    connection: &PostgresSinkConnection,
    storage_configuration: &StorageConfiguration,
    sink_id: GlobalId,
    task_name: &str,
) -> Result<Client, anyhow::Error> {
    let config = connection
        .connection
        .config(
            &storage_configuration.connection_context.secrets_reader,
            storage_configuration,
            InTask::Yes,
        )
        .await?;
    let client = config
        .connect(
            task_name,
            &storage_configuration.connection_context.ssh_tunnel_manager,
        )
        .await?;
    batch_execute(
        &*client,
        sql!(
            "SET application_name = {}",
            Sql::literal(&application_name(sink_id))
        ),
    )
    .await?;
    Ok(client)
}

/// Hands the buffered rows to the COPY in progress, opening one first if there
/// is none.
///
/// A COPY is only ever open while it has rows in it, so an idle sink holds no
/// open transaction on the staging table.
async fn send_buffered(
    client: &Client,
    copy_stmt: &Sql,
    copy: &mut Option<Pin<Box<CopyInSink<Bytes>>>>,
    buf: &mut Vec<u8>,
) -> Result<(), anyhow::Error> {
    if buf.is_empty() {
        return Ok(());
    }
    if copy.is_none() {
        let mut sink = Box::pin(client.copy_in(copy_stmt.as_str()).await?);
        sink.send(Bytes::from_static(COPY_BINARY_HEADER)).await?;
        *copy = Some(sink);
    }
    let sink = copy.as_mut().expect("opened above");
    sink.send(Bytes::from(std::mem::take(buf))).await?;
    Ok(())
}

fn health_statuses<'scope>(
    errors: StreamVec<'scope, Timestamp, Rc<anyhow::Error>>,
) -> StreamVec<'scope, Timestamp, HealthStatusMessage> {
    errors.map(|error| HealthStatusMessage {
        id: None,
        update: HealthStatusUpdate::halting(error.display_with_causes().to_string(), None),
        namespace: StatusNamespace::Postgres,
    })
}

/// Returns the staging table's relation type: the source columns followed by
/// the timestamp (`bigint`) and diff (`smallint`) columns.
///
/// Errors if a source column cannot be created or binary-copied on a stock
/// PostgreSQL server, or if it collides with one of the added column names.
fn staging_relation_type(from_desc: &RelationDesc) -> Result<SqlRelationType, anyhow::Error> {
    let mut column_types = Vec::with_capacity(from_desc.arity() + 2);
    for (name, typ) in from_desc.iter() {
        if name.as_str() == TIMESTAMP_COLUMN || name.as_str() == DIFF_COLUMN {
            bail!("column name {name} is reserved by Postgres sinks");
        }
        check_postgres_type(&typ.scalar_type).with_context(|| format!("column {name}"))?;
        column_types.push(typ.clone());
    }
    column_types.push(SqlColumnType {
        scalar_type: SqlScalarType::Int64,
        nullable: false,
    });
    column_types.push(SqlColumnType {
        scalar_type: SqlScalarType::Int16,
        nullable: false,
    });
    Ok(SqlRelationType::new(column_types))
}

/// Errors if a stock PostgreSQL server cannot create a column of this type or
/// accept it in a binary `COPY`.
fn check_postgres_type(typ: &SqlScalarType) -> Result<(), anyhow::Error> {
    match typ {
        // Types whose OIDs exist only in Materialize.
        SqlScalarType::UInt16
        | SqlScalarType::UInt32
        | SqlScalarType::UInt64
        | SqlScalarType::List { .. }
        | SqlScalarType::Map { .. }
        | SqlScalarType::Record { .. }
        | SqlScalarType::MzTimestamp
        | SqlScalarType::MzAclItem
        | SqlScalarType::AclItem
        | SqlScalarType::Int2Vector => {
            bail!(
                "type {} is not supported by Postgres sinks",
                mz_pgrepr::Type::from(typ).name()
            )
        }
        SqlScalarType::Array(element_type) | SqlScalarType::Range { element_type } => {
            check_postgres_type(element_type)?
        }
        _ => {}
    }
    if let Err(err) = mz_pgrepr::Value::binary_encoding_error(typ) {
        bail!("{err}");
    }
    Ok(())
}

fn column_ddl(name: &str, typ: &SqlScalarType, nullable: bool) -> Sql {
    let pg_type = mz_pgrepr::Type::from(typ);
    let mut type_name = pg_type.name().to_string();
    if let Some(constraint) = pg_type.constraint() {
        write!(type_name, "{constraint}").expect("writing to a String cannot fail");
    }
    let not_null = if nullable {
        Sql::new("")
    } else {
        Sql::new(" NOT NULL")
    };
    sql!(
        "{} {}{}",
        Sql::ident(name),
        Sql::raw_unchecked(type_name),
        not_null
    )
}
