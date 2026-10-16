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
//! Every worker encodes its share of the input into PostgreSQL `COPY ...
//! BINARY` rows. One worker, chosen by hashing the sink id, holds the only
//! connection to the target database and applies each completed timestamp
//! window in one transaction: it `COPY`s the window into a session temporary
//! table, replaces the affected keys in the target table, and records the new
//! frontier in `materialize.sink_progress`.
//!
//! The progress row is the fence. Each incarnation of the sink claims the row
//! by bumping its `epoch` at startup, and every window transaction checks that
//! the epoch is still its own before committing, so an incarnation that has
//! been superseded can commit nothing. The row is also the source of truth for
//! how far the sink has got: it moves in the same transaction as the rows, and
//! the persist progress shard is only brought up to it afterwards.

use std::cell::RefCell;
use std::cmp::Ordering;
use std::fmt::Write as _;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;

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
use mz_postgres_util::{Client, Sql, batch_execute, execute, query_one, query_opt, sql};
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
use timely::dataflow::StreamVec;
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::vec::{Map, ToStream};
use timely::dataflow::operators::{CapabilitySet, Concatenate};
use timely::progress::{Antichain, Timestamp as _};
use tokio_postgres::CopyInSink;
use tracing::info;

use crate::healthcheck::{HealthStatusMessage, HealthStatusUpdate, StatusNamespace};
use crate::render::sinks::{SinkBatchStream, SinkRender, SinkTrace};
use crate::statistics::SinkStatistics;
use crate::storage_state::StorageState;

type SinkBatch = <SinkTrace as TraceReader>::Batch;
type SinkCursor = <SinkBatch as Navigable>::Cursor;

/// One update, already encoded as a staging row in COPY BINARY format.
///
/// The bytes carry the source columns, the timestamp and a diff of `1` or
/// `-1`. The writer repeats them `|diff|` times, so the diff's magnitude is
/// kept alongside.
type EncodedRow = (Vec<u8>, Timestamp, Diff);

/// Staging column holding the Materialize timestamp of a row.
const TIMESTAMP_COLUMN: &str = "mz_timestamp";
/// Staging column holding `1` for an insertion or `-1` for a retraction.
const DIFF_COLUMN: &str = "mz_diff";
/// Session temporary table the writer stages each window in.
const STAGE_TABLE: &str = "mz_sink_stage";

/// Advisory lock key held while a sink creates its tables. Arbitrary, but
/// fixed so that every Postgres sink setting up against one database takes
/// the same lock.
const SETUP_LOCK: i64 = 0x6d7a_7369_6e6b;

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

        let (encoded, encode_status, encode_token) = encode_collection(
            format!("postgres-{sink_id}-encode"),
            batches,
            sink.from_desc.clone(),
        );

        let (write_status, write_token) = write_collection(
            format!("postgres-{sink_id}-write"),
            encoded,
            sink_id,
            self.clone(),
            storage_state.storage_configuration.clone(),
            sink,
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

        let status = scope.concatenate([running_status, encode_status, write_status]);

        (status, vec![encode_token, write_token])
    }
}

/// Encodes every update in `batches` as a staging row, on every worker.
fn encode_collection<'scope>(
    name: String,
    batches: SinkBatchStream<'scope>,
    from_desc: RelationDesc,
) -> (
    StreamVec<'scope, Timestamp, EncodedRow>,
    StreamVec<'scope, Timestamp, HealthStatusMessage>,
    PressOnDropButton,
) {
    let mut builder = OperatorBuilder::new(name, batches.scope());
    let (output, encoded) = builder.new_output::<CapacityContainerBuilder<Vec<EncodedRow>>>();
    let mut input = builder.new_input_for(batches, Pipeline, &output);

    let (button, errors) = builder.build_fallible(move |caps| {
        Box::pin(async move {
            let [capset]: &mut [_; 1] = caps.try_into().unwrap();
            let stage_typ = stage_relation_type(&from_desc)?;
            // Output is given at the input's capabilities, so holding this
            // one would only hold back the frontier the writer waits on.
            *capset = CapabilitySet::new();
            let mut row_buf = Row::default();
            let mut times: Vec<(Timestamp, Diff)> = Vec::new();

            while let Some(event) = input.next().await {
                let Event::Data(cap, batches) = event else {
                    continue;
                };
                for batch in &batches {
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
                                let bytes =
                                    encode_stage_row(&mut row_buf, val, &stage_typ, time, diff)?;
                                output.give(&cap, (bytes, time, diff));
                            }
                            cursor.step_val(batch);
                        }
                        cursor.step_key(batch);
                    }
                }
            }

            Ok::<(), anyhow::Error>(())
        })
    });

    (encoded, health_statuses(errors), button.press_on_drop())
}

/// Applies the encoded updates to the target database, one completed
/// timestamp window per transaction, on a single worker.
fn write_collection<'scope>(
    name: String,
    input: StreamVec<'scope, Timestamp, EncodedRow>,
    sink_id: GlobalId,
    connection: PostgresSinkConnection,
    storage_configuration: StorageConfiguration,
    sink: &StorageSinkDesc<CollectionMetadata, Timestamp>,
    statistics: SinkStatistics,
    write_handle: impl Future<
        Output = Result<WriteHandle<SourceData, (), Timestamp, StorageDiff>, anyhow::Error>,
    > + 'static,
    write_frontier: Rc<RefCell<Antichain<Timestamp>>>,
) -> (
    StreamVec<'scope, Timestamp, HealthStatusMessage>,
    PressOnDropButton,
) {
    let scope = input.scope();
    let hashed_id = sink_id.hashed();
    let is_active_worker = usize::cast_from(hashed_id) % scope.peers() == scope.index();
    let mut builder = OperatorBuilder::new(name.clone(), scope);
    let mut input = builder.new_disconnected_input(input, Exchange::new(move |_| hashed_id));

    let as_of = sink.as_of.clone();
    let sink_version = sink.version;
    let from_desc = sink.from_desc.clone();

    let (button, errors) = builder.build_fallible(move |_caps| {
        Box::pin(async move {
            if !is_active_worker {
                // Leaving a frontier here would hold back what the controller
                // sees for the whole sink.
                write_frontier.borrow_mut().clear();
                return Ok(());
            }

            // Reject unsupported schemas before touching the target database.
            let key_indices = key_indices(&connection)?;
            let statements = Statements::new(
                &connection.schema,
                &connection.table,
                &from_desc,
                key_indices,
                sink_id,
            )?;

            let mut write_handle = write_handle.await?;
            let client = connect(
                &connection,
                &storage_configuration,
                &format!("postgres-sink-{sink_id}"),
            )
            .await?;
            batch_execute(&*client, statements.create_tables.clone()).await?;

            let (epoch, recorded) = claim_progress_row(&client, &statements, sink_version).await?;
            let resume_upper =
                reconcile_frontiers(recorded, &mut write_handle, &write_frontier).await?;

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
            info!(
                "{name}: as_of: {}, resume upper: {}, epoch: {epoch}",
                as_of.pretty(),
                resume_upper.pretty()
            );

            // The loop below relies on `Timestamp` being totally ordered, so
            // work with timestamps directly.
            let Some(mut upper) = resume_upper.clone().into_option() else {
                write_frontier.borrow_mut().clear();
                return Ok(());
            };

            let mut writer = Writer::new(&client, &statements, epoch).await?;

            let mut deferred: Vec<EncodedRow> = Vec::new();
            while let Some(event) = input.next().await {
                match event {
                    Event::Data(_cap, rows) => {
                        for (bytes, time, diff) in rows {
                            // Every time below `upper` is committed. An update
                            // exactly at `upper` is the minimum pending time,
                            // so it can go straight into the open window. That
                            // is what streams a snapshot into the staging
                            // table instead of buffering it in memory.
                            match upper.cmp(&time) {
                                Ordering::Less => deferred.push((bytes, time, diff)),
                                Ordering::Equal => {
                                    writer.begin(upper).await?;
                                    writer.stage(&bytes, diff).await?;
                                    statistics.inc_messages_staged_by(diff.unsigned_abs());
                                    statistics.inc_bytes_staged_by(
                                        u64::cast_from(bytes.len()) * diff.unsigned_abs(),
                                    );
                                }
                                Ordering::Greater => continue,
                            }
                        }
                    }
                    Event::Progress(progress) => {
                        if !PartialOrder::less_equal(&resume_upper, &progress) {
                            continue;
                        }
                        // Only commit once strictly beyond the as_of. A sink
                        // restarted with an earlier as_of replays its snapshot
                        // at the earlier time, and recording progress before
                        // then would skip it.
                        if !as_of.iter().all(|t| !progress.less_equal(t)) {
                            continue;
                        }

                        writer.begin(upper).await?;
                        let closed =
                            deferred.extract_if(.., |(_, time, _)| !progress.less_equal(time));
                        for (bytes, _, diff) in closed {
                            writer.stage(&bytes, diff).await?;
                            statistics.inc_messages_staged_by(diff.unsigned_abs());
                            statistics.inc_bytes_staged_by(
                                u64::cast_from(bytes.len()) * diff.unsigned_abs(),
                            );
                        }
                        let committed = writer.commit(&progress).await?;
                        statistics.inc_messages_committed_by(committed);

                        // Only now that the rows are durable in Postgres does
                        // the progress shard move. See `reconcile_frontiers`
                        // for why this order and not the reverse.
                        advance_persist(&mut write_handle, &progress).await;
                        write_frontier.borrow_mut().clone_from(&progress);

                        match progress.into_option() {
                            Some(new_upper) => upper = new_upper,
                            None => break,
                        }
                    }
                }
            }

            Ok::<(), anyhow::Error>(())
        })
    });

    (health_statuses(errors), button.press_on_drop())
}

/// A claimed sink and its in-flight window on one connection.
struct Writer<'a> {
    client: &'a tokio_postgres::Client,
    statements: &'a Statements,
    /// The epoch this incarnation claimed. A window commits only while the
    /// progress row still carries it.
    epoch: i64,
    copy: Option<Pin<Box<CopyInSink<Bytes>>>>,
    buf: Vec<u8>,
    /// Rows staged in the open window.
    staged: u64,
    in_transaction: bool,
}

impl<'a> Writer<'a> {
    /// Creates the session's staging table.
    async fn new(
        client: &'a tokio_postgres::Client,
        statements: &'a Statements,
        epoch: i64,
    ) -> Result<Self, anyhow::Error> {
        batch_execute(client, statements.create_stage.clone()).await?;
        Ok(Writer {
            client,
            statements,
            epoch,
            copy: None,
            buf: Vec::new(),
            staged: 0,
            in_transaction: false,
        })
    }

    /// Opens the window transaction for `[lower, ..)` unless it is open.
    ///
    /// Checks the progress row before any row is staged. The checks are
    /// repeated by `commit`, so this one only fails fast.
    async fn begin(&mut self, lower: Timestamp) -> Result<(), anyhow::Error> {
        if self.in_transaction {
            return Ok(());
        }
        batch_execute(self.client, sql!("BEGIN ISOLATION LEVEL SERIALIZABLE")).await?;
        self.in_transaction = true;

        let row = query_one(self.client, self.statements.read_progress.clone(), &[]).await?;
        let (epoch, frontier): (i64, i64) = (row.get(0), row.get(1));
        if epoch != self.epoch {
            bail!(
                "fenced off by a newer incarnation of the sink: ours={}, theirs={}",
                self.epoch,
                epoch
            );
        }
        let lower = timestamp_to_i64(lower)?;
        if frontier != lower {
            bail!("sink progress changed underneath the sink: expected {lower}, found {frontier}");
        }
        Ok(())
    }

    /// Adds `|diff|` copies of an encoded row to the open window.
    async fn stage(&mut self, bytes: &[u8], diff: Diff) -> Result<(), anyhow::Error> {
        let n = diff.unsigned_abs();
        for _ in 0..n {
            self.buf.extend_from_slice(bytes);
        }
        self.staged += n;
        if self.buf.len() >= COPY_CHUNK_BYTES {
            self.flush().await?;
        }
        Ok(())
    }

    /// Hands the buffered rows to the COPY in progress, opening one first if
    /// there is none.
    async fn flush(&mut self) -> Result<(), anyhow::Error> {
        if self.buf.is_empty() {
            return Ok(());
        }
        if self.copy.is_none() {
            let mut copy = Box::pin(self.client.copy_in(self.statements.copy.as_str()).await?);
            copy.send(Bytes::from_static(COPY_BINARY_HEADER)).await?;
            self.copy = Some(copy);
        }
        let copy = self.copy.as_mut().expect("opened above");
        copy.send(Bytes::from(std::mem::take(&mut self.buf)))
            .await?;
        Ok(())
    }

    /// Applies the staged window to the target table, records `progress` as
    /// the sink's frontier and commits. Returns the number of rows applied.
    async fn commit(&mut self, progress: &Antichain<Timestamp>) -> Result<u64, anyhow::Error> {
        self.flush().await?;
        if let Some(mut copy) = self.copy.take() {
            copy.send(Bytes::from_static(COPY_BINARY_TRAILER)).await?;
            copy.as_mut().finish().await?;
        }

        if self.staged > 0 {
            let row = query_one(self.client, self.statements.apply.clone(), &[]).await?;
            let (deleted, touched): (i64, i64) = (row.get(0), row.get(1));
            if deleted > touched {
                bail!(
                    "deleted {deleted} rows for {touched} keys, so the target table holds \
                     more than one row per key"
                );
            }
        }

        let upper = frontier_bound(progress)?;
        let updated = execute(
            self.client,
            self.statements.record_progress.clone(),
            &[&upper, &self.epoch],
        )
        .await?;
        if updated != 1 {
            bail!(
                "fenced off by a newer incarnation of the sink: epoch {} is stale",
                self.epoch
            );
        }
        batch_execute(self.client, sql!("COMMIT")).await?;

        self.in_transaction = false;
        Ok(std::mem::take(&mut self.staged))
    }
}

/// Registers the sink in the progress table if needed and claims the row for
/// this incarnation by bumping its epoch.
///
/// Returns the claimed epoch and the recorded frontier. Fails if the row
/// belongs to a newer version of the sink.
async fn claim_progress_row(
    client: &tokio_postgres::Client,
    statements: &Statements,
    sink_version: u64,
) -> Result<(i64, Antichain<Timestamp>), anyhow::Error> {
    let version = i64::try_from(sink_version).context("sink version does not fit in a bigint")?;
    let row = query_opt(client, statements.claim_progress.clone(), &[&version]).await?;
    let Some(row) = row else {
        let row = query_one(client, statements.read_version.clone(), &[]).await?;
        let theirs: i64 = row.get(0);
        bail!("fenced off by a newer version of the sink: ours={version}, theirs={theirs}");
    };
    let (epoch, recorded): (i64, i64) = (row.get(0), row.get(1));
    let recorded = u64::try_from(recorded).context(
        "sink_progress.mz_frontier is negative, which no frontier this sink writes can be",
    )?;
    Ok((epoch, Antichain::from_elem(Timestamp::from(recorded))))
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
    recorded: Antichain<Timestamp>,
    write_handle: &mut WriteHandle<SourceData, (), Timestamp, StorageDiff>,
    write_frontier: &Rc<RefCell<Antichain<Timestamp>>>,
) -> Result<Antichain<Timestamp>, anyhow::Error> {
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

/// Every statement the writer runs, built once per sink.
struct Statements {
    /// Creates the progress table, the target table and the progress row.
    create_tables: Sql,
    /// Creates the session temporary staging table.
    create_stage: Sql,
    /// Claims the progress row for a new incarnation. Takes the sink version.
    claim_progress: Sql,
    read_version: Sql,
    /// Reads the progress row's epoch and frontier.
    read_progress: Sql,
    /// Records a new frontier. Takes the frontier and the expected epoch.
    record_progress: Sql,
    copy: Sql,
    /// Replaces every key the staged window touches with its final state.
    apply: Sql,
}

impl Statements {
    /// Builds the statements for sinking `from_desc` into `schema.table`,
    /// identifying rows by the columns at `key_indices`.
    fn new(
        schema: &str,
        table: &str,
        from_desc: &RelationDesc,
        key_indices: &[usize],
        sink_id: GlobalId,
    ) -> Result<Self, anyhow::Error> {
        stage_relation_type(from_desc)?;
        let columns_by_index: Vec<_> = from_desc.iter().collect();
        let keys: Vec<(String, bool)> = key_indices
            .iter()
            .map(|&index| {
                let (name, typ) = columns_by_index[index];
                (name.to_string(), typ.nullable)
            })
            .collect();

        let mz_schema = Sql::ident(PostgresSinkConnection::MZ_SCHEMA);
        let progress = Sql::ident(PostgresSinkConnection::PROGRESS_TABLE);
        let target_name = table;
        let schema_name = schema;
        let schema = Sql::ident(schema);
        let target = Sql::ident(table);
        let stage = sql!("pg_temp.{}", Sql::ident(STAGE_TABLE));
        let sink_id = Sql::literal(&sink_id.to_string());
        let timestamp = Sql::ident(TIMESTAMP_COLUMN);
        let diff = Sql::ident(DIFF_COLUMN);
        let columns = Sql::join(
            from_desc.iter_names().map(|name| Sql::ident(name.as_str())),
            ", ",
        );
        let key_columns = Sql::join(keys.iter().map(|(key, _)| Sql::ident(key)), ", ");
        let source_columns = || {
            from_desc
                .iter()
                .map(|(name, typ)| column_ddl(name.as_str(), &typ.scalar_type, typ.nullable))
        };

        // A key with a nullable column cannot be a primary key, and a unique
        // index would let two rows with a NULL in the key coexist. Such a key
        // only gets an index to keep the delete off a sequential scan.
        let key_is_primary = keys.iter().all(|(_, nullable)| !nullable);
        let target_ddl = if key_is_primary {
            sql!(
                "CREATE TABLE IF NOT EXISTS {}.{} ({}, PRIMARY KEY ({}))",
                schema.clone(),
                target.clone(),
                Sql::join(source_columns(), ", "),
                key_columns.clone(),
            )
        } else {
            sql!(
                "CREATE TABLE IF NOT EXISTS {}.{} ({}); \
                 CREATE INDEX IF NOT EXISTS {} ON {}.{} ({})",
                schema.clone(),
                target.clone(),
                Sql::join(source_columns(), ", "),
                Sql::ident(&format!("{target_name}_mz_key_idx")),
                schema.clone(),
                target.clone(),
                key_columns.clone(),
            )
        };

        // Sinks starting at the same time against one database would race
        // each other's `CREATE ... IF NOT EXISTS`, which can fail on a
        // duplicate key when both find the object missing. The advisory lock
        // serializes setup and is released by the COMMIT.
        let create_tables = Sql::join(
            [
                sql!("BEGIN"),
                sql!("SELECT pg_advisory_xact_lock({})", Sql::from(SETUP_LOCK)),
                sql!("CREATE SCHEMA IF NOT EXISTS {}", mz_schema.clone()),
                sql!(
                    "CREATE TABLE IF NOT EXISTS {}.{} (\
                        sink_id text PRIMARY KEY, \
                        sink_schema text NOT NULL, \
                        sink_table text NOT NULL, \
                        mz_frontier bigint NOT NULL, \
                        sink_version bigint NOT NULL, \
                        epoch bigint NOT NULL\
                    )",
                    mz_schema.clone(),
                    progress.clone(),
                ),
                target_ddl,
                ensure_deletable(schema_name, target_name),
                sql!(
                    "INSERT INTO {}.{} (sink_id, sink_schema, sink_table, mz_frontier, sink_version, epoch) \
                     VALUES ({}, {}, {}, {}, 0, 0) ON CONFLICT (sink_id) DO NOTHING",
                    mz_schema.clone(),
                    progress.clone(),
                    sink_id.clone(),
                    Sql::literal(schema_name),
                    Sql::literal(target_name),
                    Sql::from(u64::from(Timestamp::minimum())),
                ),
                sql!("COMMIT"),
            ],
            ";\n",
        );

        // `ON COMMIT DELETE ROWS` empties the staging table whenever a window
        // commits, and a rollback discards what was staged. The table lives
        // for the session, so it costs one catalog entry per connection.
        let create_stage = sql!(
            "CREATE TEMPORARY TABLE {} ({}) ON COMMIT DELETE ROWS",
            Sql::ident(STAGE_TABLE),
            Sql::join(
                source_columns().chain([
                    column_ddl(TIMESTAMP_COLUMN, &SqlScalarType::Int64, false),
                    column_ddl(DIFF_COLUMN, &SqlScalarType::Int16, false),
                ]),
                ", ",
            ),
        );

        let copy = sql!(
            "COPY {} ({}, {}, {}) FROM STDIN (FORMAT BINARY)",
            stage.clone(),
            columns.clone(),
            timestamp.clone(),
            diff.clone(),
        );

        // `IS NOT DISTINCT FROM` matches NULL keys but cannot use an index,
        // so it is only used for columns that can hold NULL.
        let key_match = Sql::join(
            keys.iter().map(|(key, nullable)| {
                let key = Sql::ident(key);
                if *nullable {
                    sql!("t.{} IS NOT DISTINCT FROM l.{}", key.clone(), key)
                } else {
                    sql!("t.{} = l.{}", key.clone(), key)
                }
            }),
            " AND ",
        );

        // A window may hold several timestamps for one key. `DISTINCT ON`
        // keeps the latest, and at that timestamp the insertion over the
        // retraction, so an update lands as its new value and a delete as
        // nothing. Every key the window touches is deleted and the survivors
        // reinserted. The data-modifying CTEs all read the same snapshot, so
        // the delete never sees the rows the insert adds.
        let apply = sql!(
            "WITH latest AS (\
               SELECT DISTINCT ON ({}) {}, {} FROM {} ORDER BY {}, {} DESC, {} DESC\
             ), \
             deleted AS (\
               DELETE FROM {}.{} t USING latest l WHERE {} RETURNING 1\
             ), \
             inserted AS (\
               INSERT INTO {}.{} ({}) SELECT {} FROM latest WHERE {} > 0 RETURNING 1\
             ) \
             SELECT (SELECT count(*) FROM deleted), (SELECT count(*) FROM latest), \
                    (SELECT count(*) FROM inserted)",
            key_columns.clone(),
            columns.clone(),
            diff.clone(),
            stage,
            key_columns,
            timestamp,
            diff.clone(),
            schema.clone(),
            target.clone(),
            key_match,
            schema,
            target,
            columns.clone(),
            columns,
            diff,
        );

        Ok(Statements {
            create_tables,
            create_stage,
            claim_progress: sql!(
                "UPDATE {}.{} SET epoch = epoch + 1, sink_version = {} \
                 WHERE sink_id = {} AND sink_version <= {} RETURNING epoch, mz_frontier",
                mz_schema.clone(),
                progress.clone(),
                Sql::param(1),
                sink_id.clone(),
                Sql::param(1),
            ),
            read_version: sql!(
                "SELECT sink_version FROM {}.{} WHERE sink_id = {}",
                mz_schema.clone(),
                progress.clone(),
                sink_id.clone(),
            ),
            read_progress: sql!(
                "SELECT epoch, mz_frontier FROM {}.{} WHERE sink_id = {}",
                mz_schema.clone(),
                progress.clone(),
                sink_id.clone(),
            ),
            record_progress: sql!(
                "UPDATE {}.{} SET mz_frontier = {} WHERE sink_id = {} AND epoch = {}",
                mz_schema,
                progress,
                Sql::param(1),
                sink_id,
                Sql::param(2),
            ),
            copy,
            apply,
        })
    }
}

/// The columns that identify a row in the target table.
///
/// Prefers the user's `KEY`, then a natural key of the relation, matching the
/// order the sink's own arrangement uses.
fn key_indices(connection: &PostgresSinkConnection) -> Result<&[usize], anyhow::Error> {
    connection
        .key_desc_and_indices
        .as_ref()
        .map(|(_, indices)| indices.as_slice())
        .or(connection.relation_key_indices.as_deref())
        .ok_or_else(|| anyhow::anyhow!("Postgres sinks require a key"))
}

/// Encodes `source` followed by `time` and the sign of `diff` as one COPY
/// BINARY staging row, using `row_buf` as scratch space.
fn encode_stage_row<'d>(
    row_buf: &mut Row,
    source: impl IntoIterator<Item = Datum<'d>>,
    stage_typ: &SqlRelationType,
    time: Timestamp,
    diff: Diff,
) -> Result<Vec<u8>, anyhow::Error> {
    let mut packer = row_buf.packer();
    packer.extend(source);
    packer.push(Datum::Int64(timestamp_to_i64(time)?));
    packer.push(Datum::Int16(if diff.is_negative() { -1 } else { 1 }));
    let mut bytes = Vec::new();
    encode_copy_format(
        &CopyFormatParams::Binary,
        row_buf,
        stage_typ,
        &mut bytes,
        TextEncodeSettings::STABLE,
    )?;
    Ok(bytes)
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

/// Gives `table` a replica identity when it would otherwise be undeletable.
///
/// A `CREATE PUBLICATION ... FOR ALL TABLES` in the target database covers
/// every table created after it, and PostgreSQL rejects `DELETE` on a
/// published table that has no replica identity. The target table is deleted
/// from on every window, so without this the first window fails with `cannot
/// delete from table ... because it does not have a replica identity and
/// publishes deletes`.
///
/// Only a table that would actually fail is altered. A target table with a
/// primary key already has a replica identity, and replacing it with the full
/// row would make the user's own downstream replication match on every column
/// instead.
fn ensure_deletable(schema: &str, table: &str) -> Sql {
    let qualified = Sql::literal(&format!(
        "{}.{}",
        Sql::ident(schema).as_str(),
        Sql::ident(table).as_str()
    ));
    sql!(
        "DO $mz$ DECLARE t regclass := to_regclass({}); BEGIN \
         IF (SELECT relreplident FROM pg_class WHERE oid = t) = 'd' \
         AND NOT EXISTS (SELECT 1 FROM pg_index WHERE indrelid = t AND indisprimary) \
         THEN EXECUTE format('ALTER TABLE %s REPLICA IDENTITY FULL', t); \
         END IF; END $mz$",
        qualified,
    )
}

async fn connect(
    connection: &PostgresSinkConnection,
    storage_configuration: &StorageConfiguration,
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
    Ok(client)
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
fn stage_relation_type(from_desc: &RelationDesc) -> Result<SqlRelationType, anyhow::Error> {
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

#[cfg(test)]
mod tests {
    use mz_repr::GlobalId;
    use tokio_postgres::NoTls;

    use super::*;

    /// A sink into a fresh schema of the database at `POSTGRES_URL`, or
    /// `None` when no database is available.
    struct Fixture {
        schema: String,
        sink_id: GlobalId,
        statements: Statements,
        stage_typ: SqlRelationType,
    }

    impl Fixture {
        async fn new() -> Option<(tokio_postgres::Client, Fixture)> {
            let client = connect_test().await?;
            let unique = uuid::Uuid::new_v4();
            let schema = format!("pgsink_test_{}", unique.simple());
            let sink_id = GlobalId::User(unique.as_u64_pair().0);
            let desc = RelationDesc::builder()
                .with_column("id", SqlScalarType::Int32.nullable(false))
                .with_column("v", SqlScalarType::String.nullable(true))
                .finish();
            let statements = Statements::new(&schema, "target", &desc, &[0], sink_id).unwrap();
            batch_execute(&client, sql!("CREATE SCHEMA {}", Sql::ident(&schema)))
                .await
                .unwrap();
            batch_execute(&client, statements.create_tables.clone())
                .await
                .unwrap();
            let fixture = Fixture {
                schema,
                sink_id,
                statements,
                stage_typ: stage_relation_type(&desc).unwrap(),
            };
            Some((client, fixture))
        }

        fn row(&self, id: i32, v: Option<&str>, time: u64, diff: i64) -> Vec<u8> {
            let v = v.map_or(Datum::Null, Datum::String);
            encode_stage_row(
                &mut Row::default(),
                [Datum::Int32(id), v],
                &self.stage_typ,
                Timestamp::from(time),
                Diff::from(diff),
            )
            .unwrap()
        }

        async fn target(&self, client: &tokio_postgres::Client) -> Vec<(i32, Option<String>)> {
            let rows = mz_postgres_util::query(
                client,
                sql!(
                    "SELECT id, v FROM {}.target ORDER BY id",
                    Sql::ident(&self.schema)
                ),
                &[],
            )
            .await
            .unwrap();
            rows.iter().map(|row| (row.get(0), row.get(1))).collect()
        }

        async fn frontier(&self, client: &tokio_postgres::Client) -> i64 {
            let row = query_one(client, self.statements.read_progress.clone(), &[])
                .await
                .unwrap();
            row.get(1)
        }

        async fn drop(self, client: &tokio_postgres::Client) {
            batch_execute(
                client,
                sql!(
                    "DROP SCHEMA {} CASCADE; DELETE FROM materialize.sink_progress WHERE sink_id = {}",
                    Sql::ident(&self.schema),
                    Sql::literal(&self.sink_id.to_string()),
                ),
            )
            .await
            .unwrap();
        }
    }

    async fn connect_test() -> Option<tokio_postgres::Client> {
        let url = std::env::var("POSTGRES_URL").ok()?;
        let (client, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
        mz_ore::task::spawn(|| "postgres-sink-test", async move {
            connection.await.unwrap();
        });
        Some(client)
    }

    fn frontier(time: u64) -> Antichain<Timestamp> {
        Antichain::from_elem(Timestamp::from(time))
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // needs a database
    async fn windows_replace_keys() {
        let Some((client, fx)) = Fixture::new().await else {
            return;
        };
        let (epoch, recorded) = claim_progress_row(&client, &fx.statements, 0)
            .await
            .unwrap();
        assert_eq!(recorded, frontier(0));
        let mut writer = Writer::new(&client, &fx.statements, epoch).await.unwrap();

        // The primary key makes the delete an index lookup.
        let row = query_one(
            &client,
            sql!(
                "SELECT count(*) FROM pg_index WHERE indrelid = {}::regclass AND indisprimary",
                Sql::literal(&format!("{}.target", Sql::ident(&fx.schema).as_str())),
            ),
            &[],
        )
        .await
        .unwrap();
        assert_eq!(row.get::<_, i64>(0), 1);

        writer.begin(Timestamp::from(0)).await.unwrap();
        for bytes in [fx.row(1, Some("a"), 0, 1), fx.row(2, None, 1, 1)] {
            writer.stage(&bytes, Diff::ONE).await.unwrap();
        }
        assert_eq!(writer.commit(&frontier(2)).await.unwrap(), 2);
        assert_eq!(
            fx.target(&client).await,
            vec![(1, Some("a".into())), (2, None)]
        );
        assert_eq!(fx.frontier(&client).await, 2);

        // One window holding an update, a delete, and an insert that is
        // retracted again at a later timestamp.
        writer.begin(Timestamp::from(2)).await.unwrap();
        let updates = [
            fx.row(1, Some("a"), 2, -1),
            fx.row(1, Some("b"), 2, 1),
            fx.row(2, None, 3, -1),
            fx.row(3, Some("c"), 3, 1),
            fx.row(3, Some("c"), 4, -1),
        ];
        for bytes in updates {
            writer.stage(&bytes, Diff::ONE).await.unwrap();
        }
        assert_eq!(writer.commit(&frontier(5)).await.unwrap(), 5);
        assert_eq!(fx.target(&client).await, vec![(1, Some("b".into()))]);
        assert_eq!(fx.frontier(&client).await, 5);

        // An empty window still records progress.
        writer.begin(Timestamp::from(5)).await.unwrap();
        assert_eq!(writer.commit(&frontier(6)).await.unwrap(), 0);
        assert_eq!(fx.frontier(&client).await, 6);

        // A window has to start where the last one ended.
        let err = writer.begin(Timestamp::from(7)).await.unwrap_err();
        assert!(err.to_string().contains("changed underneath"), "{err}");
        batch_execute(&client, sql!("ROLLBACK")).await.unwrap();

        // Reclaiming resumes from the recorded frontier.
        let (epoch, recorded) = claim_progress_row(&client, &fx.statements, 0)
            .await
            .unwrap();
        assert_eq!(epoch, 2);
        assert_eq!(recorded, frontier(6));

        fx.drop(&client).await;
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // needs a database
    async fn newer_incarnation_fences_older() {
        let Some((old, fx)) = Fixture::new().await else {
            return;
        };
        let new = connect_test().await.unwrap();

        let (old_epoch, _) = claim_progress_row(&old, &fx.statements, 0).await.unwrap();
        let mut old_writer = Writer::new(&old, &fx.statements, old_epoch).await.unwrap();
        old_writer.begin(Timestamp::from(0)).await.unwrap();
        let bytes = fx.row(1, Some("stale"), 0, 1);
        old_writer.stage(&bytes, Diff::ONE).await.unwrap();

        // The new incarnation claims the row while the old window is open.
        let (new_epoch, _) = claim_progress_row(&new, &fx.statements, 0).await.unwrap();
        assert!(new_epoch > old_epoch);

        // The old window cannot commit, and nothing of it reaches the target.
        assert!(old_writer.commit(&frontier(1)).await.is_err());
        batch_execute(&old, sql!("ROLLBACK")).await.unwrap();
        assert_eq!(fx.target(&new).await, vec![]);
        assert_eq!(fx.frontier(&new).await, 0);

        // Nor can a later window of the old incarnation start.
        let stale = connect_test().await.unwrap();
        let mut stale_writer = Writer::new(&stale, &fx.statements, old_epoch)
            .await
            .unwrap();
        let err = stale_writer.begin(Timestamp::from(0)).await.unwrap_err();
        assert!(err.to_string().contains("fenced off"), "{err}");

        let mut new_writer = Writer::new(&new, &fx.statements, new_epoch).await.unwrap();
        new_writer.begin(Timestamp::from(0)).await.unwrap();
        let bytes = fx.row(1, Some("fresh"), 0, 1);
        new_writer.stage(&bytes, Diff::ONE).await.unwrap();
        assert_eq!(new_writer.commit(&frontier(1)).await.unwrap(), 1);
        assert_eq!(fx.target(&new).await, vec![(1, Some("fresh".into()))]);

        fx.drop(&new).await;
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // needs a database
    async fn newer_version_fences_older() {
        let Some((client, fx)) = Fixture::new().await else {
            return;
        };
        claim_progress_row(&client, &fx.statements, 2)
            .await
            .unwrap();
        let err = claim_progress_row(&client, &fx.statements, 1)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("newer version"), "{err}");
        // The same version may reclaim, as a restart does.
        claim_progress_row(&client, &fx.statements, 2)
            .await
            .unwrap();
        fx.drop(&client).await;
    }
}
