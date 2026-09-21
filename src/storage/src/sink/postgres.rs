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
//! The write path has two stages. The setup operator, on a single worker,
//! creates the target table, the staging table and the shared progress table,
//! and truncates the staging table. Once it releases its barrier every worker
//! streams its share of updates into the staging table with `COPY ... FROM
//! STDIN`, tagging each row with the Materialize timestamp and a diff of `1`
//! or `-1`. Rows are copied as soon as they arrive, including rows beyond the
//! input frontier. Correctness rests on two rules: only completed timestamps
//! are ever moved from staging into the target table, and the staging table is
//! truncated on every dataflow start. A COPY that fails on any worker halts the
//! sink, which restarts the whole dataflow and therefore re-runs setup, because
//! after a failed COPY there is no way to know which rows landed.

use std::convert::Infallible;
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
use mz_postgres_util::{Client, Sql, batch_execute, sql};
use mz_repr::{
    Datum, Diff, GlobalId, RelationDesc, Row, SqlColumnType, SqlRelationType, SqlScalarType,
    Timestamp,
};
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sinks::{PostgresSinkConnection, StorageSinkDesc};
use mz_storage_types::sources::SourceData;
use mz_timely_util::builder_async::{Event, OperatorBuilder, PressOnDropButton};
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

        // TODO: consumed by the staging-to-target step, which advances the
        // progress shard in lockstep with the progress table.
        let _write_handle = {
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
            tables_ready,
            self.clone(),
            storage_state.storage_configuration.clone(),
            sink.from_desc.clone(),
            sink_id,
            statistics,
        );

        insert_into_target_table(
            format!("postgres-{sink_id}-insert-target"),
            staged,
            self,
            sink_id,
        );

        let running_status = Some(HealthStatusMessage {
            id: None,
            update: HealthStatusUpdate::Running,
            namespace: StatusNamespace::Postgres,
        })
        .to_stream(scope);

        let status = scope.concatenate([running_status, setup_status, copy_status]);

        (status, vec![setup_token, copy_token])
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

/// TODO: move completed timestamps from the staging table into the target
/// table and advance the sink's row in the progress table, skipping rows at or
/// below the frontier recorded there.
fn insert_into_target_table<'scope>(
    _name: String,
    _staged: StreamVec<'scope, Timestamp, Infallible>,
    _connection: &PostgresSinkConnection,
    _sink_id: GlobalId,
) {
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
