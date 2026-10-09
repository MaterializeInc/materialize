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
use std::rc::Rc;
use std::sync::Arc;
use std::{cell::RefCell, fmt::Write as _};

use anyhow::{Context, bail};
use differential_dataflow::{Hashable, VecCollection};
use mz_ore::cast::CastFrom;
use mz_ore::error::ErrorExt;
use mz_ore::future::InTask;
use mz_persist_client::Diagnostics;
use mz_persist_client::write::WriteHandle;
use mz_persist_types::codec_impls::UnitSchema;
use mz_postgres_util::{Client, Sql, batch_execute, sql};
use mz_repr::{
    Diff, GlobalId, RelationDesc, SqlColumnType, SqlRelationType, SqlScalarType, Timestamp,
};
use mz_storage_types::StorageDiff;
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sinks::{PostgresSinkConnection, StorageSinkDesc};
use mz_storage_types::sources::SourceData;
use mz_timely_util::builder_async::{OperatorBuilder, PressOnDropButton};
use timely::container::CapacityContainerBuilder;
use timely::dataflow::operators::vec::{Map, ToStream};
use timely::dataflow::operators::{CapabilitySet, Concatenate};
use timely::dataflow::{Scope, StreamVec};
use timely::progress::{Antichain, Timestamp as _};

use crate::healthcheck::{HealthStatusMessage, HealthStatusUpdate, StatusNamespace};
use crate::render::sinks::{SinkBatchStream, SinkRender};
use crate::statistics::SinkStatistics;
use crate::storage_state::StorageState;

/// Staging table column holding the Materialize timestamp of a row.
const TIMESTAMP_COLUMN: &str = "mz_timestamp";
/// Staging table column holding `1` for an insertion or `-1` for a retraction.
const DIFF_COLUMN: &str = "mz_diff";

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
    _name: String,
    _batches: SinkBatchStream<'scope>,
    _tables_ready: StreamVec<'scope, Timestamp, Infallible>,
    _connection: PostgresSinkConnection,
    _storage_configuration: StorageConfiguration,
    _from_desc: RelationDesc,
    _sink_id: GlobalId,
    _statistics: SinkStatistics,
) -> (
    StreamVec<'scope, Timestamp, Infallible>,
    StreamVec<'scope, Timestamp, HealthStatusMessage>,
    PressOnDropButton,
) {
    unimplemented!("Postgres sink staging operator")
}

/// Moves each completed timestamp window from the staging table into the target
/// table, advancing the sink's recorded frontier in the same transaction.
///
/// Runs on one worker, because the windows have to be applied in order and a
/// window is applied by a single transaction. Reads nothing from `staged`: that
/// stream carries no data, and its frontier is what says a timestamp is
/// complete in staging across every worker.
fn insert_into_target_table<'scope>(
    _name: String,
    _staged: StreamVec<'scope, Timestamp, Infallible>,
    _tables_ready: StreamVec<'scope, Timestamp, Infallible>,
    _connection: PostgresSinkConnection,
    _storage_configuration: StorageConfiguration,
    _from_desc: RelationDesc,
    _as_of: Antichain<Timestamp>,
    _sink_id: GlobalId,
    _statistics: SinkStatistics,
    _write_handle: impl Future<
        Output = Result<WriteHandle<SourceData, (), Timestamp, StorageDiff>, anyhow::Error>,
    > + 'static,
    _write_frontier: Rc<RefCell<Antichain<Timestamp>>>,
) -> (
    StreamVec<'scope, Timestamp, HealthStatusMessage>,
    PressOnDropButton,
) {
    unimplemented!("Postgres sink apply operator")
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
        ensure_deletable(&connection.schema, &connection.table),
        ensure_deletable(&connection.schema, &staging_name),
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

/// Gives `table` a replica identity when it would otherwise be undeletable.
///
/// A `CREATE PUBLICATION ... FOR ALL TABLES` in the target database covers
/// every table created after it, and PostgreSQL rejects `DELETE` on a
/// published table that has no replica identity. Both tables this sink creates
/// are deleted from on every window, so without this the first window fails
/// with `cannot delete from table ... because it does not have a replica
/// identity and publishes deletes`.
///
/// Only a table that would actually fail is altered. A target table the user
/// created with a primary key already has a replica identity, and replacing it
/// with the full row would make their own downstream replication match on every
/// column instead.
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
