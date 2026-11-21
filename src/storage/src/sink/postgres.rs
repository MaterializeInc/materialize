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
//! table, applies it to the target table, and records the new frontier in
//! `materialize.sink_progress`.
//!
//! The progress row is the fence. Each incarnation of the sink claims the row
//! by bumping its `epoch` at startup, and every window transaction checks that
//! the epoch is still its own before committing, so an incarnation that has
//! been superseded can commit nothing. The row is also the source of truth for
//! how far the sink has got: it moves in the same transaction as the rows, and
//! the persist progress shard is only brought up to it afterwards.

use std::cell::RefCell;
use std::fmt::Write as _;
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;

use anyhow::{Context, bail};
use differential_dataflow::{Hashable, VecCollection};
use mz_ore::cast::CastFrom;
use mz_ore::error::ErrorExt;
use mz_ore::future::InTask;
use mz_persist_client::Diagnostics;
use mz_persist_client::write::WriteHandle;
use mz_persist_types::codec_impls::UnitSchema;
use mz_postgres_util::{Client, Sql, batch_execute, query_one, query_opt, sql};
use mz_repr::{
    Diff, GlobalId, RelationDesc, SqlColumnType, SqlRelationType, SqlScalarType, Timestamp,
};
use mz_storage_types::StorageDiff;
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sinks::{PostgresSinkConnection, StorageSinkDesc};
use mz_storage_types::sources::SourceData;
use mz_timely_util::antichain::AntichainExt;
use mz_timely_util::builder_async::{OperatorBuilder, PressOnDropButton};
use timely::dataflow::StreamVec;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::Concatenate;
use timely::dataflow::operators::vec::{Map, ToStream};
use timely::progress::{Antichain, Timestamp as _};
use tracing::info;

use crate::healthcheck::{HealthStatusMessage, HealthStatusUpdate, StatusNamespace};
use crate::render::sinks::{SinkBatchStream, SinkRender};
use crate::statistics::SinkStatistics;
use crate::storage_state::StorageState;

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
/// Advisory lock key held while a sink creates its tables. Arbitrary, but
/// fixed so that every Postgres sink setting up against one database takes
/// the same lock.
const SETUP_LOCK: i64 = 0x6d7a_7369_6e6b;

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
    _name: String,
    _batches: SinkBatchStream<'scope>,
    _from_desc: RelationDesc,
) -> (
    StreamVec<'scope, Timestamp, EncodedRow>,
    StreamVec<'scope, Timestamp, HealthStatusMessage>,
    PressOnDropButton,
) {
    unimplemented!("Postgres sink encode operator")
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
    _statistics: SinkStatistics,
    _write_handle: impl Future<
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
    let _input = builder.new_disconnected_input(input, Exchange::new(move |_| hashed_id));

    let sink_version = sink.version;
    let from_desc = sink.from_desc.clone();

    let (button, errors) = builder.build_fallible(move |_caps| {
        Box::pin(async move {
            if !is_active_worker {
                // Leaving a frontier here would hold back what the controller
                // sees for the whole sink.
                write_frontier.borrow_mut().clear();
                return Ok::<(), anyhow::Error>(());
            }

            // Reject unsupported schemas before touching the target database.
            let statements = Statements::new(
                &connection.schema,
                &connection.table,
                &from_desc,
                key_indices(&connection),
                sink_id,
            )?;

            let client = connect(
                &connection,
                &storage_configuration,
                &format!("postgres-sink-{sink_id}"),
            )
            .await?;
            batch_execute(&*client, statements.create_tables.clone()).await?;

            let (epoch, recorded) = claim_progress_row(&client, &statements, sink_version).await?;
            info!(
                "{name}: recorded frontier: {}, epoch: {epoch}",
                recorded.pretty()
            );

            // TODO: Stage and apply the input.
            unimplemented!("Postgres sink write loop")
        })
    });

    (health_statuses(errors), button.press_on_drop())
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

/// Every statement the writer runs, built once per sink.
struct Statements {
    /// Creates the progress table, the target table and the progress row.
    create_tables: Sql,
    /// Claims the progress row for a new incarnation. Takes the sink version.
    claim_progress: Sql,
    read_version: Sql,
}

impl Statements {
    /// Builds the statements for sinking `from_desc` into `schema.table`,
    /// identifying rows by the columns at `key_indices` if there are any.
    fn new(
        schema: &str,
        table: &str,
        from_desc: &RelationDesc,
        key_indices: Option<&[usize]>,
        sink_id: GlobalId,
    ) -> Result<Self, anyhow::Error> {
        stage_relation_type(from_desc)?;
        let columns_by_index: Vec<_> = from_desc.iter().collect();
        let keys: Option<Vec<(String, bool)>> = key_indices.map(|indices| {
            indices
                .iter()
                .map(|&index| {
                    let (name, typ) = columns_by_index[index];
                    (name.to_string(), typ.nullable)
                })
                .collect()
        });

        let mz_schema = Sql::ident(PostgresSinkConnection::MZ_SCHEMA);
        let progress = Sql::ident(PostgresSinkConnection::PROGRESS_TABLE);
        let target_name = table;
        let schema_name = schema;
        let schema = Sql::ident(schema);
        let target = Sql::ident(table);
        let sink_id = Sql::literal(&sink_id.to_string());
        let source_columns = || {
            from_desc
                .iter()
                .map(|(name, typ)| column_ddl(name.as_str(), &typ.scalar_type, typ.nullable))
        };

        // A key with a nullable column cannot be a primary key, and a unique
        // index would let two rows with a NULL in the key coexist. Such a key
        // only gets an index to keep the delete off a sequential scan.
        let target_ddl = match &keys {
            None => sql!(
                "CREATE TABLE IF NOT EXISTS {}.{} ({})",
                schema.clone(),
                target.clone(),
                Sql::join(source_columns(), ", "),
            ),
            Some(keys) if keys.iter().all(|(_, nullable)| !nullable) => sql!(
                "CREATE TABLE IF NOT EXISTS {}.{} ({}, PRIMARY KEY ({}))",
                schema.clone(),
                target.clone(),
                Sql::join(source_columns(), ", "),
                Sql::join(keys.iter().map(|(key, _)| Sql::ident(key)), ", "),
            ),
            Some(keys) => sql!(
                "CREATE TABLE IF NOT EXISTS {}.{} ({}); \
                 CREATE INDEX IF NOT EXISTS {} ON {}.{} ({})",
                schema.clone(),
                target.clone(),
                Sql::join(source_columns(), ", "),
                Sql::ident(&format!("{target_name}_mz_key_idx")),
                schema.clone(),
                target.clone(),
                Sql::join(keys.iter().map(|(key, _)| Sql::ident(key)), ", "),
            ),
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

        Ok(Statements {
            create_tables,
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
        })
    }
}

/// The columns that identify a row in the target table, if the sink has any.
///
/// Prefers the user's `KEY`, then a natural key of the relation, matching the
/// order the sink's own arrangement uses.
fn key_indices(connection: &PostgresSinkConnection) -> Option<&[usize]> {
    connection
        .key_desc_and_indices
        .as_ref()
        .map(|(_, indices)| indices.as_slice())
        .or(connection.relation_key_indices.as_deref())
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
