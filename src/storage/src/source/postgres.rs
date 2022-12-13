// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::error::Error;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail};
use futures::{FutureExt, StreamExt};
use once_cell::sync::Lazy;
use postgres_protocol::message::backend::{
    LogicalReplicationMessage, ReplicationMessage, TupleData,
};
use timely::scheduling::SyncActivator;
use tokio::runtime::Handle as TokioHandle;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_postgres::error::DbError;
use tokio_postgres::replication::LogicalReplicationStream;
use tokio_postgres::types::PgLsn;
use tokio_postgres::SimpleQueryMessage;
use tracing::{error, info, warn};

use mz_expr::{MirScalarExpr, PartitionId};
use mz_ore::{halt, task};
use mz_postgres_util::desc::PostgresTableDesc;
use mz_repr::{Datum, DatumVec, Diff, GlobalId, Row};
use mz_storage_client::types::connections::ConnectionContext;
use mz_storage_client::types::errors::SourceErrorDetails;
use mz_storage_client::types::sources::{
    encoding::SourceDataEncoding, MzOffset, PostgresSourceConnection,
};

use self::metrics::PgSourceMetrics;
use super::metrics::SourceBaseMetrics;
use crate::source::commit::LogCommitter;

use crate::source::source_reader_pipeline::HealthStatus;
use crate::source::types::{OffsetCommitter, SourceConnectionBuilder};
use crate::source::{
    NextMessage, SourceMessage, SourceMessageType, SourceReader, SourceReaderError,
};

mod metrics;

/// Postgres epoch is 2000-01-01T00:00:00Z
static PG_EPOCH: Lazy<SystemTime> = Lazy::new(|| UNIX_EPOCH + Duration::from_secs(946_684_800));

/// How often a status update message should be sent to the server
static FEEDBACK_INTERVAL: Duration = Duration::from_secs(30);

/// The amount of time we should wait after the last received message before worrying about WAL lag
static WAL_LAG_GRACE_PERIOD: Duration = Duration::from_secs(5 * 60); // 5 minutes

/// The maximum amount of WAL lag allowed before restarting the replication process
static MAX_WAL_LAG: u64 = 100 * 1024 * 1024;

trait ErrorExt {
    fn is_definite(&self) -> bool;
}

impl ErrorExt for tokio::time::error::Elapsed {
    fn is_definite(&self) -> bool {
        false
    }
}

impl ErrorExt for tokio_postgres::Error {
    fn is_definite(&self) -> bool {
        match self.source() {
            Some(err) => match err.downcast_ref::<DbError>() {
                Some(db_err) => {
                    let class = match db_err.code().code().get(0..2) {
                        None => return false,
                        Some(class) => class,
                    };
                    match class {
                        // See https://www.postgresql.org/docs/current/errcodes-appendix.html
                        // for the class definitions.

                        // unknown catalog or schema names
                        "3D" | "3F" => true,
                        // syntax error or access rule violation
                        "42" => true,
                        _ => false,
                    }
                }
                None => false,
            },
            // We have no information about what happened, it might be a fatal error or
            // it might not. Unexpected errors can happen if the upstream crashes for
            // example in which case we should retry.
            //
            // Therefore, we adopt a "indefinite unless proven otherwise" policy and
            // keep retrying in the event of unexpected errors.
            None => false,
        }
    }
}

enum ReplicationError {
    /// This error is definite: this source is permanently wedged.
    /// Returning a definite error will cause the collection to become un-queryable.
    Definite(anyhow::Error),
    /// This error may or may not resolve itself in the future, and
    /// should be retried instead of being added to the output.
    Indefinite(anyhow::Error),
}

impl<E: ErrorExt + Into<anyhow::Error>> From<E> for ReplicationError {
    fn from(err: E) -> Self {
        if err.is_definite() {
            Self::Definite(err.into())
        } else {
            Self::Indefinite(err.into())
        }
    }
}

macro_rules! try_definite {
    ($expr:expr $(,)?) => {
        match $expr {
            Ok(val) => val,
            Err(err) => return Err(ReplicationError::Definite(err.into())),
        }
    };
}
macro_rules! try_indefinite {
    ($expr:expr $(,)?) => {
        match $expr {
            Ok(val) => val,
            Err(err) => return Err(ReplicationError::Indefinite(err.into())),
        }
    };
}

// Message used to communicate between `get_next_message` and the tokio task
enum InternalMessage {
    Err(SourceReaderError),
    Status(HealthStatus),
    Value {
        output: usize,
        value: Row,
        lsn: PgLsn,
        diff: Diff,
        end: bool,
    },
}

/// Information required to sync data from Postgres
pub struct PostgresSourceReader {
    receiver_stream: Receiver<InternalMessage>,

    // Postgres sources support single-threaded ingestion only, so only one of
    // the `PostgresSourceReader`s will actually produce data.
    active_read_worker: bool,

    // The non-active reader (see above `active_read_worker`) has to report back
    // that is is not consuming from the one [`PartitionId:None`] partition.
    // Before it can return a [`NextMessage::Finished`]. This is keeping track
    // of that.
    reported_unconsumed_partitions: bool,

    /// The lsn we last emitted data at. Used to fabricate timestamps for errors. This should
    /// ideally go away and only emit errors that we can associate with source timestamps
    last_lsn: PgLsn,
}

/// An OffsetCommitter for postgres, that sends
/// the offsets (lsns) to the replication stream
/// through a channel
pub struct PgOffsetCommitter {
    logger: LogCommitter,
    tx: Sender<HashMap<PartitionId, MzOffset>>,
}

/// Information about an ingested upstream table
struct SourceTable {
    /// The source output index of this table
    output_index: usize,
    /// The relational description of this table
    desc: PostgresTableDesc,
    /// The scalar expressions required to cast the text encoded columns received from postgres
    /// into the target relational types
    casts: Vec<MirScalarExpr>,
}

/// An internal struct held by the spawned tokio task
struct PostgresTaskInfo {
    source_id: GlobalId,
    connection_config: mz_postgres_util::Config,
    publication: String,
    slot: String,
    /// Our cursor into the WAL
    lsn: PgLsn,
    metrics: PgSourceMetrics,
    /// A map of the table oid to its information
    source_tables: HashMap<u32, SourceTable>,
    row_sender: RowSender,
    sender: Sender<InternalMessage>,
    /// Channel to receive lsn's from the PgOffsetCommitter
    /// that are safe to send status updates for.
    offset_rx: Receiver<HashMap<PartitionId, MzOffset>>,
}

impl SourceConnectionBuilder for PostgresSourceConnection {
    type Reader = PostgresSourceReader;
    type OffsetCommitter = PgOffsetCommitter;

    fn into_reader(
        self,
        _source_name: String,
        source_id: GlobalId,
        worker_id: usize,
        worker_count: usize,
        consumer_activator: SyncActivator,
        start_offsets: Vec<(PartitionId, Option<MzOffset>)>,
        _encoding: SourceDataEncoding,
        metrics: SourceBaseMetrics,
        connection_context: ConnectionContext,
    ) -> Result<(Self::Reader, Self::OffsetCommitter), anyhow::Error> {
        let active_read_worker =
            crate::source::responsible_for(&source_id, worker_id, worker_count, &PartitionId::None);

        // TODO: figure out the best default here; currently this is optimized
        // for the speed to pass pg-cdc-resumption tests on a local machine.
        let (dataflow_tx, dataflow_rx) = tokio::sync::mpsc::channel(50_000);

        let (offset_tx, offset_rx) = tokio::sync::mpsc::channel(10);

        // Pick out the partition we care about
        // TODO(petrosagg): add an associated type to SourceReader so that each source can define
        // its own gauge type
        let start_offset = start_offsets
            .into_iter()
            .find_map(|(pid, offset)| {
                if pid == PartitionId::None {
                    offset
                } else {
                    None
                }
            })
            .unwrap_or_default();

        let connection_config = TokioHandle::current()
            .block_on(self.connection.config(&*connection_context.secrets_reader))
            .expect("Postgres connection unexpectedly missing secrets");

        if active_read_worker {
            let mut source_tables = HashMap::new();
            let tables_iter = self.publication_details.tables.iter();

            for (i, desc) in tables_iter.enumerate() {
                let output_index = i + 1;
                // We maintain descriptions for all tables in the publication,
                // but only casts for those we aim to use (and have validated
                // that their types are ingestable). This also prevents us from
                // creating snapshots for tables in the publication that are
                // not referenced in the source.
                match self.table_casts.get(&output_index) {
                    Some(casts) => {
                        let source_table = SourceTable {
                            output_index,
                            desc: desc.clone(),
                            casts: casts.to_vec(),
                        };
                        source_tables.insert(desc.oid, source_table);
                    }
                    None => continue,
                }
            }

            let task_info = PostgresTaskInfo {
                source_id,
                connection_config,
                publication: self.publication,
                slot: self.publication_details.slot,
                /// Our cursor into the WAL
                lsn: start_offset.offset.into(),
                metrics: PgSourceMetrics::new(&metrics, source_id),
                source_tables,
                row_sender: RowSender::new(dataflow_tx.clone(), consumer_activator),
                sender: dataflow_tx,
                offset_rx,
            };

            task::spawn(
                || format!("postgres_source:{}", source_id),
                postgres_replication_loop(task_info),
            );
        }

        Ok((
            PostgresSourceReader {
                receiver_stream: dataflow_rx,
                active_read_worker,
                reported_unconsumed_partitions: false,
                last_lsn: start_offset.offset.into(),
            },
            PgOffsetCommitter {
                logger: LogCommitter {
                    source_id,
                    worker_id,
                    worker_count,
                },
                tx: offset_tx,
            },
        ))
    }
}

impl SourceReader for PostgresSourceReader {
    type Key = ();
    type Value = Row;
    // Postgres can produce deletes that cause retractions
    type Diff = Diff;

    // TODO(guswynn): use `next` instead of using a channel
    fn get_next_message(&mut self) -> NextMessage<Self::Key, Self::Value, Self::Diff> {
        if !self.active_read_worker {
            if !self.reported_unconsumed_partitions {
                self.reported_unconsumed_partitions = true;
                return NextMessage::Ready(SourceMessageType::DropPartitionCapabilities(vec![
                    PartitionId::None,
                ]));
            }
            return NextMessage::Finished;
        }

        // TODO(guswynn): consider if `try_recv` is better or the same as `now_or_never`
        let ret = match self.receiver_stream.recv().now_or_never() {
            Some(Some(InternalMessage::Value {
                output,
                value,
                diff,
                lsn,
                end,
            })) => {
                self.last_lsn = lsn;
                if end {
                    let msg = SourceMessage {
                        output,
                        upstream_time_millis: None,
                        key: (),
                        value,
                        headers: None,
                    };
                    let ts = (PartitionId::None, lsn.into());
                    NextMessage::Ready(SourceMessageType::Finalized(Ok(msg), ts, diff))
                } else {
                    let msg = SourceMessage {
                        output,
                        upstream_time_millis: None,
                        key: (),
                        value,
                        headers: None,
                    };
                    let ts = (PartitionId::None, lsn.into());
                    NextMessage::Ready(SourceMessageType::InProgress(Ok(msg), ts, diff))
                }
            }
            Some(Some(InternalMessage::Status(update))) => {
                NextMessage::Ready(SourceMessageType::SourceStatus(update))
            }
            Some(Some(InternalMessage::Err(err))) => {
                // XXX(petrosagg): we are fabricating a timestamp here!!
                let non_definite_ts = (PartitionId::None, MzOffset::from(self.last_lsn) + 1);
                NextMessage::Ready(SourceMessageType::Finalized(Err(err), non_definite_ts, 1))
            }
            None => NextMessage::Pending,
            Some(None) => NextMessage::Finished,
        };

        ret
    }
}

#[async_trait::async_trait]
impl OffsetCommitter for PgOffsetCommitter {
    async fn commit_offsets(
        &self,
        offsets: HashMap<PartitionId, MzOffset>,
    ) -> Result<(), anyhow::Error> {
        self.tx.send(offsets.clone()).await?;
        self.logger.commit_offsets(offsets).await?;

        Ok(())
    }
}

/// Defers to `postgres_replication_loop_inner` and sends errors through the channel if they occur
async fn postgres_replication_loop(mut task_info: PostgresTaskInfo) {
    match postgres_replication_loop_inner(&mut task_info).await {
        Ok(()) => {}
        Err(e) => {
            // Drop the send error, as we have no way of communicating back to the
            // source operator if the channel is gone.
            let _ = task_info
                .row_sender
                .sender
                .send(InternalMessage::Err(e))
                .await;
            task_info
                .row_sender
                .activator
                .activate()
                .expect("postgres reader activation failed");
        }
    }
}

/// Core logic
async fn postgres_replication_loop_inner(
    task_info: &mut PostgresTaskInfo,
) -> Result<(), SourceReaderError> {
    if task_info.lsn == PgLsn::from(0) {
        // Buffer rows from snapshot to retract and retry, if initial snapshot fails.
        // Postgres sources cannot proceed without a successful snapshot.
        match task_info.produce_snapshot().await {
            Ok(_) => {
                info!(
                    "replication snapshot for source {} succeeded",
                    &task_info.source_id
                );
            }
            Err(ReplicationError::Indefinite(e)) => {
                // TODO: In the future we probably want to handle this more gracefully,
                // but for now halting is the easiest way to dump the data in the pipe.
                // The restarted storaged instance will restart the snapshot fresh, which will
                // avoid any inconsistencies. Note that if the same lsn is chosen in the
                // next snapshotting, the remapped timestamp chosen will be the same for
                // both instances of storaged.
                halt!(
                    "replication snapshot for source {} failed: {}",
                    &task_info.source_id,
                    e
                );
            }
            Err(ReplicationError::Definite(e)) => {
                return Err(SourceReaderError {
                    inner: SourceErrorDetails::Initialization(e.to_string()),
                })
            }
        }
    }

    loop {
        match task_info.produce_replication().await {
            Err(ReplicationError::Indefinite(e)) => {
                // If the channel is shutting down, so is the source.
                let _ = task_info
                    .sender
                    .send(InternalMessage::Status(HealthStatus::StalledWithError(
                        e.to_string(),
                    )))
                    .await;
                warn!(
                    "replication for source {} interrupted, retrying: {}",
                    task_info.source_id, e
                );
            }
            Err(ReplicationError::Definite(e)) => {
                return Err(SourceReaderError {
                    inner: SourceErrorDetails::Other(e.to_string()),
                })
            }
            Ok(_) => {
                // shutdown initiated elsewhere
                return Ok(());
            }
        }

        // TODO(petrosagg): implement exponential back-off
        tokio::time::sleep(Duration::from_secs(3)).await;
        info!("resuming replication for source {}", task_info.source_id);
    }
}

struct RowMessage {
    output_index: usize,
    row: Row,
    lsn: PgLsn,
    diff: i64,
}

/// A type that makes it easy to correctly send inserts and deletes.
///
/// Note: `RowSender::delete/insert` should be called with the same
/// lsn until `close_lsn` is called, which should be called and awaited
/// before dropping the `RowSender` or moving onto a new lsn.
/// Internally, this type uses asserts to uphold the first requirement.
struct RowSender {
    sender: Sender<InternalMessage>,
    activator: SyncActivator,
    buffered_message: Option<RowMessage>,
}

impl RowSender {
    /// Create a new `RowSender`.
    pub fn new(sender: Sender<InternalMessage>, activator: SyncActivator) -> Self {
        Self {
            sender,
            activator,
            buffered_message: None,
        }
    }

    /// Insert a row at an lsn.
    pub async fn insert(&mut self, output_index: usize, row: Row, lsn: PgLsn) {
        if let Some(buffered) = self.buffered_message.take() {
            assert_eq!(buffered.lsn, lsn);
            self.send_row(
                buffered.output_index,
                buffered.row,
                buffered.lsn,
                buffered.diff,
                false,
            )
            .await;
        }

        self.buffered_message = Some(RowMessage {
            output_index,
            row,
            lsn,
            diff: 1,
        });
    }
    /// Delete a row at an lsn.
    pub async fn delete(&mut self, output_index: usize, row: Row, lsn: PgLsn) {
        if let Some(buffered) = self.buffered_message.take() {
            assert_eq!(buffered.lsn, lsn);
            self.send_row(
                buffered.output_index,
                buffered.row,
                buffered.lsn,
                buffered.diff,
                false,
            )
            .await;
        }

        self.buffered_message = Some(RowMessage {
            output_index,
            row,
            lsn,
            diff: -1,
        });
    }

    /// Finalize an lsn, making sure all messages that my be buffered are flushed, and that the
    /// last message sent is marked as closing the `lsn` (which is the messages `offset` in the
    /// rest of the source pipeline.
    pub async fn close_lsn(&mut self, lsn: PgLsn) {
        if let Some(buffered) = self.buffered_message.take() {
            assert_eq!(buffered.lsn, lsn);
            self.send_row(
                buffered.output_index,
                buffered.row,
                buffered.lsn,
                buffered.diff,
                true,
            )
            .await;
        }
    }

    async fn send_row(&self, output: usize, row: Row, lsn: PgLsn, diff: i64, end: bool) {
        // a closed receiver means the source has been shutdown
        // (dropped or the process is dying), so just continue on
        // without activation
        if let Ok(_) = self
            .sender
            .send(InternalMessage::Value {
                output,
                value: row,
                lsn,
                diff,
                end,
            })
            .await
        {
            self.activator
                .activate()
                .expect("postgres reader activation failed");
        }
    }
}

// implement the core pg logic in this impl block
impl PostgresTaskInfo {
    /// Validates that all expected tables exist in the publication tables and they have the same schema
    fn validate_tables(&self, tables: Vec<PostgresTableDesc>) -> Result<(), anyhow::Error> {
        let pub_tables: HashMap<u32, PostgresTableDesc> =
            tables.into_iter().map(|t| (t.oid, t)).collect();
        for (id, info) in self.source_tables.iter() {
            match pub_tables.get(id) {
                Some(pub_schema) => {
                    if pub_schema != &info.desc {
                        error!(
                            "Error validating table in publication. Expected: {:?} Actual: {:?}",
                            &info.desc, pub_schema
                        );
                        bail!("Schema for table {} differs, recreate Materialize source to use new schema", info.desc.name)
                    }
                }
                None => {
                    error!(
                        "publication missing table: {} with id {}",
                        info.desc.name, id
                    );
                    bail!(
                        "Publication missing expected table {} with oid {}",
                        info.desc.name,
                        id
                    )
                }
            }
        }
        Ok(())
    }

    /// Creates the replication slot and produces the initial snapshot of the data
    ///
    /// After the initial snapshot has been produced it returns the name of the created slot and
    /// the LSN at which we should start the replication stream at.
    async fn produce_snapshot(&mut self) -> Result<(), ReplicationError> {
        // Get all the relevant tables for this publication
        let publication_tables = try_indefinite!(
            mz_postgres_util::publication_info(&self.connection_config, &self.publication).await
        );

        let client = try_indefinite!(self.connection_config.clone().connect_replication().await);

        // We're initializing this source so any previously existing slot must be removed and
        // re-created. Once we have data persistence we will be able to reuse slots across restarts
        let _ = client
            .simple_query(&format!("DROP_REPLICATION_SLOT {:?}", &self.slot))
            .await;

        // Validate publication tables against the state snapshot
        try_definite!(self.validate_tables(publication_tables));

        // Start a transaction and immediately create a replication slot with the USE SNAPSHOT
        // directive. This makes the starting point of the slot and the snapshot of the transaction
        // identical.
        client
            .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
            .await?;

        let slot_query = format!(
            r#"CREATE_REPLICATION_SLOT {:?} LOGICAL "pgoutput" USE_SNAPSHOT"#,
            &self.slot
        );
        let slot_row = client
            .simple_query(&slot_query)
            .await?
            .into_iter()
            .next()
            .and_then(|msg| match msg {
                SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .ok_or_else(|| {
                ReplicationError::Indefinite(anyhow!(
                    "empty result after creating replication slot"
                ))
            })?;

        // Store the lsn at which we will need to start the replication stream from
        let consistent_point = try_indefinite!(slot_row
            .get("consistent_point")
            .ok_or_else(|| anyhow!("missing expected column: `consistent_point`")));
        self.lsn = try_definite!(consistent_point
            .parse()
            .or_else(|_| Err(anyhow!("invalid lsn"))));

        // Scratch space to use while evaluating casts
        let mut datum_vec = DatumVec::new();

        for info in self.source_tables.values() {
            let reader = client
                .copy_out_simple(
                    format!(
                        "COPY {:?}.{:?} TO STDOUT (FORMAT TEXT, DELIMITER '\t')",
                        info.desc.namespace, info.desc.name
                    )
                    .as_str(),
                )
                .await?;

            tokio::pin!(reader);
            let mut text_row = Row::default();
            // TODO: once tokio-stream is released with https://github.com/tokio-rs/tokio/pull/4502
            //    we can convert this into a single `timeout(...)` call on the reader CopyOutStream
            while let Some(b) = tokio::time::timeout(Duration::from_secs(30), reader.next())
                .await?
                .transpose()?
            {
                let mut packer = text_row.packer();
                // Convert raw rows from COPY into repr:Row. Each Row is a relation_id
                // and list of string-encoded values, e.g. Row{ 16391 , ["1", "2"] }
                let parser = mz_pgcopy::CopyTextFormatParser::new(b.as_ref(), "\t", "\\N");

                // TODO(benesch): rewrite to avoid `as`.
                #[allow(clippy::as_conversions)]
                let mut raw_values = parser.iter_raw(info.desc.columns.len() as i32);
                while let Some(raw_value) = raw_values.next() {
                    match try_definite!(raw_value) {
                        Some(value) => {
                            packer.push(Datum::String(try_definite!(std::str::from_utf8(value))))
                        }
                        None => packer.push(Datum::Null),
                    }
                }

                let mut datums = datum_vec.borrow();
                datums.extend(text_row.iter());

                let row = try_definite!(PostgresTaskInfo::cast_row(&info.casts, &datums));

                self.row_sender
                    .insert(info.output_index, row, self.lsn)
                    .await;
                // Failure scenario after we have produced at least one row, but before a
                // successful `COMMIT`
                fail::fail_point!("pg_snapshot_failure", |_| {
                    Err(ReplicationError::Indefinite(anyhow::anyhow!(
                        "recoverable errors should crash the process"
                    )))
                });
            }

            self.metrics.tables.inc();
        }
        self.metrics.lsn.set(self.lsn.into());
        client.simple_query("COMMIT;").await?;

        // close the current `row_sender` context after we are sure we have not errored
        // out (in the commit).
        self.row_sender.close_lsn(self.lsn).await;
        Ok(())
    }

    /// Packs a Tuple received in the replication stream into a Row packer.
    fn datums_from_tuple<'a, T>(
        rel_id: u32,
        tuple_data: T,
        datums: &mut Vec<Datum<'a>>,
    ) -> Result<(), anyhow::Error>
    where
        T: IntoIterator<Item = &'a TupleData>,
    {
        for val in tuple_data.into_iter() {
            let datum = match val {
                TupleData::Null => Datum::Null,
                TupleData::UnchangedToast => bail!(
                    "Missing TOASTed value from table with OID = {}. \
                    Did you forget to set REPLICA IDENTITY to FULL for your table?",
                    rel_id
                ),
                TupleData::Text(b) => std::str::from_utf8(b)?.into(),
            };
            datums.push(datum);
        }
        Ok(())
    }

    /// Casts a text row into the target types
    fn cast_row(table_cast: &[MirScalarExpr], datums: &[Datum<'_>]) -> Result<Row, anyhow::Error> {
        let arena = mz_repr::RowArena::new();
        let mut row = Row::default();
        let mut packer = row.packer();
        for column_cast in table_cast {
            let datum = column_cast.eval(datums, &arena)?;
            packer.push(datum);
        }
        Ok(row)
    }

    async fn produce_replication(&mut self) -> Result<(), ReplicationError> {
        use ReplicationError::*;

        // An lsn that is safe to send status updates for. This is primarily derived from
        // the resumption frontier, as that represents an lsn that is durably recorded
        // into persist. In the beginning, we can use this initial lsn, which is either:
        // - From the initial resumption frontier if we are restarting and skipping snapshotting
        // - The end lsn from the snapshot, which is safe to use because pg keeps
        //   all updates >= this lsn.
        let mut committed_lsn: PgLsn = self.lsn;

        let client = try_indefinite!(self.connection_config.clone().connect_replication().await);

        // Before consuming the replication stream we will peek into the replication slot using a
        // normal SQL query and the `pg_logical_slot_peek_binary_changes` administrative function.
        //
        // By doing so we can get a positive statement about existence or absence of relevant data
        // from the LSN we wish to restart from until the last known LSN end of the database. If
        // there are no message then it is safe to fast forward to the end WAL LSN and start the
        // replication stream from there.
        let cur_lsn = {
            let rows = try_indefinite!(
                client
                    .simple_query("SELECT pg_current_wal_flush_lsn()")
                    .await
            );
            match rows.first().expect("query returns exactly one row") {
                SimpleQueryMessage::Row(row) => row
                    .get(0)
                    .expect("query returns one column")
                    .parse::<PgLsn>()
                    .expect("pg_current_wal_flush_lsn returned invalid lsn"),
                _ => panic!(),
            }
        };

        self.lsn = {
            let query = format!(
                "SELECT COUNT(*) FROM pg_logical_slot_peek_binary_changes(
                     '{name}', '{lsn}', 1,
                     'proto_version', '1',
                     'publication_names', '{publication}'
                )",
                name = &self.slot,
                lsn = cur_lsn,
                publication = self.publication
            );

            let peek_binary_start_time = Instant::now();
            let rows = try_indefinite!(client.simple_query(&query).await);

            match rows.first().expect("query returns exactly one row") {
                SimpleQueryMessage::Row(row) => {
                    let changes: u64 = row
                        .get(0)
                        .expect("query returns one column")
                        .parse()
                        .expect("count returned invalid number");
                    let chosen_lsn = if changes == 0 {
                        // If there are no changes until the end of the WAL it's safe to fast forward
                        cur_lsn
                    } else {
                        self.lsn
                    };

                    tracing::info!(
                        slot = ?self.slot,
                        query_time = ?peek_binary_start_time.elapsed(),
                        ?chosen_lsn,
                        current_lsn = ?cur_lsn,
                        resumption_lsn = ?self.lsn,
                        "Found {} changes in the wal.",
                        changes
                    );

                    chosen_lsn
                }
                _ => panic!(),
            }
        };

        let query = format!(
            r#"START_REPLICATION SLOT "{name}" LOGICAL {lsn}
              ("proto_version" '1', "publication_names" '{publication}')"#,
            name = &self.slot,
            lsn = self.lsn,
            publication = self.publication
        );
        let copy_stream = try_indefinite!(client.copy_both_simple(&query).await);

        let stream = LogicalReplicationStream::new(copy_stream).take_until(self.sender.closed());
        tokio::pin!(stream);

        let mut last_data_message = Instant::now();
        let mut inserts = vec![];
        let mut deletes = vec![];

        let mut last_feedback = Instant::now();

        // Scratch space to use while evaluating casts
        let mut datum_vec = DatumVec::new();

        loop {
            let data_next = stream.next();
            tokio::pin!(data_next);
            let offset_recv = self.offset_rx.recv();
            tokio::pin!(offset_recv);

            use futures::future::Either;
            let item = match futures::future::select(offset_recv, data_next).await {
                Either::Left((Some(to_commit), _)) => {
                    // We assume there is only a single partition here.
                    let lsn: PgLsn = to_commit[&PartitionId::None].offset.into();
                    // Set the committed lsn so we can send a correct status update
                    // next time we are required. We assume this is always
                    // increasing, and >= the initial lsn.
                    committed_lsn = lsn;
                    continue;
                }
                Either::Right((Some(item), _)) => item,
                Either::Left((None, _)) | Either::Right((None, _)) => {
                    break;
                }
            };

            let item = item?;
            use ReplicationMessage::*;

            // The upstream will periodically request status updates by setting the keepalive's
            // reply field to 1. However, we cannot rely on these messages arriving on time. For
            // example, when the upstream is sending a big transaction its keepalive messages are
            // queued and can be delayed arbitrarily. Therefore, we also make sure to
            // send a proactive status update every 30 seconds, but only after we receive
            // resumption_frontier advancement. There is an implicit requirement that
            // a new resumption frontier is converted into an lsn relatively soon
            // after startup.
            //
            // See: https://www.postgresql.org/message-id/CAMsr+YE2dSfHVr7iEv1GSPZihitWX-PMkD9QALEGcTYa+sdsgg@mail.gmail.com
            let mut needs_status_update = last_feedback.elapsed() > FEEDBACK_INTERVAL;

            self.metrics.total.inc();
            use LogicalReplicationMessage::*;
            match &item {
                XLogData(xlog_data) => match xlog_data.data() {
                    Begin(_) => {
                        last_data_message = Instant::now();
                        if !inserts.is_empty() || !deletes.is_empty() {
                            return Err(Definite(anyhow!(
                                "got BEGIN statement after uncommitted data"
                            )));
                        }
                    }
                    Insert(insert) if self.source_tables.contains_key(&insert.rel_id()) => {
                        last_data_message = Instant::now();
                        self.metrics.inserts.inc();
                        let rel_id = insert.rel_id();
                        let info = self.source_tables.get(&rel_id).unwrap();
                        let new_tuple = insert.tuple().tuple_data();
                        let mut datums = datum_vec.borrow();
                        try_definite!(PostgresTaskInfo::datums_from_tuple(
                            rel_id,
                            new_tuple,
                            &mut *datums
                        ));
                        let row = try_definite!(PostgresTaskInfo::cast_row(&info.casts, &datums));
                        inserts.push((info.output_index, row));
                    }
                    Update(update) if self.source_tables.contains_key(&update.rel_id()) => {
                        last_data_message = Instant::now();
                        self.metrics.updates.inc();
                        let rel_id = update.rel_id();
                        let info = self.source_tables.get(&rel_id).unwrap();
                        let err = || {
                            anyhow!(
                                "Old row missing from replication stream for table with OID = {}.
                                 Did you forget to set REPLICA IDENTITY to FULL for your table?",
                                rel_id
                            )
                        };
                        let old_tuple =
                            try_definite!(update.old_tuple().ok_or_else(err)).tuple_data();
                        let mut old_datums = datum_vec.borrow();
                        try_definite!(PostgresTaskInfo::datums_from_tuple(
                            rel_id,
                            old_tuple,
                            &mut *old_datums
                        ));
                        let old_row =
                            try_definite!(PostgresTaskInfo::cast_row(&info.casts, &old_datums));
                        deletes.push((info.output_index, old_row));
                        drop(old_datums);

                        // If the new tuple contains unchanged toast values, reuse the ones
                        // from the old tuple
                        let new_tuple = update
                            .new_tuple()
                            .tuple_data()
                            .iter()
                            .zip(old_tuple.iter())
                            .map(|(new, old)| match new {
                                TupleData::UnchangedToast => old,
                                _ => new,
                            });
                        let mut new_datums = datum_vec.borrow();
                        try_definite!(PostgresTaskInfo::datums_from_tuple(
                            rel_id,
                            new_tuple,
                            &mut *new_datums
                        ));
                        let new_row =
                            try_definite!(PostgresTaskInfo::cast_row(&info.casts, &new_datums));
                        inserts.push((info.output_index, new_row));
                    }
                    Delete(delete) if self.source_tables.contains_key(&delete.rel_id()) => {
                        last_data_message = Instant::now();
                        self.metrics.deletes.inc();
                        let rel_id = delete.rel_id();
                        let info = self.source_tables.get(&rel_id).unwrap();
                        let err = || {
                            anyhow!(
                                "Old row missing from replication stream for table with OID = {}.
                                 Did you forget to set REPLICA IDENTITY to FULL for your table?",
                                rel_id
                            )
                        };
                        let old_tuple =
                            try_definite!(delete.old_tuple().ok_or_else(err)).tuple_data();
                        let mut datums = datum_vec.borrow();
                        try_definite!(PostgresTaskInfo::datums_from_tuple(
                            rel_id,
                            old_tuple,
                            &mut *datums
                        ));
                        let row = try_definite!(PostgresTaskInfo::cast_row(&info.casts, &datums));
                        deletes.push((info.output_index, row));
                    }
                    Commit(commit) => {
                        last_data_message = Instant::now();
                        self.metrics.transactions.inc();
                        self.lsn = commit.end_lsn().into();

                        for (output, row) in deletes.drain(..) {
                            self.row_sender.delete(output, row, self.lsn).await;
                        }
                        for (output, row) in inserts.drain(..) {
                            self.row_sender.insert(output, row, self.lsn).await;
                        }

                        self.row_sender.close_lsn(self.lsn).await;
                        self.metrics.lsn.set(self.lsn.into());
                    }
                    Relation(relation) => {
                        last_data_message = Instant::now();
                        let rel_id = relation.rel_id();
                        if let Some(info) = self.source_tables.get(&rel_id) {
                            // Start with the cheapest check first, this will catch the majority of alters
                            if info.desc.columns.len() != relation.columns().len() {
                                error!(
                                    "alter table detected on {} with id {}",
                                    info.desc.name, info.desc.oid
                                );
                                return Err(Definite(anyhow!(
                                    "source table {} with oid {} has been altered",
                                    info.desc.name,
                                    info.desc.oid
                                )));
                            }
                            let same_name = info.desc.name == relation.name().unwrap();
                            let same_namespace =
                                info.desc.namespace == relation.namespace().unwrap();
                            if !same_name || !same_namespace {
                                error!(
                                    "table name changed on {}.{} with id {} to {}.{}",
                                    info.desc.namespace,
                                    info.desc.name,
                                    info.desc.oid,
                                    relation.namespace().unwrap(),
                                    relation.name().unwrap()
                                );
                                return Err(Definite(anyhow!(
                                    "source table {} with oid {} has been altered",
                                    info.desc.name,
                                    info.desc.oid
                                )));
                            }
                            // Relation messages do not include nullability/primary_key data so we
                            // check the name, type_oid, and type_mod explicitly and error if any
                            // of them differ
                            for (src, rel) in info.desc.columns.iter().zip(relation.columns()) {
                                let same_name = src.name == rel.name().unwrap();
                                let rel_typoid = u32::try_from(rel.type_id()).unwrap();
                                let same_typoid = src.type_oid == rel_typoid;
                                let same_typmod = src.type_mod == rel.type_modifier();

                                if !same_name || !same_typoid || !same_typmod {
                                    error!(
                                        "alter table error: name {}, oid {}, old_schema {:?}, new_schema {:?}",
                                        info.desc.name,
                                        info.desc.oid,
                                        info.desc.columns,
                                        relation.columns()
                                    );
                                    return Err(Definite(anyhow!(
                                        "source table {} with oid {} has been altered",
                                        info.desc.name,
                                        info.desc.oid
                                    )));
                                }
                            }
                        }
                    }
                    Insert(_) | Update(_) | Delete(_) | Origin(_) | Type(_) => {
                        last_data_message = Instant::now();
                        self.metrics.ignored.inc();
                    }
                    Truncate(truncate) => {
                        let tables = truncate
                            .rel_ids()
                            .iter()
                            // Filter here makes option handling in map "safe"
                            .filter_map(|id| self.source_tables.get(id))
                            .map(|info| format!("name: {} id: {}", info.desc.name, info.desc.oid))
                            .collect::<Vec<String>>();
                        return Err(Definite(anyhow!(
                            "source table(s) {} got truncated",
                            tables.join(", ")
                        )));
                    }
                    // The enum is marked as non_exhaustive. Better to be conservative here in
                    // case a new message is relevant to the semantics of our source
                    _ => return Err(Definite(anyhow!("unexpected logical replication message"))),
                },
                PrimaryKeepAlive(keepalive) => {
                    needs_status_update = needs_status_update || keepalive.reply() == 1;

                    // Additional logging for incident 25
                    if last_data_message.elapsed() > WAL_LAG_GRACE_PERIOD
                        && keepalive.wal_end().saturating_sub(self.lsn.into()) > MAX_WAL_LAG
                    {
                        tracing::info!(
                            wal_lag_grace_period = ?WAL_LAG_GRACE_PERIOD,
                            max_wal_lag = %bytesize::to_string(MAX_WAL_LAG, false),
                            last_elapsed = ?last_data_message.elapsed(),
                            lsn_diff = ?keepalive.wal_end().saturating_sub(self.lsn.into()),
                            resumption_lsn = ?self.lsn,
                            "Got PrimaryKeepAlive");
                        return Err(Indefinite(anyhow!("reached maximum WAL lag")));
                    }
                }
                // The enum is marked non_exhaustive, better be conservative
                _ => return Err(Definite(anyhow!("Unexpected replication message"))),
            }
            if needs_status_update {
                let ts: i64 = PG_EPOCH
                    .elapsed()
                    .expect("system clock set earlier than year 2000!")
                    .as_micros()
                    .try_into()
                    .expect("software more than 200k years old, consider updating");

                try_indefinite!(
                    stream
                        .as_mut()
                        .get_pin_mut()
                        .standby_status_update(committed_lsn, committed_lsn, committed_lsn, ts, 0)
                        .await
                );
                last_feedback = Instant::now();
            }
        }
        if !stream.is_stopped() {
            return Err(Indefinite(anyhow!("replication stream ended")));
        }
        Ok(())
    }
}
