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
use futures::{pin_mut, FutureExt, StreamExt, TryStreamExt};
use once_cell::sync::Lazy;
use postgres_protocol::message::backend::{
    LogicalReplicationMessage, ReplicationMessage, TupleData,
};
use timely::scheduling::SyncActivator;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_postgres::error::{DbError, Severity, SqlState};
use tokio_postgres::replication::LogicalReplicationStream;
use tokio_postgres::types::PgLsn;
use tokio_postgres::SimpleQueryMessage;
use tracing::{error, info, warn};

use mz_dataflow_types::connections::ConnectionContext;
use mz_dataflow_types::sources::{
    encoding::SourceDataEncoding, ExternalSourceConnection, MzOffset, PostgresSourceConnection,
};
use mz_dataflow_types::SourceErrorDetails;
use mz_expr::PartitionId;
use mz_ore::task;
use mz_postgres_util::desc::PostgresTableDesc;
use mz_repr::{Datum, Diff, GlobalId, Row};

use self::metrics::PgSourceMetrics;
use super::metrics::SourceBaseMetrics;
use crate::source::{
    NextMessage, SourceMessage, SourceMessageType, SourceReader, SourceReaderError,
};

mod metrics;

/// Postgres epoch is 2000-01-01T00:00:00Z
static PG_EPOCH: Lazy<SystemTime> = Lazy::new(|| UNIX_EPOCH + Duration::from_secs(946_684_800));

trait ErrorExt {
    fn is_recoverable(&self) -> bool;
}

impl ErrorExt for tokio::time::error::Elapsed {
    fn is_recoverable(&self) -> bool {
        true
    }
}

impl ErrorExt for tokio_postgres::Error {
    fn is_recoverable(&self) -> bool {
        match self.source() {
            Some(err) => {
                match err.downcast_ref::<DbError>() {
                    Some(db_err) => {
                        use Severity::*;
                        // Connection and non-fatal errors
                        db_err.code() == &SqlState::CONNECTION_EXCEPTION
                            || db_err.code() == &SqlState::CONNECTION_DOES_NOT_EXIST
                            || db_err.code() == &SqlState::CONNECTION_FAILURE
                            || db_err.code() == &SqlState::TOO_MANY_CONNECTIONS
                            || db_err.code() == &SqlState::CANNOT_CONNECT_NOW
                            || db_err.code() == &SqlState::ADMIN_SHUTDOWN
                            || db_err.code() == &SqlState::CRASH_SHUTDOWN
                            || !matches!(
                                db_err.parsed_severity(),
                                Some(Error) | Some(Fatal) | Some(Panic)
                            )
                    }
                    // IO errors
                    None => err.is::<std::io::Error>(),
                }
            }
            // We have no information about what happened, it might be a fatal error or
            // it might not. Unexpected errors can happen if the upstream crashes for
            // example in which case we should retry.
            //
            // Therefore, we adopt a "recoverable unless proven otherwise" policy and
            // keep retrying in the event of unexpected errors.
            None => true,
        }
    }
}

enum ReplicationError {
    Recoverable(anyhow::Error),
    Fatal(anyhow::Error),
}

impl<E: ErrorExt + Into<anyhow::Error>> From<E> for ReplicationError {
    fn from(err: E) -> Self {
        if err.is_recoverable() {
            Self::Recoverable(err.into())
        } else {
            Self::Fatal(err.into())
        }
    }
}

macro_rules! try_fatal {
    ($expr:expr $(,)?) => {
        match $expr {
            Ok(val) => val,
            Err(err) => return Err(ReplicationError::Fatal(err.into())),
        }
    };
}
macro_rules! try_recoverable {
    ($expr:expr $(,)?) => {
        match $expr {
            Ok(val) => val,
            Err(err) => return Err(ReplicationError::Recoverable(err.into())),
        }
    };
}

// Message used to communicate between `get_next_message` and the tokio task
enum InternalMessage {
    Err(SourceReaderError),
    Value {
        value: Row,
        lsn: PgLsn,
        diff: Diff,
        end: bool,
    },
}

/// Information required to sync data from Postgres
pub struct PostgresSourceReader {
    receiver_stream: Receiver<InternalMessage>,
}

/// An internal struct held by the spawned tokio task
struct PostgresTaskInfo {
    source_id: GlobalId,
    connection: PostgresSourceConnection,
    /// Our cursor into the WAL
    lsn: PgLsn,
    metrics: PgSourceMetrics,
    source_tables: HashMap<u32, PostgresTableDesc>,
    sender: Sender<InternalMessage>,
    activator: SyncActivator,
}

impl SourceReader for PostgresSourceReader {
    type Key = ();
    type Value = Row;
    // Postgres can produce deletes that cause retractions
    type Diff = Diff;

    fn new(
        _source_name: String,
        source_id: GlobalId,
        _worker_id: usize,
        _worker_count: usize,
        consumer_activator: SyncActivator,
        connection: ExternalSourceConnection,
        _restored_offsets: Vec<(PartitionId, Option<MzOffset>)>,
        _encoding: SourceDataEncoding,
        metrics: SourceBaseMetrics,
        _connection_context: ConnectionContext,
    ) -> Result<Self, anyhow::Error> {
        let connection = match connection {
            ExternalSourceConnection::Postgres(pg) => pg,
            _ => {
                panic!("Postgres is the only legitimate ExternalSourceConnection for PostgresSourceReader")
            }
        };

        let (dataflow_tx, dataflow_rx) = tokio::sync::mpsc::channel(10_000);

        let task_info = PostgresTaskInfo {
            source_id: source_id.clone(),
            connection: connection.clone(),
            /// Our cursor into the WAL
            lsn: 0.into(),
            metrics: PgSourceMetrics::new(&metrics, source_id),
            source_tables: HashMap::from_iter(
                connection.details.tables.iter().map(|t| (t.oid, t.clone())),
            ),
            sender: dataflow_tx,
            activator: consumer_activator,
        };

        task::spawn(
            || format!("postgres_source:{}", source_id),
            postgres_replication_loop(task_info),
        );
        Ok(Self {
            receiver_stream: dataflow_rx,
        })
    }

    // TODO(guswynn): use `next` instead of using a channel
    fn get_next_message(
        &mut self,
    ) -> Result<NextMessage<Self::Key, Self::Value, Self::Diff>, SourceReaderError> {
        // TODO(guswynn): consider if `try_recv` is better or the same as `now_or_never`
        let ret = match self.receiver_stream.recv().now_or_never() {
            Some(Some(InternalMessage::Value {
                value,
                diff,
                lsn,
                end,
            })) => {
                if end {
                    Ok(NextMessage::Ready(SourceMessageType::Finalized(
                        SourceMessage {
                            partition: PartitionId::None,
                            offset: lsn.into(),
                            upstream_time_millis: None,
                            key: (),
                            value,
                            headers: None,
                            specific_diff: diff,
                        },
                    )))
                } else {
                    Ok(NextMessage::Ready(SourceMessageType::InProgress(
                        SourceMessage {
                            partition: PartitionId::None,
                            offset: lsn.into(),
                            upstream_time_millis: None,
                            key: (),
                            value,
                            headers: None,
                            specific_diff: diff,
                        },
                    )))
                }
            }
            Some(Some(InternalMessage::Err(e))) => Err(e),
            None => Ok(NextMessage::Pending),
            Some(None) => Ok(NextMessage::Finished),
        };

        ret
    }
}

/// Defers to `postgres_replication_loop_inner` and sends errors through the channel if they occur
async fn postgres_replication_loop(mut task_info: PostgresTaskInfo) {
    match postgres_replication_loop_inner(&mut task_info).await {
        Ok(()) => {}
        Err(e) => {
            // Drop the send error, as we have no way of communicating back to the
            // source operator if the channel is gone.
            let _ = task_info.sender.send(InternalMessage::Err(e)).await;
            task_info
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
    let source_id = task_info.source_id.clone();
    // Buffer rows from snapshot to retract and retry, if initial snapshot fails.
    // Postgres sources cannot proceed without a successful snapshot.
    let mut snapshot_tx = Transaction::new();
    match task_info.produce_snapshot(&mut snapshot_tx).await {
        Ok(_) => {
            info!(
                "replication snapshot for source {} succeeded",
                &task_info.source_id
            );
            snapshot_tx
                .close(task_info.lsn, &task_info.sender, &task_info.activator)
                .await;
        }
        Err(ReplicationError::Recoverable(e)) => {
            // TODO: In the future we probably want to handle this more gracefully,
            // to avoid stressing out any monitoring tools,
            // but for now panicking is the easiest way to dump the data in the pipe.
            // The restarted storaged instance will restart the snapshot fresh, which will
            // avoid any inconsistencies. Note that if the same lsn is chosen in the
            // next snapshotting, the remapped timestamp chosen will be the same for
            // both instances of storaged.
            panic!(
                "replication snapshot for source {} failed: {}",
                &task_info.source_id, e
            );
        }
        Err(ReplicationError::Fatal(e)) => {
            return Err(SourceReaderError {
                inner: SourceErrorDetails::Initialization(e.to_string()),
            })
        }
    }

    loop {
        match task_info.produce_replication().await {
            Err(ReplicationError::Recoverable(e)) => {
                warn!(
                    "replication for source {} interrupted, retrying: {}",
                    source_id, e
                )
            }
            Err(ReplicationError::Fatal(e)) => {
                return Err(SourceReaderError {
                    inner: SourceErrorDetails::FileIO(e.to_string()),
                })
            }
            Ok(_) => {
                // shutdown iniated elsewhere
                return Ok(());
            }
        }

        // TODO(petrosagg): implement exponential back-off
        tokio::time::sleep(Duration::from_secs(3)).await;
        info!("resuming replication for source {}", source_id);
    }
}

/// A helper struct build and produce transactions
struct Transaction {
    rows: Vec<(Row, Diff)>,
}

impl Transaction {
    pub fn new() -> Self {
        Transaction { rows: vec![] }
    }

    /// Record an insertion of a row in the current transaction
    pub fn insert(&mut self, row: Row) {
        self.rows.push((row, 1));
    }

    /// Record a deletion of a row in the current transaction
    pub fn delete(&mut self, row: Row) {
        self.rows.push((row, -1));
    }

    /// Finalize a transaction and send it off through the channel
    ///
    /// Care must be taken to ALWAYS call this if inserts/deletes have occurred,
    /// unless you are on an early-exit error path, and the `PostgresSourceReader`
    /// is shutting down.
    pub async fn close(
        &mut self,
        lsn: PgLsn,
        sender: &Sender<InternalMessage>,
        activator: &SyncActivator,
    ) {
        let num = self.rows.len();
        for (i, (row, diff)) in self.rows.drain(..).enumerate() {
            // a closed receiver means the source has been shutdown
            // (dropped or the process is dying), so just continue on
            // without activation
            if let Ok(_) = sender
                .send(InternalMessage::Value {
                    value: row,
                    lsn,
                    diff,
                    end: i == (num - 1),
                })
                .await
            {
                activator
                    .activate()
                    .expect("postgres reader activation failed");
            }
        }
    }
}

// implement the core pg logic in this impl block
impl PostgresTaskInfo {
    /// Validates that all expected tables exist in the publication tables and they have the same schema
    fn validate_tables(&self, tables: Vec<PostgresTableDesc>) -> Result<(), anyhow::Error> {
        let pub_tables: HashMap<u32, PostgresTableDesc> =
            tables.into_iter().map(|t| (t.oid, t)).collect();
        for (id, schema) in self.source_tables.iter() {
            match pub_tables.get(id) {
                Some(pub_schema) => {
                    if pub_schema != schema {
                        error!(
                            "Error validating table in publication. Expected: {:?} Actual: {:?}",
                            schema, pub_schema
                        );
                        bail!("Schema for table {} differs, recreate Materialize source to use new schema", schema.name)
                    }
                }
                None => {
                    error!("publication missing table: {} with id {}", schema.name, id);
                    bail!(
                        "Publication missing expected table {} with oid {}",
                        schema.name,
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
    async fn produce_snapshot(
        &mut self,
        snapshot_tx: &mut Transaction,
    ) -> Result<(), ReplicationError> {
        let client =
            try_recoverable!(mz_postgres_util::connect_replication(&self.connection.conn).await);

        // We're initialising this source so any previously existing slot must be removed and
        // re-created. Once we have data persistence we will be able to reuse slots across restarts
        let _ = client
            .simple_query(&format!(
                "DROP_REPLICATION_SLOT {:?}",
                &self.connection.details.slot
            ))
            .await;

        // Get all the relevant tables for this publication
        let publication_tables = try_recoverable!(
            mz_postgres_util::publication_info(&self.connection.conn, &self.connection.publication)
                .await
        );
        // Validate publication tables against the state snapshot
        try_fatal!(self.validate_tables(publication_tables));

        // Start a transaction and immediately create a replication slot with the USE SNAPSHOT
        // directive. This makes the starting point of the slot and the snapshot of the transaction
        // identical.
        client
            .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
            .await?;

        let slot_query = format!(
            r#"CREATE_REPLICATION_SLOT {:?} LOGICAL "pgoutput" USE_SNAPSHOT"#,
            &self.connection.details.slot
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
                ReplicationError::Recoverable(anyhow!(
                    "empty result after creating replication slot"
                ))
            })?;

        // Store the lsn at which we will need to start the replication stream from
        let consistent_point = try_recoverable!(slot_row
            .get("consistent_point")
            .ok_or_else(|| anyhow!("missing expected column: `consistent_point`")));
        self.lsn = try_fatal!(consistent_point
            .parse()
            .or_else(|_| Err(anyhow!("invalid lsn"))));
        for info in self.source_tables.values() {
            let relation_id: Datum = (i32::try_from(info.oid).unwrap()).into();
            let reader = client
                .copy_out_simple(
                    format!(
                        "COPY {:?}.{:?} TO STDOUT (FORMAT TEXT, DELIMITER '\t')",
                        info.namespace, info.name
                    )
                    .as_str(),
                )
                .await?;

            pin_mut!(reader);
            let mut mz_row = Row::default();
            // TODO: once tokio-stream is released with https://github.com/tokio-rs/tokio/pull/4502
            //    we can convert this into a single `timeout(...)` call on the reader CopyOutStream
            while let Some(b) = tokio::time::timeout(Duration::from_secs(30), reader.next())
                .await?
                .transpose()?
            {
                let mut packer = mz_row.packer();
                packer.push(relation_id);
                // Convert raw rows from COPY into repr:Row. Each Row is a relation_id
                // and list of string-encoded values, e.g. Row{ 16391 , ["1", "2"] }
                let parser = mz_pgcopy::CopyTextFormatParser::new(b.as_ref(), "\t", "\\N");

                let mut raw_values = parser.iter_raw(info.columns.len() as i32);
                try_fatal!(packer.push_list_with(|rp| -> Result<(), anyhow::Error> {
                    while let Some(raw_value) = raw_values.next() {
                        match raw_value? {
                            Some(value) => rp.push(Datum::String(std::str::from_utf8(value)?)),
                            None => rp.push(Datum::Null),
                        }
                    }
                    Ok(())
                }));
                snapshot_tx.insert(mz_row.clone());
            }

            self.metrics.tables.inc();
        }
        self.metrics.lsn.set(self.lsn.into());
        client.simple_query("COMMIT;").await?;
        Ok(())
    }

    /// Converts a Tuple received in the replication stream into a Row instance. The logical
    /// replication protocol doesn't use the binary encoding for column values so contrary to the
    /// initial snapshot here we need to parse the textual form of each column.
    ///
    /// The `old_tuple` argument can be used as a source of data to use when encountering unchanged
    /// TOAST values.
    fn row_from_tuple<'a, T>(rel_id: u32, tuple_data: T) -> Result<Row, anyhow::Error>
    where
        T: IntoIterator<Item = &'a TupleData>,
    {
        let mut row = Row::default();
        let mut packer = row.packer();

        let rel_id: Datum = (rel_id as i32).into();
        packer.push(rel_id);
        packer.push_list_with(move |packer| {
            for val in tuple_data.into_iter() {
                let datum = match val {
                    TupleData::Null => Datum::Null,
                    TupleData::UnchangedToast => bail!(
                        "Missing TOASTed value from table with OID = {}. \
                        Did you forget to set REPLICA IDENTITY to FULL for your table?",
                        rel_id
                    ),
                    TupleData::Text(b) => std::str::from_utf8(&b)?.into(),
                };
                packer.push(datum);
            }
            Ok(())
        })?;

        Ok(row)
    }

    async fn produce_replication(&mut self) -> Result<(), ReplicationError> {
        use ReplicationError::*;

        let client =
            try_recoverable!(mz_postgres_util::connect_replication(&self.connection.conn).await);

        let query = format!(
            r#"START_REPLICATION SLOT "{name}" LOGICAL {lsn}
              ("proto_version" '1', "publication_names" '{publication}')"#,
            name = &self.connection.details.slot,
            lsn = self.lsn,
            publication = self.connection.publication
        );
        let copy_stream = try_recoverable!(client.copy_both_simple(&query).await);

        let stream = LogicalReplicationStream::new(copy_stream);
        tokio::pin!(stream);

        let mut last_keepalive = Instant::now();
        let mut inserts = vec![];
        let mut deletes = vec![];
        let closer = self.sender.clone();
        loop {
            // This select is safe because `Sender::closed` is cancel-safe
            // and when `closed` finishes, dropping the `try_next` future is fine,
            // as we are shutting down and don't have any control over whether we
            // would have seen the next item or not. Additionally, `try_next`
            // just holds a reference to the stream, so it is cancel-safe.
            //
            // TODO(guswynn): avoid this select! complexity by just moving to `SourceReader::next`
            tokio::select! {
                biased;
                _ = closer.closed() => {
                    return Ok(())
                },
                item = stream.try_next() => {
                    if let Some(item) = item? {
                        use ReplicationMessage::*;
                        // The upstream will periodically request keepalive responses by setting the reply field
                        // to 1. However, we cannot rely on these messages arriving on time. For example, when
                        // the upstream is sending a big transaction its keepalive messages are queued and can
                        // be delayed arbitrarily.  Therefore, we also make sure to send a proactive keepalive
                        // every 30 seconds.
                        //
                        // See: https://www.postgresql.org/message-id/CAMsr+YE2dSfHVr7iEv1GSPZihitWX-PMkD9QALEGcTYa+sdsgg@mail.gmail.com
                        if matches!(item, PrimaryKeepAlive(ref k) if k.reply() == 1)
                            || last_keepalive.elapsed() > Duration::from_secs(30)
                        {
                            let ts: i64 = PG_EPOCH
                                .elapsed()
                                .expect("system clock set earlier than year 2000!")
                                .as_micros()
                                .try_into()
                                .expect("software more than 200k years old, consider updating");

                            try_recoverable!(
                                stream
                                    .as_mut()
                                    .standby_status_update(self.lsn, self.lsn, self.lsn, ts, 0)
                                    .await
                            );
                            last_keepalive = Instant::now();
                        }
                        match item {
                            XLogData(xlog_data) => {
                                self.metrics.total.inc();
                                use LogicalReplicationMessage::*;

                                match xlog_data.data() {
                                    Begin(_) => {
                                        if !inserts.is_empty() || !deletes.is_empty() {
                                            return Err(Fatal(anyhow!(
                                                "got BEGIN statement after uncommitted data"
                                            )));
                                        }
                                    }
                                    Insert(insert) => {
                                        self.metrics.inserts.inc();
                                        let rel_id = insert.rel_id();
                                        if !self.source_tables.contains_key(&rel_id) {
                                            continue;
                                        }
                                        let new_tuple = insert.tuple().tuple_data();
                                        let row = try_fatal!(PostgresTaskInfo::row_from_tuple(rel_id, new_tuple));
                                        inserts.push(row);
                                    }
                                    Update(update) => {
                                        self.metrics.updates.inc();
                                        let rel_id = update.rel_id();
                                        if !self.source_tables.contains_key(&rel_id) {
                                            continue;
                                        }
                                        let old_tuple = try_fatal!(update
                                            .old_tuple()
                                            .ok_or_else(|| anyhow!("Old row missing from replication stream for table with OID = {}. \
                                                    Did you forget to set REPLICA IDENTITY to FULL for your table?", rel_id)))
                                        .tuple_data();
                                        let old_row =
                                            try_fatal!(PostgresTaskInfo::row_from_tuple(rel_id, old_tuple));
                                        deletes.push(old_row);

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
                                        let new_row =
                                            try_fatal!(PostgresTaskInfo::row_from_tuple(rel_id, new_tuple));
                                        inserts.push(new_row);
                                    }
                                    Delete(delete) => {
                                        self.metrics.deletes.inc();
                                        let rel_id = delete.rel_id();
                                        if !self.source_tables.contains_key(&rel_id) {
                                            continue;
                                        }
                                        let old_tuple = try_fatal!(delete
                                            .old_tuple()
                                            .ok_or_else(|| anyhow!("Old row missing from replication stream for table with OID = {}. \
                                                    Did you forget to set REPLICA IDENTITY to FULL for your table?", rel_id)))
                                        .tuple_data();
                                        let row = try_fatal!(PostgresTaskInfo::row_from_tuple(rel_id, old_tuple));
                                        deletes.push(row);
                                    }
                                    Commit(commit) => {
                                        self.metrics.transactions.inc();
                                        self.lsn = commit.end_lsn().into();

                                        let mut tx = Transaction::new();

                                        for row in deletes.drain(..) {
                                            tx.delete(row);
                                        }
                                        for row in inserts.drain(..) {
                                            tx.insert(row);
                                        }

                                        tx.close(self.lsn, &self.sender, &self.activator).await;
                                        self.metrics.lsn.set(self.lsn.into());
                                    }
                                    Relation(relation) => {
                                        let rel_id = relation.rel_id();
                                        match self.source_tables.get(&rel_id) {
                                            Some(source_table) => {
                                                // Start with the cheapest check first, this will catch the majority of alters
                                                if source_table.columns.len() != relation.columns().len() {
                                                    error!(
                                                        "alter table detected on {} with id {}",
                                                        source_table.name, source_table.oid
                                                    );
                                                    return Err(Fatal(anyhow!(
                                                        "source table {} with oid {} has been altered",
                                                        source_table.name,
                                                        source_table.oid
                                                    )));
                                                }
                                                if source_table.name.ne(relation.name().unwrap())
                                                    || source_table.namespace.ne(relation.namespace().unwrap())
                                                {
                                                    error!(
                                                        "table name changed on {}.{} with id {} to {}.{}",
                                                        source_table.namespace,
                                                        source_table.name,
                                                        source_table.oid,
                                                        relation.namespace().unwrap(),
                                                        relation.name().unwrap()
                                                    );
                                                    return Err(Fatal(anyhow!(
                                                        "source table {} with oid {} has been altered",
                                                        source_table.name,
                                                        source_table.oid
                                                    )));
                                                }
                                                // Relation messages do not include nullability/primary_key data
                                                // so we check the name, type_oid, and type_mod explicitly and error if any of them differ
                                                if !source_table.columns.iter().zip(relation.columns()).all(
                                                    |(src, rel)| {
                                                        src.name == rel.name().unwrap()
                                                            && src.type_oid == u32::from_be_bytes(rel.type_id().to_be_bytes())
                                                            && src.type_mod == rel.type_modifier()
                                                    },
                                                ) {
                                                    error!("alter table error: name {}, oid {}, old_schema {:?}, new_schema {:?}", source_table.name, source_table.oid, source_table.columns, relation.columns());
                                                    return Err(Fatal(anyhow!(
                                                        "source table {} with oid {} has been altered",
                                                        source_table.name,
                                                        source_table.oid
                                                    )));
                                                }
                                            }
                                            // Ignore messages for tables we do not know about
                                            None => continue,
                                        }
                                    }
                                    Origin(_) | Type(_) => {
                                        self.metrics.ignored.inc();
                                    }
                                    Truncate(truncate) => {
                                        let tables = truncate
                                            .rel_ids()
                                            .iter()
                                            // Filter here makes option handling in map "safe"
                                            .filter_map(|id| self.source_tables.get(&id))
                                            .map(|table| {
                                                format!("name: {} id: {}", table.name, table.oid)
                                            })
                                            .collect::<Vec<String>>();
                                        return Err(Fatal(anyhow!(
                                            "source table(s) {} got truncated",
                                            tables.join(", ")
                                        )));
                                    }
                                    // The enum is marked as non_exhaustive. Better to be conservative here in
                                    // case a new message is relevant to the semantics of our source
                                    _ => return Err(Fatal(anyhow!("unexpected logical replication message"))),
                                }
                            }
                            // Handled above
                            PrimaryKeepAlive(_) => {}
                            // The enum is marked non_exhaustive, better be conservative
                            _ => return Err(Fatal(anyhow!("Unexpected replication message"))),
                        }
                    } else {
                        return Err(Recoverable(anyhow!("replication stream ended")))
                    }
                }
            }
        }
    }
}
