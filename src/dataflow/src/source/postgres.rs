// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail};
use async_trait::async_trait;
use futures::{pin_mut, StreamExt, TryStreamExt};
use lazy_static::lazy_static;
use postgres_protocol::message::backend::{
    LogicalReplicationMessage, ReplicationMessage, TupleData,
};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::runtime::Handle;
use tokio_postgres::error::{DbError, Severity, SqlState};
use tokio_postgres::replication::LogicalReplicationStream;
use tokio_postgres::types::PgLsn;
use tokio_postgres::SimpleQueryMessage;
use tracing::{info, warn};

use crate::source::{SimpleSource, SourceError, SourceTransaction, Timestamper};
use mz_dataflow_types::{sources::PostgresSourceConnector, SourceErrorDetails};
use mz_expr::SourceInstanceId;
use mz_repr::{Datum, Row};

use self::metrics::PgSourceMetrics;
use super::metrics::SourceBaseMetrics;

mod metrics;

lazy_static! {
    /// Postgres epoch is 2000-01-01T00:00:00Z
    static ref PG_EPOCH: SystemTime = UNIX_EPOCH + Duration::from_secs(946_684_800);
}

/// Information required to sync data from Postgres
pub struct PostgresSourceReader {
    source_id: SourceInstanceId,
    connector: PostgresSourceConnector,
    /// Our cursor into the WAL
    lsn: PgLsn,
    metrics: PgSourceMetrics,
}

trait ErrorExt {
    fn is_recoverable(&self) -> bool;
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

impl PostgresSourceReader {
    /// Constructs a new instance
    pub fn new(
        source_id: SourceInstanceId,
        connector: PostgresSourceConnector,
        metrics: &SourceBaseMetrics,
    ) -> Self {
        Self {
            source_id,
            connector,
            lsn: 0.into(),
            metrics: PgSourceMetrics::new(metrics, source_id),
        }
    }

    /// Creates the replication slot and produces the initial snapshot of the data
    ///
    /// After the initial snapshot has been produced it returns the name of the created slot and
    /// the LSN at which we should start the replication stream at.
    async fn produce_snapshot<W: AsyncWrite + Unpin>(
        &mut self,
        snapshot_tx: &mut SourceTransaction<'_>,
        buffer: &mut W,
    ) -> Result<(), ReplicationError> {
        let client =
            try_recoverable!(mz_postgres_util::connect_replication(&self.connector.conn).await);

        // We're initialising this source so any previously existing slot must be removed and
        // re-created. Once we have data persistence we will be able to reuse slots across restarts
        let _ = client
            .simple_query(&format!(
                "DROP_REPLICATION_SLOT {:?}",
                &self.connector.slot_name
            ))
            .await;

        // Get all the relevant tables for this publication
        let publication_tables = try_recoverable!(
            mz_postgres_util::publication_info(&self.connector.conn, &self.connector.publication)
                .await
        );

        // Start a transaction and immediatelly create a replication slot with the USE SNAPSHOT
        // directive. This makes the starting point of the slot and the snapshot of the transaction
        // identical.
        client
            .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
            .await?;

        let slot_query = format!(
            r#"CREATE_REPLICATION_SLOT {:?} LOGICAL "pgoutput" USE_SNAPSHOT"#,
            &self.connector.slot_name
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
        for info in publication_tables {
            let relation_id: Datum = (info.rel_id as i32).into();
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
            while let Some(Ok(b)) = reader.next().await {
                let mut packer = mz_row.packer();
                packer.push(relation_id);
                // Convert raw rows from COPY into repr:Row. Each Row is a relation_id
                // and list of string-encoded values, e.g. Row{ 16391 , ["1", "2"] }
                let parser = mz_pgcopy::CopyTextFormatParser::new(b.as_ref(), "\t", "\\N");

                let mut raw_values = parser.iter_raw(info.schema.len() as i32);
                try_fatal!(packer.push_list_with(|rp| -> Result<(), anyhow::Error> {
                    while let Some(raw_value) = raw_values.next() {
                        match raw_value? {
                            Some(value) => rp.push(Datum::String(std::str::from_utf8(value)?)),
                            None => rp.push(Datum::Null),
                        }
                    }
                    Ok(())
                }));
                try_recoverable!(snapshot_tx.insert(mz_row.clone()).await);
                try_fatal!(buffer.write(&try_fatal!(bincode::serialize(&mz_row))).await);
            }

            self.metrics.tables.inc();
        }
        self.metrics.lsn.set(self.lsn.into());
        client.simple_query("COMMIT;").await?;
        Ok(())
    }

    /// Reverts a failed snapshot by deleting any processed rows from the dataflow.
    async fn revert_snapshot<R: Read + Seek>(
        &self,
        snapshot_tx: &mut SourceTransaction<'_>,
        mut reader: R,
    ) -> Result<(), anyhow::Error> {
        tokio::task::block_in_place(|| -> Result<(), anyhow::Error> {
            let len = reader.seek(SeekFrom::Current(0))?;
            reader.seek(SeekFrom::Start(0))?;
            let mut reader = reader.take(len);
            let handle = Handle::current();
            while reader.limit() > 0 {
                let row = bincode::deserialize_from(&mut reader)?;
                handle.block_on(snapshot_tx.delete(row))?;
            }
            Ok(())
        })
    }

    /// Converts a Tuple received in the replication stream into a Row instance. The logical
    /// replication protocol doesn't use the binary encoding for column values so contrary to the
    /// initial snapshot here we need to parse the textual form of each column.
    ///
    /// The `old_tuple` argument can be used as a source of data to use when encountering unchanged
    /// TOAST values.
    fn row_from_tuple<'a, T>(&mut self, rel_id: u32, tuple_data: T) -> Result<Row, anyhow::Error>
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

    async fn produce_replication(
        &mut self,
        timestamper: &Timestamper,
    ) -> Result<(), ReplicationError> {
        use ReplicationError::*;

        let client =
            try_recoverable!(mz_postgres_util::connect_replication(&self.connector.conn).await);

        let query = format!(
            r#"START_REPLICATION SLOT "{name}" LOGICAL {lsn}
              ("proto_version" '1', "publication_names" '{publication}')"#,
            name = &self.connector.slot_name,
            lsn = self.lsn,
            publication = self.connector.publication
        );
        let copy_stream = try_recoverable!(client.copy_both_simple(&query).await);

        let stream = LogicalReplicationStream::new(copy_stream);
        tokio::pin!(stream);

        let mut last_keepalive = Instant::now();
        let mut inserts = vec![];
        let mut deletes = vec![];
        while let Some(item) = stream.try_next().await? {
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
                            let new_tuple = insert.tuple().tuple_data();
                            let row = try_fatal!(self.row_from_tuple(rel_id, new_tuple));
                            inserts.push(row);
                        }
                        Update(update) => {
                            self.metrics.updates.inc();
                            let rel_id = update.rel_id();
                            let old_tuple = try_fatal!(update
                                .old_tuple()
                                .ok_or_else(|| anyhow!("Old row missing from replication stream for table with OID = {}. \
                                        Did you forget to set REPLICA IDENTITY to FULL for your table?", rel_id)))
                            .tuple_data();
                            let old_row = try_fatal!(self.row_from_tuple(rel_id, old_tuple));
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
                            let new_row = try_fatal!(self.row_from_tuple(rel_id, new_tuple));
                            inserts.push(new_row);
                        }
                        Delete(delete) => {
                            self.metrics.deletes.inc();
                            let rel_id = delete.rel_id();
                            let old_tuple = try_fatal!(delete
                                .old_tuple()
                                .ok_or_else(|| anyhow!("Old row missing from replication stream for table with OID = {}. \
                                        Did you forget to set REPLICA IDENTITY to FULL for your table?", rel_id)))
                            .tuple_data();
                            let row = try_fatal!(self.row_from_tuple(rel_id, old_tuple));
                            deletes.push(row);
                        }
                        Commit(commit) => {
                            self.metrics.transactions.inc();
                            self.lsn = commit.end_lsn().into();

                            let tx = timestamper.start_tx().await;

                            for row in deletes.drain(..) {
                                try_fatal!(tx.delete(row).await);
                            }
                            for row in inserts.drain(..) {
                                try_fatal!(tx.insert(row).await);
                            }
                            self.metrics.lsn.set(self.lsn.into());
                        }
                        Origin(_) | Relation(_) | Type(_) => {
                            self.metrics.ignored.inc();
                        }
                        Truncate(_) => return Err(Fatal(anyhow!("source table got truncated"))),
                        // The enum is marked as non_exaustive. Better to be conservative here in
                        // case a new message is relevant to the semantics of our source
                        _ => return Err(Fatal(anyhow!("unexpected logical replication message"))),
                    }
                }
                // Handled above
                PrimaryKeepAlive(_) => {}
                // The enum is marked non_exaustive, better be conservative
                _ => return Err(Fatal(anyhow!("Unexpected replication message"))),
            }
        }
        Err(Recoverable(anyhow!("replication stream ended")))
    }
}

#[async_trait]
impl SimpleSource for PostgresSourceReader {
    /// The top-level control of the state machine and retry logic
    async fn start(mut self, timestamper: &Timestamper) -> Result<(), SourceError> {
        // Buffer rows from snapshot to retract and retry, if initial snapshot fails.
        // Postgres sources cannot proceed without a successful snapshot.
        {
            let mut snapshot_tx = timestamper.start_tx().await;
            loop {
                let file =
                    tokio::fs::File::from_std(tempfile::tempfile().map_err(|e| SourceError {
                        source_id: self.source_id,
                        error: SourceErrorDetails::FileIO(e.to_string()),
                    })?);
                let mut writer = tokio::io::BufWriter::new(file);
                match self.produce_snapshot(&mut snapshot_tx, &mut writer).await {
                    Ok(_) => {
                        info!(
                            "replication snapshot for source {} succeeded",
                            &self.source_id
                        );
                        break;
                    }
                    Err(ReplicationError::Recoverable(e)) => {
                        writer.flush().await.map_err(|e| SourceError {
                            source_id: self.source_id,
                            error: SourceErrorDetails::Initialization(e.to_string()),
                        })?;
                        warn!(
                            "replication snapshot for source {} failed, retrying: {}",
                            &self.source_id, e
                        );
                        let reader = BufReader::new(writer.into_inner().into_std().await);
                        self.revert_snapshot(&mut snapshot_tx, reader)
                            .await
                            .map_err(|e| SourceError {
                                source_id: self.source_id,
                                error: SourceErrorDetails::FileIO(e.to_string()),
                            })?;
                    }
                    Err(ReplicationError::Fatal(e)) => {
                        return Err(SourceError {
                            source_id: self.source_id,
                            error: SourceErrorDetails::Initialization(e.to_string()),
                        })
                    }
                }

                // TODO(petrosagg): implement exponential back-off
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        }

        loop {
            match self.produce_replication(timestamper).await {
                Err(ReplicationError::Recoverable(e)) => {
                    warn!(
                        "replication for source {} interrupted, retrying: {}",
                        self.source_id, e
                    )
                }
                Err(ReplicationError::Fatal(e)) => {
                    return Err(SourceError {
                        source_id: self.source_id,
                        error: SourceErrorDetails::FileIO(e.to_string()),
                    })
                }
                Ok(_) => unreachable!("replication stream cannot exit without an error"),
            }

            // TODO(petrosagg): implement exponential back-off
            tokio::time::sleep(Duration::from_secs(3)).await;
            info!("resuming replication for source {}", self.source_id);
        }
    }
}
