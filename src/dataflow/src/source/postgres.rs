// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::TryInto;
use std::error::Error;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail};
use async_trait::async_trait;
use futures::TryStreamExt;
use lazy_static::lazy_static;

use postgres_protocol::message::backend::{
    LogicalReplicationMessage, ReplicationMessage, Tuple, TupleData,
};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::runtime::Handle;
use tokio_postgres::error::{DbError, SqlState};
use tokio_postgres::replication::LogicalReplicationStream;
use tokio_postgres::types::PgLsn;
use tokio_postgres::SimpleQueryMessage;

use crate::source::{SimpleSource, SourceError, SourceTransaction, Timestamper};
use dataflow_types::{PostgresSourceConnector, SourceErrorDetails};
use ore::result::*;
use repr::{Datum, Row};

lazy_static! {
    /// Postgres epoch is 2000-01-01T00:00:00Z
    static ref PG_EPOCH: SystemTime = UNIX_EPOCH + Duration::from_secs(946_684_800);
}

/// Information required to sync data from Postgres
pub struct PostgresSourceReader {
    /// Used to produce useful error messages
    source_name: String,
    connector: PostgresSourceConnector,
    /// Our cursor into the WAL
    lsn: PgLsn,
}

/// Free standing function that converts a tokio_postgres error into a Severity level.
///
/// This cannot be a [From] implementation due to Rust's orphan rules
fn into_severity(e: tokio_postgres::Error) -> Severity<anyhow::Error> {
    let recoverable = match e.source() {
        Some(err) => {
            match err.downcast_ref::<DbError>() {
                Some(db_err) => {
                    use tokio_postgres::error::Severity::*;
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
    };
    if recoverable {
        Severity::Recoverable(e.into())
    } else {
        Severity::Fatal(e.into())
    }
}

impl PostgresSourceReader {
    /// Constructs a new instance
    pub fn new(source_name: String, connector: PostgresSourceConnector) -> Self {
        Self {
            source_name,
            connector,
            lsn: 0.into(),
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
    ) -> Result<(), Severity<anyhow::Error>> {
        let client = postgres_util::connect_replication(&self.connector.conn)
            .await
            .recoverable()?;

        // We're initialising this source so any previously existing slot must be removed and
        // re-created. Once we have data persistence we will be able to reuse slots across restarts
        let _ = client
            .simple_query(&format!(
                "DROP_REPLICATION_SLOT {:?}",
                &self.connector.slot_name
            ))
            .await;

        // Get all the relevant tables for this publication
        let publication_tables =
            postgres_util::publication_info(&self.connector.conn, &self.connector.publication)
                .await
                .recoverable()?;

        // Start a transaction and immediatelly create a replication slot with the USE SNAPSHOT
        // directive. This makes the starting point of the slot and the snapshot of the transaction
        // identical.
        client
            .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
            .await
            .map_err(into_severity)?;

        let slot_query = format!(
            r#"CREATE_REPLICATION_SLOT {:?} LOGICAL "pgoutput" USE_SNAPSHOT"#,
            &self.connector.slot_name
        );
        let slot_row = client
            .simple_query(&slot_query)
            .await
            .map_err(into_severity)?
            .into_iter()
            .next()
            .and_then(|msg| match msg {
                SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .ok_or_else(|| anyhow!("empty result after creating replication slot"))
            .recoverable()?;

        // Store the lsn at which we will need to start the replication stream from
        let consistent_point = slot_row
            .get("consistent_point")
            .ok_or_else(|| anyhow!("missing expected column: `consistent_point`"))
            .recoverable()?;
        self.lsn = consistent_point
            .parse()
            .or_else(|_| Err(anyhow!("invalid lsn")))
            .fatal()?;

        for info in publication_tables {
            // TODO(petrosagg): use a COPY statement here for more efficient network transfer
            let data = client
                .simple_query(&format!(
                    "SELECT * FROM {:?}.{:?}",
                    info.namespace, info.name
                ))
                .await
                .map_err(into_severity)?;
            for msg in data {
                if let SimpleQueryMessage::Row(row) = msg {
                    let mut mz_row = Row::default();
                    let rel_id: Datum = (info.rel_id as i32).into();
                    mz_row.push(rel_id);
                    mz_row.push_list((0..row.len()).map(|n| {
                        let a: Datum = row.get(n).into();
                        a
                    }));
                    snapshot_tx.insert(mz_row.clone()).await.recoverable()?;
                    buffer
                        .write(&bincode::serialize(&mz_row).map_err(|e| e.into()).fatal()?)
                        .await
                        .map_err(|e| e.into())
                        .fatal()?;
                }
            }
        }
        client
            .simple_query("COMMIT;")
            .await
            .map_err(into_severity)?;
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
    fn row_from_tuple(&mut self, rel_id: u32, tuple: &Tuple) -> Result<Row, anyhow::Error> {
        let mut row = Row::default();

        let rel_id: Datum = (rel_id as i32).into();
        row.push(rel_id);
        row.push_list_with(|packer| {
            for val in tuple.tuple_data() {
                let datum = match val {
                    TupleData::Null => Datum::Null,
                    TupleData::UnchangedToast => bail!("Unsupported TOAST value"),
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
    ) -> Result<(), Severity<anyhow::Error>> {
        let client = postgres_util::connect_replication(&self.connector.conn)
            .await
            .recoverable()?;

        let query = format!(
            r#"START_REPLICATION SLOT "{name}" LOGICAL {lsn}
              ("proto_version" '1', "publication_names" '{publication}')"#,
            name = &self.connector.slot_name,
            lsn = self.lsn.to_string(),
            publication = self.connector.publication
        );
        let copy_stream = client
            .copy_both_simple(&query)
            .await
            .map_err(into_severity)?;

        let stream = LogicalReplicationStream::new(copy_stream);
        tokio::pin!(stream);

        let mut last_keepalive = Instant::now();
        let mut inserts = vec![];
        let mut deletes = vec![];
        while let Some(item) = stream.try_next().await.map_err(into_severity)? {
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

                stream
                    .as_mut()
                    .standby_status_update(self.lsn, self.lsn, self.lsn, ts, 0)
                    .await
                    .map_err(into_severity)?;
                last_keepalive = Instant::now();
            }
            match item {
                XLogData(xlog_data) => {
                    use LogicalReplicationMessage::*;

                    match xlog_data.data() {
                        Begin(_) => {
                            if !inserts.is_empty() || !deletes.is_empty() {
                                return Err(Severity::Fatal(anyhow!(
                                    "got BEGIN statement after uncommitted data"
                                )));
                            }
                        }
                        Insert(insert) => {
                            let rel_id = insert.rel_id();
                            let row = self.row_from_tuple(rel_id, insert.tuple()).fatal()?;
                            inserts.push(row);
                        }
                        Update(update) => {
                            let rel_id = update.rel_id();
                            let old_tuple = update
                                .old_tuple()
                                .ok_or_else(|| anyhow!("full row missing from update"))
                                .fatal()?;
                            let old_row = self.row_from_tuple(rel_id, old_tuple).fatal()?;
                            deletes.push(old_row);

                            let new_row =
                                self.row_from_tuple(rel_id, update.new_tuple()).fatal()?;
                            inserts.push(new_row);
                        }
                        Delete(delete) => {
                            let rel_id = delete.rel_id();
                            let old_tuple = delete
                                .old_tuple()
                                .ok_or_else(|| anyhow!("full row missing from delete"))
                                .fatal()?;
                            let row = self.row_from_tuple(rel_id, old_tuple).fatal()?;
                            deletes.push(row);
                        }
                        Commit(commit) => {
                            self.lsn = commit.end_lsn().into();

                            let tx = timestamper.start_tx().await;

                            for row in deletes.drain(..) {
                                tx.delete(row).await.fatal()?;
                            }
                            for row in inserts.drain(..) {
                                tx.insert(row).await.fatal()?;
                            }
                        }
                        Origin(_) | Relation(_) | Type(_) => (),
                        Truncate(_) => {
                            return Err(Severity::Fatal(anyhow!("source table got truncated")))
                        }
                        // The enum is marked as non_exaustive. Better to be conservative here in
                        // case a new message is relevant to the semantics of our source
                        _ => {
                            return Err(Severity::Fatal(anyhow!(
                                "unexpected logical replication message"
                            )))
                        }
                    }
                }
                // Handled above
                PrimaryKeepAlive(_) => {}
                // The enum is marked non_exaustive, better be conservative
                _ => return Err(Severity::Fatal(anyhow!("Unexpected replication message"))),
            }
        }
        Err(Severity::Recoverable(anyhow!("replication stream ended")))
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
                        source_name: self.source_name.clone(),
                        error: SourceErrorDetails::FileIO(e.to_string()),
                    })?);
                let mut writer = tokio::io::BufWriter::new(file);
                match self.produce_snapshot(&mut snapshot_tx, &mut writer).await {
                    Ok(_) => {
                        log::info!(
                            "replication snapshot for source {} succeeded",
                            &self.source_name
                        );
                        break;
                    }
                    Err(Severity::Recoverable(e)) => {
                        writer.flush().await.map_err(|e| SourceError {
                            source_name: self.source_name.clone(),
                            error: SourceErrorDetails::Initialization(e.to_string()),
                        })?;
                        log::warn!(
                            "replication snapshot for source {} failed, retrying: {}",
                            &self.source_name,
                            e
                        );
                        let reader = BufReader::new(writer.into_inner().into_std().await);
                        self.revert_snapshot(&mut snapshot_tx, reader)
                            .await
                            .map_err(|e| SourceError {
                                source_name: self.source_name.clone(),
                                error: SourceErrorDetails::FileIO(e.to_string()),
                            })?;
                    }
                    Err(Severity::Fatal(e)) => {
                        return Err(SourceError {
                            source_name: self.source_name,
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
                Err(Severity::Recoverable(e)) => {
                    log::warn!(
                        "replication for source {} interrupted, retrying: {}",
                        &self.source_name,
                        e
                    )
                }
                Err(Severity::Fatal(e)) => {
                    return Err(SourceError {
                        source_name: self.source_name,
                        error: SourceErrorDetails::FileIO(e.to_string()),
                    })
                }
                Ok(_) => unreachable!("replication stream cannot exit without an error"),
            }

            // TODO(petrosagg): implement exponential back-off
            tokio::time::sleep(Duration::from_secs(3)).await;
            log::info!("resuming replication for source {}", &self.source_name);
        }
    }
}
