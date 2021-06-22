// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};
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
use tokio_postgres::error::{DbError, Severity, SqlState};
use tokio_postgres::replication::LogicalReplicationStream;
use tokio_postgres::types::PgLsn;
use tokio_postgres::SimpleQueryMessage;
use tokio_postgres::{Client, SimpleQueryRow};
use uuid::Uuid;

use crate::source::{SimpleSource, SourceError, SourceTransaction, Timestamper};
use dataflow_types::{PostgresSourceConnector, SourceErrorDetails};
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
    /// Upstream relation ids that have been successfully
    /// synced to this source
    synced_relids: HashSet<u32>,
    /// Mapping of relation ids that are currently being synced
    /// to the source to the consistent LSN of their snapshots
    pending_relids: HashMap<u32, PgLsn>,
    /// Buffered snapshot rows of pending relations to be inserted
    buffered_snapshots: Option<tokio::fs::File>,
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
    pub fn new(source_name: String, connector: PostgresSourceConnector) -> Self {
        Self {
            source_name,
            connector,
            lsn: 0.into(),
            synced_relids: HashSet::new(),
            pending_relids: HashMap::new(),
            buffered_snapshots: None,
        }
    }

    /// Creates the replication slot and produces the initial snapshot of the data
    ///
    /// After the initial snapshot has been produced it returns the name of the created slot and
    /// the LSN at which we should start the replication stream at.
    async fn produce_snapshot<W: AsyncWrite + Unpin>(
        &mut self,
        buffer: &mut W,
        snapshot_tx: Option<&mut SourceTransaction<'_>>,
        lsn: Option<PgLsn>,
    ) -> Result<Option<(PgLsn, Vec<u32>)>, ReplicationError> {
        let initial_sync = snapshot_tx.is_some();

        let client =
            try_recoverable!(postgres_util::connect_replication(&self.connector.conn).await);

        // We're initialising this source so any previously existing slot must be removed and
        // re-created. Once we have data persistence we will be able to reuse slots across restarts
        let slot_name = if initial_sync {
            self.connector.slot_name.clone()
        } else {
            let mut temp_slot_name = format!(
                "{}_{}",
                &self.connector.slot_name,
                Uuid::new_v4().to_string().replace('-', "")
            );
            // Postgres slot names cannot be longer than 63 characters
            temp_slot_name.truncate(63);
            temp_slot_name
        };
        let _ = self.drop_replication_slot(&client, &slot_name).await;

        // Get all the tables for this publication and filter down to the relevant ones
        let target_tables: Vec<postgres_util::TableInfo> = try_recoverable!(
            postgres_util::publication_info(&self.connector.conn, &self.connector.publication,)
                .await
        )
        .into_iter()
        .filter(|t| !self.synced_relids.contains(&t.rel_id))
        .collect();

        // Start a transaction and immediately create a replication slot with the USE SNAPSHOT
        // directive. This makes the starting point of the slot and the snapshot of the transaction
        // identical.
        client
            .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
            .await?;

        let slot_row = self
            .create_replication_slot(&client, &slot_name, !initial_sync)
            .await?;

        // Store the lsn at which we will need to start the replication stream from
        let consistent_point = try_recoverable!(slot_row
            .get("consistent_point")
            .ok_or_else(|| anyhow!("missing expected column: `consistent_point`")));
        let consistent_lsn: PgLsn = try_fatal!(consistent_point
            .parse()
            .or_else(|_| Err(anyhow!("invalid lsn"))));
        log::info!("source {}: consistent LSN for {} is {:#?}", &self.source_name, &slot_name, &consistent_lsn);
        if initial_sync {
            self.lsn = consistent_lsn;
        } else {
            if let Some(lsn) = lsn {
                if consistent_lsn <= lsn {
                    log::info!(
                        "source {}: snapshot LSN ({:#?}) is less than synced LSN ({:#?}) for relids {:#?}, skipping snapshot",
                        &self.source_name,
                        &consistent_lsn,
                        &lsn,
                        &target_tables.iter().map(|t| t.rel_id).collect::<Vec<u32>>()
                    )
                }
                return Ok(None);
            }
        }

        for info in &target_tables {
            // TODO(petrosagg): use a COPY statement here for more efficient network transfer
            let data = client
                .simple_query(&format!(
                    "SELECT * FROM {:?}.{:?}",
                    info.namespace, info.name
                ))
                .await?;
            for msg in data {
                if let SimpleQueryMessage::Row(row) = msg {
                    let mz_row = try_fatal!(self.row_from_query_row(info.rel_id, row));
                    if let Some(snapshot_tx) = &snapshot_tx {
                        try_recoverable!(snapshot_tx.insert(mz_row.clone()).await);
                    }
                    try_fatal!(buffer.write(&try_fatal!(bincode::serialize(&mz_row))).await);
                }
            }
        }
        client.simple_query("COMMIT;").await?;
        let synced_relids: Vec<u32> = target_tables.iter().map(|t| t.rel_id).collect();
        if initial_sync {
            self.synced_relids.extend(&synced_relids);
        } else {
            self.drop_replication_slot(&client, &slot_name).await?;
        }
        Ok(Some((consistent_lsn, synced_relids)))
    }

    /// Processes a buffered snapshot file. If insert is true, it inserts the
    /// contained rows into the transaction. If false, it retracts them.
    async fn process_snapshot_file<R: Read + Seek>(
        &self,
        snapshot_tx: &mut SourceTransaction<'_>,
        mut reader: R,
        insert: bool,
    ) -> Result<(), anyhow::Error> {
        tokio::task::block_in_place(|| -> Result<(), anyhow::Error> {
            let len = reader.seek(SeekFrom::Current(0))?;
            reader.seek(SeekFrom::Start(0))?;
            let mut reader = reader.take(len);
            let handle = Handle::current();
            if insert {
                while reader.limit() > 0 {
                    let row = bincode::deserialize_from(&mut reader)?;
                    handle.block_on(snapshot_tx.insert(row))?;
                }
            } else {
                while reader.limit() > 0 {
                    let row = bincode::deserialize_from(&mut reader)?;
                    handle.block_on(snapshot_tx.delete(row))?;
                }
            }
            Ok(())
        })
    }

    async fn create_replication_slot(
        &self,
        client: &Client,
        slot_name: &str,
        temporary: bool,
    ) -> Result<SimpleQueryRow, ReplicationError> {
        let slot_query = format!(
            r#"CREATE_REPLICATION_SLOT {}{}LOGICAL "pgoutput" USE_SNAPSHOT"#,
            slot_name,
            if temporary { " TEMPORARY " } else { " " }
        );
        let row = client
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
        log::info!(
            "source {}: created Postgres replication slot {}",
            &self.source_name,
            slot_name
        );
        Ok(row)
    }

    async fn drop_replication_slot(
        &self,
        client: &Client,
        slot_name: &str,
    ) -> Result<(), ReplicationError> {
        let _ = client
            .simple_query(&format!("DROP_REPLICATION_SLOT {:?} WAIT", slot_name))
            .await?;
        log::info!(
            "source {}: dropped Postgres replication slot {}",
            &self.source_name,
            slot_name
        );
        Ok(())
    }

    /// Converts a SimpleQueryRow of a snapshot into a Row instance.
    fn row_from_query_row(
        &self,
        rel_id: u32,
        query_row: SimpleQueryRow,
    ) -> Result<Row, anyhow::Error> {
        let mut mz_row = Row::default();
        let rel_id: Datum = (rel_id as i32).into();
        mz_row.push(rel_id);
        mz_row.push_list((0..query_row.len()).map(|n| {
            let a: Datum = query_row.get(n).into();
            a
        }));
        Ok(mz_row)
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
    ) -> Result<(), ReplicationError> {
        use ReplicationError::*;

        let client =
            try_recoverable!(postgres_util::connect_replication(&self.connector.conn).await);

        let query = format!(
            r#"START_REPLICATION SLOT "{name}" LOGICAL {lsn}
              ("proto_version" '1', "publication_names" '{publication}')"#,
            name = &self.connector.slot_name,
            lsn = self.lsn.to_string(),
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
            if !self.pending_relids.is_empty() {
                match &item {
                    ReplicationMessage::XLogData(body) => {
                        let wal_start: PgLsn = body.wal_start().into();
                        let wal_end: PgLsn = body.wal_end().into();
                        log::info!("source {}: self.lsn is {:#?}, got XLogData wal_start {:#?}, wal_end {:#?}", &self.source_name, &self.lsn, &wal_start, &wal_end);
                    }
                    ReplicationMessage::PrimaryKeepAlive(body) => {
                        let wal_end: PgLsn = body.wal_end().into();
                        log::info!("source {}: self.lsn is {:#?}, got PrimaryKeepAlive wal_end {:#?}", &self.source_name, &self.lsn, &wal_end);
                    }
                    _ => unreachable!("different message"),
                }
            }
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
                    use LogicalReplicationMessage::*;

                    // If we are still catching up with a relation added to the upstream
                    // publication, skip any inserts, updates, or deletes that would have
                    // been captured in the inital snapshot of the relation.
                    if !self.pending_relids.is_empty() {
                        let snapshot_lsn = match xlog_data.data() {
                            Insert(insert) => self.pending_relids.get(&insert.rel_id()),
                            Update(update) => self.pending_relids.get(&update.rel_id()),
                            Delete(delete) => self.pending_relids.get(&delete.rel_id()),
                            _ => None,
                        };
                        if let Some(lsn) = snapshot_lsn {
                            if &PgLsn::from(xlog_data.wal_start()) < lsn {
                                continue;
                            }
                        }
                    }

                    match xlog_data.data() {
                        Begin(_) => {
                            if !inserts.is_empty() || !deletes.is_empty() {
                                return Err(Fatal(anyhow!(
                                    "got BEGIN statement after uncommitted data"
                                )));
                            }
                        }
                        Insert(insert) => {
                            let rel_id = insert.rel_id();
                            let row = try_fatal!(self.row_from_tuple(rel_id, insert.tuple()));
                            inserts.push(row);
                        }
                        Update(update) => {
                            let rel_id = update.rel_id();
                            let old_tuple = try_fatal!(update
                                .old_tuple()
                                .ok_or_else(|| anyhow!("full row missing from update")));
                            let old_row = try_fatal!(self.row_from_tuple(rel_id, old_tuple));
                            deletes.push(old_row);

                            let new_row =
                                try_fatal!(self.row_from_tuple(rel_id, update.new_tuple()));
                            inserts.push(new_row);
                        }
                        Delete(delete) => {
                            let rel_id = delete.rel_id();
                            let old_tuple = try_fatal!(delete
                                .old_tuple()
                                .ok_or_else(|| anyhow!("full row missing from delete")));
                            let row = try_fatal!(self.row_from_tuple(rel_id, old_tuple));
                            deletes.push(row);
                        }
                        Commit(commit) => {
                            let commit_lsn: PgLsn = commit.end_lsn().into();
                            self.lsn = commit_lsn;
                            let mut tx = timestamper.start_tx().await;
                            for row in deletes.drain(..) {
                                try_fatal!(tx.delete(row).await);
                            }
                            for row in inserts.drain(..) {
                                try_fatal!(tx.insert(row).await);
                            }

                            if let Some(max_pending_lsn) = self.pending_relids.values().max() {
                                if max_pending_lsn > &commit_lsn {
                                    log::info!(
                                        "source {}: added relations not synced at LSN {:#?}, waiting until LSN {:#?} to maintain consistency: {:?}",
                                        &self.source_name,
                                        &commit_lsn,
                                        &max_pending_lsn,
                                        &self.pending_relids.keys().collect::<Vec<&u32>>()
                                    );
                                } else {
                                    let file = self.buffered_snapshots.take().expect("must exist");
                                    let reader = BufReader::new(file.into_std().await);
                                    self.process_snapshot_file(&mut tx, reader, true)
                                        .await
                                        .map_err(|e| {
                                            ReplicationError::Fatal(anyhow!(e.to_string()))
                                        })?;
                                    for (relid, _) in self.pending_relids.drain() {
                                        self.synced_relids.insert(relid);
                                        log::info!(
                                            "source {}: replication snapshot for relation {} succeeded",
                                            &self.source_name,
                                            relid,
                                        );
                                    }
                                }
                            }
                        }
                        Relation(relation) => {
                            // Relation messages indicate that we're either receiving messages for the relation for
                            // the first time this session or that the relation definition has changed.
                            // Currently, we only handle new relations, not definition updates.
                            let relid = relation.rel_id();
                            if self.synced_relids.contains(&relid) {
                                log::warn!("source {}: definition for upstream relation {} may have changed, update will be ignored", &self.source_name, relid);
                                continue;
                            }

                            let prev_lsn = self.pending_relids.get(&relid).cloned();
                            loop {
                                let file = if self.buffered_snapshots.is_some() {
                                    self.buffered_snapshots.take().unwrap()
                                } else {
                                    tokio::fs::File::from_std(tempfile::tempfile().map_err(
                                        |e| ReplicationError::Fatal(anyhow!(e.to_string())),
                                    )?)
                                };
                                let prev_len = file
                                    .metadata()
                                    .await
                                    .map_err(|e| ReplicationError::Fatal(anyhow!(e.to_string())))?
                                    .len();
                                let mut writer = tokio::io::BufWriter::new(file);
                                match self.produce_snapshot(&mut writer, None, prev_lsn).await {
                                    Ok(res) => {
                                        writer.flush().await.map_err(|e| {
                                            ReplicationError::Fatal(anyhow!(e.to_string()))
                                        })?;
                                        self.buffered_snapshots = Some(writer.into_inner());
                                        if let Some((lsn, relids)) = res {
                                            for relid in relids {
                                                self.pending_relids.insert(relid, lsn);
                                            }
                                        }
                                        break;
                                    }
                                    Err(ReplicationError::Recoverable(e)) => {
                                        writer.flush().await.map_err(|e| {
                                            ReplicationError::Fatal(anyhow!(e.to_string()))
                                        })?;
                                        let file = writer.into_inner();
                                        file.set_len(prev_len).await.map_err(|e| {
                                            ReplicationError::Fatal(anyhow!(e.to_string()))
                                        })?;
                                        self.buffered_snapshots = Some(file);

                                        log::warn!(
                                            "replication snapshot for source {} relid {} failed, retrying: {}",
                                            &self.source_name,
                                            relid,
                                            e
                                        );
                                    }
                                    Err(ReplicationError::Fatal(e)) => {
                                        return Err(ReplicationError::Fatal(anyhow!(e.to_string())))
                                    }
                                }

                                // TODO(petrosagg): implement exponential back-off
                                tokio::time::sleep(Duration::from_secs(3)).await;
                            }
                        }
                        Origin(_) | Type(_) => (),
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
                        source_name: self.source_name.clone(),
                        error: SourceErrorDetails::FileIO(e.to_string()),
                    })?);
                let mut writer = tokio::io::BufWriter::new(file);
                match self
                    .produce_snapshot(&mut writer, Some(&mut snapshot_tx), None)
                    .await
                {
                    Ok(_) => {
                        log::info!(
                            "source {}: replication snapshot succeeded",
                            &self.source_name
                        );
                        break;
                    }
                    Err(ReplicationError::Recoverable(e)) => {
                        writer.flush().await.map_err(|e| SourceError {
                            source_name: self.source_name.clone(),
                            error: SourceErrorDetails::Initialization(e.to_string()),
                        })?;
                        log::warn!(
                            "source {}: replication snapshot for failed, retrying: {}",
                            &self.source_name,
                            e
                        );
                        let reader = BufReader::new(writer.into_inner().into_std().await);
                        self.process_snapshot_file(&mut snapshot_tx, reader, false)
                            .await
                            .map_err(|e| SourceError {
                                source_name: self.source_name.clone(),
                                error: SourceErrorDetails::FileIO(e.to_string()),
                            })?;
                    }
                    Err(ReplicationError::Fatal(e)) => {
                        return Err(SourceError {
                            source_name: self.source_name.clone(),
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
                    log::warn!(
                        "replication for source {} interrupted, retrying: {}",
                        &self.source_name,
                        e
                    );
                }
                Err(ReplicationError::Fatal(e)) => {
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
