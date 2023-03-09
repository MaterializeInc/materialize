// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::error::Error;
use std::future;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail};
use futures::{FutureExt, Stream, StreamExt};
use once_cell::sync::Lazy;
use postgres_protocol::message::backend::{
    LogicalReplicationMessage, ReplicationMessage, TupleData,
};
use timely::dataflow::operators::to_stream::Event;
use timely::dataflow::operators::Capability;
use timely::progress::Antichain;
use timely::scheduling::SyncActivator;
use tokio::runtime::Handle as TokioHandle;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_postgres::error::DbError;
use tokio_postgres::replication::LogicalReplicationStream;
use tokio_postgres::types::PgLsn;
use tokio_postgres::Client;
use tokio_postgres::SimpleQueryMessage;
use tracing::{info, warn};

use mz_expr::MirScalarExpr;
use mz_ore::display::DisplayExt;
use mz_ore::task;
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
use crate::source::types::{HealthStatusUpdate, OffsetCommitter, SourceConnectionBuilder};
use crate::source::{
    NextMessage, SourceMessage, SourceMessageType, SourceReader, SourceReaderError,
};

mod metrics;

/// Postgres epoch is 2000-01-01T00:00:00Z
static PG_EPOCH: Lazy<SystemTime> = Lazy::new(|| UNIX_EPOCH + Duration::from_secs(946_684_800));

/// How often a status update message should be sent to the server
static FEEDBACK_INTERVAL: Duration = Duration::from_secs(30);

/// The amount of time we should wait after the last received message before worrying about WAL lag
static WAL_LAG_GRACE_PERIOD: Duration = Duration::from_secs(30);

trait ErrorExt {
    fn is_definite(&self) -> bool;
}

impl ErrorExt for tokio::time::error::Elapsed {
    fn is_definite(&self) -> bool {
        false
    }
}

impl ErrorExt for DbError {
    fn is_definite(&self) -> bool {
        let class = match self.code().code().get(0..2) {
            None => return false,
            Some(class) => class,
        };
        // See https://www.postgresql.org/docs/current/errcodes-appendix.html for the class
        // definitions.
        match class {
            // unknown catalog or schema names
            "3D" | "3F" => true,
            // syntax error or access rule violation
            "42" => true,
            _ => false,
        }
    }
}

impl ErrorExt for tokio_postgres::Error {
    fn is_definite(&self) -> bool {
        match self.source() {
            Some(err) => match err.downcast_ref::<DbError>() {
                Some(db_err) => db_err.is_definite(),
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

impl ErrorExt for std::io::Error {
    fn is_definite(&self) -> bool {
        match self.source() {
            Some(err) => match err.downcast_ref::<tokio_postgres::Error>() {
                Some(tokio_err) => tokio_err.is_definite(),
                None => match err.downcast_ref::<DbError>() {
                    Some(db_err) => db_err.is_definite(),
                    None => false,
                },
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

#[derive(Debug)]
enum ReplicationError {
    /// This error is definite: this source is permanently wedged.
    /// Returning a definite error will cause the collection to become un-queryable.
    Definite(anyhow::Error),
    /// This error may or may not resolve itself in the future, and
    /// should be retried instead of being added to the output.
    Indefinite(anyhow::Error),
    /// When this error happens we must halt
    Irrecoverable(anyhow::Error),
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

trait ResultExt<T, E> {
    fn err_definite(self) -> Result<T, ReplicationError>;
    fn err_indefinite(self) -> Result<T, ReplicationError>;
    fn err_irrecoverable(self) -> Result<T, ReplicationError>;
}

impl<T, E: Into<anyhow::Error>> ResultExt<T, E> for Result<T, E> {
    fn err_definite(self) -> Result<T, ReplicationError> {
        match self {
            Ok(val) => Ok(val),
            Err(err) => Err(ReplicationError::Definite(err.into())),
        }
    }
    fn err_indefinite(self) -> Result<T, ReplicationError> {
        match self {
            Ok(val) => Ok(val),
            Err(err) => Err(ReplicationError::Indefinite(err.into())),
        }
    }
    fn err_irrecoverable(self) -> Result<T, ReplicationError> {
        match self {
            Ok(val) => Ok(val),
            Err(err) => Err(ReplicationError::Irrecoverable(err.into())),
        }
    }
}

// Message used to communicate between `get_next_message` and the tokio task
enum InternalMessage {
    Err(SourceReaderError),
    Status(HealthStatusUpdate),
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

    /// The lsn we last emitted data at. Used to fabricate timestamps for errors. This should
    /// ideally go away and only emit errors that we can associate with source timestamps
    last_lsn: PgLsn,

    /// Capabilities used to produce messages
    data_capability: Capability<MzOffset>,
    upper_capability: Capability<MzOffset>,
}

/// An OffsetCommitter for postgres, that sends
/// the offsets (lsns) to the replication stream
/// through a channel
pub struct PgOffsetCommitter {
    logger: LogCommitter,
    resume_lsn: Arc<AtomicU64>,
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
    replication_lsn: PgLsn,
    metrics: PgSourceMetrics,
    /// A map of the table oid to its information
    source_tables: BTreeMap<u32, SourceTable>,
    row_sender: RowSender,
    sender: Sender<InternalMessage>,
    resume_lsn: Arc<AtomicU64>,
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
        mut data_capability: Capability<MzOffset>,
        mut upper_capability: Capability<MzOffset>,
        resume_upper: Antichain<MzOffset>,
        _encoding: SourceDataEncoding,
        metrics: SourceBaseMetrics,
        connection_context: ConnectionContext,
    ) -> Result<(Self::Reader, Self::OffsetCommitter), anyhow::Error> {
        let active_read_worker =
            crate::source::responsible_for(&source_id, worker_id, worker_count, ());

        // TODO: figure out the best default here; currently this is optimized
        // for the speed to pass pg-cdc-resumption tests on a local machine.
        let (dataflow_tx, dataflow_rx) = tokio::sync::mpsc::channel(50_000);

        // TODO(petrosagg): handle the empty frontier correctly. Currenty the framework code never
        // constructs a reader when the resumption frontier is the empty antichain
        let start_offset = resume_upper.into_option().unwrap();
        data_capability.downgrade(&start_offset);
        upper_capability.downgrade(&start_offset);

        let resume_lsn = Arc::new(AtomicU64::new(start_offset.offset));

        let connection_config = TokioHandle::current()
            .block_on(self.connection.config(&*connection_context.secrets_reader))
            .expect("Postgres connection unexpectedly missing secrets");

        if active_read_worker {
            let mut source_tables = BTreeMap::new();
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
                replication_lsn: start_offset.offset.into(),
                metrics: PgSourceMetrics::new(&metrics, source_id),
                source_tables,
                row_sender: RowSender::new(dataflow_tx.clone(), consumer_activator),
                sender: dataflow_tx,
                resume_lsn: Arc::clone(&resume_lsn),
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
                last_lsn: start_offset.offset.into(),
                data_capability,
                upper_capability,
            },
            PgOffsetCommitter {
                logger: LogCommitter {
                    source_id,
                    worker_id,
                    worker_count,
                },
                resume_lsn,
            },
        ))
    }
}

impl SourceReader for PostgresSourceReader {
    type Key = ();
    type Value = Row;
    type Time = MzOffset;
    type Diff = Diff;

    // TODO(guswynn): use `next` instead of using a channel
    fn get_next_message(&mut self) -> NextMessage<Self::Key, Self::Value, Self::Time, Self::Diff> {
        if !self.active_read_worker {
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
                let msg = SourceMessage {
                    output,
                    upstream_time_millis: None,
                    key: (),
                    value,
                    headers: None,
                };

                let ts = lsn.into();
                let cap = self.data_capability.delayed(&ts);
                let next_ts = ts + 1;
                self.upper_capability.downgrade(&next_ts);
                if end {
                    self.data_capability.downgrade(&next_ts);
                }
                NextMessage::Ready(SourceMessageType::Message(Ok(msg), cap, diff))
            }
            Some(Some(InternalMessage::Status(update))) => {
                NextMessage::Ready(SourceMessageType::SourceStatus(update))
            }
            Some(Some(InternalMessage::Err(err))) => {
                // XXX(petrosagg): we are fabricating a timestamp here!!
                let non_definite_ts = MzOffset::from(self.last_lsn) + 1;

                let cap = self.data_capability.delayed(&non_definite_ts);
                let next_ts = non_definite_ts + 1;
                self.data_capability.downgrade(&next_ts);
                self.upper_capability.downgrade(&next_ts);
                NextMessage::Ready(SourceMessageType::Message(Err(err), cap, 1))
            }
            None => NextMessage::Pending,
            Some(None) => NextMessage::Finished,
        };

        ret
    }
}

#[async_trait::async_trait]
impl OffsetCommitter<MzOffset> for PgOffsetCommitter {
    async fn commit_offsets(&self, frontier: Antichain<MzOffset>) -> Result<(), anyhow::Error> {
        if let Some(offset) = frontier.as_option() {
            // TODO(petrosagg): this minus one is very suspicious. It is replicating the previous
            // behaviour where the commit offset was calculated by calling
            // OffsetAntichain::as_data_offsets, which subtracted one. Investigate if it's truly
            // needed
            self.resume_lsn
                .store(offset.offset.saturating_sub(1), Ordering::SeqCst);
        }
        self.logger.commit_offsets(frontier).await?;

        Ok(())
    }
}

/// Defers to `postgres_replication_loop_inner` and sends errors through the channel if they occur
#[allow(clippy::or_fun_call)]
async fn postgres_replication_loop(mut task_info: PostgresTaskInfo) {
    loop {
        match postgres_replication_loop_inner(&mut task_info).await {
            Ok(()) => {}
            Err(ReplicationError::Indefinite(e)) => {
                warn!(
                    "replication for source {} interrupted, retrying: {e}",
                    task_info.source_id
                );
                // If the channel is shutting down, so is the source.
                let _ = task_info
                    .sender
                    .send(InternalMessage::Status(HealthStatusUpdate {
                        update: HealthStatus::StalledWithError {
                            error: e.to_string_alt(),
                            hint: None,
                        },
                        should_halt: false,
                    }))
                    .await;
            }
            Err(ReplicationError::Irrecoverable(e)) => {
                warn!(
                    "irrecoverable error for source {}: {}, cause: {}",
                    &task_info.source_id,
                    e,
                    e.source().unwrap_or(anyhow::anyhow!("unknown").as_ref())
                );
                // If the channel is shutting down, so is the source.
                let _ = task_info
                    .sender
                    .send(InternalMessage::Status(HealthStatusUpdate {
                        update: HealthStatus::StalledWithError {
                            error: e.to_string_alt(),
                            hint: None,
                        },
                        // TODO: In the future we probably want to handle this more gracefully,
                        // but for now halting is the easiest way to dump the data in the pipe.
                        // The restarted clusterd instance will restart the snapshot fresh, which will
                        // avoid any inconsistencies. Note that if the same lsn is chosen in the
                        // next snapshotting, the remapped timestamp chosen will be the same for
                        // both instances of clusterd.
                        should_halt: true,
                    }))
                    .await;

                future::pending().await
            }
            Err(ReplicationError::Definite(e)) => {
                warn!(
                    "definite error for source {}: {}, cause: {}",
                    &task_info.source_id,
                    e,
                    e.source().unwrap_or(anyhow::anyhow!("unknown").as_ref())
                );
                // Drop the send error, as we have no way of communicating back to the
                // source operator if the channel is gone.
                let _ = task_info
                    .row_sender
                    .sender
                    .send(InternalMessage::Err(SourceReaderError {
                        inner: SourceErrorDetails::Initialization(e.to_string()),
                    }))
                    .await;
                task_info
                    .row_sender
                    .activator
                    .activate()
                    .expect("postgres reader activation failed");
                return;
            }
        }
        // TODO(petrosagg): implement exponential back-off
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

/// Core logic
async fn postgres_replication_loop_inner(
    task_info: &mut PostgresTaskInfo,
) -> Result<(), ReplicationError> {
    if task_info.replication_lsn == PgLsn::from(0) {
        // Get all the relevant tables for this publication
        let publication_tables = mz_postgres_util::publication_info(
            &task_info.connection_config,
            &task_info.publication,
        )
        .await
        .err_indefinite()?;

        // Validate publication tables against the state snapshot
        validate_tables(&task_info.source_tables, publication_tables).err_definite()?;

        let client = task_info
            .connection_config
            .clone()
            .connect_replication()
            .await
            .err_indefinite()?;

        // Technically there is TOCTOU problem here but it makes the code easier and if we end
        // up attempting to create a slot and it already exists we will simply retry
        // Also, we must check if the slot exists before we start a transaction because creating a
        // slot must be the first statement in a transaction
        let res = client
            .simple_query(&format!(
                r#"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '{}'"#,
                task_info.slot
            ))
            .await?;
        let slot_lsn = parse_single_row(&res, "confirmed_flush_lsn");
        client
            .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
            .await?;

        let (slot_lsn, snapshot_lsn, temp_slot) = match slot_lsn {
            Ok(slot_lsn) => {
                // The main slot already exists which means we can't use it for the snapshot. So
                // we'll create a temporary replication slot in order to both set the transaction's
                // snapshot to be a consistent point and also to find out the LSN that the snapshot
                // is going to run at.
                //
                // When this happens we'll most likely be snapshotting at a later LSN than the slot
                // which we will take care below by rewinding.
                let temp_slot = uuid::Uuid::new_v4().to_string().replace('-', "");
                let res = client
                    .simple_query(&format!(
                        r#"CREATE_REPLICATION_SLOT {:?} TEMPORARY LOGICAL "pgoutput" USE_SNAPSHOT"#,
                        temp_slot
                    ))
                    .await?;
                let snapshot_lsn = parse_single_row(&res, "consistent_point")?;
                (slot_lsn, snapshot_lsn, Some(temp_slot))
            }
            Err(_) => {
                let res = client
                    .simple_query(&format!(
                        r#"CREATE_REPLICATION_SLOT {:?} LOGICAL "pgoutput" USE_SNAPSHOT"#,
                        task_info.slot
                    ))
                    .await?;
                let slot_lsn = parse_single_row(&res, "consistent_point")?;
                (slot_lsn, slot_lsn, None)
            }
        };

        let mut stream = Box::pin(
            produce_snapshot(&client, &task_info.metrics, &task_info.source_tables).enumerate(),
        );

        while let Some((i, event)) = stream.as_mut().next().await {
            if i > 0 {
                // Failure scenario after we have produced at least one row, but before a
                // successful `COMMIT`
                fail::fail_point!("pg_snapshot_failure", |_| {
                    Err(ReplicationError::Indefinite(anyhow::anyhow!(
                        "recoverable errors should crash the process"
                    )))
                });
            }
            let (output, row) = match event {
                Ok(event) => event,
                Err(err @ ReplicationError::Definite(_)) => return Err(err),
                Err(ReplicationError::Indefinite(err) | ReplicationError::Irrecoverable(err)) => {
                    return Err(ReplicationError::Irrecoverable(err))
                }
            };
            task_info
                .row_sender
                .send_row(output, row, slot_lsn, 1)
                .await;
        }

        if let Some(temp_slot) = temp_slot {
            let _ = client
                .simple_query(&format!("DROP_REPLICATION_SLOT {temp_slot:?}"))
                .await;
        }
        client.simple_query("COMMIT;").await?;

        // Drop the stream and the client, to ensure that the future `produce_replication` don't
        // conflict with the above processing.
        //
        // Its possible we can avoid dropping the `client` value here, but we do it out of an
        // abundance of caution, as rust-postgres has had curious bugs around this.
        drop(stream);
        drop(client);

        assert!(slot_lsn <= snapshot_lsn);
        if slot_lsn < snapshot_lsn {
            tracing::info!("postgres snapshot was at {snapshot_lsn:?} but we need it at {slot_lsn:?}. Rewinding");
            // Our snapshot was too far ahead so we must rewind it by reading the replication
            // stream until the snapshot lsn and emitting any rows that we find with negated diffs
            let replication_stream = produce_replication(
                task_info.connection_config.clone(),
                &task_info.slot,
                &task_info.publication,
                slot_lsn,
                Arc::clone(&task_info.resume_lsn),
                &task_info.metrics,
                &task_info.source_tables,
            )
            .await;
            tokio::pin!(replication_stream);

            while let Some(event) = replication_stream.next().await {
                match event {
                    Ok(Event::Message(lsn, (output, row, diff))) => {
                        // Here we ignore the lsn that this row actually happened at and we
                        // forcefully emit it at the slot_lsn with a negated diff.
                        if lsn <= snapshot_lsn {
                            task_info
                                .row_sender
                                .send_row(output, row, slot_lsn, -diff)
                                .await;
                        }
                    }
                    Ok(Event::Progress([lsn])) => {
                        if lsn > snapshot_lsn {
                            // We successfully rewinded the snapshot from snapshot_lsn to slot_lsn
                            task_info.row_sender.close_lsn(slot_lsn).await;
                            break;
                        }
                    }
                    Err(err @ ReplicationError::Definite(_)) => return Err(err),
                    Err(
                        ReplicationError::Indefinite(err) | ReplicationError::Irrecoverable(err),
                    ) => return Err(ReplicationError::Irrecoverable(err)),
                }
            }
        }
        task_info.metrics.lsn.set(slot_lsn.into());
        task_info.row_sender.close_lsn(slot_lsn).await;

        info!(
            "replication snapshot for source {} succeeded",
            &task_info.source_id
        );
        task_info.replication_lsn = slot_lsn;
    }

    let replication_stream = produce_replication(
        task_info.connection_config.clone(),
        &task_info.slot,
        &task_info.publication,
        task_info.replication_lsn,
        Arc::clone(&task_info.resume_lsn),
        &task_info.metrics,
        &task_info.source_tables,
    )
    .await;
    tokio::pin!(replication_stream);

    // TODO(petrosagg): The API does not guarantee that we won't see an error after we have already
    // partially emitted a transaction, but we know it is the case due to the implementation. Find
    // a way to encode this in the type signature
    while let Some(event) = replication_stream.next().await.transpose()? {
        match event {
            Event::Message(lsn, (output, row, diff)) => {
                task_info.row_sender.send_row(output, row, lsn, diff).await;
            }
            Event::Progress([lsn]) => {
                // The lsn passed to `START_REPLICATION_SLOT` produces all transactions that
                // committed at LSNs *strictly after*, but upper frontiers have "greater than
                // or equal" semantics, so we must subtract one from the upper to make it
                // compatible with what `START_REPLICATION_SLOT` expects.
                task_info.replication_lsn = PgLsn::from(u64::from(lsn) - 1);
                task_info.row_sender.close_lsn(lsn).await;
            }
        }
    }

    Ok(())
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

    /// Send a triplet for the specific output
    pub async fn send_row(&mut self, output_index: usize, row: Row, lsn: PgLsn, diff: Diff) {
        if let Some(buffered) = self.buffered_message.take() {
            assert_eq!(buffered.lsn, lsn);
            self.send_row_inner(
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
            diff,
        });
    }

    /// Finalize an lsn, making sure all messages that my be buffered are flushed, and that the
    /// last message sent is marked as closing the `lsn` (which is the messages `offset` in the
    /// rest of the source pipeline.
    pub async fn close_lsn(&mut self, lsn: PgLsn) {
        if let Some(buffered) = self.buffered_message.take() {
            assert!(buffered.lsn <= lsn);
            self.send_row_inner(
                buffered.output_index,
                buffered.row,
                buffered.lsn,
                buffered.diff,
                true,
            )
            .await;
        }
    }

    async fn send_row_inner(&self, output: usize, row: Row, lsn: PgLsn, diff: i64, end: bool) {
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

/// Validates that all expected tables exist in the publication tables and they have the same schema
fn validate_tables(
    source_tables: &BTreeMap<u32, SourceTable>,
    tables: Vec<PostgresTableDesc>,
) -> Result<(), anyhow::Error> {
    let pub_tables: BTreeMap<u32, PostgresTableDesc> =
        tables.into_iter().map(|t| (t.oid, t)).collect();
    for (id, info) in source_tables.iter() {
        match pub_tables.get(id) {
            Some(pub_schema) => {
                if pub_schema != &info.desc {
                    warn!(
                        "Error validating table in publication. Expected: {:?} Actual: {:?}",
                        &info.desc, pub_schema
                    );
                    bail!(
                        "source table {} with oid {} has been altered",
                        info.desc.name,
                        info.desc.oid
                    )
                }
            }
            None => {
                warn!(
                    "publication missing table: {} with id {}",
                    info.desc.name, id
                );
                bail!(
                    "source table {} with oid {} has been dropped",
                    info.desc.name,
                    info.desc.oid
                )
            }
        }
    }
    Ok(())
}

/// Parses SQL results that are expected to be a single row into a Rust type
fn parse_single_row<T: FromStr>(
    result: &[SimpleQueryMessage],
    column: &str,
) -> Result<T, ReplicationError> {
    let mut rows = result.into_iter().filter_map(|msg| match msg {
        SimpleQueryMessage::Row(row) => Some(row),
        _ => None,
    });
    match (rows.next(), rows.next()) {
        (Some(row), None) => row
            .get(column)
            .ok_or_else(|| anyhow!("missing expected column: {column}"))
            .and_then(|col| col.parse().or_else(|_| Err(anyhow!("invalid data"))))
            .err_indefinite(),
        (None, None) => Err(anyhow!("empty result")).err_indefinite(),
        _ => Err(anyhow!("ambiguous result, more than one row")).err_indefinite(),
    }
}

/// Produces the initial snapshot of the data by performing a `COPY` query for each of the provided
/// `source_tables`.
///
/// The return stream of data returned is not annotated with LSN numbers. It is up to the caller to
/// provide a client that is in a known LSN context in which the snapshot will be taken. For
/// example by calling this method while being in a transaction for which the LSN is known.
fn produce_snapshot<'a>(
    client: &'a Client,
    metrics: &'a PgSourceMetrics,
    source_tables: &'a BTreeMap<u32, SourceTable>,
) -> impl Stream<Item = Result<(usize, Row), ReplicationError>> + 'a {
    async_stream::try_stream! {
        // Scratch space to use while evaluating casts
        let mut datum_vec = DatumVec::new();

        for info in source_tables.values() {
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

                let mut raw_values = parser.iter_raw(info.desc.columns.len());
                while let Some(raw_value) = raw_values.next() {
                    match raw_value.err_definite()? {
                        Some(value) => {
                            packer.push(Datum::String(std::str::from_utf8(value).err_definite()?))
                        }
                        None => packer.push(Datum::Null),
                    }
                }

                let mut datums = datum_vec.borrow();
                datums.extend(text_row.iter());

                let row = cast_row(&info.casts, &datums).err_definite()?;

                yield (info.output_index, row);
            }

            metrics.tables.inc();
        }
    }
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

// TODO(guswynn|petrosagg): fix the underlying bug that prevents client re-use
// when exiting the CopyBoth mode, so we don't need to re-create clients in every loop
// in this function.
async fn produce_replication<'a>(
    client_config: mz_postgres_util::Config,
    slot: &'a str,
    publication: &'a str,
    as_of: PgLsn,
    committed_lsn: Arc<AtomicU64>,
    metrics: &'a PgSourceMetrics,
    source_tables: &'a BTreeMap<u32, SourceTable>,
) -> impl Stream<Item = Result<Event<[PgLsn; 1], (usize, Row, Diff)>, ReplicationError>> + 'a {
    use ReplicationError::*;
    use ReplicationMessage::*;
    async_stream::try_stream!({
        //let mut last_data_message = Instant::now();
        let mut inserts = vec![];
        let mut deletes = vec![];

        let mut last_feedback = Instant::now();

        // Scratch space to use while evaluating casts
        let mut datum_vec = DatumVec::new();

        let mut last_commit_lsn = as_of;
        let mut observed_wal_end = as_of;
        // The outer loop alternates the client between streaming the replication slot and using
        // normal SQL queries with pg admin functions to fast-foward our cursor in the event of WAL
        // lag.
        //
        // TODO(petrosagg): we need to do the above because a replication slot can be active only
        // one place which is why we need to do this dance of entering and exiting replication mode
        // in order to be able to use the administrative functions below. Perhaps it's worth
        // creating two independent slots so that we can use the secondary to check without
        // interrupting the stream on the first one
        loop {
            let client = client_config
                .clone()
                .connect_replication()
                .await
                .err_indefinite()?;
            tracing::trace!("starting replication slot");
            let query = format!(
                r#"START_REPLICATION SLOT "{name}" LOGICAL {lsn}
                  ("proto_version" '1', "publication_names" '{publication}')"#,
                name = &slot,
                lsn = last_commit_lsn,
                publication = publication
            );
            let copy_stream = client.copy_both_simple(&query).await.err_indefinite()?;
            let mut stream = Box::pin(LogicalReplicationStream::new(copy_stream));

            let mut last_data_message = Instant::now();

            // The inner loop
            loop {
                // The upstream will periodically request status updates by setting the keepalive's
                // reply field to 1. However, we cannot rely on these messages arriving on time. For
                // example, when the upstream is sending a big transaction its keepalive messages are
                // queued and can be delayed arbitrarily. Therefore, we also make sure to
                // send a proactive status update every 30 seconds There is an implicit requirement
                // that a new resumption frontier is converted into an lsn relatively soon after
                // startup.
                //
                // See: https://www.postgresql.org/message-id/CAMsr+YE2dSfHVr7iEv1GSPZihitWX-PMkD9QALEGcTYa+sdsgg@mail.gmail.com
                let mut needs_status_update = last_feedback.elapsed() > FEEDBACK_INTERVAL;

                metrics.total.inc();
                use LogicalReplicationMessage::*;
                match stream.as_mut().next().await {
                    Some(Ok(XLogData(xlog_data))) => match xlog_data.data() {
                        Begin(_) => {
                            last_data_message = Instant::now();
                            if !inserts.is_empty() || !deletes.is_empty() {
                                return Err(Definite(anyhow!(
                                    "got BEGIN statement after uncommitted data"
                                )))?;
                            }
                        }
                        Insert(insert) if source_tables.contains_key(&insert.rel_id()) => {
                            last_data_message = Instant::now();
                            metrics.inserts.inc();
                            let rel_id = insert.rel_id();
                            let info = source_tables.get(&rel_id).unwrap();
                            let new_tuple = insert.tuple().tuple_data();
                            let mut datums = datum_vec.borrow();
                            datums_from_tuple(rel_id, new_tuple, &mut *datums).err_definite()?;
                            let row = cast_row(&info.casts, &datums).err_definite()?;
                            inserts.push((info.output_index, row));
                        }
                        Update(update) if source_tables.contains_key(&update.rel_id()) => {
                            last_data_message = Instant::now();
                            metrics.updates.inc();
                            let rel_id = update.rel_id();
                            let info = source_tables.get(&rel_id).unwrap();
                            let err = || {
                                anyhow!(
                                    "Old row missing from replication stream for table with OID = {}.
                                     Did you forget to set REPLICA IDENTITY to FULL for your table?",
                                    rel_id
                                )
                            };
                            let old_tuple = update
                                .old_tuple()
                                .ok_or_else(err)
                                .err_definite()?
                                .tuple_data();

                            let mut old_datums = datum_vec.borrow();
                            datums_from_tuple(rel_id, old_tuple, &mut *old_datums)
                                .err_definite()?;
                            let old_row = cast_row(&info.casts, &old_datums).err_definite()?;
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
                            datums_from_tuple(rel_id, new_tuple, &mut *new_datums)
                                .err_definite()?;
                            let new_row = cast_row(&info.casts, &new_datums).err_definite()?;
                            inserts.push((info.output_index, new_row));
                        }
                        Delete(delete) if source_tables.contains_key(&delete.rel_id()) => {
                            last_data_message = Instant::now();
                            metrics.deletes.inc();
                            let rel_id = delete.rel_id();
                            let info = source_tables.get(&rel_id).unwrap();
                            let err = || {
                                anyhow!(
                                    "Old row missing from replication stream for table with OID = {}.
                                     Did you forget to set REPLICA IDENTITY to FULL for your table?",
                                    rel_id
                                )
                            };
                            let old_tuple = delete
                                .old_tuple()
                                .ok_or_else(err)
                                .err_definite()?
                                .tuple_data();
                            let mut datums = datum_vec.borrow();
                            datums_from_tuple(rel_id, old_tuple, &mut *datums).err_definite()?;
                            let row = cast_row(&info.casts, &datums).err_definite()?;
                            deletes.push((info.output_index, row));
                        }
                        Commit(commit) => {
                            last_data_message = Instant::now();
                            metrics.transactions.inc();
                            last_commit_lsn = PgLsn::from(commit.end_lsn());

                            for (output, row) in deletes.drain(..) {
                                yield Event::Message(last_commit_lsn, (output, row, -1));
                            }
                            for (output, row) in inserts.drain(..) {
                                yield Event::Message(last_commit_lsn, (output, row, 1));
                            }
                            yield Event::Progress([PgLsn::from(u64::from(last_commit_lsn) + 1)]);
                            metrics.lsn.set(last_commit_lsn.into());
                        }
                        Relation(relation) => {
                            last_data_message = Instant::now();
                            let rel_id = relation.rel_id();
                            if let Some(info) = source_tables.get(&rel_id) {
                                // Start with the cheapest check first, this will catch the majority of alters
                                if info.desc.columns.len() != relation.columns().len() {
                                    warn!(
                                        "alter table detected on {} with id {}",
                                        info.desc.name, info.desc.oid
                                    );
                                    return Err(Definite(anyhow!(
                                        "source table {} with oid {} has been altered",
                                        info.desc.name,
                                        info.desc.oid
                                    )))?;
                                }
                                let same_name = info.desc.name == relation.name().unwrap();
                                let same_namespace =
                                    info.desc.namespace == relation.namespace().unwrap();
                                if !same_name || !same_namespace {
                                    warn!(
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
                                    )))?;
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
                                        warn!(
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
                                        )))?;
                                    }
                                }
                            }
                        }
                        Insert(_) | Update(_) | Delete(_) | Origin(_) | Type(_) => {
                            last_data_message = Instant::now();
                            metrics.ignored.inc();
                        }
                        Truncate(truncate) => {
                            let tables = truncate
                                .rel_ids()
                                .iter()
                                // Filter here makes option handling in map "safe"
                                .filter_map(|id| source_tables.get(id))
                                .map(|info| {
                                    format!("name: {} id: {}", info.desc.name, info.desc.oid)
                                })
                                .collect::<Vec<String>>();
                            return Err(Definite(anyhow!(
                                "source table(s) {} got truncated",
                                tables.join(", ")
                            )))?;
                        }
                        // The enum is marked as non_exhaustive. Better to be conservative here in
                        // case a new message is relevant to the semantics of our source
                        _ => {
                            return Err(Definite(anyhow!(
                                "unexpected logical replication message"
                            )))?;
                        }
                    },
                    Some(Ok(PrimaryKeepAlive(keepalive))) => {
                        needs_status_update = needs_status_update || keepalive.reply() == 1;
                        observed_wal_end = PgLsn::from(keepalive.wal_end());

                        if last_data_message.elapsed() > WAL_LAG_GRACE_PERIOD {
                            break;
                        }
                    }
                    Some(Err(err)) => {
                        return Err(ReplicationError::from(err))?;
                    }
                    None => {
                        break;
                    }
                    // The enum is marked non_exhaustive, better be conservative
                    _ => {
                        return Err(Definite(anyhow!("Unexpected replication message")))?;
                    }
                }
                if needs_status_update {
                    let ts: i64 = PG_EPOCH
                        .elapsed()
                        .expect("system clock set earlier than year 2000!")
                        .as_micros()
                        .try_into()
                        .expect("software more than 200k years old, consider updating");

                    let committed_lsn = PgLsn::from(committed_lsn.load(Ordering::SeqCst));
                    let standby_res = stream
                        .as_mut()
                        .standby_status_update(committed_lsn, committed_lsn, committed_lsn, ts, 0)
                        .await;
                    if let Err(err) = standby_res {
                        return Err(Indefinite(err.into()))?;
                    }
                    last_feedback = Instant::now();
                }
            }
            // This may not be required, but as mentioned above in
            // `postgres_replication_loop_inner`, we drop clients aggressively out of caution.
            drop(stream);

            let client = client_config
                .clone()
                .connect_replication()
                .await
                .err_indefinite()?;

            // We reach this place if the consume loop above detected large WAL lag. This
            // section determines whether or not we can skip over that part of the WAL by
            // peeking into the replication slot using a normal SQL query and the
            // `pg_logical_slot_peek_binary_changes` administrative function.
            //
            // By doing so we can get a positive statement about existence or absence of
            // relevant data from the current LSN to the observed WAL end. If there are no
            // messages then it is safe to fast forward last_commit_lsn to the WAL end LSN and restart
            // the replication stream from there.
            let query = format!(
                "SELECT lsn FROM pg_logical_slot_peek_binary_changes(
                     '{name}', NULL, NULL,
                     'proto_version', '1',
                     'publication_names', '{publication}'
                )",
                name = &slot,
                publication = publication
            );

            let peek_binary_start_time = Instant::now();
            let rows = client.simple_query(&query).await.err_indefinite()?;

            let changes = rows
                .into_iter()
                .filter(|row| match row {
                    SimpleQueryMessage::Row(row) => {
                        let change_lsn: PgLsn = row
                            .get("lsn")
                            .expect("missing expected column: `lsn`")
                            .parse()
                            .expect("invalid lsn");
                        // Keep all the changes that may exist after our last observed transaction
                        // commit
                        change_lsn > last_commit_lsn
                    }
                    SimpleQueryMessage::CommandComplete(_) => false,
                    _ => panic!("unexpected enum variant"),
                })
                .count();

            // If there are no changes until the end of the WAL it's safe to fast forward
            if changes == 0 {
                last_commit_lsn = observed_wal_end;
                // `Progress` events are _frontiers_, so we add 1, just like when we
                // handle data in `Commit` above.
                yield Event::Progress([PgLsn::from(u64::from(last_commit_lsn) + 1)]);
            }

            tracing::info!(
                slot = ?slot,
                query_time = ?peek_binary_start_time.elapsed(),
                current_lsn = ?last_commit_lsn,
                "Found {} changes in the wal.",
                changes
            );
        }
    })
}
