// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::error::Error;
use std::future;
use std::rc::Rc;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail};
use differential_dataflow::{AsCollection, Collection};
use futures::{StreamExt, TryStreamExt};
use once_cell::sync::Lazy;
use postgres_protocol::message::backend::{
    LogicalReplicationMessage, ReplicationMessage, TupleData,
};
use timely::dataflow::operators::to_stream::Event;
use timely::dataflow::operators::Capability;
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_postgres::error::DbError;
use tokio_postgres::replication::LogicalReplicationStream;
use tokio_postgres::types::PgLsn;
use tokio_postgres::Client;
use tokio_postgres::SimpleQueryMessage;
use tracing::{info, warn};

use mz_expr::MirScalarExpr;
use mz_ore::error::ErrorExt;
use mz_ore::future::TimeoutError;
use mz_ore::task;
use mz_postgres_util::desc::PostgresTableDesc;
use mz_repr::{Datum, DatumVec, Diff, GlobalId, Row};
use mz_storage_client::types::connections::ConnectionContext;
use mz_storage_client::types::errors::SourceErrorDetails;
use mz_storage_client::types::sources::{MzOffset, PostgresSourceConnection, SourceTimestamp};
use mz_timely_util::antichain::AntichainExt;
use mz_timely_util::builder_async::OperatorBuilder as AsyncOperatorBuilder;

use self::metrics::PgSourceMetrics;

use crate::source::types::{HealthStatus, HealthStatusUpdate, SourceReaderMetrics, SourceRender};
use crate::source::{RawSourceCreationConfig, SourceMessage, SourceReaderError};

mod metrics;

/// Postgres epoch is 2000-01-01T00:00:00Z
static PG_EPOCH: Lazy<SystemTime> = Lazy::new(|| UNIX_EPOCH + Duration::from_secs(946_684_800));

/// How often a status update message should be sent to the server
static FEEDBACK_INTERVAL: Duration = Duration::from_secs(30);

/// The amount of time we should wait after the last received message before worrying about WAL lag
static WAL_LAG_GRACE_PERIOD: Duration = Duration::from_secs(30);

trait PgErrorExt {
    fn is_definite(&self) -> bool;
}

impl PgErrorExt for tokio::time::error::Elapsed {
    fn is_definite(&self) -> bool {
        false
    }
}

impl PgErrorExt for DbError {
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

impl PgErrorExt for tokio_postgres::Error {
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

impl PgErrorExt for std::io::Error {
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

impl<E: PgErrorExt + Into<anyhow::Error>> From<E> for ReplicationError {
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

/// Message used to communicate between `get_next_message` and the tokio task
enum InternalMessage {
    /// A definite error for the source, i.e. not a subsource error.
    Err(SourceReaderError),
    Status(HealthStatusUpdate),
    /// A value meant for a subsource.
    Value {
        lsn: PgLsn,
        /// Errors sent here are meant to express a subsource error that occurred in the process of
        /// decoding values, i.e. it is in lieu of a value. This contrasts with
        /// `InternalEssage::Err`, which signals an error in the source itself, e.g. a programming
        /// error.
        value: Result<(Row, Diff), anyhow::Error>,
        end: bool,
    },
}

/// Information required to sync data from Postgres
pub struct PostgresSourceReader {
    receiver_stream: Receiver<(usize, InternalMessage)>,

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
    /// A map of the table oid to its information.
    ///
    /// Note that we populate this information with state from the catalog, but only remove it if we
    /// encounter errors during execution. This means it is possible for items to be removed during
    /// one execution cycle, only to have them re-appear on restart. For instance, if we remove a
    /// table because of an issue with its schema, and the operator then "fixes" the issue with the
    /// schema, we will not remove the table during the next execution cycle. This isn't a large
    /// concern because the fact that we never retract errors from subsources means that the source
    /// never return readable values, though it will start sending data along its Ok stream again.
    ///
    /// At the time of writing, the plan is to resolve the above issue when we track each source
    /// table's frontier independently; in that world, we can close the source table's frontier so
    /// we have a durable signal that it should never produce any data.
    source_tables: BTreeMap<u32, SourceTable>,
    row_sender: RowSender,
    sender: Sender<(usize, InternalMessage)>,
    resume_lsn: Arc<AtomicU64>,
}

impl SourceRender for PostgresSourceConnection {
    type Key = ();
    type Value = Row;
    type Time = MzOffset;

    fn render<G: Scope<Timestamp = MzOffset>>(
        self,
        scope: &mut G,
        config: RawSourceCreationConfig,
        connection_context: ConnectionContext,
        resume_uppers: impl futures::Stream<Item = Antichain<MzOffset>> + 'static,
    ) -> (
        Collection<
            G,
            (
                usize,
                Result<SourceMessage<Self::Key, Self::Value>, SourceReaderError>,
            ),
            Diff,
        >,
        Option<Stream<G, Infallible>>,
        Stream<G, (usize, HealthStatusUpdate)>,
        Rc<dyn Any>,
    ) {
        let mut builder = AsyncOperatorBuilder::new(config.name.clone(), scope.clone());

        let (mut data_output, stream) = builder.new_output();
        let (mut _upper_output, progress) = builder.new_output();
        let (mut health_output, health_stream) = builder.new_output();

        let button = builder.build(move |mut capabilities| async move {
            let health_capability = capabilities.pop().unwrap();
            let mut upper_capability = capabilities.pop().unwrap();
            let mut data_capability = capabilities.pop().unwrap();
            assert!(capabilities.is_empty());

            let active_read_worker = crate::source::responsible_for(
                &config.id,
                config.worker_id,
                config.worker_count,
                (),
            );

            if !active_read_worker {
                return;
            }

            // TODO: figure out the best default here; currently this is optimized
            // for the speed to pass pg-cdc-resumption tests on a local machine.
            let (dataflow_tx, dataflow_rx) = tokio::sync::mpsc::channel(50_000);

            let resume_upper =
                Antichain::from_iter(config.source_resume_upper.iter().map(MzOffset::decode_row));
            let Some(start_offset) = resume_upper.into_option() else {
                return;
            };
            data_capability.downgrade(&start_offset);
            upper_capability.downgrade(&start_offset);

            let resume_lsn = Arc::new(AtomicU64::new(start_offset.offset));

            let connection_config = self
                .connection
                .config(&*connection_context.secrets_reader)
                .await
                .expect("Postgres connection unexpectedly missing secrets")
                .replication_timeouts(config.params.pg_replication_timeouts);

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
                source_id: config.id,
                connection_config,
                publication: self.publication,
                slot: self.publication_details.slot,
                replication_lsn: start_offset.offset.into(),
                metrics: PgSourceMetrics::new(&config.base_metrics, config.id),
                source_tables,
                row_sender: RowSender::new(dataflow_tx.clone()),
                sender: dataflow_tx,
                resume_lsn: Arc::clone(&resume_lsn),
            };

            task::spawn(|| format!("postgres_source:{}", config.id), {
                postgres_replication_loop(task_info)
            });

            let source_metrics = SourceReaderMetrics::new(&config.base_metrics, config.id);
            let offset_commit_metrics = source_metrics.offset_commit_metrics();

            let mut reader = PostgresSourceReader {
                receiver_stream: dataflow_rx,
                last_lsn: start_offset.offset.into(),
                data_capability,
                upper_capability,
            };

            let offset_committer = PgOffsetCommitter { resume_lsn };
            let offset_commit_loop = async move {
                tokio::pin!(resume_uppers);
                while let Some(frontier) = resume_uppers.next().await {
                    tracing::trace!(
                        "timely-{} source({}) committing offsets: resume_upper={}",
                        config.id,
                        config.worker_id,
                        frontier.pretty()
                    );
                    if let Err(e) = offset_committer.commit_offsets(frontier.clone()) {
                        offset_commit_metrics.offset_commit_failures.inc();
                        tracing::warn!(
                            %e,
                            "timely-{} source({}) failed to commit offsets: resume_upper={}",
                            config.id,
                            config.worker_id,
                            frontier.pretty()
                        );
                    }
                }
            };
            tokio::pin!(offset_commit_loop);

            loop {
                tokio::select! {
                    message = reader.receiver_stream.recv() => match message {
                        Some((output_index, InternalMessage::Value {
                            lsn,
                            value,
                            end,
                        })) => {
                            mz_ore::soft_assert!(
                                output_index != 0,
                                "InternalMessage::Value is meant only for subsources"
                            );

                            reader.last_lsn = lsn;
                            let (msg, diff) = match value {
                                Ok((row, diff)) => (
                                    Ok(SourceMessage {
                                        upstream_time_millis: None,
                                        key: (),
                                        value: row,
                                        headers: None,
                                    }),
                                    diff,
                                ),
                                Err(err) => (Err(SourceReaderError::other_definite(err)), 1),
                            };

                            let ts = lsn.into();
                            let cap = reader.data_capability.delayed(&ts);
                            let next_ts = ts + 1;
                            reader.upper_capability.downgrade(&next_ts);
                            if end {
                                reader.data_capability.downgrade(&next_ts);
                            }
                            data_output.give(&cap, ((output_index, msg), *cap.time(), diff)).await;
                        }
                        Some((output_index, InternalMessage::Status(update))) => {
                            health_output.give(&health_capability, (output_index, update)).await;
                        }
                        Some((output_index, InternalMessage::Err(err))) => {
                            mz_ore::soft_assert!(
                                output_index == 0,
                                "InternalMessage::Err is meant only for the primary source"
                            );

                            // XXX(petrosagg): we are fabricating a timestamp here!!
                            let non_definite_ts = MzOffset::from(reader.last_lsn) + 1;

                            let cap = reader.data_capability.delayed(&non_definite_ts);
                            let next_ts = non_definite_ts + 1;
                            reader.data_capability.downgrade(&next_ts);
                            reader.upper_capability.downgrade(&next_ts);
                            data_output.give(&cap, ((output_index, Err(err)), *cap.time(), 1)).await;
                        }
                        None => return,
                    },
                    // This future is not cancel safe but we are only passing a reference to it in
                    // the select! loop so the future stays on the stack and never gets cancelled
                    // until the end of the function.
                    _ = offset_commit_loop.as_mut() => {},
                }
            }
        });

        (
            stream.as_collection(),
            Some(progress),
            health_stream,
            Rc::new(button.press_on_drop()),
        )
    }
}

impl PgOffsetCommitter {
    fn commit_offsets(&self, frontier: Antichain<MzOffset>) -> Result<(), anyhow::Error> {
        if let Some(offset) = frontier.as_option() {
            // TODO(petrosagg): this minus one is very suspicious. It is replicating the previous
            // behaviour where the commit offset was calculated by calling
            // OffsetAntichain::as_data_offsets, which subtracted one. Investigate if it's truly
            // needed
            self.resume_lsn
                .store(offset.offset.saturating_sub(1), Ordering::SeqCst);
        }

        Ok(())
    }
}

/// Defers to `postgres_replication_loop_inner` and sends errors through the channel if they occur
#[allow(clippy::or_fun_call)]
async fn postgres_replication_loop(mut task_info: PostgresTaskInfo) {
    loop {
        // Signal that the primary source is running.
        let _ = task_info
            .sender
            .send((
                0,
                InternalMessage::Status(HealthStatusUpdate {
                    update: HealthStatus::Running,
                    should_halt: false,
                }),
            ))
            .await;

        match postgres_replication_loop_inner(&mut task_info).await {
            Ok(()) => {}
            Err(ReplicationError::Indefinite(e)) => {
                warn!(
                    "replication for source {} interrupted, retrying: {e}",
                    task_info.source_id
                );
                // Indefinite errors affect all subsources.
                for output_index in std::iter::once(0)
                    .chain(task_info.source_tables.values().map(|t| t.output_index))
                {
                    // If the channel is shutting down, so is the source.
                    let _ = task_info
                        .sender
                        .send((
                            output_index,
                            InternalMessage::Status(HealthStatusUpdate {
                                update: HealthStatus::StalledWithError {
                                    error: e.to_string_with_causes(),
                                    hint: None,
                                },
                                should_halt: false,
                            }),
                        ))
                        .await;
                }
            }
            Err(ReplicationError::Irrecoverable(e)) => {
                warn!(
                    "irrecoverable error for source {}: {}, cause: {}",
                    &task_info.source_id,
                    e,
                    e.source().unwrap_or(anyhow::anyhow!("unknown").as_ref())
                );
                // Irrecoverable errors affect all subsources.
                for output_index in std::iter::once(0)
                    .chain(task_info.source_tables.values().map(|t| t.output_index))
                {
                    // If the channel is shutting down, so is the source.
                    let _ = task_info
                        .sender
                        .send((
                            output_index,
                            InternalMessage::Status(HealthStatusUpdate {
                                update: HealthStatus::StalledWithError {
                                    error: e.to_string_with_causes(),
                                    hint: None,
                                },
                                // TODO: In the future we probably want to handle this more gracefully,
                                // but for now halting is the easiest way to dump the data in the pipe.
                                // The restarted clusterd instance will restart the snapshot fresh, which will
                                // avoid any inconsistencies. Note that if the same lsn is chosen in the
                                // next snapshotting, the remapped timestamp chosen will be the same for
                                // both instances of clusterd.
                                should_halt: true,
                            }),
                        ))
                        .await;
                }

                future::pending().await
            }
            Err(ReplicationError::Definite(e)) => {
                warn!(
                    "definite error for source {}: {}, cause: {}",
                    &task_info.source_id,
                    e,
                    e.source().unwrap_or(anyhow::anyhow!("unknown").as_ref())
                );

                // Definite errors affect all subsources in a way that they should be errored out.
                for output_index in task_info.source_tables.values().map(|t| t.output_index) {
                    // If the channel is shutting down, so is the source.
                    let _ = task_info
                        .row_sender
                        .send_row(
                            task_info.replication_lsn,
                            output_index,
                            Err(anyhow!(e.to_string())),
                        )
                        .await;
                }

                // Close the LSN to "commit" the messages we just sent.
                task_info
                    .row_sender
                    .close_lsn(task_info.replication_lsn)
                    .await;

                // Drop the send error, as we have no way of communicating back to the
                // source operator if the channel is gone.
                let _ = task_info
                    .row_sender
                    .sender
                    .send((
                        0,
                        InternalMessage::Err(SourceReaderError {
                            inner: SourceErrorDetails::Initialization(e.to_string()),
                        }),
                    ))
                    .await;
                return;
            }
        }
        // TODO(petrosagg): implement exponential back-off
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

/// The logic to snapshot the PG collections.
///
/// Importantly this is broken out so we can ensure nothing returns an indefinite error, which can
/// happen inadvertently.
async fn postgres_replication_loop_inner_snapshot(
    task_info: &mut PostgresTaskInfo,
) -> Result<(), ReplicationError> {
    // Verify relevant tables for this publication; we do this on every loop to cleanup source
    // tables and very lazily detect dropped tables.
    let publication_tables = mz_postgres_util::publication_info(
        &task_info.connection_config,
        &task_info.publication,
        None,
    )
    .await
    .err_indefinite()?;

    // Validate publication tables against the state snapshot
    let incompatible_tables =
        determine_table_compatibility(task_info.source_tables.iter(), publication_tables);
    for (id, output, err) in incompatible_tables {
        task_info.source_tables.remove(&id);
        task_info
            .row_sender
            .send_row(task_info.replication_lsn, output, Err(err))
            .await;
    }

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

    // If we sent any messages earlier, we need to close that LSN to send the next messages from
    // the slot.
    if task_info.replication_lsn < slot_lsn {
        task_info
            .row_sender
            .close_lsn(task_info.replication_lsn)
            .await;
    }

    let mut stream = Box::pin(
        produce_snapshot(
            &client,
            &task_info.metrics,
            &mut task_info.source_tables,
            task_info.source_id,
        )
        .enumerate(),
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
        let (output_index, value) = match event {
            Ok(event) => event,
            Err(err @ ReplicationError::Definite(_)) => return Err(err),
            Err(ReplicationError::Indefinite(err) | ReplicationError::Irrecoverable(err)) => {
                return Err(ReplicationError::Irrecoverable(err))
            }
        };
        task_info
            .row_sender
            .send_row(slot_lsn, output_index, value)
            .await
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

    // We only need to rewind values from the WAL if there are any values between the `slot_lsn`
    // and `snapshot_lsn`. If there is nothing in the WAL for us to see between those times, we
    // would just sit there dumbly waiting for a timeout.
    if slot_lsn < snapshot_lsn
        && peek_wal_lsns(
            task_info.connection_config.clone(),
            &task_info.slot,
            &task_info.publication,
            Some(snapshot_lsn),
        )
        .await
        .err_irrecoverable()?
        .count()
            > 0
    {
        tracing::info!(
            "postgres snapshot was at {snapshot_lsn:?} but we need it at {slot_lsn:?}. Rewinding"
        );
        // Our snapshot was too far ahead so we must rewind it by reading the replication
        // stream until the snapshot lsn and emitting any rows that we find with negated diffs
        let replication_stream = produce_replication(
            task_info.connection_config.clone(),
            &task_info.slot,
            &task_info.publication,
            slot_lsn,
            Arc::clone(&task_info.resume_lsn),
            &task_info.metrics,
            &mut task_info.source_tables,
            task_info.source_id,
        )
        .await;
        tokio::pin!(replication_stream);

        while let Some(event) = replication_stream.next().await {
            match event {
                Ok(Event::Message(lsn, (output_index, row))) => {
                    // Here we ignore the lsn that this row actually happened at and we
                    // forcefully emit it at the slot_lsn with a negated diff.
                    if lsn <= snapshot_lsn {
                        task_info
                            .row_sender
                            .send_row(slot_lsn, output_index, row.map(|(row, diff)| (row, -diff)))
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
                Err(ReplicationError::Indefinite(err) | ReplicationError::Irrecoverable(err)) => {
                    return Err(ReplicationError::Irrecoverable(err))
                }
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

    Ok(())
}

/// Core logic
async fn postgres_replication_loop_inner(
    task_info: &mut PostgresTaskInfo,
) -> Result<(), ReplicationError> {
    if task_info.replication_lsn == PgLsn::from(0) {
        match postgres_replication_loop_inner_snapshot(task_info).await {
            // Snapshotting cannot, under any circmstance, return indefinite errors; everything must
            // be restarted.
            Err(ReplicationError::Indefinite(err)) => {
                return Err(ReplicationError::Irrecoverable(err))
            }
            o => o?,
        }
    }

    let replication_stream = produce_replication(
        task_info.connection_config.clone(),
        &task_info.slot,
        &task_info.publication,
        task_info.replication_lsn,
        Arc::clone(&task_info.resume_lsn),
        &task_info.metrics,
        &mut task_info.source_tables,
        task_info.source_id,
    )
    .await;
    tokio::pin!(replication_stream);

    // TODO(petrosagg): The API does not guarantee that we won't see an error after we have already
    // partially emitted a transaction, but we know it is the case due to the implementation. Find
    // a way to encode this in the type signature
    while let Some(event) = replication_stream.next().await.transpose()? {
        match event {
            Event::Message(lsn, (output_index, row)) => {
                task_info.row_sender.send_row(lsn, output_index, row).await;
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
    lsn: Option<PgLsn>,
    output_index: usize,
    value: Result<(Row, Diff), anyhow::Error>,
}

/// A type that makes it easy to correctly send inserts and deletes.
///
/// Note: `RowSender::delete/insert` should be called with the same
/// lsn until `close_lsn` is called, which should be called and awaited
/// before dropping the `RowSender` or moving onto a new lsn.
/// Internally, this type uses asserts to uphold the first requirement.
struct RowSender {
    sender: Sender<(usize, InternalMessage)>,
    buffered_messages: Vec<RowMessage>,
}

impl RowSender {
    /// Create a new `RowSender`.
    pub fn new(sender: Sender<(usize, InternalMessage)>) -> Self {
        Self {
            sender,
            buffered_messages: vec![],
        }
    }

    /// Buffer an error without an LSN.
    ///
    /// If you can produce an LSN, instead use [`RowSender::send_row`].
    pub fn buffer_error(&mut self, output_index: usize, err: anyhow::Error) {
        let lsn = self
            .buffered_messages
            .iter()
            .map(|row| row.lsn)
            .max()
            .flatten();

        self.buffered_messages.push(RowMessage {
            lsn,
            output_index,
            value: Err(err),
        })
    }

    /// Send a triplet for the specific output
    pub async fn send_row(
        &mut self,
        lsn: PgLsn,
        output_index: usize,
        value: Result<(Row, Diff), anyhow::Error>,
    ) {
        while let Some(buffered) = self.buffered_messages.pop() {
            assert!(buffered.lsn.is_none() || buffered.lsn == Some(lsn));
            self.send_row_inner(lsn, buffered.output_index, buffered.value, false)
                .await
        }

        self.buffered_messages.push(RowMessage {
            lsn: Some(lsn),
            output_index,
            value,
        });
    }

    /// Finalize an lsn, making sure all messages that my be buffered are flushed, and that the
    /// last message sent is marked as closing the `lsn` (which is the messages `offset` in the
    /// rest of the source pipeline.
    pub async fn close_lsn(&mut self, lsn: PgLsn) {
        for message in self.buffered_messages.iter_mut() {
            message.lsn = Some(message.lsn.unwrap_or(lsn));
            assert!(message.lsn <= Some(lsn));
        }

        // Get last message or notify that source is running.
        let last = match self.buffered_messages.pop() {
            Some(last) => last,
            None => {
                // If the channel is shutting down, so is the source.
                let _ = self
                    .sender
                    .send((
                        0,
                        InternalMessage::Status(HealthStatusUpdate {
                            update: HealthStatus::Running,
                            should_halt: false,
                        }),
                    ))
                    .await;
                return;
            }
        };

        while let Some(buffered) = self.buffered_messages.pop() {
            self.send_row_inner(
                buffered.lsn.expect("set to Some earlier"),
                buffered.output_index,
                buffered.value,
                false,
            )
            .await
        }

        self.send_row_inner(
            last.lsn.expect("set to Some earlier"),
            last.output_index,
            last.value,
            true,
        )
        .await
    }

    async fn send_row_inner(
        &self,
        lsn: PgLsn,
        output: usize,
        value: Result<(Row, Diff), anyhow::Error>,
        end: bool,
    ) {
        let message = InternalMessage::Value { lsn, value, end };
        // a closed receiver means the source has been shutdown (dropped or the process is dying),
        // so just continue on without activation
        let _ = self.sender.send((output, message)).await;
    }
}

/// Determines if a set of [`SourceTable`]s and a set of [`PostgresTableDesc`] are compatible with
/// one another in a way that Materialize can handle.
///
/// The returned tuples are `(a,b)`:
/// - `a`: the ID of the incompatible table
/// - `b`: the subsource error to propagate reflecting that the subsource is in an errored state
fn determine_table_compatibility<'a, I>(
    source_tables: I,
    tables: Vec<PostgresTableDesc>,
) -> Vec<(u32, usize, anyhow::Error)>
where
    I: Iterator<Item = (&'a u32, &'a SourceTable)>,
{
    let pub_tables: BTreeMap<u32, PostgresTableDesc> =
        tables.into_iter().map(|t| (t.oid, t)).collect();
    let mut errors = vec![];
    for (id, info) in source_tables {
        match pub_tables.get(id) {
            Some(desc) => {
                // Keep this method in sync with the Relation check in the replication stream.
                if let Err(err) = info.desc.determine_compatibility(desc) {
                    warn!(
                        "Error validating table in publication. Expected: {:?} Actual: {:?}",
                        &info.desc, desc
                    );
                    errors.push((*id, info.output_index, err));
                }
            }
            None => {
                warn!(
                    "alter table error, table removed from upstream source: name {}, oid {}, old_schema {:?}",
                    info.desc.name,
                    info.desc.oid,
                    info.desc.columns,
                );
                errors.push((
                    *id,
                    info.output_index,
                    anyhow!(
                        "source table {} with oid {} has been dropped",
                        info.desc.name,
                        info.desc.oid
                    ),
                ));
            }
        }
    }
    errors
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
    source_tables: &'a mut BTreeMap<u32, SourceTable>,
    source_id: GlobalId,
) -> impl futures::Stream<Item = Result<(usize, Result<(Row, Diff), anyhow::Error>), ReplicationError>>
       + 'a {
    async_stream::try_stream! {
        // Scratch space to use while evaluating casts
        let mut datum_vec = DatumVec::new();
        let mut subsources_to_remove = vec![];

        'tables: for (id, info) in source_tables.iter() {
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
                // Try catch
                let mut pack_row = || -> Result<(Row, Diff), anyhow::Error> {
                    let mut packer = text_row.packer();
                    // Convert raw rows from COPY into repr:Row. Each Row is a relation_id
                    // and list of string-encoded values, e.g. Row{ 16391 , ["1", "2"] }
                    let parser = mz_pgcopy::CopyTextFormatParser::new(b.as_ref(), "\t", "\\N");

                    let mut raw_values = parser.iter_raw_truncating(info.desc.columns.len());
                    while let Some(raw_value) = raw_values.next() {
                        match raw_value.map_err(|e| if e.to_string() == "missing data for column" {
                            anyhow!(
                                "source table {} with oid {} has been altered",
                                info.desc.name,
                                id
                            )
                        } else {
                             e.into()
                        })? {
                            Some(value) => {
                                packer.push(Datum::String(std::str::from_utf8(value)?))
                            }
                            None => packer.push(Datum::Null),
                        }
                    }

                    let mut datums = datum_vec.borrow();
                    datums.extend(text_row.iter());

                    Ok((cast_row(&info.casts, &datums)?, 1))
                };

                let row = pack_row();
                let is_err = if let Err(e) = &row {
                    warn!(
                        "definite error for subsource of {} with oid {}: {}, cause: {}",
                        source_id,
                        id,
                        e,
                        e.source().unwrap_or(anyhow::anyhow!("unknown").as_ref())
                    );
                    true
                } else {
                    false
                };
                yield (info.output_index, row);
                if is_err {
                    subsources_to_remove.push(*id);
                    continue 'tables;
                }
            }

            metrics.tables.inc();
        }

        for id in subsources_to_remove {
            source_tables.remove(&id);
        }
    }
}

/// Packs a Tuple received in the replication stream into a Row packer.
fn datums_from_tuple<'a, T>(
    rel_id: u32,
    len: usize,
    tuple_data: T,
    datums: &mut Vec<Datum<'a>>,
) -> Result<(), anyhow::Error>
where
    T: IntoIterator<Item = &'a TupleData>,
{
    for val in tuple_data.into_iter().take(len) {
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
    source_tables: &'a mut BTreeMap<u32, SourceTable>,
    source_id: GlobalId,
) -> impl futures::Stream<
    Item = Result<Event<[PgLsn; 1], (usize, Result<(Row, Diff), anyhow::Error>)>, ReplicationError>,
> + 'a {
    use ReplicationError::*;
    use ReplicationMessage::*;
    async_stream::try_stream!({
        //let mut last_data_message = Instant::now();
        let mut inserts = vec![];
        let mut deletes = vec![];
        let mut errors = vec![];

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
            // If either inserts or deletes have values, we have mishandled ingesting data and risk
            // double-counting values.
            if !inserts.is_empty() || !deletes.is_empty() || !errors.is_empty() {
                return Err(Definite(anyhow!(
                    "tried to reconnect to PG replication stream with uncommitted data",
                )))?;
            }

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
                use LogicalReplicationMessage::*;
                metrics.total.inc();

                // Ensure no more than `FEEDBACK_INTERVAL` passes; if we exceed the deadline, we
                // should let the upstream PG source know we're still here. This also gives us an
                // opportunity to check that it's still alive.
                let res = mz_ore::future::timeout(
                    FEEDBACK_INTERVAL.saturating_sub(last_feedback.elapsed()),
                    stream.as_mut().try_next(),
                )
                .await;

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

                // A gentle suggestion for how to properly handle subsource errors. This is
                // annoyingly long because we can't capture anything.
                macro_rules! handle_subsource_err {
                    ($errors:expr, $source_tables:expr, $output_index:expr, $err:expr, $rel_id:expr) => {{
                        warn!(
                            "definite error for subsource of {} with oid {}: {}, cause: {}",
                            source_id,
                            $rel_id,
                            $err,
                            $err.source().unwrap_or(anyhow::anyhow!("unknown").as_ref())
                        );
                        $errors.push(($output_index, $err));
                        $source_tables.remove(&$rel_id);
                    }};
                }

                match res {
                    Ok(Some(XLogData(xlog_data))) => match xlog_data.data() {
                        Begin(_) => {
                            last_data_message = Instant::now();
                            if !inserts.is_empty() || !deletes.is_empty() {
                                return Err(Definite(anyhow!(
                                    "got BEGIN statement after uncommitted data"
                                )))?;
                            }
                        }
                        Insert(insert) if source_tables.contains_key(&insert.rel_id()) => {
                            let rel_id = insert.rel_id();
                            let info = source_tables.get(&rel_id).unwrap();
                            last_data_message = Instant::now();
                            metrics.inserts.inc();
                            let new_tuple = insert.tuple().tuple_data();
                            let mut datums = datum_vec.borrow();

                            let mut gen_row = || -> Result<Row, anyhow::Error> {
                                datums_from_tuple(
                                    rel_id,
                                    info.desc.columns.len(),
                                    new_tuple,
                                    &mut *datums,
                                )?;
                                cast_row(&info.casts, &datums)
                            };

                            match gen_row() {
                                Ok(row) => inserts.push((info.output_index, row)),
                                Err(err) => handle_subsource_err!(
                                    errors,
                                    source_tables,
                                    info.output_index,
                                    err,
                                    rel_id
                                ),
                            }
                        }
                        Update(update) if source_tables.contains_key(&update.rel_id()) => {
                            last_data_message = Instant::now();
                            metrics.updates.inc();
                            let rel_id = update.rel_id();
                            let info = source_tables.get(&rel_id).unwrap();

                            let mut gen_row = || -> Result<(Row, Row), anyhow::Error> {
                                let err = || {
                                    anyhow!(
                                            "Old row missing from replication stream for table with OID = {}.
                                             Did you forget to set REPLICA IDENTITY to FULL for your table?",
                                            rel_id
                                        )
                                };
                                let old_tuple = update.old_tuple().ok_or_else(err)?.tuple_data();

                                let mut old_datums = datum_vec.borrow();
                                datums_from_tuple(
                                    rel_id,
                                    info.desc.columns.len(),
                                    old_tuple,
                                    &mut *old_datums,
                                )?;
                                let old_row = cast_row(&info.casts, &old_datums)?;

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
                                datums_from_tuple(
                                    rel_id,
                                    info.desc.columns.len(),
                                    new_tuple,
                                    &mut *new_datums,
                                )?;
                                Ok((old_row, cast_row(&info.casts, &new_datums)?))
                            };

                            match gen_row() {
                                Ok((old_row, new_row)) => {
                                    deletes.push((info.output_index, old_row));
                                    inserts.push((info.output_index, new_row));
                                }
                                Err(err) => handle_subsource_err!(
                                    errors,
                                    source_tables,
                                    info.output_index,
                                    err,
                                    rel_id
                                ),
                            }
                        }
                        Delete(delete) if source_tables.contains_key(&delete.rel_id()) => {
                            last_data_message = Instant::now();
                            metrics.deletes.inc();
                            let rel_id = delete.rel_id();
                            let info = source_tables.get(&rel_id).unwrap();
                            let mut gen_row = || -> Result<Row, anyhow::Error> {
                                let err = || {
                                    anyhow!(
                                            "Old row missing from replication stream for table with OID = {}.
                                             Did you forget to set REPLICA IDENTITY to FULL for your table?",
                                            rel_id
                                        )
                                };
                                let old_tuple = delete.old_tuple().ok_or_else(err)?.tuple_data();
                                let mut datums = datum_vec.borrow();
                                datums_from_tuple(
                                    rel_id,
                                    info.desc.columns.len(),
                                    old_tuple,
                                    &mut *datums,
                                )?;
                                cast_row(&info.casts, &datums)
                            };

                            match gen_row() {
                                Ok(row) => deletes.push((info.output_index, row)),
                                Err(err) => handle_subsource_err!(
                                    errors,
                                    source_tables,
                                    info.output_index,
                                    err,
                                    rel_id
                                ),
                            };
                        }
                        Commit(commit) => {
                            last_data_message = Instant::now();
                            metrics.transactions.inc();
                            last_commit_lsn = PgLsn::from(commit.end_lsn());

                            for (output, row) in deletes.drain(..) {
                                yield Event::Message(last_commit_lsn, (output, Ok((row, -1))));
                            }
                            for (output, row) in inserts.drain(..) {
                                yield Event::Message(last_commit_lsn, (output, Ok((row, 1))));
                            }
                            for (output, err) in errors.drain(..) {
                                yield Event::Message(last_commit_lsn, (output, Err(err)));
                            }
                            yield Event::Progress([PgLsn::from(u64::from(last_commit_lsn) + 1)]);
                            metrics.lsn.set(last_commit_lsn.into());
                        }
                        Relation(relation) => {
                            last_data_message = Instant::now();
                            let rel_id = relation.rel_id();
                            if let Some(info) = source_tables.get(&rel_id) {
                                // Because the replication stream doesn't include columns'
                                // attnums, we need to check the current local schema against
                                // the current remote schema to ensure e.g. we haven't received
                                // a schema update with the same terminal column name which is
                                // actually a different column.
                                let current_publication_info = mz_postgres_util::publication_info(
                                    &client_config,
                                    publication,
                                    Some(rel_id),
                                )
                                .await
                                .err_indefinite()?;

                                // Validate publication tables against the state snapshot
                                let incompatible_tables = determine_table_compatibility(
                                    std::iter::once((&rel_id, info)),
                                    current_publication_info,
                                );
                                for (rel_id, output_index, err) in incompatible_tables {
                                    handle_subsource_err!(
                                        errors,
                                        source_tables,
                                        output_index,
                                        err,
                                        rel_id
                                    );
                                }
                            }
                        }
                        Insert(_) | Update(_) | Delete(_) | Origin(_) | Type(_) => {
                            last_data_message = Instant::now();
                            metrics.ignored.inc();
                        }
                        Truncate(truncate) => {
                            let truncated_tables = truncate
                                .rel_ids()
                                .iter()
                                // Filter here makes option handling in map "safe"
                                .filter_map(|id| source_tables.get(id).map(|info| (id, info)))
                                .map(|(id, info)| {
                                    (
                                        id,
                                        info.output_index,
                                        anyhow!(
                                            "source table truncated: name: {} id: {}",
                                            info.desc.name,
                                            info.desc.oid
                                        ),
                                    )
                                })
                                .collect::<Vec<_>>();

                            for (rel_id, output_index, err) in truncated_tables {
                                handle_subsource_err!(
                                    errors,
                                    source_tables,
                                    output_index,
                                    err,
                                    rel_id
                                );
                            }
                        }
                        // The enum is marked as non_exhaustive. Better to be conservative here in
                        // case a new message is relevant to the semantics of our source
                        _ => {
                            return Err(Definite(anyhow!(
                                "unexpected logical replication message"
                            )))?;
                        }
                    },
                    Ok(Some(PrimaryKeepAlive(keepalive))) => {
                        needs_status_update = needs_status_update || keepalive.reply() == 1;
                        // Irrespective of the WAL lag, we do not want to reconnect if we have
                        // pending writes, nor do we want to consider fast-forwarding the WAL. If
                        // the WAL lag is intense enough, we'll receive a TCP error.
                        if inserts.is_empty() && deletes.is_empty() && errors.is_empty() {
                            observed_wal_end = PgLsn::from(keepalive.wal_end());

                            if last_data_message.elapsed() > WAL_LAG_GRACE_PERIOD {
                                break;
                            }
                        }
                    }
                    // The enum is marked non_exhaustive, better be conservative
                    Ok(Some(_)) => {
                        return Err(Definite(anyhow!("Unexpected replication message")))?
                    }
                    Ok(None) => break,
                    Err(TimeoutError::Inner(err)) => return Err(ReplicationError::from(err))?,
                    Err(TimeoutError::DeadlineElapsed) => {
                        // if we did timeout, skip message handling to let us continue polling, but do
                        // ensure we perform a status update to heartbeat the upstream PG source.
                        mz_ore::soft_assert!(
                            needs_status_update,
                            "if our request timed out, it must have been at least long enough to require \
                             a status update"
                        );
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

            // We reach this place if the consume loop above detected large WAL lag. This
            // section determines whether or not we can skip over that part of the WAL by
            // peeking into the replication slot using a normal SQL query and the
            // `pg_logical_slot_peek_binary_changes` administrative function.
            //
            // By doing so we can get a positive statement about existence or absence of
            // relevant data from the current LSN to the observed WAL end. If there are no
            // messages then it is safe to fast forward last_commit_lsn to the WAL end LSN and restart
            // the replication stream from there.
            let peek_binary_start_time = Instant::now();

            let changes = peek_wal_lsns(client_config.clone(), slot, publication, None)
                .await
                .err_indefinite()?
                .filter(|change_lsn| change_lsn > &last_commit_lsn)
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

/// Return a peek of all LSNs in the WAL beyond the current position.
///
/// If `up_to` is `None`, this will be all LSNs until the end of the log, otherwise will be up to
/// the specified LSN.
///
/// For more details, see <https://pgpedia.info/p/pg_logical_slot_peek_binary_changes.html>.
async fn peek_wal_lsns(
    config: mz_postgres_util::Config,
    slot: &str,
    publication: &str,
    up_to: Option<PgLsn>,
) -> Result<impl Iterator<Item = PgLsn>, mz_postgres_util::PostgresError> {
    let client = config.connect_replication().await?;
    let query = format!(
        "SELECT lsn FROM pg_logical_slot_peek_binary_changes(
            '{}', {}, NULL,
            'proto_version', '1',
            'publication_names', '{}'
        )",
        slot,
        match up_to {
            Some(lsn) => format!("'{lsn}'"),
            None => "NULL".to_string(),
        },
        publication,
    );

    let rows = client.simple_query(&query).await?;

    Ok(rows.into_iter().filter_map(|row| match row {
        SimpleQueryMessage::Row(row) => {
            let lsn: PgLsn = row
                .get("lsn")
                .expect("missing expected column: `lsn`")
                .parse()
                .expect("invalid lsn");
            Some(lsn)
        }
        SimpleQueryMessage::CommandComplete(_) => None,
        _ => panic!("unexpected enum variant"),
    }))
}
