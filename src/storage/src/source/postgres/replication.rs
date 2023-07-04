// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Renders the logical replication side of the [`PostgresSourceConnection`] ingestion dataflow.
//!
//! ```text
//!              o
//!              │rewind
//!              │requests
//!          ╭───┴────╮
//!          │exchange│ (collect all requests to one worker)
//!          ╰───┬────╯
//!           ┏━━v━━━━━━━━━━┓
//!           ┃ replication ┃ (single worker)
//!           ┃   reader    ┃
//!           ┗━┯━━━━━━━━┯━━┛
//!             │raw     │
//!             │data    │
//!        ╭────┴─────╮  │
//!        │distribute│  │ (distribute to all workers)
//!        ╰────┬─────╯  │
//! ┏━━━━━━━━━━━┷━┓      │
//! ┃ replication ┃      │ (parallel decode)
//! ┃   decoder   ┃      │
//! ┗━━━━━┯━━━━━━━┛      │
//!       │ replication  │ progress
//!       │ updates      │ output
//!       v              v
//! ```
//!
//! # Progress tracking
//!
//! In order to avoid causing excessive resource usage in the upstream server it's important to
//! track the LSN that we have successfully committed to persist and communicate that back to
//! PostgreSQL. Under normal operation this gauge of progress is provided by the presence of
//! transactions themselves. Since at a given LSN offset there can be only a single message, when a
//! transaction is received and processed we can infer that we have seen all the messages that are
//! not beyond `commit_lsn + 1`.
//!
//! Things are a bit more complicated in the absence of transactions though because even though we
//! don't receive any the server might very well be generating WAL records. This can happen if
//! there is a separate logical database performing writes (which is the case for RDS databases),
//! or, in servers running PostgreSQL version 15 or greater, the logical replication process
//! includes an optimization that omits empty transactions, which can happen if you're only
//! replicating a subset of the tables and there writes going to the other ones.
//!
//! If we fail to detect this situation and don't send LSN feedback in a timely manner the server
//! will be forced to keep around WAL data that can eventually lead to disk space exhaustion.
//!
//! In the absence of transactions the only available piece of information in the replication
//! stream are keepalive messages. Keepalive messages are documented[1] to contain the current end
//! of WAL on the server. That is a useless number when it comes to progress tracking because there
//! might be pending messages at LSNs between the last received commit_lsn and the current end of
//! WAL.
//!
//! Fortunately for us, the documentation for PrimaryKeepalive messages is wrong and it actually
//! contains the last *sent* LSN[2]. Here sent doesn't necessarily mean sent over the wire, but
//! sent to the upstream process that is handling producing the logical stream. Therefore, if we
//! receive a keepalive with a particular LSN we can be certain that there are no other replication
//! messages at previous LSNs, because they would have been already generated and received. We
//! therefore connect the keepalive messages directly to our capability.
//!
//! [1]: https://www.postgresql.org/docs/15/protocol-replication.html#PROTOCOL-REPLICATION-START-REPLICATION
//! [2]: https://www.postgresql.org/message-id/CAFPTHDZS9O9WG02EfayBd6oONzK%2BqfUxS6AbVLJ7W%2BKECza2gg%40mail.gmail.com

use std::any::Any;
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::pin::pin;
use std::rc::Rc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use differential_dataflow::{AsCollection, Collection};
use futures::{future, future::select, FutureExt, Stream as AsyncStream, StreamExt, TryStreamExt};
use once_cell::sync::Lazy;
use postgres_protocol::message::backend::{
    LogicalReplicationMessage, ReplicationMessage, TupleData,
};
use serde::{Deserialize, Serialize};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use tokio_postgres::replication::LogicalReplicationStream;
use tokio_postgres::types::PgLsn;
use tokio_postgres::Client;
use tracing::{error, trace};

use mz_expr::MirScalarExpr;
use mz_ore::cast::CastFrom;
use mz_ore::collections::HashSet;
use mz_ore::result::ResultExt;
use mz_postgres_util::desc::PostgresTableDesc;
use mz_repr::{Datum, DatumVec, Diff, GlobalId, Row};
use mz_sql_parser::ast::{display::AstDisplay, Ident};
use mz_storage_client::types::connections::ConnectionContext;
use mz_storage_client::types::sources::{MzOffset, PostgresSourceConnection};
use mz_timely_util::builder_async::{Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder};
use mz_timely_util::operator::StreamExt as TimelyStreamExt;

use crate::source::postgres::verify_schema;
use crate::source::postgres::{metrics::PgSourceMetrics, DefiniteError, TransientError};
use crate::source::types::SourceReaderError;
use crate::source::RawSourceCreationConfig;

/// Postgres epoch is 2000-01-01T00:00:00Z
static PG_EPOCH: Lazy<SystemTime> = Lazy::new(|| UNIX_EPOCH + Duration::from_secs(946_684_800));

/// How often a proactive standby status update message should be sent to the server.
///
/// The upstream will periodically request status updates by setting the keepalive's reply field to
/// 1. However, we cannot rely on these messages arriving on time. For example, when the upstream
/// is sending a big transaction its keepalive messages are queued and can be delayed arbitrarily.
///
/// See: <https://www.postgresql.org/message-id/CAMsr+YE2dSfHVr7iEv1GSPZihitWX-PMkD9QALEGcTYa+sdsgg@mail.gmail.com>
const FEEDBACK_INTERVAL: Duration = Duration::from_secs(30);

// A request to rewind a snapshot taken at `snapshot_lsn` to the initial LSN of the replication
// slot. This is accomplished by emitting `(data, 0, -diff)` for all updates `(data, lsn, diff)`
// whose `lsn <= snapshot_lsn`. By convention the snapshot is always emitted at LSN 0.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RewindRequest {
    /// The table OID that should be rewound.
    pub(crate) oid: u32,
    /// The LSN that the snapshot was taken at.
    pub(crate) snapshot_lsn: MzOffset,
}

/// Renders the replication dataflow. See the module documentation for more information.
pub(crate) fn render<G: Scope<Timestamp = MzOffset>>(
    scope: G,
    config: RawSourceCreationConfig,
    connection: PostgresSourceConnection,
    context: ConnectionContext,
    subsource_resume_uppers: BTreeMap<GlobalId, Antichain<MzOffset>>,
    table_info: BTreeMap<u32, (usize, PostgresTableDesc, Vec<MirScalarExpr>)>,
    rewind_stream: &Stream<G, RewindRequest>,
    committed_uppers: impl futures::Stream<Item = Antichain<MzOffset>> + 'static,
) -> (
    Collection<G, (usize, Result<Row, SourceReaderError>), Diff>,
    Stream<G, Infallible>,
    Stream<G, Rc<TransientError>>,
    Rc<dyn Any>,
) {
    let op_name = format!("ReplicationReader({})", config.id);
    let mut builder = AsyncOperatorBuilder::new(op_name, scope);

    let slot_reader = u64::cast_from(config.responsible_worker("slot"));
    let mut rewind_input = builder.new_input(rewind_stream, Exchange::new(move |_| slot_reader));
    let (mut data_output, data_stream) = builder.new_output();
    let (_upper_output, upper_stream) = builder.new_output();

    let metrics = PgSourceMetrics::new(&config.base_metrics, config.id);
    metrics.tables.set(u64::cast_from(table_info.len()));

    let reader_table_info = table_info.clone();
    let (button, errors) = builder.build_fallible(move |caps| {
        let table_info = reader_table_info;
        Box::pin(async move {
            let (id, worker_id) = (config.id, config.worker_id);
            let [data_cap, upper_cap]: &mut [_; 2] = caps.try_into().unwrap();
            let (data_cap, upper_cap) = (data_cap.as_mut().unwrap(), upper_cap.as_mut().unwrap());

            if !config.responsible_for("slot") {
                return Ok(());
            }

            // Determine the slot lsn.
            let connection_config = connection
                .connection
                .config(&*context.secrets_reader)
                .await?
                .replication_timeouts(config.params.pg_replication_timeouts.clone());
            let slot = &connection.publication_details.slot;
            let replication_client = connection_config.connect_replication().await?;
            super::ensure_replication_slot(&replication_client, slot).await?;
            let slot_lsn = super::fetch_slot_resume_lsn(&replication_client, slot).await?;

            let resume_upper = Antichain::from_iter(
                subsource_resume_uppers
                    .values()
                    .flat_map(|f| f.elements())
                    // Advance any upper as far as the slot_lsn.
                    .map(|t|std::cmp::max(*t, slot_lsn))
            );

            let Some(resume_lsn) = resume_upper.into_option() else {
                return Ok(());
            };
            data_cap.downgrade(&resume_lsn);
            upper_cap.downgrade(&resume_lsn);
            trace!(%id, "timely-{worker_id} replication reader started lsn={}", resume_lsn);


            let mut rewinds = BTreeMap::new();
            while let Some(event) = rewind_input.next_mut().await {
                if let AsyncEvent::Data(cap, data) = event {
                    let cap = cap.retain_for_output(0);
                    for req in data.drain(..) {
                        assert!(
                            resume_lsn <= req.snapshot_lsn + 1,
                            "slot compacted past snapshot point. snapshot consistent point={} resume_lsn={resume_lsn}", req.snapshot_lsn + 1
                        );
                        rewinds.insert(req.oid, (cap.clone(), req));
                    }
                }
            }
            trace!(%id, "timely-{worker_id} pending rewinds {rewinds:?}");

            let mut committed_uppers = pin!(committed_uppers);

            let client = connection_config.connect("replication metadata").await?;
            if let Err(err) = ensure_publication_exists(&client, &connection.publication).await? {
                // If the publication gets deleted there is nothing else to do. These errors
                // are not retractable.
                for &oid in table_info.keys() {
                    let update = ((oid, Err(err.clone())), *data_cap.time(), 1);
                    data_output.give(data_cap, update).await;
                }
                return Ok(());
            }

            let slot = &connection.publication_details.slot;
            let mut stream = pin!(raw_stream(
                &config,
                &connection_config,
                &client,
                slot,
                &connection.publication,
                *data_cap.time(),
                committed_uppers.as_mut()
            )
            .peekable());

            let mut errored = HashSet::new();
            let mut container = Vec::new();
            let max_capacity = timely::container::buffer::default_capacity::<((u32, Result<Vec<Option<Bytes>>, DefiniteError>), MzOffset, Diff)>();

            while let Some(event) = stream.as_mut().next().await {
                use LogicalReplicationMessage::*;
                use ReplicationMessage::*;
                let mut new_upper = *data_cap.time();
                match event {
                    Ok(XLogData(data)) => match data.data() {
                        Begin(begin) => {
                            let commit_lsn = MzOffset::from(begin.final_lsn());

                            let mut tx = pin!(extract_transaction(
                                stream.by_ref(),
                                commit_lsn,
                                &table_info,
                                &connection_config,
                                &metrics,
                                &connection.publication,
                                &mut errored
                            ));

                            trace!(%id, "timely-{worker_id} extracting transaction at {commit_lsn}");
                            while let Some((oid, event, diff)) = tx.try_next().await? {
                                if !table_info.contains_key(&oid) {
                                    continue;
                                }
                                let data = (oid, event);
                                if let Some((rewind_cap, req)) = rewinds.get(&oid) {
                                    if commit_lsn <= req.snapshot_lsn {
                                        let update = (data.clone(), MzOffset::from(0), -diff);
                                        data_output.give(rewind_cap, update).await;
                                    }
                                }
                                assert!(new_upper <= commit_lsn, "new_upper={} tx_lsn={}", new_upper, commit_lsn);
                                container.push((data, commit_lsn, diff));
                            }
                            new_upper = commit_lsn + 1;
                            if container.len() > max_capacity {
                                data_output.give_container(data_cap, &mut container).await;
                                upper_cap.downgrade(&new_upper);
                                data_cap.downgrade(&new_upper);
                            }
                        },
                        _ => return Err(TransientError::BareTransactionEvent),
                    }
                    Ok(PrimaryKeepAlive(keepalive)) => {
                        trace!(%id, "timely-{worker_id} received keepalive lsn={}", keepalive.wal_end());
                        new_upper = std::cmp::max(new_upper, keepalive.wal_end().into());
                    }
                    Ok(_) => return Err(TransientError::UnknownReplicationMessage),
                    Err(err) => return Err(err),
                }

                let will_yield = stream.as_mut().peek().now_or_never().is_none();
                if will_yield {
                    data_output.give_container(data_cap, &mut container).await;
                    upper_cap.downgrade(&new_upper);
                    data_cap.downgrade(&new_upper);
                    rewinds.retain(|_, (_, req)| data_cap.time() <= &req.snapshot_lsn);
                }
            }
            // We never expect the replication stream to gracefully end
            Err(TransientError::ReplicationEOF)
        })
    });

    // Distribute the raw slot data to all workers and turn it into a collection
    let raw_collection = data_stream.distribute().as_collection();

    // We now process the slot updates and apply the cast expressions
    let mut final_row = Row::default();
    let mut datum_vec = DatumVec::new();
    let replication_updates = raw_collection.map(move |(oid, event)| {
        let (output_index, _, casts) = &table_info[&oid];
        let event = event.and_then(|row| {
            let mut datums = datum_vec.borrow();
            for col in row.iter() {
                let datum = col.as_deref().map(super::decode_utf8_text).transpose()?;
                datums.push(datum.unwrap_or(Datum::Null));
            }
            super::cast_row(casts, &datums, &mut final_row)?;
            Ok(final_row.clone())
        });

        (*output_index, event.err_into())
    });

    (
        replication_updates,
        upper_stream,
        errors,
        Rc::new(button.press_on_drop()),
    )
}

/// Produces the logical replication stream while taking care of regularly sending standby
/// keepalive messages with the provided `uppers` stream.
///
/// The returned stream will contain all transactions that whose commit LSN is beyond `resume_lsn`.
fn raw_stream<'a>(
    config: &'a RawSourceCreationConfig,
    connection_config: &'a mz_postgres_util::Config,
    metadata_client: &'a Client,
    slot: &'a str,
    publication: &'a str,
    resume_lsn: MzOffset,
    uppers: impl futures::Stream<Item = Antichain<MzOffset>> + 'a,
) -> impl AsyncStream<Item = Result<ReplicationMessage<LogicalReplicationMessage>, TransientError>> + 'a
{
    async_stream::try_stream!({
        let mut uppers = pin!(uppers);
        let mut last_committed_upper = resume_lsn;

        let replication_client = connection_config.connect_replication().await?;
        super::ensure_replication_slot(&replication_client, slot).await?;

        // Postgres will return all transactions that commit *at or after* after the provided LSN,
        // following the timely upper semantics.
        let lsn = PgLsn::from(resume_lsn.offset);
        let query = format!(
            r#"START_REPLICATION SLOT "{}" LOGICAL {} ("proto_version" '1', "publication_names" '{}')"#,
            Ident::from(slot.clone()).to_ast_string(),
            lsn,
            publication,
        );
        let copy_stream = replication_client.copy_both_simple(&query).await?;
        let mut stream = pin!(LogicalReplicationStream::new(copy_stream));

        // According to the documentation [1] we must check that the slot LSN matches our
        // expectations otherwise we risk getting silently fast-forwarded to a future LSN. In order
        // to avoid a TOCTOU issue we must do this check after starting the replication stream. We
        // cannot use the replication client to do that because it's already in CopyBoth mode.
        // [1] https://www.postgresql.org/docs/15/protocol-replication.html#PROTOCOL-REPLICATION-START-REPLICATION-SLOT-LOGICAL
        let min_resume_lsn = super::fetch_slot_resume_lsn(metadata_client, slot).await?;
        if !(resume_lsn == MzOffset::from(0) || min_resume_lsn <= resume_lsn) {
            let err = TransientError::OvercompactedReplicationSlot {
                available_lsn: min_resume_lsn,
                requested_lsn: resume_lsn,
            };
            error!("timely-{} ({}) {err}", config.worker_id, config.id);
            Err(err)?;
        }

        let mut last_feedback = Instant::now();
        loop {
            let send_feedback = match select(stream.as_mut().next(), uppers.as_mut().next()).await {
                future::Either::Left((next_message, _)) => match next_message.transpose()? {
                    Some(ReplicationMessage::XLogData(data)) => {
                        yield ReplicationMessage::XLogData(data);
                        last_feedback.elapsed() > FEEDBACK_INTERVAL
                    }
                    Some(ReplicationMessage::PrimaryKeepAlive(keepalive)) => {
                        let send_feedback = keepalive.reply() == 1;
                        yield ReplicationMessage::PrimaryKeepAlive(keepalive);
                        send_feedback
                    }
                    Some(_) => Err(TransientError::UnknownReplicationMessage)?,
                    None => return,
                },
                future::Either::Right((upper, _)) => match upper.and_then(|u| u.into_option()) {
                    Some(lsn) => {
                        last_committed_upper = std::cmp::max(last_committed_upper, lsn);
                        true
                    }
                    None => false,
                },
            };
            if send_feedback {
                let ts: i64 = PG_EPOCH.elapsed().unwrap().as_micros().try_into().unwrap();
                let lsn = PgLsn::from(last_committed_upper.offset);
                stream
                    .as_mut()
                    .standby_status_update(lsn, lsn, lsn, ts, 0)
                    .await?;
                last_feedback = Instant::now();
            }
        }
    })
}

/// Extracts a single transaction from the replication stream delimited by a BEGIN and COMMIT
/// message. The BEGIN message must have already been consumed from the stream before calling this
/// function.
fn extract_transaction<'a>(
    stream: impl AsyncStream<Item = Result<ReplicationMessage<LogicalReplicationMessage>, TransientError>>
        + 'a,
    commit_lsn: MzOffset,
    table_info: &'a BTreeMap<u32, (usize, PostgresTableDesc, Vec<MirScalarExpr>)>,
    connection_config: &'a mz_postgres_util::Config,
    metrics: &'a PgSourceMetrics,
    publication: &'a str,
    errored_tables: &'a mut HashSet<u32>,
) -> impl AsyncStream<
    Item = Result<(u32, Result<Vec<Option<Bytes>>, DefiniteError>, Diff), TransientError>,
> + 'a {
    use LogicalReplicationMessage::*;
    async_stream::try_stream!({
        let mut stream = pin!(stream);
        metrics.transactions.inc();
        metrics.lsn.set(commit_lsn.offset);
        while let Some(event) = stream.try_next().await? {
            // We can ignore keepalive messages while processing a transaction because the
            // commit_lsn will drive progress.
            let message = match event {
                ReplicationMessage::XLogData(data) => data.into_data(),
                ReplicationMessage::PrimaryKeepAlive(_) => continue,
                _ => Err(TransientError::UnknownReplicationMessage)?,
            };
            metrics.total.inc();
            match message {
                Insert(body) if errored_tables.contains(&body.rel_id()) => continue,
                Update(body) if errored_tables.contains(&body.rel_id()) => continue,
                Delete(body) if errored_tables.contains(&body.rel_id()) => continue,
                Relation(body) if errored_tables.contains(&body.rel_id()) => continue,
                Insert(body) => {
                    metrics.inserts.inc();
                    let row = unpack_tuple(body.tuple().tuple_data());
                    yield (body.rel_id(), row, 1);
                }
                Update(body) => match body.old_tuple() {
                    Some(old_tuple) => {
                        metrics.updates.inc();
                        // If the new tuple contains unchanged toast values we reference the old ones
                        let new_tuple =
                            std::iter::zip(body.new_tuple().tuple_data(), old_tuple.tuple_data())
                                .map(|(new, old)| match new {
                                    TupleData::UnchangedToast => old,
                                    _ => new,
                                });
                        let old_row = unpack_tuple(old_tuple.tuple_data());
                        yield (body.rel_id(), old_row, -1);
                        let new_row = unpack_tuple(new_tuple);
                        yield (body.rel_id(), new_row, 1);
                    }
                    None => {
                        yield (body.rel_id(), Err(DefiniteError::DefaultReplicaIdentity), 1);
                    }
                },
                Delete(body) => match body.old_tuple() {
                    Some(old_tuple) => {
                        metrics.deletes.inc();
                        let row = unpack_tuple(old_tuple.tuple_data());
                        yield (body.rel_id(), row, -1);
                    }
                    None => {
                        yield (body.rel_id(), Err(DefiniteError::DefaultReplicaIdentity), 1);
                    }
                },
                Relation(body) => {
                    let rel_id = body.rel_id();
                    if let Some((_, expected_desc, _)) = table_info.get(&rel_id) {
                        // Because the replication stream doesn't include columns' attnums, we need
                        // to check the current local schema against the current remote schema to
                        // ensure e.g. we haven't received a schema update with the same terminal
                        // column name which is actually a different column.
                        let upstream_info = mz_postgres_util::publication_info(
                            connection_config,
                            publication,
                            Some(rel_id),
                        )
                        .await?;
                        let upstream_info = upstream_info.into_iter().map(|t| (t.oid, t)).collect();

                        if let Err(err) = verify_schema(rel_id, expected_desc, &upstream_info) {
                            errored_tables.insert(rel_id);
                            yield (rel_id, Err(err), 1);
                        }
                    }
                }
                Truncate(body) => {
                    for &rel_id in body.rel_ids() {
                        if errored_tables.insert(rel_id) {
                            yield (rel_id, Err(DefiniteError::TableTruncated), 1);
                        }
                    }
                }
                Commit(body) => {
                    if commit_lsn != body.commit_lsn().into() {
                        Err(TransientError::InvalidTransaction)?
                    }
                    return;
                }
                // TODO: We should handle origin messages and emit an error as they indicate that
                // the upstream performed a point in time restore so all bets are off about the
                // continuity of the stream.
                Origin(_) | Type(_) => metrics.ignored.inc(),
                Begin(_) => Err(TransientError::NestedTransaction)?,
                // The enum is marked as non_exhaustive. Better to be conservative
                _ => Err(TransientError::UnknownLogicalReplicationMessage)?,
            }
        }
        Err(TransientError::ReplicationEOF)?;
    })
}

/// Unpacks an iterator of TupleData into a list of nullable bytes or an error if this can't be
/// done.
fn unpack_tuple<'a, I>(tuple_data: I) -> Result<Vec<Option<Bytes>>, DefiniteError>
where
    I: IntoIterator<Item = &'a TupleData>,
    I::IntoIter: ExactSizeIterator,
{
    let iter = tuple_data.into_iter();
    let mut row = Vec::with_capacity(iter.len());
    for data in iter {
        let datum = match data {
            TupleData::Text(bytes) => Some(bytes.clone()),
            TupleData::Null => None,
            TupleData::UnchangedToast => return Err(DefiniteError::MissingToast),
        };
        row.push(datum);
    }
    Ok(row)
}

/// Ensures the publication exists on the server. It returns an outer transient error in case of
/// connection issues and an inner definite error if the publication is dropped.
async fn ensure_publication_exists(
    client: &Client,
    publication: &str,
) -> Result<Result<(), DefiniteError>, TransientError> {
    // Figure out the last written LSN and then add one to convert it into an upper.
    let result = client
        .query_opt(
            "SELECT 1 FROM pg_publication WHERE pubname = $1;",
            &[&publication],
        )
        .await?;
    match result {
        Some(_) => Ok(Ok(())),
        None => Ok(Err(DefiniteError::PublicationDropped(
            publication.to_owned(),
        ))),
    }
}
