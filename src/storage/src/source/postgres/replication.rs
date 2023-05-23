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
use timely::dataflow::operators::Capability;
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
use mz_repr::{Datum, DatumVec, Diff, Row};
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

/// The maximum amount of waiting for a transaction to appear in the replication stream before
/// checking for replication lag.
const TX_TIMEOUT: Duration = Duration::from_secs(30);

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
    resume_upper: Antichain<MzOffset>,
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

            let Some(resume_lsn) = resume_upper.into_option() else {
                return Ok(());
            };
            data_cap.downgrade(&resume_lsn);
            upper_cap.downgrade(&resume_lsn);

            let connection_config = connection
                .connection
                .config(&*context.secrets_reader)
                .await?
                .replication_timeouts(config.params.pg_replication_timeouts.clone());

            let mut rewinds = BTreeMap::new();
            while let Some(event) = rewind_input.next_mut().await {
                if let AsyncEvent::Data(cap, data) = event {
                    let cap = cap.retain_for_output(0);
                    for req in data.drain(..) {
                        assert!(
                            resume_lsn <= req.snapshot_lsn,
                            "slot compacted past snapshot point. snapshot_lsn={} resume_lsn={resume_lsn}", req.snapshot_lsn
                        );
                        rewinds.insert(req.oid, (cap.clone(), req));
                    }
                }
            }

            let mut committed_uppers = pin!(committed_uppers);

            // This loop alternates  between streaming the replication slot and using normal SQL
            // queries with pg admin functions to fast-foward our cursor in the event of WAL lag.
            let client = connection_config.connect("replication metadata").await?;
            loop {
                if let Err(err) = ensure_publication_exists(&client, &connection.publication).await? {
                    // If the publication gets deleted there is nothing else to do. These errors
                    // are not retractable.
                    for &oid in table_info.keys() {
                        let update = ((oid, Err(err.clone())), *data_cap.time(), 1);
                        data_output.give(data_cap, update).await;
                    }
                    return Ok(());
                }

                // We will peek the slot first to eagerly advance the frontier in case the stream
                // is currently empty. This avoids needlesly holding back the capability and allows
                // the rest of the ingestion pipeline to proceed immediately.
                let slot = &connection.publication_details.slot;
                advance_upper(&client, slot, &connection.publication, data_cap).await?;
                upper_cap.downgrade(data_cap.time());
                rewinds.retain(|_, (_, req)| data_cap.time() <= &req.snapshot_lsn);
                trace!(%id, "timely-{worker_id} downgraded capability to {}", data_cap.time());
                trace!(%id, "timely-{worker_id} pending rewinds {rewinds:?}");

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

                while let Ok(_) = tokio::time::timeout(TX_TIMEOUT, stream.as_mut().peek()).await {
                    let mut new_upper = *data_cap.time();
                    while let Some(event) = stream.as_mut().peek().now_or_never() {
                        match event {
                            Some(Ok(LogicalReplicationMessage::Begin(begin))) => {
                                let tx_lsn = MzOffset::from(begin.final_lsn());
                                let mut tx = pin!(extract_transaction(
                                    stream.by_ref(),
                                    &table_info,
                                    &connection_config,
                                    &metrics,
                                    &connection.publication,
                                    &mut errored
                                ));

                                trace!(%id, "timely-{worker_id} extracting transaction at {tx_lsn}");
                                while let Some((oid, event, diff)) = tx.try_next().await? {
                                    if !table_info.contains_key(&oid) {
                                        continue;
                                    }
                                    let data = (oid, event);
                                    if let Some((rewind_cap, req)) = rewinds.get(&oid) {
                                        if tx_lsn <= req.snapshot_lsn {
                                            trace!(%id, "timely-{worker_id} rewinding transaction at {tx_lsn}");
                                            let update = (data.clone(), MzOffset::from(0), -diff);
                                            data_output.give(rewind_cap, update).await;
                                        }
                                    }
                                    container.push((data, tx_lsn, diff));
                                }
                                new_upper = tx_lsn + 1;
                                if container.len() > max_capacity {
                                    data_output.give_container(data_cap, &mut container).await;
                                    upper_cap.downgrade(&new_upper);
                                    data_cap.downgrade(&new_upper);
                                }
                            }
                            Some(Ok(_)) => return Err(TransientError::BareTransactionEvent),
                            Some(Err(_)) => {
                                return Err(stream.as_mut().next().await.unwrap().unwrap_err())
                            }
                            None => break,
                        }
                    }
                    data_output.give_container(data_cap, &mut container).await;
                    upper_cap.downgrade(&new_upper);
                    data_cap.downgrade(&new_upper);
                    rewinds.retain(|_, (_, req)| data_cap.time() <= &req.snapshot_lsn);
                }
            }
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
) -> impl AsyncStream<Item = Result<LogicalReplicationMessage, TransientError>> + 'a {
    async_stream::try_stream!({
        let mut uppers = pin!(uppers);
        let mut last_committed_upper = resume_lsn;

        let replication_client = connection_config.connect_replication().await?;
        super::ensure_replication_slot(&replication_client, slot).await?;

        // Postgres will return all transactions that commit *strictly* after the provided LSN but
        // we want to produce all transactions that commit *at or after* the provided LSN. We
        // therefore subtract one.
        let lsn = PgLsn::from(resume_lsn.offset.saturating_sub(1));
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
                        yield data.into_data();
                        last_feedback.elapsed() > FEEDBACK_INTERVAL
                    }
                    Some(ReplicationMessage::PrimaryKeepAlive(keepalive)) => keepalive.reply() == 1,
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
                // For the same reason as above, we must subtract one here too.
                let lsn = PgLsn::from(last_committed_upper.offset.saturating_sub(1));
                stream
                    .as_mut()
                    .standby_status_update(lsn, lsn, lsn, ts, 0)
                    .await?;
                last_feedback = Instant::now();
            }
        }
    })
}

/// Extracts a single transaction from the replication stream delimited by a BEGIN and COMMIT message.
fn extract_transaction<'a>(
    stream: impl AsyncStream<Item = Result<LogicalReplicationMessage, TransientError>> + 'a,
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
        let commit_lsn = match stream.try_next().await? {
            Some(Begin(body)) => body.final_lsn(),
            Some(_) => Err(TransientError::UnmatchedTransaction)?,
            None => Err(TransientError::ReplicationEOF)?,
        };
        metrics.transactions.inc();
        metrics.lsn.set(commit_lsn.into());
        while let Some(event) = stream.try_next().await? {
            metrics.total.inc();
            match event {
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
                    if commit_lsn != body.commit_lsn() {
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

/// Discovers LSN of the next replication stream update that happens beyond `start_lsn`. If there
/// is no such update the current upper LSN is returned.
async fn advance_upper(
    client: &Client,
    slot: &str,
    publication: &str,
    cap: &mut Capability<MzOffset>,
) -> Result<(), TransientError> {
    // Figure out the last written LSN and then add one to convert it into an upper.
    let row = client.query_one("SELECT pg_current_wal_lsn()", &[]).await?;
    let last_write: PgLsn = row.get("pg_current_wal_lsn");
    let pg_upper = PgLsn::from(u64::from(last_write) + 1);

    // `pg_logical_slot_peek_binary_changes` will include only those transactions which commit
    // *prior* to the specified LSN. i.e (commit_lsn < pg_upper).
    let query = format!(
        "SELECT data FROM pg_logical_slot_peek_binary_changes(
        '{slot}', '{pg_upper}', NULL, 'proto_version', '1', 'publication_names', '{publication}')"
    );
    let rows = client.query(&query, &[]).await?;

    for row in rows {
        let data = Bytes::from(row.get::<_, Vec<u8>>("data"));
        let message = LogicalReplicationMessage::parse(&data)
            .map_err(TransientError::MalformedReplicationMessage)?;
        if let LogicalReplicationMessage::Begin(body) = message {
            let ts = MzOffset::from(body.final_lsn());
            if cap.time() <= &ts {
                cap.downgrade(&ts);
                return Ok(());
            }
        }
    }
    // If there is no transaction with commit_lsn < pg_upper, the upper is pg_upper.
    cap.downgrade(&MzOffset::from(pg_upper));
    Ok(())
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
