// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::pin;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::{Stream as AsyncStream, StreamExt};
use mz_dyncfg::ConfigSet;
use mz_postgres_util::{Client, Sql, query_opt, simple_query_opt, sql};
use mz_storage_types::dyncfgs::PG_SOURCE_VALIDATE_TIMELINE;
use mz_storage_types::sources::{MzOffset, PostgresSourceConnection};
use mz_timely_util::builder_async::AsyncOutputHandle;
use postgres_replication::LogicalReplicationStream;
use postgres_replication::protocol::ReplicationMessage;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::operators::Capability;
use timely::progress::Antichain;
use tokio::sync::watch;
use tokio_postgres::error::SqlState;
use tokio_postgres::types::PgLsn;
use tracing::{error, trace};

use crate::source::RawSourceCreationConfig;
use crate::source::postgres::{DefiniteError, TransientError};
use crate::source::probe;
use crate::source::types::{Probe, ResumeUppers};

use super::LogicalReplMsg;

/// Postgres epoch is 2000-01-01T00:00:00Z
static PG_EPOCH: LazyLock<SystemTime> =
    LazyLock::new(|| UNIX_EPOCH + Duration::from_secs(946_684_800));

/// Produces the logical replication stream while taking care of regularly sending standby
/// keepalive messages with the provided `uppers` stream.
///
/// The returned stream will contain all transactions that whose commit LSN is beyond `resume_lsn`.
pub(super) async fn raw_stream<'a>(
    config: &'a RawSourceCreationConfig,
    connection: &'a PostgresSourceConnection,
    replication_client: Client,
    metadata_client: Arc<Client>,
    resume_lsn: MzOffset,
    uppers: impl futures::Stream<Item = ResumeUppers<MzOffset>> + 'a,
    probe_output: &'a AsyncOutputHandle<MzOffset, CapacityContainerBuilder<Vec<Probe<MzOffset>>>>,
    probe_cap: &'a Capability<MzOffset>,
) -> Result<
    Result<impl AsyncStream<Item = Result<LogicalReplMsg, TransientError>> + 'a, DefiniteError>,
    TransientError,
> {
    let slot = &connection.publication_details.slot;
    let publication = &connection.publication;
    let is_physical_replica = connection.publication_details.get_is_physical_replica();

    if let Err(err) = ensure_replication_prerequisites(
        &metadata_client,
        &replication_client,
        connection,
        config.config.config_set(),
    )
    .await?
    {
        return Ok(Err(err));
    }

    // How often a proactive standby status update message should be sent to the server.
    //
    // The upstream will periodically request status updates by setting the keepalive's reply field
    // value to 1. However, we cannot rely on these messages arriving on time. For example, when
    // the upstream is sending a big transaction its keepalive messages are queued and can be
    // delayed arbitrarily.
    //
    // See: <https://www.postgresql.org/message-id/CAMsr+YE2dSfHVr7iEv1GSPZihitWX-PMkD9QALEGcTYa+sdsgg@mail.gmail.com>
    //
    // For this reason we query the server's timeout value and proactively send a keepalive at
    // twice the frequency to have a healthy margin from the deadline.
    //
    // Note: We must use the metadata client here which is NOT in replication mode. Some Aurora
    // Postgres versions disallow SHOW commands from within replication connection.
    // See: https://github.com/readysettech/readyset/discussions/28#discussioncomment-4405671
    let row = simple_query_opt(&*metadata_client, sql!("SHOW wal_sender_timeout;"))
        .await?
        .unwrap();
    let wal_sender_timeout = match row.get("wal_sender_timeout") {
        // When this parameter is zero the timeout mechanism is disabled
        Some("0") => None,
        Some(value) => Some(
            mz_repr::adt::interval::Interval::from_str(value)
                .unwrap()
                .duration()
                .unwrap(),
        ),
        None => panic!("ubiquitous parameter missing"),
    };

    // This interval controls the cadence at which we send back status updates and, crucially,
    // request PrimaryKeepAlive messages. PrimaryKeepAlive messages drive the frontier forward in
    // the absence of data updates and we don't want a large `wal_sender_timeout` value to slow us
    // down. For this reason the feedback interval is set to one second, or less if the
    // wal_sender_timeout is less than 2 seconds.
    let feedback_interval = match wal_sender_timeout {
        Some(t) => std::cmp::min(Duration::from_secs(1), t.checked_div(2).unwrap()),
        None => Duration::from_secs(1),
    };

    let mut feedback_timer = tokio::time::interval(feedback_interval);
    // 'Delay' ensures we always tick at least 'feedback_interval'.
    feedback_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    // Postgres will return all transactions that commit *at or after* after the provided LSN,
    // following the timely upper semantics.
    let lsn = PgLsn::from(resume_lsn.offset);
    let query = sql!(
        "START_REPLICATION SLOT {} LOGICAL {} (\"proto_version\" '1', \"publication_names\" {})",
        Sql::ident(slot),
        Sql::raw_unchecked(lsn.to_string()),
        Sql::literal(publication)
    );
    let copy_stream = match replication_client.copy_both_simple(query.as_str()).await {
        Ok(copy_stream) => copy_stream,
        Err(err) if err.code() == Some(&SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE) => {
            return Ok(Err(DefiniteError::InvalidReplicationSlot));
        }
        Err(err) => return Err(err.into()),
    };

    // According to the documentation [1] we must check that the slot LSN matches our
    // expectations otherwise we risk getting silently fast-forwarded to a future LSN. In order
    // to avoid a TOCTOU issue we must do this check after starting the replication stream. We
    // cannot use the replication client to do that because it's already in CopyBoth mode.
    // [1] https://www.postgresql.org/docs/15/protocol-replication.html#PROTOCOL-REPLICATION-START-REPLICATION-SLOT-LOGICAL
    let slot_metadata = super::super::fetch_slot_metadata(
        &*metadata_client,
        slot,
        mz_storage_types::dyncfgs::PG_FETCH_SLOT_RESUME_LSN_INTERVAL
            .get(config.config.config_set()),
    )
    .await?;
    let min_resume_lsn = slot_metadata.confirmed_flush_lsn;
    tracing::info!(
        %config.id,
        "started replication using backend PID={:?}. wal_sender_timeout={:?}",
        slot_metadata.active_pid, wal_sender_timeout
    );

    let (probe_tx, mut probe_rx) = watch::channel(None);
    let timestamp_interval = config.timestamp_interval;
    let now_fn = config.now_fn.clone();
    let max_lsn_task_handle =
        mz_ore::task::spawn(|| format!("pg_current_wal_lsn:{}", config.id), async move {
            let mut probe_ticker = probe::Ticker::new(move || timestamp_interval, now_fn);

            while !probe_tx.is_closed() {
                let probe_ts = probe_ticker.tick().await;
                let probe_or_err =
                    mz_postgres_util::fetch_max_lsn(&*metadata_client, is_physical_replica)
                        .await
                        .map(|lsn| Probe {
                            probe_ts,
                            upstream_frontier: Antichain::from_elem(MzOffset::from(lsn)),
                        });
                let _ = probe_tx.send(Some(probe_or_err));
            }
        })
        .abort_on_drop();

    let stream = async_stream::try_stream!({
        // Ensure we don't pre-drop the task
        let _max_lsn_task_handle = max_lsn_task_handle;

        // ensure we don't drop the replication client!
        let _replication_client = replication_client;

        let mut uppers = pin!(uppers);
        let mut last_committed_upper = resume_lsn;

        let mut stream = pin!(LogicalReplicationStream::new(copy_stream));

        if !(resume_lsn == MzOffset::from(0) || min_resume_lsn <= resume_lsn) {
            let err = TransientError::OvercompactedReplicationSlot {
                available_lsn: min_resume_lsn,
                requested_lsn: resume_lsn,
            };
            error!("timely-{} ({}) {err}", config.worker_id, config.id);
            Err(err)?;
        }

        loop {
            tokio::select! {
                Some(next_message) = stream.next() => match next_message {
                    Ok(ReplicationMessage::XLogData(data)) => {
                        yield ReplicationMessage::XLogData(data);
                        Ok(())
                    }
                    Ok(ReplicationMessage::PrimaryKeepAlive(keepalive)) => {
                        yield ReplicationMessage::PrimaryKeepAlive(keepalive);
                        Ok(())
                    }
                    Err(err) => Err(err.into()),
                    _ => Err(TransientError::UnknownReplicationMessage),
                },
                _ = feedback_timer.tick() => {
                    let ts: i64 = PG_EPOCH.elapsed().unwrap().as_micros().try_into().unwrap();
                    let lsn = PgLsn::from(last_committed_upper.offset);
                    trace!("timely-{} ({}) sending keepalive {lsn:?}", config.worker_id, config.id);
                    // Postgres only sends PrimaryKeepAlive messages when *it* wants a reply, which
                    // happens when out status update is late. Since we send them proactively this
                    // may never happen. It is therefore *crucial* that we set the last parameter
                    // (the reply flag) to 1 here. This will cause the upstream server to send us a
                    // PrimaryKeepAlive message promptly which will give us frontier advancement
                    // information in the absence of data updates.
                    let res = stream.as_mut().standby_status_update(lsn, lsn, lsn, ts, 1).await;
                    res.map_err(|e| e.into())
                },
                Some(uppers) = uppers.next() => {
                    if let Some(lsn) = uppers.source.as_ref().and_then(|upper| upper.as_option()) {
                        if last_committed_upper < *lsn {
                            last_committed_upper = *lsn;
                        }
                    }
                    // `offset_committed` was seeded with `resume_lsn`, so never report below it.
                    for (id, upper) in &uppers.exports {
                        let stat = config.statistics.get(id);
                        if let (Some(lsn), Some(stat)) = (upper.as_option(), stat) {
                            stat.set_offset_committed(std::cmp::max(*lsn, resume_lsn).offset);
                        }
                    }
                    Ok(())
                },
                Ok(()) = probe_rx.changed() => match &*probe_rx.borrow() {
                    Some(Ok(probe)) => {
                        if let Some(offset_known) = probe.upstream_frontier.as_option() {
                            for stat in config.statistics.values() {
                                stat.set_offset_known(offset_known.offset);
                            }
                        }
                        probe_output.give(probe_cap, probe);
                        Ok(())
                    },
                    Some(Err(err)) => Err(anyhow::anyhow!("{err}").into()),
                    None => Ok(()),
                },
                else => return
            }?;
        }
    });
    Ok(Ok(stream))
}

/// Ensures the upstream still meets the conditions the replication stream depends on. Once
/// violated, these conditions stay violated. It returns an outer transient error in case of
/// connection issues and an inner definite error for the first violated condition.
pub(in crate::source::postgres) async fn ensure_replication_prerequisites(
    metadata_client: &Client,
    replication_client: &Client,
    connection: &PostgresSourceConnection,
    config_set: &ConfigSet,
) -> Result<Result<(), DefiniteError>, TransientError> {
    if let Err(err) = ensure_publication_exists(metadata_client, &connection.publication).await? {
        return Ok(Err(err));
    }

    // This runs before the timeline ID check on purpose. Promoting a physical replica to a primary
    // both flips pg_is_in_recovery() and switches the timeline, so either check could fire. Running
    // this one first means a promotion always surfaces as the specific InvalidPhysicalReplica error
    // (rather than the more generic timeline mismatch), which is the clearer signal for an operator.
    let is_physical_replica = connection.publication_details.get_is_physical_replica();
    if let Err(err) = ensure_physical_replica(metadata_client, is_physical_replica).await? {
        return Ok(Err(err));
    }

    // Sources created before the timeline ID was recorded have none to check against.
    if let Some(expected_timeline_id) = &connection.publication_details.timeline_id {
        if let Err(err) =
            ensure_replication_timeline_id(replication_client, expected_timeline_id, config_set)
                .await?
        {
            return Ok(Err(err));
        }
    }

    Ok(Ok(()))
}

/// Ensures the publication exists on the server. It returns an outer transient error in case of
/// connection issues and an inner definite error if the publication is dropped.
async fn ensure_publication_exists(
    client: &Client,
    publication: &str,
) -> Result<Result<(), DefiniteError>, TransientError> {
    // Figure out the last written LSN and then add one to convert it into an upper.
    let result = query_opt(
        &**client,
        sql!("SELECT 1 FROM pg_publication WHERE pubname = $1;"),
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

/// Ensure the active replication timeline_id matches the one we expect such that we can safely
/// resume replication. It returns an outer transient error in case of
/// connection issues and an inner definite error if the timeline id does not match.
async fn ensure_replication_timeline_id(
    replication_client: &Client,
    expected_timeline_id: &u64,
    config_set: &ConfigSet,
) -> Result<Result<(), DefiniteError>, TransientError> {
    let timeline_id = mz_postgres_util::get_timeline_id(replication_client).await?;
    if timeline_id == *expected_timeline_id {
        Ok(Ok(()))
    } else {
        if PG_SOURCE_VALIDATE_TIMELINE.get(config_set) {
            Ok(Err(DefiniteError::InvalidTimelineId {
                expected: *expected_timeline_id,
                actual: timeline_id,
            }))
        } else {
            tracing::warn!(
                "Timeline ID mismatch ignored: expected={expected_timeline_id} actual={timeline_id}"
            );
            Ok(Ok(()))
        }
    }
}

async fn ensure_physical_replica(
    metadata_client: &Client,
    expected_is_physical_replica: bool,
) -> Result<Result<(), DefiniteError>, TransientError> {
    let is_physical_replica = mz_postgres_util::get_is_in_recovery(metadata_client).await?;
    if is_physical_replica == expected_is_physical_replica {
        Ok(Ok(()))
    } else {
        Ok(Err(DefiniteError::InvalidPhysicalReplica {
            expected: expected_is_physical_replica,
            actual: is_physical_replica,
        }))
    }
}
