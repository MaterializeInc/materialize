// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::BytesMut;
use mz_controller_types::ClusterId;
use mz_ore::now::{to_datetime, NowFn};
use mz_ore::task::spawn;
use mz_ore::{cast::CastFrom, cast::CastInto, now::EpochMillis};
use mz_repr::adt::array::ArrayDimension;
use mz_repr::adt::timestamp::TimestampLike;
use mz_repr::{Datum, Diff, GlobalId, Row, RowPacker, Timestamp};
use mz_sql::plan::Params;
use mz_sql::session::metadata::SessionMetadata;
use mz_sql_parser::ast::{statement_kind_label_value, StatementKind};
use mz_storage_client::controller::IntrospectionType;
use qcell::QCell;
use rand::SeedableRng;
use rand::{distributions::Bernoulli, prelude::Distribution, thread_rng};
use sha2::{Digest, Sha256};
use tokio::time::MissedTickBehavior;
use tracing::debug;
use uuid::Uuid;

use crate::coord::{ConnMeta, Coordinator};
use crate::session::Session;
use crate::statement_logging::{
    SessionHistoryEvent, StatementBeganExecutionRecord, StatementEndedExecutionReason,
    StatementEndedExecutionRecord, StatementLifecycleEvent, StatementPreparedRecord,
};

use super::Message;

/// Metadata required for logging a prepared statement.
#[derive(Debug)]
pub enum PreparedStatementLoggingInfo {
    /// The statement has already been logged; we don't need to log it
    /// again if a future execution hits the sampling rate; we merely
    /// need to reference the corresponding UUID.
    AlreadyLogged { uuid: Uuid },
    /// The statement has not yet been logged; if a future execution
    /// hits the sampling rate, we need to log it at that point.
    StillToLog {
        /// The SQL text of the statement.
        sql: String,
        /// The SQL text of the statement, redacted to follow our data management
        /// policy
        redacted_sql: String,
        /// When the statement was prepared
        prepared_at: EpochMillis,
        /// The name with which the statement was prepared
        name: String,
        /// The ID of the session that prepared the statement
        session_id: Uuid,
        /// Whether we have already recorded this in the "would have logged" metric
        accounted: bool,
        /// The top-level kind of the statement (e.g., `Select`), or `None` for an empty statement
        kind: Option<StatementKind>,
    },
}

#[derive(Copy, Clone, Debug, Ord, Eq, PartialOrd, PartialEq)]
pub struct StatementLoggingId(Uuid);

#[derive(Debug)]
pub(crate) struct PreparedStatementEvent {
    prepared_statement: Row,
    sql_text: Row,
}

#[derive(Debug)]
pub(crate) struct StatementLogging {
    /// Information about statement executions that have been logged
    /// but not finished.
    ///
    /// This map needs to have enough state left over to later retract
    /// the system table entries (so that we can update them when the
    /// execution finished.)
    executions_begun: BTreeMap<Uuid, StatementBeganExecutionRecord>,

    /// Information about sessions that have been started, but which
    /// have not yet been logged in `mz_session_history`.
    /// They may be logged as part of a statement being executed (and chosen for logging).
    unlogged_sessions: BTreeMap<Uuid, SessionHistoryEvent>,

    /// A reproducible RNG for deciding whether to sample statement executions.
    /// Only used by tests; otherwise, `rand::thread_rng()` is used.
    /// Controlled by the system var `statement_logging_use_reproducible_rng`.
    reproducible_rng: rand_chacha::ChaCha8Rng,

    pending_statement_execution_events: Vec<(Row, Diff)>,
    pending_prepared_statement_events: Vec<PreparedStatementEvent>,
    pending_session_events: Vec<Row>,
    pending_statement_lifecycle_events: Vec<Row>,

    now: NowFn,

    /// The number of bytes that we are allowed to emit for statement logging without being throttled.
    /// Increases at a rate of [`mz_sql::session::vars::STATEMENT_LOGGING_TARGET_DATA_RATE`] per second,
    /// up to a max value of [`mz_sql::session::vars::STATEMENT_LOGGING_MAX_DATA_CREDIT`].
    tokens: u64,
    /// The last time at which a statement was logged.
    last_logged_ts_seconds: u64,
    /// The number of statements that have been throttled since the last successfully logged statement.
    throttled_count: usize,
}

impl StatementLogging {
    pub(crate) fn new(now: NowFn) -> Self {
        let last_logged_ts_seconds = (now)() / 1000;
        Self {
            executions_begun: BTreeMap::new(),
            unlogged_sessions: BTreeMap::new(),
            reproducible_rng: rand_chacha::ChaCha8Rng::seed_from_u64(42),
            pending_statement_execution_events: Vec::new(),
            pending_prepared_statement_events: Vec::new(),
            pending_session_events: Vec::new(),
            pending_statement_lifecycle_events: Vec::new(),
            tokens: 0,
            last_logged_ts_seconds,
            now: now.clone(),
            throttled_count: 0,
        }
    }

    /// Check if we need to drop a statement
    /// due to throttling, and update internal data structures appropriately.
    ///
    /// Returns `None` if we must throttle this statement, and `Some(n)` otherwise, where `n`
    /// is the number of statements that were dropped due to throttling before this one.
    fn throttling_check(
        &mut self,
        cost: u64,
        target_data_rate: u64,
        max_data_credit: Option<u64>,
    ) -> Option<usize> {
        let ts = (self.now)() / 1000;
        let elapsed = ts - self.last_logged_ts_seconds;
        self.last_logged_ts_seconds = ts;
        self.tokens = self
            .tokens
            .saturating_add(target_data_rate.saturating_mul(elapsed));
        if let Some(max_data_credit) = max_data_credit {
            self.tokens = self.tokens.min(max_data_credit);
        }
        if let Some(remaining) = self.tokens.checked_sub(cost) {
            debug!("throttling check passed. tokens remaining: {remaining}; cost: {cost}");
            self.tokens = remaining;
            Some(std::mem::take(&mut self.throttled_count))
        } else {
            debug!(
                "throttling check failed. tokens available: {}; cost: {cost}",
                self.tokens
            );
            self.throttled_count += 1;
            None
        }
    }
}

impl Coordinator {
    pub(crate) fn spawn_statement_logging_task(&self) {
        let internal_cmd_tx = self.internal_cmd_tx.clone();
        spawn(|| "statement_logging", async move {
            // TODO[btv] make this configurable via LD?
            // Although... Logging every 5 seconds seems like it
            // should have acceptable cost for now, since we do a
            // group commit for tables every 1s anyway.
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let _ = internal_cmd_tx.send(Message::DrainStatementLog);
            }
        });
    }

    #[mz_ore::instrument(level = "debug")]
    pub(crate) async fn drain_statement_log(&mut self) {
        let session_updates = std::mem::take(&mut self.statement_logging.pending_session_events)
            .into_iter()
            .map(|update| (update, 1))
            .collect();
        let (prepared_statement_updates, sql_text_updates) =
            std::mem::take(&mut self.statement_logging.pending_prepared_statement_events)
                .into_iter()
                .map(
                    |PreparedStatementEvent {
                         prepared_statement,
                         sql_text,
                     }| ((prepared_statement, 1), (sql_text, 1)),
                )
                .unzip::<_, _, Vec<_>, Vec<_>>();
        let statement_execution_updates =
            std::mem::take(&mut self.statement_logging.pending_statement_execution_events);
        let statement_lifecycle_updates =
            std::mem::take(&mut self.statement_logging.pending_statement_lifecycle_events)
                .into_iter()
                .map(|update| (update, 1))
                .collect();

        use IntrospectionType::*;
        for (type_, updates) in [
            (SessionHistory, session_updates),
            (PreparedStatementHistory, prepared_statement_updates),
            (StatementExecutionHistory, statement_execution_updates),
            (StatementLifecycleHistory, statement_lifecycle_updates),
            (SqlText, sql_text_updates),
        ] {
            self.controller
                .storage
                .record_introspection_updates(type_, updates)
                .await;
        }
    }

    /// Check whether we need to do throttling (i.e., whether `STATEMENT_LOGGING_TARGET_DATA_RATE` is set).
    /// If so, actually do the check.
    ///
    /// Returns `None` if we must throttle this statement, and `Some(n)` otherwise, where `n`
    /// is the number of statements that were dropped due to throttling before this one.
    fn statement_logging_throttling_check(&mut self, cost: usize) -> Option<usize> {
        let Some(target_data_rate) = self
            .catalog
            .system_config()
            .statement_logging_target_data_rate()
        else {
            return Some(std::mem::take(&mut self.statement_logging.throttled_count));
        };
        let max_data_credit = self
            .catalog
            .system_config()
            .statement_logging_max_data_credit();
        self.statement_logging.throttling_check(
            cost.cast_into(),
            target_data_rate.cast_into(),
            max_data_credit.map(CastInto::cast_into),
        )
    }

    /// Returns any statement logging events needed for a particular
    /// prepared statement. Possibly mutates the `PreparedStatementLoggingInfo` metadata.
    ///
    /// This function does not do a sampling check, and assumes we did so in a higher layer.
    ///
    /// It _does_ do a throttling check, and returns `None` if we must not log due to throttling.
    pub(crate) fn log_prepared_statement(
        &mut self,
        session: &mut Session,
        logging: &Arc<QCell<PreparedStatementLoggingInfo>>,
    ) -> Option<(
        Option<(StatementPreparedRecord, PreparedStatementEvent)>,
        Uuid,
    )> {
        let logging = session.qcell_rw(&*logging);
        let mut out = None;

        let uuid = match logging {
            PreparedStatementLoggingInfo::AlreadyLogged { uuid } => *uuid,
            PreparedStatementLoggingInfo::StillToLog {
                sql,
                redacted_sql,
                prepared_at,
                name,
                session_id,
                accounted,
                kind,
            } => {
                assert!(
                    *accounted,
                    "accounting for logging should be done in `begin_statement_execution`"
                );
                let uuid = Uuid::new_v4();
                let sql = std::mem::take(sql);
                let redacted_sql = std::mem::take(redacted_sql);
                let sql_hash: [u8; 32] = Sha256::digest(sql.as_bytes()).into();
                let record = StatementPreparedRecord {
                    id: uuid,
                    sql_hash,
                    name: std::mem::take(name),
                    session_id: *session_id,
                    prepared_at: *prepared_at,
                    kind: *kind,
                };
                let mut mpsh_row = Row::default();
                let mut mpsh_packer = mpsh_row.packer();
                Self::pack_statement_prepared_update(&record, &mut mpsh_packer);
                let sql_row = Row::pack([
                    Datum::TimestampTz(
                        to_datetime(*prepared_at)
                            .truncate_day()
                            .try_into()
                            .expect("must fit"),
                    ),
                    Datum::Bytes(sql_hash.as_slice()),
                    Datum::String(sql.as_str()),
                    Datum::String(redacted_sql.as_str()),
                ]);

                let cost = mpsh_packer.byte_len() + sql_row.byte_len();
                let throttled_count = self.statement_logging_throttling_check(cost)?;
                mpsh_packer.push(Datum::UInt64(throttled_count.try_into().expect("must fit")));
                out = Some((
                    record,
                    PreparedStatementEvent {
                        prepared_statement: mpsh_row,
                        sql_text: sql_row,
                    },
                ));

                *logging = PreparedStatementLoggingInfo::AlreadyLogged { uuid };
                uuid
            }
        };
        Some((out, uuid))
    }
    /// The rate at which statement execution should be sampled.
    /// This is the value of the session var `statement_logging_sample_rate`,
    /// constrained by the system var `statement_logging_max_sample_rate`.
    pub fn statement_execution_sample_rate(&self, session: &Session) -> f64 {
        let system: f64 = self
            .catalog()
            .system_config()
            .statement_logging_max_sample_rate()
            .try_into()
            .expect("value constrained to be convertible to f64");
        let user: f64 = session
            .vars()
            .get_statement_logging_sample_rate()
            .try_into()
            .expect("value constrained to be convertible to f64");
        f64::min(system, user)
    }

    /// Record the end of statement execution for a statement whose beginning was logged.
    /// It is an error to call this function for a statement whose beginning was not logged
    /// (because it was not sampled). Requiring the opaque `StatementLoggingId` type,
    /// which is only instantiated by `begin_statement_execution` if the statement is actually logged,
    /// should prevent this.
    pub fn end_statement_execution(
        &mut self,
        id: StatementLoggingId,
        reason: StatementEndedExecutionReason,
    ) {
        let StatementLoggingId(uuid) = id;
        let now = self.now();
        let ended_record = StatementEndedExecutionRecord {
            id: uuid,
            reason,
            ended_at: now,
        };

        let began_record = self
            .statement_logging
            .executions_begun
            .remove(&uuid)
            .expect(
                "matched `begin_statement_execution` and `end_statement_execution` invocations",
            );
        for (row, diff) in
            Self::pack_statement_ended_execution_updates(&began_record, &ended_record)
        {
            self.statement_logging
                .pending_statement_execution_events
                .push((row, diff));
        }
        self.record_statement_lifecycle_event(
            &id,
            &StatementLifecycleEvent::ExecutionFinished,
            now,
        );
    }

    fn pack_statement_execution_inner(
        record: &StatementBeganExecutionRecord,
        packer: &mut RowPacker,
    ) {
        let StatementBeganExecutionRecord {
            id,
            prepared_statement_id,
            sample_rate,
            params,
            began_at,
            cluster_id,
            cluster_name,
            application_name,
            transaction_isolation,
            execution_timestamp,
            transaction_id,
            transient_index_id,
            mz_version,
        } = record;

        let cluster = cluster_id.map(|id| id.to_string());
        let transient_index_id = transient_index_id.map(|id| id.to_string());
        packer.extend([
            Datum::Uuid(*id),
            Datum::Uuid(*prepared_statement_id),
            Datum::Float64((*sample_rate).into()),
            match &cluster {
                None => Datum::Null,
                Some(cluster_id) => Datum::String(cluster_id),
            },
            Datum::String(&*application_name),
            cluster_name.as_ref().map(String::as_str).into(),
            Datum::String(&*transaction_isolation),
            (*execution_timestamp).into(),
            Datum::UInt64(*transaction_id),
            match &transient_index_id {
                None => Datum::Null,
                Some(transient_index_id) => Datum::String(transient_index_id),
            },
        ]);
        packer
            .push_array(
                &[ArrayDimension {
                    lower_bound: 1,
                    length: params.len(),
                }],
                params
                    .iter()
                    .map(|p| Datum::from(p.as_ref().map(String::as_str))),
            )
            .expect("correct array dimensions");
        packer.push(Datum::from(mz_version.as_str()));
        packer.push(Datum::TimestampTz(
            to_datetime(*began_at).try_into().expect("Sane system time"),
        ));
    }

    fn pack_statement_began_execution_update(record: &StatementBeganExecutionRecord) -> Row {
        let mut row = Row::default();
        let mut packer = row.packer();
        Self::pack_statement_execution_inner(record, &mut packer);
        packer.extend([
            // finished_at
            Datum::Null,
            // finished_status
            Datum::Null,
            // error_message
            Datum::Null,
            // rows_returned
            Datum::Null,
            // execution_status
            Datum::Null,
        ]);
        row
    }

    fn pack_statement_prepared_update(record: &StatementPreparedRecord, packer: &mut RowPacker) {
        let StatementPreparedRecord {
            id,
            session_id,
            name,
            sql_hash,
            prepared_at,
            kind,
        } = record;
        packer.extend([
            Datum::Uuid(*id),
            Datum::Uuid(*session_id),
            Datum::String(name.as_str()),
            Datum::Bytes(sql_hash.as_slice()),
            Datum::TimestampTz(to_datetime(*prepared_at).try_into().expect("must fit")),
            kind.map(statement_kind_label_value).into(),
        ]);
    }

    fn pack_session_history_update(event: &SessionHistoryEvent) -> Row {
        let SessionHistoryEvent {
            id,
            connected_at,
            application_name,
            authenticated_user,
        } = event;
        Row::pack_slice(&[
            Datum::Uuid(*id),
            Datum::TimestampTz(
                mz_ore::now::to_datetime(*connected_at)
                    .try_into()
                    .expect("must fit"),
            ),
            Datum::String(&*application_name),
            Datum::String(&*authenticated_user),
        ])
    }

    fn pack_statement_lifecycle_event(
        StatementLoggingId(uuid): &StatementLoggingId,
        event: &StatementLifecycleEvent,
        when: EpochMillis,
    ) -> Row {
        Row::pack_slice(&[
            Datum::Uuid(*uuid),
            Datum::String(event.as_str()),
            Datum::TimestampTz(mz_ore::now::to_datetime(when).try_into().expect("must fit")),
        ])
    }

    pub fn pack_full_statement_execution_update(
        began_record: &StatementBeganExecutionRecord,
        ended_record: &StatementEndedExecutionRecord,
    ) -> Row {
        let mut row = Row::default();
        let mut packer = row.packer();
        Self::pack_statement_execution_inner(began_record, &mut packer);
        let (status, error_message, rows_returned, execution_strategy) = match &ended_record.reason
        {
            StatementEndedExecutionReason::Success {
                rows_returned,
                execution_strategy,
            } => (
                "success",
                None,
                rows_returned.map(|rr| i64::try_from(rr).expect("must fit")),
                execution_strategy.map(|es| es.name()),
            ),
            StatementEndedExecutionReason::Canceled => ("canceled", None, None, None),
            StatementEndedExecutionReason::Errored { error } => {
                ("error", Some(error.as_str()), None, None)
            }
            StatementEndedExecutionReason::Aborted => ("aborted", None, None, None),
        };
        packer.extend([
            Datum::TimestampTz(
                to_datetime(ended_record.ended_at)
                    .try_into()
                    .expect("Sane system time"),
            ),
            status.into(),
            error_message.into(),
            rows_returned.into(),
            execution_strategy.into(),
        ]);
        row
    }

    pub fn pack_statement_ended_execution_updates(
        began_record: &StatementBeganExecutionRecord,
        ended_record: &StatementEndedExecutionRecord,
    ) -> Vec<(Row, Diff)> {
        let retraction = Self::pack_statement_began_execution_update(began_record);
        let new = Self::pack_full_statement_execution_update(began_record, ended_record);
        vec![(retraction, -1), (new, 1)]
    }

    /// Mutate a statement execution record via the given function `f`.
    fn mutate_record<F: FnOnce(&mut StatementBeganExecutionRecord)>(
        &mut self,
        StatementLoggingId(id): StatementLoggingId,
        f: F,
    ) {
        let record = self
            .statement_logging
            .executions_begun
            .get_mut(&id)
            .expect("mutate_record must not be called after execution ends");
        let retraction = Self::pack_statement_began_execution_update(record);
        self.statement_logging
            .pending_statement_execution_events
            .push((retraction, -1));
        f(record);
        let update = Self::pack_statement_began_execution_update(record);
        self.statement_logging
            .pending_statement_execution_events
            .push((update, 1));
    }

    /// Set the `cluster_id` for a statement, once it's known.
    pub fn set_statement_execution_cluster(
        &mut self,
        id: StatementLoggingId,
        cluster_id: ClusterId,
    ) {
        let cluster_name = self.catalog().get_cluster(cluster_id).name.clone();
        self.mutate_record(id, |record| {
            record.cluster_name = Some(cluster_name);
            record.cluster_id = Some(cluster_id);
        });
    }

    /// Set the `execution_timestamp` for a statement, once it's known
    pub fn set_statement_execution_timestamp(
        &mut self,
        id: StatementLoggingId,
        timestamp: Timestamp,
    ) {
        self.mutate_record(id, |record| {
            record.execution_timestamp = Some(u64::from(timestamp));
        });
    }

    pub fn set_transient_index_id(&mut self, id: StatementLoggingId, transient_index_id: GlobalId) {
        self.mutate_record(id, |record| {
            record.transient_index_id = Some(transient_index_id)
        });
    }

    /// Possibly record the beginning of statement execution, depending on a randomly-chosen value.
    /// If the execution beginning was indeed logged, returns a `StatementLoggingId` that must be
    /// passed to `end_statement_execution` to record when it ends.
    pub fn begin_statement_execution(
        &mut self,
        session: &mut Session,
        params: Params,
        logging: &Arc<QCell<PreparedStatementLoggingInfo>>,
    ) -> Option<StatementLoggingId> {
        if session.user().is_internal() {
            return None;
        }
        let sample_rate = self.statement_execution_sample_rate(session);

        let distribution = Bernoulli::new(sample_rate).expect("rate must be in range [0, 1]");
        let sample = if self
            .catalog()
            .system_config()
            .statement_logging_use_reproducible_rng()
        {
            distribution.sample(&mut self.statement_logging.reproducible_rng)
        } else {
            distribution.sample(&mut thread_rng())
        };
        if let Some((sql, accounted)) = match session.qcell_rw(logging) {
            PreparedStatementLoggingInfo::AlreadyLogged { .. } => None,
            PreparedStatementLoggingInfo::StillToLog { sql, accounted, .. } => {
                Some((sql, accounted))
            }
        } {
            if !*accounted {
                self.metrics
                    .statement_logging_unsampled_bytes
                    .with_label_values(&[])
                    .inc_by(u64::cast_from(sql.len()));
                if sample {
                    self.metrics
                        .statement_logging_actual_bytes
                        .with_label_values(&[])
                        .inc_by(u64::cast_from(sql.len()));
                }
                *accounted = true;
            }
        }
        if !sample {
            return None;
        }
        let (ps_record, ps_uuid) = self.log_prepared_statement(session, logging)?;

        let ev_id = Uuid::new_v4();
        let now = self.now();
        self.record_statement_lifecycle_event(
            &StatementLoggingId(ev_id),
            &StatementLifecycleEvent::ExecutionBegan,
            now,
        );

        let params = std::iter::zip(params.types.iter(), params.datums.iter())
            .map(|(r#type, datum)| {
                mz_pgrepr::Value::from_datum(datum, r#type).map(|val| {
                    let mut buf = BytesMut::new();
                    val.encode_text(&mut buf);
                    String::from_utf8(Into::<Vec<u8>>::into(buf))
                        .expect("Serialization shouldn't produce non-UTF-8 strings.")
                })
            })
            .collect();
        let record = StatementBeganExecutionRecord {
            id: ev_id,
            prepared_statement_id: ps_uuid,
            sample_rate,
            params,
            began_at: self.now(),
            application_name: session.application_name().to_string(),
            transaction_isolation: session.vars().transaction_isolation().to_string(),
            transaction_id: session
                .transaction()
                .inner()
                .expect("Every statement runs in an explicit or implicit transaction")
                .id,
            mz_version: self.catalog().state().config().build_info.human_version(),
            // These are not known yet; we'll fill them in later.
            cluster_id: None,
            cluster_name: None,
            execution_timestamp: None,
            transient_index_id: None,
        };
        let mseh_update = Self::pack_statement_began_execution_update(&record);
        self.statement_logging
            .pending_statement_execution_events
            .push((mseh_update, 1));
        self.statement_logging
            .executions_begun
            .insert(ev_id, record);
        if let Some((ps_record, ps_update)) = ps_record {
            self.statement_logging
                .pending_prepared_statement_events
                .push(ps_update);
            if let Some(sh) = self
                .statement_logging
                .unlogged_sessions
                .remove(&ps_record.session_id)
            {
                let sh_update = Self::pack_session_history_update(&sh);
                self.statement_logging
                    .pending_session_events
                    .push(sh_update);
            }
        }
        Some(StatementLoggingId(ev_id))
    }

    /// Record a new connection event
    pub fn begin_session_for_statement_logging(&mut self, session: &ConnMeta) {
        let id = session.uuid();
        let session_role = session.authenticated_role_id();
        let event = SessionHistoryEvent {
            id,
            connected_at: session.connected_at(),
            application_name: session.application_name().to_owned(),
            authenticated_user: self.catalog.get_role(session_role).name.clone(),
        };
        self.statement_logging.unlogged_sessions.insert(id, event);
    }

    pub fn end_session_for_statement_logging(&mut self, uuid: Uuid) {
        self.statement_logging.unlogged_sessions.remove(&uuid);
    }

    pub fn record_statement_lifecycle_event(
        &mut self,
        id: &StatementLoggingId,
        event: &StatementLifecycleEvent,
        when: EpochMillis,
    ) {
        if self
            .catalog()
            .system_config()
            .enable_statement_lifecycle_logging()
        {
            let row = Self::pack_statement_lifecycle_event(id, event, when);
            self.statement_logging
                .pending_statement_lifecycle_events
                .push(row);
        }
    }
}
