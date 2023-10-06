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
use mz_ore::now::to_datetime;
use mz_ore::task::spawn;
use mz_ore::{cast::CastFrom, now::EpochMillis};
use mz_repr::adt::array::ArrayDimension;
use mz_repr::{Datum, Diff, Row, RowPacker};
use mz_sql::plan::Params;
use qcell::QCell;
use rand::SeedableRng;
use rand::{distributions::Bernoulli, prelude::Distribution, thread_rng};
use tokio::time::MissedTickBehavior;
use uuid::Uuid;

use crate::coord::{ConnMeta, Coordinator};
use crate::session::Session;
use crate::statement_logging::{
    SessionHistoryEvent, StatementBeganExecutionRecord, StatementEndedExecutionReason,
    StatementEndedExecutionRecord, StatementPreparedRecord,
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
    },
}

#[derive(Copy, Clone, Debug, Ord, Eq, PartialOrd, PartialEq)]
pub struct StatementLoggingId(Uuid);

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
    pending_prepared_statement_events: Vec<Row>,
    pending_session_events: Vec<Row>,
}

impl StatementLogging {
    pub(crate) fn new() -> Self {
        Self {
            executions_begun: BTreeMap::new(),
            unlogged_sessions: BTreeMap::new(),
            reproducible_rng: rand_chacha::ChaCha8Rng::seed_from_u64(42),
            pending_statement_execution_events: Vec::new(),
            pending_prepared_statement_events: Vec::new(),
            pending_session_events: Vec::new(),
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

    pub(crate) async fn drain_statement_log(&mut self) {
        let pending_session_events =
            std::mem::take(&mut self.statement_logging.pending_session_events);
        let pending_prepared_statement_events =
            std::mem::take(&mut self.statement_logging.pending_prepared_statement_events);
        let pending_statement_execution_events =
            std::mem::take(&mut self.statement_logging.pending_statement_execution_events);

        self.controller
            .storage
            .send_statement_log_updates(
                pending_statement_execution_events,
                pending_prepared_statement_events,
                pending_session_events,
            )
            .await;
    }

    /// Returns any statement logging events needed for a particular
    /// prepared statement. Possibly mutates the `PreparedStatementLoggingInfo` metadata.
    ///
    /// This function does not do a sampling check, and assumes we did so in a higher layer.
    pub(crate) fn log_prepared_statement(
        &mut self,
        session: &mut Session,
        logging: &Arc<QCell<PreparedStatementLoggingInfo>>,
    ) -> (Option<StatementPreparedRecord>, Uuid) {
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
            } => {
                assert!(
                    *accounted,
                    "accounting for logging should be done in `begin_statement_execution`"
                );
                let uuid = Uuid::new_v4();
                out = Some(StatementPreparedRecord {
                    id: uuid,
                    sql: std::mem::take(sql),
                    redacted_sql: std::mem::take(redacted_sql),
                    name: std::mem::take(name),
                    session_id: *session_id,
                    prepared_at: *prepared_at,
                });

                *logging = PreparedStatementLoggingInfo::AlreadyLogged { uuid };
                uuid
            }
        };
        (out, uuid)
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
        StatementLoggingId(id): StatementLoggingId,
        reason: StatementEndedExecutionReason,
    ) {
        let now = self.now_datetime();
        let now_millis = now.timestamp_millis().try_into().expect("sane system time");
        let ended_record = StatementEndedExecutionRecord {
            id,
            reason,
            ended_at: now_millis,
        };

        let began_record = self.statement_logging.executions_begun.remove(&id).expect(
            "matched `begin_statement_execution` and `end_statement_execution` invocations",
        );
        for (row, diff) in
            Self::pack_statement_ended_execution_updates(&began_record, &ended_record)
        {
            self.statement_logging
                .pending_statement_execution_events
                .push((row, diff));
        }
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
        } = record;

        let cluster = cluster_id.map(|id| id.to_string());
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

    fn pack_statement_prepared_update(record: &StatementPreparedRecord) -> Row {
        let StatementPreparedRecord {
            id,
            session_id,
            name,
            sql,
            redacted_sql,
            prepared_at,
        } = record;
        let row = Row::pack_slice(&[
            Datum::Uuid(*id),
            Datum::Uuid(*session_id),
            Datum::String(name.as_str()),
            Datum::String(sql.as_str()),
            Datum::String(redacted_sql.as_str()),
            Datum::TimestampTz(to_datetime(*prepared_at).try_into().expect("must fit")),
        ]);
        row
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

    /// Set the `cluster_id` for a statement, once it's known.
    pub fn set_statement_execution_cluster(
        &mut self,
        StatementLoggingId(id): StatementLoggingId,
        cluster_id: ClusterId,
    ) {
        let cluster_name = self.catalog().get_cluster(cluster_id).name.clone();
        let record = self
            .statement_logging
            .executions_begun
            .get_mut(&id)
            .expect("set_statement_execution_cluster must not be called after execution ends");
        if record.cluster_id == Some(cluster_id) {
            return;
        }
        let retraction = Self::pack_statement_began_execution_update(record);
        self.statement_logging
            .pending_statement_execution_events
            .push((retraction, -1));
        record.cluster_name = Some(cluster_name);
        record.cluster_id = Some(cluster_id);
        let update = Self::pack_statement_began_execution_update(record);
        self.statement_logging
            .pending_statement_execution_events
            .push((update, 1));
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

        let (ps_record, ps_uuid) = self.log_prepared_statement(session, logging);

        let ev_id = Uuid::new_v4();
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
            // Cluster is not known yet; we'll fill it in later
            cluster_id: None,
            cluster_name: None,
            application_name: session.application_name().to_string(),
        };
        let mseh_update = Self::pack_statement_began_execution_update(&record);
        self.statement_logging
            .pending_statement_execution_events
            .push((mseh_update, 1));
        self.statement_logging
            .executions_begun
            .insert(ev_id, record);
        if let Some(ps_record) = ps_record {
            let ps_update = Self::pack_statement_prepared_update(&ps_record);
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
}
