// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use bytes::BytesMut;
use mz_ore::{cast::CastFrom, now::EpochMillis};
use mz_repr::statement_logging::{
    SessionHistoryEvent, StatementBeganExecutionRecord, StatementEndedExecutionReason,
    StatementEndedExecutionRecord, StatementLoggingEvent, StatementPreparedRecord,
};
use mz_sql::plan::Params;
use qcell::QCell;
use rand::{distributions::Bernoulli, prelude::Distribution, thread_rng};
use uuid::Uuid;

use crate::coord::Coordinator;
use crate::session::Session;

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

#[derive(Debug, Ord, Eq, PartialOrd, PartialEq)]
pub struct StatementLoggingId(Uuid);

impl Coordinator {
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
        self.statement_logging_pending_events
            .push(StatementLoggingEvent::EndedExecution(ended_record.clone()));
        let began_record = self.statement_logging_executions_begun.remove(&id).expect(
            "matched `begin_statement_execution` and `end_statement_execution` invocations",
        );
        let updates = self
            .catalog
            .state()
            .pack_statement_ended_execution_updates(&began_record, &ended_record);
        self.buffer_builtin_table_updates(updates);
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
        let sample_rate = self.statement_execution_sample_rate(session);

        let distribution = Bernoulli::new(sample_rate).expect("rate must be in range [0, 1]");
        let sample = if self
            .catalog()
            .system_config()
            .statement_logging_use_reproducible_rng()
        {
            distribution.sample(&mut self.statement_logging_reproducible_rng)
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
        };
        self.statement_logging_pending_events
            .push(StatementLoggingEvent::BeganExecution(record.clone()));
        let ev = self
            .catalog
            .state()
            .pack_statement_began_execution_update(&record, 1);
        self.statement_logging_executions_begun
            .insert(ev_id, record);
        let updates = if let Some(ps_record) = ps_record {
            self.statement_logging_pending_events
                .push(StatementLoggingEvent::Prepared(ps_record.clone()));
            let ps_ev = self
                .catalog
                .state()
                .pack_statement_prepared_update(&ps_record);
            if let Some(sh) = self
                .statement_logging_unlogged_sessions
                .remove(&ps_record.session_id)
            {
                let sh_ev = self.catalog.state().pack_session_history_update(&sh);
                self.statement_logging_pending_events
                    .push(StatementLoggingEvent::BeganSession(sh));
                vec![sh_ev, ps_ev, ev]
            } else {
                vec![ps_ev, ev]
            }
        } else {
            vec![ev]
        };
        self.buffer_builtin_table_updates(updates);
        Some(StatementLoggingId(ev_id))
    }

    /// Record a new connection event
    pub fn begin_session(&mut self, session: &Session) {
        let id = session.uuid();
        let connected_at = session.connect_time();
        let application_name = session.application_name().to_owned();
        let session_role = session.session_role_id();
        let authenticated_user = self.catalog.get_role(session_role).name.clone();
        let event = SessionHistoryEvent {
            id,
            connected_at,
            application_name,
            authenticated_user,
        };
        self.statement_logging_unlogged_sessions.insert(id, event);
    }
}
