use std::sync::Arc;

use bytes::BytesMut;
use chrono::{DateTime, TimeZone, Utc};
use mz_ore::{cast::CastFrom, now::EpochMillis};
use mz_repr::statement_logging::{
    StatementBeganExecutionRecord, StatementEndedExecutionReason, StatementEndedExecutionRecord,
    StatementLoggingEvent, StatementPreparedRecord,
};
use mz_sql::plan::Params;
use mz_stash::TypedCollection;
use qcell::QCell;
use rand::{distributions::Bernoulli, prelude::Distribution, thread_rng};
use uuid::Uuid;

use crate::{
    catalog::{BuiltinTableUpdate, CatalogState},
    session::Session,
    AdapterError, ExecuteResponse,
};

use super::{Coordinator, Message};

pub const STATEMENT_LOGGING_EVENTS_COLLECTION: TypedCollection<
    mz_stash::objects::proto::StatementLoggingEvent,
    (),
    > = TypedCollection::new("statement_logging_events");

/// Metadata required for statement logging.
#[derive(Debug)]
pub enum StatementLogging {
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
    pub(crate) fn schedule_statement_log_flush(&mut self) {
        // We create a new task every time and use
        // `tokio::time:sleep`, rather than just awaiting an Interval
        // or Timer in a loop, so that the sleep interval will be updated if we
        // change the system var.
        let interval = self.catalog().system_config().statement_logging_flush_interval();
        let internal_cmd_tx = self.internal_cmd_tx.clone();
        mz_ore::task::spawn(|| "statement_log_flush", async move {
            tokio::time::sleep(interval).await;
            let _ = internal_cmd_tx.send(Message::DrainStatementLog).is_err(); 
        });
    }
    /// Returns any statement logging events needed for a particular
    /// prepared statement. Possibly mutates the `StatementLogging` metadata.
    ///
    /// This function does not do a sampling check, and assumes we did so in a higher layer.
    pub(crate) fn log_prepared_statement(
        &mut self,
        session: &mut Session,
        logging: &Arc<QCell<StatementLogging>>,
    ) -> (Option<StatementPreparedRecord>, Uuid) {
        let logging = session.qcell_rw(&*logging);
        let mut out = None;

        let uuid = match logging {
            StatementLogging::AlreadyLogged { uuid } => *uuid,
            StatementLogging::StillToLog {
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

                *logging = StatementLogging::AlreadyLogged { uuid };
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
        let now = Utc::now();
        let now_millis = now.timestamp_millis().try_into().expect("sane system time");
        let ended_record = StatementEndedExecutionRecord {
                    id,
                    reason: reason.clone(),
                    ended_at: now_millis,
                };
        self.statement_logging_pending_events
            .push(StatementLoggingEvent::EndedExecution(
                ended_record.clone()
            ));
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
        logging: &Arc<QCell<StatementLogging>>,
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
            StatementLogging::AlreadyLogged { .. } => None,
            StatementLogging::StillToLog { sql, accounted, .. } => Some((sql, accounted)),
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
            began_at: Utc::now()
                .timestamp_millis()
                .try_into()
                .expect("sane system time"),
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
            vec![
                self.catalog
                    .state()
                    .pack_statement_prepared_update(&ps_record),
                ev,
            ]
        } else {
            vec![ev]
        };
        self.buffer_builtin_table_updates(updates);
        Some(StatementLoggingId(ev_id))
    }
}
