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
use mz_ore::now::{NowFn, epoch_to_uuid_v7, to_datetime};
use mz_ore::task::spawn;
use mz_ore::{cast::CastFrom, cast::CastInto, now::EpochMillis};
use mz_repr::adt::array::ArrayDimension;
use mz_repr::adt::timestamp::TimestampLike;
use mz_repr::{Datum, Diff, GlobalId, Row, RowPacker, Timestamp};
use mz_sql::ast::display::AstDisplay;
use mz_sql::ast::{AstInfo, Statement};
use mz_sql::plan::Params;
use mz_sql::session::metadata::SessionMetadata;
use mz_sql_parser::ast::{StatementKind, statement_kind_label_value};
use mz_storage_client::controller::IntrospectionType;
use qcell::QCell;
use rand::SeedableRng;
use rand::distr::{Bernoulli, Distribution};
use sha2::{Digest, Sha256};
use tokio::time::MissedTickBehavior;
use tracing::debug;
use uuid::Uuid;

use crate::coord::{ConnMeta, Coordinator};
use crate::session::{LifecycleTimestamps, Session};
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

        /// Private type that forces use of the [`PreparedStatementLoggingInfo::still_to_log`]
        /// constructor.
        _sealed: sealed::Private,
    },
}

impl PreparedStatementLoggingInfo {
    /// Constructor for the [`PreparedStatementLoggingInfo::StillToLog`] variant that ensures SQL
    /// statements are properly redacted.
    pub fn still_to_log<A: AstInfo>(
        raw_sql: String,
        stmt: Option<&Statement<A>>,
        prepared_at: EpochMillis,
        name: String,
        session_id: Uuid,
        accounted: bool,
    ) -> Self {
        let kind = stmt.map(StatementKind::from);
        let sql = match kind {
            // Always redact SQL statements that may contain sensitive information.
            // CREATE SECRET and ALTER SECRET statements can contain secret values, so we redact them.
            // INSERT, UPDATE, and EXECUTE statements can include large amounts of user data, so we redact them for both
            // data privacy and to avoid logging excessive data.
            Some(
                StatementKind::CreateSecret
                | StatementKind::AlterSecret
                | StatementKind::Insert
                | StatementKind::Update
                | StatementKind::Execute,
            ) => stmt.map(|s| s.to_ast_string_redacted()).unwrap_or_default(),
            _ => raw_sql,
        };

        PreparedStatementLoggingInfo::StillToLog {
            sql,
            redacted_sql: stmt.map(|s| s.to_ast_string_redacted()).unwrap_or_default(),
            prepared_at,
            name,
            session_id,
            accounted,
            kind,
            _sealed: sealed::Private,
        }
    }
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
    /// Only used by tests; otherwise, `rand::rng()` is used.
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
    /// due to throttling, and update the number of available tokens appropriately.
    ///
    /// Returns `false` if we must throttle this statement, and `true` otherwise.
    fn throttling_check(
        &mut self,
        cost: u64,
        target_data_rate: u64,
        max_data_credit: Option<u64>,
    ) -> bool {
        let ts = (self.now)() / 1000;
        // We use saturating_sub here because system time isn't monotonic, causing cases
        // when last_logged_ts_seconds is greater than ts.
        let elapsed = ts.saturating_sub(self.last_logged_ts_seconds);
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
            true
        } else {
            debug!(
                "throttling check failed. tokens available: {}; cost: {cost}",
                self.tokens
            );
            false
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
    pub(crate) fn drain_statement_log(&mut self) {
        let session_updates = std::mem::take(&mut self.statement_logging.pending_session_events)
            .into_iter()
            .map(|update| (update, Diff::ONE))
            .collect();
        let (prepared_statement_updates, sql_text_updates) =
            std::mem::take(&mut self.statement_logging.pending_prepared_statement_events)
                .into_iter()
                .map(
                    |PreparedStatementEvent {
                         prepared_statement,
                         sql_text,
                     }| {
                        ((prepared_statement, Diff::ONE), (sql_text, Diff::ONE))
                    },
                )
                .unzip::<_, _, Vec<_>, Vec<_>>();
        let statement_execution_updates =
            std::mem::take(&mut self.statement_logging.pending_statement_execution_events);
        let statement_lifecycle_updates =
            std::mem::take(&mut self.statement_logging.pending_statement_lifecycle_events)
                .into_iter()
                .map(|update| (update, Diff::ONE))
                .collect();

        use IntrospectionType::*;
        for (type_, updates) in [
            (SessionHistory, session_updates),
            (PreparedStatementHistory, prepared_statement_updates),
            (StatementExecutionHistory, statement_execution_updates),
            (StatementLifecycleHistory, statement_lifecycle_updates),
            (SqlText, sql_text_updates),
        ] {
            if !updates.is_empty() && !self.controller.read_only() {
                self.controller
                    .storage
                    .append_introspection_updates(type_, updates);
            }
        }
    }

    /// Check whether we need to do throttling (i.e., whether `STATEMENT_LOGGING_TARGET_DATA_RATE` is set).
    /// If so, actually do the check.
    ///
    /// We expect `rows` to be the list of rows we intend to record and calculate the cost by summing the
    /// byte lengths of the rows.
    ///
    /// Returns `false` if we must throttle this statement, and `true` otherwise.
    fn statement_logging_throttling_check<'a, I>(&mut self, rows: I) -> bool
    where
        I: IntoIterator<Item = Option<&'a Row>>,
    {
        let cost = rows
            .into_iter()
            .filter_map(|row_opt| row_opt.map(|row| row.byte_len()))
            .fold(0_usize, |acc, x| acc.saturating_add(x));

        let Some(target_data_rate) = self
            .catalog
            .system_config()
            .statement_logging_target_data_rate()
        else {
            return true;
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

    /// Marks a prepared statement as "already logged".
    /// Mutates the `PreparedStatementLoggingInfo` metadata.
    fn record_prepared_statement_as_logged(
        &self,
        uuid: Uuid,
        session: &mut Session,
        logging: &Arc<QCell<PreparedStatementLoggingInfo>>,
    ) {
        let logging = session.qcell_rw(&*logging);
        if let PreparedStatementLoggingInfo::StillToLog { .. } = logging {
            *logging = PreparedStatementLoggingInfo::AlreadyLogged { uuid };
        }
    }

    /// Returns any statement logging events needed for a particular
    /// prepared statement. Possibly mutates the `PreparedStatementLoggingInfo` metadata.
    ///
    /// This function does not do a sampling check, and assumes we did so in a higher layer.
    ///
    ///
    /// Returns A tuple containing:
    /// - `Option<(StatementPreparedRecord, PreparedStatementEvent)>`: If the prepared statement
    ///   has not yet been logged, returns the prepared statement record, the packed row of the
    ///   prepared statement record, and a row for the SQL text.
    /// - `Uuid`: The UUID of the prepared statement if the prepared statement has been logged
    pub(crate) fn get_prepared_statement_info(
        &self,
        session: &Session,
        logging: &Arc<QCell<PreparedStatementLoggingInfo>>,
    ) -> (
        Option<(StatementPreparedRecord, PreparedStatementEvent)>,
        Uuid,
    ) {
        let logging = session.qcell_ro(&*logging);

        match logging {
            PreparedStatementLoggingInfo::AlreadyLogged { uuid } => (None, *uuid),
            PreparedStatementLoggingInfo::StillToLog {
                sql,
                redacted_sql,
                prepared_at,
                name,
                session_id,
                accounted,
                kind,
                _sealed: _,
            } => {
                assert!(
                    *accounted,
                    "accounting for logging should be done in `begin_statement_execution`"
                );
                let uuid = epoch_to_uuid_v7(prepared_at);
                let sql_hash: [u8; 32] = Sha256::digest(sql.as_bytes()).into();
                let record = StatementPreparedRecord {
                    id: uuid,
                    sql_hash,
                    name: name.to_string(),
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

                let throttled_count = self.statement_logging.throttled_count;
                mpsh_packer.push(Datum::UInt64(throttled_count.try_into().expect("must fit")));

                (
                    Some((
                        record,
                        PreparedStatementEvent {
                            prepared_statement: mpsh_row,
                            sql_text: sql_row,
                        },
                    )),
                    uuid,
                )
            }
        }
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
            database_name,
            search_path,
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
            Datum::String(database_name),
        ]);
        packer.push_list(search_path.iter().map(|s| Datum::String(s)));
        packer.extend([
            Datum::String(&*transaction_isolation),
            (*execution_timestamp).into(),
            Datum::UInt64(*transaction_id),
            match &transient_index_id {
                None => Datum::Null,
                Some(transient_index_id) => Datum::String(transient_index_id),
            },
        ]);
        packer
            .try_push_array(
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
            // result_size
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
        let (status, error_message, result_size, rows_returned, execution_strategy) =
            match &ended_record.reason {
                StatementEndedExecutionReason::Success {
                    result_size,
                    rows_returned,
                    execution_strategy,
                } => (
                    "success",
                    None,
                    result_size.map(|rs| i64::try_from(rs).expect("must fit")),
                    rows_returned.map(|rr| i64::try_from(rr).expect("must fit")),
                    execution_strategy.map(|es| es.name()),
                ),
                StatementEndedExecutionReason::Canceled => ("canceled", None, None, None, None),
                StatementEndedExecutionReason::Errored { error } => {
                    ("error", Some(error.as_str()), None, None, None)
                }
                StatementEndedExecutionReason::Aborted => ("aborted", None, None, None, None),
            };
        packer.extend([
            Datum::TimestampTz(
                to_datetime(ended_record.ended_at)
                    .try_into()
                    .expect("Sane system time"),
            ),
            status.into(),
            error_message.into(),
            result_size.into(),
            rows_returned.into(),
            execution_strategy.into(),
        ]);
        row
    }

    pub fn pack_statement_ended_execution_updates(
        began_record: &StatementBeganExecutionRecord,
        ended_record: &StatementEndedExecutionRecord,
    ) -> [(Row, Diff); 2] {
        let retraction = Self::pack_statement_began_execution_update(began_record);
        let new = Self::pack_full_statement_execution_update(began_record, ended_record);
        [(retraction, Diff::MINUS_ONE), (new, Diff::ONE)]
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
            .push((retraction, Diff::MINUS_ONE));
        f(record);
        let update = Self::pack_statement_began_execution_update(record);
        self.statement_logging
            .pending_statement_execution_events
            .push((update, Diff::ONE));
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
    ///
    /// `lifecycle_timestamps` has timestamps that come from the Adapter frontend (`mz-pgwire`) part
    /// of the lifecycle.
    pub fn begin_statement_execution(
        &mut self,
        session: &mut Session,
        params: &Params,
        logging: &Arc<QCell<PreparedStatementLoggingInfo>>,
        lifecycle_timestamps: Option<LifecycleTimestamps>,
    ) -> Option<StatementLoggingId> {
        let enable_internal_statement_logging = self
            .catalog()
            .system_config()
            .enable_internal_statement_logging();
        if session.user().is_internal() && !enable_internal_statement_logging {
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
            distribution.sample(&mut rand::rng())
        };

        // Figure out the cost of everything before we log.

        // Track how many statements we're recording.
        let sampled_label = sample.then_some("true").unwrap_or("false");
        self.metrics
            .statement_logging_records
            .with_label_values(&[sampled_label])
            .inc_by(1);

        if let Some((sql, accounted)) = match session.qcell_rw(logging) {
            PreparedStatementLoggingInfo::AlreadyLogged { .. } => None,
            PreparedStatementLoggingInfo::StillToLog { sql, accounted, .. } => {
                Some((sql, accounted))
            }
        } {
            if !*accounted {
                self.metrics
                    .statement_logging_unsampled_bytes
                    .inc_by(u64::cast_from(sql.len()));
                if sample {
                    self.metrics
                        .statement_logging_actual_bytes
                        .inc_by(u64::cast_from(sql.len()));
                }
                *accounted = true;
            }
        }
        if !sample {
            return None;
        }

        let (maybe_ps, ps_uuid) = self.get_prepared_statement_info(session, logging);

        let began_at = if let Some(lifecycle_timestamps) = lifecycle_timestamps {
            lifecycle_timestamps.received
        } else {
            self.now()
        };
        let now = self.now();
        let execution_uuid = epoch_to_uuid_v7(&now);

        let params = std::iter::zip(params.execute_types.iter(), params.datums.iter())
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
            id: execution_uuid,
            prepared_statement_id: ps_uuid,
            sample_rate,
            params,
            began_at,
            application_name: session.application_name().to_string(),
            transaction_isolation: session.vars().transaction_isolation().to_string(),
            transaction_id: session
                .transaction()
                .inner()
                .expect("Every statement runs in an explicit or implicit transaction")
                .id,
            mz_version: self
                .catalog()
                .state()
                .config()
                .build_info
                .human_version(None),
            // These are not known yet; we'll fill them in later.
            cluster_id: None,
            cluster_name: None,
            execution_timestamp: None,
            transient_index_id: None,
            database_name: session.vars().database().into(),
            search_path: session
                .vars()
                .search_path()
                .iter()
                .map(|s| s.as_str().to_string())
                .collect(),
        };
        let mseh_update = Self::pack_statement_began_execution_update(&record);

        let (maybe_ps_event, maybe_sh_event) = if let Some((ps_record, ps_event)) = maybe_ps {
            if let Some(sh) = self
                .statement_logging
                .unlogged_sessions
                .get(&ps_record.session_id)
            {
                (
                    Some(ps_event),
                    Some((Self::pack_session_history_update(sh), ps_record.session_id)),
                )
            } else {
                (Some(ps_event), None)
            }
        } else {
            (None, None)
        };

        let maybe_ps_prepared_statement = maybe_ps_event.as_ref().map(|e| &e.prepared_statement);
        let maybe_ps_sql_text = maybe_ps_event.as_ref().map(|e| &e.sql_text);

        if !self.statement_logging_throttling_check([
            Some(&mseh_update),
            maybe_ps_prepared_statement,
            maybe_ps_sql_text,
            maybe_sh_event.as_ref().map(|(row, _)| row),
        ]) {
            self.statement_logging.throttled_count += 1;
            return None;
        }
        // When we successfully log the first instance of a prepared statement
        // (i.e., it is not throttled), we also capture the number of previously
        // throttled statement executions in the builtin prepared statement history table above,
        // and then reset the throttled count for future tracking.
        else if let PreparedStatementLoggingInfo::StillToLog { .. } = session.qcell_ro(logging) {
            self.statement_logging.throttled_count = 0;
        }

        self.record_prepared_statement_as_logged(ps_uuid, session, logging);

        self.record_statement_lifecycle_event(
            &StatementLoggingId(execution_uuid),
            &StatementLifecycleEvent::ExecutionBegan,
            began_at,
        );

        self.statement_logging
            .pending_statement_execution_events
            .push((mseh_update, Diff::ONE));
        self.statement_logging
            .executions_begun
            .insert(execution_uuid, record);

        if let Some((sh_update, session_id)) = maybe_sh_event {
            self.statement_logging
                .pending_session_events
                .push(sh_update);
            // Mark the session as logged to avoid logging it again in the future
            self.statement_logging.unlogged_sessions.remove(&session_id);
        }
        if let Some(ps_event) = maybe_ps_event {
            self.statement_logging
                .pending_prepared_statement_events
                .push(ps_event);
        }
        Some(StatementLoggingId(execution_uuid))
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
        if mz_adapter_types::dyncfgs::ENABLE_STATEMENT_LIFECYCLE_LOGGING
            .get(self.catalog().system_config().dyncfgs())
        {
            let row = Self::pack_statement_lifecycle_event(id, event, when);
            self.statement_logging
                .pending_statement_lifecycle_events
                .push(row);
        }
    }
}

mod sealed {
    /// A struct that is purposefully private so folks are forced to use the constructor of an
    /// enum.
    #[derive(Debug, Copy, Clone)]
    pub struct Private;
}
