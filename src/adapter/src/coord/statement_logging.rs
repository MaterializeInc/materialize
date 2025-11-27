// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mz_adapter_types::connection::ConnectionId;
use mz_compute_client::controller::error::CollectionLookupError;
use mz_controller_types::ClusterId;
use mz_ore::now::{EpochMillis, NowFn, epoch_to_uuid_v7, to_datetime};
use mz_ore::task::spawn;
use mz_ore::{cast::CastFrom, cast::CastInto};
use mz_repr::adt::timestamp::TimestampLike;
use mz_repr::{Datum, Diff, GlobalId, Row, Timestamp};
use mz_sql::plan::Params;
use mz_sql::session::metadata::SessionMetadata;
use mz_storage_client::controller::IntrospectionType;
use qcell::QCell;
use rand::SeedableRng;
use sha2::{Digest, Sha256};
use tokio::time::MissedTickBehavior;
use uuid::Uuid;

use crate::coord::{ConnMeta, Coordinator, WatchSetResponse};
use crate::session::{LifecycleTimestamps, Session};
use crate::statement_logging::{
    FrontendStatementLoggingEvent, PreparedStatementEvent, PreparedStatementLoggingInfo,
    SessionHistoryEvent, StatementBeganExecutionRecord, StatementEndedExecutionReason,
    StatementEndedExecutionRecord, StatementLifecycleEvent, StatementLoggingFrontend,
    StatementLoggingId, StatementPreparedRecord, ThrottlingState, WatchSetCreation,
    create_began_execution_record, effective_sample_rate, pack_statement_began_execution_update,
    pack_statement_execution_inner, pack_statement_prepared_update, should_sample_statement,
};

use super::Message;

/// Statement logging state in the Coordinator.
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
    /// This same instance will be used by all frontend tasks.
    reproducible_rng: Arc<Mutex<rand_chacha::ChaCha8Rng>>,

    /// Events to be persisted periodically.
    pending_statement_execution_events: Vec<(Row, Diff)>,
    pending_prepared_statement_events: Vec<PreparedStatementEvent>,
    pending_session_events: Vec<Row>,
    pending_statement_lifecycle_events: Vec<Row>,

    /// Shared throttling state for rate-limiting statement logging.
    pub(crate) throttling_state: Arc<ThrottlingState>,

    /// Function to get the current time.
    pub(crate) now: NowFn,
}

impl StatementLogging {
    const REPRODUCIBLE_RNG_SEED: u64 = 42;

    pub(crate) fn new(now: NowFn) -> Self {
        Self {
            executions_begun: BTreeMap::new(),
            unlogged_sessions: BTreeMap::new(),
            reproducible_rng: Arc::new(Mutex::new(rand_chacha::ChaCha8Rng::seed_from_u64(
                Self::REPRODUCIBLE_RNG_SEED,
            ))),
            pending_statement_execution_events: Vec::new(),
            pending_prepared_statement_events: Vec::new(),
            pending_session_events: Vec::new(),
            pending_statement_lifecycle_events: Vec::new(),
            throttling_state: Arc::new(ThrottlingState::new(&now)),
            now,
        }
    }

    /// Create a `StatementLoggingFrontend` for use by frontend peek sequencing.
    ///
    /// This provides the frontend with all the state it needs to perform statement
    /// logging without direct access to the Coordinator.
    pub(crate) fn create_frontend(
        &self,
        build_info_human_version: String,
    ) -> StatementLoggingFrontend {
        StatementLoggingFrontend {
            throttling_state: Arc::clone(&self.throttling_state),
            reproducible_rng: Arc::clone(&self.reproducible_rng),
            build_info_human_version,
            now: self.now.clone(),
        }
    }
}

impl Coordinator {
    /// Helper to write began execution events to pending buffers.
    /// Can be called from both old and new peek sequencing.
    fn write_began_execution_events(
        &mut self,
        record: StatementBeganExecutionRecord,
        mseh_update: Row,
        prepared_statement: Option<PreparedStatementEvent>,
    ) {
        // `mz_statement_execution_history`
        self.statement_logging
            .pending_statement_execution_events
            .push((mseh_update, Diff::ONE));

        // Track the execution for later updates
        self.statement_logging
            .executions_begun
            .insert(record.id, record);

        // If we have a prepared statement, log it and possibly its session
        if let Some(ps_event) = prepared_statement {
            let session_id = ps_event.session_id;
            self.statement_logging
                .pending_prepared_statement_events
                .push(ps_event);

            // Check if we need to log the session for this prepared statement
            if let Some(sh) = self.statement_logging.unlogged_sessions.remove(&session_id) {
                let sh_update = Self::pack_session_history_update(&sh);
                self.statement_logging
                    .pending_session_events
                    .push(sh_update);
            }
        }
    }

    /// Handle a statement logging event from frontend peek sequencing.
    pub(crate) fn handle_frontend_statement_logging_event(
        &mut self,
        event: FrontendStatementLoggingEvent,
    ) {
        match event {
            FrontendStatementLoggingEvent::BeganExecution {
                record,
                mseh_update,
                prepared_statement,
            } => {
                self.record_statement_lifecycle_event(
                    &StatementLoggingId(record.id),
                    &StatementLifecycleEvent::ExecutionBegan,
                    record.began_at,
                );
                self.write_began_execution_events(record, mseh_update, prepared_statement);
            }
            FrontendStatementLoggingEvent::EndedExecution(ended_record) => {
                self.end_statement_execution(
                    StatementLoggingId(ended_record.id),
                    ended_record.reason,
                );
            }
            FrontendStatementLoggingEvent::SetCluster { id, cluster_id } => {
                self.set_statement_execution_cluster(id, cluster_id);
            }
            FrontendStatementLoggingEvent::SetTimestamp { id, timestamp } => {
                self.set_statement_execution_timestamp(id, timestamp);
            }
            FrontendStatementLoggingEvent::SetTransientIndex {
                id,
                transient_index_id,
            } => {
                self.set_transient_index_id(id, transient_index_id);
            }
            FrontendStatementLoggingEvent::Lifecycle { id, event, when } => {
                self.record_statement_lifecycle_event(&id, &event, when);
            }
        }
    }

    // TODO[btv] make this configurable via LD?
    // Although... Logging every 5 seconds seems like it
    // should have acceptable cost for now, since we do a
    // group commit for tables every 1s anyway.
    const STATEMENT_LOGGING_WRITE_INTERVAL: Duration = Duration::from_secs(5);

    pub(crate) fn spawn_statement_logging_task(&self) {
        let internal_cmd_tx = self.internal_cmd_tx.clone();
        spawn(|| "statement_logging", async move {
            let mut interval = tokio::time::interval(Coordinator::STATEMENT_LOGGING_WRITE_INTERVAL);
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
                         ..
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
    fn statement_logging_throttling_check<'a, I>(&self, rows: I) -> bool
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

        self.statement_logging.throttling_state.throttling_check(
            cost.cast_into(),
            target_data_rate.cast_into(),
            max_data_credit.map(CastInto::cast_into),
            &self.statement_logging.now,
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
    /// prepared statement. This is a read-only operation that does not mutate
    /// the `PreparedStatementLoggingInfo` metadata.
    ///
    /// This function does not do a sampling check, and assumes we did so in a higher layer.
    /// It also does not do a throttling check - that is done separately in `begin_statement_execution`.
    ///
    /// Returns a tuple containing:
    /// - `Option<(StatementPreparedRecord, PreparedStatementEvent)>`: If the prepared statement
    ///   has not yet been logged, returns the prepared statement record and the packed rows.
    /// - `Uuid`: The UUID of the prepared statement.
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

                // `mz_prepared_statement_history`
                let mut mpsh_row = Row::default();
                let mut mpsh_packer = mpsh_row.packer();
                pack_statement_prepared_update(&record, &mut mpsh_packer);
                let throttled_count = self
                    .statement_logging
                    .throttling_state
                    .get_throttled_count();
                mpsh_packer.push(Datum::UInt64(CastFrom::cast_from(throttled_count)));

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

                (
                    Some((
                        record,
                        PreparedStatementEvent {
                            prepared_statement: mpsh_row,
                            sql_text: sql_row,
                            session_id: *session_id,
                        },
                    )),
                    uuid,
                )
            }
        }
    }

    /// Record the end of statement execution for a statement whose beginning was logged.
    /// It is an error to call this function for a statement whose beginning was not logged
    /// (because it was not sampled). Requiring the opaque `StatementLoggingId` type,
    /// which is only instantiated by `begin_statement_execution` if the statement is actually logged,
    /// should prevent this.
    pub(crate) fn end_statement_execution(
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

    fn pack_session_history_update(event: &SessionHistoryEvent) -> Row {
        let SessionHistoryEvent {
            id,
            connected_at,
            application_name,
            authenticated_user,
        } = event;
        Row::pack_slice(&[
            Datum::Uuid(*id),
            Datum::TimestampTz(to_datetime(*connected_at).try_into().expect("must fit")),
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
            Datum::TimestampTz(to_datetime(when).try_into().expect("must fit")),
        ])
    }

    fn pack_full_statement_execution_update(
        began_record: &StatementBeganExecutionRecord,
        ended_record: &StatementEndedExecutionRecord,
    ) -> Row {
        let mut row = Row::default();
        let mut packer = row.packer();
        pack_statement_execution_inner(began_record, &mut packer);
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

    fn pack_statement_ended_execution_updates(
        began_record: &StatementBeganExecutionRecord,
        ended_record: &StatementEndedExecutionRecord,
    ) -> [(Row, Diff); 2] {
        let retraction = pack_statement_began_execution_update(began_record);
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
        let retraction = pack_statement_began_execution_update(record);
        self.statement_logging
            .pending_statement_execution_events
            .push((retraction, Diff::MINUS_ONE));
        f(record);
        let update = pack_statement_began_execution_update(record);
        self.statement_logging
            .pending_statement_execution_events
            .push((update, Diff::ONE));
    }

    /// Set the `cluster_id` for a statement, once it's known.
    ///
    /// TODO(peek-seq): We could do cluster resolution and packing in the frontend task, and just
    /// send over the rows.
    pub(crate) fn set_statement_execution_cluster(
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
    pub(crate) fn set_statement_execution_timestamp(
        &mut self,
        id: StatementLoggingId,
        timestamp: Timestamp,
    ) {
        self.mutate_record(id, |record| {
            record.execution_timestamp = Some(u64::from(timestamp));
        });
    }

    pub(crate) fn set_transient_index_id(
        &mut self,
        id: StatementLoggingId,
        transient_index_id: GlobalId,
    ) {
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
    pub(crate) fn begin_statement_execution(
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

        let sample_rate = effective_sample_rate(session, self.catalog().system_config());
        let use_reproducible_rng = self
            .catalog()
            .system_config()
            .statement_logging_use_reproducible_rng();
        // Only lock the RNG when we actually need reproducible sampling (tests only)
        let sample = if use_reproducible_rng {
            let mut rng = self
                .statement_logging
                .reproducible_rng
                .lock()
                .expect("rng lock poisoned");
            should_sample_statement(sample_rate, Some(&mut *rng))
        } else {
            should_sample_statement(sample_rate, None)
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

        let build_info_version = self
            .catalog()
            .state()
            .config()
            .build_info
            .human_version(None);
        let record = create_began_execution_record(
            execution_uuid,
            ps_uuid,
            sample_rate,
            params,
            session,
            began_at,
            build_info_version,
        );

        // `mz_statement_execution_history`
        let mseh_update = pack_statement_began_execution_update(&record);

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
            // Increment throttled_count in shared state
            self.statement_logging
                .throttling_state
                .increment_throttled_count();
            return None;
        }
        // When we successfully log the first instance of a prepared statement
        // (i.e., it is not throttled), we also capture the number of previously
        // throttled statement executions in the builtin prepared statement history table above,
        // and then reset the throttled count for future tracking.
        else if let PreparedStatementLoggingInfo::StillToLog { .. } = session.qcell_ro(logging) {
            self.statement_logging
                .throttling_state
                .reset_throttled_count();
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
    pub(crate) fn begin_session_for_statement_logging(&mut self, session: &ConnMeta) {
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

    pub(crate) fn end_session_for_statement_logging(&mut self, uuid: Uuid) {
        self.statement_logging.unlogged_sessions.remove(&uuid);
    }

    pub(crate) fn record_statement_lifecycle_event(
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

    /// Install watch sets for statement lifecycle logging.
    ///
    /// This installs both storage and compute watch sets that will fire
    /// `StatementLifecycleEvent::StorageDependenciesFinished` and
    /// `StatementLifecycleEvent::ComputeDependenciesFinished` respectively
    /// when the dependencies are ready at the given timestamp.
    pub(crate) fn install_peek_watch_sets(
        &mut self,
        conn_id: ConnectionId,
        watch_set: WatchSetCreation,
    ) -> Result<(), CollectionLookupError> {
        let WatchSetCreation {
            logging_id,
            timestamp,
            storage_ids,
            compute_ids,
        } = watch_set;

        self.install_storage_watch_set(
            conn_id.clone(),
            storage_ids,
            timestamp,
            WatchSetResponse::StatementDependenciesReady(
                logging_id,
                StatementLifecycleEvent::StorageDependenciesFinished,
            ),
        )?;
        self.install_compute_watch_set(
            conn_id,
            compute_ids,
            timestamp,
            WatchSetResponse::StatementDependenciesReady(
                logging_id,
                StatementLifecycleEvent::ComputeDependenciesFinished,
            ),
        )?;
        Ok(())
    }
}
