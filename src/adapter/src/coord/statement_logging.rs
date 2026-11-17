// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use mz_adapter_types::connection::ConnectionId;
use mz_compute_client::controller::error::CollectionLookupError;
use mz_controller_types::ClusterId;
use mz_dyncfg::ConfigSet;
use mz_ore::now::{EpochMillis, NowFn, epoch_to_uuid_v7, to_datetime};
use mz_ore::task::spawn;
use mz_ore::{cast::CastFrom, cast::CastInto};
use mz_repr::adt::timestamp::TimestampLike;
use mz_repr::{Datum, Row, Timestamp};
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
    PendingStatementLog, PreparedStatementEvent, PreparedStatementLoggingInfo, SessionHistoryEvent,
    StatementEndedExecutionReason, StatementLifecycleEvent, StatementLog, StatementLoggingFrontend,
    StatementLoggingId, StatementPreparedRecord, ThrottlingState, WatchSetCreation,
    create_began_execution_record, effective_sample_rate, pack_statement_began_execution_update,
    pack_statement_prepared_update, should_sample_statement,
};

use super::Message;

/// Statement logging state in the Coordinator.
#[derive(Debug)]
pub(crate) struct StatementLogging {
    /// The log that statements are recorded in, shared with the session tasks.
    pub(crate) log: Arc<StatementLog>,

    /// A reproducible RNG for deciding whether to sample statement executions.
    /// Only used by tests; otherwise, `rand::rng()` is used.
    /// Controlled by the system var `statement_logging_use_reproducible_rng`.
    /// This same instance will be used by all frontend tasks.
    reproducible_rng: Arc<Mutex<rand::rngs::ChaCha8Rng>>,

    /// Shared throttling state for rate-limiting statement logging.
    pub(crate) throttling_state: Arc<ThrottlingState>,

    /// Function to get the current time.
    pub(crate) now: NowFn,
}

impl StatementLogging {
    const REPRODUCIBLE_RNG_SEED: u64 = 42;

    /// `dyncfgs` must share its values with the catalog's system configuration.
    pub(crate) fn new(now: NowFn, dyncfgs: &ConfigSet) -> Self {
        Self {
            log: Arc::new(StatementLog::new(dyncfgs)),
            reproducible_rng: Arc::new(Mutex::new(rand::rngs::ChaCha8Rng::seed_from_u64(
                Self::REPRODUCIBLE_RNG_SEED,
            ))),
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
            log: Arc::clone(&self.log),
            throttling_state: Arc::clone(&self.throttling_state),
            reproducible_rng: Arc::clone(&self.reproducible_rng),
            build_info_human_version,
            now: self.now.clone(),
        }
    }
}

impl Coordinator {
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
        let PendingStatementLog {
            statement_executions,
            prepared_statements,
            sql_texts,
            sessions,
            statement_lifecycle_events,
        } = self.statement_logging.log.drain();

        use IntrospectionType::*;
        for (type_, updates) in [
            (SessionHistory, sessions),
            (PreparedStatementHistory, prepared_statements),
            (StatementExecutionHistory, statement_executions),
            (StatementLifecycleHistory, statement_lifecycle_events),
            (SqlText, sql_texts),
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
        if let PreparedStatementLoggingInfo::StillToLog { kind, .. } = logging {
            let kind = *kind;
            *logging = PreparedStatementLoggingInfo::AlreadyLogged { uuid, kind };
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
            PreparedStatementLoggingInfo::AlreadyLogged { uuid, .. } => (None, *uuid),
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

    /// Records the end of a statement execution, see [`StatementLog::end_execution`].
    pub(crate) fn end_statement_execution(
        &self,
        id: StatementLoggingId,
        reason: StatementEndedExecutionReason,
        ended_at: EpochMillis,
    ) {
        self.statement_logging
            .log
            .end_execution(id, reason, ended_at);
    }

    /// Set the `cluster_id` and `cluster_name` for a statement, once they're known.
    ///
    /// The name is resolved by the caller (from the same catalog snapshot that selected the
    /// cluster), rather than re-resolved here from `cluster_id`. This avoids a panic when the
    /// cluster was concurrently dropped, and logs the name as it was at selection time.
    pub(crate) fn set_statement_execution_cluster(
        &self,
        id: StatementLoggingId,
        cluster_id: ClusterId,
        cluster_name: String,
    ) {
        self.statement_logging
            .log
            .set_execution_cluster(id, cluster_id, cluster_name);
    }

    /// Sets the execution timestamp if the statement is still active.
    pub(crate) fn set_statement_execution_timestamp(
        &self,
        id: StatementLoggingId,
        timestamp: Timestamp,
    ) {
        self.statement_logging
            .log
            .set_execution_timestamp(id, timestamp);
    }

    /// Possibly record the beginning of statement execution, depending on a randomly-chosen value.
    /// If the execution beginning was indeed logged, returns a `StatementLoggingId` that must be
    /// passed to `end_statement_execution` to record when it ends.
    ///
    /// `lifecycle_timestamps` has timestamps that come from the Adapter frontend (`mz-pgwire`) part
    /// of the lifecycle.
    pub(crate) fn begin_statement_execution(
        &self,
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
        let kind = session.qcell_ro(logging).kind();
        let record = create_began_execution_record(
            execution_uuid,
            ps_uuid,
            sample_rate,
            params,
            session,
            began_at,
            build_info_version,
            kind,
        );

        // `mz_statement_execution_history`
        let mseh_update = pack_statement_began_execution_update(&record);

        let maybe_ps_event = maybe_ps.map(|(_record, event)| event);
        let maybe_sh_update = maybe_ps_event.as_ref().and_then(|event| {
            self.statement_logging
                .log
                .unlogged_session_update(event.session_id)
        });

        let maybe_ps_prepared_statement = maybe_ps_event.as_ref().map(|e| &e.prepared_statement);
        let maybe_ps_sql_text = maybe_ps_event.as_ref().map(|e| &e.sql_text);

        if !self.statement_logging_throttling_check([
            Some(&mseh_update),
            maybe_ps_prepared_statement,
            maybe_ps_sql_text,
            maybe_sh_update.as_ref(),
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

        self.statement_logging
            .log
            .begin_execution(record, mseh_update, maybe_ps_event);

        Some(StatementLoggingId(execution_uuid))
    }

    /// Record a new connection event
    pub(crate) fn begin_session_for_statement_logging(&self, session: &ConnMeta) {
        let id = session.uuid();
        let session_role = session.authenticated_role_id();
        let event = SessionHistoryEvent {
            id,
            connected_at: session.connected_at(),
            application_name: session.application_name().to_owned(),
            authenticated_user: self.catalog.get_role(session_role).name.clone(),
        };
        self.statement_logging.log.begin_session(event);
    }

    pub(crate) fn end_session_for_statement_logging(&self, uuid: Uuid) {
        self.statement_logging.log.end_session(uuid);
    }

    pub(crate) fn record_statement_lifecycle_event(
        &self,
        id: &StatementLoggingId,
        event: &StatementLifecycleEvent,
        when: EpochMillis,
    ) {
        self.statement_logging
            .log
            .record_lifecycle_event(*id, event, when);
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
