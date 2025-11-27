// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use bytes::BytesMut;
use mz_catalog::memory::objects::CatalogItem;
use mz_controller_types::ClusterId;
use mz_ore::cast::{CastFrom, CastInto};
use mz_ore::now::{EpochMillis, NowFn, epoch_to_uuid_v7, to_datetime};
use mz_ore::soft_panic_or_log;
use mz_repr::adt::array::ArrayDimension;
use mz_repr::adt::timestamp::TimestampLike;
use mz_repr::{Datum, GlobalId, Row, RowIterator, RowPacker, Timestamp};
use mz_sql::ast::display::AstDisplay;
use mz_sql::ast::{AstInfo, Statement};
use mz_sql::plan::Params;
use mz_sql::session::metadata::SessionMetadata;
use mz_sql::session::vars::SystemVars;
use mz_sql_parser::ast::{StatementKind, statement_kind_label_value};
use qcell::QCell;
use rand::distr::{Bernoulli, Distribution};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::catalog::CatalogState;
use crate::session::{LifecycleTimestamps, Session, TransactionId};
use crate::{AdapterError, CollectionIdBundle, ExecuteResponse};

#[derive(Clone, Debug)]
pub enum StatementLifecycleEvent {
    ExecutionBegan,
    OptimizationFinished,
    StorageDependenciesFinished,
    ComputeDependenciesFinished,
    ExecutionFinished,
}

impl StatementLifecycleEvent {
    pub fn as_str(&self) -> &str {
        match self {
            Self::ExecutionBegan => "execution-began",
            Self::OptimizationFinished => "optimization-finished",
            Self::StorageDependenciesFinished => "storage-dependencies-finished",
            Self::ComputeDependenciesFinished => "compute-dependencies-finished",
            Self::ExecutionFinished => "execution-finished",
        }
    }
}

/// Contains all the information necessary to generate the initial
/// entry in `mz_statement_execution_history`. We need to keep this
/// around in order to modify the entry later once the statement finishes executing.
#[derive(Clone, Debug)]
pub struct StatementBeganExecutionRecord {
    pub id: Uuid,
    pub prepared_statement_id: Uuid,
    pub sample_rate: f64,
    pub params: Vec<Option<String>>,
    pub began_at: EpochMillis,
    pub cluster_id: Option<ClusterId>,
    pub cluster_name: Option<String>,
    pub database_name: String,
    pub search_path: Vec<String>,
    pub application_name: String,
    pub transaction_isolation: String,
    pub execution_timestamp: Option<EpochMillis>,
    pub transaction_id: TransactionId,
    pub transient_index_id: Option<GlobalId>,
    pub mz_version: String,
}

#[derive(Clone, Copy, Debug)]
pub enum StatementExecutionStrategy {
    /// The statement was executed by spinning up a dataflow.
    Standard,
    /// The statement was executed by reading from an existing
    /// arrangement.
    FastPath,
    /// Experimental: The statement was executed by reading from an existing
    /// persist collection.
    PersistFastPath,
    /// The statement was determined to be constant by
    /// environmentd, and not sent to a cluster.
    Constant,
}

impl StatementExecutionStrategy {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::FastPath => "fast-path",
            Self::PersistFastPath => "persist-fast-path",
            Self::Constant => "constant",
        }
    }
}

#[derive(Clone, Debug)]
pub enum StatementEndedExecutionReason {
    Success {
        result_size: Option<u64>,
        rows_returned: Option<u64>,
        execution_strategy: Option<StatementExecutionStrategy>,
    },
    Canceled,
    Errored {
        error: String,
    },
    Aborted,
}

#[derive(Clone, Debug)]
pub struct StatementEndedExecutionRecord {
    pub id: Uuid,
    pub reason: StatementEndedExecutionReason,
    pub ended_at: EpochMillis,
}

/// Contains all the information necessary to generate an entry in
/// `mz_prepared_statement_history`
#[derive(Clone, Debug)]
pub(crate) struct StatementPreparedRecord {
    pub id: Uuid,
    pub sql_hash: [u8; 32],
    pub name: String,
    pub session_id: Uuid,
    pub prepared_at: EpochMillis,
    pub kind: Option<StatementKind>,
}

#[derive(Clone, Debug)]
pub(crate) struct SessionHistoryEvent {
    pub id: Uuid,
    pub connected_at: EpochMillis,
    pub application_name: String,
    pub authenticated_user: String,
}

impl From<&Result<ExecuteResponse, AdapterError>> for StatementEndedExecutionReason {
    fn from(value: &Result<ExecuteResponse, AdapterError>) -> StatementEndedExecutionReason {
        match value {
            Ok(resp) => resp.into(),
            Err(e) => StatementEndedExecutionReason::Errored {
                error: e.to_string(),
            },
        }
    }
}

impl From<&ExecuteResponse> for StatementEndedExecutionReason {
    fn from(value: &ExecuteResponse) -> StatementEndedExecutionReason {
        match value {
            ExecuteResponse::CopyTo { resp, .. } => match resp.as_ref() {
                // NB [btv]: It's not clear that this combination
                // can ever actually happen.
                ExecuteResponse::SendingRowsImmediate { rows, .. } => {
                    // Note(parkmycar): It potentially feels bad here to iterate over the entire
                    // iterator _just_ to get the encoded result size. As noted above, it's not
                    // entirely clear this case ever happens, so the simplicity is worth it.
                    let result_size: usize = rows.box_clone().map(|row| row.byte_len()).sum();
                    StatementEndedExecutionReason::Success {
                        result_size: Some(u64::cast_from(result_size)),
                        rows_returned: Some(u64::cast_from(rows.count())),
                        execution_strategy: Some(StatementExecutionStrategy::Constant),
                    }
                }
                ExecuteResponse::SendingRowsStreaming { .. } => {
                    panic!("SELECTs terminate on peek finalization, not here.")
                }
                ExecuteResponse::Subscribing { .. } => {
                    panic!("SUBSCRIBEs terminate in the protocol layer, not here.")
                }
                _ => panic!("Invalid COPY response type"),
            },
            ExecuteResponse::CopyFrom { .. } => {
                panic!("COPY FROMs terminate in the protocol layer, not here.")
            }
            ExecuteResponse::Fetch { .. } => {
                panic!("FETCHes terminate after a follow-up message is sent.")
            }
            ExecuteResponse::SendingRowsStreaming { .. } => {
                panic!("SELECTs terminate on peek finalization, not here.")
            }
            ExecuteResponse::Subscribing { .. } => {
                panic!("SUBSCRIBEs terminate in the protocol layer, not here.")
            }

            ExecuteResponse::SendingRowsImmediate { rows, .. } => {
                // Note(parkmycar): It potentially feels bad here to iterate over the entire
                // iterator _just_ to get the encoded result size, the number of Rows returned here
                // shouldn't be too large though. An alternative is to pre-compute some of the
                // result size, but that would require always decoding Rows to handle projecting
                // away columns, which has a negative impact for much larger response sizes.
                let result_size: usize = rows.box_clone().map(|row| row.byte_len()).sum();
                StatementEndedExecutionReason::Success {
                    result_size: Some(u64::cast_from(result_size)),
                    rows_returned: Some(u64::cast_from(rows.count())),
                    execution_strategy: Some(StatementExecutionStrategy::Constant),
                }
            }

            ExecuteResponse::AlteredDefaultPrivileges
            | ExecuteResponse::AlteredObject(_)
            | ExecuteResponse::AlteredRole
            | ExecuteResponse::AlteredSystemConfiguration
            | ExecuteResponse::ClosedCursor
            | ExecuteResponse::Comment
            | ExecuteResponse::Copied(_)
            | ExecuteResponse::CreatedConnection
            | ExecuteResponse::CreatedDatabase
            | ExecuteResponse::CreatedSchema
            | ExecuteResponse::CreatedRole
            | ExecuteResponse::CreatedCluster
            | ExecuteResponse::CreatedClusterReplica
            | ExecuteResponse::CreatedIndex
            | ExecuteResponse::CreatedIntrospectionSubscribe
            | ExecuteResponse::CreatedSecret
            | ExecuteResponse::CreatedSink
            | ExecuteResponse::CreatedSource
            | ExecuteResponse::CreatedTable
            | ExecuteResponse::CreatedView
            | ExecuteResponse::CreatedViews
            | ExecuteResponse::CreatedMaterializedView
            | ExecuteResponse::CreatedContinualTask
            | ExecuteResponse::CreatedType
            | ExecuteResponse::CreatedNetworkPolicy
            | ExecuteResponse::Deallocate { .. }
            | ExecuteResponse::DeclaredCursor
            | ExecuteResponse::Deleted(_)
            | ExecuteResponse::DiscardedTemp
            | ExecuteResponse::DiscardedAll
            | ExecuteResponse::DroppedObject(_)
            | ExecuteResponse::DroppedOwned
            | ExecuteResponse::EmptyQuery
            | ExecuteResponse::GrantedPrivilege
            | ExecuteResponse::GrantedRole
            | ExecuteResponse::Inserted(_)
            | ExecuteResponse::Prepare
            | ExecuteResponse::Raised
            | ExecuteResponse::ReassignOwned
            | ExecuteResponse::RevokedPrivilege
            | ExecuteResponse::RevokedRole
            | ExecuteResponse::SetVariable { .. }
            | ExecuteResponse::StartedTransaction
            | ExecuteResponse::TransactionCommitted { .. }
            | ExecuteResponse::TransactionRolledBack { .. }
            | ExecuteResponse::Updated(_)
            | ExecuteResponse::ValidatedConnection { .. } => {
                StatementEndedExecutionReason::Success {
                    result_size: None,
                    rows_returned: None,
                    execution_strategy: None,
                }
            }
        }
    }
}

mod sealed {
    /// A struct that is purposefully private so folks are forced to use the constructor of an
    /// enum.
    #[derive(Debug, Copy, Clone)]
    pub struct Private;
}

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
pub struct StatementLoggingId(pub Uuid);

/// Rows to be written to `mz_prepared_statement_history` and `mz_sql_text`, with the session id.
#[derive(Debug, Clone)]
pub struct PreparedStatementEvent {
    pub prepared_statement: Row,
    pub sql_text: Row,
    pub session_id: Uuid,
}

/// Throttling state for statement logging, shared across multiple frontend tasks (and currently
/// also shared with the old peek sequencing).
#[derive(Debug)]
pub struct ThrottlingState {
    /// Inner state protected by a mutex for rate-limiting, because the two inner fields have to be
    /// manipulated together atomically.
    /// This mutex is locked once per unsampled query. (There is both sampling and throttling.
    /// Sampling happens before throttling.) This should be ok for now: Our QPS will not be more
    /// than 10000s for now, and a mutex should be able to do 100000s of lockings per second, even
    /// with some contention. If this ever becomes an issue, then we could redesign throttling to be
    /// per-session/per-tokio-worker-thread.
    inner: Mutex<ThrottlingStateInner>,
    /// The number of statements that have been throttled since the last successfully logged
    /// statement. This is not needed for the throttling decision itself, so it can be a separate
    /// atomic to allow reading/writing without acquiring the inner mutex.
    throttled_count: std::sync::atomic::AtomicUsize,
}

#[derive(Debug)]
struct ThrottlingStateInner {
    /// The number of bytes that we are allowed to emit for statement logging without being throttled.
    /// Increases at a rate of [`mz_sql::session::vars::STATEMENT_LOGGING_TARGET_DATA_RATE`] per second,
    /// up to a max value of [`mz_sql::session::vars::STATEMENT_LOGGING_MAX_DATA_CREDIT`].
    tokens: u64,
    /// The last time at which a statement was logged.
    last_logged_ts_seconds: u64,
}

impl ThrottlingState {
    /// Create a new throttling state.
    pub fn new(now: &NowFn) -> Self {
        Self {
            inner: Mutex::new(ThrottlingStateInner {
                tokens: 0,
                last_logged_ts_seconds: now() / 1000,
            }),
            throttled_count: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Check if we need to drop a statement due to throttling, and update the number of available
    /// tokens appropriately.
    ///
    /// Returns `false` if we must throttle this statement, and `true` otherwise.
    /// Note: `throttled_count` is NOT modified by this method - callers are responsible
    /// for incrementing it on throttle failure and resetting it when appropriate.
    pub fn throttling_check(
        &self,
        cost: u64,
        target_data_rate: u64,
        max_data_credit: Option<u64>,
        now: &NowFn,
    ) -> bool {
        let ts = now() / 1000;
        let mut inner = self.inner.lock().expect("throttling state lock poisoned");
        // We use saturating_sub here because system time isn't monotonic, causing cases
        // when last_logged_ts_seconds is greater than ts.
        let elapsed = ts.saturating_sub(inner.last_logged_ts_seconds);
        inner.last_logged_ts_seconds = ts;
        inner.tokens = inner
            .tokens
            .saturating_add(target_data_rate.saturating_mul(elapsed));
        if let Some(max_data_credit) = max_data_credit {
            inner.tokens = inner.tokens.min(max_data_credit);
        }
        if let Some(remaining) = inner.tokens.checked_sub(cost) {
            tracing::debug!("throttling check passed. tokens remaining: {remaining}; cost: {cost}");
            inner.tokens = remaining;
            true
        } else {
            tracing::debug!(
                "throttling check failed. tokens available: {}; cost: {cost}",
                inner.tokens
            );
            false
        }
    }

    pub fn get_throttled_count(&self) -> usize {
        self.throttled_count.load(Ordering::Relaxed)
    }

    pub fn increment_throttled_count(&self) {
        self.throttled_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn reset_throttled_count(&self) {
        self.throttled_count.store(0, Ordering::Relaxed);
    }
}

/// Encapsulates statement logging state needed by the frontend peek sequencing.
///
/// This struct bundles together all the statement logging-related state that
/// the frontend peek sequencing needs to perform statement logging independently
/// of the Coordinator's main task.
#[derive(Debug, Clone)]
pub struct StatementLoggingFrontend {
    /// Shared throttling state for rate-limiting statement logging.
    pub throttling_state: Arc<ThrottlingState>,
    /// Reproducible RNG for statement sampling (only used in tests).
    pub reproducible_rng: Arc<Mutex<rand_chacha::ChaCha8Rng>>,
    /// Cached human version string from build info.
    pub build_info_human_version: String,
    /// Function to get current time for statement logging.
    pub now: NowFn,
}

impl StatementLoggingFrontend {
    /// Get prepared statement info for frontend peek sequencing.
    ///
    /// This function processes prepared statement logging info and builds the event rows.
    /// It does NOT do throttling - that is handled externally by the caller in `begin_statement_execution`.
    /// It DOES mutate the logging info to mark the statement as already logged.
    ///
    /// # Arguments
    /// * `session` - The session executing the statement
    /// * `logging` - Prepared statement logging info
    ///
    /// # Returns
    /// A tuple containing:
    /// - `Option<PreparedStatementEvent>`: If the prepared statement has not yet been logged,
    ///   returns the packed rows for the prepared statement.
    /// - `Uuid`: The UUID of the prepared statement.
    fn get_prepared_statement_info(
        &self,
        session: &mut Session,
        logging: &Arc<QCell<PreparedStatementLoggingInfo>>,
    ) -> (Option<PreparedStatementEvent>, Uuid) {
        let logging_ref = session.qcell_rw(&*logging);
        let mut prepared_statement_event = None;

        let ps_uuid = match logging_ref {
            PreparedStatementLoggingInfo::AlreadyLogged { uuid } => *uuid,
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
                let sql = std::mem::take(sql);
                let redacted_sql = std::mem::take(redacted_sql);
                let sql_hash: [u8; 32] = Sha256::digest(sql.as_bytes()).into();

                // Copy session_id before mutating logging_ref
                let sid = *session_id;

                let record = StatementPreparedRecord {
                    id: uuid,
                    sql_hash,
                    name: std::mem::take(name),
                    session_id: sid,
                    prepared_at: *prepared_at,
                    kind: *kind,
                };

                // `mz_prepared_statement_history`
                let mut mpsh_row = Row::default();
                let mut mpsh_packer = mpsh_row.packer();
                pack_statement_prepared_update(&record, &mut mpsh_packer);

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

                // Read throttled_count from shared state
                let throttled_count = self.throttling_state.get_throttled_count();

                mpsh_packer.push(Datum::UInt64(CastFrom::cast_from(throttled_count)));

                prepared_statement_event = Some(PreparedStatementEvent {
                    prepared_statement: mpsh_row,
                    sql_text: sql_row,
                    session_id: sid,
                });

                *logging_ref = PreparedStatementLoggingInfo::AlreadyLogged { uuid };
                uuid
            }
        };

        (prepared_statement_event, ps_uuid)
    }

    /// Begin statement execution logging from the frontend. (Corresponds to
    /// `Coordinator::begin_statement_execution`, which is used by the old peek sequencing.)
    ///
    /// This encapsulates all the statement logging setup:
    /// - Retrieves system config values
    /// - Performs sampling and throttling checks
    /// - Creates statement logging records
    /// - Attends to metrics.
    ///
    /// Returns None if the statement should not be logged (due to sampling or throttling), or the
    /// info required to proceed with statement logging.
    /// The `Row` is the pre-packed row for `mz_statement_execution_history`.
    /// The `Option<PreparedStatementEvent>` is None when we have already logged the prepared
    /// statement before, and this is just a subsequent execution.
    pub fn begin_statement_execution(
        &self,
        session: &mut Session,
        params: &Params,
        logging: &Arc<QCell<PreparedStatementLoggingInfo>>,
        system_config: &SystemVars,
        lifecycle_timestamps: Option<LifecycleTimestamps>,
    ) -> Option<(
        StatementLoggingId,
        StatementBeganExecutionRecord,
        Row,
        Option<PreparedStatementEvent>,
    )> {
        // Skip logging for internal users unless explicitly enabled
        let enable_internal_statement_logging = system_config.enable_internal_statement_logging();
        if session.user().is_internal() && !enable_internal_statement_logging {
            return None;
        }

        let sample_rate = effective_sample_rate(session, system_config);

        let use_reproducible_rng = system_config.statement_logging_use_reproducible_rng();
        let target_data_rate: Option<u64> = system_config
            .statement_logging_target_data_rate()
            .map(|rate| rate.cast_into());
        let max_data_credit: Option<u64> = system_config
            .statement_logging_max_data_credit()
            .map(|credit| credit.cast_into());

        // Only lock the RNG when we actually need reproducible sampling (tests only)
        let sample = if use_reproducible_rng {
            let mut rng = self.reproducible_rng.lock().expect("rng lock poisoned");
            should_sample_statement(sample_rate, Some(&mut *rng))
        } else {
            should_sample_statement(sample_rate, None)
        };

        let sampled_label = sample.then_some("true").unwrap_or("false");
        session
            .metrics()
            .statement_logging_records(&[sampled_label])
            .inc_by(1);

        // Clone only the metrics needed below, before the mutable borrow of session.
        let unsampled_bytes_metric = session
            .metrics()
            .statement_logging_unsampled_bytes()
            .clone();
        let actual_bytes_metric = session.metrics().statement_logging_actual_bytes().clone();

        // Handle the accounted flag and record byte metrics
        let is_new_prepared_statement = if let Some((sql, accounted)) =
            match session.qcell_rw(logging) {
                PreparedStatementLoggingInfo::AlreadyLogged { .. } => None,
                PreparedStatementLoggingInfo::StillToLog { sql, accounted, .. } => {
                    Some((sql, accounted))
                }
            } {
            if !*accounted {
                unsampled_bytes_metric.inc_by(u64::cast_from(sql.len()));
                if sample {
                    actual_bytes_metric.inc_by(u64::cast_from(sql.len()));
                }
                *accounted = true;
            }
            true
        } else {
            false
        };

        if !sample {
            return None;
        }

        // Get prepared statement info (this also marks it as logged)
        let (prepared_statement_event, ps_uuid) =
            self.get_prepared_statement_info(session, logging);

        let began_at = if let Some(lifecycle_timestamps) = lifecycle_timestamps {
            lifecycle_timestamps.received
        } else {
            (self.now)()
        };

        let current_time = (self.now)();
        let execution_uuid = epoch_to_uuid_v7(&current_time);

        // Create the execution record
        let began_execution = create_began_execution_record(
            execution_uuid,
            ps_uuid,
            sample_rate,
            params,
            session,
            began_at,
            self.build_info_human_version.clone(),
        );

        // Build rows to calculate cost for throttling
        let mseh_update = pack_statement_began_execution_update(&began_execution);
        let maybe_ps_prepared_statement = prepared_statement_event
            .as_ref()
            .map(|e| &e.prepared_statement);
        let maybe_ps_sql_text = prepared_statement_event.as_ref().map(|e| &e.sql_text);

        // Calculate cost of all rows we intend to log
        let cost: usize = [
            Some(&mseh_update),
            maybe_ps_prepared_statement,
            maybe_ps_sql_text,
        ]
        .into_iter()
        .filter_map(|row_opt| row_opt.map(|row| row.byte_len()))
        .fold(0_usize, |acc, x| acc.saturating_add(x));

        // Do throttling check
        let passed = if let Some(target_data_rate) = target_data_rate {
            self.throttling_state.throttling_check(
                cost.cast_into(),
                target_data_rate,
                max_data_credit,
                &self.now,
            )
        } else {
            true // No throttling configured
        };

        if !passed {
            // Increment throttled_count in shared state
            self.throttling_state.increment_throttled_count();
            return None;
        }

        // When we successfully log the first instance of a prepared statement
        // (i.e., it is not throttled), reset the throttled count for future tracking.
        if is_new_prepared_statement {
            self.throttling_state.reset_throttled_count();
        }

        Some((
            StatementLoggingId(execution_uuid),
            began_execution,
            mseh_update,
            prepared_statement_event,
        ))
    }
}

/// The effective rate at which statement execution should be sampled.
/// This is the value of the session var `statement_logging_sample_rate`,
/// constrained by the system var `statement_logging_max_sample_rate`.
pub(crate) fn effective_sample_rate(session: &Session, system_vars: &SystemVars) -> f64 {
    let system_max: f64 = system_vars
        .statement_logging_max_sample_rate()
        .try_into()
        .expect("value constrained to be convertible to f64");
    let user_rate: f64 = session
        .vars()
        .get_statement_logging_sample_rate()
        .try_into()
        .expect("value constrained to be convertible to f64");
    f64::min(system_max, user_rate)
}

/// Helper function to decide whether to sample a statement execution.
/// Returns `true` if the statement should be sampled based on the sample rate.
///
/// If `reproducible_rng` is `Some`, uses the provided RNG for reproducible sampling (used in tests).
/// If `reproducible_rng` is `None`, uses the thread-local RNG.
pub(crate) fn should_sample_statement(
    sample_rate: f64,
    reproducible_rng: Option<&mut rand_chacha::ChaCha8Rng>,
) -> bool {
    let distribution = Bernoulli::new(sample_rate).unwrap_or_else(|_| {
        soft_panic_or_log!("statement_logging_sample_rate is out of range [0, 1]");
        Bernoulli::new(0.0).expect("0.0 is valid for Bernoulli")
    });
    if let Some(rng) = reproducible_rng {
        distribution.sample(rng)
    } else {
        distribution.sample(&mut rand::rng())
    }
}

/// Helper function to serialize statement parameters for logging.
fn serialize_params(params: &Params) -> Vec<Option<String>> {
    std::iter::zip(params.execute_types.iter(), params.datums.iter())
        .map(|(r#type, datum)| {
            mz_pgrepr::Value::from_datum(datum, r#type).map(|val| {
                let mut buf = BytesMut::new();
                val.encode_text(&mut buf);
                String::from_utf8(Into::<Vec<u8>>::into(buf))
                    .expect("Serialization shouldn't produce non-UTF-8 strings.")
            })
        })
        .collect()
}

/// Helper function to create a `StatementBeganExecutionRecord`.
pub(crate) fn create_began_execution_record(
    execution_uuid: Uuid,
    prepared_statement_uuid: Uuid,
    sample_rate: f64,
    params: &Params,
    session: &Session,
    began_at: EpochMillis,
    build_info_version: String,
) -> StatementBeganExecutionRecord {
    let params = serialize_params(params);
    StatementBeganExecutionRecord {
        id: execution_uuid,
        prepared_statement_id: prepared_statement_uuid,
        sample_rate,
        params,
        began_at,
        application_name: session.application_name().to_string(),
        transaction_isolation: session.vars().transaction_isolation().to_string(),
        transaction_id: session
            .transaction()
            .inner()
            .map(|t| t.id)
            .unwrap_or_else(|| {
                // This should never happen because every statement runs in an explicit or implicit
                // transaction.
                soft_panic_or_log!(
                    "Statement logging got a statement with no associated transaction"
                );
                9999999
            }),
        mz_version: build_info_version,
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
    }
}

/// Represents a single statement logging event that can be sent from the frontend
/// peek sequencing to the Coordinator via an mpsc channel.
#[derive(Debug, Clone)]
pub enum FrontendStatementLoggingEvent {
    /// Statement execution began, possibly with an associated prepared statement
    /// if this is the first time the prepared statement is being logged
    BeganExecution {
        record: StatementBeganExecutionRecord,
        /// `mz_statement_execution_history`
        mseh_update: Row,
        prepared_statement: Option<PreparedStatementEvent>,
    },
    /// Statement execution ended
    EndedExecution(StatementEndedExecutionRecord),
    /// Set the cluster for a statement execution
    SetCluster {
        id: StatementLoggingId,
        cluster_id: ClusterId,
    },
    /// Set the execution timestamp for a statement
    SetTimestamp {
        id: StatementLoggingId,
        timestamp: Timestamp,
    },
    /// Set the transient index ID for a statement
    SetTransientIndex {
        id: StatementLoggingId,
        transient_index_id: GlobalId,
    },
    /// Record a statement lifecycle event
    Lifecycle {
        id: StatementLoggingId,
        event: StatementLifecycleEvent,
        when: EpochMillis,
    },
}

pub(crate) fn pack_statement_execution_inner(
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

pub(crate) fn pack_statement_began_execution_update(record: &StatementBeganExecutionRecord) -> Row {
    let mut row = Row::default();
    let mut packer = row.packer();
    pack_statement_execution_inner(record, &mut packer);
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

pub(crate) fn pack_statement_prepared_update(
    record: &StatementPreparedRecord,
    packer: &mut RowPacker,
) {
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

/// Bundles all information needed to install watch sets for statement lifecycle logging.
/// This includes the statement logging ID and the transitive dependencies to watch.
#[derive(Debug)]
pub struct WatchSetCreation {
    /// The statement logging ID for this execution.
    pub logging_id: StatementLoggingId,
    /// The timestamp at which to watch for dependencies becoming ready.
    pub timestamp: Timestamp,
    /// Transitive storage dependencies (tables, sources) to watch.
    pub storage_ids: BTreeSet<GlobalId>,
    /// Transitive compute dependencies (materialized views, indexes) to watch.
    pub compute_ids: BTreeSet<GlobalId>,
}

impl WatchSetCreation {
    /// Compute transitive dependencies for watch sets from an input ID bundle, categorized into
    /// storage and compute IDs.
    pub fn new(
        logging_id: StatementLoggingId,
        catalog_state: &CatalogState,
        input_id_bundle: &CollectionIdBundle,
        timestamp: Timestamp,
    ) -> Self {
        let mut storage_ids = BTreeSet::new();
        let mut compute_ids = BTreeSet::new();

        for item_id in input_id_bundle
            .iter()
            .map(|gid| catalog_state.get_entry_by_global_id(&gid).id())
            .flat_map(|id| catalog_state.transitive_uses(id))
        {
            let entry = catalog_state.get_entry(&item_id);
            match entry.item() {
                // TODO(alter_table): Adding all of the GlobalIds for an object is incorrect.
                // For example, this peek may depend on just a single version of a table, but
                // we would add dependencies on all versions of said table. Doing this is okay
                // for now since we can't yet version tables, but should get fixed.
                CatalogItem::Table(_) | CatalogItem::Source(_) => {
                    storage_ids.extend(entry.global_ids());
                }
                // Each catalog item is computed by at most one compute collection at a time,
                // which is also the most recent one.
                CatalogItem::MaterializedView(_) | CatalogItem::Index(_) => {
                    compute_ids.insert(entry.latest_global_id());
                }
                _ => {}
            }
        }

        Self {
            logging_id,
            timestamp,
            storage_ids,
            compute_ids,
        }
    }
}
