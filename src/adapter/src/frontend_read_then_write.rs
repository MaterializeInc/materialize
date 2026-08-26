// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Frontend sequencing for read-then-write operations.
//!
//! This module implements INSERT [...] SELECT FROM [...], DELETE and UPDATE
//! operations using a subscribe with optimistic concurrency control (OCC),
//! sequenced from the session task rather than the Coordinator.
//!
//! The motivation is correctness with concurrent writers, including writers in
//! different `environmentd` processes, which the coordinator's in-process write
//! locks cannot provide. The OCC path also fixes a serializability defect of the
//! lock path, which reads at one timestamp and commits at a later one while
//! locking only the selection's direct dependency items. See the design doc,
//! `doc/developer/design/20260210_incremental_occ_read_then_write.md`, and the
//! comment on the retry arm in `run_occ_loop`. Relieving the coordinator loop is
//! a side benefit, and only sequencing moves off it. The subscribe's data path
//! still runs through the coordinator.
//!
//! ## Whether the write reads persisted state
//!
//! Two predicates answer that one question, and they have to agree. Before
//! anything runs, `SessionClient::try_frontend_read_then_write` decides it
//! syntactically, from `depends_on()` on the planned selection, because inside
//! a transaction a read-dependent write has to be refused while refusing is
//! still possible. Once the dataflow runs, the subscribe answers it
//! dynamically: the channel closes on its own only once the sink's output
//! frontier reaches the empty antichain, which means the selection can never
//! change again.
//!
//! Reading nothing persisted is the common case for a clean close, not the
//! guarantee. An input whose frontier seals closes cleanly too, despite reading
//! persisted state, for example a `REFRESH AT` materialized view past its last
//! refresh, whose write frontier advances to the empty antichain. What holds in
//! either case is the property the write side actually needs: past the close,
//! the consolidated diffs are frontier-independent. The syntactic predicate is
//! correspondingly stricter than the dynamic one, since it refuses a sealed-MV
//! `INSERT ... SELECT` in a transaction that would technically be bufferable.
//!
//! The two answers are used for different things, and that separation matters.
//!
//! The syntactic answer decides whether the statement can belong to a
//! transaction. A selection that reads nothing produces diffs that are valid at
//! any timestamp, so they are staged as session write ops and land when the
//! transaction commits, which is what makes the statement atomic with whatever
//! surrounds it. A selection that reads persisted state cannot belong to a
//! transaction, and we refuse it in an explicit one. An extended-protocol
//! pipeline is an implicit transaction, so it must not quietly join one either:
//! it ends its own transaction instead of spanning the rest of the pipeline,
//! which is how PostgreSQL treats statements that cannot run in a transaction
//! block. It is durable once it reports success, and a later failure in the
//! pipeline does not undo it.
//!
//! The dynamic answer only decides how the write is submitted, a timestamped
//! write from inside the loop or a blind submission after it. It cannot decide
//! transaction membership, because it is a property of the inputs rather than
//! of the statement. A sealed input closes the subscribe cleanly, so an
//! `INSERT ... SELECT` over a `REFRESH AT` materialized view past its last
//! refresh takes the blind exit while still reading persisted state. Its diffs
//! really are frontier-independent and staging them would be safe, and we still
//! do not stage them. Otherwise whether a statement's rows survive a failure
//! later in the pipeline would depend on whether one of its inputs happened to
//! pass its last refresh, which no one reading the statement could predict.
//!
//! Staging is also what earns the right to span a pipeline in the first place.
//! `TransactionStatus::may_span_pipeline` lets an implicit transaction stay open
//! only for writes, precisely because they are merely staged. A statement that
//! committed on its own has no business claiming it.
//!
//! Disagreement is caught on both sides, and only one side can still refuse.
//! `frontend_read_then_write` re-checks the syntactic predicate before running a
//! dataflow, which catches a caller that skipped the gate. If the syntactic
//! predicate were laxer than the dynamic one, that check would pass and a write
//! meant for staging would commit on its own, so the loop asserts wherever the
//! two answers can be compared. The `Committed` arm catches a write timestamp
//! for a statement we meant to stage, and the zero-row arm catches a read
//! timestamp for one. Neither can undo anything by then, the write is already
//! durable in the first case and there was never anything to write in the
//! second, so all they do is make the disagreement loud.
//!
//! ## The frontier certifies, the oracle chooses
//!
//! The target `T` comes from the oracle, and a progress message at `F` only
//! certifies completeness below `F`, so it gates the write rather than choosing
//! its timestamp. The design doc's "The OCC loop" says why. Three invariants
//! hold for every write this path makes:
//!
//! * `F >= T` before it submits, so the payload is a complete view of `T - 1`.
//! * The payload is every diff below `T`, strictly. A diff at `T` is concurrent
//!   with the write and waits for a later target.
//! * `T > as_of`, so the snapshot, which arrives at `as_of`, is in the payload.
//!
//! NOTE: `F >= T` does not make the two equal, because `F` is a minimum over the
//! selection's inputs. Where `F` runs above `T`, a selection that reads the
//! target table makes the compare-and-append refuse, and retrying higher is the
//! design rather than a failure.
//!
//! ## A zero-row answer
//!
//! A write linearizes itself, because group commit advances the oracle before it
//! acknowledges. An answer of "no rows" performs no write, so the loop reports
//! the timestamp its view is complete through and the caller waits for the
//! oracle to reach it. A selection empty at `as_of` is complete through `as_of`,
//! which the caller put behind the oracle before the subscribe started, so that
//! answer waits for nothing, while reporting the frontier the subscribe observed
//! would cost a group commit every time. The design doc's "Linearization" argues
//! why the lower timestamp is not the weaker guarantee.
//!
//! ## Rollout note
//!
//! The `FRONTEND_READ_THEN_WRITE` dyncfg is read once at process startup and
//! fixed for the lifetime of the `environmentd` process. This avoids a
//! mixed-mode window where both the lock-based coordinator path and this OCC
//! path are active concurrently. The coordinator path acquires write locks to
//! prevent concurrent writes between its read and write phases, but this OCC
//! path does not use write locks, so concurrent operation of both paths could
//! allow an OCC write to slip between a coordinator-path reader's read and
//! write.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::num::{NonZeroI64, NonZeroUsize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytesize::ByteSize;
use differential_dataflow::consolidation;
use mz_catalog::memory::error::ErrorKind;
use mz_cluster_client::ReplicaId;
use mz_compute_types::ComputeInstanceId;
use mz_expr::Eval;
use mz_expr::row::RowCollection;
use mz_expr::{CollectionPlan, Id, LocalId, MirRelationExpr, MirScalarExpr, RowSetFinishing};
use mz_ore::cast::CastFrom;
use mz_ore::{soft_assert_or_log, soft_panic_or_log};
use mz_repr::optimize::OverrideFrom;
use mz_repr::{CatalogItemId, Diff, GlobalId, RelationDesc, Row, RowArena, Timestamp};
use mz_sql::catalog::CatalogError;
use mz_sql::plan::{self, MutationKind, QueryWhen};
use mz_sql::session::metadata::SessionMetadata;
use mz_sql::session::vars::IsolationLevel;
use mz_storage_client::client::TableData;
use mz_storage_types::sources::Timeline;
use mz_timestamp_oracle::TimestampOracle;
use prometheus::Histogram;
use timely::progress::Antichain;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::active_compute_sink::ActiveSubscribeOwner;
use crate::catalog::Catalog;
use crate::command::{Command, ExecuteResponse, WriteAttemptKind};
use crate::coord::appends::WriteResult;
use crate::coord::read_then_write::{DependencyPolicy, validate_read_then_write_dependencies};
use crate::coord::timestamp_selection::TimestampProvider;
use crate::coord::{Coordinator, TargetCluster};
use crate::error::AdapterError;
use crate::optimize::Optimize;
use crate::optimize::dataflows::{ComputeInstanceSnapshot, EvalTime, ExprPrep, ExprPrepOneShot};
use crate::peek_client::CoordinatorClient;
use crate::session::{Session, TransactionOps, WriteOp};
use crate::statement_logging::{StatementLifecycleEvent, StatementLoggingId};
use crate::{PeekClient, PeekResponseUnary, TimelineContext, optimize};

/// Reason a frontend write attempt is being torn down early.
#[derive(Clone, Copy)]
pub(crate) enum FrontendWriteCancellation {
    Canceled,
    StatementTimeout,
}

impl From<FrontendWriteCancellation> for AdapterError {
    fn from(cancellation: FrontendWriteCancellation) -> Self {
        match cancellation {
            FrontendWriteCancellation::Canceled => AdapterError::Canceled,
            FrontendWriteCancellation::StatementTimeout => AdapterError::StatementTimeout,
        }
    }
}

/// State shared between an in-flight frontend write attempt and its
/// cancellation wrapper,
/// `SessionClient::try_frontend_read_then_write_with_cancel`.
///
/// The contract: `write_submitted` is true from just before the
/// `AttemptWrite` command is sent until the attempt resolves as definitively
/// not committed. While it is true, cancellation and statement timeout must
/// not synthesize an error but await the definitive write result instead,
/// because the write may already be durable.
///
/// The wrapper and the attempt it wraps are polled by the same task, so the
/// mutex and the atomic are here to satisfy `Send`, not to arbitrate between
/// concurrent writers. There is one writer for each field.
pub(crate) struct FrontendWriteAttemptState {
    write_submitted: AtomicBool,
    /// Set at most once, by the cancellation wrapper.
    cancellation: Mutex<Option<FrontendWriteCancellation>>,
}

impl FrontendWriteAttemptState {
    pub(crate) fn new() -> Self {
        Self {
            write_submitted: AtomicBool::new(false),
            cancellation: Mutex::new(None),
        }
    }

    pub(crate) fn mark_write_submitted(&self) {
        self.write_submitted.store(true, Ordering::Release);
    }

    /// Marks the submitted write as definitively not committed.
    ///
    /// NOTE: This must only be called for outcomes where the write is known
    /// to not have landed (`TimestampPassed`). Terminal outcomes leave
    /// `write_submitted` set so a concurrent cancellation path can never
    /// fabricate an error for a write that may have committed.
    fn mark_write_resolved(&self) {
        self.write_submitted.store(false, Ordering::Release);
    }

    pub(crate) fn write_submitted(&self) -> bool {
        self.write_submitted.load(Ordering::Acquire)
    }

    /// Records why the attempt is being torn down. The first reason recorded
    /// is the one the attempt reports.
    pub(crate) fn request(&self, cancellation: FrontendWriteCancellation) {
        self.cancellation
            .lock()
            .expect("cancellation lock poisoned")
            .get_or_insert(cancellation);
    }

    fn requested_error(&self) -> Option<AdapterError> {
        self.cancellation
            .lock()
            .expect("cancellation lock poisoned")
            .map(AdapterError::from)
    }
}

/// Which kind of caller is driving a read-then-write.
///
/// Dependency rules, replica selection and write cancellation all follow from
/// this, and they have to move together. Pinning a replica without also allowing
/// system reads, or the reverse, is never correct.
#[derive(Clone, Copy)]
enum RtwCaller {
    /// A user statement. Cancelled with its connection, and restricted to
    /// reading user tables.
    Session,
    /// Coordinator-owned maintenance, pinned to one replica.
    ///
    /// The caller must build the statement itself rather than accept one from a
    /// user, and must tolerate reading a log relation that is sealed empty,
    /// which is how a replica with introspection disabled presents one.
    Background { replica_id: ReplicaId },
}

impl RtwCaller {
    fn is_background(&self) -> bool {
        matches!(self, RtwCaller::Background { .. })
    }

    /// Which relations the selection may read.
    fn dependency_policy(&self) -> DependencyPolicy {
        match self {
            RtwCaller::Session => DependencyPolicy::UserDml,
            RtwCaller::Background { .. } => DependencyPolicy::SystemReads,
        }
    }

    /// The replica a background caller pins its subscribe to, overriding the
    /// session's replica selection.
    fn replica_override(&self) -> Option<ReplicaId> {
        match self {
            RtwCaller::Background { replica_id } => Some(*replica_id),
            RtwCaller::Session => None,
        }
    }

    /// Who owns the subscribe, which decides whether it is cancelled with a
    /// connection and whether it counts against one.
    fn subscribe_owner(
        &self,
        conn_id: &mz_adapter_types::connection::ConnectionId,
        session_uuid: Uuid,
    ) -> ActiveSubscribeOwner {
        match self {
            RtwCaller::Session => ActiveSubscribeOwner::Session {
                conn_id: conn_id.clone(),
                session_uuid,
            },
            RtwCaller::Background { .. } => ActiveSubscribeOwner::Background,
        }
    }

    /// The connection a pending write is cancelled with, if any.
    fn write_conn_id(
        &self,
        conn_id: &mz_adapter_types::connection::ConnectionId,
    ) -> Option<mz_adapter_types::connection::ConnectionId> {
        match self {
            RtwCaller::Session => Some(conn_id.clone()),
            RtwCaller::Background { .. } => None,
        }
    }
}

/// What the OCC loop produced.
enum OccOutcome {
    /// The write is durable at `write_ts`.
    Committed {
        response: ExecuteResponse,
        write_ts: Timestamp,
    },
    /// The selection was empty, so there was nothing to write.
    ///
    /// `empty_as_of` is the timestamp the emptiness holds at, and the caller must
    /// bring the oracle's read timestamp up to it before responding. `None` when
    /// the subscribe ran to completion, where the emptiness holds at every
    /// timestamp. See the module docs for why the choice of timestamp matters.
    NoRowsMatched {
        response: ExecuteResponse,
        empty_as_of: Option<Timestamp>,
    },
    /// Diffs no frontier can change, from a subscribe that ran to completion.
    /// The close says the selection can never change again, not that it reads
    /// nothing persisted, and either way the caller chooses whether to submit
    /// them now or buffer them into the transaction.
    Blind {
        response: ExecuteResponse,
        diffs: Vec<(Row, Diff)>,
    },
}

/// What the coordinator's answer to a submitted write means for the statement.
enum WriteOutcome {
    /// The write is durable at this timestamp.
    Committed(Timestamp),
    /// The write did not land, and resubmitting these diffs cannot change
    /// that. This is the error to report.
    Failed(AdapterError),
    /// Another writer advanced the target's upper past the timestamp we asked
    /// for. The diffs still describe the mutation, so the OCC loop can
    /// resubmit them once the subscribe has caught up.
    Conflict { next_eligible_timestamp: Timestamp },
}

/// Maps a [`WriteResult`] to the outcome the statement reports, or to the one
/// conflict the OCC loop can retry.
fn classify_write_result(
    result: WriteResult,
    target_id: CatalogItemId,
    attempt_state: &FrontendWriteAttemptState,
) -> WriteOutcome {
    match result {
        WriteResult::Success { timestamp } => WriteOutcome::Committed(timestamp),
        WriteResult::TimestampPassed {
            next_eligible_timestamp,
            ..
        } => WriteOutcome::Conflict {
            next_eligible_timestamp,
        },
        WriteResult::Canceled => WriteOutcome::Failed(
            attempt_state
                .requested_error()
                .unwrap_or(AdapterError::Canceled),
        ),
        WriteResult::TimestampTooFarAhead {
            target_timestamp,
            limit,
        } => WriteOutcome::Failed(AdapterError::ReadThenWriteTimestampTooFarAhead {
            target_timestamp,
            limit,
        }),
        WriteResult::ReadOnly => WriteOutcome::Failed(AdapterError::ReadOnly),
        WriteResult::TargetChanged => {
            // A concurrent DDL gave the table a new generation after we
            // computed these diffs against the old one. The same error the
            // coordinator raises when a dependency changes underneath a
            // statement, so clients see one retryable outcome for both.
            WriteOutcome::Failed(AdapterError::ConcurrentDependencyMutation {
                dependency_id: target_id.to_string(),
            })
        }
        WriteResult::Indeterminate => WriteOutcome::Failed(AdapterError::Internal(
            "write outcome is indeterminate because the group committer shut down".into(),
        )),
    }
}

/// Ends the implicit transaction that a statement which cannot run in a
/// transaction block opened for itself.
///
/// A read-then-write that reads persisted state is refused inside a transaction,
/// so it must not quietly become part of one. Clearing the ops it staged leaves
/// [`crate::session::TransactionStatus::may_span_pipeline`] false, so pgwire
/// commits the implicit transaction rather than letting the rest of an
/// extended-protocol pipeline join it. The write is already durable at this
/// point, and the caller has established there is nothing else staged to lose.
fn end_own_transaction(session: &mut Session, stages_rows: bool) {
    if !stages_rows {
        session.clear_transaction_ops();
    }
}

/// Checks that a read-then-write may read what its selection depends on.
///
/// An invalid selection is invalid wherever the statement runs, so a caller with
/// something contextual to report, such as the transaction state, must ask this
/// first. Reporting the transaction for a statement that can never work suggests
/// it would work outside one.
///
/// `catalog` must be the snapshot the plan was built against. A missing item
/// means the caller mixed snapshots, which is reported as a catalog error rather
/// than treated as a dropped dependency.
pub(crate) fn validate_selection_dependencies(
    catalog: &Catalog,
    depends_on: &BTreeSet<GlobalId>,
    policy: DependencyPolicy,
) -> Result<(), AdapterError> {
    let dependency_ids = depends_on
        .iter()
        .copied()
        .map(|gid| {
            catalog.try_resolve_item_id(&gid).ok_or_else(|| {
                AdapterError::Catalog(mz_catalog::memory::error::Error {
                    kind: ErrorKind::Sql(CatalogError::UnknownItem(gid.to_string())),
                })
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let max_rw_dependencies = mz_adapter_types::dyncfgs::READ_THEN_WRITE_MAX_DEPENDENCIES
        .get(catalog.system_config().dyncfgs());
    validate_read_then_write_dependencies(catalog, dependency_ids, max_rw_dependencies, policy)
}

/// Validates a read-then-write and resolves the context the rest of the
/// pipeline runs against.
///
/// Rejects `mz_now()` in the selection, the assignments or the returning
/// clause. `optimize_mir_read_then_write` relies on that rejection by name
/// when it prepares unmaterializable functions one-shot. Also enforces the
/// dependency cap, resolves the target cluster and requires it to have a
/// live replica, honors the session's replica pin, computes the read side's
/// `TimelineContext`, and fetches the target table's descriptor.
///
/// `catalog` must be the snapshot the plan was built against. One snapshot
/// serves planning, validation and optimization, so items the plan names
/// cannot disappear from it, and the missing-entry branches below are
/// failsafes rather than a live concurrent-DDL path.
///
/// `dependency_policy` decides which relations the selection may read. Both
/// policies reject `mz_now()` anywhere in the transitive dependencies.
fn validate_read_then_write(
    catalog: &Arc<Catalog>,
    session: &Session,
    plan: &plan::ReadThenWritePlan,
    target_cluster: TargetCluster,
    dependency_policy: DependencyPolicy,
) -> Result<ValidationResult, AdapterError> {
    if contains_mz_now(plan) {
        return Err(AdapterError::Unsupported(
            "calls to mz_now in write statements",
        ));
    }

    // One walk of the selection serves both the dependency check and the
    // timeline validation below.
    let depends_on = plan.selection.depends_on();

    validate_selection_dependencies(catalog, &depends_on, dependency_policy)?;

    let cluster = catalog.resolve_target_cluster(target_cluster, session)?;
    let cluster_id = cluster.id;

    if cluster.replicas().next().is_none() {
        return Err(AdapterError::NoClusterReplicasAvailable {
            name: cluster.name.clone(),
            is_managed: cluster.is_managed(),
        });
    }

    let replica_id = session
        .vars()
        .cluster_replica()
        .map(|name| {
            cluster
                .replica_id(name)
                .ok_or(AdapterError::UnknownClusterReplica {
                    cluster_name: cluster.name.clone(),
                    replica_name: name.to_string(),
                })
        })
        .transpose()?;

    let timeline = catalog.validate_timeline_context(depends_on.iter().copied())?;

    // The loop waits for the subscribe's frontier to reach a target timestamp
    // taken from the `EpochMilliseconds` oracle, and it is the target table's
    // upper the write then competes for. A selection in another timeline
    // counts something else, transactions rather than milliseconds for a CDCv2
    // source, so its frontier is not comparable with that target and the
    // statement would burn until `statement_timeout`. Refuse it up front
    // instead.
    //
    // Only `INSERT ... SELECT` reaches this. A DELETE or UPDATE selection
    // includes the target table, so a foreign timeline already fails above
    // as a mixed-timeline query.
    if let TimelineContext::TimelineDependent(t) = &timeline {
        if t != &Timeline::EpochMilliseconds {
            return Err(AdapterError::Unsupported(
                "read-then-write on a selection outside the EpochMilliseconds timeline",
            ));
        }
    }

    // Get the table descriptor for constraint validation. As above, a
    // missing entry would mean the snapshot contract was broken.
    let table_desc = match catalog.try_get_entry(&plan.id) {
        Some(entry) => entry
            .relation_desc_latest()
            .expect("table has desc")
            .into_owned(),
        None => {
            return Err(AdapterError::Catalog(mz_catalog::memory::error::Error {
                kind: ErrorKind::Sql(CatalogError::UnknownItem(plan.id.to_string())),
            }));
        }
    };

    Ok(ValidationResult {
        cluster_id,
        replica_id,
        timeline,
        depends_on,
        table_desc,
    })
}

/// Builds the response for a write that is about to be submitted.
///
/// This runs before the write, so the result-size checks in here reject the
/// statement without having written anything.
fn build_success_response(
    kind: &MutationKind,
    returning: &[MirScalarExpr],
    diffs: &[(Row, Diff)],
    max_result_size: u64,
    max_query_result_size: u64,
    row_set_finishing_seconds: &Histogram,
) -> Result<ExecuteResponse, AdapterError> {
    if returning.is_empty() {
        // For UPDATE each changed row produces a retraction (-1) and an
        // insertion (+1), so we divide by 2 below.
        let row_count = diffs
            .iter()
            .map(|(_, diff)| diff.into_inner().unsigned_abs())
            .sum::<u64>();
        let row_count = usize::try_from(row_count).expect("positive row count must fit in usize");

        return Ok(match kind {
            MutationKind::Delete => ExecuteResponse::Deleted(row_count),
            MutationKind::Update => ExecuteResponse::Updated(row_count / 2),
            MutationKind::Insert => ExecuteResponse::Inserted(row_count),
        });
    }

    let mut returning_rows = Vec::new();
    let arena = RowArena::new();
    // RETURNING expressions are evaluated row-by-row in this loop, so an
    // expression like `RETURNING repeat('x', 10_000_000)` will allocate
    // unbounded data unless we bail mid-loop. The post-loop
    // `RowSetFinishing::finish` below would also reject this, but only
    // after we've materialized everything. The early-bail caps the
    // temporary allocation. We pick the lower of the two configured caps,
    // whichever fires first wins.
    let mut projected_byte_size: u64 = 0;
    let early_cap = std::cmp::min(max_result_size, max_query_result_size);

    for (row, diff) in diffs {
        let include = match kind {
            MutationKind::Delete => diff.is_negative(),
            MutationKind::Update | MutationKind::Insert => diff.is_positive(),
        };

        if !include {
            continue;
        }

        let mut returning_row = Row::with_capacity(returning.len());
        let mut packer = returning_row.packer();
        let datums: Vec<_> = row.iter().collect();

        for expr in returning {
            match expr.eval(&datums, &arena) {
                Ok(datum) => packer.push(datum),
                Err(err) => return Err(err.into()),
            }
        }

        let multiplicity = NonZeroUsize::try_from(
            NonZeroI64::try_from(diff.into_inner().abs()).expect("diff is non-zero"),
        )
        .map_err(AdapterError::from)?;

        let row_bytes = u64::cast_from(returning_row.byte_len())
            .saturating_mul(u64::cast_from(multiplicity.get()));
        projected_byte_size = projected_byte_size.saturating_add(row_bytes);
        if projected_byte_size > early_cap {
            return Err(AdapterError::ResultSize(format!(
                "result exceeds max size of {}",
                ByteSize::b(early_cap)
            )));
        }

        returning_rows.push((returning_row, multiplicity));
    }

    // Run the canonical finish to enforce both caps with full precision
    // (including the sorted-view memory overhead) and to register the
    // row-set-finishing duration histogram, mirroring the legacy
    // `send_diffs` path.
    let finishing = RowSetFinishing {
        order_by: Vec::new(),
        limit: None,
        offset: 0,
        project: (0..returning.len()).collect(),
    };
    match finishing.finish(
        RowCollection::new(returning_rows, &finishing.order_by),
        max_result_size,
        Some(max_query_result_size),
        row_set_finishing_seconds,
    ) {
        Ok((rows, _size_bytes)) => Ok(ExecuteResponse::SendingRowsImmediate {
            rows: Box::new(rows),
        }),
        Err(e) => Err(AdapterError::ResultSize(e)),
    }
}

/// Whether a read-then-write mentions `mz_now()` anywhere.
///
/// Read time and write time differ on this path, so `mz_now()` has no single
/// answer and is refused in every position.
pub(crate) fn contains_mz_now(plan: &plan::ReadThenWritePlan) -> bool {
    plan.selection.contains_temporal()
        || plan.assignments.values().any(|e| e.contains_temporal())
        || plan.returning.iter().any(|e| e.contains_temporal())
}

/// The timeline whose oracle governs a read-then-write with the given read-side
/// [`TimelineContext`].
///
/// The write target is always a table on `EpochMilliseconds`, so a read side
/// that pins no timeline (`TimestampDependent`) still maps to
/// `EpochMilliseconds`. `None` only for timestamp-independent selections, which
/// need no oracle at all.
fn governing_timeline(timeline: &TimelineContext) -> Option<Timeline> {
    <Coordinator as TimestampProvider>::get_timeline(timeline)
}

/// A handle to an internal subscribe, meaning one that writes no
/// `mz_subscriptions` row. A `Drop` impl ensures the subscribe's dataflow is
/// cleaned up when dropped.
struct SubscribeHandle {
    rx: mpsc::UnboundedReceiver<PeekResponseUnary>,
    sink_id: GlobalId,
    /// Wrapped in `Option` so we can move it out in `Drop`.
    client: Option<CoordinatorClient>,
}

impl SubscribeHandle {
    /// Receive the next message from the subscribe, waiting if necessary.
    pub async fn recv(&mut self) -> Option<PeekResponseUnary> {
        self.rx.recv().await
    }

    /// Try to receive a message without waiting.
    pub fn try_recv(&mut self) -> Result<PeekResponseUnary, mpsc::error::TryRecvError> {
        self.rx.try_recv()
    }
}

impl Drop for SubscribeHandle {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            // Fire-and-forget: if the coordinator is gone, the subscribe will
            // be cleaned up when the process exits anyway.
            client.try_send(Command::DropInternalSubscribe {
                sink_id: self.sink_id,
            });
        }
    }
}

impl PeekClient {
    /// Execute a read-then-write operation using frontend sequencing.
    ///
    /// Called by session code when the frontend_read_then_write dyncfg is
    /// enabled. The caller owns the end-of-execution logging for
    /// `statement_logging_id` and verified and planned the portal against
    /// `catalog`, which stays in force through optimization and write-target
    /// generation capture.
    pub(crate) async fn frontend_read_then_write(
        &mut self,
        session: &mut Session,
        plan: plan::ReadThenWritePlan,
        target_cluster: TargetCluster,
        catalog: &Arc<Catalog>,
        statement_logging_id: Option<StatementLoggingId>,
        attempt_state: Arc<FrontendWriteAttemptState>,
    ) -> Result<ExecuteResponse, AdapterError> {
        self.read_then_write(
            session,
            plan,
            target_cluster,
            catalog,
            statement_logging_id,
            attempt_state,
            RtwCaller::Session,
        )
        .await
    }

    /// Executes a coordinator-owned read-then-write against system relations,
    /// pinned to `replica_id`.
    ///
    /// See `RtwCaller::Background` for what the caller takes on by using this.
    pub(crate) async fn background_read_then_write(
        &mut self,
        session: &mut Session,
        plan: plan::ReadThenWritePlan,
        cluster_id: ComputeInstanceId,
        replica_id: ReplicaId,
        catalog: &Arc<Catalog>,
    ) -> Result<ExecuteResponse, AdapterError> {
        self.read_then_write(
            session,
            plan,
            TargetCluster::Transaction(cluster_id),
            catalog,
            None,
            // Nothing cancels a background write, so this state only ever
            // records that a write was submitted.
            Arc::new(FrontendWriteAttemptState::new()),
            RtwCaller::Background { replica_id },
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn read_then_write(
        &mut self,
        session: &mut Session,
        mut plan: plan::ReadThenWritePlan,
        target_cluster: TargetCluster,
        catalog: &Arc<Catalog>,
        statement_logging_id: Option<StatementLoggingId>,
        attempt_state: Arc<FrontendWriteAttemptState>,
        caller: RtwCaller,
    ) -> Result<ExecuteResponse, AdapterError> {
        // A transaction that has taken a timestamped read, was opened READ
        // ONLY, or is committed to some other kind of operation cannot take a
        // write. Check up front, mirroring `sequence_insert`: the marker op
        // below rejects only some of those states, and only with its own
        // errors, so without this check the reported error and SQLSTATE would
        // depend on which path sequenced the statement.
        //
        // Both this and the marker op require an open transaction. The
        // frontends start one before they execute anything, and a `Failed`
        // transaction only ever admits COMMIT/ROLLBACK, so DML never arrives
        // in a state where these panic.
        if !session.transaction().allows_writes() {
            return Err(AdapterError::ReadOnlyTransaction);
        }

        let validation_result = validate_read_then_write(
            catalog,
            session,
            &plan,
            target_cluster,
            caller.dependency_policy(),
        )?;

        let ValidationResult {
            cluster_id,
            mut replica_id,
            timeline,
            depends_on,
            table_desc,
        } = validation_result;
        if let Some(pinned) = caller.replica_override() {
            replica_id = Some(pinned);
        }

        // A write that reads no persisted state may join a surrounding
        // transaction. Its rows do not come from a snapshot, so staging them
        // and letting the transaction flush them keeps them atomic with
        // everything else the transaction does.
        //
        // A write that does read persisted state may not. We refuse it in an
        // explicit transaction, and an extended-protocol pipeline is an
        // implicit transaction, so it must not silently span one either. It
        // runs as its own transaction instead, which is how PostgreSQL treats
        // statements that cannot run in a transaction block.
        let stages_rows = depends_on.is_empty();

        // Snapshot this before the marker op below, which makes the predicate
        // true unconditionally.
        let in_transaction = session
            .transaction()
            .may_share_transaction_with_other_statements();

        if !stages_rows && in_transaction {
            // Defense in depth for the gate in
            // `SessionClient::try_frontend_read_then_write`. Rejecting here,
            // before we run a dataflow, is the last point where refusing is
            // still possible: past the OCC loop the write may already be
            // durable.
            soft_panic_or_log!(
                "read-dependent read-then-write reached the OCC path inside a transaction"
            );
            return Err(AdapterError::Internal(
                "read-then-write cannot be run inside a transaction block".into(),
            ));
        }

        // Mark this as a write transaction in the session state machine, so
        // auto-commit treats the statement as a write. The rows follow once we
        // know them.
        session.add_transaction_ops(TransactionOps::Writes(vec![]))?;

        // Prepare expressions (resolve unmaterializable functions like
        // current_user())
        let style = ExprPrepOneShot {
            logical_time: EvalTime::NotAvailable, // We already errored out on mz_now above.
            session,
            catalog_state: catalog.state(),
        };
        for expr in plan
            .assignments
            .values_mut()
            .chain(plan.returning.iter_mut())
        {
            style.prep_scalar_expr(expr)?;
        }

        let (mut optimizer, global_mir_plan) =
            self.optimize_mir_read_then_write(catalog, session, &plan, cluster_id)?;

        // Acquire the OCC semaphore permit *before* acquiring read holds in
        // `frontend_determine_timestamp`. Under contention, waiters will
        // otherwise sit on read holds on the RTW's read dependencies for the
        // entire time they are queued, pinning compaction on those
        // collections. Waiting on the permit first keeps queued operations
        // hold-free. Once we have a permit we proceed to acquire the read holds
        // needed for the rest of the operation.
        //
        // The cost of this ordering is that a permit held by a long-running
        // operation stalls every read-then-write in the process, including ones
        // on unrelated tables, where the coordinator's write lock would only
        // stall writes to the target table. We accept that because the
        // statement timeout in
        // `SessionClient::try_frontend_read_then_write_with_cancel` covers the
        // permit wait, so the stall is bounded for everyone but a session that
        // disabled its own timeout.
        //
        // The semaphore is owned by the coordinator and outlives every
        // session task, so `acquire_owned` cannot return `Err` in practice.
        //
        // Background maintenance skips the queue entirely. It is single-flight
        // by construction, one sweep at a time and one mutation at a time, so it
        // adds at most one concurrent read-then-write. Taking a permit instead
        // would let it hold one for as long as a subscribe on a loaded user
        // replica takes to hydrate, and with `max_concurrent_occ_writes` set low
        // that stalls user DML behind a background sampler. The bound above does
        // not apply to it either: it has no statement timeout, only its own much
        // longer one.
        let permit = if caller.is_background() {
            None
        } else {
            Some(
                Arc::clone(&self.occ_write_semaphore)
                    .acquire_owned()
                    .await
                    .expect("semaphore is never closed during coordinator lifetime"),
            )
        };

        // Determine timestamp and acquire read holds.
        let oracle_read_ts = self.oracle_read_ts(&timeline).await?;

        // Real-time recency, on the same terms as the frontend peek path. The
        // coordinator round trip polls the selection's upstream sources, so we
        // only pay it when the session actually asked for recency.
        let vars = session.vars();
        let real_time_recency_ts: Option<Timestamp> = if vars.real_time_recency()
            && vars.transaction_isolation() == &IsolationLevel::StrictSerializable
            && !session.contains_read_timestamp()
        {
            let real_time_recency_timeout = *vars.real_time_recency_timeout();
            self.call_coordinator(|tx| Command::DetermineRealTimeRecentTimestamp {
                source_ids: depends_on.iter().copied().collect(),
                real_time_recency_timeout,
                tx,
            })
            .await??
        } else {
            None
        };

        let bundle = global_mir_plan.id_bundle(cluster_id);
        let (determination, read_holds) = self
            .frontend_determine_timestamp(
                session,
                &bundle,
                &QueryWhen::FreshestTableWrite,
                cluster_id,
                &timeline,
                oracle_read_ts,
                real_time_recency_ts,
            )
            .await?;

        let as_of = determination.timestamp_context.timestamp_or_default();

        let global_mir_plan = global_mir_plan.resolve(Antichain::from_elem(as_of));
        let global_lir_plan = optimizer.optimize(global_mir_plan)?;

        // Log optimization finished
        if let Some(logging_id) = statement_logging_id {
            self.log_lifecycle_event(logging_id, StatementLifecycleEvent::OptimizationFinished);
        }

        let sink_id = global_lir_plan.sink_id();
        let target_id = plan.id;
        let target_global_id = catalog.get_entry(&target_id).latest_global_id();
        let kind = plan.kind.clone();
        let returning = plan.returning.clone();

        let (df_desc, df_meta) = global_lir_plan.unapply();

        // The coordinator sequences this statement's read as a real peek, so the
        // optimizer's notices and the timestamp notice reach the session there.
        // Emit both here for the same statement to look the same on either path.
        crate::coord::sequencer::emit_optimizer_notices(
            &**catalog,
            session,
            &df_meta.optimizer_notices,
        );
        if session.vars().emit_timestamp_notice() {
            let conn_id = session.conn_id().clone();
            let session_wall_time = session.pcx().wall_time;
            let explanation = self
                .call_coordinator(|tx| Command::ExplainTimestamp {
                    conn_id,
                    session_wall_time,
                    cluster_id,
                    id_bundle: bundle,
                    determination,
                    tx,
                })
                .await?;
            session.add_notice(crate::AdapterNotice::QueryTimestamp { explanation });
        }

        let arity = df_desc
            .sink_exports
            .values()
            .next()
            .expect("has sink")
            .from_desc
            .arity();

        let conn_id = session.conn_id().clone();
        let session_uuid = session.uuid();
        let start_time = (self.statement_logging_frontend.now)();
        let max_result_size = catalog.system_config().max_result_size();
        let max_query_result_size = session.vars().max_query_result_size();
        let row_set_finishing_seconds = session.metrics().row_set_finishing_seconds().clone();
        let max_occ_retries = usize::cast_from(catalog.system_config().max_occ_retries());

        // Linearize the read BEFORE subscribing or writing: block until
        // the oracle for this query's timeline has advanced to `as_of`.
        //
        // Ordering is load-bearing: this leaves the oracle at or above `as_of`,
        // which is what makes the loop's target clear `as_of` and so include the
        // snapshot. A far-future `as_of` parks here until the clock arrives,
        // bounded by `statement_timeout`.
        self.ensure_read_linearized(&timeline, as_of).await?;

        // The loop takes its write target from this oracle, and reaching one takes
        // `&mut self`, which the loop does not have. `None` for a
        // timestamp-independent selection, which reads at `Timestamp::maximum()`
        // and so always leaves through the blind path rather than reaching a write.
        let write_oracle = match governing_timeline(&timeline) {
            Some(tl) => Some(Arc::clone(self.ensure_oracle(tl).await?)),
            None => None,
        };

        let subscribe_handle = self
            .create_internal_subscribe(
                Box::new(df_desc),
                cluster_id,
                replica_id,
                depends_on.clone(),
                as_of,
                arity,
                sink_id,
                caller.subscribe_owner(&conn_id, session_uuid),
                start_time,
                read_holds,
            )
            .await?;

        let (retry_count, result) = self
            .run_occ_loop(
                subscribe_handle,
                target_id,
                target_global_id,
                kind,
                returning,
                max_result_size,
                max_query_result_size,
                row_set_finishing_seconds,
                max_occ_retries,
                table_desc,
                caller.write_conn_id(&conn_id),
                statement_logging_id,
                as_of,
                write_oracle,
                &attempt_state,
            )
            .await;

        self.coordinator_client()
            .metrics()
            .occ_retry_count
            .observe(f64::from(u32::try_from(retry_count).unwrap_or(u32::MAX)));

        // Finish the operation, including a blind write's submission, before
        // releasing the OCC permit. Holding it for the entire operation is what
        // bounds concurrency. An early drop would let a waiter start its
        // subscribe while we are still consolidating diffs, retrying, or
        // waiting for our write to commit.
        //
        // The zero-row linearization wait below is the one exception, and hands
        // the permit back before it parks.
        let mut permit = permit;
        let response = match result {
            Ok(OccOutcome::Committed { response, write_ts }) => {
                // A committed write timestamp for a statement we meant to
                // stage means the two predicates disagreed: the syntactic one
                // said it reads nothing, the subscribe then read persisted
                // state. The write is already durable, so there is nothing to
                // refuse, and `apply_write` still has to run to keep the
                // session's read timestamps ahead of it.
                soft_assert_or_log!(
                    !stages_rows,
                    "read-then-write committed a write it meant to stage"
                );
                session.apply_write(write_ts);
                end_own_transaction(session, stages_rows);
                Ok(response)
            }
            Ok(OccOutcome::NoRowsMatched {
                response,
                empty_as_of,
            }) => {
                // An `empty_as_of` for a statement we meant to stage is the
                // same predicate disagreement the `Committed` arm guards
                // against: the syntactic answer said it reads nothing, the
                // subscribe then read persisted state. Nothing is durable here,
                // so there is nothing to undo, but the disagreement itself is
                // the bug and it would otherwise park silently.
                soft_assert_or_log!(
                    !(stages_rows && empty_as_of.is_some()),
                    "read-then-write observed a read timestamp for a statement \
                     it meant to stage"
                );
                end_own_transaction(session, stages_rows);
                match empty_as_of {
                    Some(empty_as_of) => {
                        // The wait is a no-op where the oracle is already past
                        // `empty_as_of`, which is the common `WHERE <no match>`,
                        // and otherwise costs the group commit that
                        // `ensure_read_linearized` asks for. Either way the
                        // subscribe handle is gone, so the permit guards nothing
                        // and holding it would throttle unrelated writes.
                        drop(permit.take());
                        self.ensure_read_linearized(&timeline, empty_as_of)
                            .await
                            .map(|()| response)
                    }
                    None => Ok(response),
                }
            }
            Ok(OccOutcome::Blind { response, diffs }) if stages_rows => {
                // Staging rather than writing here is what makes the statement
                // atomic with its transaction. An extended-protocol pipeline is
                // an implicit transaction, and it may still fail after us, so a
                // write of our own would survive a rollback that discards
                // everything around it.
                //
                // NOTE: A staged session write carries no target-generation
                // guard the way the immediate path's `target_global_id` does. A
                // `WriteOp` only names the `CatalogItemId`, and commit staging
                // resolves whatever global id is current then. What keeps it
                // safe is a check at the far end: group commit compares the
                // arity of the rows each staged write carries against the
                // target's latest `RelationDesc` and rolls the transaction back
                // with `ConcurrentDependencyMutation` instead of encoding old
                // rows against a new schema. So an `ALTER TABLE ... ADD COLUMN`
                // landing between here and the commit becomes the same
                // retryable failure the immediate path reports as
                // `TargetChanged`. The comparison reads one row per staged
                // write and looks only at arity, so it stands in for the
                // descriptor rather than pinning it.
                //
                // Missing the pin is true of every staged write. The arity
                // check is not: rows staged as a batch, which is how `COPY
                // FROM` arrives, carry their schema into persist instead.
                session
                    .add_transaction_ops(TransactionOps::Writes(vec![WriteOp {
                        id: target_id,
                        rows: TableData::Rows(diffs),
                    }]))
                    .map(|()| response)
            }
            Ok(OccOutcome::Blind { response, diffs }) => {
                if caller.is_background() {
                    return Err(AdapterError::Internal(
                        "background read-then-write unexpectedly had no persisted dependency"
                            .into(),
                    ));
                }
                // The subscribe closed on its own even though the selection
                // reads persisted state, so the input is sealed and the diffs
                // are frontier-independent after all. Staging them would be
                // safe for that reason, and we still do not, because it would
                // make a statement's transaction semantics depend on whether an
                // input happens to be past its last refresh. What the statement
                // reads decides, so this commits as its own transaction like
                // any other read-dependent write.
                match self
                    .submit_blind_write(
                        conn_id,
                        target_id,
                        target_global_id,
                        diffs,
                        statement_logging_id,
                        &attempt_state,
                    )
                    .await
                {
                    Ok(write_ts) => {
                        session.apply_write(write_ts);
                        end_own_transaction(session, stages_rows);
                        Ok(response)
                    }
                    Err(err) => Err(err),
                }
            }
            Err(err) => Err(err),
        };

        drop(permit);

        response
    }

    /// Builds the subscribe optimizer and the unresolved global MIR plan for a
    /// read-then-write.
    ///
    /// The optimized expression is the selection with the mutation already
    /// applied, so the subscribe's sink emits ready-to-write table diffs rather
    /// than query results. `finishing` and `returning` are deliberately not part
    /// of the dataflow, and unmaterializable functions are prepared one-shot.
    fn optimize_mir_read_then_write(
        &self,
        catalog: &Arc<Catalog>,
        session: &dyn SessionMetadata,
        plan: &plan::ReadThenWritePlan,
        cluster_id: ComputeInstanceId,
    ) -> Result<
        (
            optimize::subscribe::Optimizer,
            optimize::subscribe::GlobalMirPlan<optimize::subscribe::Unresolved>,
        ),
        AdapterError,
    > {
        // `finishing` is unused: the OCC path emits raw diffs and
        // `apply_mutation_to_mir` handles update projection.
        let plan::ReadThenWritePlan {
            id: _,
            selection,
            finishing: _,
            assignments,
            kind,
            returning: _,
        } = plan;

        let expr = selection.clone().lower(catalog.system_config(), None)?;
        let mut expr = apply_mutation_to_mir(expr, kind, assignments);

        // Resolve unmaterializable functions (now(), current_user, ...) before
        // the subscribe optimizer sees them: it uses `ExprPrepMaintained`,
        // which rejects them, but our subscribe is a one-shot read so we can
        // resolve them to constants. `mz_now()` is rejected upstream by
        // `validate_read_then_write`.
        let style = ExprPrepOneShot {
            logical_time: EvalTime::NotAvailable,
            session,
            catalog_state: catalog.state(),
        };
        expr.try_visit_scalars_mut(&mut |s| style.prep_scalar_expr(s))?;

        let compute_instance = ComputeInstanceSnapshot::new_without_collections(cluster_id);
        let (_, view_id) = self.transient_id_gen.allocate_id();
        let (_, sink_id) = self.transient_id_gen.allocate_id();
        let debug_name = format!("frontend-read-then-write-subscribe-{}", sink_id);
        let optimizer_config = optimize::OptimizerConfig::from(catalog.system_config())
            .override_from(&catalog.get_cluster(cluster_id).config.features())
            .override_from(
                &catalog
                    .state()
                    .cluster_scoped_optimizer_overrides(cluster_id),
            );

        let mut optimizer = optimize::subscribe::Optimizer::new(
            Arc::<Catalog>::clone(catalog),
            compute_instance,
            view_id,
            sink_id,
            true, // with_snapshot
            None, // up_to
            debug_name,
            optimizer_config,
            self.optimizer_metrics.clone(),
        );

        let expr_typ = expr.typ();
        let sql_typ = mz_repr::SqlRelationType::from_repr(&expr_typ);
        let column_names: Vec<String> = (0..sql_typ.column_types.len())
            .map(|i| format!("column{}", i))
            .collect();
        let relation_desc = RelationDesc::new(sql_typ, column_names.iter().map(|s| s.as_str()));

        // MIR ⇒ MIR optimization (global). The mutation is already applied in
        // MIR, so we hand the expression to the subscribe optimizer directly
        // instead of going through the `SubscribePlan` path, which expects HIR.
        // An empty `output` makes the sink emit raw diffs.
        let global_mir_plan = optimizer.optimize_query(expr, relation_desc, vec![])?;

        Ok((optimizer, global_mir_plan))
    }

    /// The governing oracle's read timestamp, used as a lower bound for
    /// timestamp selection. `None` when the selection needs no oracle.
    async fn oracle_read_ts(
        &mut self,
        timeline: &TimelineContext,
    ) -> Result<Option<Timestamp>, AdapterError> {
        match governing_timeline(timeline) {
            Some(timeline) => {
                let oracle = self.ensure_oracle(timeline).await?;
                Ok(Some(oracle.read_ts().await))
            }
            None => Ok(None),
        }
    }

    /// Block until the oracle for this query's timeline has advanced to
    /// `as_of`. Returns immediately if it already has.
    ///
    /// This implements the strict-serializable read guarantee for RTW:
    /// once this returns, any session observing the oracle sees a read
    /// timestamp at least as large as `as_of`, so reads at `as_of` (and
    /// writes derived from them) cannot appear to "go backwards" relative
    /// to subsequent queries.
    async fn ensure_read_linearized(
        &mut self,
        timeline: &TimelineContext,
        as_of: Timestamp,
    ) -> Result<(), AdapterError> {
        // Linearization must target the oracle future readers of the target
        // table will consult, which is why this uses `governing_timeline` and
        // not `TimelineContext::timeline()`. The latter answers "is there a
        // source-forced timeline?" and would skip linearization entirely for a
        // read side that pins none.
        let tl = match governing_timeline(timeline) {
            Some(tl) => tl,
            None => return Ok(()),
        };

        // Cloned before `ensure_oracle` borrows `self` for the rest of this
        // function. The handle is an `Arc` internally, so this is cheap.
        let group_commit_notifier = self.group_commit_notifier.clone();
        let oracle = self.ensure_oracle(tl).await?;

        // The oracle advances only when a group commit applies, and an empty
        // group commit is already the periodic keepalive. So when we have
        // nothing to write ourselves, waiting for the next tick costs up to a
        // full `default_timestamp_interval`. We ask for that commit instead of
        // waiting for it, which also spares the oracle the ~1ms poll below
        // running for the whole interval.
        //
        // Once per wait rather than once per poll. The committer never
        // allocates a write timestamp above wall clock, so a far-future `as_of`
        // cannot be reached by asking, and nudging per iteration would spin for
        // as long as such a statement legitimately parks. That case pays one
        // empty commit, which is what the keepalive would have done anyway.
        let mut nudged = false;

        loop {
            let oracle_ts = oracle.read_ts().await;
            if as_of <= oracle_ts {
                return Ok(());
            }

            if !nudged {
                group_commit_notifier.notify();
                nudged = true;
            }

            // Sleep for roughly the difference between as_of and the current
            // oracle timestamp. Since timestamps are epoch milliseconds, the
            // difference is the approximate wall-clock time we need to wait.
            // Cap at 1s to avoid very long sleeps if clocks are skewed,
            // matching the cap in `message_linearize_reads`.
            let wait_ms = u64::from(as_of.saturating_sub(oracle_ts));
            let wait = Duration::from_millis(wait_ms).min(Duration::from_secs(1));
            tokio::time::sleep(wait).await;
        }
    }

    /// Submits frontier-independent diffs to group commit, which picks the
    /// write timestamp, and returns the timestamp the write committed at.
    ///
    /// Only valid for diffs that do not depend on an observed read frontier:
    /// the write lands at a timestamp this caller does not choose.
    async fn submit_blind_write(
        &self,
        conn_id: mz_adapter_types::connection::ConnectionId,
        target_id: CatalogItemId,
        target_global_id: GlobalId,
        diffs: Vec<(Row, Diff)>,
        statement_logging_id: Option<StatementLoggingId>,
        attempt_state: &FrontendWriteAttemptState,
    ) -> Result<Timestamp, AdapterError> {
        attempt_state.mark_write_submitted();
        let result = self
            .call_coordinator(|tx| Command::AttemptWrite {
                attempt: WriteAttemptKind::Session {
                    conn_id,
                    write_ts: None,
                },
                target_id,
                target_global_id,
                diffs,
                tx,
            })
            .await?;

        // Every outcome here terminates the attempt, so `write_submitted`
        // stays set per its contract.
        match classify_write_result(result, target_id, attempt_state) {
            WriteOutcome::Committed(timestamp) => {
                if let Some(id) = statement_logging_id {
                    self.log_set_timestamp(id, timestamp);
                }
                Ok(timestamp)
            }
            WriteOutcome::Failed(err) => Err(err),
            WriteOutcome::Conflict { .. } => {
                // Unreachable: a write that requests no timestamp cannot have
                // one pass. Group commit resolves it through
                // `UserWriteResponder::Internal`, which only reports a conflict
                // to a write that asked for a specific timestamp.
                soft_panic_or_log!("blind read-then-write unexpectedly got TimestampPassed");
                Err(AdapterError::Internal(
                    "blind write unexpectedly got TimestampPassed".into(),
                ))
            }
        }
    }

    /// Creates an internal subscribe, meaning one that writes no
    /// `mz_subscriptions` row. Returns a [`SubscribeHandle`] that ensures
    /// cleanup on drop.
    async fn create_internal_subscribe(
        &self,
        df_desc: Box<optimize::LirDataflowDescription>,
        cluster_id: ComputeInstanceId,
        replica_id: Option<ReplicaId>,
        depends_on: BTreeSet<GlobalId>,
        as_of: Timestamp,
        arity: usize,
        sink_id: GlobalId,
        owner: ActiveSubscribeOwner,
        start_time: mz_ore::now::EpochMillis,
        read_holds: crate::ReadHolds,
    ) -> Result<SubscribeHandle, AdapterError> {
        let rx: mpsc::UnboundedReceiver<PeekResponseUnary> = self
            .call_coordinator(|tx| Command::CreateInternalSubscribe {
                df_desc,
                cluster_id,
                replica_id,
                depends_on,
                as_of,
                arity,
                sink_id,
                owner,
                start_time,
                read_holds,
                tx,
            })
            .await??;

        Ok(SubscribeHandle {
            rx,
            sink_id,
            client: Some(self.coordinator_client().clone()),
        })
    }

    /// Run the OCC loop: drain the subscribe at `as_of`, apply the
    /// mutation, and submit the resulting diffs as a write.
    ///
    /// Semantically a SELECT at `target - 1` followed by an INSERT at `target`.
    /// `write_oracle` chooses `target`, the subscribe's frontier certifies the
    /// payload is complete below it, and a target the target table has moved
    /// past comes back as `WriteResult::TimestampPassed`, whose next eligible
    /// timestamp the loop adopts. At most `max_occ_retries` attempts.
    ///
    /// A subscribe that ends on its own has diffs no frontier can change, and
    /// those are returned as [`OccOutcome::Blind`] rather than written.
    ///
    /// Contract on the caller, both ends of the read: the oracle's read
    /// timestamp must be at or above `as_of` on entry, and an
    /// [`OccOutcome::NoRowsMatched`] must be linearized against its
    /// `empty_as_of` before the response goes out.
    ///
    /// `write_oracle` is `None` only for a timestamp-independent selection. Such
    /// a statement reads at `Timestamp::maximum()`, so it observes no progress
    /// past its `as_of` and always leaves through the blind path.
    ///
    /// Returns `(retry_count, result)` so the caller can record OCC retry
    /// metrics regardless of whether the operation succeeded or failed.
    async fn run_occ_loop(
        &self,
        mut subscribe_handle: SubscribeHandle,
        target_id: CatalogItemId,
        target_global_id: GlobalId,
        kind: MutationKind,
        returning: Vec<MirScalarExpr>,
        max_result_size: u64,
        max_query_result_size: u64,
        row_set_finishing_seconds: Histogram,
        max_occ_retries: usize,
        table_desc: RelationDesc,
        write_conn_id: Option<mz_adapter_types::connection::ConnectionId>,
        statement_logging_id: Option<StatementLoggingId>,
        as_of: Timestamp,
        write_oracle: Option<Arc<dyn TimestampOracle<Timestamp> + Send + Sync>>,
        attempt_state: &FrontendWriteAttemptState,
    ) -> (usize, Result<OccOutcome, AdapterError>) {
        let mut state = OccState::new();

        // The timestamp the next attempt writes at, chosen when we are first
        // ready to attempt one and replaced only by a conflict. `None` until
        // then.
        let mut write_target: Option<Timestamp> = None;

        // The smallest timestamp an attempt may target. `as_of` itself is out,
        // since the payload has to contain the snapshot the subscribe emits
        // there.
        //
        // `as_of` is `Timestamp::MAX` for a selection with no timestamp at all,
        // which is the selection whose subscribe closes on its own and leaves
        // through the blind path rather than a write. There is no timestamp
        // above `MAX`, so saturating keeps this total instead of asserting a
        // property of a value the write path never uses.
        let min_target = as_of.try_step_forward().unwrap_or(as_of);

        // Retry invariant: the payload is the selection consolidated at
        // `target - 1`, and the diffs at or above `target` are concurrent with
        // the write, so a retry folds them in only once it raises the target.
        let result = loop {
            if let Some(error) = attempt_state.requested_error() {
                break Err(error);
            }

            // Before a target is chosen, `min_target` is a lower bound on it, so
            // folding there consolidates the snapshot without admitting a diff a
            // write would have to treat as concurrent.
            let fold_target = write_target.unwrap_or(min_target);

            // Already certified for the target we hold? Write. Otherwise wait for
            // the next subscribe message. Waiting first would hang after a
            // conflict, since an input settled until its next refresh sends
            // nothing further and does not close the channel either.
            //
            // Termination: the write arm awaits a round trip and only a conflict
            // returns to this one, raising `retry_count` towards
            // `max_occ_retries`, so neither arm spins.
            let attempt_write = match write_target {
                Some(target) if state.current_upper.is_some_and(|upper| upper >= target) => true,
                _ => {
                    let msg = match subscribe_handle.recv().await {
                        Some(msg) => msg,
                        None => {
                            // The channel closed cleanly, which says the
                            // selection can never change again, so these diffs
                            // are frontier-independent. It does not say the
                            // selection reads nothing persisted, a sealed
                            // `REFRESH AT` MV closes cleanly too. Either way no
                            // target separates them, and the caller decides
                            // where they go.
                            state.fold_all();
                            if state.payload.is_empty() {
                                break Ok(OccOutcome::NoRowsMatched {
                                    response: build_no_rows_response(&kind),
                                    empty_as_of: None,
                                });
                            }
                            let success_response = match build_success_response(
                                &kind,
                                &returning,
                                &state.payload,
                                max_result_size,
                                max_query_result_size,
                                &row_set_finishing_seconds,
                            ) {
                                Ok(response) => response,
                                Err(e) => break Err(e),
                            };

                            break Ok(OccOutcome::Blind {
                                response: success_response,
                                diffs: std::mem::take(&mut state.payload),
                            });
                        }
                    };

                    match process_message(
                        msg,
                        &mut state,
                        as_of,
                        fold_target,
                        max_result_size,
                        &table_desc,
                    ) {
                        ProcessResult::Continue { ready_to_write } => ready_to_write,
                        ProcessResult::NoRowsMatched { empty_as_of } => {
                            break Ok(OccOutcome::NoRowsMatched {
                                response: build_no_rows_response(&kind),
                                empty_as_of: Some(empty_as_of),
                            });
                        }
                        ProcessResult::Error(e) => break Err(e),
                    }
                }
            };

            if !attempt_write {
                continue;
            }

            // Drain buffered messages before attempting the write.
            let drain_err = loop {
                match subscribe_handle.try_recv() {
                    Ok(msg) => {
                        match process_message(
                            msg,
                            &mut state,
                            as_of,
                            fold_target,
                            max_result_size,
                            &table_desc,
                        ) {
                            ProcessResult::Continue { .. } => {}
                            ProcessResult::NoRowsMatched { empty_as_of } => {
                                break Some(Ok(OccOutcome::NoRowsMatched {
                                    response: build_no_rows_response(&kind),
                                    empty_as_of: Some(empty_as_of),
                                }));
                            }
                            ProcessResult::Error(e) => {
                                break Some(Err(e));
                            }
                        }
                    }
                    Err(mpsc::error::TryRecvError::Empty) => break None,
                    // The subscribe can finish (coordinator drops the sender
                    // after `process_response` returns true) between our last
                    // recv() and this drain. This is benign, all buffered
                    // messages have already been consumed via the Ok(msg) arm
                    // above.
                    Err(mpsc::error::TryRecvError::Disconnected) => break None,
                }
            };
            if let Some(result) = drain_err {
                break result;
            }

            let upper = state
                .current_upper
                .expect("a write attempt requires an observed frontier");

            let target = match write_target {
                Some(target) => target,
                None => {
                    let Some(oracle) = &write_oracle else {
                        // Invariant: a statement with no governing timeline
                        // reads at `as_of == Timestamp::maximum()`, so it
                        // observes no progress past its `as_of` and leaves
                        // through the blind arm above rather than reaching a
                        // write.
                        soft_panic_or_log!(
                            "read-then-write reached a write attempt with no governing timeline"
                        );
                        break Err(AdapterError::Internal(
                            "read-then-write has no oracle to take a write timestamp from".into(),
                        ));
                    };

                    // One step above the oracle's write timestamp is the smallest
                    // value `commit_timestamped` accepts.
                    let peek_write_ts = oracle.peek_write_ts().await;
                    let Some(chosen) = peek_write_ts.try_step_forward() else {
                        // A timeline that reached `Timestamp::MAX` is a broken
                        // environment, not anything this statement did.
                        soft_panic_or_log!(
                            "read-then-write cannot target a timestamp above the write \
                             timeline's timestamp {peek_write_ts}"
                        );
                        break Err(AdapterError::Internal(format!(
                            "write timeline exhausted at timestamp {peek_write_ts}"
                        )));
                    };

                    // Unreachable while the oracle's read timestamp is at or
                    // above `as_of` on entry, and the clamp keeps the payload
                    // rule rather than only reporting the violation.
                    if chosen < min_target {
                        soft_panic_or_log!(
                            "read-then-write target {chosen} does not clear the as_of {as_of}, \
                             so the payload would miss the snapshot"
                        );
                    }
                    let chosen = std::cmp::max(chosen, min_target);

                    write_target = Some(chosen);
                    chosen
                }
            };

            // Fold in what the drain picked up, plus anything a target raised
            // by the last conflict now admits.
            state.fold_below(target);

            // A write at `target` needs every diff below it, which is what
            // progress at or above `target` certifies. Waiting for the next
            // message is bounded by `statement_timeout`, like every wait here.
            if upper < target {
                continue;
            }

            if state.payload.is_empty() {
                // Everything below `target` cancelled out, so there is nothing to
                // write and the answer holds as of `target - 1`. Diffs pending at
                // or above `target` are concurrent with the write this would have
                // been and do not enter it.
                break Ok(OccOutcome::NoRowsMatched {
                    response: build_no_rows_response(&kind),
                    empty_as_of: Some(empty_as_of(target)),
                });
            }

            let success_response = match build_success_response(
                &kind,
                &returning,
                &state.payload,
                max_result_size,
                max_query_result_size,
                &row_set_finishing_seconds,
            ) {
                Ok(response) => response,
                Err(e) => break Err(e),
            };

            // Submit write.
            //
            // TODO(aljoscha): Store `Arc<Row>` in the payload if this shows up
            // in profiles. Every attempt clones every row, and we retry up to
            // `max_occ_retries` times.
            attempt_state.mark_write_submitted();
            let result = match self
                .call_coordinator(|tx| Command::AttemptWrite {
                    attempt: match write_conn_id.clone() {
                        Some(conn_id) => WriteAttemptKind::Session {
                            conn_id,
                            write_ts: Some(target),
                        },
                        None => WriteAttemptKind::Background { write_ts: target },
                    },
                    target_id,
                    target_global_id,
                    diffs: state.payload.clone(),
                    tx,
                })
                .await
            {
                Ok(result) => result,
                Err(error) => break Err(error),
            };

            match classify_write_result(result, target_id, attempt_state) {
                WriteOutcome::Committed(timestamp) => {
                    if let Some(id) = statement_logging_id {
                        self.log_set_timestamp(id, timestamp);
                    }
                    // N.B. subscribe_handle is dropped here, which fires off
                    // the cleanup message.
                    break Ok(OccOutcome::Committed {
                        response: success_response,
                        write_ts: timestamp,
                    });
                }
                WriteOutcome::Failed(err) => break Err(err),
                WriteOutcome::Conflict {
                    next_eligible_timestamp,
                } => {
                    // The write definitively did not land, so the attempt is
                    // resolved. Clearing `write_submitted` lets a cancel or
                    // statement timeout that fires during the upcoming
                    // subscribe wait resolve promptly instead of awaiting a
                    // write result.
                    attempt_state.mark_write_resolved();
                    // Adopt the timestamp the committer reported as next
                    // eligible. The accumulated diffs say nothing about it
                    // yet, and they do not have to: the readiness check above
                    // holds the next attempt until the subscribe has certified
                    // everything below the new target, and the fold then moves
                    // the diffs in between into the payload.
                    write_target = Some(next_eligible_timestamp);
                    state.retry_count += 1;
                    // Cancellation wins over the retry budget: if both apply,
                    // the user asked us to stop and that is the more truthful
                    // answer.
                    if let Some(error) = attempt_state.requested_error() {
                        break Err(error);
                    }
                    if state.retry_count > max_occ_retries {
                        // Contention is a user-visible condition, not an
                        // internal invariant violation, and every attempt was
                        // refused before anything was appended, so the
                        // statement is retryable.
                        break Err(AdapterError::ReadThenWriteContention);
                    }
                    tracing::debug!(
                        retry_count = state.retry_count,
                        write_ts = %target,
                        next_eligible_timestamp = %next_eligible_timestamp,
                        "OCC write conflict, retrying"
                    );
                    continue;
                }
            }
        };

        (state.retry_count, result)
    }
}

/// Result of validating a read-then-write operation.
struct ValidationResult {
    cluster_id: ComputeInstanceId,
    replica_id: Option<ReplicaId>,
    timeline: TimelineContext,
    depends_on: BTreeSet<GlobalId>,
    /// The table descriptor, used for constraint validation.
    table_desc: RelationDesc,
}

/// Accumulated state for the OCC loop in `run_occ_loop`.
///
/// Every diff the subscribe ever sent is kept, split at [`Self::split`]. The
/// split only rises, so each diff crosses it once.
struct OccState {
    /// Consolidated net diffs from strictly below [`Self::split`].
    payload: Vec<(Row, Diff)>,
    /// Diffs at or above [`Self::split`], consolidated by `(row, timestamp)`.
    pending: Vec<(Row, Timestamp, Diff)>,
    /// Where the last fold split the diffs, `None` before the first one.
    split: Option<Timestamp>,
    /// Timestamp of the last progress message, which certifies that no diff
    /// will arrive below it.
    current_upper: Option<Timestamp>,
    retry_count: usize,
    /// Row bytes held in `payload` and `pending` together, which is what the
    /// `max_result_size` check measures. `pending` is consolidated by
    /// `(row, timestamp)`, so a row touched at several timestamps occupies
    /// several entries until a rising split folds them together, and the count
    /// can exceed the size of the payload that eventually goes out.
    byte_size: u64,
}

impl OccState {
    fn new() -> Self {
        Self {
            payload: Vec::new(),
            pending: Vec::new(),
            split: None,
            current_upper: None,
            retry_count: 0,
            byte_size: 0,
        }
    }

    /// Raises the split to `split`, moving the diffs below it into the payload.
    ///
    /// Lowering it is a bug: the payload is consolidated and never re-split, so
    /// the diffs above a lowered split would stay in it. We clamp to the old
    /// split, which keeps the payload's contract intact.
    fn fold_below(&mut self, split: Timestamp) {
        let split = match self.split {
            Some(previous) if split < previous => {
                soft_panic_or_log!(
                    "read-then-write folded at {split}, below its previous split {previous}"
                );
                previous
            }
            _ => split,
        };
        self.split = Some(split);
        self.fold(Some(split));
    }

    /// Moves every accumulated diff into the payload, whatever its timestamp.
    ///
    /// Only valid once the subscribe has run to completion, where the diffs are
    /// frontier-independent and no split separates them.
    fn fold_all(&mut self) {
        self.fold(None);
    }

    /// Moves the diffs below `split`, or all of them when it is `None`, into the
    /// payload, consolidates both halves, and recomputes `byte_size`.
    fn fold(&mut self, split: Option<Timestamp>) {
        for (row, ts, diff) in std::mem::take(&mut self.pending) {
            match split {
                Some(split) if ts >= split => self.pending.push((row, ts, diff)),
                _ => self.payload.push((row, diff)),
            }
        }
        consolidation::consolidate(&mut self.payload);
        consolidation::consolidate_updates(&mut self.pending);
        self.byte_size = self
            .payload
            .iter()
            .map(|(row, _)| u64::cast_from(row.byte_len()))
            .chain(
                self.pending
                    .iter()
                    .map(|(row, _, _)| u64::cast_from(row.byte_len())),
            )
            .sum();
    }

    /// Whether nothing has been accumulated on either side of the split.
    fn is_empty(&self) -> bool {
        self.payload.is_empty() && self.pending.is_empty()
    }
}

/// Result of processing a single subscribe message in the OCC loop.
enum ProcessResult {
    Continue {
        ready_to_write: bool,
    },
    /// The consolidated selection is empty, as of the timestamp reported. See
    /// [`OccOutcome::NoRowsMatched`].
    NoRowsMatched {
        empty_as_of: Timestamp,
    },
    Error(AdapterError),
}

/// Process one subscribe message, updating `state` in place.
///
/// Data rows are accumulated into `state` (with per-row constraint and
/// max-result-size checks). Progress messages fold everything below
/// `fold_target` into the payload and can promote the accumulated diffs to
/// "ready to write".
///
/// `fold_target` must not exceed the timestamp the next write attempt uses, or
/// the payload takes in a diff that is concurrent with that write.
fn process_message(
    response: PeekResponseUnary,
    state: &mut OccState,
    as_of: Timestamp,
    fold_target: Timestamp,
    max_result_size: u64,
    table_desc: &RelationDesc,
) -> ProcessResult {
    match response {
        PeekResponseUnary::Rows(mut rows) => {
            let mut saw_progress = false;

            while let Some(row) = rows.next() {
                let mut datums = row.iter();

                // Extract mz_timestamp (SubscribeOutput::Diffs format:
                // mz_timestamp, mz_progressed, mz_diff, ...data columns...).
                //
                // Format drift would mean we'd silently commit an incorrect
                // write, so surface every shape mismatch as an internal
                // error rather than panicking the process.
                let Some(ts_datum) = datums.next() else {
                    return ProcessResult::Error(AdapterError::Internal(
                        "missing mz_timestamp in subscribe output".into(),
                    ));
                };
                let ts = match ts_datum {
                    mz_repr::Datum::Numeric(n) => match n.0.try_into() {
                        Ok(ts_u64) => Timestamp::new(ts_u64),
                        Err(_) => {
                            return ProcessResult::Error(AdapterError::Internal(format!(
                                "mz_timestamp in subscribe output is not a valid u64: {n}"
                            )));
                        }
                    },
                    other => {
                        return ProcessResult::Error(AdapterError::Internal(format!(
                            "unexpected mz_timestamp datum: {other:?}"
                        )));
                    }
                };

                let Some(progressed_datum) = datums.next() else {
                    return ProcessResult::Error(AdapterError::Internal(
                        "missing mz_progressed in subscribe output".into(),
                    ));
                };
                let is_progress = matches!(progressed_datum, mz_repr::Datum::True);

                if is_progress {
                    state.current_upper = Some(ts);
                    saw_progress = true;

                    // Fold and consolidate incrementally on each progress
                    // message. This keeps memory bounded by the consolidated
                    // size and makes the byte_size check below accurate (except
                    // for rows received between two progress messages, which is
                    // a small window).
                    state.fold_below(fold_target);

                    // NOTE: The first progress message is always at `as_of`,
                    // emitted by `ActiveSubscribe::initialize` before any data
                    // batch, so the accumulation is empty there whatever the
                    // snapshot holds. Later progress is gated on `batch.upper >
                    // as_of` (see `crate::active_compute_sink`), so `ts > as_of`
                    // is what distinguishes a real answer from that first one.
                    //
                    // Nothing accumulated at all, so no write is coming and the
                    // loop would otherwise wait for diffs that will not arrive.
                    // Our view is complete below `ts` and the payload covers
                    // below `fold_target`, so the emptiness holds as of one below
                    // the earlier of the two.
                    if ts > as_of && state.is_empty() {
                        return ProcessResult::NoRowsMatched {
                            empty_as_of: empty_as_of(std::cmp::min(ts, fold_target)),
                        };
                    }
                } else {
                    let Some(diff_datum) = datums.next() else {
                        return ProcessResult::Error(AdapterError::Internal(
                            "missing mz_diff in subscribe output".into(),
                        ));
                    };
                    let diff = match diff_datum {
                        mz_repr::Datum::Int64(d) => Diff::from(d),
                        other => {
                            return ProcessResult::Error(AdapterError::Internal(format!(
                                "unexpected mz_diff datum while processing read-then-write: {other:?}"
                            )));
                        }
                    };

                    let data_row = Row::pack(datums);

                    // Validate constraints for rows being added (positive diff)
                    if diff.is_positive() {
                        for (idx, datum) in data_row.iter().enumerate() {
                            if let Err(e) = table_desc.constraints_met(idx, &datum) {
                                return ProcessResult::Error(e.into());
                            }
                        }
                    }

                    state.byte_size = state
                        .byte_size
                        .saturating_add(u64::cast_from(data_row.byte_len()));
                    if state.byte_size > max_result_size {
                        return ProcessResult::Error(AdapterError::ResultSize(format!(
                            "result exceeds max size of {}",
                            ByteSize::b(max_result_size)
                        )));
                    }
                    state.pending.push((data_row, ts, diff));
                }
            }

            // The complement of the zero-row exit above: something accumulated
            // means a write is coming, nothing at all means there is none.
            let ready_to_write = saw_progress && !state.is_empty();
            ProcessResult::Continue { ready_to_write }
        }
        PeekResponseUnary::Error(e) => {
            ProcessResult::Error(AdapterError::Unstructured(anyhow::anyhow!(e)))
        }
        // Match the lock path's classification. `Unstructured` would render
        // this as an internal error (XX000) for what is an ordinary concurrent
        // DDL race.
        PeekResponseUnary::DependencyDropped(dep) => {
            ProcessResult::Error(dep.to_concurrent_dependency_drop())
        }
        PeekResponseUnary::Canceled => ProcessResult::Error(AdapterError::Canceled),
    }
}

/// The timestamp an answer holds as of, given a view complete strictly below
/// `complete_below`.
///
/// Both callers derive `complete_below` from a timestamp strictly above the
/// subscribe's `as_of`, so it is never `Timestamp::MIN` and the saturating
/// fallback is unreachable.
fn empty_as_of(complete_below: Timestamp) -> Timestamp {
    complete_below.step_back().unwrap_or(complete_below)
}

/// Build the response returned when no rows matched the selection.
///
/// Bug-compatible with the coordinator path, which evaluates RETURNING over the
/// diffs and so reports a plain row count when there are none. Postgres returns
/// an empty result set for a zero-row `INSERT ... RETURNING` instead, but
/// changing that is a change to the path that ships today, not to this one.
fn build_no_rows_response(kind: &MutationKind) -> ExecuteResponse {
    match kind {
        MutationKind::Delete => ExecuteResponse::Deleted(0),
        MutationKind::Update => ExecuteResponse::Updated(0),
        MutationKind::Insert => ExecuteResponse::Inserted(0),
    }
}

/// Transform a MIR expression to produce the appropriate diffs for a mutation.
///
/// - DELETE: Negates the expression to produce `(row, -1)` diffs
/// - UPDATE: Unions negated old rows with mapped new rows to produce both
///   `(old_row, -1)` and `(new_row, +1)` diffs
fn apply_mutation_to_mir(
    expr: MirRelationExpr,
    kind: &MutationKind,
    assignments: &BTreeMap<usize, MirScalarExpr>,
) -> MirRelationExpr {
    match kind {
        MutationKind::Delete => MirRelationExpr::Negate {
            input: Box::new(expr),
        },
        MutationKind::Update => {
            let arity = expr.arity();

            // Find a fresh LocalId that won't conflict with any in the expression.
            //
            // Invariant: `Let` and `LetRec` are the only MIR nodes that *bind*
            // LocalIds. `Get` references them but does not introduce new ones.
            // So scanning just those two node kinds and picking `max + 1` is
            // guaranteed to produce an id unused by the subtree.
            let mut max_id = 0_u64;
            expr.visit_pre(|e| match e {
                MirRelationExpr::Let { id, .. } => {
                    max_id = std::cmp::max(max_id, id.into());
                }
                MirRelationExpr::LetRec { ids, .. } => {
                    for id in ids {
                        max_id = std::cmp::max(max_id, id.into());
                    }
                }
                _ => {}
            });
            let binding_id = LocalId::new(max_id + 1);

            let get_binding = MirRelationExpr::Get {
                id: Id::Local(binding_id),
                typ: expr.typ(),
                access_strategy: mz_expr::AccessStrategy::UnknownOrLocal,
            };

            let map_scalars: Vec<MirScalarExpr> = (0..arity)
                .map(|i| {
                    assignments
                        .get(&i)
                        .cloned()
                        .unwrap_or_else(|| MirScalarExpr::column(i))
                })
                .collect();

            let new_rows = get_binding
                .clone()
                .map(map_scalars)
                .project((arity..2 * arity).collect());

            let old_rows = MirRelationExpr::Negate {
                input: Box::new(get_binding),
            };

            let body = new_rows.union(old_rows);

            MirRelationExpr::Let {
                id: binding_id,
                value: Box::new(expr),
                body: Box::new(body),
            }
        }
        // INSERT: rows pass through unchanged, the subscribe emits them with
        // diff +1.
        MutationKind::Insert => expr,
    }
}

#[cfg(test)]
mod tests {
    use mz_repr::adt::numeric;
    use mz_repr::{Datum, IntoRowIterator};

    use super::*;

    fn row(value: i64) -> Row {
        Row::pack_slice(&[Datum::Int64(value)])
    }

    /// A progress message in the subscribe's `SubscribeOutput::Diffs` shape:
    /// `mz_timestamp, mz_progressed, mz_diff, data...`.
    fn progress(ts: u64) -> PeekResponseUnary {
        let mut row = Row::default();
        let mut packer = row.packer();
        packer.push(Datum::from(numeric::Numeric::from(ts)));
        packer.push(Datum::True);
        packer.push(Datum::Null);
        PeekResponseUnary::Rows(Box::new(row.into_row_iter()))
    }

    /// Accumulates `(row value, timestamp, diff)` triples the way
    /// `process_message` does, without going through a subscribe.
    fn accumulate(diffs: impl IntoIterator<Item = (i64, u64, i64)>) -> OccState {
        let mut state = OccState::new();
        for (value, ts, diff) in diffs {
            state
                .pending
                .push((row(value), Timestamp::new(ts), Diff::from(diff)));
        }
        state
    }

    /// The target is the boundary the write turns on, so the off-by-one is the
    /// whole point: a diff at exactly the target is concurrent with the write
    /// and must not be in its payload.
    #[mz_ore::test]
    fn test_fold_below_splits_at_the_target() {
        let mut state = accumulate([(1, 9, 1), (2, 10, 1), (3, 11, 1)]);
        state.fold_below(Timestamp::new(10));

        assert_eq!(state.payload, vec![(row(1), Diff::ONE)]);
        assert_eq!(
            state.pending,
            vec![
                (row(2), Timestamp::new(10), Diff::ONE),
                (row(3), Timestamp::new(11), Diff::ONE),
            ]
        );
    }

    /// A retry raises the target, which is what admits the diffs that were
    /// concurrent with the attempt that lost.
    #[mz_ore::test]
    fn test_fold_below_moves_each_diff_once() {
        let mut state = accumulate([(1, 9, 1), (2, 10, 1), (3, 11, 1)]);

        state.fold_below(Timestamp::new(10));
        assert_eq!(state.payload, vec![(row(1), Diff::ONE)]);

        state.fold_below(Timestamp::new(11));
        assert_eq!(
            state.payload,
            vec![(row(1), Diff::ONE), (row(2), Diff::ONE)]
        );
        assert_eq!(state.pending, vec![(row(3), Timestamp::new(11), Diff::ONE)]);

        state.fold_below(Timestamp::new(12));
        assert_eq!(
            state.payload,
            vec![
                (row(1), Diff::ONE),
                (row(2), Diff::ONE),
                (row(3), Diff::ONE),
            ]
        );
        assert!(state.pending.is_empty());
    }

    /// A row inserted and retracted below the target leaves nothing behind,
    /// which is what makes the payload the net change rather than a log.
    #[mz_ore::test]
    fn test_fold_below_cancels_opposite_diffs() {
        let mut state = accumulate([(1, 9, 1), (1, 10, -1), (2, 9, 1)]);
        state.fold_below(Timestamp::new(11));

        assert_eq!(state.payload, vec![(row(2), Diff::ONE)]);
        assert!(state.pending.is_empty());
        assert!(!state.is_empty());
    }

    /// The same rows stay pending or move to the payload depending on the
    /// target, so a size check that saw only one half would let a statement
    /// past `max_result_size` by picking the other one.
    #[mz_ore::test]
    fn test_byte_size_counts_payload_and_pending() {
        let row_bytes = u64::cast_from(row(1).byte_len());

        let mut state = accumulate([(1, 9, 1), (2, 10, 1), (3, 11, 1)]);
        state.fold_below(Timestamp::new(10));
        assert_eq!(state.payload.len(), 1);
        assert_eq!(state.pending.len(), 2);
        assert_eq!(state.byte_size, 3 * row_bytes);

        state.fold_below(Timestamp::new(12));
        assert!(state.pending.is_empty());
        assert_eq!(state.byte_size, 3 * row_bytes);
    }

    /// A subscribe that ran to completion has no target to split on, and
    /// cancellation still applies.
    #[mz_ore::test]
    fn test_fold_all_takes_every_timestamp() {
        let mut state = accumulate([(1, 9, 1), (1, 10, -1), (2, u64::MAX, 1)]);
        state.fold_all();

        assert_eq!(state.payload, vec![(row(2), Diff::ONE)]);
        assert!(state.pending.is_empty());
    }

    /// A zero-row answer holds as of the timestamp the answer was reached at,
    /// never an input's frontier. The caller waits for the oracle to reach
    /// whatever it gets, and an input settled until its next refresh reports a
    /// frontier days out, so reporting that would spend the statement's timeout
    /// on an answer of "0 rows".
    #[mz_ore::test]
    fn test_zero_rows_report_the_answer_not_the_frontier() {
        let as_of = Timestamp::new(10);
        let desc = RelationDesc::empty();

        // First pass, where the fold target is `as_of + 1`. The answer holds at
        // `as_of`, which the caller linearized before the subscribe started, so
        // it costs no wait however far out the frontier is.
        let mut state = OccState::new();
        match process_message(
            progress(u64::MAX / 2),
            &mut state,
            as_of,
            as_of.step_forward(),
            u64::MAX,
            &desc,
        ) {
            ProcessResult::NoRowsMatched { empty_as_of } => assert_eq!(empty_as_of, as_of),
            _ => panic!("an empty selection past `as_of` must report no rows matched"),
        }

        // A target raised by a conflict, with the frontier past it. The answer
        // holds at one below the target, the same timestamp a write there would
        // have been read at.
        let fold_target = Timestamp::new(20);
        let mut state = OccState::new();
        match process_message(
            progress(u64::MAX / 2),
            &mut state,
            as_of,
            fold_target,
            u64::MAX,
            &desc,
        ) {
            ProcessResult::NoRowsMatched { empty_as_of } => {
                assert_eq!(empty_as_of, Timestamp::new(19))
            }
            _ => panic!("an empty selection past `as_of` must report no rows matched"),
        }

        // A frontier below the target certifies less, so the answer holds one
        // below the frontier instead.
        let mut state = OccState::new();
        match process_message(
            progress(15),
            &mut state,
            as_of,
            fold_target,
            u64::MAX,
            &desc,
        ) {
            ProcessResult::NoRowsMatched { empty_as_of } => {
                assert_eq!(empty_as_of, Timestamp::new(14))
            }
            _ => panic!("an empty selection past `as_of` must report no rows matched"),
        }
    }

    /// Diffs waiting above the target mean a write is still coming, so the
    /// answer is not "no rows" yet even with an empty payload.
    #[mz_ore::test]
    fn test_pending_diffs_are_not_a_zero_row_answer() {
        let as_of = Timestamp::new(10);
        let desc = RelationDesc::empty();

        let mut state = accumulate([(1, 30, 1)]);
        match process_message(
            progress(20),
            &mut state,
            as_of,
            as_of.step_forward(),
            u64::MAX,
            &desc,
        ) {
            ProcessResult::Continue { ready_to_write } => assert!(ready_to_write),
            _ => panic!("a selection with diffs above the target must not report no rows"),
        }
    }
}
