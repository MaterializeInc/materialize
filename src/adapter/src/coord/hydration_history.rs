// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Durable history collection for completed compute-object hydration episodes.
//!
//! One sweep visits a single user replica, installs a replica-targeted
//! subscribe that diffs that replica's live hydration timestamps against the
//! durable history table, and appends what is missing through the timestamped
//! OCC write path. Including the history table in the read expression is what
//! makes the write idempotent across concurrent `environmentd` processes: two
//! collectors that compute the same row race for one write timestamp, and the
//! loser observes the winner's append through its own subscribe and finds
//! nothing left to write.
//!
//! One replica is sampled per interval, so an environment with `N` eligible
//! replicas revisits each one approximately every `N * interval`. Lowering the
//! interval improves freshness at the cost of more replica dataflow installs.
//!
//! Collection is sampling, not an event log. An episode whose live row is
//! retracted before its replica's turn in the sweep (a dropped object, or a
//! replica that restarts first) is not recorded, and cannot be, because
//! the only evidence is gone. See the design doc for why that is accepted here.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use itertools::Itertools;
use mz_adapter_types::dyncfgs::{
    FRONTEND_READ_THEN_WRITE, HYDRATION_HISTORY_COLLECTION_INTERVAL,
    HYDRATION_HISTORY_RETENTION_PERIOD,
};
use mz_catalog::builtin::{MZ_CATALOG_SERVER_CLUSTER, MZ_OBJECT_HYDRATION_HISTORY};
use mz_cluster_client::ReplicaId;
use mz_controller::clusters::ClusterStatus;
use mz_controller_types::ClusterId;
use mz_ore::cast::CastFrom;
use mz_ore::collections::CollectionExt;
use mz_ore::now::EpochMillis;
use mz_ore::task;
use mz_repr::CatalogItemId;
use mz_sql::plan::{MutationKind, Params, Plan, ReadThenWritePlan};
use mz_storage_client::controller::IntrospectionType;
use sha2::{Digest, Sha256};
use tracing::warn;

use crate::catalog::Catalog;
use crate::command::ExecuteResponse;
use crate::coord::{Coordinator, Message};
use crate::metrics::Metrics;
use crate::peek_client::CoordinatorClient;
use crate::session::Session;
use crate::{AdapterError, PeekClient};

/// Longest a scheduler sleep may run before it rechecks the configuration.
///
/// Sleeping the whole interval would leave a dyncfg change ineffective until the
/// old interval elapsed, so lowering the interval at runtime (which tests do)
/// would not take effect for up to the previous interval.
const SCHEDULE_RECHECK_CAP: Duration = Duration::from_secs(5);

/// How often a disabled collector rechecks whether it was enabled.
///
/// This is the cadence of every environment in the default configuration, so it
/// is much coarser than the enabled one: nothing is waiting on it.
const DISABLED_RECHECK_INTERVAL: Duration = Duration::from_secs(60);

/// Bound on one replica-targeted mutation.
///
/// Subscribe installation, replica-side progress, OCC conflict retries, and the
/// external commit can all wait indefinitely. Exceeding the bound skips the
/// step. The next sweep recomputes from current state.
const MUTATION_TIMEOUT: Duration = Duration::from_secs(300);

/// Rows retracted per retention step.
///
/// Retention has to be bounded: the OCC path refuses a selection larger than
/// `max_result_size` before submitting any write, so an unbounded delete over a
/// large backlog would fail identically on every sweep and never shrink the
/// table. Retention repeats bounded batches across sweeps until it drains the
/// fixed-cutoff backlog.
const RETENTION_BATCH_SIZE: usize = 1000;

/// Milliseconds until the next fire on this environment's own grid.
///
/// The grid has period `interval_ms` and is shifted by `offset`. When this
/// environment's point in the current period has already passed, the next one is a
/// full period later.
fn next_fire_delay(now: EpochMillis, interval_ms: EpochMillis, offset: EpochMillis) -> Duration {
    debug_assert!(interval_ms > 0);
    let this_period = (now - (now % interval_ms)).saturating_add(offset);
    let next = if this_period > now {
        this_period
    } else {
        this_period.saturating_add(interval_ms)
    };
    Duration::from_millis(next.saturating_sub(now))
}

/// Stable offset within `interval_ms` for one environment id.
fn environment_schedule_offset(environment_id: &str, interval_ms: EpochMillis) -> EpochMillis {
    debug_assert!(interval_ms > 0);
    let digest = Sha256::digest(environment_id);
    let hash = u64::from_le_bytes(digest[..8].try_into().expect("SHA-256 digest has 32 bytes"));
    hash % interval_ms
}

/// Returns whether a replica is ready for hydration-history collection.
///
/// Subscribe data is sufficient for unmanaged replicas whose status may remain
/// offline. An online status covers a delayed restart event invalidating the
/// probe after the replacement subscribe delivered its initial snapshot. A
/// missing probe preserves operation when introspection subscribes are disabled.
fn hydration_replica_ready(
    introspection_subscribe_ready: Option<bool>,
    replica_status: ClusterStatus,
) -> bool {
    introspection_subscribe_ready.unwrap_or(true) || replica_status == ClusterStatus::Online
}

impl Coordinator {
    /// Schedules the next hydration history sweep.
    ///
    /// Fires are aligned to interval boundaries so that they stay evenly spaced
    /// across restarts, offset per environment so that a fleet-wide interval does
    /// not make every environment sweep at the same instant, and each sleep is
    /// capped so a configuration change is picked up promptly. Sweeps never
    /// overlap: the next one is only scheduled once the previous one has finished
    /// or failed.
    ///
    /// NOTE: Alignment reads the wall clock, so a test that freezes `NowFn` and
    /// configures an interval longer than the recheck cap never reaches a
    /// boundary and never fires.
    pub(super) fn schedule_hydration_history_collection(&self) {
        let interval =
            HYDRATION_HISTORY_COLLECTION_INTERVAL.get(self.catalog().system_config().dyncfgs());

        // A zero interval disables collection. Keep polling so that enabling it
        // takes effect without an `environmentd` restart.
        let (delay, fire) = if interval.is_zero() {
            (DISABLED_RECHECK_INTERVAL, false)
        } else {
            // An absurd interval saturates rather than panicking. The setting is
            // durable, so a panic here would recur on every restart.
            let interval_ms = EpochMillis::try_from(interval.as_millis())
                .unwrap_or(EpochMillis::MAX)
                .max(1);
            let remaining = next_fire_delay(
                self.now(),
                interval_ms,
                self.hydration_history_schedule_offset(interval_ms),
            );
            if remaining <= SCHEDULE_RECHECK_CAP {
                (remaining, true)
            } else {
                (SCHEDULE_RECHECK_CAP, false)
            }
        };

        let internal_cmd_tx = self.internal_cmd_tx.clone();
        task::spawn(|| "hydration_history_schedule", async move {
            tokio::time::sleep(delay).await;
            let message = if fire {
                Message::HydrationHistoryRun
            } else {
                Message::HydrationHistorySchedule
            };
            // Best effort: the coordinator may be shutting down.
            let _ = internal_cmd_tx.send(message);
        });
    }

    /// A stable offset into the collection interval for this environment.
    ///
    /// Seeded from the full environment id, so it survives restarts but differs
    /// between environments in the same organization. Without it every
    /// environment would sweep on the same absolute grid, turning each boundary
    /// into a fleet-wide burst of dataflow installs, oracle round trips and persist
    /// writes.
    fn hydration_history_schedule_offset(&self, interval_ms: EpochMillis) -> EpochMillis {
        let environment_id = self.catalog().state().config().environment_id.to_string();
        environment_schedule_offset(&environment_id, interval_ms)
    }

    /// Runs one sweep: collect from the next replica, then apply retention.
    pub(super) fn run_hydration_history_collection(&mut self) {
        let (collection_interval, retention) = {
            let dyncfgs = self.catalog().system_config().dyncfgs();
            (
                HYDRATION_HISTORY_COLLECTION_INTERVAL.get(dyncfgs),
                HYDRATION_HISTORY_RETENTION_PERIOD.get(dyncfgs),
            )
        };
        // Builtin tables are not writable in read-only mode, and a disabled
        // collector must do no background work at all. Retention is part of the
        // sweep, so disabling collection also suspends it. That is deliberate:
        // the table can only be non-empty if collection ran at some point, and
        // the alternative is an always-on subscribe in the default (disabled)
        // production configuration.
        if collection_interval.is_zero() || self.controller.read_only() {
            self.schedule_hydration_history_collection();
            return;
        }

        // A replica with introspection disabled is skipped: its log arrangements
        // are installed but never populated, so a subscribe would read a sealed,
        // empty collection and find nothing, every sweep. When the existing
        // hydration-times subscribe is installed, its first data is sufficient
        // for unmanaged replicas. A rolled-up Online status also suffices because
        // a delayed restart event can invalidate the probe after the replacement
        // subscribe's initial snapshot. A never-connected managed replica remains
        // Offline and is skipped rather than parking this single-flight sweep
        // until MUTATION_TIMEOUT. With no probe, preserve this collector's
        // behavior independently of enable_introspection_subscribes.
        let replicas = self
            .catalog()
            .user_cluster_replicas()
            .filter(|replica| replica.config.compute.logging.enabled())
            .filter(|replica| {
                let subscribe_ready = self.introspection_subscribe_ready(
                    IntrospectionType::ComputeHydrationTimes,
                    replica.replica_id,
                );
                let replica_status = self
                    .cluster_replica_statuses
                    .get_cluster_replica_status(replica.cluster_id, replica.replica_id);
                hydration_replica_ready(subscribe_ready, replica_status)
            })
            .map(|replica| (replica.cluster_id, replica.replica_id))
            .sorted_by_key(|(_, replica_id)| *replica_id)
            .collect_vec();

        let catalog = self.owned_catalog();
        // Retention runs on the catalog server, so that it keeps working when
        // there are no user replicas to collect from at all. Without a replica
        // there it is skipped, while collection still runs.
        let catalog_server = catalog.resolve_builtin_cluster(&MZ_CATALOG_SERVER_CLUSTER);
        let catalog_server_target = catalog_server
            .replicas()
            .next()
            .map(|replica| (catalog_server.id, replica.replica_id));

        let replica = next_replica(&replicas, self.hydration_history_replica_cursor);
        if let Some((_, replica_id)) = replica {
            self.hydration_history_replica_cursor = Some(replica_id);
        }
        let mut sweep = self.new_sweep(catalog, retention);
        let internal_cmd_tx = self.internal_cmd_tx.clone();

        let handle = task::spawn(|| "hydration_history_sweep", async move {
            let started = Instant::now();
            if let Some((cluster_id, replica_id)) = replica {
                sweep.collect(cluster_id, replica_id).await;
            }

            // Retention runs even when collection failed above. A replica that
            // is crash-looping or slow must not be able to stop the table from
            // shrinking back to its retention bound.
            if let Some((cluster_id, replica_id)) = catalog_server_target {
                sweep.retain(cluster_id, replica_id).await;
            }

            sweep
                .metrics
                .hydration_history_sweep_duration_seconds
                .observe(started.elapsed().as_secs_f64());
            let _ = internal_cmd_tx.send(Message::HydrationHistorySchedule);
        });

        // NOTE: The sweep must not outlive the coordinator. Unlike a session it
        // holds no `Client`, so nothing stops the coordinator from exiting while
        // a mutation is in flight, and the runtime teardown that follows drops
        // the timestamp oracle's worker task. A sweep still running at that
        // point reads a timestamp from a dead oracle and panics. Parking the
        // handle here means dropping the coordinator cancels the sweep first.
        self.hydration_history_sweep = Some(handle.abort_on_drop());
    }

    /// Assembles the sweep context, including the client it writes through.
    fn new_sweep(&self, catalog: Arc<Catalog>, retention: Duration) -> Sweep {
        let retention_ms = u64::try_from(retention.as_millis()).unwrap_or(u64::MAX);
        let build_version = catalog.state().config().build_info.human_version(None);
        // Background read-then-write always uses the frontend OCC path. This
        // shared constructor field only controls session fallback, so the flag
        // does not gate history collection.
        let client = PeekClient::new(
            CoordinatorClient::Background {
                tx: self.internal_cmd_tx.clone(),
                metrics: self.metrics.clone(),
            },
            &catalog,
            Arc::clone(&self.controller.storage_collections),
            Arc::clone(&self.transient_id_gen),
            self.optimizer_metrics.clone(),
            self.persist_client.clone(),
            self.statement_logging.create_frontend(build_version),
            Arc::clone(&self.occ_write_semaphore),
            FRONTEND_READ_THEN_WRITE.get(self.catalog().system_config().dyncfgs()),
            self.group_commit_tx.clone(),
            self.controller.read_only(),
        );
        Sweep {
            client,
            history_id: catalog.resolve_builtin_table(&MZ_OBJECT_HYDRATION_HISTORY),
            catalog,
            metrics: self.metrics.clone(),
            wall_time: self.now_datetime(),
            cutoff: mz_ore::now::to_datetime(self.now().saturating_sub(retention_ms)).to_rfc3339(),
        }
    }
}

/// Picks the replica after `cursor`, wrapping around at the end.
///
/// `replicas` must be sorted ascending by replica id. Unsorted input still
/// returns a replica but degenerates the rotation, revisiting some replicas and
/// starving others.
fn next_replica(
    replicas: &[(ClusterId, ReplicaId)],
    cursor: Option<ReplicaId>,
) -> Option<(ClusterId, ReplicaId)> {
    replicas
        .iter()
        .find(|(_, replica_id)| cursor.is_none_or(|cursor| *replica_id > cursor))
        .or_else(|| replicas.first())
        .copied()
}

/// The rows this replica has completed that the history table is missing.
///
/// Aggregates every worker's row for an export, and records nothing until all of
/// them have hydrated. One worker is not enough, because a materialized view's
/// persist sink has a single active worker, `hash(sink_id) % workers`. Only that
/// worker's reported output frontier is gated on the shard upper, so only its
/// `hydrated_at` covers the initial snapshot write. Every other worker clears its
/// sink write frontier and stamps at compute completion, which for a materialized
/// view is before the data is durable. Taking `max` over a complete set of workers
/// is therefore the only way to get a finish that means the same thing for every
/// object, and it is the rule `mz_compute_hydration_times` already applies.
///
/// Completeness needs no configured worker count. The log carries a row per
/// `(export_id, worker_id)` from installation with a null `hydrated_at`, so
/// `count(*) = count(hydrated_at)` says every row visible at the OCC read
/// timestamp has finished. Per-process logging clocks also determine Differential
/// update timestamps, so a worker whose clock is ahead can be absent at that
/// timestamp. A visible unfinished object is skipped and picked up by a later
/// sweep.
///
/// A worker missing at the read timestamp cannot later change the episode key.
/// Its logging clock stamps both the Differential update and `installed_at`, so
/// late visibility means its installation stamp is later than the visible
/// minimum. The anti-join therefore keeps matching the recorded row. A later
/// `hydrated_at` can raise the aggregate's maximum, but history is not repaired
/// after the episode key has been recorded.
///
/// The collector deliberately accepts this sampling race rather than depending on
/// `ReplicaLocation::workers()`. A durable finish can therefore precede the latest
/// worker's finish. A whole-replica restart resets the collection as a unit.
///
/// The interval spans workers, so it carries whatever skew there is between the
/// process clocks that stamped its ends. Each process anchors its logging clock at
/// its own `SystemTime`. That inflates a duration, and nothing here rejects a row
/// for being inconsistent, which is deliberate: an ordering guard on cross-worker
/// stamps rejects complete episodes permanently, since the log values never change.
///
/// Collection has no explicit batch bound. It returns at most one row per
/// not-yet-recorded dataflow, and the OCC path rejects a result that exceeds
/// `max_result_size` or `max_query_result_size`. At their 1 GiB defaults that
/// ceiling only matters at millions of dataflows per replica.
fn collect_sql(cluster_id: ClusterId, replica_id: ReplicaId, cutoff: &str) -> String {
    // Interpolating into SQL is safe here: the ids are catalog-internal and the
    // cutoff is an RFC 3339 timestamp we formatted ourselves. Nothing in this
    // query comes from a user.
    //
    // NOTE: The cutoff and the anti-join sit outside the aggregate deliberately. As
    // a `WHERE` clause either one drops not-yet-hydrated rows, which would make
    // `count(*) = count(hydrated_at)` trivially true and hand back a compute-only
    // finish for a materialized view whose active worker is still writing.
    //
    // NOTE: `hydrated_at` is the terminal stamp for a history episode. Nothing
    // waits for the history row before proceeding. If the log gains a separate
    // `written_at` stamp, only `hydrated_at` belongs in this completeness check.
    // A materialized view being replaced can hydrate while it runs read-only, and
    // may never write if the replacement is rolled back.
    format!(
        "SELECT
            e.object_id,
            '{cluster_id}'::text AS cluster_id,
            '{replica_id}'::text AS replica_id,
            e.installed_at,
            e.started_at,
            e.hydrated_at,
            'hydrated'::text AS status
        FROM (
            SELECT
                t.export_id AS object_id,
                min(t.installed_at) AS installed_at,
                min(t.started_at) AS started_at,
                max(t.hydrated_at) AS hydrated_at
            FROM mz_introspection.mz_compute_hydration_times_per_worker AS t
            JOIN mz_internal.mz_object_global_ids AS ids ON ids.global_id = t.export_id
            JOIN mz_catalog.mz_objects AS o ON o.id = ids.id
            WHERE t.export_id LIKE 'u%'
              AND o.type IN ('index', 'materialized-view')
            GROUP BY t.export_id
            HAVING count(*) = count(t.hydrated_at)
        ) AS e
        WHERE e.hydrated_at >= TIMESTAMPTZ '{cutoff}'
          AND NOT EXISTS (
              SELECT 1
              FROM mz_internal.mz_object_hydration_history AS h
              WHERE h.object_id = e.object_id
                AND h.replica_id = '{replica_id}'::text
                AND h.installed_at = e.installed_at
          )"
    )
}

/// A bounded batch of history rows that have aged out.
///
/// Only rows with a `hydrated_at` age out. Every row written today has one, and
/// a row without one would be immortal here, so an unfinished-episode
/// representation needs a second age basis before it can be recorded.
fn retention_sql(cutoff: &str) -> String {
    // The LIMIT has to sit inside a subquery. A top-level LIMIT lands in the
    // plan's `RowSetFinishing`, which the OCC path deliberately ignores, so it
    // would be silently dropped and the delete would be unbounded again. Inside
    // a derived table it lowers into the relation expression instead.
    format!(
        "SELECT * FROM (
            SELECT
                object_id, cluster_id, replica_id, installed_at, started_at,
                hydrated_at, status
            FROM mz_internal.mz_object_hydration_history
            WHERE hydrated_at < TIMESTAMPTZ '{cutoff}'
            ORDER BY hydrated_at
            LIMIT {RETENTION_BATCH_SIZE}
        )"
    )
}

/// What one sweep needs to run its mutations against the history table.
struct Sweep {
    client: PeekClient,
    catalog: Arc<Catalog>,
    history_id: CatalogItemId,
    metrics: Metrics,
    wall_time: chrono::DateTime<chrono::Utc>,
    /// Rows finishing before this have aged out. Both steps apply it, so a live
    /// log row cannot resurrect an episode retention just retracted.
    cutoff: String,
}

impl Sweep {
    /// Appends this replica's completed episodes that the table is missing.
    async fn collect(&mut self, cluster_id: ClusterId, replica_id: ReplicaId) {
        let sql = collect_sql(cluster_id, replica_id, &self.cutoff);
        let _ = self
            .run(
                "collection",
                cluster_id,
                replica_id,
                MutationKind::Insert,
                &sql,
            )
            .await;
    }

    /// Retracts one bounded batch of aged-out rows.
    async fn retain(&mut self, cluster_id: ClusterId, replica_id: ReplicaId) {
        let sql = retention_sql(&self.cutoff);
        let Some(deleted) = self
            .run(
                "retention",
                cluster_id,
                replica_id,
                MutationKind::Delete,
                &sql,
            )
            .await
        else {
            return;
        };
        if deleted == RETENTION_BATCH_SIZE {
            self.metrics.hydration_history_retention_batch_full.inc();
        }
    }

    /// Runs one mutation, logging rather than propagating failure.
    ///
    /// Every failure mode here is expected in normal operation: the targeted
    /// replica can fail or be dropped, a dependency can be replaced, and the
    /// write can lose its timestamp race. None of them are actionable, and the
    /// next sweep recomputes from current state, so they are logged and the
    /// sweep continues.
    ///
    /// NOTE: A timed-out mutation can still commit. The write is submitted
    /// before we wait for its answer, and a background write carries no
    /// connection to cancel it with, so a `timeout` outcome says we stopped
    /// waiting, not that nothing landed. Rows such a write commits afterwards
    /// are never counted, which makes `rows_affected` a lower bound.
    async fn run(
        &mut self,
        step: &'static str,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        kind: MutationKind,
        sql: &str,
    ) -> Option<usize> {
        let mutation = async {
            let plan = plan_mutation(&self.catalog, self.history_id, kind, sql)?;
            let mut session = Session::dummy();
            session.start_transaction_single_stmt(self.wall_time);
            let response = self
                .client
                .background_read_then_write(
                    &mut session,
                    plan,
                    cluster_id,
                    replica_id,
                    &self.catalog,
                )
                .await?;
            Ok::<_, AdapterError>(response)
        };
        match tokio::time::timeout(MUTATION_TIMEOUT, mutation).await {
            Ok(Ok(response)) => {
                let (rows, action) = match (kind, response) {
                    (MutationKind::Insert, ExecuteResponse::Inserted(rows)) => (rows, "appended"),
                    (MutationKind::Update, ExecuteResponse::Updated(rows)) => (rows, "updated"),
                    (MutationKind::Delete, ExecuteResponse::Deleted(rows)) => (rows, "deleted"),
                    (_, response) => {
                        self.observe_mutation(step, "error");
                        mz_ore::soft_panic_or_log!(
                            "hydration history {step} returned an unexpected response: {response:?}"
                        );
                        return None;
                    }
                };
                self.metrics
                    .hydration_history_rows_affected
                    .with_label_values(&[action])
                    .inc_by(u64::cast_from(rows));
                let outcome = if rows == 0 { "noop" } else { "success" };
                self.observe_mutation(step, outcome);
                Some(rows)
            }
            Ok(Err(error)) => {
                self.observe_mutation(step, "error");
                if step == "collection" && matches!(&error, AdapterError::ReadThenWriteContention) {
                    warn!(
                        %step, %cluster_id, %replica_id, %error,
                        "hydration history step failed, the replica's introspection frontier \
                         may be trailing the write frontier"
                    );
                } else {
                    warn!(%step, %cluster_id, %replica_id, %error, "hydration history step failed");
                }
                None
            }
            // A trailing replica can repeatedly certify a target only after the
            // oracle has advanced past it. Each refused write raises the target,
            // and the conflict loop can continue until this timeout fires.
            Err(_) if step == "collection" => {
                self.observe_mutation(step, "timeout");
                warn!(
                    %step, %cluster_id, %replica_id,
                    "hydration history step timed out, \
                     the replica's introspection frontier may be trailing the write frontier"
                );
                None
            }
            Err(_) => {
                self.observe_mutation(step, "timeout");
                warn!(%step, %cluster_id, %replica_id, "hydration history step timed out");
                None
            }
        }
    }

    fn observe_mutation(&self, operation: &str, outcome: &str) {
        self.metrics
            .hydration_history_mutations
            .with_label_values(&[operation, outcome])
            .inc();
    }
}

/// Plans `sql` as the read side of a mutation against `target_id`.
///
/// The statement is planned as a `SELECT` whose columns are already in the
/// target table's order, so the mutation needs no assignments or projection.
///
/// The selection's column types are checked against the target table here. A
/// user `INSERT ... SELECT` gets that from the planner, but a hand-built plan
/// bypasses it, and a wrong type would be written into the shard verbatim and
/// break every later read of a table that is deliberately never truncated.
fn plan_mutation(
    catalog: &Arc<Catalog>,
    target_id: CatalogItemId,
    kind: MutationKind,
    sql: &str,
) -> Result<ReadThenWritePlan, AdapterError> {
    let session_catalog = catalog.for_system_session();
    let parsed = mz_sql::parse::parse(sql)
        .map_err(AdapterError::from)?
        .into_element();
    let (stmt, resolved_ids) = mz_sql::names::resolve(&session_catalog, parsed.ast)?;
    let (plan, _) = mz_sql::plan::plan(
        None,
        &session_catalog,
        stmt,
        &Params::empty(),
        &resolved_ids,
    )?;
    let Plan::Select(select) = plan else {
        return Err(AdapterError::Internal(
            "hydration history query did not plan as SELECT".into(),
        ));
    };

    let target_desc = catalog
        .get_entry(&target_id)
        .relation_desc_latest()
        .expect("hydration history target is a table");
    let selection_types = select.source.typ(&[], &BTreeMap::new()).column_types;
    let target_types = &target_desc.typ().column_types;
    let matches = selection_types.len() == target_types.len()
        && selection_types
            .iter()
            .zip_eq(target_types)
            // Nullability may be tighter than the column allows, only the
            // scalar types have to agree.
            .all(|(selected, target)| selected.scalar_type == target.scalar_type);
    if !matches {
        return Err(AdapterError::Internal(format!(
            "hydration history query does not match the target table: \
             selection {selection_types:?}, table {target_types:?}"
        )));
    }

    Ok(ReadThenWritePlan {
        id: target_id,
        selection: select.source,
        finishing: select.finishing,
        assignments: BTreeMap::new(),
        kind,
        returning: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn replica_sweep_advances_and_wraps() {
        let cluster = ClusterId::user(1).expect("valid cluster ID");
        let replicas = [(cluster, ReplicaId::User(1)), (cluster, ReplicaId::User(3))];

        assert_eq!(next_replica(&replicas, None), Some(replicas[0]));
        assert_eq!(
            next_replica(&replicas, Some(ReplicaId::User(1))),
            Some(replicas[1])
        );
        assert_eq!(
            next_replica(&replicas, Some(ReplicaId::User(3))),
            Some(replicas[0])
        );
        assert_eq!(next_replica(&[], None), None);
    }

    #[mz_ore::test]
    fn replica_readiness_uses_subscribe_data_or_online_status() {
        let offline = ClusterStatus::Offline(None);

        assert!(hydration_replica_ready(Some(true), offline));
        assert!(hydration_replica_ready(Some(false), ClusterStatus::Online));
        assert!(!hydration_replica_ready(Some(false), offline));
        assert!(hydration_replica_ready(None, offline));
    }

    /// Every environment shares one interval, so the grid has to be shifted per
    /// environment or the whole fleet sweeps at the same instant.
    #[mz_ore::test]
    fn fire_delay_is_offset_within_the_interval() {
        let interval = 60_000;

        // Before this environment's point in the period, we wait for it.
        assert_eq!(
            next_fire_delay(1_000, interval, 5_000),
            Duration::from_millis(4_000)
        );
        // On it, we take the next period rather than firing twice.
        assert_eq!(
            next_fire_delay(5_000, interval, 5_000),
            Duration::from_millis(interval)
        );
        // After it, the next period's point.
        assert_eq!(
            next_fire_delay(6_000, interval, 5_000),
            Duration::from_millis(59_000)
        );
        // A zero offset is plain alignment, and never returns a zero delay.
        assert_eq!(
            next_fire_delay(59_999, interval, 0),
            Duration::from_millis(1)
        );
        assert_eq!(
            next_fire_delay(60_000, interval, 0),
            Duration::from_millis(interval)
        );

        // Region and ordinal are part of the seed, not just the organization.
        let one = environment_schedule_offset(
            "aws-us-east-1-00000000-0000-0000-0000-000000000000-0",
            interval,
        );
        let two = environment_schedule_offset(
            "aws-us-west-1-00000000-0000-0000-0000-000000000000-1",
            interval,
        );
        assert_eq!(one, 30_189);
        assert_eq!(two, 38_252);
        assert_ne!(one, two);
    }

    /// A materialized view's finish is only durable on the sink's active worker, so
    /// the query has to see every worker and take the latest stamp. Pinning a single
    /// worker, or letting the cutoff or the anti-join filter rows before the
    /// completeness check, silently reintroduces a finish that precedes the write.
    #[mz_ore::test]
    fn collect_requires_every_worker() {
        let cutoff = "1970-01-01T00:00:00+00:00";
        let sql = collect_sql(
            ClusterId::user(1).expect("valid cluster ID"),
            ReplicaId::User(2),
            cutoff,
        );
        assert!(
            sql.contains("HAVING count(*) = count(t.hydrated_at)"),
            "{sql}"
        );
        assert!(sql.contains("max(t.hydrated_at)"), "{sql}");
        assert!(!sql.contains("worker_id"), "{sql}");

        // Both of these have to apply to the aggregate's output, not to the rows
        // feeding it.
        let aggregate_end = sql.find(") AS e").expect("aggregate subquery");
        assert!(sql.find(cutoff).expect("cutoff") > aggregate_end, "{sql}");
        assert!(
            sql.find("NOT EXISTS").expect("anti-join") > aggregate_end,
            "{sql}"
        );
    }
}
