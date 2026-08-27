// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Durable history collection for completed object and replica hydration episodes.
//!
//! One sweep visits a single user replica, installs a replica-targeted
//! subscribe that diffs that replica's live hydration timestamps against the
//! durable history tables, and appends what is missing through the timestamped
//! OCC write path. Including each history table in its read expression is what
//! makes the write idempotent across concurrent `environmentd` processes: two
//! collectors that compute the same row race for one write timestamp, and the
//! loser observes the winner's append through its own subscribe and finds
//! nothing left to write.
//!
//! One replica is sampled per interval, so an environment with `N` eligible
//! replicas revisits each one approximately every `N * interval`. Lowering the
//! interval improves freshness at the cost of more replica dataflow installs.
//!
//! Collection is sampling, not an event log. Replica history records only the
//! latest completed component visible in a sweep. Intermediate episodes and
//! intervals retracted before collection leave no evidence and are not recorded.
//! See the design doc for the resulting semantics.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use itertools::Itertools;
use mz_adapter_types::dyncfgs::{
    FRONTEND_READ_THEN_WRITE, HYDRATION_HISTORY_COLLECTION_INTERVAL,
    HYDRATION_HISTORY_RETENTION_PERIOD,
};
use mz_catalog::builtin::{
    MZ_CATALOG_SERVER_CLUSTER, MZ_OBJECT_HYDRATION_HISTORY, MZ_REPLICA_HYDRATION_HISTORY,
};
use mz_cluster_client::ReplicaId;
use mz_controller::clusters::{ClusterStatus, ReplicaLocation};
use mz_controller_types::ClusterId;
use mz_ore::cast::CastFrom;
use mz_ore::collections::CollectionExt;
use mz_ore::now::EpochMillis;
use mz_ore::task;
use mz_repr::CatalogItemId;
use mz_sql::plan::{MutationKind, Params, Plan, ReadThenWritePlan};
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

        let replicas = self
            .catalog()
            .user_cluster_replicas()
            .filter(|replica| replica.config.compute.logging.enabled())
            .filter(|replica| match &replica.config.location {
                ReplicaLocation::Managed(_) => {
                    self.cluster_replica_statuses
                        .get_cluster_replica_status(replica.cluster_id, replica.replica_id)
                        == ClusterStatus::Online
                }
                // Unmanaged replicas have no orchestrator status and are only
                // used by tests. Their bounded mutation determines readiness.
                ReplicaLocation::Unmanaged(_) => true,
            })
            .map(|replica| ReplicaTarget {
                cluster_id: replica.cluster_id,
                replica_id: replica.replica_id,
                process_count: replica.config.location.num_processes(),
            })
            .sorted_by_key(|replica| replica.replica_id)
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
        if let Some(replica) = replica {
            self.hydration_history_replica_cursor = Some(replica.replica_id);
        }
        let mut sweep = self.new_sweep(catalog, retention);
        let internal_cmd_tx = self.internal_cmd_tx.clone();

        let handle = task::spawn(|| "hydration_history_sweep", async move {
            let started = Instant::now();
            if let Some(replica) = replica {
                sweep.collect(replica).await;
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

        // Keep the sweep coordinator-owned so dropping the coordinator requests
        // its abort. Fallible coordinator calls make concurrent shutdown safe.
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
            object_history_id: catalog.resolve_builtin_table(&MZ_OBJECT_HYDRATION_HISTORY),
            replica_history_id: catalog.resolve_builtin_table(&MZ_REPLICA_HYDRATION_HISTORY),
            catalog,
            metrics: self.metrics.clone(),
            wall_time: self.now_datetime(),
            cutoff: mz_ore::now::to_datetime(self.now().saturating_sub(retention_ms)).to_rfc3339(),
        }
    }
}

/// A user replica eligible for one collection step.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ReplicaTarget {
    cluster_id: ClusterId,
    replica_id: ReplicaId,
    process_count: usize,
}

/// Picks the replica after `cursor`, wrapping around at the end.
///
/// `replicas` must be sorted ascending by replica id. Unsorted input still
/// returns a replica but degenerates the rotation, revisiting some replicas and
/// starving others.
fn next_replica(replicas: &[ReplicaTarget], cursor: Option<ReplicaId>) -> Option<ReplicaTarget> {
    replicas
        .iter()
        .find(|replica| cursor.is_none_or(|cursor| replica.replica_id > cursor))
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
fn object_collection_sql(cluster_id: ClusterId, replica_id: ReplicaId, cutoff: &str) -> String {
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

/// The latest completed transition of this replica from hydrated to hydrating
/// and back, together with the process resource peaks visible when collected.
///
/// Each object's interval starts at installation and ends at hydration. The
/// union of overlapping intervals is one replica episode. The running maximum
/// of prior finishes identifies gaps between those unions, and the latest gap
/// identifies the most recent episode. Collection waits until every currently
/// visible object and every configured replica process has reported.
///
/// Process-local wall clocks stamp both ends. Clock skew can therefore merge
/// components that did not overlap in real time. Replica processes are fate
/// shared and restart together, so the query never combines process generations.
///
/// Retracted intervals leave no evidence. `hydrated` therefore describes the
/// surviving component at collection time, not objects that disappeared before
/// the sweep. Once an episode is recorded, the history guard prevents a later
/// snapshot from inserting a component that precedes or overlaps it.
///
/// Resource high-water marks reset when a process restarts, not at an episode
/// boundary. They cover the process lifetime through collection, so even the
/// initial episode can include work after hydration. For a later episode they
/// can also include an earlier peak. The filesystem peak remains a sampled lower
/// bound. The maximum across processes is the relevant figure because managed
/// replica resource limits apply to each process independently.
fn replica_collection_sql(target: ReplicaTarget, cutoff: &str) -> String {
    let ReplicaTarget {
        cluster_id,
        replica_id,
        process_count,
    } = target;
    // Interpolating into SQL is safe here: the ids and process count are
    // catalog-internal and the cutoff is an RFC 3339 timestamp we formatted.
    format!(
        "WITH
        worker_hydration AS (
            SELECT t.export_id, t.installed_at, t.hydrated_at
            FROM mz_introspection.mz_compute_hydration_times_per_worker AS t
            JOIN mz_internal.mz_object_global_ids AS ids ON ids.global_id = t.export_id
            JOIN mz_catalog.mz_objects AS o ON o.id = ids.id
            WHERE t.export_id LIKE 'u%'
              AND o.type IN ('index', 'materialized-view')
        ),
        objects AS (
            SELECT
                export_id AS object_id,
                min(installed_at) AS installed_at,
                max(hydrated_at) AS hydrated_at,
                count(*) = count(hydrated_at) AS hydrated
            FROM worker_hydration
            GROUP BY export_id
        ),
        running AS (
            SELECT
                object_id,
                installed_at,
                hydrated_at,
                max(hydrated_at) OVER (
                    ORDER BY installed_at, object_id
                    ROWS UNBOUNDED PRECEDING
                ) AS finished_through
            FROM objects
            WHERE hydrated
        ),
        ordered AS (
            SELECT
                object_id,
                installed_at,
                hydrated_at,
                lag(finished_through) OVER (
                    ORDER BY installed_at, object_id
                ) AS prior_finished_at
            FROM running
        ),
        episode_start AS (
            SELECT max(installed_at) FILTER (
                WHERE prior_finished_at IS NULL OR prior_finished_at < installed_at
            ) AS started_at
            FROM ordered
        ),
        episode AS (
            SELECT
                s.started_at,
                max(o.hydrated_at) AS finished_at,
                count(*)::uint8 AS object_count
            FROM ordered AS o
            CROSS JOIN episode_start AS s
            WHERE o.installed_at >= s.started_at
            GROUP BY s.started_at
        ),
        resources AS (
            SELECT
                count(DISTINCT process_id) AS process_count,
                max(value) FILTER (
                    WHERE source = 'cgroup' AND metric = 'memory_peak'
                ) AS peak_memory_bytes,
                coalesce(
                    max(value) FILTER (
                        WHERE source = 'statvfs' AND metric = 'fs_used_peak'
                    ),
                    max(value) FILTER (
                        WHERE source = 'cgroup' AND metric = 'swap_peak'
                    )
                ) AS peak_disk_bytes
            FROM mz_introspection.mz_cluster_replica_resource_usage
        ),
        candidate AS (
            SELECT
                '{replica_id}'::text AS replica_id,
                '{cluster_id}'::text AS cluster_id,
                e.started_at,
                e.finished_at,
                e.object_count,
                r.peak_memory_bytes,
                r.peak_disk_bytes,
                'hydrated'::text AS status
            FROM episode AS e
            CROSS JOIN resources AS r
            WHERE (SELECT bool_and(hydrated) FROM objects)
              AND r.process_count = {process_count}::uint8
              AND e.finished_at >= TIMESTAMPTZ '{cutoff}'
        )
        SELECT c.*
        FROM candidate AS c
        WHERE NOT EXISTS (
            SELECT 1
            FROM mz_internal.mz_replica_hydration_history AS h
            WHERE h.replica_id = c.replica_id
              AND h.finished_at >= c.started_at
        )"
    )
}

/// A bounded batch of history rows that have aged out.
///
/// Only rows with a `hydrated_at` age out. Every row written today has one, and
/// a row without one would be immortal here, so an unfinished-episode
/// representation needs a second age basis before it can be recorded.
fn object_retention_sql(cutoff: &str) -> String {
    // The LIMIT has to sit inside a subquery. A top-level LIMIT lands in the
    // plan's `RowSetFinishing`, which this OCC stage cannot apply. Inside a
    // derived table it lowers into the relation expression instead.
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

/// A bounded batch of replica history rows that have aged out.
fn replica_retention_sql(cutoff: &str) -> String {
    format!(
        "SELECT * FROM (
            SELECT
                replica_id, cluster_id, started_at, finished_at, object_count,
                peak_memory_bytes, peak_disk_bytes, status
            FROM mz_internal.mz_replica_hydration_history
            WHERE finished_at < TIMESTAMPTZ '{cutoff}'
            ORDER BY finished_at
            LIMIT {RETENTION_BATCH_SIZE}
        )"
    )
}

/// What one sweep needs to run its mutations against the history table.
struct Sweep {
    client: PeekClient,
    catalog: Arc<Catalog>,
    object_history_id: CatalogItemId,
    replica_history_id: CatalogItemId,
    metrics: Metrics,
    wall_time: chrono::DateTime<chrono::Utc>,
    /// Rows finishing before this have aged out. Both steps apply it, so this
    /// sweep cannot resurrect an episode its own retention step retracts.
    /// Concurrent sweeps can have different cutoffs, making retention eventual.
    cutoff: String,
}

impl Sweep {
    /// Appends completed object and replica episodes from one replica.
    async fn collect(&mut self, target: ReplicaTarget) {
        let ReplicaTarget {
            cluster_id,
            replica_id,
            ..
        } = target;
        let sql = object_collection_sql(cluster_id, replica_id, &self.cutoff);
        let _ = self
            .run(
                "collection",
                self.object_history_id,
                cluster_id,
                replica_id,
                MutationKind::Insert,
                &sql,
            )
            .await;

        let sql = replica_collection_sql(target, &self.cutoff);
        let _ = self
            .run(
                "replica_collection",
                self.replica_history_id,
                cluster_id,
                replica_id,
                MutationKind::Insert,
                &sql,
            )
            .await;
    }

    /// Retracts one bounded batch of aged-out rows.
    async fn retain(&mut self, cluster_id: ClusterId, replica_id: ReplicaId) {
        let sql = object_retention_sql(&self.cutoff);
        if let Some(deleted) = self
            .run(
                "retention",
                self.object_history_id,
                cluster_id,
                replica_id,
                MutationKind::Delete,
                &sql,
            )
            .await
            && deleted == RETENTION_BATCH_SIZE
        {
            self.metrics.hydration_history_retention_batch_full.inc();
        }

        let sql = replica_retention_sql(&self.cutoff);
        if let Some(deleted) = self
            .run(
                "replica_retention",
                self.replica_history_id,
                cluster_id,
                replica_id,
                MutationKind::Delete,
                &sql,
            )
            .await
            && deleted == RETENTION_BATCH_SIZE
        {
            self.metrics.hydration_history_retention_batch_full.inc();
        }
    }

    /// Runs one mutation, leaving transient failures for the next sweep to retry.
    ///
    /// Replica loss, dependency replacement, and write races are logged rather
    /// than propagated because the next sweep recomputes from current state.
    ///
    /// NOTE: A timed-out mutation can still commit. The write is submitted
    /// before we wait for its answer, and a background write carries no
    /// connection to cancel it with, so a `timeout` outcome says we stopped
    /// waiting, not that nothing landed. Rows such a write commits afterwards
    /// are never counted, which makes `rows_affected` a lower bound.
    async fn run(
        &mut self,
        step: &'static str,
        history_id: CatalogItemId,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        kind: MutationKind,
        sql: &str,
    ) -> Option<usize> {
        let mutation = async {
            let plan = plan_mutation(&self.catalog, history_id, kind, sql)?;
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
                if step.ends_with("collection")
                    && matches!(&error, AdapterError::ReadThenWriteContention)
                {
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
            Err(_) if step.ends_with("collection") => {
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
        let replicas = [
            ReplicaTarget {
                cluster_id: cluster,
                replica_id: ReplicaId::User(1),
                process_count: 1,
            },
            ReplicaTarget {
                cluster_id: cluster,
                replica_id: ReplicaId::User(3),
                process_count: 1,
            },
        ];

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
        let sql = object_collection_sql(
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

    /// Replica episodes are connected components of object hydration intervals.
    /// The query must also wait for every replica process before it snapshots
    /// process-local high-water marks.
    #[mz_ore::test]
    fn replica_collection_uses_latest_completed_interval_island() {
        let sql = replica_collection_sql(
            ReplicaTarget {
                cluster_id: ClusterId::user(1).expect("valid cluster ID"),
                replica_id: ReplicaId::User(2),
                process_count: 3,
            },
            "1970-01-01T00:00:00+00:00",
        );

        assert!(sql.contains("ROWS UNBOUNDED PRECEDING"), "{sql}");
        assert!(sql.contains("lag(finished_through)"), "{sql}");
        assert!(sql.contains("prior_finished_at < installed_at"), "{sql}");
        assert!(
            sql.contains("SELECT bool_and(hydrated) FROM objects"),
            "{sql}"
        );
        assert!(sql.contains("r.process_count = 3::uint8"), "{sql}");
        let normalized_sql = sql.split_whitespace().collect::<Vec<_>>().join(" ");
        assert!(
            normalized_sql
                .contains("max(value) FILTER ( WHERE source = 'cgroup' AND metric = 'memory_peak'"),
            "{sql}"
        );
        assert!(
            normalized_sql.contains(
                "max(value) FILTER ( WHERE source = 'statvfs' AND metric = 'fs_used_peak'"
            ),
            "{sql}"
        );
        assert!(
            normalized_sql
                .contains("max(value) FILTER ( WHERE source = 'cgroup' AND metric = 'swap_peak'"),
            "{sql}"
        );
        assert!(!normalized_sql.contains("sum(value)"), "{sql}");
        assert!(
            sql.contains("FROM mz_internal.mz_replica_hydration_history"),
            "{sql}"
        );
        assert!(sql.contains("h.finished_at >= c.started_at"), "{sql}");
    }
}
