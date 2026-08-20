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
//! One sweep visits a single managed user replica, installs a replica-targeted
//! subscribe that diffs that replica's live hydration timestamps against the
//! durable history table, and appends what is missing through the timestamped
//! OCC write path. Including the history table in the read expression is what
//! makes the write idempotent across concurrent `environmentd` processes: two
//! collectors that compute the same row race for one write timestamp, and the
//! loser observes the winner's append through its own subscribe and finds
//! nothing left to write.
//!
//! Collection is sampling, not an event log. An episode whose live row is
//! retracted before its replica's turn in the sweep (a dropped object, or a
//! replica process that restarts first) is not recorded, and cannot be, because
//! the only evidence is gone. See the design doc for why that is accepted here.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use itertools::Itertools;
use mz_adapter_types::dyncfgs::{
    FRONTEND_READ_THEN_WRITE, HYDRATION_HISTORY_COLLECTION_INTERVAL,
    HYDRATION_HISTORY_RETENTION_PERIOD,
};
use mz_catalog::builtin::{MZ_CATALOG_SERVER_CLUSTER, MZ_OBJECT_HYDRATION_HISTORY};
use mz_cluster_client::ReplicaId;
use mz_controller_types::ClusterId;
use mz_ore::collections::CollectionExt;
use mz_ore::now::EpochMillis;
use mz_ore::task;
use mz_repr::CatalogItemId;
use mz_sql::plan::{MutationKind, Params, Plan, ReadThenWritePlan};
use tracing::warn;

use crate::catalog::Catalog;
use crate::coord::{Coordinator, Message};
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

/// Bound on one mutation, including the wait for its read to linearize.
///
/// A mutation that finds nothing to write still waits for the oracle to advance,
/// which can take a full `default_timestamp_interval`, so this has to be well
/// above any sane value of that parameter. Exceeding it skips the step. The next
/// sweep recomputes from current state.
const MUTATION_TIMEOUT: Duration = Duration::from_secs(300);

/// Rows retracted per retention step.
///
/// Retention has to be bounded: the OCC path refuses a selection larger than
/// `max_result_size` before submitting any write, so an unbounded delete over a
/// large backlog would fail identically on every sweep and never shrink the
/// table. Deleting a bounded batch converges instead, across as many sweeps as
/// it takes.
const RETENTION_BATCH_SIZE: usize = 1000;

impl Coordinator {
    /// Schedules the next hydration history sweep.
    ///
    /// Fires are aligned to interval boundaries so that they stay evenly spaced
    /// across restarts, and each sleep is capped so a configuration change is
    /// picked up promptly. Sweeps never overlap: the next one is only scheduled
    /// once the previous one has finished or failed.
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
            let now = self.now();
            let remaining = Duration::from_millis(interval_ms - (now % interval_ms));
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
        // empty collection and find nothing, every sweep.
        let replicas = self
            .catalog()
            .user_cluster_replicas()
            .filter(|replica| replica.config.compute.logging.enabled())
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
            if let Some((cluster_id, replica_id)) = replica {
                sweep.collect(cluster_id, replica_id).await;
            }

            // Retention runs even when collection failed above. A replica that
            // is crash-looping or slow must not be able to stop the table from
            // shrinking back to its retention bound.
            if let Some((cluster_id, replica_id)) = catalog_server_target {
                sweep.retain(cluster_id, replica_id).await;
            }

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
/// Reads worker 0 only, which loses nothing. A worker stamps `hydrated_at` when the
/// reported output frontier passes the as_of, and that frontier moves only as
/// timely's dataflow-wide progress tracking allows, so no worker sees it advance
/// while another still holds capabilities below it. Worker 0's stamp therefore
/// already accounts for the slowest worker, and there is no need to know the worker
/// count or to wait for a complete set of rows.
///
/// Reading one worker also keeps the two stamps on one clock. Each process anchors
/// its logging clock at its own `SystemTime`, so comparing `installed_at` from one
/// worker with `hydrated_at` from another measures skew as much as elapsed time.
///
/// Needs no batch bound. The result is at most the replica's not-yet-recorded
/// dataflows.
fn collect_sql(cluster_id: ClusterId, replica_id: ReplicaId, cutoff: &str) -> String {
    // Interpolating into SQL is safe here: the ids are catalog-internal and the
    // cutoff is an RFC 3339 timestamp we formatted ourselves. Nothing in this
    // query comes from a user.
    //
    // NOTE: The grouping is not cross-worker. We key rows by item id, while the
    // log keys them by global id, and one item can own several global ids at once,
    // as a materialized view being replaced does. Each has its own dataflow and so
    // its own log row, and a replica that installs both in the same instant would
    // otherwise write this table's identity twice in one batch.
    format!(
        "SELECT
            ids.id AS object_id,
            '{cluster_id}'::text AS cluster_id,
            '{replica_id}'::text AS replica_id,
            t.installed_at,
            min(t.started_at) AS started_at,
            min(t.hydrated_at) AS finished_at,
            'hydrated'::text AS status
        FROM mz_introspection.mz_compute_hydration_times_per_worker AS t
        JOIN mz_internal.mz_object_global_ids AS ids ON ids.global_id = t.export_id
        JOIN mz_catalog.mz_objects AS o ON o.id = ids.id
        WHERE t.worker_id = 0
          AND t.hydrated_at IS NOT NULL
          AND t.hydrated_at >= TIMESTAMPTZ '{cutoff}'
          AND ids.id LIKE 'u%'
          AND o.type IN ('index', 'materialized-view')
          AND NOT EXISTS (
              SELECT 1
              FROM mz_internal.mz_object_hydration_history AS h
              WHERE h.object_id = ids.id
                AND h.replica_id = '{replica_id}'::text
                AND h.installed_at = t.installed_at
          )
        GROUP BY ids.id, t.installed_at"
    )
}

/// A bounded batch of history rows that have aged out.
///
/// Only rows with a `finished_at` age out. Every row written today has one, and
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
                finished_at, status
            FROM mz_internal.mz_object_hydration_history
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
    history_id: CatalogItemId,
    wall_time: chrono::DateTime<chrono::Utc>,
    /// Rows finishing before this have aged out. Both steps apply it, so a live
    /// log row cannot resurrect an episode retention just retracted.
    cutoff: String,
}

impl Sweep {
    /// Appends this replica's completed episodes that the table is missing.
    async fn collect(&mut self, cluster_id: ClusterId, replica_id: ReplicaId) {
        let sql = collect_sql(cluster_id, replica_id, &self.cutoff);
        self.run(
            "collect",
            cluster_id,
            replica_id,
            MutationKind::Insert,
            &sql,
        )
        .await
    }

    /// Retracts a bounded batch of aged-out rows.
    async fn retain(&mut self, cluster_id: ClusterId, replica_id: ReplicaId) {
        let sql = retention_sql(&self.cutoff);
        self.run(
            "retention",
            cluster_id,
            replica_id,
            MutationKind::Delete,
            &sql,
        )
        .await
    }

    /// Runs one mutation, logging rather than propagating failure.
    ///
    /// Every failure mode here is expected in normal operation: the targeted
    /// replica can fail or be dropped, a dependency can be replaced, and the
    /// write can lose its timestamp race. None of them are actionable, and the
    /// next sweep recomputes from current state, so they are logged and the
    /// sweep continues.
    async fn run(
        &mut self,
        step: &'static str,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        kind: MutationKind,
        sql: &str,
    ) {
        let mutation = async {
            let plan = plan_mutation(&self.catalog, self.history_id, kind, sql)?;
            let mut session = Session::dummy();
            session.start_transaction_single_stmt(self.wall_time);
            self.client
                .background_read_then_write(
                    &mut session,
                    plan,
                    cluster_id,
                    replica_id,
                    &self.catalog,
                )
                .await?;
            Ok::<_, AdapterError>(())
        };
        match tokio::time::timeout(MUTATION_TIMEOUT, mutation).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                warn!(%step, %cluster_id, %replica_id, %error, "hydration history step failed")
            }
            // The oracle names the timestamp to write at and the subscribe's
            // frontier has to reach it. The log half of that frontier advances on
            // the replica's clock, so a replica whose clock trails `environmentd`
            // by more than its introspection interval keeps sitting below the
            // target and waits here until this fires, every sweep.
            Err(_) => warn!(
                %step, %cluster_id, %replica_id,
                "hydration history step timed out, \
                 the replica's introspection frontier may be trailing the write frontier"
            ),
        }
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

    /// Cross-worker stamps are not comparable, since each process anchors its
    /// logging clock at its own `SystemTime`. The query must therefore stay on one
    /// worker, and must not grow a cross-worker aggregate again.
    #[mz_ore::test]
    fn collect_reads_one_worker() {
        let sql = collect_sql(
            ClusterId::user(1).expect("valid cluster ID"),
            ReplicaId::User(2),
            "1970-01-01T00:00:00+00:00",
        );
        assert!(sql.contains("t.worker_id = 0"), "{sql}");
        assert!(!sql.contains("count("), "{sql}");
        assert!(!sql.contains("max("), "{sql}");
    }
}
