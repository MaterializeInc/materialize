// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The pure strategy interface and the strategy implementations.
//!
//! A strategy is two pure functions over `(observed cluster state, now)`:
//!
//! - [`Strategy::update_state`] returns the durable writes the strategy wants
//!   (cut-overs, record writes/clears). The controller transacts these in the
//!   tick's first phase.
//! - [`Strategy::desired_replicas`] returns the replica slots the strategy
//!   contributes to the cluster's desired set. The controller unions every
//!   strategy's contribution in the tick's second phase.
//!
//! Both are pure: same inputs, same output, no I/O. The controller is the sole
//! mutator. Strategies never touch the [`ClusterControllerCtx`]; the controller
//! assembles their inputs by pulling through it.
//!
//! [`ClusterControllerCtx`]: crate::ctx::ClusterControllerCtx

use mz_repr::{Timestamp, TimestampManipulation};

use crate::ctx::{
    BurstRecord, ClusterSchedule, ClusterState, OnTimeout, ReconfigurationRecord,
    RefreshWindowDecision, RefreshWindowInputs, ReplicaShape, StateWrite,
};

/// A replica slot a strategy desires this tick. The reconcile kernel unions
/// slots across strategies and matches them by [`ReplicaShape`] against the
/// actual replica set.
#[derive(Clone, Debug)]
pub struct DesiredReplica {
    pub shape: ReplicaShape,
    /// The window decision behind the slot, set only by the on-refresh strategy.
    /// Carried onto the create decision a slot may produce, for the audit log's
    /// `scheduling_policies` detail.
    pub audit_detail: Option<RefreshWindowDecision>,
}

/// One cluster-autoscaling strategy: a pair of pure functions the controller
/// runs each tick. See the module docs.
///
/// `Send + Sync` so the controller (which holds a set of boxed strategies) can
/// run on its own task.
pub trait Strategy: Send + Sync {
    /// A stable identifier used in audit attribution (which strategies desired a
    /// create; drops carry no attribution).
    fn name(&self) -> &'static str;

    /// The durable writes this strategy wants for `state` at time `now`. The
    /// default is no write, which suits a strategy that only ever contributes
    /// replicas (like the baseline).
    fn update_state(&self, _state: &ClusterState, _now: Timestamp) -> StateWrite {
        StateWrite::default()
    }

    /// The replica slots this strategy contributes to `state`'s desired set at
    /// time `now`.
    fn desired_replicas(&self, state: &ClusterState, now: Timestamp) -> Vec<DesiredReplica>;
}

/// The implicit baseline strategy, always present.
///
/// Desires `replication_factor` replicas at the cluster's realized shape
/// (`cluster.size` plus its AZ pool and logging). It holds the steady-state set
/// so that the policy strategies can be purely additive — they only ever add to
/// the baseline. With only the baseline engaged, the desired set equals the
/// realized set, so a steady-state managed cluster reconciles to no decisions.
///
/// The baseline holds the set only for MANUAL clusters. On a scheduled cluster
/// the controller — not the user's `replication_factor` — owns the replica set,
/// so the baseline desires nothing there and the on-refresh strategy is the sole
/// contributor. (The on-refresh strategy also normalizes a scheduled cluster's
/// `replication_factor` to `0` via `update_state`, so the two views agree after
/// the first tick regardless.)
#[derive(Clone, Copy, Debug, Default)]
pub struct BaselineStrategy;

/// The audit-attribution name of the baseline strategy.
pub const BASELINE_STRATEGY_NAME: &str = "baseline";

impl Strategy for BaselineStrategy {
    fn name(&self) -> &'static str {
        BASELINE_STRATEGY_NAME
    }

    fn desired_replicas(&self, state: &ClusterState, _now: Timestamp) -> Vec<DesiredReplica> {
        if !matches!(state.schedule, ClusterSchedule::Manual) {
            return Vec::new();
        }
        let shape = state.realized_shape();
        (0..state.replication_factor)
            .map(|_| DesiredReplica {
                shape: shape.clone(),
                audit_detail: None,
            })
            .collect()
    }
}

/// The graceful (zero-downtime) reconfiguration strategy.
///
/// Engaged whenever the durable `reconfiguration` record is present. It desires
/// `target.replication_factor` replicas at the target shape *in addition* to
/// the baseline's realized-shape replicas, so both sets serve while the new one
/// hydrates. Once the target replicas are all present and hydrated,
/// `update_state` cuts over: the realized config advances to the target, the
/// record clears, and the old replicas fall out of the union and are dropped.
/// Success takes precedence over the deadline. On a timeout, `Commit` cuts
/// over to the un-hydrated target anyway while `Rollback` (the default) clears
/// the record without touching the realized config and stops desiring the
/// target replicas, reverting to the pre-reconfiguration set; the timeout's
/// papertrail is the audit event the clear induces.
///
/// Both functions are pure over the observed [`ClusterState`]; hydration is read
/// from [`ClusterState::hydrated_replicas`], which the controller populates by
/// pulling the live signal while a reconfiguration is in flight.
#[derive(Clone, Copy, Debug, Default)]
pub struct GracefulReconfigurationStrategy;

/// The audit-attribution name of the graceful reconfiguration strategy.
pub const GRACEFUL_RECONFIGURATION_STRATEGY_NAME: &str = "graceful-reconfiguration";

impl GracefulReconfigurationStrategy {
    /// Whether the target replicas are all present and hydrated: there are at
    /// least `target.replication_factor` replicas of the target shape and every
    /// such replica reports hydrated.
    ///
    /// Requiring *all* target replicas (not just one) is deliberate: a partial
    /// cut-over would not satisfy the high-availability guarantee of
    /// `replication_factor > 1` at any single size, so we wait for the full target
    /// set before retiring the old one.
    fn target_hydrated(&self, state: &ClusterState, record: &ReconfigurationRecord) -> bool {
        let target_shape = record.target.shape();
        let target_replicas: Vec<_> = state
            .replicas
            .iter()
            .filter(|r| r.shape.matches(&target_shape))
            .collect();
        let target_rf = usize::try_from(record.target.replication_factor).unwrap_or(usize::MAX);
        target_replicas.len() >= target_rf
            && target_replicas
                .iter()
                .all(|r| state.hydrated_replicas.contains(&r.replica_id))
    }
}

impl Strategy for GracefulReconfigurationStrategy {
    fn name(&self) -> &'static str {
        GRACEFUL_RECONFIGURATION_STRATEGY_NAME
    }

    fn update_state(&self, state: &ClusterState, now: Timestamp) -> StateWrite {
        let Some(record) = &state.reconfiguration else {
            return StateWrite::default();
        };

        // Cut over — advancing the realized config to the target and clearing the
        // record — on either of two conditions:
        //   1. the target replicas are all present and hydrated (success, which
        //      takes precedence over the deadline regardless of `on_timeout`), or
        //   2. the deadline has passed un-hydrated and `on_timeout` is `Commit`
        //      (cut over to the not-yet-hydrated target anyway).
        let hydrated = self.target_hydrated(state, record);
        let commit_on_timeout =
            now > record.deadline && matches!(record.on_timeout, OnTimeout::Commit);
        if hydrated || commit_on_timeout {
            return StateWrite {
                new_size: Some(record.target.size.clone()),
                new_replication_factor: Some(record.target.replication_factor),
                new_availability_zones: Some(record.target.availability_zones.clone()),
                new_logging: Some(record.target.logging.clone()),
                reconfiguration: Some(None),
                ..Default::default()
            };
        }

        // Past the deadline un-hydrated under `Rollback`: abandon the
        // reconfiguration by clearing the record while leaving the realized
        // config untouched. The clear is the durable transition the audit
        // `timed-out` event classifies (a clear that does not advance the
        // realized config can only be this), so the timeout is in the history
        // exactly once. With the record gone the strategy disengages and the
        // baseline alone shapes the cluster, dropping the in-flight target set.
        if now > record.deadline && matches!(record.on_timeout, OnTimeout::Rollback) {
            return StateWrite {
                reconfiguration: Some(None),
                ..Default::default()
            };
        }

        // Before the deadline: keep waiting.
        StateWrite::default()
    }

    fn desired_replicas(&self, state: &ClusterState, now: Timestamp) -> Vec<DesiredReplica> {
        let Some(record) = &state.reconfiguration else {
            return Vec::new();
        };

        // Past the deadline with the target not hydrated under `Rollback`: stop
        // contributing the target replicas. `update_state` clears the record in
        // this same tick's first phase, so this usually never fires against a
        // re-read state — it matters when that clear could not land (e.g. the
        // compare-and-append witness was stale), keeping the rollback's replica
        // drops prompt rather than waiting for the clear to retry. Everything
        // else — before the deadline, awaiting a success cut-over past it, or a
        // `Commit` cut-over `update_state` performs this tick — keeps desiring
        // the target set.
        let timed_out = now > record.deadline && !self.target_hydrated(state, record);
        if timed_out && matches!(record.on_timeout, OnTimeout::Rollback) {
            return Vec::new();
        }

        let shape = record.target.shape();
        (0..record.target.replication_factor)
            .map(|_| DesiredReplica {
                shape: shape.clone(),
                audit_detail: None,
            })
            .collect()
    }
}

/// The `ON REFRESH` scheduling strategy.
///
/// Engaged for clusters with a non-MANUAL [`ClusterSchedule`]. It contributes one
/// replica at the cluster's realized shape while the cluster is inside a refresh
/// window, and nothing otherwise. The window decision keys on the bound REFRESH
/// materialized views' write frontiers, their refresh schedules, the configured
/// hydration-time estimate, and the current read timestamp — the same signals the
/// legacy scheduler reads — all carried in [`RefreshWindowInputs`].
///
/// The controller — not the user's `replication_factor` — owns a scheduled
/// cluster's replica set, so [`Strategy::update_state`] normalizes the realized
/// `replication_factor` to `0`. This is self-healing (no migration needed to
/// enable the controller) and makes `mz_clusters.replication_factor` read `0` for
/// a scheduled cluster, with `mz_cluster_replicas` authoritative for what is
/// actually running.
///
/// NB: the decision is re-derived purely from the live signals each tick; there
/// is no "all policies have decided" latch like `cluster_scheduling.rs` needs.
/// That scheduler collects policy decisions asynchronously and across ticks, so
/// turning a cluster off is only safe once every policy has reported. We pull a
/// complete decision from durable + storage state on every tick, so the first
/// tick after a restart already decides from the same inputs as a steady tick.
#[derive(Clone, Copy, Debug, Default)]
pub struct OnRefreshStrategy;

/// The audit-attribution name of the on-refresh scheduling strategy.
pub const ON_REFRESH_STRATEGY_NAME: &str = "on-refresh";

/// A millisecond [`Duration`] as a [`Timestamp`], saturating at [`Timestamp::MAX`]
/// on overflow.
///
/// Both inputs (the cluster's hydration-time estimate and the system compaction
/// estimate) are validated to fit during planning/config, so the saturation is
/// not expected to trigger. Saturating rather than panicking is deliberate: this
/// is a pure strategy kernel, and clamping an out-of-range estimate to the
/// largest representable window degrades to "keep the cluster on" rather than
/// crashing the controller on a bad input.
///
/// [`Duration`]: std::time::Duration
fn duration_to_ts(duration: std::time::Duration) -> Timestamp {
    Timestamp::try_from(duration).unwrap_or(Timestamp::MAX)
}

impl OnRefreshStrategy {
    /// The window decision for the cluster: which bound REFRESH MVs either still
    /// need a refresh (their write frontier has not advanced past the read
    /// timestamp adjusted by the hydration-time estimate) or are estimated to
    /// still need Persist compaction after their last refresh. The cluster
    /// should be On iff either list is non-empty
    /// ([`RefreshWindowDecision::window_open`]), so an open window always names
    /// the MVs that explain it.
    ///
    /// `hydration_time_estimate` comes from the schedule; the remaining signals
    /// come from `inputs`. With no bound REFRESH MVs both lists are empty and
    /// the cluster is Off.
    fn window_decision(
        &self,
        hydration_time_estimate: std::time::Duration,
        inputs: &RefreshWindowInputs,
    ) -> RefreshWindowDecision {
        // 1. Needs refresh: write_frontier < read_ts + hydration_time_estimate.
        // The cluster is turned on `hydration_time_estimate` ahead of a refresh
        // so it can rehydrate before the refresh time.
        let read_ts_adjusted = inputs
            .read_ts
            .step_forward_by(&duration_to_ts(hydration_time_estimate));
        let objects_needing_refresh = inputs
            .refresh_mvs
            .iter()
            .filter(|mv| mv.write_frontier.less_than(&read_ts_adjusted))
            .map(|mv| mv.id)
            .collect();

        // 2. Needs compaction: prev_refresh + compaction_estimate > read_ts. We
        // keep the cluster on for a while after a refresh so Persist can compact.
        let compaction_estimate = duration_to_ts(inputs.compaction_estimate);
        let objects_needing_compaction = inputs
            .refresh_mvs
            .iter()
            .filter(|mv| {
                // `prev_refresh` is None in two cases, both meaning "schedule no
                // compaction time now": no refresh has happened yet (no frontier to
                // round down and no past `AT`), or a `REFRESH EVERY` MV with an empty
                // write frontier (we have no wall-clock handle on its last refresh).
                let prev_refresh = match mv.write_frontier.as_option() {
                    Some(frontier) => frontier.round_down_minus_1(&mv.refresh_schedule),
                    None => mv.refresh_schedule.last_refresh(),
                };
                prev_refresh
                    .is_some_and(|prev| prev.step_forward_by(&compaction_estimate) > inputs.read_ts)
            })
            .map(|mv| mv.id)
            .collect();

        RefreshWindowDecision {
            objects_needing_refresh,
            objects_needing_compaction,
            hydration_time_estimate,
        }
    }
}

impl Strategy for OnRefreshStrategy {
    fn name(&self) -> &'static str {
        ON_REFRESH_STRATEGY_NAME
    }

    fn update_state(&self, state: &ClusterState, _now: Timestamp) -> StateWrite {
        // The controller owns a scheduled cluster's replica set, so hold the
        // realized `replication_factor` at `0`. A stale non-zero value (e.g. left
        // by the legacy scheduler toggling 0↔1) would otherwise have the implicit
        // baseline desire a replica the on-refresh strategy does not — a flap.
        // Only write when it is actually non-zero, to keep steady ticks no-ops.
        if matches!(state.schedule, ClusterSchedule::Manual) || state.replication_factor == 0 {
            return StateWrite::default();
        }
        StateWrite {
            new_replication_factor: Some(0),
            ..Default::default()
        }
    }

    fn desired_replicas(&self, state: &ClusterState, _now: Timestamp) -> Vec<DesiredReplica> {
        let ClusterSchedule::Refresh {
            hydration_time_estimate,
        } = state.schedule
        else {
            return Vec::new();
        };
        // The refresh-window signals are pulled only for scheduled clusters; if
        // they are absent we cannot decide a window, so contribute nothing (the
        // next tick re-pulls).
        let Some(inputs) = &state.refresh_window else {
            return Vec::new();
        };
        let decision = self.window_decision(hydration_time_estimate, inputs);
        if !decision.window_open() {
            return Vec::new();
        }
        // One replica at the realized shape (`cluster.size` plus the cluster's AZ
        // pool and logging), matching what the legacy scheduler brings up. The
        // window decision rides along so the create it may produce can carry the
        // audit detail.
        vec![DesiredReplica {
            shape: state.realized_shape(),
            audit_detail: Some(decision),
        }]
    }
}

/// The hydration-burst strategy.
///
/// Engaged for clusters whose `AUTO SCALING STRATEGY` sets `ON HYDRATION`. While
/// the cluster is On and no steady-state (realized-config) replica has all current
/// objects hydrated, it runs one extra replica at the configured `HYDRATION SIZE`
/// to accelerate hydration; the burst replica tears down a `linger_duration` after
/// the steady set first hydrates. The burst is keyed entirely on the presence of a
/// durable `burst` record (written/cleared by [`Strategy::update_state`]); the
/// burst replica is an ordinary replica — the union/diff reconciler creates and
/// drops it by shape+count with no special identity.
///
/// There is deliberately no TTL on the burst replica: if the steady set can never
/// hydrate at `cluster.size`, the burst stays up indefinitely (the cluster runs
/// permanently oversized, visible in billing and the audit log) — the accepted
/// trade for keeping the cluster serving. Burst is **not** suppressed during a
/// reconfiguration; the two coexist.
///
/// Steady-replica hydration is read from [`ClusterState::hydrated_replicas`], which
/// the controller populates by pulling the live signal while a burst is warranted
/// or in flight.
#[derive(Clone, Copy, Debug, Default)]
pub struct HydrationBurstStrategy;

/// The audit-attribution name of the hydration-burst strategy.
pub const HYDRATION_BURST_STRATEGY_NAME: &str = "hydration-burst";

impl HydrationBurstStrategy {
    /// The cluster's active `ON HYDRATION` policy, but only when burst is permitted
    /// at all: the break-glass flag is on and the cluster is On (`rf > 0`). `None`
    /// otherwise — no burst is warranted and any existing record is torn down.
    fn active_policy<'a>(
        &self,
        state: &'a ClusterState,
    ) -> Option<&'a crate::ctx::OnHydrationPolicy> {
        if !state.burst_enabled || state.replication_factor == 0 {
            return None;
        }
        state.auto_scaling_policy.as_ref()?.on_hydration.as_ref()
    }

    /// Whether at least one steady-state (realized-config) replica reports all
    /// current objects hydrated.
    fn steady_hydrated(&self, state: &ClusterState) -> bool {
        let steady_shape = state.realized_shape();
        state
            .replicas
            .iter()
            .filter(|r| r.shape.matches(&steady_shape))
            .any(|r| state.hydrated_replicas.contains(&r.replica_id))
    }
}

impl Strategy for HydrationBurstStrategy {
    fn name(&self) -> &'static str {
        HYDRATION_BURST_STRATEGY_NAME
    }

    fn update_state(&self, state: &ClusterState, now: Timestamp) -> StateWrite {
        let clear = StateWrite {
            burst: Some(None),
            ..Default::default()
        };

        // Cleanup precedence: a burst no longer warranted by current config tears
        // down regardless of linger. `active_policy` is `None` when burst is
        // disabled / the cluster is off / no policy; a `HYDRATION SIZE` change makes
        // the record's size stale (a fresh record at the new size is written below
        // on a later tick once warranted).
        let Some(policy) = self.active_policy(state) else {
            return if state.burst.is_some() {
                clear
            } else {
                StateWrite::default()
            };
        };
        if let Some(record) = &state.burst {
            if record.burst_size != policy.hydration_size {
                return clear;
            }
        }

        let steady_hydrated = self.steady_hydrated(state);

        match &state.burst {
            // No record: arm a burst only while the steady set is un-hydrated.
            None => {
                if steady_hydrated {
                    StateWrite::default()
                } else {
                    let linger_duration =
                        policy.linger_duration.unwrap_or(state.default_burst_linger);
                    StateWrite {
                        burst: Some(Some(BurstRecord {
                            burst_size: policy.hydration_size.clone(),
                            linger_duration,
                            steady_hydrated_at: None,
                        })),
                        ..Default::default()
                    }
                }
            }
            // Record present: drive the linger/teardown/re-arm lifecycle.
            Some(record) => {
                match (record.steady_hydrated_at, steady_hydrated) {
                    // Steady set hydrated and the linger has elapsed: tear down.
                    // A linger so large it overflows the timestamp space reads as
                    // never-elapsed (hold the burst), matching `duration_to_ts`'s
                    // degrade-to-keep-on intent and keeping the kernel panic-free.
                    (Some(hydrated_at), true)
                        if now
                            > hydrated_at
                                .try_step_forward_by(&duration_to_ts(record.linger_duration))
                                .unwrap_or(Timestamp::MAX) =>
                    {
                        clear
                    }
                    // Steady set hydrated, linger not yet elapsed: hold.
                    (Some(_), true) => StateWrite::default(),
                    // First observation of the steady set hydrated: stamp the
                    // linger start.
                    (None, true) => StateWrite {
                        burst: Some(Some(BurstRecord {
                            steady_hydrated_at: Some(now),
                            ..record.clone()
                        })),
                        ..Default::default()
                    },
                    // The steady set went un-hydrated again after we had stamped a
                    // hydration time: re-arm so the linger restarts after the next
                    // successful hydration.
                    (Some(_), false) => StateWrite {
                        burst: Some(Some(BurstRecord {
                            steady_hydrated_at: None,
                            ..record.clone()
                        })),
                        ..Default::default()
                    },
                    // Steady set still un-hydrated and never stamped: keep waiting.
                    (None, false) => StateWrite::default(),
                }
            }
        }
    }

    fn desired_replicas(&self, state: &ClusterState, _now: Timestamp) -> Vec<DesiredReplica> {
        // Keyed purely on the record's presence: the cleanup arm of `update_state`
        // clears a stale record, so a record present here always reflects a burst
        // the current config still warrants. One replica at the burst size, with
        // the cluster's AZ pool and logging (only the size differs from steady).
        let Some(record) = &state.burst else {
            return Vec::new();
        };
        vec![DesiredReplica {
            shape: ReplicaShape {
                size: record.burst_size.clone(),
                availability_zones: state.availability_zones.clone(),
                logging: state.logging.clone(),
            },
            audit_detail: None,
        }]
    }
}
