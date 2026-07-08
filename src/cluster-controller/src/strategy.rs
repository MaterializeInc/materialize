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
//! A strategy is two pure functions over `(observed cluster state, live
//! signals, now)`:
//!
//! - [`Strategy::update_state`] returns the durable writes the strategy wants
//!   (cut-overs, record writes/clears). The controller transacts these in the
//!   tick's first phase.
//! - [`Strategy::desired_replicas`] returns the replica slots the strategy
//!   contributes to the cluster's desired set. The controller unions every
//!   strategy's contribution in the tick's second phase.
//!
//! Both are pure: same inputs, same output, no I/O. The controller is the sole
//! mutator. Strategies never touch the [`ClusterControllerCtx`] directly. They
//! declare the live signals they need via [`Strategy::signal_request`] and the
//! controller fetches those before evaluating them.
//!
//! [`ClusterControllerCtx`]: crate::ctx::ClusterControllerCtx

use std::collections::BTreeSet;

use mz_controller_types::ReplicaId;
use mz_repr::Timestamp;

use crate::ctx::{
    ClusterState, OnTimeout, ReconfigurationAudit, ReconfigurationRecord, ReconfigurationStatus,
    ReconfigurationWrite, ReplicaShape, StateWrite,
};

/// A replica slot a strategy desires this tick. The reconcile kernel unions
/// slots across strategies and matches them by [`ReplicaShape`] against the
/// actual replica set.
#[derive(Clone, Debug)]
pub struct DesiredReplica {
    pub shape: ReplicaShape,
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

    /// The live signals this strategy needs to evaluate `state` this tick,
    /// declared as a pure function of the durable state. The kernel unions the
    /// requests across strategies, fetches them through the ctx, and passes the
    /// result to [`Strategy::update_state`] and [`Strategy::desired_replicas`].
    /// The default requests nothing, which suits a strategy that works off
    /// durable state alone (like the baseline).
    fn signal_request(&self, _state: &ClusterState) -> SignalRequest {
        SignalRequest::default()
    }

    /// The durable writes this strategy wants for `state` at time `now`. The
    /// default is no write, which suits a strategy that only ever contributes
    /// replicas (like the baseline). An empty [`StateWrite`] means "write
    /// nothing": the kernel drops it without emitting a decision.
    fn update_state(
        &self,
        _state: &ClusterState,
        _signals: &LiveSignals,
        _now: Timestamp,
    ) -> StateWrite {
        StateWrite::default()
    }

    /// The replica slots this strategy contributes to `state`'s desired set at
    /// time `now`.
    fn desired_replicas(
        &self,
        state: &ClusterState,
        signals: &LiveSignals,
        now: Timestamp,
    ) -> Vec<DesiredReplica>;
}

/// The live signals a strategy asks the kernel to fetch before evaluating a
/// cluster, declared through [`Strategy::signal_request`].
///
/// Live signals are observations (hydration and the like) that are not durable
/// state, so they never participate in the compare-and-append witness. Keeping
/// them out of [`ClusterState`] keeps that type exactly the witness material
/// plus the observed replica set.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SignalRequest {
    /// Probe which of the cluster's replicas report all collections hydrated.
    pub hydration: bool,
}

impl SignalRequest {
    /// The union of two requests: a signal is fetched if any strategy asks.
    pub fn union(self, other: SignalRequest) -> SignalRequest {
        // Exhaustive destructure (no `..`): a signal added to the request is a
        // compile error here until its union is spelled out.
        let SignalRequest { hydration } = other;
        SignalRequest {
            hydration: self.hydration || hydration,
        }
    }
}

/// The fulfilled live signals for one cluster, fetched by the kernel per the
/// unioned [`SignalRequest`] and passed alongside [`ClusterState`].
///
/// A signal nobody requested is left at its empty default, so a strategy must
/// only read what it declared in [`Strategy::signal_request`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LiveSignals {
    /// The replicas observed this tick to have *all* current collections on the
    /// cluster hydrated.
    pub hydrated_replicas: BTreeSet<ReplicaId>,
}

/// The implicit baseline strategy, always present.
///
/// Desires `replication_factor` replicas at the cluster's realized shape
/// (`cluster.size` plus its AZ pool and logging). It holds the steady-state set
/// so that the policy strategies can be purely additive. They only ever add to
/// the baseline. With only the baseline engaged, the desired set equals the
/// realized set, so a steady-state managed cluster reconciles to no decisions.
#[derive(Clone, Copy, Debug, Default)]
pub struct BaselineStrategy;

/// The audit-attribution name of the baseline strategy.
pub const BASELINE_STRATEGY_NAME: &str = "baseline";

impl Strategy for BaselineStrategy {
    fn name(&self) -> &'static str {
        BASELINE_STRATEGY_NAME
    }

    fn desired_replicas(
        &self,
        state: &ClusterState,
        _signals: &LiveSignals,
        _now: Timestamp,
    ) -> Vec<DesiredReplica> {
        let shape = state.realized_shape();
        (0..state.replication_factor)
            .map(|_| DesiredReplica {
                shape: shape.clone(),
            })
            .collect()
    }
}

/// The graceful (zero-downtime) reconfiguration strategy.
///
/// Engaged whenever the durable `reconfiguration` record is in progress. It
/// desires `target.replication_factor` replicas at the target shape in addition
/// to the baseline's realized-shape replicas, so both sets serve while the new
/// one hydrates. Once rf-many target replicas are present and hydrated,
/// `update_state` cuts over: the realized config advances to the target, the
/// record is marked finalized, and the old replicas fall out of the union and
/// are dropped. Success takes precedence over the deadline. On a timeout,
/// `Commit` cuts over to the un-hydrated target anyway while `Rollback` (the
/// default) marks the record timed out without touching the realized config and
/// stops desiring the target replicas, reverting to the pre-reconfiguration set.
///
/// Both functions are pure over the observed [`ClusterState`] and the fetched
/// [`LiveSignals`]. Hydration is requested via [`Strategy::signal_request`]
/// exactly while an in-progress reconfiguration is present.
#[derive(Clone, Copy, Debug, Default)]
pub struct GracefulReconfigurationStrategy;

/// The audit-attribution name of the graceful reconfiguration strategy.
pub const GRACEFUL_RECONFIGURATION_STRATEGY_NAME: &str = "graceful-reconfiguration";

impl GracefulReconfigurationStrategy {
    /// Whether the cut-over precondition holds: at least
    /// `target.replication_factor` replicas of the target shape report
    /// hydrated.
    ///
    /// Requiring rf-many hydrated replicas (not just one) preserves the
    /// high-availability guarantee of `replication_factor > 1` across the
    /// cut-over. Extra target-shape replicas beyond the rf do not block: the
    /// post-cut-over reconcile retires them anyway, so waiting for them to
    /// hydrate would only delay the cut-over.
    fn target_hydrated(
        &self,
        state: &ClusterState,
        signals: &LiveSignals,
        record: &ReconfigurationRecord,
    ) -> bool {
        let target_shape = record.target.shape();
        let hydrated_target_replicas = state
            .replicas
            .iter()
            .filter(|r| r.shape.matches(&target_shape))
            .filter(|r| signals.hydrated_replicas.contains(&r.replica_id))
            .count();
        let target_rf = usize::try_from(record.target.replication_factor).unwrap_or(usize::MAX);
        hydrated_target_replicas >= target_rf
    }
}

impl Strategy for GracefulReconfigurationStrategy {
    fn name(&self) -> &'static str {
        GRACEFUL_RECONFIGURATION_STRATEGY_NAME
    }

    fn signal_request(&self, state: &ClusterState) -> SignalRequest {
        SignalRequest {
            hydration: state
                .reconfiguration
                .as_ref()
                .is_some_and(|record| record.is_in_progress()),
        }
    }

    fn update_state(
        &self,
        state: &ClusterState,
        signals: &LiveSignals,
        now: Timestamp,
    ) -> StateWrite {
        let Some(record) = &state.reconfiguration else {
            return StateWrite::default();
        };
        if !record.is_in_progress() {
            return StateWrite::default();
        }

        // Cut over by advancing the realized config to the target and marking
        // the record finalized on either of two conditions:
        //   1. rf-many target replicas are present and hydrated (success, which
        //      takes precedence over the deadline regardless of `on_timeout`), or
        //   2. the deadline has been reached un-hydrated and `on_timeout` is
        //      `Commit` (cut over to the not-yet-hydrated target anyway).
        //
        // NOTE: the deadline is reached at `now >= deadline`, not `now > deadline`.
        // A `WAIT FOR '0s'` writes `deadline = now` to request an immediate
        // cut-over. With a strict `>`, a first tick landing at exactly that
        // timestamp would miss the deadline, so phase 2 would provision the overlap
        // target replicas and only a later tick would cut over. `>=` fires the
        // deadline the instant it is reached, so the zero-timeout cut-over happens
        // on the first tick, before any overlap replica is desired.
        let hydrated = self.target_hydrated(state, signals, record);
        let deadline_reached = now >= record.deadline;
        let commit_on_timeout = deadline_reached && matches!(record.on_timeout, OnTimeout::Commit);
        if hydrated || commit_on_timeout {
            return StateWrite {
                new_size: Some(record.target.size.clone()),
                new_replication_factor: Some(record.target.replication_factor),
                new_availability_zones: Some(record.target.availability_zones.0.clone()),
                new_logging: Some(record.target.logging.clone()),
                reconfiguration: Some(ReconfigurationWrite {
                    record: Some(ReconfigurationRecord {
                        status: ReconfigurationStatus::Finalized,
                        ..record.clone()
                    }),
                    // A cut-over that only happens because the deadline passed
                    // under `Commit` is forced: the target has not hydrated.
                    // Declared here because only this decision point knows.
                    // The durable status reads `Finalized` either way.
                    audit: Some(ReconfigurationAudit::Finalized { forced: !hydrated }),
                }),
                ..Default::default()
            };
        }

        // Past the deadline un-hydrated under `Rollback`: abandon the
        // reconfiguration while leaving the realized config untouched. The
        // terminal status is the durable transition the audit event records. With
        // the record no longer in progress the strategy stops contributing the
        // target set, so the baseline alone shapes the cluster.
        if deadline_reached && matches!(record.on_timeout, OnTimeout::Rollback) {
            return StateWrite {
                reconfiguration: Some(ReconfigurationWrite {
                    record: Some(ReconfigurationRecord {
                        status: ReconfigurationStatus::TimedOut,
                        ..record.clone()
                    }),
                    audit: Some(ReconfigurationAudit::TimedOut),
                }),
                ..Default::default()
            };
        }

        // Before the deadline: keep waiting.
        StateWrite::default()
    }

    fn desired_replicas(
        &self,
        state: &ClusterState,
        signals: &LiveSignals,
        now: Timestamp,
    ) -> Vec<DesiredReplica> {
        let Some(record) = &state.reconfiguration else {
            return Vec::new();
        };
        if !record.is_in_progress() {
            return Vec::new();
        }

        // Past the deadline with the target not hydrated under `Rollback`: stop
        // contributing the target replicas. `update_state` marks the record
        // timed out in this same tick's first phase, so this usually never fires
        // against a re-read state. It matters when the deadline crosses between
        // the two phases' `ctx.now()` reads within one tick: phase 1 saw the
        // deadline unreached and wrote nothing, phase 2 sees it reached here and
        // already stops desiring the target, keeping the rollback's replica
        // drops prompt rather than waiting a tick for the status write.
        // Everything else (before the deadline, awaiting a success cut-over
        // past it, or a `Commit` cut-over `update_state` performs this tick)
        // keeps desiring the target set.
        // `now >= deadline` matches `update_state`'s boundary, so a zero-timeout
        // rollback stops desiring the target on the same tick it marks the
        // record timed out.
        let timed_out = now >= record.deadline && !self.target_hydrated(state, signals, record);
        if timed_out && matches!(record.on_timeout, OnTimeout::Rollback) {
            return Vec::new();
        }

        let shape = record.target.shape();
        (0..record.target.replication_factor)
            .map(|_| DesiredReplica {
                shape: shape.clone(),
            })
            .collect()
    }
}
