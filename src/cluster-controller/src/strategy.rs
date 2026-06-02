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

use mz_repr::Timestamp;

use crate::ctx::{ClusterState, OnTimeout, ReconfigurationRecord, ReplicaShape, StateWrite};

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
    /// create / that none desired a drop).
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
#[derive(Clone, Copy, Debug, Default)]
pub struct BaselineStrategy;

/// The audit-attribution name of the baseline strategy.
pub const BASELINE_STRATEGY_NAME: &str = "baseline";

impl Strategy for BaselineStrategy {
    fn name(&self) -> &'static str {
        BASELINE_STRATEGY_NAME
    }

    fn desired_replicas(&self, state: &ClusterState, _now: Timestamp) -> Vec<DesiredReplica> {
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
/// Engaged whenever the durable `reconfiguration` record is present. It desires
/// `target.replication_factor` replicas at the target shape *in addition* to
/// the baseline's realized-shape replicas, so both sets serve while the new one
/// hydrates. Once the target replicas are all present and hydrated,
/// `update_state` cuts over: the realized config advances to the target, the
/// record clears, and the old replicas fall out of the union and are dropped.
/// Success takes precedence over the deadline. On a timeout, `Commit` cuts
/// over to the un-hydrated target anyway while `Rollback` (the default) stops
/// desiring the target replicas, reverting to the pre-reconfiguration set, and
/// retains the record as a tombstone.
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
        // Otherwise leave durable state untouched: before the deadline we keep
        // waiting, and past it under `Rollback` we leave the record as a tombstone
        // (`desired_replicas` ceases to contribute the target set, so the
        // controller drops it).
        let hydrated = self.target_hydrated(state, record);
        let commit_on_timeout =
            now > record.deadline && matches!(record.on_timeout, OnTimeout::Commit);
        if hydrated || commit_on_timeout {
            StateWrite {
                new_size: Some(record.target.size.clone()),
                new_replication_factor: Some(record.target.replication_factor),
                new_availability_zones: Some(record.target.availability_zones.clone()),
                new_logging: Some(record.target.logging.clone()),
                reconfiguration: Some(None),
                ..Default::default()
            }
        } else {
            StateWrite::default()
        }
    }

    fn desired_replicas(&self, state: &ClusterState, now: Timestamp) -> Vec<DesiredReplica> {
        let Some(record) = &state.reconfiguration else {
            return Vec::new();
        };

        // Keep desiring the target replicas while the reconfiguration is live —
        // before the deadline, or after it once the target set has hydrated (the
        // success path, awaiting a cut-over a prior tick could not yet apply).
        // Once past the deadline with the target not hydrated the action decides:
        // `Rollback` stops contributing them (the controller drops the in-flight
        // target set), while `Commit` keeps desiring them (they become the realized
        // set at the cut-over `update_state` performs this tick).
        let timed_out = now > record.deadline && !self.target_hydrated(state, record);
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
