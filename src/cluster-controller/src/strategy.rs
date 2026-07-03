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

use crate::ctx::{ClusterState, ReplicaShape, StateWrite};

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

    fn desired_replicas(&self, state: &ClusterState, _now: Timestamp) -> Vec<DesiredReplica> {
        let shape = state.realized_shape();
        (0..state.replication_factor)
            .map(|_| DesiredReplica {
                shape: shape.clone(),
            })
            .collect()
    }
}
