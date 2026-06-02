// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The cluster controller: the single decision-maker for the replica set of
//! every managed cluster.
//!
//! It is a **reconciler**. Each tick it reads desired cluster state and live
//! signals through the [`ClusterControllerCtx`] seam, runs a set of pure
//! [`Strategy`]s, unions their desired contributions, diffs that against the
//! actual replica set, and emits the create/drop and durable-state-write
//! [`Decision`]s that close the gap. It holds no in-memory state: the source of
//! truth is always the catalog plus live signals, pulled fresh each tick.
//!
//! The crate is **pure**. It depends only on primitive id/shape types and the
//! [`ClusterControllerCtx`] trait, never on the adapter or catalog. That seam is
//! what makes the controller testable against a fake implementation and
//! extractable later without touching controller code.
//!
//! # The reconcile tick
//!
//! Each tick reconciles every managed cluster in two phases separated by a
//! barrier:
//!
//! 1. **`update_state`.** Run every strategy's [`Strategy::update_state`],
//!    collect the durable writes (cut-overs, record writes/clears) as
//!    [`Decision::UpdateClusterState`]s, and apply them under their
//!    compare-and-append guards, awaiting confirmation.
//! 2. **`desired_replicas`.** With those writes applied, re-read state, run
//!    every strategy's [`Strategy::desired_replicas`], union the contributions
//!    (the implicit baseline included), match by [`ReplicaShape`] against the
//!    actual replicas, and emit the creates and drops that close the gap.
//!
//! Every [`Decision`], the phase-1 writes and the phase-2 creates/drops alike,
//! carries the durable state it was derived from, and the apply path transacts a
//! batch only if that state still holds (compare-and-append). This is what keeps
//! a create or drop derived from a pre-`ALTER` snapshot from reshaping the
//! replica set against the config the `ALTER` has since established; on a
//! rejection the controller recomputes from the new state next tick.
//!
//! Commands name explicit replicas, so re-emission across a lagging view or a
//! restart is harmless: a create of a name that already exists and a drop of one
//! already gone are both no-ops.
//!
//! [`ClusterControllerCtx`]: crate::ctx::ClusterControllerCtx
//! [`ReplicaShape`]: crate::ctx::ReplicaShape

pub mod ctx;
pub mod strategy;

use std::collections::BTreeSet;

use crate::ctx::{
    ApplyOutcome, ClusterControllerCtx, ClusterState, Decision, ObservedReplica, ReplicaShape,
};
use crate::strategy::{BaselineStrategy, DesiredReplica, Strategy};

/// The cluster controller. Holds the (stateless) set of strategies and drives a
/// reconcile tick against a [`ClusterControllerCtx`].
pub struct ClusterController {
    strategies: Vec<Box<dyn Strategy>>,
}

impl Default for ClusterController {
    fn default() -> Self {
        Self::new()
    }
}

impl ClusterController {
    /// A controller with only the implicit baseline strategy. This reconciles a
    /// steady-state managed cluster to no decisions; later PRs add the policy
    /// strategies.
    pub fn new() -> Self {
        Self {
            strategies: vec![Box::new(BaselineStrategy)],
        }
    }

    /// Run one reconcile tick over every managed cluster the ctx reports; see
    /// the module docs for the two-phase structure.
    pub async fn reconcile(&self, ctx: &mut dyn ClusterControllerCtx) {
        let cluster_ids = ctx.managed_cluster_ids().await;
        if cluster_ids.is_empty() {
            return;
        }

        // Phase 1: update_state. Collect every strategy's durable writes and
        // apply them under their compare-and-append guards.
        let states = ctx.cluster_states(&cluster_ids).await;
        let now = ctx.now();
        let mut state_decisions = Vec::new();
        for state in &states {
            if let Some(decision) = self.collect_state_write(state, now) {
                state_decisions.push(decision);
            }
        }

        // Clusters whose phase-1 writes were rejected are skipped this tick: a
        // rejection means a concurrent `ALTER` changed the durable state the
        // write was derived from, so any create/drop derived from the same stale
        // snapshot would be unsafe too. We recompute everything next tick.
        let phase_1_wrote = !state_decisions.is_empty();
        let rejected = if !phase_1_wrote {
            Vec::new()
        } else {
            match ctx.apply(state_decisions.clone()).await {
                ApplyOutcome::Applied => Vec::new(),
                ApplyOutcome::Rejected => state_decisions
                    .iter()
                    .filter_map(|decision| match decision {
                        Decision::UpdateClusterState { cluster_id, .. } => Some(*cluster_id),
                        // collect_state_write only ever produces UpdateClusterState.
                        Decision::CreateReplica { .. } | Decision::DropReplica { .. } => None,
                    })
                    .collect(),
            }
        };

        // Phase 2: desired_replicas. The barrier exists so that a cut-over a
        // phase-1 write performed is visible before we diff the replica set
        // against the realized config. When phase 1 wrote nothing the first read
        // is still the current view, so we reuse it; otherwise we re-read to pick
        // up the applied writes. A stale diff is harmless either way: every
        // create/drop carries its `expected` and is rejected by the apply guard
        // if the durable state has since diverged.
        let states = if phase_1_wrote {
            ctx.cluster_states(&cluster_ids).await
        } else {
            states
        };
        let now = ctx.now();
        let mut replica_decisions = Vec::new();
        for state in &states {
            if rejected.contains(&state.cluster_id) {
                continue;
            }
            replica_decisions.extend(self.collect_replica_decisions(state, now));
        }

        if !replica_decisions.is_empty() {
            // A rejection here is benign: every command names an explicit
            // replica and is reconciled away next tick. We do not retry within
            // the tick.
            let _ = ctx.apply(replica_decisions).await;
        }
    }

    /// Merge every strategy's `update_state` for one cluster into a single
    /// [`Decision::UpdateClusterState`], or `None` if no strategy writes
    /// anything. Strategies are additive and never contradict, so a field a
    /// strategy leaves unset is taken from another strategy that sets it; if two
    /// set the same field the later strategy wins (the baseline never sets any).
    fn collect_state_write(
        &self,
        state: &ClusterState,
        now: mz_repr::Timestamp,
    ) -> Option<Decision> {
        let mut merged = crate::ctx::StateWrite::default();
        for strategy in &self.strategies {
            // Exhaustive destructure (no `..`): a field added to `StateWrite` is a
            // compile error here until its merge is spelled out.
            let crate::ctx::StateWrite {
                new_size,
                new_replication_factor,
                new_availability_zones,
                new_logging,
                reconfiguration,
                burst,
            } = strategy.update_state(state, now);
            if new_size.is_some() {
                merged.new_size = new_size;
            }
            if new_replication_factor.is_some() {
                merged.new_replication_factor = new_replication_factor;
            }
            if new_availability_zones.is_some() {
                merged.new_availability_zones = new_availability_zones;
            }
            if new_logging.is_some() {
                merged.new_logging = new_logging;
            }
            if reconfiguration.is_some() {
                merged.reconfiguration = reconfiguration;
            }
            if burst.is_some() {
                merged.burst = burst;
            }
        }
        if merged.is_empty() {
            return None;
        }
        Some(Decision::UpdateClusterState {
            cluster_id: state.cluster_id,
            expected: state.expected(),
            write: merged,
        })
    }

    /// Diff the unioned desired set against the actual replicas of one cluster
    /// and emit the create/drop decisions that close the gap.
    fn collect_replica_decisions(
        &self,
        state: &ClusterState,
        now: mz_repr::Timestamp,
    ) -> Vec<Decision> {
        // Each strategy's contribution, tagged with the strategy name for
        // attribution.
        let contributions: Vec<(&'static str, Vec<DesiredReplica>)> = self
            .strategies
            .iter()
            .map(|strategy| (strategy.name(), strategy.desired_replicas(state, now)))
            .collect();

        reconcile_replicas(state, &contributions)
    }
}

/// The pure multiset union/diff kernel for one cluster: given each strategy's
/// desired replica slots and the actual replicas, match slots to replicas by
/// shape and emit the creates and drops that close the gap.
///
/// Semantics:
/// - The desired set is the multiset **union** of every strategy's slots: a
///   given shape is desired `max` over strategies (not the sum), since a replica
///   of that shape satisfies every strategy that wants one. This is what makes a
///   replica survive iff *some* strategy desires its shape.
/// - For each shape, if actual count < desired count we create the difference;
///   if actual count > desired count we drop the difference, picking specific
///   excess replicas. A replica of a shape no strategy desires is dropped.
/// - Creates carry the names of the strategies that desired the shape. Drops
///   carry no attribution, because a drop happens exactly when no strategy
///   desires the replica.
fn reconcile_replicas(
    state: &ClusterState,
    contributions: &[(&'static str, Vec<DesiredReplica>)],
) -> Vec<Decision> {
    // Desired count per shape = max over strategies of how many that strategy
    // wants of the shape, and the union of which strategies want it.
    let mut desired: Vec<DesiredShape> = Vec::new();
    for (name, slots) in contributions {
        // How many of each shape this strategy wants.
        let mut per_shape: Vec<(ReplicaShape, usize)> = Vec::new();
        for slot in slots {
            match per_shape.iter_mut().find(|(s, _)| s.matches(&slot.shape)) {
                Some((_, count)) => *count += 1,
                None => per_shape.push((slot.shape.clone(), 1)),
            }
        }
        for (shape, count) in per_shape {
            match desired.iter_mut().find(|d| d.shape.matches(&shape)) {
                Some(existing) => {
                    existing.count = existing.count.max(count);
                    if !existing.reasons.contains(name) {
                        existing.reasons.push(*name);
                    }
                }
                None => desired.push(DesiredShape {
                    shape,
                    count,
                    reasons: vec![*name],
                }),
            }
        }
    }

    // Bucket the actual replicas by shape.
    let mut actual_by_shape: Vec<(ReplicaShape, Vec<&ObservedReplica>)> = Vec::new();
    for replica in &state.replicas {
        match actual_by_shape
            .iter_mut()
            .find(|(s, _)| s.matches(&replica.shape))
        {
            Some((_, replicas)) => replicas.push(replica),
            None => actual_by_shape.push((replica.shape.clone(), vec![replica])),
        }
    }

    let mut decisions = Vec::new();

    // Track existing names so freshly-created replicas avoid collisions.
    let used_names: Vec<&str> = state.replicas.iter().map(|r| r.name.as_str()).collect();
    let mut name_gen = ReplicaNameGen::new(&used_names);

    // The compare-and-append witness for every create/drop this tick emits for
    // the cluster: the apply path rejects the batch if the cluster's durable
    // state has diverged from what we diffed against (e.g. a concurrent `ALTER`),
    // so a stale create/drop can never reshape the replica set against the new
    // config.
    let expected = state.expected();

    // Creates: for each desired shape, fill the gap below its desired count.
    for d in &desired {
        let actual_count = actual_by_shape
            .iter()
            .find(|(s, _)| s.matches(&d.shape))
            .map(|(_, replicas)| replicas.len())
            .unwrap_or(0);
        for _ in actual_count..d.count {
            decisions.push(Decision::CreateReplica {
                cluster_id: state.cluster_id,
                name: name_gen.next_name(),
                shape: d.shape.clone(),
                reasons: d.reasons.clone(),
                expected: expected.clone(),
            });
        }
    }

    // Drops: any actual replica beyond the desired count for its shape, plus
    // every replica of a shape no strategy desires.
    for (shape, replicas) in &actual_by_shape {
        let desired_count = desired
            .iter()
            .find(|d| d.shape.matches(shape))
            .map(|d| d.count)
            .unwrap_or(0);
        for replica in replicas.iter().skip(desired_count) {
            decisions.push(Decision::DropReplica {
                cluster_id: state.cluster_id,
                replica_id: replica.replica_id,
                expected: expected.clone(),
            });
        }
    }

    decisions
}

/// A shape the union desires, how many, and which strategies wanted it.
struct DesiredShape {
    shape: ReplicaShape,
    count: usize,
    reasons: Vec<&'static str>,
}

/// Generates deterministic fresh replica names that avoid a set of in-use names.
///
/// The controller derives names from the observed actual set rather than
/// renaming existing replicas, which keeps re-emission harmless. The concrete
/// naming convention (the `rNN` managed-replica scheme) is the environment's; the
/// kernel only needs distinct, stable-per-tick names, so it uses a simple
/// monotonic scheme starting past the highest observed `rNN` index, and never
/// below `r1` since managed-replica names are 1-based.
struct ReplicaNameGen {
    next: u32,
    used: BTreeSet<String>,
}

impl ReplicaNameGen {
    fn new(used: &[&str]) -> Self {
        let mut highest = 1;
        for name in used {
            if let Some(idx) = name.strip_prefix('r').and_then(|n| n.parse::<u32>().ok()) {
                highest = highest.max(idx + 1);
            }
        }
        Self {
            next: highest,
            used: used.iter().map(|n| n.to_string()).collect(),
        }
    }

    fn next_name(&mut self) -> String {
        loop {
            let name = format!("r{}", self.next);
            self.next += 1;
            if !self.used.contains(&name) {
                self.used.insert(name.clone());
                return name;
            }
        }
    }
}

#[cfg(test)]
mod tests;
