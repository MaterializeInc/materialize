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
//! The crate is **pure** — it depends only on primitive id/shape types and the
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
//! Every [`Decision`] — the phase-1 writes and the phase-2 creates/drops alike —
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
    ApplyOutcome, ClusterControllerCtx, ClusterState, Decision, ObservedReplica,
    RefreshWindowDecision, ReplicaShape,
};
use crate::strategy::{
    BaselineStrategy, DesiredReplica, GracefulReconfigurationStrategy, HydrationBurstStrategy,
    OnRefreshStrategy, Strategy,
};

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
    /// A controller with the full set of strategies; each strategy's rustdoc
    /// describes when it engages.
    pub fn new() -> Self {
        Self {
            strategies: vec![
                Box::new(BaselineStrategy),
                Box::new(GracefulReconfigurationStrategy),
                Box::new(OnRefreshStrategy),
                Box::new(HydrationBurstStrategy),
            ],
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
        let mut states = ctx.cluster_states(&cluster_ids).await;
        self.enrich_hydration(ctx, &mut states).await;
        self.enrich_refresh_window(ctx, &mut states).await;
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

        // Phase 2: desired_replicas. We re-read (and re-enrich) the cluster
        // state when phase 1 wrote anything, so that a cut-over performed by
        // those writes is visible before we diff the replica set against the
        // realized config; if phase 1 wrote nothing the first read is still
        // current and we reuse it as is. A stale diff would be harmless either
        // way: every create/drop carries its `expected` and is rejected by the
        // apply guard if the durable state has since diverged.
        let states = if phase_1_wrote {
            let mut states = ctx.cluster_states(&cluster_ids).await;
            self.enrich_hydration(ctx, &mut states).await;
            self.enrich_refresh_window(ctx, &mut states).await;
            states
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

    /// Populate each cluster's [`ClusterState::hydrated_replicas`] live signal
    /// where a strategy will consult it this tick (see
    /// [`Self::needs_hydration_signal`]). Probing is per-cluster and not free,
    /// so a steady cluster with no reconfiguration and no possible burst is
    /// never probed.
    async fn enrich_hydration(
        &self,
        ctx: &mut dyn ClusterControllerCtx,
        states: &mut [ClusterState],
    ) {
        for state in states.iter_mut() {
            if !Self::needs_hydration_signal(state) {
                continue;
            }
            let replica_ids: Vec<_> = state.replicas.iter().map(|r| r.replica_id).collect();
            if replica_ids.is_empty() {
                continue;
            }
            state.hydrated_replicas = ctx.hydrated_replicas(state.cluster_id, &replica_ids).await;
        }
    }

    /// Whether a strategy needs this cluster's per-replica hydration signal this
    /// tick: a reconfiguration is in flight (graceful cut-over) or a burst is in
    /// flight or could be warranted (hydration burst). A burst is *possible* when
    /// the cluster carries an `ON HYDRATION` policy, burst is enabled env-wide, and
    /// the cluster is On — exactly the condition under which the burst strategy
    /// reads hydration.
    fn needs_hydration_signal(state: &ClusterState) -> bool {
        if state.reconfiguration.is_some() || state.burst.is_some() {
            return true;
        }
        state.burst_enabled
            && state.replication_factor > 0
            && state
                .auto_scaling_policy
                .as_ref()
                .is_some_and(|p| p.on_hydration.is_some())
    }

    /// Populate each scheduled cluster's [`ClusterState::refresh_window`] live
    /// signal. Only the on-refresh strategy consults it, and only for
    /// non-MANUAL clusters, so a MANUAL cluster is never probed.
    async fn enrich_refresh_window(
        &self,
        ctx: &mut dyn ClusterControllerCtx,
        states: &mut [ClusterState],
    ) {
        for state in states.iter_mut() {
            if matches!(state.schedule, crate::ctx::ClusterSchedule::Manual) {
                continue;
            }
            state.refresh_window = ctx.refresh_window_inputs(state.cluster_id).await;
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
            let write = strategy.update_state(state, now);
            if write.new_size.is_some() {
                merged.new_size = write.new_size;
            }
            if write.new_replication_factor.is_some() {
                merged.new_replication_factor = write.new_replication_factor;
            }
            if write.new_availability_zones.is_some() {
                merged.new_availability_zones = write.new_availability_zones;
            }
            if write.new_logging.is_some() {
                merged.new_logging = write.new_logging;
            }
            if write.reconfiguration.is_some() {
                merged.reconfiguration = write.reconfiguration;
            }
            if write.burst.is_some() {
                merged.burst = write.burst;
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
/// - Creates carry the names of the strategies that desired the shape, plus the
///   merged `audit_detail` of the slots behind it (per shape, first `Some` wins —
///   only the on-refresh strategy produces one today, so the rule is documented,
///   not load-bearing); drops carry no attribution — a drop happens exactly when
///   no strategy desires the replica.
fn reconcile_replicas(
    state: &ClusterState,
    contributions: &[(&'static str, Vec<DesiredReplica>)],
) -> Vec<Decision> {
    // Desired count per shape = max over strategies of how many that strategy
    // wants of the shape, the union of which strategies want it, and the merged
    // audit detail of the slots.
    let mut desired: Vec<DesiredShape> = Vec::new();
    for (name, slots) in contributions {
        // How many of each shape this strategy wants, and the detail (if any) it
        // attached to the shape's slots.
        let mut per_shape: Vec<(ReplicaShape, usize, Option<RefreshWindowDecision>)> = Vec::new();
        for slot in slots {
            match per_shape
                .iter_mut()
                .find(|(s, _, _)| s.matches(&slot.shape))
            {
                Some((_, count, detail)) => {
                    *count += 1;
                    if detail.is_none() {
                        *detail = slot.audit_detail.clone();
                    }
                }
                None => per_shape.push((slot.shape.clone(), 1, slot.audit_detail.clone())),
            }
        }
        for (shape, count, audit_detail) in per_shape {
            match desired.iter_mut().find(|d| d.shape.matches(&shape)) {
                Some(existing) => {
                    existing.count = existing.count.max(count);
                    if !existing.reasons.contains(name) {
                        existing.reasons.push(*name);
                    }
                    if existing.audit_detail.is_none() {
                        existing.audit_detail = audit_detail;
                    }
                }
                None => desired.push(DesiredShape {
                    shape,
                    count,
                    reasons: vec![*name],
                    audit_detail,
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
                // Multiple creates of one shape in a tick share the same window
                // decision, as the legacy scheduler's events did.
                audit_detail: d.audit_detail.clone(),
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

/// A shape the union desires, how many, which strategies wanted it, and the
/// merged audit detail of the slots behind it.
struct DesiredShape {
    shape: ReplicaShape,
    count: usize,
    reasons: Vec<&'static str>,
    audit_detail: Option<RefreshWindowDecision>,
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
