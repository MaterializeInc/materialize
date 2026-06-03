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
//! signals through the [`ClusterControllerCtx`] boundary, runs a set of pure
//! [`Strategy`]s, unions their desired contributions, diffs that against the
//! actual replica set, and emits the create/drop and durable-state-write
//! [`Decision`]s that close the gap. It holds no in-memory state: the source of
//! truth is always the catalog plus live signals, pulled fresh each tick.
//!
//! The crate is **pure**. It depends only on primitive id/shape types and the
//! [`ClusterControllerCtx`] trait, never on the adapter or catalog. That
//! boundary is what makes the controller testable against a fake
//! implementation and extractable later without touching controller code.
//!
//! A tick runs two phases per cluster, `update_state` then `desired_replicas`
//! (see [`ClusterController::reconcile`]). Every [`Decision`] carries the
//! durable state it was derived from, and the apply path transacts it only if
//! that state still holds (compare-and-append). So a create or drop derived
//! from a pre-`ALTER` snapshot can never reshape the replica set against the
//! config the `ALTER` has since established. Applies are per cluster, so one
//! cluster's rejection does not block the others, and commands name explicit
//! replicas, so re-emitting one across a lagging view or a restart is a no-op.
//!
//! [`ClusterControllerCtx`]: crate::ctx::ClusterControllerCtx

pub mod ctx;
pub mod strategy;

use std::collections::BTreeSet;

use mz_ore::soft_panic_or_log;

use crate::ctx::{
    ApplyOutcome, ClusterControllerCtx, ClusterState, Decision, ObservedReplica,
    RefreshWindowDecision, ReplicaShape, StateWrite,
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
    /// A controller with the full set of strategies. Each strategy's rustdoc
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

    /// Run one reconcile tick over every managed cluster the ctx reports.
    ///
    /// See the module docs for the two-phase structure. Both phases apply per
    /// cluster, so a compare-and-append rejection on one cluster never blocks
    /// progress on the others.
    pub async fn reconcile(&self, ctx: &mut dyn ClusterControllerCtx) {
        let cluster_ids = ctx.managed_cluster_ids().await;
        if cluster_ids.is_empty() {
            return;
        }

        // Phase 1: update_state. We merge every strategy's write for a cluster
        // into one compare-and-append, applied per cluster and independently of
        // other clusters. Two separate decisions live here.
        //
        // Per cluster, not one batch per tick: a write rejected because a
        // concurrent `ALTER` moved the cluster off its `expected` rejects only
        // that cluster and leaves the rest free to progress. One batched apply
        // would let a single mid-`ALTER` cluster sink the whole tick, the failure
        // mode at large cluster counts where some cluster is almost always
        // mid-`ALTER`.
        //
        // Merged across strategies, not one apply per strategy: every strategy
        // for a cluster shares the same start-of-tick `expected`, so applying
        // them one at a time would let the first write move the cluster off that
        // `expected` and reject all the rest, serializing a cluster's disjoint
        // writes one-per-tick. Merging lands them together under one guard. We
        // still rely on the compare-and-append, not the merge, for `ALTER`
        // safety, which is why the merged write carries the cluster's `expected`.
        // See `merge_state_writes` for the join and its conflict handling.
        let mut states = ctx.cluster_states(&cluster_ids).await;
        self.enrich_hydration(ctx, &mut states).await;
        self.enrich_refresh_window(ctx, &mut states).await;
        let now = ctx.now();
        // Set when we issue any phase-1 apply, applied or rejected. Either way
        // the durable state may have moved (our write, or the concurrent `ALTER`
        // that rejected it), so phase 2 re-reads.
        let mut phase_1_wrote = false;
        // Clusters whose phase-1 write was rejected. We skip their phase 2 this
        // tick. Proceeding would be safe (we re-read below and every create/drop
        // is guard-checked), but a cluster that just lost a race is likely still
        // settling, so we let it recompute next tick instead of emitting work
        // that is probably about to go stale.
        let mut rejected = BTreeSet::new();
        for state in &states {
            let write = self.merge_state_writes(state, now);
            if write.is_empty() {
                continue;
            }
            phase_1_wrote = true;
            let decision = Decision::UpdateClusterState {
                cluster_id: state.cluster_id,
                expected: state.expected(),
                write,
            };
            if ctx.apply(vec![decision]).await == ApplyOutcome::Rejected {
                rejected.insert(state.cluster_id);
            }
        }

        // Phase 2: desired_replicas. The barrier exists so that a cut-over a
        // phase-1 write performed is visible before we diff the replica set
        // against the realized config. We re-read (and re-enrich) only if phase 1
        // wrote. The first read is otherwise still current. A stale diff is
        // harmless: every create/drop carries its `expected` and is guard-rejected
        // if the durable state has since diverged.
        let states = if phase_1_wrote {
            let mut states = ctx.cluster_states(&cluster_ids).await;
            self.enrich_hydration(ctx, &mut states).await;
            self.enrich_refresh_window(ctx, &mut states).await;
            states
        } else {
            states
        };
        let now = ctx.now();
        for state in &states {
            if rejected.contains(&state.cluster_id) {
                continue;
            }
            let decisions = self.collect_replica_decisions(state, now);
            if decisions.is_empty() {
                continue;
            }
            // Per-cluster apply: a guard failure here is isolated to this cluster,
            // and benign anyway since every command names an explicit replica and
            // is reconciled away next tick. We do not retry within the tick.
            let _ = ctx.apply(decisions).await;
        }
    }

    /// Merge every strategy's [`Strategy::update_state`] for one cluster into the
    /// single [`StateWrite`] the tick applies under one compare-and-append.
    ///
    /// The merge is a per-field join, independent of the order strategies run
    /// in: a field set by exactly one strategy is taken as-is, a field no
    /// strategy sets is left unchanged, and a field set to the same value by
    /// several is that value.
    ///
    /// Two strategies setting one field to *different* values is a conflict.
    /// Every field is owned by exactly one strategy, so by design it cannot
    /// happen and the merge is really a disjoint union. We treat a conflict as
    /// an invariant violation rather than a condition to resolve: there is no
    /// safety-meaningful winner to pick for a contended `size` or record, so we
    /// trip [`soft_panic_or_log!`] (a panic under test/CI soft assertions, a
    /// logged error in production) and leave the field unchanged, the only
    /// outcome that cannot make things worse. A persistent conflict then freezes
    /// that field and keeps tripping the alarm, which is the point: surface the
    /// design bug loudly instead of silently picking an arbitrary value.
    fn merge_state_writes(&self, state: &ClusterState, now: mz_repr::Timestamp) -> StateWrite {
        let writes: Vec<StateWrite> = self
            .strategies
            .iter()
            .map(|strategy| strategy.update_state(state, now))
            .filter(|write| !write.is_empty())
            .collect();

        let mut conflicts: Vec<&'static str> = Vec::new();
        // Exhaustive construction (every field named, no `..`): a field added to
        // `StateWrite` is a compile error here until its join is spelled out.
        let merged = StateWrite {
            new_size: join(
                "size",
                writes.iter().map(|w| w.new_size.clone()),
                &mut conflicts,
            ),
            new_replication_factor: join(
                "replication_factor",
                writes.iter().map(|w| w.new_replication_factor),
                &mut conflicts,
            ),
            new_availability_zones: join(
                "availability_zones",
                writes.iter().map(|w| w.new_availability_zones.clone()),
                &mut conflicts,
            ),
            new_logging: join(
                "logging",
                writes.iter().map(|w| w.new_logging.clone()),
                &mut conflicts,
            ),
            reconfiguration: join(
                "reconfiguration",
                writes.iter().map(|w| w.reconfiguration.clone()),
                &mut conflicts,
            ),
            burst: join(
                "burst",
                writes.iter().map(|w| w.burst.clone()),
                &mut conflicts,
            ),
        };

        if !conflicts.is_empty() {
            soft_panic_or_log!(
                "cluster {:?}: strategies produced conflicting state writes for \
                 field(s) {}; leaving those fields unchanged. Strategies must own \
                 disjoint `StateWrite` fields.",
                state.cluster_id,
                conflicts.join(", "),
            );
        }

        merged
    }

    /// Populate each cluster's [`ClusterState::has_hydratable_objects`] and
    /// [`ClusterState::hydrated_replicas`] live signals where a strategy will
    /// consult them this tick (see [`Self::needs_hydration_signal`]). Probing is
    /// per-cluster and not free, so a steady cluster with no reconfiguration and
    /// no possible burst is never probed.
    async fn enrich_hydration(
        &self,
        ctx: &mut dyn ClusterControllerCtx,
        states: &mut [ClusterState],
    ) {
        for state in states.iter_mut() {
            // The burst strategy arms only for a cluster with at least one
            // hydratable object, so pull that bit first for arming candidates.
            // Besides gating the arm condition itself, it lets us skip the
            // hydration probe entirely for an object-less cluster nothing else
            // needs probed.
            if Self::burst_arming_candidate(state) {
                state.has_hydratable_objects = ctx.has_hydratable_objects(state.cluster_id).await;
            }
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

    /// Whether the burst strategy could *arm* on this cluster this tick: no
    /// burst record yet, but the cluster carries an `ON HYDRATION` policy, burst
    /// is enabled env-wide, and the cluster is On. Exactly the condition under
    /// which the strategy's arm branch consults the object-existence and
    /// hydration signals.
    fn burst_arming_candidate(state: &ClusterState) -> bool {
        state.burst.is_none()
            && state.burst_enabled
            && state.replication_factor > 0
            && state
                .auto_scaling_policy
                .as_ref()
                .is_some_and(|p| p.on_hydration.is_some())
    }

    /// Whether a strategy needs this cluster's per-replica hydration signal this
    /// tick: a reconfiguration is in flight (graceful cut-over), a burst is in
    /// flight (linger/re-arm lifecycle), or a burst could arm *and* the cluster
    /// has something to hydrate. With zero hydratable objects the arm branch
    /// never fires, so the probe would be wasted.
    fn needs_hydration_signal(state: &ClusterState) -> bool {
        if state.reconfiguration.is_some() || state.burst.is_some() {
            return true;
        }
        Self::burst_arming_candidate(state) && state.has_hydratable_objects
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

/// Join one `StateWrite` field across the strategies that set it: `None` if
/// none did, the common value if one or more set it to the same value, and
/// `None` with `field` pushed onto `conflicts` if two set it to different
/// values. The result and the conflict signal depend only on the set of values,
/// not the order they arrive in.
fn join<T: PartialEq>(
    field: &'static str,
    values: impl IntoIterator<Item = Option<T>>,
    conflicts: &mut Vec<&'static str>,
) -> Option<T> {
    let mut merged: Option<T> = None;
    for value in values.into_iter().flatten() {
        match &merged {
            None => merged = Some(value),
            Some(existing) if *existing == value => {}
            // Two strategies disagree on this field. Record it and leave the
            // field unchanged; merge_state_writes raises the alarm.
            Some(_) => {
                conflicts.push(field);
                return None;
            }
        }
    }
    merged
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
///   merged `audit_detail` of the slots behind it (per shape, first `Some` wins, as
///   only the on-refresh strategy produces one, so the rule is documented,
///   not load-bearing); drops carry no attribution. A drop happens exactly when
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

    // Track existing names so freshly-created replicas avoid collisions: both the
    // controller-owned replicas and the non-owned (INTERNAL / BILLED AS) names the
    // observation reserves, so a generated name can never collide with a replica
    // already on the cluster.
    let used_names: Vec<&str> = state
        .replicas
        .iter()
        .map(|r| r.name.as_str())
        .chain(state.reserved_replica_names.iter().map(String::as_str))
        .collect();
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
