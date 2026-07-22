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
use std::time::Duration;

use mz_controller_types::ReplicaId;
use mz_repr::Timestamp;

use crate::ctx::{
    AvailabilityZones, BurstAudit, BurstFinishCause, BurstRecord, BurstWrite, ClusterState,
    OnTimeout, ReconfigurationAudit, ReconfigurationRecord, ReconfigurationStatus,
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
    /// declared as a pure function of the durable state and the tick's config
    /// signals. The kernel unions the requests across strategies, fetches them
    /// through the ctx, and passes the result to [`Strategy::update_state`] and
    /// [`Strategy::desired_replicas`]. The default requests nothing, which suits
    /// a strategy that works off durable state alone (like the baseline).
    fn signal_request(&self, _state: &ClusterState, _config: &ConfigSignals) -> SignalRequest {
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
        _config: &ConfigSignals,
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
        config: &ConfigSignals,
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
    /// Check whether the cluster has at least one hydratable object bound to
    /// it. See `ClusterControllerCtx::has_hydratable_objects` for what counts.
    pub hydratable_objects: bool,
}

impl SignalRequest {
    /// The union of two requests: a signal is fetched if any strategy asks.
    pub fn union(self, other: SignalRequest) -> SignalRequest {
        // Exhaustive destructure (no `..`): a signal added to the request is a
        // compile error here until its union is spelled out.
        let SignalRequest {
            hydration,
            hydratable_objects,
        } = other;
        SignalRequest {
            hydration: self.hydration || hydration,
            hydratable_objects: self.hydratable_objects || hydratable_objects,
        }
    }
}

/// Environment-wide configuration the strategies consult, latched by the kernel
/// once per tick from the controller's dyncfgs so every strategy decides against
/// one consistent config. Not durable cluster state, so never witness material.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConfigSignals {
    /// Whether the hydration-burst strategy is enabled environment-wide (the
    /// break-glass flag).
    pub burst_enabled: bool,
    /// The system-default burst linger duration, written into a new `burst`
    /// record when the policy's `linger_duration` is omitted.
    pub default_burst_linger: Duration,
}

/// The fulfilled live signals for one cluster, fetched by the kernel per the
/// unioned [`SignalRequest`] and passed alongside [`ClusterState`].
///
/// A signal nobody requested is left at its empty default, so a strategy must
/// only read what it declared in [`Strategy::signal_request`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LiveSignals {
    /// The replicas observed this tick to be online and to have *all* current
    /// collections on the cluster hydrated.
    pub hydrated_replicas: BTreeSet<ReplicaId>,
    /// Whether the cluster has at least one hydratable object. `false` when not
    /// requested.
    pub has_hydratable_objects: bool,
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
        _config: &ConfigSignals,
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
            .filter(|r| r.owned_shape().is_some_and(|s| s.matches(&target_shape)))
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

    fn signal_request(&self, state: &ClusterState, _config: &ConfigSignals) -> SignalRequest {
        SignalRequest {
            hydration: state
                .reconfiguration
                .as_ref()
                .is_some_and(|record| record.is_in_progress()),
            ..Default::default()
        }
    }

    fn update_state(
        &self,
        state: &ClusterState,
        signals: &LiveSignals,
        _config: &ConfigSignals,
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
        _config: &ConfigSignals,
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

/// A millisecond [`Duration`] as a [`Timestamp`], saturating at [`Timestamp::MAX`]
/// on overflow rather than panicking the controller on a bad input.
///
/// [`Duration`]: std::time::Duration
fn duration_to_ts(duration: std::time::Duration) -> Timestamp {
    Timestamp::try_from(duration).unwrap_or(Timestamp::MAX)
}

/// The hydration-burst strategy.
///
/// Engaged for clusters whose `AUTO SCALING STRATEGY` sets `ON HYDRATION`. While
/// the cluster is On and there exists an object on it that no steady-state
/// (realized-config) replica has hydrated, it runs one extra replica at the
/// configured `HYDRATION SIZE` to accelerate hydration; the burst replica tears
/// down a `linger_duration` after the steady set first hydrates. Zero objects
/// make the condition vacuously unsatisfied, so a brand-new cluster never bursts
/// before its first object lands. The burst is keyed entirely on the presence of a
/// durable `burst` record (written/cleared by [`Strategy::update_state`]); the
/// burst replica is an ordinary replica. The union/diff reconciler creates and
/// drops it by shape+count with no special identity.
///
/// There is deliberately no TTL on the burst replica: if the steady set can never
/// hydrate at `cluster.size`, the burst stays up indefinitely (the cluster runs
/// permanently oversized, visible in billing and the audit log), the accepted
/// trade for keeping the cluster serving. Burst is **not** suppressed during a
/// reconfiguration; the two coexist.
///
/// Steady-replica hydration and object existence are live signals requested via
/// [`Strategy::signal_request`] while an `ON HYDRATION` policy is active.
#[derive(Clone, Copy, Debug, Default)]
pub struct HydrationBurstStrategy;

/// The audit-attribution name of the hydration-burst strategy.
pub const HYDRATION_BURST_STRATEGY_NAME: &str = "hydration-burst";

impl HydrationBurstStrategy {
    /// The cluster's active `ON HYDRATION` policy, but only when burst is permitted
    /// at all: the break-glass flag is on and the cluster is On (`rf > 0`). `None`
    /// otherwise. No burst is warranted and any existing record is torn down.
    fn active_policy<'a>(
        &self,
        state: &'a ClusterState,
        config: &ConfigSignals,
    ) -> Option<&'a crate::ctx::OnHydrationPolicy> {
        if !config.burst_enabled || state.replication_factor == 0 {
            return None;
        }
        state.auto_scaling_policy.as_ref()?.on_hydration.as_ref()
    }

    /// The in-flight burst record, but only while the current config still
    /// warrants it: the policy is active ([`Self::active_policy`]) and the
    /// record's size matches the policy's `HYDRATION SIZE`. `None` for a stale
    /// record, which `update_state` tears down.
    fn warranted_record<'a>(
        &self,
        state: &'a ClusterState,
        config: &ConfigSignals,
    ) -> Option<&'a BurstRecord> {
        let record = state.burst.as_ref()?;
        // `active_policy` already folds in `replication_factor != 0`, so the
        // shared predicate's own check is redundant here, but passing the real
        // value keeps this a faithful call of the one warrant definition.
        let hydration_size = self
            .active_policy(state, config)
            .map(|policy| policy.hydration_size.as_str());
        mz_adapter_types::cluster_state::burst_record_warranted(
            &record.burst_size,
            state.replication_factor,
            hydration_size,
        )
        .then_some(record)
    }

    /// Whether at least one steady-state (realized-config) replica reports all
    /// current objects hydrated. `false` when no steady replica reports at all
    /// (absent, or not yet registered with the compute controller).
    fn steady_hydrated(&self, state: &ClusterState, signals: &LiveSignals) -> bool {
        let steady_shape = state.realized_shape();
        state
            .replicas
            .iter()
            .filter(|r| r.owned_shape().is_some_and(|s| s.matches(&steady_shape)))
            .any(|r| signals.hydrated_replicas.contains(&r.replica_id))
    }
}

impl Strategy for HydrationBurstStrategy {
    fn name(&self) -> &'static str {
        HYDRATION_BURST_STRATEGY_NAME
    }

    fn signal_request(&self, state: &ClusterState, config: &ConfigSignals) -> SignalRequest {
        // Hydration drives both the arm check and the linger lifecycle. Object
        // existence only gates arming, so it is requested only record-less.
        let active = self.active_policy(state, config).is_some();
        SignalRequest {
            hydration: active,
            hydratable_objects: active && state.burst.is_none(),
        }
    }

    fn update_state(
        &self,
        state: &ClusterState,
        signals: &LiveSignals,
        config: &ConfigSignals,
        now: Timestamp,
    ) -> StateWrite {
        // Both teardown arms clear the record, but they declare different
        // causes: only this decision point knows whether the burst ran its
        // course or was cut short by a config change.
        let clear = |cause: BurstFinishCause| StateWrite {
            burst: Some(BurstWrite {
                record: None,
                audit: Some(BurstAudit::Finished { cause }),
            }),
            ..Default::default()
        };

        // Cleanup precedence: a burst no longer warranted tears down regardless
        // of linger. Catalog writes retire records they invalidate themselves,
        // so this arm mainly covers the burst dyncfg switching off, and
        // backstops any stale record that reaches us anyway.
        if state.burst.is_some() && self.warranted_record(state, config).is_none() {
            return clear(BurstFinishCause::NoLongerWarranted);
        }
        let Some(policy) = self.active_policy(state, config) else {
            // No record (the cleanup above handled that) and no active policy:
            // nothing to arm.
            return StateWrite::default();
        };

        let steady_hydrated = self.steady_hydrated(state, signals);

        match &state.burst {
            // No record: arm a burst only while some object exists that the
            // steady set has not hydrated. Without the object gate, a brand-new
            // cluster would burst at creation with nothing to accelerate (an
            // absent steady replica reads as un-hydrated). The record-present
            // arms below do not consult the gate: if all objects are dropped
            // mid-burst, the steady set reads hydrated and the linger clears
            // the record.
            None => {
                if steady_hydrated || !signals.has_hydratable_objects {
                    StateWrite::default()
                } else {
                    let linger_duration = policy
                        .linger_duration
                        .unwrap_or(config.default_burst_linger);
                    StateWrite {
                        burst: Some(BurstWrite {
                            record: Some(BurstRecord {
                                burst_size: policy.hydration_size.clone(),
                                linger_duration,
                                steady_hydrated_at: None,
                            }),
                            audit: Some(BurstAudit::Started),
                        }),
                        ..Default::default()
                    }
                }
            }
            // Record present: drive the linger/teardown/re-arm lifecycle.
            Some(record) => {
                match (record.steady_hydrated_at, steady_hydrated) {
                    // Steady set hydrated and the linger has elapsed: tear down.
                    // A linger that overflows the timestamp space reads as
                    // never-elapsed.
                    (Some(hydrated_at), true)
                        if now
                            > hydrated_at
                                .try_step_forward_by(&duration_to_ts(record.linger_duration))
                                .unwrap_or(Timestamp::MAX) =>
                    {
                        clear(BurstFinishCause::LingerElapsed)
                    }
                    // Steady set hydrated, linger not yet elapsed: hold.
                    (Some(_), true) => StateWrite::default(),
                    // First observation of the steady set hydrated: stamp the
                    // linger start. A bookkeeping rewrite, not a lifecycle
                    // transition, so it declares no audit.
                    (None, true) => StateWrite {
                        burst: Some(BurstWrite {
                            record: Some(BurstRecord {
                                steady_hydrated_at: Some(now),
                                ..record.clone()
                            }),
                            audit: None,
                        }),
                        ..Default::default()
                    },
                    // The steady set went un-hydrated again after we had stamped a
                    // hydration time: re-arm so the linger restarts after the next
                    // successful hydration. Also bookkeeping: the burst replica
                    // keeps running throughout, so no lifecycle event.
                    (Some(_), false) => StateWrite {
                        burst: Some(BurstWrite {
                            record: Some(BurstRecord {
                                steady_hydrated_at: None,
                                ..record.clone()
                            }),
                            audit: None,
                        }),
                        ..Default::default()
                    },
                    // Steady set still un-hydrated and never stamped: keep waiting.
                    (None, false) => StateWrite::default(),
                }
            }
        }
    }

    fn desired_replicas(
        &self,
        state: &ClusterState,
        _signals: &LiveSignals,
        _config: &ConfigSignals,
        _now: Timestamp,
    ) -> Vec<DesiredReplica> {
        // A present record is never stale: catalog writes retire records they
        // invalidate in the same transaction, and a dyncfg switch-off is
        // handled by phase 1's cleanup (config signals are latched per tick).
        // One replica at the burst size (only the size differs from steady).
        let Some(record) = &state.burst else {
            return Vec::new();
        };
        vec![DesiredReplica {
            shape: ReplicaShape {
                size: record.burst_size.clone(),
                availability_zones: AvailabilityZones(state.availability_zones.clone()),
                logging: state.logging.clone(),
            },
        }]
    }
}
