// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;
use std::time::Duration;

use itertools::Itertools;
use mz_adapter_types::cluster_state::{ReconfigurationAudit, burst_record_warranted};
use mz_catalog::builtin::BUILTINS;
use mz_catalog::durable::managed_cluster_replica_name;
use mz_catalog::memory::error::ErrorKind;
use mz_catalog::memory::objects::{
    Cluster, ClusterConfig, ClusterVariant, ClusterVariantManaged, DataSourceDesc,
    ManagedReplicaConfigShape, ReconfigurationState, ReconfigurationStatus, ReconfigurationTarget,
};
use mz_cluster_controller::ctx::{AvailabilityZones, ClusterState, ReplicaShape};
use mz_compute_types::config::ComputeReplicaConfig;
use mz_controller::clusters::{
    ManagedReplicaLocation, ReplicaConfig, ReplicaLocation, ReplicaLogging,
};
use mz_controller_types::{ClusterId, DEFAULT_REPLICA_LOGGING_INTERVAL, ReplicaId};
use mz_ore::cast::CastFrom;
use mz_ore::collections::CollectionExt;
use mz_ore::instrument;
use mz_repr::Timestamp;
use mz_repr::adt::numeric::Numeric;
use mz_repr::role_id::RoleId;
use mz_sql::catalog::{CatalogCluster, CatalogError, ObjectType};
use mz_sql::names::QualifiedItemName;
use mz_sql::plan::{
    self, AlterClusterPlanStrategy, AlterClusterRenamePlan, AlterClusterReplicaRenamePlan,
    AlterClusterSwapPlan, AlterOptionParameter, AlterSetClusterPlan, CreateClusterManagedPlan,
    CreateClusterPlan, CreateClusterReplicaPlan, CreateClusterUnmanagedPlan, CreateClusterVariant,
    PlanClusterOption,
};
use mz_sql::plan::{AlterClusterPlan, OnTimeoutAction};
use mz_sql::session::metadata::SessionMetadata;
use mz_sql::session::vars::{
    MAX_CREDIT_CONSUMPTION_RATE, MAX_REPLICAS_PER_CLUSTER, SystemVars, Var,
};
use mz_storage_types::sources::SourceConnection;
use tracing::{Instrument, Span};

use mz_adapter_types::dyncfgs::{
    DEFAULT_CLUSTER_RECONFIGURATION_TIMEOUT, ENABLE_BACKGROUND_ALTER_CLUSTER,
};

use super::return_if_err;
use crate::catalog::{self, Op, ReplicaCreateDropReason};
use crate::coord::{
    AlterCluster, AlterClusterAwaitReconfiguration, ClusterStage, Coordinator, Message,
    PlanValidity, StageResult, Staged,
};
use crate::{AdapterError, AdapterNotice, ExecuteContext, ExecuteResponse, session::Session};

impl Staged for ClusterStage {
    type Ctx = ExecuteContext;

    fn validity(&mut self) -> &mut PlanValidity {
        match self {
            Self::Alter(stage) => &mut stage.validity,
            Self::AwaitReconfiguration(stage) => &mut stage.validity,
        }
    }

    async fn stage(
        self,
        coord: &mut Coordinator,
        ctx: &mut ExecuteContext,
    ) -> Result<StageResult<Box<Self>>, crate::AdapterError> {
        match self {
            Self::Alter(stage) => {
                coord
                    .sequence_alter_cluster_stage(ctx.session(), stage.plan.clone(), stage.validity)
                    .await
            }
            Self::AwaitReconfiguration(stage) => {
                coord.await_reconfiguration_stage(stage.validity, stage.cluster_id, stage.target)
            }
        }
    }

    fn message(self, ctx: ExecuteContext, span: tracing::Span) -> Message {
        Message::ClusterStageReady {
            ctx,
            span,
            stage: self,
        }
    }

    fn cancel_enabled(&self) -> bool {
        true
    }
}

impl Coordinator {
    #[instrument]
    pub(crate) async fn sequence_alter_cluster_staged(
        &mut self,
        ctx: ExecuteContext,
        plan: plan::AlterClusterPlan,
    ) {
        let stage = return_if_err!(self.alter_cluster_validate(ctx.session(), plan).await, ctx);
        self.sequence_staged(ctx, Span::current(), stage).await;
    }

    #[instrument]
    async fn alter_cluster_validate(
        &self,
        session: &Session,
        plan: plan::AlterClusterPlan,
    ) -> Result<ClusterStage, AdapterError> {
        let validity = PlanValidity::new(
            self.catalog(),
            BTreeSet::new(),
            Some(plan.id.clone()),
            None,
            session.role_metadata().clone(),
        );
        Ok(ClusterStage::Alter(AlterCluster { validity, plan }))
    }

    async fn sequence_alter_cluster_stage(
        &mut self,
        session: &Session,
        plan: plan::AlterClusterPlan,
        validity: PlanValidity,
    ) -> Result<StageResult<Box<ClusterStage>>, AdapterError> {
        let AlterClusterPlan {
            id: cluster_id,
            name: _,
            ref options,
            ref strategy,
        } = plan;

        use mz_catalog::memory::objects::ClusterVariant::*;
        use mz_sql::plan::AlterOptionParameter::*;
        let cluster = self.catalog.get_cluster(cluster_id);
        let config = cluster.config.clone();
        let mut new_config = config.clone();

        match (&new_config.variant, &options.managed) {
            (Managed(_), Reset) | (Managed(_), Unchanged) | (Managed(_), Set(true)) => {}
            (Managed(_), Set(false)) => new_config.variant = Unmanaged,
            (Unmanaged, Unchanged) | (Unmanaged, Set(false)) => {}
            (Unmanaged, Reset) | (Unmanaged, Set(true)) => {
                // Generate a minimal correct configuration

                // Size adjusted later when sequencing the actual configuration change.
                let size = "".to_string();
                let logging = ReplicaLogging {
                    log_logging: false,
                    interval: Some(DEFAULT_REPLICA_LOGGING_INTERVAL),
                };
                new_config.variant = Managed(ClusterVariantManaged {
                    size,
                    availability_zones: Default::default(),
                    logging,
                    arrangement_compression: false,
                    replication_factor: 1,
                    optimizer_feature_overrides: Default::default(),
                    schedule: Default::default(),
                    auto_scaling_strategy: None,
                    reconfiguration: None,
                    burst: None,
                });
            }
        }

        match &mut new_config.variant {
            Managed(ClusterVariantManaged {
                size,
                availability_zones,
                logging,
                arrangement_compression,
                replication_factor,
                optimizer_feature_overrides: _,
                schedule,
                auto_scaling_strategy,
                reconfiguration: _,
                burst: _,
            }) => {
                match &options.size {
                    Set(s) => size.clone_from(s),
                    Reset => coord_bail!("SIZE has no default value"),
                    Unchanged => {}
                }
                match &options.availability_zones {
                    Set(az) => availability_zones.clone_from(az),
                    Reset => *availability_zones = Default::default(),
                    Unchanged => {}
                }
                match &options.introspection_debugging {
                    Set(id) => logging.log_logging = *id,
                    Reset => logging.log_logging = false,
                    Unchanged => {}
                }
                match &options.introspection_interval {
                    Set(ii) => logging.interval = ii.0,
                    Reset => logging.interval = Some(DEFAULT_REPLICA_LOGGING_INTERVAL),
                    Unchanged => {}
                }
                match &options.arrangement_compression {
                    Set(ac) => *arrangement_compression = *ac,
                    Reset => *arrangement_compression = false,
                    Unchanged => {}
                }
                match &options.replication_factor {
                    Set(rf) => *replication_factor = *rf,
                    Reset => {
                        *replication_factor = self
                            .catalog
                            .system_config()
                            .default_cluster_replication_factor()
                    }
                    Unchanged => {}
                }
                match &options.schedule {
                    Set(new_schedule) => {
                        *schedule = new_schedule.clone();
                    }
                    Reset => *schedule = Default::default(),
                    Unchanged => {}
                }
                match &options.auto_scaling_strategy {
                    Set(new_strategy) => auto_scaling_strategy.clone_from(new_strategy),
                    // The default is autoscaling disabled.
                    Reset => *auto_scaling_strategy = None,
                    Unchanged => {}
                }
                if !matches!(options.replicas, Unchanged) {
                    coord_bail!("Cannot change REPLICAS of managed clusters");
                }
            }
            Unmanaged => {
                if !matches!(options.size, Unchanged) {
                    coord_bail!("Cannot change SIZE of unmanaged clusters");
                }
                if !matches!(options.availability_zones, Unchanged) {
                    coord_bail!("Cannot change AVAILABILITY ZONES of unmanaged clusters");
                }
                if !matches!(options.introspection_debugging, Unchanged) {
                    coord_bail!("Cannot change INTROSPECTION DEGUBBING of unmanaged clusters");
                }
                if !matches!(options.introspection_interval, Unchanged) {
                    coord_bail!("Cannot change INTROSPECTION INTERVAL of unmanaged clusters");
                }
                if !matches!(options.arrangement_compression, Unchanged) {
                    coord_bail!(
                        "Cannot change EXPERIMENTAL ARRANGEMENT COMPRESSION of unmanaged clusters"
                    );
                }
                if !matches!(options.replication_factor, Unchanged) {
                    coord_bail!("Cannot change REPLICATION FACTOR of unmanaged clusters");
                }
                if !matches!(options.auto_scaling_strategy, Unchanged) {
                    coord_bail!("Cannot change AUTO SCALING STRATEGY of unmanaged clusters");
                }
            }
        }

        match &options.workload_class {
            Set(wc) => new_config.workload_class.clone_from(wc),
            Reset => new_config.workload_class = None,
            Unchanged => {}
        }

        let reconfiguration_in_flight = matches!(
            &config.variant,
            Managed(managed) if managed
                .reconfiguration
                .as_ref()
                .is_some_and(|record| record.is_in_progress())
        );

        // The schedule decides which strategy owns the cluster's replica set
        // (the baseline for MANUAL, on-refresh otherwise), and the sequencer
        // never writes a reconfiguration record for a scheduled cluster (see
        // the routing below). Refuse flipping the schedule under an in-flight
        // record rather than let the two ownership regimes overlap mid-flight.
        if reconfiguration_in_flight && !matches!(options.schedule, Unchanged) {
            return Err(AdapterError::AlterClusterScheduleWhileReconfiguring);
        }

        // Replication factor is one of the dimensions the cut-over sets
        // atomically from the record's target (`fold_reconfiguration_target`),
        // so a change applied independently while a reconfiguration is in
        // flight would be silently clobbered at cut-over. Refused even when the
        // same statement also re-targets the shape, so a record's target
        // replication factor is always the one it started with.
        if reconfiguration_in_flight && !matches!(options.replication_factor, Unchanged) {
            return Err(AdapterError::AlterClusterReplicationFactorWhileReconfiguring);
        }

        // A no-op `ALTER` short-circuits, except that an `ALTER` back to the
        // realized shape while a reconfiguration is in flight produces a
        // byte-identical `new_config` and is still meaningful: it must reach
        // the reshape path below to cancel the record.
        let cancels_or_retargets =
            reconfiguration_in_flight && alter_changes_replica_shape(options);
        if new_config == config && !cancels_or_retargets {
            return Ok(StageResult::Response(ExecuteResponse::AlteredObject(
                ObjectType::Cluster,
            )));
        }

        // An `ALTER` that raises a managed cluster's replication factor above
        // one deserves a notice when the cluster contains sources that run on
        // only one replica, since the additional replicas do not benefit those
        // sources. Computed here, emitted only after the alter succeeds. The
        // unmanaged conversion paths never change the replica count, so only
        // the managed-to-managed transition is of interest.
        let single_replica_sources_notice = match (&config.variant, &new_config.variant) {
            (Managed(old_managed), Managed(new_managed))
                if new_managed.replication_factor > old_managed.replication_factor
                    && new_managed.replication_factor > 1 =>
            {
                let sources = self.single_replica_source_names(cluster);
                (!sources.is_empty()).then(|| {
                    AdapterNotice::SingleReplicaSourcesOnMultiReplicaCluster {
                        cluster: cluster.name.clone(),
                        sources,
                    }
                })
            }
            _ => None,
        };

        // A shape-changing `ALTER` reshapes into a durable `reconfiguration`
        // record (starting, retargeting, or cancelling one) that the controller
        // converges on. Everything else falls through to the realized-config
        // update below without touching the record, in flight or not.
        //
        // With a record in flight the statement decides: an `ALTER` back to the
        // realized shape is value-identical yet must reach the reshape path to
        // cancel. With nothing in flight the values decide: a shape option set
        // to its current value reconfigures nothing, and reshaping it anyway
        // would write a spurious pre-cancelled record.
        if let (Managed(old_managed), Managed(new_managed)) = (&config.variant, &new_config.variant)
        {
            let needs_record = if reconfiguration_in_flight {
                alter_changes_replica_shape(options)
            } else {
                new_managed.replica_config_shape() != old_managed.replica_config_shape()
            };
            // A scheduled (non-MANUAL) cluster holds its replication factor
            // at 0 and the on-refresh strategy owns its replica set, so a
            // graceful hydrate-overlap has nothing meaningful to wait for.
            // A config-shape `ALTER` on such a cluster takes the direct
            // path below instead of writing a record: that path only updates
            // the realized config, and the controller reconciles any in-window
            // replica to the new shape on its next tick. The schedule guard
            // above keeps a schedule change from reaching here mid-record, so a
            // record on a scheduled cluster can only pre-date the schedule
            // (written on an older version). For that case the reshape
            // path stays reachable, so the record can still be retargeted
            // or cancelled until it settles.
            let scheduled_direct =
                !matches!(new_managed.schedule, mz_sql::plan::ClusterSchedule::Manual)
                    && !reconfiguration_in_flight;
            // A `WAIT` option would be silently vacuous on the direct
            // path: there may be no replica at all (window closed), and
            // an in-window replica is bounced to the new shape without a
            // hydrate-overlap to wait on. Reject it rather than return an
            // instant success that waited for nothing, mirroring the
            // planner's rejection of a `WAIT` without a shape change.
            if scheduled_direct && !matches!(strategy, AlterClusterPlanStrategy::None) {
                return Err(AdapterError::AlterClusterWaitOnScheduledCluster);
            }
            if needs_record && !scheduled_direct {
                let result = self
                    .reshape_alter_cluster_managed(
                        session,
                        cluster_id,
                        new_config.clone(),
                        options,
                        strategy,
                        validity,
                    )
                    .await;
                if result.is_ok() {
                    if let Some(notice) = single_replica_sources_notice {
                        session.add_notice(notice);
                    }
                }
                return result;
            }
        }

        match (&config.variant, &new_config.variant) {
            (Managed(_), Managed(_)) => {
                self.sequence_alter_cluster_managed_to_managed(
                    session,
                    cluster_id,
                    new_config.clone(),
                )
                .await?;
                if let Some(notice) = single_replica_sources_notice {
                    session.add_notice(notice);
                }
            }
            (Unmanaged, Managed(new_managed)) => {
                // The conversion path creates no overlap replicas to wait on,
                // and a scheduled target makes the `WAIT` permanently
                // meaningless, mirroring the managed-to-managed rejection
                // above.
                if !matches!(new_managed.schedule, mz_sql::plan::ClusterSchedule::Manual)
                    && !matches!(strategy, AlterClusterPlanStrategy::None)
                {
                    return Err(AdapterError::AlterClusterWaitOnScheduledCluster);
                }
                self.sequence_alter_cluster_unmanaged_to_managed(
                    session,
                    cluster_id,
                    new_config,
                    options.to_owned(),
                )
                .await?;
            }
            (Managed(_), Unmanaged) => {
                self.sequence_alter_cluster_managed_to_unmanaged(session, cluster_id, new_config)
                    .await?;
            }
            (Unmanaged, Unmanaged) => {
                self.sequence_alter_cluster_unmanaged_to_unmanaged(
                    session,
                    cluster_id,
                    new_config,
                    options.replicas.clone(),
                )
                .await?;
            }
        }

        Ok(StageResult::Response(ExecuteResponse::AlteredObject(
            ObjectType::Cluster,
        )))
    }

    /// Validates that a reconfiguration to `target` fits the resource budget.
    ///
    /// `cuts_over_on_first_tick` (see [`cuts_over_on_first_tick`]) says whether
    /// the realized and target replica sets ever coexist. Their configured
    /// baselines are summed when they overlap. Otherwise the model applies the
    /// baseline replacement as a signed delta to the live inventory.
    fn validate_reconfiguration_resource_limits(
        &self,
        cluster_id: ClusterId,
        target: &ReconfigurationTarget,
        prospective: &ClusterVariantManaged,
        cuts_over_on_first_tick: bool,
    ) -> Result<(), AdapterError> {
        // System clusters are exempt from `max_replicas_per_cluster` and from
        // credit accounting everywhere else (see the `is_user` guards in
        // `catalog_transact`'s validation), so a reconfiguration of one has no
        // budget to fit either.
        if !cluster_id.is_user() {
            return Ok(());
        }
        let cluster = self.catalog().get_cluster(cluster_id);
        let ClusterVariant::Managed(realized) = &cluster.config.variant else {
            return Ok(());
        };

        // An `ALTER` back to the realized shape cancels the reconfiguration and
        // materializes nothing new, so there is nothing to validate. The peak
        // model below would double count the realized set and spuriously reject
        // the cancel, exactly when the environment is at its limits and the
        // escape hatch matters most.
        if target.matches_realized_config(realized) {
            return Ok(());
        }

        // Both checks below model the transient peak this cluster contributes.
        // The controller normally runs the realized and target sets side by side
        // until cut-over, so the peak is both shapes at once, computed from
        // config as realized plus target. That slightly over-counts a same-shape
        // overlap, where existing replicas double as target replicas. We accept
        // the over-count: rejecting here is strictly better than the
        // asynchronous abort the controller falls back to when a limit shrinks or
        // the environment grows after the record is written.
        //
        // A record that cuts over on the controller's first tick never has the
        // two baseline sets coexist. Start from the live inventory, retain
        // everything outside the realized baseline, reuse target matches, and
        // charge only the remaining target creates minus realized retires. This
        // carries materialized strategy contributions. A warranted durable burst
        // reserves its slot even when its replica has not materialized yet.

        // Per-cluster replica count. Expressed as a signed increase, so
        // `validate_resource_limit`'s early return on a non-positive increase
        // covers both an rf-0 target and a first-tick cut-over that does not
        // grow the set.
        let first_tick_delta = if cuts_over_on_first_tick {
            let state = self
                .observe_cluster_state(cluster_id)
                .expect("managed cluster has an observable controller state");
            let hydration_size = prospective
                .auto_scaling_strategy
                .as_ref()
                .and_then(|strategy| strategy.on_hydration.as_ref())
                .map(|policy| policy.hydration_size.as_str());
            let preserve_burst = state.burst.as_ref().is_some_and(|burst| {
                burst_record_warranted(&burst.burst_size, target.replication_factor, hydration_size)
            });
            Some(first_tick_replica_delta(&state, target, preserve_burst))
        } else {
            None
        };
        let (current_replicas, replica_increase) = if let Some(delta) = &first_tick_delta {
            (
                cluster.user_replicas().count(),
                i64::from(delta.create_target) + i64::from(delta.create_burst.is_some())
                    - i64::from(delta.retire_realized)
                    - i64::from(delta.retire_burst.is_some()),
            )
        } else {
            (
                usize::cast_from(realized.replication_factor),
                i64::from(target.replication_factor),
            )
        };
        self.validate_resource_limit(
            current_replicas,
            replica_increase,
            SystemVars::max_replicas_per_cluster,
            "cluster replica",
            MAX_REPLICAS_PER_CLUSTER.name(),
        )?;

        // Global credit rate.
        self.validate_reconfiguration_credit_peak(
            cluster_id,
            realized,
            target,
            first_tick_delta.as_ref(),
        )?;

        Ok(())
    }

    /// Validates that the transient credit-rate peak of a reconfiguration fits
    /// the environment-wide budget.
    ///
    /// The peak is the realized plus the target shape. For a first-tick cut-over
    /// it is the live environment rate plus the target-create credits minus the
    /// realized-retire credits.
    ///
    /// The base is the live consumption of every other cluster. It excludes
    /// this cluster's own replicas so a re-target of an in-flight record does
    /// not additionally count an already-materialized overlap on top of the
    /// modeled peak. The first-tick model starts from the full live rate instead.
    fn validate_reconfiguration_credit_peak(
        &self,
        cluster_id: ClusterId,
        realized: &ClusterVariantManaged,
        target: &ReconfigurationTarget,
        first_tick_delta: Option<&FirstTickReplicaDelta>,
    ) -> Result<(), AdapterError> {
        let shape_credit = |size: &str, replication_factor: u32| -> Numeric {
            let per_replica = self
                .catalog()
                .cluster_replica_sizes()
                .0
                .get(size)
                .map(|allocation| allocation.credits_per_hour)
                // Sizes are validated by `ensure_valid_replica_size` before we get
                // here, so an unknown size contributes nothing rather than panics.
                .unwrap_or_else(Numeric::zero);
            per_replica * Numeric::from(replication_factor)
        };
        let (current_credit, credit_increase) = if let Some(delta) = first_tick_delta {
            let burst_credit = delta
                .create_burst
                .as_deref()
                .map_or_else(Numeric::zero, |size| shape_credit(size, 1));
            let retired_burst_credit = delta
                .retire_burst
                .as_deref()
                .map_or_else(Numeric::zero, |size| shape_credit(size, 1));
            (
                self.current_credit_consumption_rate(None),
                shape_credit(&target.size, delta.create_target) + burst_credit
                    - shape_credit(&realized.size, delta.retire_realized)
                    - retired_burst_credit,
            )
        } else {
            (
                self.current_credit_consumption_rate(Some(cluster_id)),
                shape_credit(&target.size, target.replication_factor)
                    + shape_credit(&realized.size, realized.replication_factor),
            )
        };
        self.validate_resource_limit_numeric(
            current_credit,
            credit_increase,
            |system_vars| {
                self.license_key
                    .max_credit_consumption_rate()
                    .map_or_else(|| system_vars.max_credit_consumption_rate(), Numeric::from)
            },
            "cluster replica",
            MAX_CREDIT_CONSUMPTION_RATE.name(),
        )?;

        Ok(())
    }

    /// Reshape a managed→managed `ALTER` into a durable `reconfiguration` record.
    ///
    /// Writes (or folds into) the `reconfiguration` record carrying the full target
    /// config shape and a deadline, while leaving the realized *shape* in place.
    /// Non-shape fields the `ALTER` changed (`workload_class`, `schedule`,
    /// `auto_scaling_strategy`, ...) need no hydrate-overlap, so they are applied
    /// to the realized config immediately. The controller converges the replica
    /// set onto the target and cuts the realized shape over at hydration.
    ///
    /// The target is folded onto any in-flight one by
    /// [`alter_reconfiguration_target`].
    ///
    /// **Timeout action.** The record carries an `on_timeout` action (resolved
    /// from `WITH (WAIT ...)`, defaulting to `ROLLBACK`), which the controller
    /// applies at the deadline only if the target has not hydrated: `ROLLBACK`
    /// marks the record timed out and drops the in-flight target set, leaving the
    /// realized config untouched, so the cluster reverts to its
    /// pre-reconfiguration shape and the strategy disengages. `COMMIT` cuts the
    /// realized config over to the not-fully-hydrated target and marks the record
    /// finalized. Success always takes precedence. A target that hydrates before the deadline cuts over regardless
    /// of the action.
    ///
    /// With `enable_background_alter_cluster` on, the statement returns
    /// immediately. With it off, the session blocks on a wait-shim
    /// ([`ClusterStage::AwaitReconfiguration`]) that polls until the controller
    /// resolves the record, reporting success only if the realized config
    /// reached the target, preserving today's foreground UX over the same
    /// durable mechanism.
    async fn reshape_alter_cluster_managed(
        &mut self,
        session: &Session,
        cluster_id: ClusterId,
        new_config: ClusterConfig,
        options: &PlanClusterOption,
        strategy: &AlterClusterPlanStrategy,
        validity: PlanValidity,
    ) -> Result<StageResult<Box<ClusterStage>>, AdapterError> {
        let ClusterVariant::Managed(new_managed) = &new_config.variant else {
            return Err(AdapterError::Internal(
                "reshape_alter_cluster_managed requires a managed target config".into(),
            ));
        };

        let cluster = self.catalog.get_cluster(cluster_id);
        let in_flight = match &cluster.config.variant {
            ClusterVariant::Managed(managed) => managed
                .reconfiguration
                .as_ref()
                .filter(|record| record.is_in_progress())
                .cloned(),
            ClusterVariant::Unmanaged => None,
        };
        let target = alter_reconfiguration_target(
            new_managed,
            options,
            in_flight.as_ref().map(|r| &r.target),
        );

        // Validate the target up front, so a bad reshape errors at `ALTER` time
        // rather than silently parking an unconvergeable record.
        let role_id = session.role_metadata().current_role;
        self.catalog.ensure_valid_replica_size(
            &self
                .catalog()
                .get_role_allowed_cluster_sizes(&Some(role_id)),
            &target.size,
            false,
        )?;
        self.ensure_valid_azs(target.availability_zones.iter())?;
        // Validate the reconfiguration's resource footprint up front, so a
        // reshape that cannot fit errors at `ALTER` time rather than writing a
        // record the controller aborts asynchronously.
        self.validate_reconfiguration_resource_limits(
            cluster_id,
            &target,
            new_managed,
            cuts_over_on_first_tick(strategy),
        )?;

        // Resolve the deadline and the on-timeout action, both written relative
        // to the current time so they survive session disconnect and restart.
        // The target folds per-dimension onto the in-flight one. The deadline
        // and `on_timeout`, in contrast, are the contract carried by a `WAIT`
        // clause, so how a folding `ALTER` treats them depends on whether it
        // carries one:
        //   - no `WAIT`, reconfiguration in flight -> keep the in-flight
        //                          record's deadline and `on_timeout`. The
        //                          statement carries no contract of its own, so
        //                          an unrelated config-shape `ALTER` must not
        //                          silently reset the deadline and action the
        //                          user set on the reconfiguration in progress.
        //   - no `WAIT`, nothing in flight -> the system-default timeout and the
        //                          implicit `on_timeout` default (`ROLLBACK`).
        //   - `WAIT FOR`        -> sugar for `ON TIMEOUT COMMIT` (cut over at the
        //                          deadline regardless of hydration).
        //   - `WAIT UNTIL READY -> the explicit `TIMEOUT` / `ON TIMEOUT`, with
        //                          `ON TIMEOUT` defaulting to `ROLLBACK` when
        //                          omitted.
        // An explicit `WAIT` clause is folded onto an in-flight record wholesale,
        // which lets a later `ALTER` steer the deadline and timeout action of a
        // reconfiguration in progress without discarding the hydration progress
        // its target may already have. `ROLLBACK` (the default) reverts an
        // un-hydrated reconfiguration to its pre-reconfiguration shape rather
        // than cutting over to a not-yet-hydrated target, which could induce
        // downtime.
        let now = self.now();
        let deadline_from = |timeout: Duration| -> Timestamp {
            now.saturating_add(u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX))
                .into()
        };
        let (deadline, on_timeout) = match strategy {
            AlterClusterPlanStrategy::None => match &in_flight {
                Some(record) => (record.deadline, record.on_timeout),
                None => (
                    deadline_from(
                        DEFAULT_CLUSTER_RECONFIGURATION_TIMEOUT
                            .get(self.catalog().system_config().dyncfgs()),
                    ),
                    OnTimeoutAction::Rollback,
                ),
            },
            AlterClusterPlanStrategy::For(timeout) => {
                (deadline_from(*timeout), OnTimeoutAction::Commit)
            }
            AlterClusterPlanStrategy::UntilReady {
                timeout,
                on_timeout,
            } => (
                deadline_from(*timeout),
                on_timeout.unwrap_or(OnTimeoutAction::Rollback),
            ),
        };

        // Build the durable write from `new_config`, which carries every field the
        // `ALTER` changed, then reset the config *shape* (every
        // `ReconfigurationTarget` dimension) back to the realized values: that
        // transition is deferred to the `reconfiguration` record and applied at
        // cut-over. This applies non-shape changes (`workload_class`, `schedule`,
        // `auto_scaling_strategy`, ...) immediately rather than silently dropping
        // them. Any existing record is folded over by the `record` we just built.
        let cluster = self.catalog.get_cluster(cluster_id);
        let cluster_name = cluster.name().to_string();
        let ClusterVariant::Managed(realized_now) = &cluster.config.variant else {
            return Err(AdapterError::Internal(
                "reshape_alter_cluster_managed requires a managed realized config".into(),
            ));
        };
        let realized_target = realized_now.realized_reconfiguration_target();
        // The status and the audit intent are two views of the same decision,
        // made together here: an ALTER back to the realized shape is a cancel,
        // anything else starts (or re-targets) a reconfiguration.
        let (status, audit) = if target.matches_realized_config(realized_now) {
            (
                ReconfigurationStatus::Cancelled,
                ReconfigurationAudit::Cancelled,
            )
        } else {
            (
                ReconfigurationStatus::InProgress,
                ReconfigurationAudit::Started,
            )
        };
        let record = ReconfigurationState {
            target: target.clone(),
            deadline,
            on_timeout,
            status,
        };

        let mut realized = new_config.clone();
        let ClusterVariant::Managed(realized_managed) = &mut realized.variant else {
            return Err(AdapterError::Internal(
                "reshape_alter_cluster_managed requires a managed target config".into(),
            ));
        };
        realized_managed.apply_reconfiguration_target(realized_target);
        realized_managed.reconfiguration = Some(record);

        self.catalog_transact(
            Some(session),
            vec![Op::UpdateClusterConfig {
                id: cluster_id,
                name: cluster_name,
                config: realized,
                reconfiguration_audit: Some(audit),
                burst_audit: None,
            }],
        )
        .await?;

        let background =
            ENABLE_BACKGROUND_ALTER_CLUSTER.get(self.catalog().system_config().dyncfgs());
        if background {
            return Ok(StageResult::Response(ExecuteResponse::AlteredObject(
                ObjectType::Cluster,
            )));
        }

        // Foreground wait-shim: poll the durable record until it resolves. The
        // reconfiguration continues in the background regardless of the session. A
        // disconnect during the wait only stops waiting.
        Ok(StageResult::Immediate(Box::new(
            ClusterStage::AwaitReconfiguration(AlterClusterAwaitReconfiguration {
                validity,
                cluster_id,
                target,
            }),
        )))
    }

    /// Polls the durable `reconfiguration` record for the foreground wait-shim.
    ///
    /// The controller owns deadline handling. This stage reports success only once
    /// the realized config reaches `target`, and otherwise keeps polling while
    /// the record is in progress.
    fn await_reconfiguration_stage(
        &self,
        validity: PlanValidity,
        cluster_id: ClusterId,
        target: ReconfigurationTarget,
    ) -> Result<StageResult<Box<ClusterStage>>, AdapterError> {
        let Some(cluster) = self.catalog().try_get_cluster(cluster_id) else {
            // The cluster was dropped out from under the reconfiguration.
            // There is nothing to wait on.
            return Ok(StageResult::Response(ExecuteResponse::AlteredObject(
                ObjectType::Cluster,
            )));
        };
        let record = match &cluster.config.variant {
            ClusterVariant::Managed(managed) => managed.reconfiguration.clone(),
            ClusterVariant::Unmanaged => None,
        };

        let realized_matches_target = match &cluster.config.variant {
            ClusterVariant::Managed(managed) => target.matches_realized_config(managed),
            ClusterVariant::Unmanaged => false,
        };

        match record {
            None => {
                // Defensive fallback for old or manually-edited catalogs. New
                // controller writes retain a terminal record.
                if realized_matches_target {
                    Ok(StageResult::Response(ExecuteResponse::AlteredObject(
                        ObjectType::Cluster,
                    )))
                } else {
                    Err(AdapterError::AlterClusterTimeout)
                }
            }
            Some(record) if !record.is_in_progress() => {
                if matches!(
                    record.status,
                    ReconfigurationStatus::Finalized | ReconfigurationStatus::Cancelled
                ) && realized_matches_target
                {
                    Ok(StageResult::Response(ExecuteResponse::AlteredObject(
                        ObjectType::Cluster,
                    )))
                } else {
                    Err(AdapterError::AlterClusterTimeout)
                }
            }
            Some(_) => {
                // Still in progress. Re-poll after the configured interval and
                // wait for the controller to resolve the record. We deliberately
                // do not consult the deadline here: erroring while the record is
                // in progress can race the controller and misreport an `ON
                // TIMEOUT COMMIT` cut-over as a timeout.
                //
                // NOTE: If the controller stops resolving a record while it is
                // in progress, the shim waits indefinitely. Cancelling the session
                // only stops waiting. It does not abort the durable reconfiguration.
                let poll_duration = self
                    .catalog
                    .system_config()
                    .cluster_alter_check_ready_interval();
                let span = Span::current();
                Ok(StageResult::Handle(mz_ore::task::spawn(
                    || "Await Cluster Reconfiguration",
                    async move {
                        tokio::time::sleep(poll_duration).await;
                        Ok(Box::new(ClusterStage::AwaitReconfiguration(
                            AlterClusterAwaitReconfiguration {
                                validity,
                                cluster_id,
                                target,
                            },
                        )))
                    }
                    .instrument(span),
                )))
            }
        }
    }

    #[mz_ore::instrument(level = "debug")]
    pub(crate) async fn sequence_create_cluster(
        &mut self,
        session: &Session,
        CreateClusterPlan {
            name,
            variant,
            workload_class,
            if_not_exists,
        }: CreateClusterPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        tracing::debug!("sequence_create_cluster");

        let id_ts = self.get_catalog_write_ts().await;
        let id = self.catalog().allocate_user_cluster_id(id_ts).await?;
        // The catalog items for the introspection sources are shared between all replicas
        // of a compute instance, so we create them unconditionally during instance creation.
        // Whether a replica actually maintains introspection arrangements is determined by the
        // per-replica introspection configuration.
        let introspection_sources = BUILTINS::logs().collect();
        let cluster_variant = match &variant {
            CreateClusterVariant::Managed(plan) => {
                let logging = if let Some(config) = plan.compute.introspection {
                    ReplicaLogging {
                        log_logging: config.debugging,
                        interval: Some(config.interval),
                    }
                } else {
                    ReplicaLogging::default()
                };
                ClusterVariant::Managed(ClusterVariantManaged {
                    size: plan.size.clone(),
                    availability_zones: plan.availability_zones.clone(),
                    logging,
                    arrangement_compression: plan.compute.arrangement_compression,
                    replication_factor: plan.replication_factor,
                    optimizer_feature_overrides: plan.optimizer_feature_overrides.clone(),
                    schedule: plan.schedule.clone(),
                    auto_scaling_strategy: plan.auto_scaling_strategy.clone(),
                    reconfiguration: None,
                    burst: None,
                })
            }
            CreateClusterVariant::Unmanaged(_) => ClusterVariant::Unmanaged,
        };
        let config = ClusterConfig {
            variant: cluster_variant,
            workload_class,
        };
        let ops = vec![catalog::Op::CreateCluster {
            id,
            name: name.clone(),
            introspection_sources,
            owner_id: *session.current_role_id(),
            config,
        }];

        match variant {
            CreateClusterVariant::Managed(plan) => {
                self.sequence_create_managed_cluster(session, plan, id, ops)
                    .await
            }
            CreateClusterVariant::Unmanaged(plan) => {
                self.sequence_create_unmanaged_cluster(session, plan, id, ops)
                    .await
            }
        }
        .or_else(|err| match err {
            AdapterError::Catalog(mz_catalog::memory::error::Error {
                kind: ErrorKind::Sql(CatalogError::ClusterAlreadyExists(_)),
            }) if if_not_exists => {
                session.add_notice(AdapterNotice::ObjectAlreadyExists {
                    name,
                    ty: "cluster",
                });
                Ok(ExecuteResponse::CreatedCluster)
            }
            err => Err(err),
        })
    }

    #[mz_ore::instrument(level = "debug")]
    async fn sequence_create_managed_cluster(
        &mut self,
        session: &Session,
        CreateClusterManagedPlan {
            availability_zones,
            compute,
            replication_factor,
            size,
            optimizer_feature_overrides: _,
            schedule: _,
            auto_scaling_strategy,
        }: CreateClusterManagedPlan,
        cluster_id: ClusterId,
        mut ops: Vec<catalog::Op>,
    ) -> Result<ExecuteResponse, AdapterError> {
        tracing::debug!("sequence_create_managed_cluster");

        self.ensure_valid_azs(availability_zones.iter())?;

        // The shape every replica below is created at, matching the cluster's
        // own config (see `sequence_create_cluster`) so the controller
        // reconciles the replicas as already conforming.
        let replica_shape = ReplicaShape {
            size: size.clone(),
            availability_zones: AvailabilityZones(availability_zones.clone()),
            logging: match compute.introspection {
                Some(config) => ReplicaLogging {
                    log_logging: config.debugging,
                    interval: Some(config.interval),
                },
                None => ReplicaLogging::default(),
            },
            arrangement_compression: compute.arrangement_compression,
        };

        let role_id = session.role_metadata().current_role;
        self.catalog.ensure_valid_replica_size(
            &self
                .catalog()
                .get_role_allowed_cluster_sizes(&Some(role_id)),
            &size,
            false,
        )?;
        // A HYDRATION SIZE is validated like SIZE itself: it must name a real
        // replica size the session role may use. Without this, a typo would
        // fail invisibly at burst-arm time (the controller retrying every
        // tick), and a size-restricted role could burst at a size it may not
        // CREATE with.
        if let Some(on_hydration) = auto_scaling_strategy
            .as_ref()
            .and_then(|strategy| strategy.on_hydration.as_ref())
        {
            self.catalog.ensure_valid_replica_size(
                &self
                    .catalog()
                    .get_role_allowed_cluster_sizes(&Some(role_id)),
                &on_hydration.hydration_size,
                false,
            )?;
        }

        // Eagerly validate the `max_replicas_per_cluster` limit.
        // `catalog_transact` will do this validation too, but allocating
        // replica IDs is expensive enough that we need to do this validation
        // before allocating replica IDs. See database-issues#6046.
        if cluster_id.is_user() {
            self.validate_resource_limit(
                0,
                i64::from(replication_factor),
                SystemVars::max_replicas_per_cluster,
                "cluster replica",
                MAX_REPLICAS_PER_CLUSTER.name(),
            )?;
        }

        // Pre-allocate replica ids out-of-band via the durable allocator,
        // picking the id type from the owning cluster. This mirrors how cluster
        // and item ids are allocated, so nothing allocates a replica id in-apply.
        let id_ts = self.get_catalog_write_ts().await;
        let replica_ids = self
            .catalog()
            .allocate_replica_ids(cluster_id, u64::from(replication_factor), id_ts)
            .await?;

        for (replica_id, replica_name) in replica_ids
            .into_iter()
            .zip_eq((0..replication_factor).map(managed_cluster_replica_name))
        {
            self.create_managed_cluster_replica_op(
                cluster_id,
                replica_id,
                replica_name,
                &replica_shape,
                &mut ops,
                *session.current_role_id(),
                ReplicaCreateDropReason::Manual,
            )?;
        }

        self.catalog_transact(Some(session), ops).await?;

        Ok(ExecuteResponse::CreatedCluster)
    }

    /// Pushes an [`Op::CreateClusterReplica`] for a managed replica of `shape`.
    ///
    /// Takes the [`ReplicaShape`] the controller reconciles against rather than
    /// a planned `ComputeReplicaConfig`, so the replica the op creates and the
    /// cluster config that called for it cannot disagree. The planned form
    /// cannot represent `INTROSPECTION DEBUGGING` without an interval, which
    /// `ALTER CLUSTER` can durably write.
    fn create_managed_cluster_replica_op(
        &self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        name: String,
        shape: &ReplicaShape,
        ops: &mut Vec<Op>,
        owner_id: RoleId,
        reason: ReplicaCreateDropReason,
    ) -> Result<(), AdapterError> {
        let location = mz_catalog::durable::ReplicaLocation::Managed {
            // Concretized below from the cluster config; this intermediate value
            // is discarded, so the list is left empty here.
            availability_zones: Vec::new(),
            billed_as: None,
            internal: false,
            size: shape.size.clone(),
            pending: false,
        };

        // An empty pool is "no restriction", not "restricted to nothing".
        let azs: Option<&[String]> = if shape.availability_zones.0.is_empty() {
            None
        } else {
            Some(&shape.availability_zones.0)
        };
        let config = ReplicaConfig {
            location: self.catalog().concretize_replica_location(
                location,
                &self
                    .catalog()
                    .get_role_allowed_cluster_sizes(&Some(owner_id)),
                azs,
                false,
            )?,
            compute: ComputeReplicaConfig {
                logging: shape.logging.clone(),
                arrangement_compression: shape.arrangement_compression,
            },
        };

        // The caller pre-allocates `replica_id` out-of-band via the durable
        // allocator, so nothing allocates a replica id in-apply.
        ops.push(catalog::Op::CreateClusterReplica {
            cluster_id,
            replica_id,
            name,
            config,
            owner_id,
            reason,
        });
        Ok(())
    }

    fn ensure_valid_azs<'a, I: IntoIterator<Item = &'a String>>(
        &self,
        azs: I,
    ) -> Result<(), AdapterError> {
        let cat_azs = self.catalog().state().availability_zones();
        for az in azs.into_iter() {
            if !cat_azs.contains(az) {
                return Err(AdapterError::InvalidClusterReplicaAz {
                    az: az.to_string(),
                    expected: cat_azs.to_vec(),
                });
            }
        }
        Ok(())
    }

    #[mz_ore::instrument(level = "debug")]
    async fn sequence_create_unmanaged_cluster(
        &mut self,
        session: &Session,
        CreateClusterUnmanagedPlan { replicas }: CreateClusterUnmanagedPlan,
        id: ClusterId,
        mut ops: Vec<catalog::Op>,
    ) -> Result<ExecuteResponse, AdapterError> {
        tracing::debug!("sequence_create_unmanaged_cluster");

        self.ensure_valid_azs(replicas.iter().filter_map(|(_, r)| {
            if let mz_sql::plan::ReplicaConfig::Orchestrated {
                availability_zone: Some(az),
                ..
            } = &r
            {
                Some(az)
            } else {
                None
            }
        }))?;

        // Eagerly validate the `max_replicas_per_cluster` limit.
        // `catalog_transact` will do this validation too, but allocating
        // replica IDs is expensive enough that we need to do this validation
        // before allocating replica IDs. See database-issues#6046.
        if id.is_user() {
            self.validate_resource_limit(
                0,
                i64::try_from(replicas.len()).unwrap_or(i64::MAX),
                SystemVars::max_replicas_per_cluster,
                "cluster replica",
                MAX_REPLICAS_PER_CLUSTER.name(),
            )?;
        }

        // Pre-allocate replica ids out-of-band via the durable allocator,
        // picking the id type from the owning cluster. This mirrors how cluster
        // and item ids are allocated, so nothing allocates a replica id in-apply.
        let id_ts = self.get_catalog_write_ts().await;
        let replica_ids = self
            .catalog()
            .allocate_replica_ids(id, u64::cast_from(replicas.len()), id_ts)
            .await?;

        for (replica_id, (replica_name, replica_config)) in replica_ids.into_iter().zip_eq(replicas)
        {
            // If the AZ was not specified, choose one, round-robin, from the ones with
            // the lowest number of configured replicas for this cluster.
            let (compute, location) = match replica_config {
                mz_sql::plan::ReplicaConfig::Unorchestrated {
                    storagectl_addrs,
                    computectl_addrs,
                    compute,
                } => {
                    let location = mz_catalog::durable::ReplicaLocation::Unmanaged {
                        storagectl_addrs,
                        computectl_addrs,
                    };
                    (compute, location)
                }
                mz_sql::plan::ReplicaConfig::Orchestrated {
                    availability_zone,
                    billed_as,
                    compute,
                    internal,
                    size,
                } => {
                    // Only internal users have access to INTERNAL and BILLED AS
                    if !session.user().is_internal() && (internal || billed_as.is_some()) {
                        coord_bail!("cannot specify INTERNAL or BILLED AS as non-internal user")
                    }
                    // BILLED AS implies the INTERNAL flag.
                    if billed_as.is_some() && !internal {
                        coord_bail!("must specify INTERNAL when specifying BILLED AS");
                    }

                    let location = mz_catalog::durable::ReplicaLocation::Managed {
                        // The user-pinned `AVAILABILITY ZONE`, if any, as a zero-
                        // or one-element list.
                        availability_zones: availability_zone.into_iter().collect(),
                        billed_as,
                        internal,
                        size: size.clone(),
                        pending: false,
                    };
                    (compute, location)
                }
            };

            let logging = if let Some(config) = compute.introspection {
                ReplicaLogging {
                    log_logging: config.debugging,
                    interval: Some(config.interval),
                }
            } else {
                ReplicaLogging::default()
            };

            let role_id = session.role_metadata().current_role;
            let config = ReplicaConfig {
                location: self.catalog().concretize_replica_location(
                    location,
                    &self
                        .catalog()
                        .get_role_allowed_cluster_sizes(&Some(role_id)),
                    None,
                    false,
                )?,
                compute: ComputeReplicaConfig {
                    logging,
                    arrangement_compression: compute.arrangement_compression,
                },
            };

            ops.push(catalog::Op::CreateClusterReplica {
                cluster_id: id,
                replica_id,
                name: replica_name,
                config,
                owner_id: *session.current_role_id(),
                reason: ReplicaCreateDropReason::Manual,
            });
        }

        self.catalog_transact(Some(session), ops).await?;

        Ok(ExecuteResponse::CreatedCluster)
    }

    /// Returns the full names of all sources bound to `cluster` whose
    /// connections prefer to run on a single replica, so additional replicas
    /// do not make them more fault tolerant or increase their throughput.
    fn single_replica_source_names(&self, cluster: &Cluster) -> Vec<String> {
        cluster
            .bound_objects
            .iter()
            .filter_map(|id| {
                let entry = self.catalog().get_entry(id);
                let single_replica =
                    entry
                        .source()
                        .is_some_and(|source| match &source.data_source {
                            DataSourceDesc::Ingestion { desc, .. }
                            | DataSourceDesc::OldSyntaxIngestion { desc, .. } => {
                                desc.connection.prefers_single_replica()
                            }
                            _ => false,
                        });
                single_replica.then(|| {
                    self.catalog()
                        .resolve_full_name(entry.name(), None)
                        .to_string()
                })
            })
            .collect()
    }

    /// The number of replicas `cluster` aims to run, for deciding whether to
    /// emit the single-replica-sources notice.
    ///
    /// For a managed cluster this is the replication factor, taking the target
    /// of an in-progress reconfiguration over the realized one, plus any
    /// INTERNAL or BILLED AS replicas, which are manually managed outside the
    /// replication-factor domain. Replicas belonging to a reconfiguration's
    /// hydrate-overlap are deliberately not counted: they replace the serving
    /// set at cut-over rather than adding to it. Counting the replication
    /// factor instead of replicas excludes the ordinary replicas the cluster
    /// controller creates for the target shape.
    fn notice_relevant_replica_count(&self, cluster: &Cluster) -> usize {
        match &cluster.config.variant {
            ClusterVariant::Managed(managed) => {
                let replication_factor = managed
                    .reconfiguration
                    .as_ref()
                    .filter(|record| record.is_in_progress())
                    .map_or(managed.replication_factor, |record| {
                        record.target.replication_factor
                    });
                let manual_replicas = cluster
                    .replicas()
                    .filter(|r| {
                        r.config.location.internal() || r.config.location.billed_as().is_some()
                    })
                    .count();
                usize::cast_from(replication_factor) + manual_replicas
            }
            ClusterVariant::Unmanaged => cluster.replicas().count(),
        }
    }

    /// Emits a notice if `cluster` aims to run more than one replica while
    /// containing sources that run on only one replica. Call after a command
    /// that added a replica or such a source.
    ///
    /// `creating_source` names a source the current command is creating in
    /// `cluster`. It is included in the notice even when it is not yet visible
    /// in the catalog, which happens when the creation is staged in a DDL
    /// transaction that commits later.
    pub(crate) fn notify_single_replica_sources(
        &self,
        session: &Session,
        cluster: &Cluster,
        creating_source: Option<&QualifiedItemName>,
    ) {
        if self.notice_relevant_replica_count(cluster) <= 1 {
            return;
        }
        let mut sources = self.single_replica_source_names(cluster);
        if let Some(name) = creating_source {
            let full_name = self.catalog().resolve_full_name(name, None).to_string();
            if !sources.contains(&full_name) {
                sources.push(full_name);
            }
        }
        if !sources.is_empty() {
            session.add_notice(AdapterNotice::SingleReplicaSourcesOnMultiReplicaCluster {
                cluster: cluster.name.clone(),
                sources,
            });
        }
    }

    #[mz_ore::instrument(level = "debug")]
    pub(crate) async fn sequence_create_cluster_replica(
        &mut self,
        session: &Session,
        CreateClusterReplicaPlan {
            name,
            cluster_id,
            config,
            if_not_exists,
        }: CreateClusterReplicaPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        // Choose default AZ if necessary
        let (compute, location) = match config {
            mz_sql::plan::ReplicaConfig::Unorchestrated {
                storagectl_addrs,
                computectl_addrs,
                compute,
            } => {
                let location = mz_catalog::durable::ReplicaLocation::Unmanaged {
                    storagectl_addrs,
                    computectl_addrs,
                };
                (compute, location)
            }
            mz_sql::plan::ReplicaConfig::Orchestrated {
                availability_zone,
                billed_as,
                compute,
                internal,
                size,
            } => {
                let availability_zone = match availability_zone {
                    Some(az) => {
                        self.ensure_valid_azs([&az])?;
                        Some(az)
                    }
                    None => None,
                };
                let location = mz_catalog::durable::ReplicaLocation::Managed {
                    // The user-pinned `AVAILABILITY ZONE`, if any, as a zero- or
                    // one-element list.
                    availability_zones: availability_zone.into_iter().collect(),
                    billed_as,
                    internal,
                    size,
                    pending: false,
                };
                (compute, location)
            }
        };

        let logging = if let Some(config) = compute.introspection {
            ReplicaLogging {
                log_logging: config.debugging,
                interval: Some(config.interval),
            }
        } else {
            ReplicaLogging::default()
        };

        let role_id = session.role_metadata().current_role;
        let config = ReplicaConfig {
            location: self.catalog().concretize_replica_location(
                location,
                &self
                    .catalog()
                    .get_role_allowed_cluster_sizes(&Some(role_id)),
                // Planning ensures all replicas in this codepath
                // are unmanaged.
                None,
                false,
            )?,
            compute: ComputeReplicaConfig {
                logging,
                arrangement_compression: compute.arrangement_compression,
            },
        };

        let cluster = self.catalog().get_cluster(cluster_id);

        if let ReplicaLocation::Managed(ManagedReplicaLocation {
            internal,
            billed_as,
            ..
        }) = &config.location
        {
            // Only internal users have access to INTERNAL and BILLED AS
            if !session.user().is_internal() && (*internal || billed_as.is_some()) {
                coord_bail!("cannot specify INTERNAL or BILLED AS as non-internal user")
            }
            // Managed clusters require the INTERNAL flag.
            if cluster.is_managed() && !*internal {
                coord_bail!("must specify INTERNAL when creating a replica in a managed cluster");
            }
            // BILLED AS implies the INTERNAL flag.
            if billed_as.is_some() && !*internal {
                coord_bail!("must specify INTERNAL when specifying BILLED AS");
            }
        }

        // Replicas have the same owner as their cluster. Extract the owned
        // cluster info we need before the borrow is dropped for the awaits below.
        let owner_id = cluster.owner_id();

        let cluster_name = cluster.name.clone();
        // A replica name is only unique within its cluster, so the notice on the
        // `IF NOT EXISTS` path below has to name both.
        let qualified_name = format!("{cluster_name}.{name}");

        // Pre-allocate the replica id out-of-band via the durable allocator,
        // picking the id type from the target cluster, which may be a system
        // cluster. This mirrors how cluster and item ids are allocated, so
        // nothing allocates a replica id in-apply.
        let id_ts = self.get_catalog_write_ts().await;
        let replica_id = self
            .catalog()
            .allocate_replica_ids(cluster_id, 1, id_ts)
            .await?
            .into_element();

        let ops = vec![catalog::Op::CreateClusterReplica {
            cluster_id,
            replica_id,
            name: name.clone(),
            config,
            owner_id,
            reason: ReplicaCreateDropReason::Manual,
        }];

        match self.catalog_transact(Some(session), ops).await {
            Ok(()) => {
                // The commit made the new replica visible in the catalog, so
                // the check sees the updated replica count.
                self.notify_single_replica_sources(
                    session,
                    self.catalog().get_cluster(cluster_id),
                    None,
                );
                Ok(ExecuteResponse::CreatedClusterReplica)
            }
            Err(AdapterError::Catalog(mz_catalog::memory::error::Error {
                kind: ErrorKind::Sql(CatalogError::DuplicateReplica(_, _)),
            })) if if_not_exists => {
                session.add_notice(AdapterNotice::ObjectAlreadyExists {
                    name: qualified_name,
                    ty: "cluster replica",
                });
                Ok(ExecuteResponse::CreatedClusterReplica)
            }
            Err(err) => Err(err),
        }
    }

    /// Applies a managed→managed `ALTER CLUSTER`.
    ///
    /// This is a config-only write: the cluster controller owns the replica set
    /// and reconciles it to the new realized config on its next tick. Emitting
    /// creates and drops here as well would fight it, since it derives replica
    /// names from the observed set, so an adapter create by canonical `rN` can
    /// collide with a controller-chosen name and an adapter drop by canonical
    /// `rN` can miss a churned one.
    ///
    /// # Panics
    ///
    /// Panics if the identified cluster is not a managed cluster.
    /// Panics if `new_config` is not a configuration for a managed cluster.
    pub(crate) async fn sequence_alter_cluster_managed_to_managed(
        &mut self,
        session: &Session,
        cluster_id: ClusterId,
        new_config: ClusterConfig,
    ) -> Result<(), AdapterError> {
        let cluster = self.catalog.get_cluster(cluster_id);
        let name = cluster.name().to_string();

        let ClusterVariant::Managed(ClusterVariantManaged {
            size,
            availability_zones,
            logging,
            arrangement_compression,
            replication_factor,
            optimizer_feature_overrides: _,
            schedule: _,
            auto_scaling_strategy,
            reconfiguration,
            burst: _,
        }) = &cluster.config.variant
        else {
            panic!("expected existing managed cluster config");
        };
        let ClusterVariant::Managed(new_managed) = &new_config.variant else {
            panic!("expected new managed cluster config");
        };
        let ClusterVariantManaged {
            size: new_size,
            replication_factor: new_replication_factor,
            availability_zones: new_availability_zones,
            logging: _,
            arrangement_compression: _,
            optimizer_feature_overrides: _,
            schedule: _,
            auto_scaling_strategy: new_auto_scaling_strategy,
            reconfiguration: _,
            burst: _,
        } = new_managed;

        let role_id = Some(session.role_metadata().current_role);
        self.catalog.ensure_valid_replica_size(
            &self.catalog().get_role_allowed_cluster_sizes(&role_id),
            new_size,
            false,
        )?;
        // A newly set (or changed) AUTO SCALING STRATEGY gets its HYDRATION
        // SIZE validated like SIZE itself: it must name a real replica size the
        // session role may use. Only a changed strategy is checked, so an
        // existing policy does not block unrelated ALTERs if the size
        // allow-list later shrinks (matching how SIZE itself behaves).
        if new_auto_scaling_strategy != auto_scaling_strategy {
            if let Some(on_hydration) = new_auto_scaling_strategy
                .as_ref()
                .and_then(|strategy| strategy.on_hydration.as_ref())
            {
                self.catalog.ensure_valid_replica_size(
                    &self.catalog().get_role_allowed_cluster_sizes(&role_id),
                    &on_hydration.hydration_size,
                    false,
                )?;
                // The planner validated the hydration size against the
                // *realized* SIZE only. An in-flight reconfiguration will cut
                // the realized SIZE over to its target, so also reject equality
                // with that target. Letting it through would end the reshape
                // with a no-op burst shape and a stored statement that fails
                // its own re-plan.
                if reconfiguration.as_ref().is_some_and(|record| {
                    record.is_in_progress() && record.target.size == on_hydration.hydration_size
                }) {
                    coord_bail!(
                        "HYDRATION SIZE must differ from the target SIZE \
                         ('{}') of the in-progress cluster resize",
                        on_hydration.hydration_size
                    );
                }
            }
        }

        // Validate the `max_replicas_per_cluster` limit for a raised replication
        // factor. This is the only place it is enforced on this path:
        // `Op::UpdateClusterConfig` contributes nothing to `catalog_transact`'s
        // replica accounting, because the controller materializes the replicas
        // on a later tick rather than this transaction emitting creates. Without
        // the check the ALTER would succeed and the controller would then fail
        // its own create transaction on every tick. See database-issues#6046.
        if new_replication_factor > replication_factor {
            if cluster_id.is_user() {
                self.validate_resource_limit(
                    usize::cast_from(*replication_factor),
                    i64::from(*new_replication_factor) - i64::from(*replication_factor),
                    SystemVars::max_replicas_per_cluster,
                    "cluster replica",
                    MAX_REPLICAS_PER_CLUSTER.name(),
                )?;
            }
        }

        let config_changed = new_managed.replica_config_shape()
            != ManagedReplicaConfigShape::new(
                size,
                availability_zones,
                logging,
                *arrangement_compression,
            );
        // The controller creates replicas from the realized config without
        // re-validating availability zones, so an invalid pool written here
        // would produce an unplaceable replica.
        if config_changed {
            self.ensure_valid_azs(new_availability_zones.iter())?;
        }

        // A record still in progress belongs to a live, converging
        // reconfiguration a config-only write did not touch, so carry it through
        // untouched. Hence no declared audit intent.
        let ops = vec![catalog::Op::UpdateClusterConfig {
            id: cluster_id,
            name,
            config: new_config,
            reconfiguration_audit: None,
            burst_audit: None,
        }];

        self.catalog_transact(Some(session), ops).await?;
        Ok(())
    }

    /// # Panics
    ///
    /// Panics if `new_config` is not a configuration for a managed cluster.
    async fn sequence_alter_cluster_unmanaged_to_managed(
        &mut self,
        session: &Session,
        cluster_id: ClusterId,
        mut new_config: ClusterConfig,
        options: PlanClusterOption,
    ) -> Result<(), AdapterError> {
        let cluster = self.catalog.get_cluster(cluster_id);
        let cluster_name = cluster.name().to_string();

        let ClusterVariant::Managed(ClusterVariantManaged {
            size: new_size,
            replication_factor: new_replication_factor,
            availability_zones: new_availability_zones,
            logging: _,
            arrangement_compression: _,
            optimizer_feature_overrides: _,
            schedule: _,
            auto_scaling_strategy: _,
            reconfiguration: _,
            burst: _,
        }) = &mut new_config.variant
        else {
            panic!("expected new managed cluster config");
        };

        // Validate replication factor parameter
        let user_replica_count = cluster
            .user_replicas()
            .count()
            .try_into()
            .expect("must_fit");
        match options.replication_factor {
            AlterOptionParameter::Set(_) => {
                // Validate that the replication factor matches the current length only if specified.
                if user_replica_count != *new_replication_factor {
                    coord_bail!(
                        "REPLICATION FACTOR {new_replication_factor} does not match number of replicas ({user_replica_count})"
                    );
                }
            }
            _ => {
                *new_replication_factor = user_replica_count;
            }
        }

        let mut names = BTreeSet::new();
        let mut sizes = BTreeSet::new();

        self.ensure_valid_azs(new_availability_zones.iter())?;

        // Validate per-replica configuration
        for replica in cluster.user_replicas() {
            names.insert(replica.name.clone());
            match &replica.config.location {
                ReplicaLocation::Unmanaged(_) => coord_bail!(
                    "Cannot convert unmanaged cluster with unmanaged replicas to managed cluster"
                ),
                ReplicaLocation::Managed(location) => {
                    sizes.insert(location.size.clone());

                    // An unmanaged cluster's replica carries its single
                    // user-pinned AZ (if any) as the sole entry; every pin must
                    // fall within the managed cluster's `AVAILABILITY ZONES`.
                    for az in &location.availability_zones {
                        if !new_availability_zones.contains(az) {
                            coord_bail!(
                                "unmanaged replica has availability zone {az} which is not \
                                in managed {new_availability_zones:?}"
                            )
                        }
                    }
                }
            }
        }

        if sizes.is_empty() {
            assert!(
                cluster.user_replicas().next().is_none(),
                "Cluster should not have replicas"
            );
            // We didn't collect any size, so the user has to name it.
            match &options.size {
                AlterOptionParameter::Reset | AlterOptionParameter::Unchanged => {
                    coord_bail!("Missing SIZE for empty cluster")
                }
                AlterOptionParameter::Set(_) => {} // Was set within the calling function.
            }
        } else if sizes.len() == 1 {
            let size = sizes.into_iter().next().expect("must exist");
            match &options.size {
                AlterOptionParameter::Set(sz) if *sz != size => {
                    coord_bail!("Cluster replicas of size {size} do not match expected SIZE {sz}");
                }
                _ => *new_size = size,
            }
        } else {
            let formatted = sizes
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .join(", ");
            coord_bail!(
                "Cannot convert unmanaged cluster to managed, non-unique replica sizes: {formatted}"
            );
        }

        for i in 0..*new_replication_factor {
            let name = managed_cluster_replica_name(i);
            names.remove(&name);
        }
        if !names.is_empty() {
            let formatted = names
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .join(", ");
            coord_bail!(
                "Cannot convert unmanaged cluster to managed, invalid replica names: {formatted}"
            );
        }

        let ops = vec![catalog::Op::UpdateClusterConfig {
            id: cluster_id,
            name: cluster_name,
            config: new_config,
            reconfiguration_audit: None,
            burst_audit: None,
        }];

        self.catalog_transact(Some(session), ops).await?;
        Ok(())
    }

    async fn sequence_alter_cluster_managed_to_unmanaged(
        &mut self,
        session: &Session,
        cluster_id: ClusterId,
        new_config: ClusterConfig,
    ) -> Result<(), AdapterError> {
        let cluster = self.catalog().get_cluster(cluster_id);

        // The unmanaged variant has no reconfiguration field, so converting
        // would silently drop an in-progress record with no terminal status
        // and no audit event, and strand any overlap replicas the controller
        // already created. Refuse instead: the user can cancel (ALTER back to
        // the realized size) or wait for the record to settle first.
        if let ClusterVariant::Managed(managed) = &cluster.config.variant {
            if managed
                .reconfiguration
                .as_ref()
                .is_some_and(|record| record.is_in_progress())
            {
                return Err(AdapterError::AlterClusterUnmanagedWhileReconfiguring);
            }
            // Same hazard for an in-flight burst: the unmanaged variant has no
            // burst field either, so converting would drop the record with no
            // `Finished` audit event and strand the billed burst replica as an
            // ordinary unmanaged replica nothing ever tears down. Absence of a
            // record means the burst has settled, so no in-progress check is
            // needed.
            if managed.burst.is_some() {
                return Err(AdapterError::AlterClusterUnmanagedWhileBursting);
            }
        }

        let ops = vec![catalog::Op::UpdateClusterConfig {
            id: cluster_id,
            name: cluster.name().to_string(),
            config: new_config,
            reconfiguration_audit: None,
            burst_audit: None,
        }];

        self.catalog_transact(Some(session), ops).await?;
        Ok(())
    }

    async fn sequence_alter_cluster_unmanaged_to_unmanaged(
        &mut self,
        session: &Session,
        cluster_id: ClusterId,
        new_config: ClusterConfig,
        replicas: AlterOptionParameter<Vec<(String, mz_sql::plan::ReplicaConfig)>>,
    ) -> Result<(), AdapterError> {
        if !matches!(replicas, AlterOptionParameter::Unchanged) {
            coord_bail!("Cannot alter replicas in unmanaged cluster");
        }

        let cluster = self.catalog().get_cluster(cluster_id);

        let ops = vec![catalog::Op::UpdateClusterConfig {
            id: cluster_id,
            name: cluster.name().to_string(),
            config: new_config,
            reconfiguration_audit: None,
            burst_audit: None,
        }];

        self.catalog_transact(Some(session), ops).await?;
        Ok(())
    }

    pub(crate) async fn sequence_alter_cluster_rename(
        &mut self,
        ctx: &mut ExecuteContext,
        AlterClusterRenamePlan { id, name, to_name }: AlterClusterRenamePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let op = Op::RenameCluster {
            id,
            name,
            to_name,
            check_reserved_names: true,
        };
        match self
            .catalog_transact_with_ddl_transaction(ctx, vec![op], |_, _| Box::pin(async {}))
            .await
        {
            Ok(()) => Ok(ExecuteResponse::AlteredObject(ObjectType::Cluster)),
            Err(err) => Err(err),
        }
    }

    pub(crate) async fn sequence_alter_cluster_swap(
        &mut self,
        ctx: &mut ExecuteContext,
        AlterClusterSwapPlan {
            id_a,
            id_b,
            name_a,
            name_b,
            name_temp,
        }: AlterClusterSwapPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let op_a = Op::RenameCluster {
            id: id_a,
            name: name_a.clone(),
            to_name: name_temp.clone(),
            check_reserved_names: false,
        };
        let op_b = Op::RenameCluster {
            id: id_b,
            name: name_b.clone(),
            to_name: name_a,
            check_reserved_names: false,
        };
        let op_temp = Op::RenameCluster {
            id: id_a,
            name: name_temp,
            to_name: name_b,
            check_reserved_names: false,
        };

        match self
            .catalog_transact_with_ddl_transaction(ctx, vec![op_a, op_b, op_temp], |_, _| {
                Box::pin(async {})
            })
            .await
        {
            Ok(()) => Ok(ExecuteResponse::AlteredObject(ObjectType::Cluster)),
            Err(err) => Err(err),
        }
    }

    pub(crate) async fn sequence_alter_cluster_replica_rename(
        &mut self,
        session: &Session,
        AlterClusterReplicaRenamePlan {
            cluster_id,
            replica_id,
            name,
            to_name,
        }: AlterClusterReplicaRenamePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let op = catalog::Op::RenameClusterReplica {
            cluster_id,
            replica_id,
            name,
            to_name,
        };
        match self.catalog_transact(Some(session), vec![op]).await {
            Ok(()) => Ok(ExecuteResponse::AlteredObject(ObjectType::ClusterReplica)),
            Err(err) => Err(err),
        }
    }

    /// Convert a [`AlterSetClusterPlan`] to a sequence of catalog operators and adjust state.
    pub(crate) async fn sequence_alter_set_cluster(
        &self,
        _session: &Session,
        AlterSetClusterPlan { id, set_cluster: _ }: AlterSetClusterPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        // TODO: This function needs to be implemented.

        // Satisfy Clippy that this is an async func.
        async {}.await;
        let entry = self.catalog().get_entry(&id);
        match entry.item().typ() {
            _ => {
                // Unexpected; planner permitted unsupported plan.
                Err(AdapterError::Unsupported("ALTER SET CLUSTER"))
            }
        }
    }
}

/// Which reconfiguration-target dimensions an `ALTER` left unset (`Unchanged`).
/// Drives [`fold_reconfiguration_target`]. Logging is two sub-dimensions
/// because `INTROSPECTION DEBUGGING` and `INTROSPECTION INTERVAL` are
/// independently alterable.
struct ReconfigurationDimensionsUnchanged {
    size: bool,
    replication_factor: bool,
    availability_zones: bool,
    log_logging: bool,
    interval: bool,
    arrangement_compression: bool,
}

/// Whether the record an `ALTER` with this `WITH (WAIT ...)` clause writes cuts
/// over on the controller's first tick, so the realized and target baseline
/// replica sets never coexist.
///
/// True for a zero timeout that commits: `WAIT FOR '0s'` (sugar for `ON TIMEOUT
/// COMMIT`) or an explicit `WAIT UNTIL READY (TIMEOUT '0s', ON TIMEOUT
/// 'COMMIT')`. Such a record's deadline has already elapsed when it is written,
/// so the tick's first phase advances the realized config to the target before
/// its second phase desires any replica. The reshape's transient peak is then
/// the target set plus other surviving strategy contributions, which is what
/// [`Coordinator::validate_reconfiguration_resource_limits`] models it with.
///
/// This is a statement about resource footprint only. Every shape-changing
/// `ALTER` takes the same route, writing a record the controller converges on.
fn cuts_over_on_first_tick(strategy: &AlterClusterPlanStrategy) -> bool {
    match strategy {
        AlterClusterPlanStrategy::None => false,
        AlterClusterPlanStrategy::For(timeout) => timeout.is_zero(),
        AlterClusterPlanStrategy::UntilReady {
            timeout,
            on_timeout,
        } => timeout.is_zero() && matches!(on_timeout, Some(OnTimeoutAction::Commit)),
    }
}

/// Replica creates and retires the first controller tick performs to replace the
/// realized baseline with `target`.
#[derive(Clone, Debug, PartialEq, Eq)]
struct FirstTickReplicaDelta {
    create_target: u32,
    create_burst: Option<String>,
    retire_realized: u32,
    retire_burst: Option<String>,
}

/// Computes a first-tick replacement from the controller's own state projection
/// and ownership test.
///
/// Replicas outside the realized and target shapes are carried conservatively.
/// A warranted durable burst also reserves its post-cut-over slot before that
/// replica materializes. The shared warrant predicate keeps this projection in
/// lockstep with catalog application.
fn first_tick_replica_delta(
    state: &ClusterState,
    target: &ReconfigurationTarget,
    preserve_burst: bool,
) -> FirstTickReplicaDelta {
    let target_shape = ReplicaShape {
        size: target.size.clone(),
        availability_zones: AvailabilityZones(target.availability_zones.clone()),
        logging: target.logging.clone(),
        arrangement_compression: target.arrangement_compression,
    };
    let realized_shape = state.realized_shape();
    let burst_shape = preserve_burst.then(|| {
        let burst = state
            .burst
            .as_ref()
            .expect("a preserved burst has a durable record");
        ReplicaShape {
            size: burst.burst_size.clone(),
            availability_zones: AvailabilityZones(target.availability_zones.clone()),
            logging: target.logging.clone(),
            arrangement_compression: target.arrangement_compression,
        }
    });
    let old_burst_shape = preserve_burst.then(|| {
        let burst = state
            .burst
            .as_ref()
            .expect("a preserved burst has a durable record");
        ReplicaShape {
            size: burst.burst_size.clone(),
            availability_zones: AvailabilityZones(state.availability_zones.clone()),
            logging: state.logging.clone(),
            arrangement_compression: state.arrangement_compression,
        }
    });
    let mut target_matches = 0u32;
    let mut burst_matches = 0u32;
    let mut old_burst_matches = 0u32;
    let mut realized_matches = 0u32;
    for replica in &state.replicas {
        let Some(shape) = replica.owned_shape() else {
            continue;
        };
        if shape.matches(&target_shape) {
            target_matches = target_matches.saturating_add(1);
        }
        if burst_shape
            .as_ref()
            .is_some_and(|burst| shape.matches(burst))
        {
            burst_matches = burst_matches.saturating_add(1);
        }
        if old_burst_shape
            .as_ref()
            .is_some_and(|burst| shape.matches(burst))
        {
            old_burst_matches = old_burst_matches.saturating_add(1);
        }
        if shape.matches(&realized_shape) {
            realized_matches = realized_matches.saturating_add(1);
        }
    }
    let burst_shares_target = burst_shape
        .as_ref()
        .is_some_and(|burst| burst.matches(&target_shape));
    let create_burst = burst_shape
        .as_ref()
        .filter(|_| !burst_shares_target && burst_matches == 0)
        .map(|burst| burst.size.clone());
    let retire_burst = old_burst_shape
        .as_ref()
        .filter(|old| {
            old_burst_matches > 0
                && !old.matches(&target_shape)
                && !old.matches(&realized_shape)
                && !burst_shape.as_ref().is_some_and(|burst| old.matches(burst))
        })
        .map(|burst| burst.size.clone());
    let realized_reserved_for_burst = burst_shape
        .as_ref()
        .is_some_and(|burst| burst.matches(&realized_shape))
        .then_some(1)
        .unwrap_or(0);
    let retire_realized = if target_shape.matches(&realized_shape) {
        0
    } else {
        state
            .replication_factor
            .min(realized_matches.saturating_sub(realized_reserved_for_burst))
    };
    FirstTickReplicaDelta {
        create_target: target.replication_factor.saturating_sub(target_matches),
        create_burst,
        retire_realized,
        retire_burst,
    }
}

/// Whether an `ALTER` statement sets a replica config shape dimension (`SIZE`,
/// `AVAILABILITY ZONES`, either `INTROSPECTION` option, or `EXPERIMENTAL
/// ARRANGEMENT COMPRESSION`), the changes that need a durable
/// `reconfiguration` record and a hydrate-overlap.
///
/// A statement-level check, used while a reconfiguration is in flight: an
/// `ALTER` back to the realized shape sets a shape option without changing its
/// value, yet must reach the reshape path to cancel the record. With nothing
/// in flight the routing compares values instead (see
/// `sequence_alter_cluster_stage`).
fn alter_changes_replica_shape(options: &PlanClusterOption) -> bool {
    use mz_sql::plan::AlterOptionParameter::Unchanged;
    let PlanClusterOption {
        availability_zones,
        introspection_debugging,
        introspection_interval,
        arrangement_compression,
        managed: _,
        replicas: _,
        replication_factor: _,
        size,
        schedule: _,
        workload_class: _,
        auto_scaling_strategy: _,
    } = options;
    !matches!(size, Unchanged)
        || !matches!(availability_zones, Unchanged)
        || !matches!(introspection_debugging, Unchanged)
        || !matches!(introspection_interval, Unchanged)
        || !matches!(arrangement_compression, Unchanged)
}

/// The reconfiguration target an `ALTER` establishes: the shape it asks for,
/// folded onto the in-flight target when a reconfiguration is in progress.
///
/// `new_managed` was built against the *realized* config, which still holds the
/// pre-reconfiguration shape (the realized config advances only at cut-over), so
/// a dimension the statement left `Unchanged` carries the realized value there.
/// Folding replaces those with the in-flight target's values, leaving only the
/// dimensions the statement explicitly set to diverge. Without it, an `ALTER`
/// that mentions one dimension would silently revert the transition along every
/// dimension it did not mention.
fn alter_reconfiguration_target(
    new_managed: &ClusterVariantManaged,
    options: &PlanClusterOption,
    in_flight: Option<&ReconfigurationTarget>,
) -> ReconfigurationTarget {
    use mz_sql::plan::AlterOptionParameter::Unchanged;

    // Both structs are destructured exhaustively: a new shape dimension on the
    // cluster config, or a new `ALTER` option that names one, then fails to
    // compile until it is either folded or explicitly ruled out here.
    let ClusterVariantManaged {
        size,
        replication_factor,
        availability_zones,
        logging,
        arrangement_compression,
        // Cluster-level settings, not per-replica shape. A reconfiguration does
        // not transition them, so they never enter a target.
        optimizer_feature_overrides: _,
        schedule: _,
        auto_scaling_strategy: _,
        reconfiguration: _,
        burst: _,
    } = new_managed;
    let PlanClusterOption {
        size: size_opt,
        replication_factor: replication_factor_opt,
        availability_zones: availability_zones_opt,
        arrangement_compression: arrangement_compression_opt,
        introspection_debugging,
        introspection_interval,
        // Not shape dimensions: `managed`/`replicas` change the variant rather
        // than reshape it, and the rest are cluster-level settings that apply
        // immediately instead of transitioning through a reconfiguration.
        managed: _,
        replicas: _,
        schedule: _,
        workload_class: _,
        auto_scaling_strategy: _,
    } = options;

    let new_target = ReconfigurationTarget {
        size: size.clone(),
        replication_factor: *replication_factor,
        availability_zones: availability_zones.clone(),
        logging: logging.clone(),
        arrangement_compression: *arrangement_compression,
    };
    let unchanged = ReconfigurationDimensionsUnchanged {
        size: matches!(size_opt, Unchanged),
        replication_factor: matches!(replication_factor_opt, Unchanged),
        availability_zones: matches!(availability_zones_opt, Unchanged),
        // The two logging options fold independently, so a debugging-only
        // `ALTER` cannot revert an in-flight interval change (or vice versa).
        log_logging: matches!(introspection_debugging, Unchanged),
        interval: matches!(introspection_interval, Unchanged),
        arrangement_compression: matches!(arrangement_compression_opt, Unchanged),
    };
    fold_reconfiguration_target(in_flight, new_target, unchanged)
}

/// Fold a new `ALTER` onto an in-flight reconfiguration target.
///
/// `new_target` was built against the *realized* config, so any dimension the
/// `ALTER` left `Unchanged` carries the realized (pre-reconfiguration) value. When
/// a reconfiguration is in flight (`in_flight` is `Some`), the realized config is
/// the pre-reconfiguration shape, so for each `Unchanged` dimension we instead
/// keep the in-flight target's value. Only dimensions the `ALTER` explicitly set
/// re-target. With nothing in flight (`in_flight` is `None`) the target is exactly
/// `new_target`. This is what keeps an `ALTER` that touches one dimension (e.g.
/// AZ-only) from silently reverting the in-flight transition along every dimension
/// it did not mention.
///
/// Replication factor folds the same way, though the fold is vacuous for it in
/// practice: `sequence_alter_cluster_stage` refuses a replication-factor change
/// while a reconfiguration is in flight, so `unchanged.replication_factor` is
/// always `true` under an `in_flight` target.
fn fold_reconfiguration_target(
    in_flight: Option<&ReconfigurationTarget>,
    new_target: ReconfigurationTarget,
    unchanged: ReconfigurationDimensionsUnchanged,
) -> ReconfigurationTarget {
    let Some(prev) = in_flight else {
        return new_target;
    };
    ReconfigurationTarget {
        size: if unchanged.size {
            prev.size.clone()
        } else {
            new_target.size
        },
        replication_factor: if unchanged.replication_factor {
            prev.replication_factor
        } else {
            new_target.replication_factor
        },
        availability_zones: if unchanged.availability_zones {
            prev.availability_zones.clone()
        } else {
            new_target.availability_zones
        },
        logging: ReplicaLogging {
            log_logging: if unchanged.log_logging {
                prev.logging.log_logging
            } else {
                new_target.logging.log_logging
            },
            interval: if unchanged.interval {
                prev.logging.interval
            } else {
                new_target.logging.interval
            },
        },
        arrangement_compression: if unchanged.arrangement_compression {
            prev.arrangement_compression
        } else {
            new_target.arrangement_compression
        },
    }
}

#[cfg(test)]
mod tests {
    use mz_cluster_controller::ctx::{BurstRecord, ClusterSchedule, ObservedReplica};
    use mz_controller::clusters::ReplicaLogging;
    use mz_controller_types::DEFAULT_REPLICA_LOGGING_INTERVAL;

    use super::*;

    fn target(size: &str, rf: u32, azs: &[&str], log_logging: bool) -> ReconfigurationTarget {
        ReconfigurationTarget {
            size: size.to_string(),
            replication_factor: rf,
            availability_zones: azs.iter().map(|s| s.to_string()).collect(),
            logging: ReplicaLogging {
                log_logging,
                interval: Some(DEFAULT_REPLICA_LOGGING_INTERVAL),
            },
            arrangement_compression: false,
        }
    }

    fn all_changed() -> ReconfigurationDimensionsUnchanged {
        ReconfigurationDimensionsUnchanged {
            size: false,
            replication_factor: false,
            availability_zones: false,
            log_logging: false,
            interval: false,
            arrangement_compression: false,
        }
    }

    fn observed(id: u64, size: &str, internal: bool) -> ObservedReplica {
        ObservedReplica {
            replica_id: ReplicaId::User(id),
            name: format!("r{id}"),
            shape: Some(ReplicaShape {
                size: size.to_string(),
                availability_zones: AvailabilityZones::default(),
                logging: ReplicaLogging::default(),
                arrangement_compression: false,
            }),
            internal,
            billed_as: false,
            pending: false,
        }
    }

    fn observed_state(replicas: Vec<ObservedReplica>) -> ClusterState {
        ClusterState {
            cluster_id: ClusterId::User(1),
            size: "100cc".to_string(),
            replication_factor: 1,
            availability_zones: Vec::new(),
            logging: ReplicaLogging::default(),
            arrangement_compression: false,
            schedule: ClusterSchedule::Manual,
            auto_scaling_policy: None,
            reconfiguration: None,
            burst: None,
            replicas,
        }
    }

    fn all_unchanged() -> ReconfigurationDimensionsUnchanged {
        ReconfigurationDimensionsUnchanged {
            size: true,
            replication_factor: true,
            availability_zones: true,
            log_logging: true,
            interval: true,
            arrangement_compression: true,
        }
    }

    #[mz_ore::test]
    fn fold_with_no_record_takes_new_target() {
        // No reconfiguration in flight: the target is exactly the new one.
        let new = target("200cc", 3, &["az1"], true);
        let folded = fold_reconfiguration_target(None, new.clone(), all_changed());
        assert_eq!(folded, new);
    }

    #[mz_ore::test]
    fn fold_rf_only_keeps_in_flight_shape() {
        // A 200cc size change is in flight. An rf-only fold must NOT revert the
        // in-flight size/AZ/logging back to the realized (100cc) values that
        // `new_target` carries for the dimensions the ALTER left unchanged.
        // Unreachable from the `ALTER` path (a replication-factor change while a
        // reconfiguration is in flight is refused), pinned as a property of the
        // pure function.
        let in_flight = target("200cc", 1, &["az2"], true);
        // new_target reflects realized 100cc/az1 for every dimension but rf, which
        // the ALTER set to 5.
        let new = target("100cc", 5, &["az1"], false);
        let unchanged = ReconfigurationDimensionsUnchanged {
            size: true,
            replication_factor: false,
            availability_zones: true,
            log_logging: true,
            interval: true,
            arrangement_compression: true,
        };
        let folded = fold_reconfiguration_target(Some(&in_flight), new, unchanged);
        // The in-flight size/AZ/logging survive. Only rf is re-targeted.
        assert_eq!(folded, target("200cc", 5, &["az2"], true));
    }

    #[mz_ore::test]
    fn fold_with_all_set_overwrites_every_dimension() {
        // Every dimension explicitly set: the fold takes all of new_target.
        // Setting the replication factor over an in-flight target is unreachable
        // from the `ALTER` path (that change is refused while a reconfiguration
        // is in flight), pinned as a property of the pure function.
        let in_flight = target("200cc", 1, &["az2"], true);
        let new = target("400cc", 9, &["az9"], false);
        let folded = fold_reconfiguration_target(Some(&in_flight), new.clone(), all_changed());
        assert_eq!(folded, new);
    }

    #[mz_ore::test]
    fn fold_all_unchanged_is_alter_back_to_in_flight() {
        // An all-unchanged fold keeps the in-flight target intact rather than
        // reverting it to the realized shape. Unreachable from the `ALTER`
        // path (non-shape statements no longer reach the fold), pinned as a
        // property of the pure function.
        let in_flight = target("200cc", 2, &["az2"], true);
        let realized_shaped = target("100cc", 1, &["az1"], false);
        let folded =
            fold_reconfiguration_target(Some(&in_flight), realized_shaped, all_unchanged());
        assert_eq!(folded, in_flight);
    }

    #[mz_ore::test]
    fn fold_logging_subdimensions_fold_independently() {
        // An interval change is in flight. A later ALTER that sets only
        // INTROSPECTION DEBUGGING must not revert the in-flight interval to the
        // realized value that `new_target` carries for options the ALTER left
        // unset.
        let mut in_flight = target("100cc", 1, &["az1"], false);
        in_flight.logging.interval = Some(Duration::from_secs(5));
        let new = target("100cc", 1, &["az1"], true);
        let unchanged = ReconfigurationDimensionsUnchanged {
            size: true,
            replication_factor: true,
            availability_zones: true,
            log_logging: false,
            interval: true,
            arrangement_compression: true,
        };
        let folded = fold_reconfiguration_target(Some(&in_flight), new, unchanged);
        assert_eq!(
            folded.logging,
            ReplicaLogging {
                log_logging: true,
                interval: Some(Duration::from_secs(5)),
            }
        );
    }

    #[mz_ore::test]
    fn cuts_over_on_first_tick_only_for_a_zero_timeout_commit() {
        let zero = Duration::ZERO;
        let nonzero = Duration::from_millis(1);

        assert!(cuts_over_on_first_tick(&AlterClusterPlanStrategy::For(
            zero
        )));
        assert!(cuts_over_on_first_tick(
            &AlterClusterPlanStrategy::UntilReady {
                timeout: zero,
                on_timeout: Some(OnTimeoutAction::Commit),
            }
        ));

        // No `WAIT` at all, or any non-zero timeout: the deadline is in the
        // future, so the controller provisions the overlap set and waits.
        assert!(!cuts_over_on_first_tick(&AlterClusterPlanStrategy::None));
        assert!(!cuts_over_on_first_tick(&AlterClusterPlanStrategy::For(
            nonzero
        )));
        assert!(!cuts_over_on_first_tick(
            &AlterClusterPlanStrategy::UntilReady {
                timeout: nonzero,
                on_timeout: Some(OnTimeoutAction::Commit),
            }
        ));
        // A zero timeout that does not commit abandons the target instead of
        // cutting over to it. It is not modelled as a cut-over.
        assert!(!cuts_over_on_first_tick(
            &AlterClusterPlanStrategy::UntilReady {
                timeout: zero,
                on_timeout: Some(OnTimeoutAction::Rollback),
            }
        ));
        assert!(!cuts_over_on_first_tick(
            &AlterClusterPlanStrategy::UntilReady {
                timeout: zero,
                on_timeout: None,
            }
        ));
    }

    #[mz_ore::test]
    fn first_tick_resource_delta_carries_non_baseline_replicas() {
        let target_config = target("200cc", 2, &[], false);
        let state = observed_state(vec![
            observed(1, "100cc", false),
            observed(2, "400cc", false),
        ]);
        assert_eq!(
            first_tick_replica_delta(&state, &target_config, false),
            FirstTickReplicaDelta {
                create_target: 2,
                create_burst: None,
                retire_realized: 1,
                retire_burst: None,
            }
        );

        let state = observed_state(vec![
            observed(1, "100cc", false),
            observed(2, "200cc", false),
            observed(3, "400cc", false),
        ]);
        assert_eq!(
            first_tick_replica_delta(&state, &target_config, false),
            FirstTickReplicaDelta {
                create_target: 1,
                create_burst: None,
                retire_realized: 1,
                retire_burst: None,
            },
            "an existing target replica is reused"
        );

        let state = observed_state(vec![
            observed(1, "100cc", false),
            observed(2, "200cc", true),
        ]);
        assert_eq!(
            first_tick_replica_delta(&state, &target_config, false),
            FirstTickReplicaDelta {
                create_target: 2,
                create_burst: None,
                retire_realized: 1,
                retire_burst: None,
            },
            "a controller-unowned target replica cannot satisfy the target"
        );

        let mut state = observed_state(vec![observed(1, "100cc", false)]);
        state.burst = Some(BurstRecord {
            burst_size: "400cc".to_string(),
            linger_duration: Duration::from_secs(60),
            steady_hydrated_at: None,
        });
        assert_eq!(
            first_tick_replica_delta(&state, &target_config, true),
            FirstTickReplicaDelta {
                create_target: 2,
                create_burst: Some("400cc".to_string()),
                retire_realized: 1,
                retire_burst: None,
            },
            "a durable burst reserves its replica before materialization"
        );

        let mut state = observed_state(vec![
            observed(1, "100cc", false),
            observed(2, "400cc", false),
        ]);
        state.burst = Some(BurstRecord {
            burst_size: "400cc".to_string(),
            linger_duration: Duration::from_secs(60),
            steady_hydrated_at: None,
        });
        let target = target("100cc", 1, &["az2"], false);
        assert_eq!(
            first_tick_replica_delta(&state, &target, true),
            FirstTickReplicaDelta {
                create_target: 1,
                create_burst: Some("400cc".to_string()),
                retire_realized: 1,
                retire_burst: Some("400cc".to_string()),
            },
            "a burst whose inherited shape changes is replaced, not added"
        );
    }
}
