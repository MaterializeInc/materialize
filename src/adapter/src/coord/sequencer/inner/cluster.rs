// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use itertools::Itertools;
use maplit::btreeset;
use mz_adapter_types::cluster_state::ReconfigurationAudit;
use mz_catalog::builtin::BUILTINS;
use mz_catalog::memory::objects::{
    ClusterConfig, ClusterReplica, ClusterVariant, ClusterVariantManaged,
    ManagedReplicaConfigShape, ReconfigurationState, ReconfigurationStatus, ReconfigurationTarget,
};
use mz_compute_types::config::ComputeReplicaConfig;
use mz_controller::clusters::{
    ClusterStatus, ManagedReplicaLocation, ReplicaConfig, ReplicaLocation, ReplicaLogging,
};
use mz_controller_types::{ClusterId, DEFAULT_REPLICA_LOGGING_INTERVAL, ReplicaId};
use mz_ore::cast::CastFrom;
use mz_ore::collections::CollectionExt;
use mz_ore::instrument;
use mz_repr::adt::numeric::Numeric;
use mz_repr::role_id::RoleId;
use mz_sql::ast::{Ident, QualifiedReplica};
use mz_sql::catalog::{CatalogCluster, ObjectType};
use mz_sql::plan::{
    self, AlterClusterPlanStrategy, AlterClusterRenamePlan, AlterClusterReplicaRenamePlan,
    AlterClusterSwapPlan, AlterOptionParameter, AlterSetClusterPlan,
    ComputeReplicaIntrospectionConfig, CreateClusterManagedPlan, CreateClusterPlan,
    CreateClusterReplicaPlan, CreateClusterUnmanagedPlan, CreateClusterVariant, PlanClusterOption,
};
use mz_sql::plan::{AlterClusterPlan, OnTimeoutAction};
use mz_sql::session::metadata::SessionMetadata;
use mz_sql::session::vars::{
    MAX_CREDIT_CONSUMPTION_RATE, MAX_REPLICAS_PER_CLUSTER, SystemVars, Var,
};
use tracing::{Instrument, Span, debug};

use mz_adapter_types::dyncfgs::{
    DEFAULT_CLUSTER_RECONFIGURATION_TIMEOUT, ENABLE_BACKGROUND_ALTER_CLUSTER,
    ENABLE_CLUSTER_CONTROLLER,
};

use super::return_if_err;
use crate::AdapterError::AlterClusterWhilePendingReplicas;
use crate::catalog::{self, Op, ReplicaCreateDropReason};
use crate::config::{
    ClusterEvalContext, ClusterScopeContext, ReplicaEvalContext, ReplicaScopeContext,
};
use crate::coord::{
    AlterCluster, AlterClusterAwaitReconfiguration, AlterClusterFinalize,
    AlterClusterWaitForHydrated, ClusterReplicaStatuses, ClusterStage, Coordinator, Message,
    PlanValidity, StageResult, Staged,
};
use crate::{AdapterError, ExecuteContext, ExecuteResponse, session::Session};

const PENDING_REPLICA_SUFFIX: &str = "-pending";

impl Staged for ClusterStage {
    type Ctx = ExecuteContext;

    fn validity(&mut self) -> &mut PlanValidity {
        match self {
            Self::Alter(stage) => &mut stage.validity,
            Self::WaitForHydrated(stage) => &mut stage.validity,
            Self::Finalize(stage) => &mut stage.validity,
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
            Self::WaitForHydrated(stage) => {
                let AlterClusterWaitForHydrated {
                    validity,
                    plan,
                    new_config,
                    workload_class,
                    timeout_time,
                    on_timeout,
                } = stage;
                coord
                    .check_if_pending_replicas_hydrated_stage(
                        ctx.session(),
                        plan,
                        new_config,
                        workload_class,
                        timeout_time,
                        on_timeout,
                        validity,
                    )
                    .await
            }
            Self::Finalize(stage) => {
                coord
                    .finalize_alter_cluster_stage(
                        ctx.session(),
                        stage.plan.clone(),
                        stage.new_config.clone(),
                        stage.workload_class.clone(),
                    )
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

        // The controller owns only *user* managed clusters (see `ManagedClusterIds`
        // in cluster_controller.rs and `controller_owns` in the managed-to-managed
        // path below). A system/builtin cluster is never converged by the
        // controller, so it must not be reshaped into a durable reconfiguration
        // record nobody would cut over. It takes the direct realized-config path
        // below, exactly as it does with the controller off.
        let cluster_controller_owns = ENABLE_CLUSTER_CONTROLLER
            .get(self.catalog().system_config().dyncfgs())
            && cluster_id.is_user();
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
        if cluster_controller_owns
            && reconfiguration_in_flight
            && !matches!(options.schedule, Unchanged)
        {
            return Err(AdapterError::AlterClusterScheduleWhileReconfiguring);
        }

        // Replication factor is one of the four dimensions the cut-over sets
        // atomically from the record's target (`fold_reconfiguration_target`),
        // so a change applied independently while a reconfiguration is in
        // flight would be silently clobbered at cut-over. Refused even when the
        // same statement also re-targets the shape, so a record's target
        // replication factor is always the one it started with.
        if cluster_controller_owns
            && reconfiguration_in_flight
            && !matches!(options.replication_factor, Unchanged)
        {
            return Err(AdapterError::AlterClusterReplicationFactorWhileReconfiguring);
        }

        // A no-op `ALTER` short-circuits, except that an `ALTER` back to the
        // realized shape while a reconfiguration is in flight produces a
        // byte-identical `new_config` and is still meaningful: it must reach
        // the reshape path below to cancel the record.
        let cancels_or_retargets =
            reconfiguration_in_flight && alter_changes_replica_shape(options);
        if new_config == config && !(cluster_controller_owns && cancels_or_retargets) {
            return Ok(StageResult::Response(ExecuteResponse::AlteredObject(
                ObjectType::Cluster,
            )));
        }

        // When the controller owns the replica set, a shape-changing `ALTER`
        // reshapes into a durable `reconfiguration` record (starting,
        // retargeting, or cancelling one) instead of going through the legacy
        // 3-stage machine. Everything else falls through to the realized-config
        // update below without touching the record, in flight or not.
        //
        // With a record in flight the statement decides: an `ALTER` back to the
        // realized shape is value-identical yet must reach the reshape path to
        // cancel. With nothing in flight the values decide: a shape option set
        // to its current value reconfigures nothing, and reshaping it anyway
        // would write a spurious pre-cancelled record.
        if cluster_controller_owns {
            if let (Managed(old_managed), Managed(new_managed)) =
                (&config.variant, &new_config.variant)
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
                // path below instead of writing a record: with the controller
                // owning the cluster the direct path only updates the realized
                // config, and the controller reconciles any in-window replica
                // to the new shape on its next tick. The schedule guard above
                // keeps a schedule change from reaching here mid-record, so a
                // record on a scheduled cluster can only pre-date the schedule
                // (written on an older version). For that case the reshape
                // path stays reachable, so the record can still be retargeted
                // or cancelled until it settles.
                let scheduled_direct =
                    !matches!(new_managed.schedule, mz_sql::plan::ClusterSchedule::Manual)
                        && !reconfiguration_in_flight;
                if needs_record && !scheduled_direct {
                    return self
                        .reshape_alter_cluster_managed(
                            session,
                            cluster_id,
                            new_config.clone(),
                            options,
                            strategy,
                            validity,
                        )
                        .await;
                }
            }
        }

        match (&config.variant, &new_config.variant) {
            (Managed(_), Managed(new_config_managed)) => {
                let alter_followup = self
                    .sequence_alter_cluster_managed_to_managed(
                        Some(session),
                        cluster_id,
                        new_config.clone(),
                        ReplicaCreateDropReason::Manual,
                        strategy.clone(),
                    )
                    .await?;
                if alter_followup == NeedsFinalization::Yes {
                    // For non backgrounded zero-downtime alters, store the
                    // cluster_id in the ConnMeta to allow for cancellation.
                    self.active_conns
                        .get_mut(session.conn_id())
                        .expect("There must be an active connection")
                        .pending_cluster_alters
                        .insert(cluster_id.clone());
                    let new_config_managed = new_config_managed.clone();
                    return match &strategy {
                        AlterClusterPlanStrategy::None => Err(AdapterError::Internal(
                            "AlterClusterPlanStrategy must not be None if NeedsFinalization is Yes"
                                .into(),
                        )),
                        AlterClusterPlanStrategy::For(duration) => {
                            let span = Span::current();
                            let plan = plan.clone();
                            let duration = duration.clone().to_owned();
                            let workload_class = new_config.workload_class.clone();
                            Ok(StageResult::Handle(mz_ore::task::spawn(
                                || "Finalize Alter Cluster",
                                async move {
                                    tokio::time::sleep(duration).await;
                                    let stage = ClusterStage::Finalize(AlterClusterFinalize {
                                        validity,
                                        plan,
                                        new_config: new_config_managed,
                                        workload_class,
                                    });
                                    Ok(Box::new(stage))
                                }
                                .instrument(span),
                            )))
                        }
                        AlterClusterPlanStrategy::UntilReady {
                            timeout,
                            on_timeout,
                        } => Ok(StageResult::Immediate(Box::new(
                            ClusterStage::WaitForHydrated(AlterClusterWaitForHydrated {
                                validity,
                                plan: plan.clone(),
                                new_config: new_config_managed.clone(),
                                workload_class: new_config.workload_class.clone(),
                                timeout_time: Instant::now() + timeout.to_owned(),
                                // The legacy foreground wait uses COMMIT as
                                // its implicit default. The controller-owned
                                // paths default to ROLLBACK.
                                on_timeout: on_timeout.unwrap_or(OnTimeoutAction::Commit),
                            }),
                        ))),
                    };
                }
            }
            (Unmanaged, Managed(_)) => {
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
    fn validate_reconfiguration_resource_limits(
        &self,
        cluster_id: ClusterId,
        target: &ReconfigurationTarget,
    ) -> Result<(), AdapterError> {
        // Only user clusters are converged by the controller and counted against
        // these limits. A system cluster never reshapes into a record.
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

        // Both checks below model the transient peak: the controller runs the
        // realized and target sets side by side until cut-over, so this cluster's
        // peak contribution is both shapes at once, computed from config as
        // realized plus target. That slightly over-counts a same-shape overlap,
        // where existing replicas double as target replicas, but it matches the
        // legacy wait path, which creates the full target set as pending replicas
        // at `ALTER` time and therefore enforces both limits on the overlap.
        // Rejecting here is also strictly better than the asynchronous abort the
        // controller falls back to when a limit shrinks or the environment grows
        // after the record is written.

        // Per-cluster replica count: the peak is `realized_rf + target_rf`,
        // deterministic from the cluster's own config. `validate_resource_limit`
        // returns early on an rf-0 target.
        self.validate_resource_limit(
            usize::cast_from(realized.replication_factor),
            i64::from(target.replication_factor),
            SystemVars::max_replicas_per_cluster,
            "cluster replica",
            MAX_REPLICAS_PER_CLUSTER.name(),
        )?;

        // Global credit rate: the peak is `credit(realized) + credit(target)`.
        self.validate_reconfiguration_credit_peak(cluster_id, realized, target)?;

        Ok(())
    }

    /// Validates that the transient credit-rate peak of a reconfiguration, the
    /// realized plus the target shape, fits the environment-wide budget.
    ///
    /// The base is the live consumption of every other cluster. It excludes
    /// this cluster's own replicas so a re-target of an in-flight record does
    /// not additionally count an already-materialized overlap on top of the
    /// modeled peak.
    fn validate_reconfiguration_credit_peak(
        &self,
        cluster_id: ClusterId,
        realized: &ClusterVariantManaged,
        target: &ReconfigurationTarget,
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
        let mut peak_credit = shape_credit(&target.size, target.replication_factor);
        peak_credit += shape_credit(&realized.size, realized.replication_factor);
        self.validate_resource_limit_numeric(
            self.current_credit_consumption_rate(Some(cluster_id)),
            peak_credit,
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
    /// **Fold semantics.** When a record is already in flight, the target is an
    /// overlay on the *in-flight target*, not the realized config: a dimension the
    /// `ALTER` set (`options.*` is `Set`/`Reset`) takes the new value, a dimension
    /// left `Unchanged` keeps the in-flight target's value. `new_config` was built
    /// against the realized config, which still holds the pre-reconfiguration shape
    /// (the realized config is advanced only at cut-over), so seeding `Unchanged`
    /// dimensions from it would silently revert the in-flight transition along any
    /// dimension this `ALTER` did not mention. With no record in flight there is
    /// nothing to fold and the target is exactly `new_config`'s shape.
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
        use mz_sql::plan::AlterOptionParameter::Unchanged;

        let ClusterVariant::Managed(new_managed) = &new_config.variant else {
            return Err(AdapterError::Internal(
                "reshape_alter_cluster_managed requires a managed target config".into(),
            ));
        };

        // Fold onto the in-flight target when one exists: `new_config` carries the
        // realized value for any dimension the `ALTER` left `Unchanged`, but the
        // realized config is the pre-reconfiguration shape, so we instead carry the
        // in-flight target's value for those dimensions. Only dimensions the `ALTER`
        // explicitly set diverge from the in-flight target.
        let cluster = self.catalog.get_cluster(cluster_id);
        let in_flight = match &cluster.config.variant {
            ClusterVariant::Managed(managed) => managed
                .reconfiguration
                .as_ref()
                .filter(|record| record.is_in_progress())
                .cloned(),
            ClusterVariant::Unmanaged => None,
        };
        let new_target = ReconfigurationTarget {
            size: new_managed.size.clone(),
            replication_factor: new_managed.replication_factor,
            availability_zones: new_managed.availability_zones.clone(),
            logging: new_managed.logging.clone(),
            arrangement_compression: new_managed.arrangement_compression,
        };
        let unchanged = ReconfigurationDimensionsUnchanged {
            size: matches!(options.size, Unchanged),
            replication_factor: matches!(options.replication_factor, Unchanged),
            availability_zones: matches!(options.availability_zones, Unchanged),
            // The two logging options fold independently, so a debugging-only
            // `ALTER` cannot revert an in-flight interval change (or vice versa).
            log_logging: matches!(options.introspection_debugging, Unchanged),
            interval: matches!(options.introspection_interval, Unchanged),
            arrangement_compression: matches!(options.arrangement_compression, Unchanged),
        };
        let target = fold_reconfiguration_target(
            in_flight.as_ref().map(|r| &r.target),
            new_target,
            unchanged,
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
        self.validate_reconfiguration_resource_limits(cluster_id, &target)?;

        // Resolve the deadline and the on-timeout action from the existing
        // `WITH (WAIT ...)` surface. Both are written relative to the current time
        // so they survive session disconnect and restart. Unlike the target, which
        // folds per-dimension onto the in-flight one, the deadline and `on_timeout`
        // are replaced wholesale by the latest `ALTER`'s `WAIT` clause (they are
        // resolved fresh here, not merged), so re-issuing an `ALTER` with a
        // different `ON TIMEOUT` overwrites the prior action.
        //   - no `WAIT`         -> the system-default timeout and the implicit
        //                          `on_timeout` default (`ROLLBACK`).
        //   - `WAIT FOR`        -> sugar for `ON TIMEOUT COMMIT` (cut over at the
        //                          deadline regardless of hydration).
        //   - `WAIT UNTIL READY -> the explicit `TIMEOUT` / `ON TIMEOUT`, with
        //                          `ON TIMEOUT` defaulting to `ROLLBACK` when
        //                          omitted. The safe default for the controller
        //                          path reverts an un-hydrated reconfiguration to
        //                          its pre-reconfiguration shape rather than
        //                          cutting over to a not-yet-hydrated target
        //                          (which could induce downtime). The legacy
        //                          foreground path uses implicit `COMMIT`.
        let (timeout, on_timeout) = match strategy {
            AlterClusterPlanStrategy::None => (
                DEFAULT_CLUSTER_RECONFIGURATION_TIMEOUT
                    .get(self.catalog().system_config().dyncfgs()),
                OnTimeoutAction::Rollback,
            ),
            AlterClusterPlanStrategy::For(timeout) => (*timeout, OnTimeoutAction::Commit),
            AlterClusterPlanStrategy::UntilReady {
                timeout,
                on_timeout,
            } => (*timeout, on_timeout.unwrap_or(OnTimeoutAction::Rollback)),
        };
        let deadline = self
            .now()
            .saturating_add(u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX));

        // Build the durable write from `new_config`, which carries every field the
        // `ALTER` changed, then reset the config *shape* (size, replication factor,
        // availability zones, logging) back to the realized values: that transition
        // is deferred to the `reconfiguration` record and applied at cut-over. This
        // applies non-shape changes (`workload_class`, `schedule`,
        // `auto_scaling_strategy`, ...) immediately, matching the legacy path,
        // rather than silently dropping them. Any existing record is folded over by
        // the `record` we just built.
        let cluster = self.catalog.get_cluster(cluster_id);
        let cluster_name = cluster.name().to_string();
        let ClusterVariant::Managed(realized_now) = &cluster.config.variant else {
            return Err(AdapterError::Internal(
                "reshape_alter_cluster_managed requires a managed realized config".into(),
            ));
        };
        let realized_size = realized_now.size.clone();
        let realized_replication_factor = realized_now.replication_factor;
        let realized_availability_zones = realized_now.availability_zones.clone();
        let realized_logging = realized_now.logging.clone();
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
            deadline: deadline.into(),
            on_timeout,
            status,
        };

        let mut realized = new_config.clone();
        let ClusterVariant::Managed(realized_managed) = &mut realized.variant else {
            return Err(AdapterError::Internal(
                "reshape_alter_cluster_managed requires a managed target config".into(),
            ));
        };
        realized_managed.size = realized_size;
        realized_managed.replication_factor = realized_replication_factor;
        realized_managed.availability_zones = realized_availability_zones;
        realized_managed.logging = realized_logging;
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

    async fn finalize_alter_cluster_stage(
        &mut self,
        session: &Session,
        AlterClusterPlan {
            id: cluster_id,
            name: cluster_name,
            ..
        }: AlterClusterPlan,
        new_config: ClusterVariantManaged,
        workload_class: Option<String>,
    ) -> Result<StageResult<Box<ClusterStage>>, AdapterError> {
        let cluster = self.catalog.get_cluster(cluster_id);
        let mut ops = vec![];

        // Gather the ops to remove the non pending replicas
        // Also skip any billed_as free replicas
        let remove_replicas = cluster
            .replicas()
            .filter_map(|r| {
                if !r.config.location.pending() && !r.config.location.internal() {
                    Some(catalog::DropObjectInfo::ClusterReplica((
                        cluster_id.clone(),
                        r.replica_id,
                        ReplicaCreateDropReason::Manual,
                    )))
                } else {
                    None
                }
            })
            .collect();
        ops.push(catalog::Op::DropObjects(remove_replicas));

        // Gather the Ops to remove the "-pending" suffix from the name and set
        // pending to false
        let finalize_replicas: Vec<catalog::Op> = cluster
            .replicas()
            .filter_map(|r| {
                if r.config.location.pending() {
                    let cluster_ident = match Ident::new(cluster.name.clone()) {
                        Ok(id) => id,
                        Err(err) => {
                            return Some(Err(AdapterError::internal(
                                "Unexpected error parsing cluster name",
                                err,
                            )));
                        }
                    };
                    let replica_ident = match Ident::new(r.name.clone()) {
                        Ok(id) => id,
                        Err(err) => {
                            return Some(Err(AdapterError::internal(
                                "Unexpected error parsing replica name",
                                err,
                            )));
                        }
                    };
                    Some(Ok((cluster_ident, replica_ident, r)))
                } else {
                    None
                }
            })
            // Early collection is to handle errors from generating of the
            // Idents
            .collect::<Result<Vec<(Ident, Ident, &ClusterReplica)>, _>>()?
            .into_iter()
            .map(|(cluster_ident, replica_ident, replica)| {
                let mut new_replica_config = replica.config.clone();
                debug!("Promoting replica: {}", replica.name);
                match new_replica_config.location {
                    mz_controller::clusters::ReplicaLocation::Managed(ManagedReplicaLocation {
                        ref mut pending,
                        ..
                    }) => {
                        *pending = false;
                    }
                    mz_controller::clusters::ReplicaLocation::Unmanaged(_) => {}
                }

                let mut replica_ops = vec![];
                let to_name = replica.name.strip_suffix(PENDING_REPLICA_SUFFIX);
                if let Some(to_name) = to_name {
                    replica_ops.push(catalog::Op::RenameClusterReplica {
                        cluster_id: cluster_id.clone(),
                        replica_id: replica.replica_id.to_owned(),
                        name: QualifiedReplica {
                            cluster: cluster_ident,
                            replica: replica_ident,
                        },
                        to_name: to_name.to_owned(),
                    });
                }
                replica_ops.push(catalog::Op::UpdateClusterReplicaConfig {
                    cluster_id,
                    replica_id: replica.replica_id.to_owned(),
                    config: new_replica_config,
                });
                replica_ops
            })
            .flatten()
            .collect();

        ops.extend(finalize_replicas);

        // Add the Op to update the cluster state. A stale in-progress
        // reconfiguration record carried by this legacy write is retained as
        // cancelled, with the matching audit intent declared.
        let mut final_config = ClusterConfig {
            variant: ClusterVariant::Managed(new_config),
            workload_class: workload_class.clone(),
        };
        let reconfiguration_audit = cancel_carried_reconfiguration(&mut final_config);
        ops.push(Op::UpdateClusterConfig {
            id: cluster_id,
            name: cluster_name,
            config: final_config,
            reconfiguration_audit,
            burst_audit: None,
        });
        self.catalog_transact(Some(session), ops).await?;
        // Remove the cluster being altered from the ConnMeta
        // pending_cluster_alters BTreeSet
        self.active_conns
            .get_mut(session.conn_id())
            .expect("There must be an active connection")
            .pending_cluster_alters
            .remove(&cluster_id);

        Ok(StageResult::Response(ExecuteResponse::AlteredObject(
            ObjectType::Cluster,
        )))
    }

    async fn check_if_pending_replicas_hydrated_stage(
        &mut self,
        session: &Session,
        plan: AlterClusterPlan,
        new_config: ClusterVariantManaged,
        workload_class: Option<String>,
        timeout_time: Instant,
        on_timeout: OnTimeoutAction,
        validity: PlanValidity,
    ) -> Result<StageResult<Box<ClusterStage>>, AdapterError> {
        // wait and re-signal wait for hydrated if not hydrated
        let cluster = self.catalog.get_cluster(plan.id);
        let pending_replicas = cluster
            .replicas()
            .filter_map(|r| {
                if r.config.location.pending() {
                    Some(r.replica_id.clone())
                } else {
                    None
                }
            })
            .collect_vec();
        // Check For timeout
        if Instant::now() > timeout_time {
            // Timed out handle timeout action
            match on_timeout {
                OnTimeoutAction::Rollback => {
                    self.active_conns
                        .get_mut(session.conn_id())
                        .expect("There must be an active connection")
                        .pending_cluster_alters
                        .remove(&cluster.id);
                    self.drop_reconfiguration_replicas(btreeset!(cluster.id))
                        .await?;
                    return Err(AdapterError::AlterClusterTimeout);
                }
                OnTimeoutAction::Commit => {
                    let span = Span::current();
                    let poll_duration = self
                        .catalog
                        .system_config()
                        .cluster_alter_check_ready_interval()
                        .clone();
                    return Ok(StageResult::Handle(mz_ore::task::spawn(
                        || "Finalize Alter Cluster",
                        async move {
                            tokio::time::sleep(poll_duration).await;
                            let stage = ClusterStage::Finalize(AlterClusterFinalize {
                                validity,
                                plan,
                                new_config,
                                workload_class,
                            });
                            Ok(Box::new(stage))
                        }
                        .instrument(span),
                    )));
                }
            }
        }
        let compute_hydrated_fut = self
            .controller
            .compute
            .collections_hydrated_for_replicas(cluster.id, pending_replicas.clone(), [].into())
            .map_err(|e| AdapterError::internal("Failed to check hydration", e))?;

        let storage_hydrated = self
            .controller
            .storage
            .collections_hydrated_on_replicas(
                Some(pending_replicas.clone()),
                &cluster.id,
                &[].into(),
            )
            .map_err(|e| AdapterError::internal("Failed to check hydration", e))?;

        // Also require every pending replica to be online, in case it has no
        // objects that need hydration on it (e.g. a single-replica source).
        let replicas_online = pending_replicas.iter().all(|replica_id| {
            let status = self
                .cluster_replica_statuses
                .try_get_cluster_replica_statuses(cluster.id, *replica_id)
                .map(ClusterReplicaStatuses::cluster_replica_status);
            matches!(status, Some(ClusterStatus::Online))
        });

        let span = Span::current();
        Ok(StageResult::Handle(mz_ore::task::spawn(
            || "Alter Cluster: wait for hydrated",
            async move {
                let compute_hydrated = compute_hydrated_fut
                    .await
                    .map_err(|e| AdapterError::internal("Failed to check hydration", e))?;

                if compute_hydrated && storage_hydrated && replicas_online {
                    // We're done
                    Ok(Box::new(ClusterStage::Finalize(AlterClusterFinalize {
                        validity,
                        plan,
                        new_config: new_config.clone(),
                        workload_class: workload_class.clone(),
                    })))
                } else {
                    // Check later
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    let stage = ClusterStage::WaitForHydrated(AlterClusterWaitForHydrated {
                        validity,
                        plan,
                        new_config,
                        workload_class,
                        timeout_time,
                        on_timeout,
                    });
                    Ok(Box::new(stage))
                }
            }
            .instrument(span),
        )))
    }

    #[mz_ore::instrument(level = "debug")]
    pub(crate) async fn sequence_create_cluster(
        &mut self,
        session: &Session,
        CreateClusterPlan {
            name,
            variant,
            workload_class,
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
                self.sequence_create_managed_cluster(session, plan, id, name, ops)
                    .await
            }
            CreateClusterVariant::Unmanaged(plan) => {
                self.sequence_create_unmanaged_cluster(session, plan, id, name, ops)
                    .await
            }
        }
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
        cluster_name: String,
        mut ops: Vec<catalog::Op>,
    ) -> Result<ExecuteResponse, AdapterError> {
        tracing::debug!("sequence_create_managed_cluster");

        self.ensure_valid_azs(availability_zones.iter())?;

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
        // picking the id type from the owning cluster, so each replica's scoped
        // overrides can be folded into the create transaction below (the
        // overrides are keyed by the replica id). This mirrors how cluster and
        // item ids are allocated, so nothing allocates a replica id in-apply.
        let id_ts = self.get_catalog_write_ts().await;
        let replica_ids = self
            .catalog()
            .allocate_replica_ids(cluster_id, u64::from(replication_factor), id_ts)
            .await?;

        let cluster_ctx = ClusterScopeContext {
            id: cluster_id.to_string(),
            name: cluster_name.clone(),
            is_builtin: cluster_id.is_system(),
        };

        let mut replica_ctxs = Vec::new();
        for (replica_id, replica_name) in replica_ids
            .into_iter()
            .zip_eq((0..replication_factor).map(managed_cluster_replica_name))
        {
            let size_family = self.create_managed_cluster_replica_op(
                cluster_id,
                replica_id,
                replica_name.clone(),
                &compute,
                &size,
                &mut ops,
                if availability_zones.is_empty() {
                    None
                } else {
                    Some(availability_zones.as_ref())
                },
                false,
                *session.current_role_id(),
                ReplicaCreateDropReason::Manual,
            )?;
            replica_ctxs.push(ReplicaEvalContext {
                cluster_id,
                replica_id,
                cluster: cluster_ctx.clone(),
                replica: ReplicaScopeContext {
                    id: replica_id.to_string(),
                    name: replica_name,
                    is_builtin: cluster_id.is_system(),
                    size: size.clone(),
                    size_family,
                    cluster_id: cluster_id.to_string(),
                    cluster_name: cluster_name.clone(),
                },
            });
        }

        // Fold the new cluster's cluster-coherent and the replicas' replica-local
        // scoped overrides into the create transaction. Folding (rather than a
        // post-transact resolve) makes the committed diff drive the
        // replica-scoped controller push before create_replica, which
        // render-frozen flags require, and gives the new cluster its optimizer
        // overrides for its first plan.
        let cluster_eval = ClusterEvalContext {
            cluster_id,
            cluster: cluster_ctx,
        };
        if let Some(scoped_op) = self.scoped_overrides_create_op(&[cluster_eval], &replica_ctxs) {
            ops.push(scoped_op);
        }

        self.catalog_transact(Some(session), ops).await?;

        Ok(ExecuteResponse::CreatedCluster)
    }

    fn create_managed_cluster_replica_op(
        &self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        name: String,
        compute: &mz_sql::plan::ComputeReplicaConfig,
        size: &String,
        ops: &mut Vec<Op>,
        azs: Option<&[String]>,
        pending: bool,
        owner_id: RoleId,
        reason: ReplicaCreateDropReason,
    ) -> Result<String, AdapterError> {
        let location = mz_catalog::durable::ReplicaLocation::Managed {
            // Concretized below from the cluster config; this intermediate value
            // is discarded, so the list is left empty here.
            availability_zones: Vec::new(),
            billed_as: None,
            internal: false,
            size: size.clone(),
            pending,
        };

        let logging = if let Some(config) = compute.introspection {
            ReplicaLogging {
                log_logging: config.debugging,
                interval: Some(config.interval),
            }
        } else {
            ReplicaLogging::default()
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
                logging,
                arrangement_compression: compute.arrangement_compression,
            },
        };

        // The caller pre-allocates `replica_id` out-of-band via the durable
        // allocator, so nothing allocates a replica id in-apply.
        //
        // Extract the size family before `config` moves into the op, for the
        // replica's scoped eval context.
        let size_family = match &config.location {
            ReplicaLocation::Managed(location) => location.allocation.family().to_string(),
            // A managed replica always concretizes to a managed location.
            ReplicaLocation::Unmanaged(_) => {
                unreachable!("managed cluster replica has a managed location")
            }
        };

        ops.push(catalog::Op::CreateClusterReplica {
            cluster_id,
            replica_id,
            name,
            config,
            owner_id,
            reason,
        });
        Ok(size_family)
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
        cluster_name: String,
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
        // picking the id type from the owning cluster, so each replica's scoped
        // overrides can be folded into the create transaction below. This
        // mirrors how cluster and item ids are allocated, so nothing allocates
        // a replica id in-apply.
        let id_ts = self.get_catalog_write_ts().await;
        let replica_ids = self
            .catalog()
            .allocate_replica_ids(id, u64::cast_from(replicas.len()), id_ts)
            .await?;

        let cluster_ctx = ClusterScopeContext {
            id: id.to_string(),
            name: cluster_name.clone(),
            is_builtin: id.is_system(),
        };
        let mut replica_ctxs = Vec::new();

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

            // Only orchestrated (managed-location) replicas have a size and size
            // family, so only they carry replica-local overrides.
            if let ReplicaLocation::Managed(location) = &config.location {
                replica_ctxs.push(ReplicaEvalContext {
                    cluster_id: id,
                    replica_id,
                    cluster: cluster_ctx.clone(),
                    replica: ReplicaScopeContext {
                        id: replica_id.to_string(),
                        name: replica_name.clone(),
                        is_builtin: id.is_system(),
                        size: location.size.clone(),
                        size_family: location.allocation.family().to_string(),
                        cluster_id: id.to_string(),
                        cluster_name: cluster_name.clone(),
                    },
                });
            }

            ops.push(catalog::Op::CreateClusterReplica {
                cluster_id: id,
                replica_id,
                name: replica_name.clone(),
                config,
                owner_id: *session.current_role_id(),
                reason: ReplicaCreateDropReason::Manual,
            });
        }

        // Fold the new cluster's and replicas' scoped overrides into the create
        // transaction (see the managed path for rationale).
        let cluster_eval = ClusterEvalContext {
            cluster_id: id,
            cluster: cluster_ctx,
        };
        if let Some(scoped_op) = self.scoped_overrides_create_op(&[cluster_eval], &replica_ctxs) {
            ops.push(scoped_op);
        }

        self.catalog_transact(Some(session), ops).await?;

        Ok(ExecuteResponse::CreatedCluster)
    }

    #[mz_ore::instrument(level = "debug")]
    pub(crate) async fn sequence_create_cluster_replica(
        &mut self,
        session: &Session,
        CreateClusterReplicaPlan {
            name,
            cluster_id,
            config,
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
        let is_builtin = cluster_id.is_system();

        // Pre-allocate the replica id out-of-band via the durable allocator,
        // picking the id type from the target cluster, which may be a system
        // cluster, so the replica's scoped overrides can be folded into the same
        // transaction. The overrides are keyed by replica id, and the
        // replica-scoped controller push must run before `create_replica`. This
        // mirrors how cluster and item ids are allocated, so nothing allocates a
        // replica id in-apply.
        let id_ts = self.get_catalog_write_ts().await;
        let replica_id = self
            .catalog()
            .allocate_replica_ids(cluster_id, 1, id_ts)
            .await?
            .into_element();

        // Build the replica's eval context from the plan before `config` moves
        // into the op. Only managed replicas have a size (and size family).
        let replica_ctx = match &config.location {
            ReplicaLocation::Managed(location) => Some(ReplicaEvalContext {
                cluster_id,
                replica_id,
                cluster: ClusterScopeContext {
                    id: cluster_id.to_string(),
                    name: cluster_name.clone(),
                    is_builtin,
                },
                replica: ReplicaScopeContext {
                    id: replica_id.to_string(),
                    name: name.to_string(),
                    is_builtin,
                    size: location.size.clone(),
                    size_family: location.allocation.family().to_string(),
                    cluster_id: cluster_id.to_string(),
                    cluster_name,
                },
            }),
            ReplicaLocation::Unmanaged(_) => None,
        };

        let mut ops = vec![catalog::Op::CreateClusterReplica {
            cluster_id,
            replica_id,
            name: name.clone(),
            config,
            owner_id,
            reason: ReplicaCreateDropReason::Manual,
        }];

        // The cluster already exists, so only this replica's local overrides
        // need resolving. Fold them into the create transaction so the
        // replica-scoped push runs before `create_replica`.
        if let Some(replica_ctx) = replica_ctx {
            if let Some(scoped_op) = self.scoped_overrides_create_op(&[], &[replica_ctx]) {
                ops.push(scoped_op);
            }
        }

        self.catalog_transact(Some(session), ops).await?;

        Ok(ExecuteResponse::CreatedClusterReplica)
    }

    /// When this is called by the automated cluster scheduling, `scheduling_decision_reason` should
    /// contain information on why is a cluster being turned On/Off. It will be forwarded to the
    /// `details` field of the audit log event that records creating or dropping replicas.
    ///
    /// # Panics
    ///
    /// Panics if the identified cluster is not a managed cluster.
    /// Panics if `new_config` is not a configuration for a managed cluster.
    pub(crate) async fn sequence_alter_cluster_managed_to_managed(
        &mut self,
        session: Option<&Session>,
        cluster_id: ClusterId,
        new_config: ClusterConfig,
        reason: ReplicaCreateDropReason,
        strategy: AlterClusterPlanStrategy,
    ) -> Result<NeedsFinalization, AdapterError> {
        let cluster = self.catalog.get_cluster(cluster_id);
        let name = cluster.name().to_string();
        let owner_id = cluster.owner_id();

        let mut ops = vec![];
        let mut finalization_needed = NeedsFinalization::No;

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
        // Clone the existing managed config out of the cluster so the immutable
        // catalog borrow can be released before the out-of-band replica id
        // allocation below, which needs mutable access to self.
        let size = size.clone();
        let availability_zones = availability_zones.clone();
        let logging = logging.clone();
        let arrangement_compression = *arrangement_compression;
        let replication_factor = *replication_factor;
        let ClusterVariant::Managed(new_managed) = &new_config.variant else {
            panic!("expected new managed cluster config");
        };
        let ClusterVariantManaged {
            size: new_size,
            replication_factor: new_replication_factor,
            availability_zones: new_availability_zones,
            logging: new_logging,
            arrangement_compression: new_arrangement_compression,
            optimizer_feature_overrides: _,
            schedule: _,
            auto_scaling_strategy: new_auto_scaling_strategy,
            reconfiguration: _,
            burst: _,
        } = new_managed;

        let role_id = session.map(|s| s.role_metadata().current_role);
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

        // check for active updates
        if cluster.replicas().any(|r| r.config.location.pending()) {
            return Err(AlterClusterWhilePendingReplicas);
        }

        // Resolve existing replica ids by name before releasing the catalog
        // borrow, so the drop branches below can build their ops without it.
        let replica_id_by_name: BTreeMap<String, ReplicaId> = cluster
            .replicas()
            .map(|r| (r.name.clone(), r.replica_id))
            .collect();

        let compute = mz_sql::plan::ComputeReplicaConfig {
            introspection: new_logging
                .interval
                .map(|interval| ComputeReplicaIntrospectionConfig {
                    debugging: new_logging.log_logging,
                    interval,
                }),
            arrangement_compression: *new_arrangement_compression,
        };

        // Eagerly validate the `max_replicas_per_cluster` limit.
        // `catalog_transact` will do this validation too, but allocating
        // replica IDs is expensive enough that we need to do this validation
        // before allocating replica IDs. See database-issues#6046.
        if *new_replication_factor > replication_factor {
            if cluster_id.is_user() {
                self.validate_resource_limit(
                    usize::cast_from(replication_factor),
                    i64::from(*new_replication_factor) - i64::from(replication_factor),
                    SystemVars::max_replicas_per_cluster,
                    "cluster replica",
                    MAX_REPLICAS_PER_CLUSTER.name(),
                )?;
            }
        }

        // When the controller owns the managed replica set (master gate on, user
        // cluster), a non-record change reaching this path is replication-factor
        // only. Config-shape changes (size/logging/AZ) are reshaped into a durable
        // reconfiguration record before they get here. The controller reconciles
        // the replica set to the realized config's new count on its next tick, so
        // we update only the realized config and emit no create/drop here. Doing
        // both fights the controller. It derives replica names from the observed
        // set, so an adapter create by canonical `rN` can collide with a
        // controller-chosen name, and an adapter drop by canonical `rN` can miss a
        // churned one. With the gate off (or a system cluster, which the
        // controller never owns) the legacy path below still does the create/drop
        // directly.
        let controller_owns = ENABLE_CLUSTER_CONTROLLER
            .get(self.catalog().system_config().dyncfgs())
            && cluster_id.is_user();

        // Count exactly as many replica ids as the branches below consume. The
        // config-changed branches recreate all replicas. A pure scale-up creates
        // only the delta. Scale-down and no-op create none. A controller-owned
        // alter emits no create/drop at all, so it must not allocate. Allocating
        // there burns those ids durably and throws them away. The controller
        // allocates its own when it materializes the change.
        let config_changed = new_managed.replica_config_shape()
            != ManagedReplicaConfigShape::new(
                &size,
                &availability_zones,
                &logging,
                arrangement_compression,
            );
        let needed_replica_ids = if controller_owns {
            0
        } else if config_changed {
            *new_replication_factor
        } else if *new_replication_factor > replication_factor {
            *new_replication_factor - replication_factor
        } else {
            0
        };
        // Allocate the replica ids out-of-band via the durable allocator, only
        // after the eager limit validation above so a rejected alter allocates
        // nothing. Pick the id type from the target cluster, which may be a
        // system cluster. This mirrors how cluster and item ids are allocated,
        // so nothing allocates a replica id in-apply. Fetch the catalog write
        // timestamp lazily here, since it needs mutable access to self (the
        // cluster borrow above is already released) and scale-down, no-op, and
        // automated scheduling turn-off alters must not pay an oracle
        // round-trip just to allocate nothing.
        let mut new_replica_ids = if needed_replica_ids > 0 {
            let id_ts = self.get_catalog_write_ts().await;
            let ids = self
                .catalog()
                .allocate_replica_ids(cluster_id, u64::from(needed_replica_ids), id_ts)
                .await?;
            ids.into_iter()
        } else {
            Vec::<ReplicaId>::new().into_iter()
        };

        // Collect an eval context for each replica recreated below, so the alter
        // transaction folds the replicas' replica-scoped overrides the same way
        // the create paths do. ALTER CLUSTER SET (SIZE ...) to a different size
        // family flips size-family-keyed render-frozen flags, so the override
        // must reach the controller before the recreated replica renders. Only
        // the replica scope is folded. The cluster already exists and its
        // cluster-scoped overrides are unaffected by this alter.
        let cluster_ctx = ClusterScopeContext {
            id: cluster_id.to_string(),
            name: name.clone(),
            is_builtin: cluster_id.is_system(),
        };
        let mut replica_ctxs = Vec::new();

        if controller_owns {
            // Defer all replica create/drop to the controller. Only the realized
            // config update below is applied here.
        } else if config_changed {
            self.ensure_valid_azs(new_availability_zones.iter())?;
            // If we're not doing a zero-downtime reconfig tear down all
            // replicas, create new ones else create the pending replicas and
            // return early asking for finalization
            match strategy {
                AlterClusterPlanStrategy::None => {
                    let replica_ids_and_reasons = (0..replication_factor)
                        .map(managed_cluster_replica_name)
                        .filter_map(|name| replica_id_by_name.get(&name).copied())
                        .map(|replica_id| {
                            catalog::DropObjectInfo::ClusterReplica((
                                cluster_id,
                                replica_id,
                                reason.clone(),
                            ))
                        })
                        .collect();
                    ops.push(catalog::Op::DropObjects(replica_ids_and_reasons));
                    for replica_name in
                        (0..*new_replication_factor).map(managed_cluster_replica_name)
                    {
                        // The replica id is pre-allocated above like the create
                        // paths so its scoped overrides can be folded below.
                        let replica_id = new_replica_ids
                            .next()
                            .expect("pre-allocated enough replica ids");
                        let size_family = self.create_managed_cluster_replica_op(
                            cluster_id,
                            replica_id,
                            replica_name.clone(),
                            &compute,
                            new_size,
                            &mut ops,
                            Some(new_availability_zones.as_ref()),
                            false,
                            owner_id,
                            reason.clone(),
                        )?;
                        replica_ctxs.push(ReplicaEvalContext {
                            cluster_id,
                            replica_id,
                            cluster: cluster_ctx.clone(),
                            replica: ReplicaScopeContext {
                                id: replica_id.to_string(),
                                name: replica_name,
                                is_builtin: cluster_id.is_system(),
                                size: new_size.clone(),
                                size_family,
                                cluster_id: cluster_id.to_string(),
                                cluster_name: cluster_ctx.name.clone(),
                            },
                        });
                    }
                }
                AlterClusterPlanStrategy::For(_) | AlterClusterPlanStrategy::UntilReady { .. } => {
                    for replica_name in
                        (0..*new_replication_factor).map(managed_cluster_replica_name)
                    {
                        let replica_name = format!("{replica_name}{PENDING_REPLICA_SUFFIX}");
                        let replica_id = new_replica_ids
                            .next()
                            .expect("pre-allocated enough replica ids");
                        let size_family = self.create_managed_cluster_replica_op(
                            cluster_id,
                            replica_id,
                            replica_name.clone(),
                            &compute,
                            new_size,
                            &mut ops,
                            Some(new_availability_zones.as_ref()),
                            true,
                            owner_id,
                            reason.clone(),
                        )?;
                        replica_ctxs.push(ReplicaEvalContext {
                            cluster_id,
                            replica_id,
                            cluster: cluster_ctx.clone(),
                            replica: ReplicaScopeContext {
                                id: replica_id.to_string(),
                                name: replica_name,
                                is_builtin: cluster_id.is_system(),
                                size: new_size.clone(),
                                size_family,
                                cluster_id: cluster_id.to_string(),
                                cluster_name: cluster_ctx.name.clone(),
                            },
                        });
                    }
                    finalization_needed = NeedsFinalization::Yes;
                }
            }
        } else if *new_replication_factor < replication_factor {
            // Adjust replica count down
            let replica_ids = (*new_replication_factor..replication_factor)
                .map(managed_cluster_replica_name)
                .filter_map(|name| replica_id_by_name.get(&name).copied())
                .map(|replica_id| {
                    catalog::DropObjectInfo::ClusterReplica((
                        cluster_id,
                        replica_id,
                        reason.clone(),
                    ))
                })
                .collect();
            ops.push(catalog::Op::DropObjects(replica_ids));
        } else if *new_replication_factor > replication_factor {
            // Adjust replica count up
            for replica_name in
                (replication_factor..*new_replication_factor).map(managed_cluster_replica_name)
            {
                let replica_id = new_replica_ids
                    .next()
                    .expect("pre-allocated enough replica ids");
                let size_family = self.create_managed_cluster_replica_op(
                    cluster_id,
                    replica_id,
                    replica_name.clone(),
                    &compute,
                    new_size,
                    &mut ops,
                    // AVAILABILITY ZONES hasn't changed, so existing replicas don't need to be
                    // rescheduled.
                    Some(new_availability_zones.as_ref()),
                    false,
                    owner_id,
                    reason.clone(),
                )?;
                replica_ctxs.push(ReplicaEvalContext {
                    cluster_id,
                    replica_id,
                    cluster: cluster_ctx.clone(),
                    replica: ReplicaScopeContext {
                        id: replica_id.to_string(),
                        name: replica_name,
                        is_builtin: cluster_id.is_system(),
                        size: new_size.clone(),
                        size_family,
                        cluster_id: cluster_id.to_string(),
                        cluster_name: cluster_ctx.name.clone(),
                    },
                });
            }
        }

        // If finalization is needed, finalization should update the cluster
        // config. Otherwise the config write happens here. With the controller
        // owning the cluster, a record still in progress belongs to a live,
        // converging reconfiguration this write didn't touch: carry it through
        // untouched. Without (gate off, or a system cluster), such a record is
        // orphaned, so retain it as cancelled with the matching audit intent
        // rather than risk a bogus revival if the gate comes back on.
        //
        // NOTE: `handle_scheduling_decisions` also calls this function and
        // bypasses the sequencer's guards. It runs only while the controller
        // gate is off, where the cancel-carried write below retires any
        // in-progress record instead of leaving it behind for a controller
        // that is not running.
        match finalization_needed {
            NeedsFinalization::No => {
                let mut new_config = new_config;
                let reconfiguration_audit = if controller_owns {
                    None
                } else {
                    cancel_carried_reconfiguration(&mut new_config)
                };
                ops.push(catalog::Op::UpdateClusterConfig {
                    id: cluster_id,
                    name: name.clone(),
                    config: new_config,
                    reconfiguration_audit,
                    burst_audit: None,
                });
            }
            NeedsFinalization::Yes => {}
        }

        // Fold the recreated replicas' replica-scoped overrides into the same
        // transaction, so the committed diff drives the replica-scoped controller
        // push before create_replica. Render-frozen flags (chosen at
        // arrangement-build time) require the override to land before the replica
        // renders. Scale-down and no-op alters recreate no replicas, so this is
        // empty and folds nothing.
        if let Some(scoped_op) = self.scoped_overrides_create_op(&[], &replica_ctxs) {
            ops.push(scoped_op);
        }

        self.catalog_transact(session, ops).await?;
        Ok(finalization_needed)
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

fn managed_cluster_replica_name(index: u32) -> String {
    format!("r{}", index + 1)
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

/// Retains a stale in-progress reconfiguration record carried by a legacy-path
/// config write as cancelled, returning the audit intent to declare with the
/// write.
///
/// The legacy ALTER paths (controller gate off) change the realized config
/// directly and know nothing about reconfiguration records. Nothing on those
/// paths ever settles a record, and carrying an in-progress one forward invites
/// a bogus revival, up to a forced cut-over to an obsolete target, if the gate
/// is turned back on later. A record can only be in progress here if it was
/// written while the gate was on.
fn cancel_carried_reconfiguration(config: &mut ClusterConfig) -> Option<ReconfigurationAudit> {
    let ClusterVariant::Managed(managed) = &mut config.variant else {
        return None;
    };
    let record = managed.reconfiguration.as_mut()?;
    if !record.is_in_progress() {
        return None;
    }
    record.status = ReconfigurationStatus::Cancelled;
    Some(ReconfigurationAudit::Cancelled)
}

/// Whether an `ALTER` statement sets a replica config shape dimension (`SIZE`,
/// `AVAILABILITY ZONES`, or either `INTROSPECTION` option), the changes that
/// need a durable `reconfiguration` record and a hydrate-overlap.
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
/// Replication factor folds the same way, but only matters for the
/// nothing-in-flight case: a change to it while a reconfiguration is in
/// flight is refused before an `ALTER` reaches here, so
/// `unchanged.replication_factor` is always `true` when `in_flight` is
/// `Some`.
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

/// The type of finalization needed after an
/// operation such as alter_cluster_managed_to_managed.
#[derive(PartialEq)]
pub(crate) enum NeedsFinalization {
    /// Wait for the provided duration before finalizing
    Yes,
    No,
}

#[cfg(test)]
mod tests {
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
        // A 200cc size change is in flight. A later rf-only ALTER must NOT revert
        // the in-flight size/AZ/logging back to the realized (100cc) values that
        // `new_target` carries for the dimensions the ALTER left unchanged.
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
}
