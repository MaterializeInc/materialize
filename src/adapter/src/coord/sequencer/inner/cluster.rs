// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use itertools::Itertools;
use maplit::btreeset;
use mz_catalog::builtin::BUILTINS;
use mz_catalog::memory::objects::{
    ClusterConfig, ClusterReplica, ClusterVariant, ClusterVariantManaged, ReconfigurationState,
    ReconfigurationTarget,
};
use mz_compute_types::config::ComputeReplicaConfig;
use mz_controller::clusters::{
    ManagedReplicaLocation, ReplicaConfig, ReplicaLocation, ReplicaLogging,
};
use mz_controller_types::{ClusterId, DEFAULT_REPLICA_LOGGING_INTERVAL};
use mz_ore::cast::CastFrom;
use mz_ore::instrument;
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
use mz_sql::session::vars::{MAX_REPLICAS_PER_CLUSTER, SystemVars, Var};
use tracing::{Instrument, Span, debug};

use mz_adapter_types::dyncfgs::{
    DEFAULT_CLUSTER_RECONFIGURATION_TIMEOUT, ENABLE_BACKGROUND_ALTER_CLUSTER,
    ENABLE_CLUSTER_CONTROLLER,
};

use super::return_if_err;
use crate::AdapterError::AlterClusterWhilePendingReplicas;
use crate::catalog::{self, Op, ReplicaCreateDropReason};
use crate::coord::{
    AlterCluster, AlterClusterAwaitReconfiguration, AlterClusterFinalize,
    AlterClusterWaitForHydrated, ClusterStage, Coordinator, Message, PlanValidity, StageResult,
    Staged,
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
            Self::AwaitReconfiguration(stage) => coord.await_reconfiguration_stage(
                stage.validity,
                stage.cluster_id,
                stage.past_deadline_grace_used,
            ),
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
            self.catalog().transient_revision(),
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
                replication_factor,
                optimizer_feature_overrides: _,
                schedule,
                auto_scaling_strategy: _,
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
                if !matches!(options.replication_factor, Unchanged) {
                    coord_bail!("Cannot change REPLICATION FACTOR of unmanaged clusters");
                }
            }
        }

        match &options.workload_class {
            Set(wc) => new_config.workload_class.clone_from(wc),
            Reset => new_config.workload_class = None,
            Unchanged => {}
        }

        // A no-op `ALTER` short-circuits — but only when no reconfiguration is in
        // flight. When the controller owns an in-flight reconfiguration, an `ALTER`
        // that leaves the realized config unchanged is not a no-op: it re-targets
        // (folds onto) the in-flight record. This is how an `ALTER` back to the
        // realized shape cancels an in-flight reconfiguration — so it must reach the
        // reshape path below rather than early-returning here. (`config` carries the
        // in-flight record, so `new_config == config` holds for such an `ALTER`.)
        // The controller owns only *user* managed clusters (see `ManagedClusterIds`
        // in cluster_controller.rs and `controller_owns` in the managed-to-managed
        // path below). A system/builtin cluster is never converged by the
        // controller, so it must not be reshaped into a durable reconfiguration
        // record nobody would cut over — it takes the direct realized-config path
        // below, exactly as it does with the controller off.
        let cluster_controller_owns = ENABLE_CLUSTER_CONTROLLER
            .get(self.catalog().system_config().dyncfgs())
            && cluster_id.is_user();
        let reconfiguration_in_flight = matches!(
            &config.variant,
            Managed(managed) if managed.reconfiguration.is_some()
        );
        if new_config == config && !(cluster_controller_owns && reconfiguration_in_flight) {
            return Ok(StageResult::Response(ExecuteResponse::AlteredObject(
                ObjectType::Cluster,
            )));
        }

        // When the cluster controller owns the replica set, a config-shape change
        // (or any change while a reconfiguration is already in flight) is reshaped
        // into a durable `reconfiguration` record rather than driven by the legacy
        // 3-stage machine; the controller converges on it. Non-shape changes with
        // no record in flight fall through to the realized-config update below.
        if cluster_controller_owns {
            if let (Managed(old_managed), Managed(new_managed)) =
                (&config.variant, &new_config.variant)
            {
                if self.reconfiguration_needs_record(old_managed, new_managed) {
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
                                on_timeout: on_timeout.to_owned(),
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

    /// Whether a managed→managed `ALTER` must be reshaped into a durable
    /// `reconfiguration` record (controller-driven) rather than applied to the
    /// realized config directly.
    ///
    /// True when the change touches a replica's **config shape** (`SIZE`, logging,
    /// or `AVAILABILITY ZONES`) — which needs a hydrate-overlap — or when a
    /// reconfiguration is already in flight, in which case every further `ALTER`
    /// folds into the existing record (re-targets it and refreshes its deadline)
    /// rather than racing a direct config write against the in-flight transition.
    ///
    /// The fold is an **overlay on the in-flight target, not the realized config**:
    /// a dimension the `ALTER` set replaces the record's target for that dimension,
    /// a dimension left unset keeps the in-flight target's value (see
    /// [`Coordinator::reshape_alter_cluster_managed`]). This is why a record in
    /// flight forces the record path even for a change that, against the realized
    /// config alone, would look like a no-shape (e.g. rf-only) update: applying it
    /// to the realized config would discard the in-flight target.
    fn reconfiguration_needs_record(
        &self,
        old: &ClusterVariantManaged,
        new: &ClusterVariantManaged,
    ) -> bool {
        old.reconfiguration.is_some()
            || new.size != old.size
            || new.logging != old.logging
            || new.availability_zones != old.availability_zones
    }

    /// Reshape a managed→managed `ALTER` into a durable `reconfiguration` record.
    ///
    /// Writes (or folds into) the `reconfiguration` record carrying the full target
    /// config shape and a deadline, leaving the realized config in place (only
    /// `workload_class`, which needs no hydrate-overlap, is applied immediately).
    /// The controller converges the replica set onto the target and cuts the
    /// realized config over at hydration.
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
    /// drops the whole in-flight target set and retains the record as a tombstone
    /// (the realized config never advanced, so the cluster keeps its old shape);
    /// `COMMIT` cuts the realized config over to the not-fully-hydrated target and
    /// clears the record. Success always takes precedence — a target that hydrates
    /// before the deadline cuts over regardless of the action.
    ///
    /// With `enable_background_alter_cluster` on, the statement returns
    /// immediately. With it off, the session blocks on a wait-shim
    /// ([`ClusterStage::AwaitReconfiguration`]) that polls the record until it
    /// clears (success) or its deadline passes (timeout), preserving today's
    /// foreground UX over the same durable mechanism.
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
            ClusterVariant::Managed(managed) => managed.reconfiguration.clone(),
            ClusterVariant::Unmanaged => None,
        };
        let new_target = ReconfigurationTarget {
            size: new_managed.size.clone(),
            replication_factor: new_managed.replication_factor,
            availability_zones: new_managed.availability_zones.clone(),
            logging: new_managed.logging.clone(),
        };
        let unchanged = ReconfigurationDimensionsUnchanged {
            size: matches!(options.size, Unchanged),
            replication_factor: matches!(options.replication_factor, Unchanged),
            availability_zones: matches!(options.availability_zones, Unchanged),
            // Logging is driven by two options; "unchanged" only if neither was set.
            logging: matches!(options.introspection_debugging, Unchanged)
                && matches!(options.introspection_interval, Unchanged),
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
        )?;
        self.ensure_valid_azs(target.availability_zones.iter())?;

        // Resolve the deadline and the on-timeout action from the existing
        // `WITH (WAIT ...)` surface; both are written relative to the current time
        // so they survive session disconnect and restart. Unlike the target — which
        // folds per-dimension onto the in-flight one — the deadline and `on_timeout`
        // are replaced wholesale by the latest `ALTER`'s `WAIT` clause (they are
        // resolved fresh here, not merged), so re-issuing an `ALTER` with a
        // different `ON TIMEOUT` overwrites the prior action.
        //   - no `WAIT`         -> the system-default timeout and the implicit
        //                          `on_timeout` default (`ROLLBACK`).
        //   - `WAIT FOR`        -> sugar for `ON TIMEOUT COMMIT` (cut over at the
        //                          deadline regardless of hydration).
        //   - `WAIT UNTIL READY -> the explicit `TIMEOUT` / `ON TIMEOUT`.
        let (timeout, on_timeout) = match strategy {
            AlterClusterPlanStrategy::None => (
                DEFAULT_CLUSTER_RECONFIGURATION_TIMEOUT
                    .get(self.catalog().system_config().dyncfgs()),
                OnTimeoutAction::default(),
            ),
            AlterClusterPlanStrategy::For(timeout) => (*timeout, OnTimeoutAction::Commit),
            AlterClusterPlanStrategy::UntilReady {
                timeout,
                on_timeout,
            } => (*timeout, *on_timeout),
        };
        let deadline = self.now() + u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX);
        let record = ReconfigurationState {
            target,
            deadline: deadline.into(),
            on_timeout,
        };

        // Write the record onto the *realized* config (which is left otherwise
        // untouched), folding over any existing record. `workload_class` needs no
        // overlap, so it is applied immediately.
        let cluster = self.catalog.get_cluster(cluster_id);
        let cluster_name = cluster.name().to_string();
        let mut realized = cluster.config.clone();
        let ClusterVariant::Managed(realized_managed) = &mut realized.variant else {
            return Err(AdapterError::Internal(
                "reshape_alter_cluster_managed requires a managed realized config".into(),
            ));
        };
        realized_managed.reconfiguration = Some(record);
        realized.workload_class = new_config.workload_class.clone();

        self.catalog_transact(
            Some(session),
            vec![Op::UpdateClusterConfig {
                id: cluster_id,
                name: cluster_name,
                config: realized,
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

        // Foreground wait-shim: poll the durable record until it clears or times
        // out. The reconfiguration continues in the background regardless of the
        // session, so a disconnect during the wait no longer aborts it.
        Ok(StageResult::Immediate(Box::new(
            ClusterStage::AwaitReconfiguration(AlterClusterAwaitReconfiguration {
                validity,
                cluster_id,
                past_deadline_grace_used: false,
            }),
        )))
    }

    /// One step of the foreground wait-shim: read the durable `reconfiguration`
    /// record and either complete, error on timeout, or re-poll after the
    /// configured interval.
    ///
    /// Completion is the absence of the record (the controller cleared it at
    /// cut-over). A record whose deadline has passed is a candidate timeout,
    /// not a conclusive one, so we grant one post-deadline grace re-poll
    /// before erroring. The reconfiguration itself is durable and continues
    /// (or has already settled) independently of this session.
    fn await_reconfiguration_stage(
        &self,
        validity: PlanValidity,
        cluster_id: ClusterId,
        past_deadline_grace_used: bool,
    ) -> Result<StageResult<Box<ClusterStage>>, AdapterError> {
        let Some(cluster) = self.catalog().try_get_cluster(cluster_id) else {
            // The cluster was dropped out from under the reconfiguration; nothing
            // to wait on.
            return Ok(StageResult::Response(ExecuteResponse::AlteredObject(
                ObjectType::Cluster,
            )));
        };
        let record = match &cluster.config.variant {
            ClusterVariant::Managed(managed) => managed.reconfiguration.clone(),
            ClusterVariant::Unmanaged => None,
        };

        match record {
            // Cleared: the controller cut over. Done.
            None => Ok(StageResult::Response(ExecuteResponse::AlteredObject(
                ObjectType::Cluster,
            ))),
            Some(record) => {
                let past_deadline = mz_repr::Timestamp::from(self.now()) > record.deadline;
                if past_deadline && past_deadline_grace_used {
                    // The record is still present after the grace re-poll: the
                    // controller could not cut over in time and has tombstoned the
                    // record. Surface a timeout to the session; the durable
                    // tombstone records the abandoned target.
                    return Err(AdapterError::AlterClusterTimeout);
                }
                // Past the deadline for the first time, re-poll once more so the
                // controller's success-precedence cut-over can clear the record;
                // before the deadline this flag stays unset.
                let past_deadline_grace_used = past_deadline_grace_used || past_deadline;
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
                                past_deadline_grace_used,
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

        // Add the Op to update the cluster state
        ops.push(Op::UpdateClusterConfig {
            id: cluster_id,
            name: cluster_name,
            config: ClusterConfig {
                variant: ClusterVariant::Managed(new_config),
                workload_class: workload_class.clone(),
            },
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
            .collections_hydrated_on_replicas(Some(pending_replicas), &cluster.id, &[].into())
            .map_err(|e| AdapterError::internal("Failed to check hydration", e))?;

        let span = Span::current();
        Ok(StageResult::Handle(mz_ore::task::spawn(
            || "Alter Cluster: wait for hydrated",
            async move {
                let compute_hydrated = compute_hydrated_fut
                    .await
                    .map_err(|e| AdapterError::internal("Failed to check hydration", e))?;

                if compute_hydrated && storage_hydrated {
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
                    replication_factor: plan.replication_factor,
                    optimizer_feature_overrides: plan.optimizer_feature_overrides.clone(),
                    schedule: plan.schedule.clone(),
                    auto_scaling_strategy: None,
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
        }: CreateClusterManagedPlan,
        cluster_id: ClusterId,
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
        )?;

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

        for replica_name in (0..replication_factor).map(managed_cluster_replica_name) {
            self.create_managed_cluster_replica_op(
                cluster_id,
                replica_name,
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
        }

        self.catalog_transact(Some(session), ops).await?;

        Ok(ExecuteResponse::CreatedCluster)
    }

    fn create_managed_cluster_replica_op(
        &self,
        cluster_id: ClusterId,
        name: String,
        compute: &mz_sql::plan::ComputeReplicaConfig,
        size: &String,
        ops: &mut Vec<Op>,
        azs: Option<&[String]>,
        pending: bool,
        owner_id: RoleId,
        reason: ReplicaCreateDropReason,
    ) -> Result<(), AdapterError> {
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
            )?,
            compute: ComputeReplicaConfig { logging },
        };

        ops.push(catalog::Op::CreateClusterReplica {
            cluster_id,
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

        for (replica_name, replica_config) in replicas {
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
                )?,
                compute: ComputeReplicaConfig { logging },
            };

            ops.push(catalog::Op::CreateClusterReplica {
                cluster_id: id,
                name: replica_name.clone(),
                config,
                owner_id: *session.current_role_id(),
                reason: ReplicaCreateDropReason::Manual,
            });
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
            )?,
            compute: ComputeReplicaConfig { logging },
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

        // Replicas have the same owner as their cluster.
        let owner_id = cluster.owner_id();
        let op = catalog::Op::CreateClusterReplica {
            cluster_id,
            name: name.clone(),
            config,
            owner_id,
            reason: ReplicaCreateDropReason::Manual,
        };

        self.catalog_transact(Some(session), vec![op]).await?;

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
            replication_factor,
            optimizer_feature_overrides: _,
            schedule: _,
            auto_scaling_strategy: _,
            reconfiguration: _,
            burst: _,
        }) = &cluster.config.variant
        else {
            panic!("expected existing managed cluster config");
        };
        let ClusterVariant::Managed(ClusterVariantManaged {
            size: new_size,
            replication_factor: new_replication_factor,
            availability_zones: new_availability_zones,
            logging: new_logging,
            optimizer_feature_overrides: _,
            schedule: _,
            auto_scaling_strategy: _,
            reconfiguration: _,
            burst: _,
        }) = &new_config.variant
        else {
            panic!("expected new managed cluster config");
        };

        let role_id = session.map(|s| s.role_metadata().current_role);
        self.catalog.ensure_valid_replica_size(
            &self.catalog().get_role_allowed_cluster_sizes(&role_id),
            new_size,
        )?;

        // check for active updates
        if cluster.replicas().any(|r| r.config.location.pending()) {
            return Err(AlterClusterWhilePendingReplicas);
        }

        let compute = mz_sql::plan::ComputeReplicaConfig {
            introspection: new_logging
                .interval
                .map(|interval| ComputeReplicaIntrospectionConfig {
                    debugging: new_logging.log_logging,
                    interval,
                }),
        };

        // Eagerly validate the `max_replicas_per_cluster` limit.
        // `catalog_transact` will do this validation too, but allocating
        // replica IDs is expensive enough that we need to do this validation
        // before allocating replica IDs. See database-issues#6046.
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

        // When the controller owns the managed replica set (master gate on, user
        // cluster), a non-record change reaching this path is replication-factor
        // only: config-shape changes (size/logging/AZ) are reshaped into a durable
        // reconfiguration record before they get here. The controller reconciles
        // the replica set to the realized config's new count on its next tick, so
        // we update only the realized config and emit no create/drop here. Doing
        // both would fight the controller: it derives replica names from the
        // observed set, so an adapter create by canonical `rN` would risk
        // colliding with a controller-chosen name and an adapter drop by canonical
        // `rN` would miss a churned one. With the gate off (or a system cluster,
        // which the controller never owns) the legacy path below still does the
        // create/drop directly.
        let controller_owns = ENABLE_CLUSTER_CONTROLLER
            .get(self.catalog().system_config().dyncfgs())
            && cluster_id.is_user();

        if controller_owns {
            // Defer all replica create/drop to the controller; only the realized
            // config update below is applied here.
        } else if new_size != size
            || new_availability_zones != availability_zones
            || new_logging != logging
        {
            self.ensure_valid_azs(new_availability_zones.iter())?;
            // If we're not doing a zero-downtime reconfig tear down all
            // replicas, create new ones else create the pending replicas and
            // return early asking for finalization
            match strategy {
                AlterClusterPlanStrategy::None => {
                    let replica_ids_and_reasons = (0..*replication_factor)
                        .map(managed_cluster_replica_name)
                        .filter_map(|name| cluster.replica_id(&name))
                        .map(|replica_id| {
                            catalog::DropObjectInfo::ClusterReplica((
                                cluster.id(),
                                replica_id,
                                reason.clone(),
                            ))
                        })
                        .collect();
                    ops.push(catalog::Op::DropObjects(replica_ids_and_reasons));
                    for name in (0..*new_replication_factor).map(managed_cluster_replica_name) {
                        self.create_managed_cluster_replica_op(
                            cluster_id,
                            name.clone(),
                            &compute,
                            new_size,
                            &mut ops,
                            Some(new_availability_zones.as_ref()),
                            false,
                            owner_id,
                            reason.clone(),
                        )?;
                    }
                }
                AlterClusterPlanStrategy::For(_) | AlterClusterPlanStrategy::UntilReady { .. } => {
                    for name in (0..*new_replication_factor).map(managed_cluster_replica_name) {
                        let name = format!("{name}{PENDING_REPLICA_SUFFIX}");
                        self.create_managed_cluster_replica_op(
                            cluster_id,
                            name.clone(),
                            &compute,
                            new_size,
                            &mut ops,
                            Some(new_availability_zones.as_ref()),
                            true,
                            owner_id,
                            reason.clone(),
                        )?;
                    }
                    finalization_needed = NeedsFinalization::Yes;
                }
            }
        } else if new_replication_factor < replication_factor {
            // Adjust replica count down
            let replica_ids = (*new_replication_factor..*replication_factor)
                .map(managed_cluster_replica_name)
                .filter_map(|name| cluster.replica_id(&name))
                .map(|replica_id| {
                    catalog::DropObjectInfo::ClusterReplica((
                        cluster.id(),
                        replica_id,
                        reason.clone(),
                    ))
                })
                .collect();
            ops.push(catalog::Op::DropObjects(replica_ids));
        } else if new_replication_factor > replication_factor {
            // Adjust replica count up
            for name in
                (*replication_factor..*new_replication_factor).map(managed_cluster_replica_name)
            {
                self.create_managed_cluster_replica_op(
                    cluster_id,
                    name.clone(),
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
            }
        }

        // If finalization is needed, finalization should update the cluster
        // config.
        match finalization_needed {
            NeedsFinalization::No => {
                ops.push(catalog::Op::UpdateClusterConfig {
                    id: cluster_id,
                    name: name.clone(),
                    config: new_config,
                });
            }
            NeedsFinalization::Yes => {}
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

        let ops = vec![catalog::Op::UpdateClusterConfig {
            id: cluster_id,
            name: cluster.name().to_string(),
            config: new_config,
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
/// Drives [`fold_reconfiguration_target`].
struct ReconfigurationDimensionsUnchanged {
    size: bool,
    replication_factor: bool,
    availability_zones: bool,
    logging: bool,
}

/// Fold a new `ALTER` onto an in-flight reconfiguration target.
///
/// `new_target` was built against the *realized* config, so any dimension the
/// `ALTER` left `Unchanged` carries the realized (pre-reconfiguration) value. When
/// a reconfiguration is in flight (`in_flight` is `Some`), the realized config is
/// the pre-reconfiguration shape, so for each `Unchanged` dimension we instead
/// keep the in-flight target's value — only dimensions the `ALTER` explicitly set
/// re-target. With nothing in flight (`in_flight` is `None`) the target is exactly
/// `new_target`. This is what keeps an `ALTER` that touches one dimension (e.g.
/// rf-only) from silently reverting the in-flight transition along every dimension
/// it did not mention.
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
        logging: if unchanged.logging {
            prev.logging.clone()
        } else {
            new_target.logging
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
        }
    }

    fn all_changed() -> ReconfigurationDimensionsUnchanged {
        ReconfigurationDimensionsUnchanged {
            size: false,
            replication_factor: false,
            availability_zones: false,
            logging: false,
        }
    }

    fn all_unchanged() -> ReconfigurationDimensionsUnchanged {
        ReconfigurationDimensionsUnchanged {
            size: true,
            replication_factor: true,
            availability_zones: true,
            logging: true,
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
        // A 200cc size change is in flight; a later rf-only ALTER must NOT revert
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
            logging: true,
        };
        let folded = fold_reconfiguration_target(Some(&in_flight), new, unchanged);
        // The in-flight size/AZ/logging survive; only rf is re-targeted.
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
        // An ALTER that sets nothing while a record is in flight (e.g. only
        // workload_class, handled elsewhere) folds to the in-flight target
        // unchanged — a no-op re-target, not a revert to realized.
        let in_flight = target("200cc", 2, &["az2"], true);
        let realized_shaped = target("100cc", 1, &["az1"], false);
        let folded =
            fold_reconfiguration_target(Some(&in_flight), realized_shaped, all_unchanged());
        assert_eq!(folded, in_flight);
    }
}
