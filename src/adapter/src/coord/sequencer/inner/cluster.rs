// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;

use crate::catalog::{self, Op, ReplicaCreateDropReason};
use crate::coord::sequencer::cluster::{FinalizationNeeded, PENDING_REPLICA_SUFFIX};
use crate::coord::{
    AlterCluster, ClusterStage, Coordinator, FinalizeAlterCluster, Message, PlanValidity,
    StageResult, Staged,
};
use crate::{session::Session, AdapterError, ExecuteContext, ExecuteResponse};
use mz_catalog::memory::objects::{
    ClusterConfig, ClusterReplica, ClusterVariant, ClusterVariantManaged,
};
use mz_controller::clusters::{ManagedReplicaLocation, ReplicaLogging};
use mz_controller_types::DEFAULT_REPLICA_LOGGING_INTERVAL;
use mz_ore::instrument;
use mz_sql::ast::{Ident, QualifiedReplica};
use mz_sql::catalog::ObjectType;
use mz_sql::plan;
use mz_sql::plan::AlterClusterPlan;
use mz_sql::session::metadata::SessionMetadata;
use tracing::{debug, Instrument, Span};

use super::return_if_err;

impl Staged for ClusterStage {
    type Ctx = ExecuteContext;

    fn validity(&mut self) -> &mut PlanValidity {
        match self {
            Self::Alter(stage) => &mut stage.validity,
            Self::Finalize(stage) => &mut stage.validity,
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
                    .sequence_alter_cluster(ctx.session(), stage.plan.clone(), stage.validity)
                    .await
            }
            Self::Finalize(stage) => {
                coord
                    .finalize_alter_cluster_managed_to_managed(
                        ctx.session(),
                        stage.plan.clone(),
                        stage.new_config.clone(),
                    )
                    .await
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
        &mut self,
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

    pub(super) async fn sequence_alter_cluster(
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
        let config = self.catalog.get_cluster(cluster_id).config.clone();
        let mut new_config = config.clone();

        match (&new_config.variant, &options.managed) {
            (Managed(_), Reset) | (Managed(_), Unchanged) | (Managed(_), Set(true)) => {}
            (Managed(_), Set(false)) => new_config.variant = Unmanaged,
            (Unmanaged, Unchanged) | (Unmanaged, Set(false)) => {}
            (Unmanaged, Reset) | (Unmanaged, Set(true)) => {
                // Generate a minimal correct configuration

                // Size and disk adjusted later when sequencing the actual configuration change.
                let size = "".to_string();
                let disk = false;
                let logging = ReplicaLogging {
                    log_logging: false,
                    interval: Some(DEFAULT_REPLICA_LOGGING_INTERVAL),
                };
                new_config.variant = Managed(ClusterVariantManaged {
                    size,
                    availability_zones: Default::default(),
                    logging,
                    replication_factor: 1,
                    disk,
                    optimizer_feature_overrides: Default::default(),
                    schedule: Default::default(),
                });
            }
        }

        match &mut new_config.variant {
            Managed(ClusterVariantManaged {
                size,
                availability_zones,
                logging,
                replication_factor,
                disk,
                optimizer_feature_overrides: _,
                schedule,
            }) => {
                match &options.size {
                    Set(s) => size.clone_from(s),
                    Reset => coord_bail!("SIZE has no default value"),
                    Unchanged => {}
                }
                match &options.disk {
                    Set(d) => *disk = *d,
                    Reset => *disk = self.catalog.system_config().disk_cluster_replicas_default(),
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
                    Reset => *replication_factor = 1,
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

        if new_config == config {
            return Ok(StageResult::Response(ExecuteResponse::AlteredObject(
                ObjectType::Cluster,
            )));
        }

        let new_workload_class = new_config.workload_class.clone();
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
                return match alter_followup {
                    FinalizationNeeded::In(duration) => {
                        // For non backgrounded graceful alters,
                        // store the cluster_id in the ConnMeta
                        // to allow for cancellation.
                        self.active_conns
                            .get_mut(session.conn_id())
                            .expect("There must be an active connection")
                            .pending_cluster_alters
                            .insert(cluster_id.clone());
                        let span = Span::current();
                        let new_config_managed = new_config_managed.clone();
                        Ok(StageResult::Handle(mz_ore::task::spawn(
                            || "Finalize Alter Cluster",
                            async move {
                                tokio::time::sleep(duration).await;
                                let stage = ClusterStage::Finalize(FinalizeAlterCluster {
                                    validity,
                                    plan,
                                    new_config: new_config_managed,
                                });
                                Ok(Box::new(stage))
                            }
                            .instrument(span),
                        )))
                    }
                    FinalizationNeeded::False => Ok(StageResult::Response(
                        ExecuteResponse::AlteredObject(ObjectType::Cluster),
                    )),
                };
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

        self.controller
            .update_cluster_workload_class(cluster_id, new_workload_class)?;

        Ok(StageResult::Response(ExecuteResponse::AlteredObject(
            ObjectType::Cluster,
        )))
    }

    pub(crate) async fn finalize_alter_cluster_managed_to_managed(
        &mut self,
        session: &Session,
        AlterClusterPlan {
            id: cluster_id,
            name: cluster_name,
            ..
        }: AlterClusterPlan,
        new_config: ClusterVariantManaged,
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
                    _ => {}
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
                workload_class: cluster.config.workload_class.clone(),
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
}
