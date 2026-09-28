// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Projects shared cluster intent and deployment-owned realization into the
//! [`ExpectedClusterState`] compare-and-append witness.
//!
//! [`project_deployment_expected`] is shared by observation and transaction
//! validation, so a decision cannot discard either its shared configuration
//! dependency or its own realization context.

use crate::memory::objects::{
    BurstState, ClusterVariant, ClusterVariantManaged, ReconfigurationState,
};
use mz_adapter_types::cluster_state::{
    AutoScalingPolicy, AvailabilityZones, BurstRecord, ClusterSchedule, ExpectedClusterState,
    OnHydrationPolicy, OnTimeout, ReconfigurationRecord, ReconfigurationStatus,
    ReconfigurationTarget,
};
use mz_controller_types::ClusterId;
use mz_sql::plan::OnTimeoutAction;

use crate::catalog::CatalogState;

/// Project a managed cluster's durable config into the compare-and-append
/// witness: the fields a conditional write is conditioned on.
pub fn project_expected(managed: &ClusterVariantManaged) -> ExpectedClusterState {
    // Exhaustive destructure (no `..`): a field added to the managed config is a
    // compile error here until we decide whether the witness must cover it.
    let ClusterVariantManaged {
        size,
        availability_zones,
        logging,
        arrangement_compression,
        replication_factor,
        optimizer_feature_overrides: _,
        schedule,
        auto_scaling_strategy,
        reconfiguration,
        burst,
    } = managed;
    ExpectedClusterState {
        intent: None,
        size: size.clone(),
        replication_factor: *replication_factor,
        availability_zones: AvailabilityZones(availability_zones.clone()),
        logging: logging.clone(),
        arrangement_compression: *arrangement_compression,
        schedule: cluster_schedule(schedule),
        auto_scaling_policy: auto_scaling_strategy.as_ref().map(auto_scaling_policy),
        reconfiguration: reconfiguration.as_ref().map(reconfiguration_record),
        burst: burst.as_ref().map(burst_record),
    }
}

/// Projects shared policy and this deployment's durable realization together.
/// The witness includes shared request state, but never a peer's runtime state.
pub fn project_deployment_expected(
    state: &CatalogState,
    cluster_id: ClusterId,
) -> Option<ExpectedClusterState> {
    let cluster = state.try_get_cluster(cluster_id)?;
    let ClusterVariant::Managed(managed) = &cluster.config.variant else {
        return None;
    };
    let mut expected = project_expected(managed);
    if !state.catalog_read_protection_enabled() {
        return Some(expected);
    }
    expected.intent = Some(mz_adapter_types::cluster_state::ClusterIntent {
        accepted: ReconfigurationTarget {
            size: expected.size.clone(),
            replication_factor: expected.replication_factor,
            availability_zones: expected.availability_zones.clone(),
            logging: expected.logging.clone(),
            arrangement_compression: expected.arrangement_compression,
        },
        reconfiguration: expected.reconfiguration.clone(),
        may_settle: state.active_deployment_generation() == Some(state.deployment_generation),
    });
    // A newly joining deployment starts from accepted intent, not from another
    // deployment's hydration or terminal local outcome.
    expected.reconfiguration = None;
    expected.burst = None;
    if let Some(runtime) = state
        .cluster_runtimes
        .get(&(cluster_id, state.deployment_generation))
    {
        let realized = &runtime.realized_config;
        expected.size.clone_from(&realized.size);
        expected.replication_factor = realized.replication_factor;
        expected.availability_zones = AvailabilityZones(realized.availability_zones.clone());
        expected.logging = realized.logging.clone();
        expected.arrangement_compression = realized.arrangement_compression;
        expected.reconfiguration = runtime
            .reconfiguration
            .clone()
            .map(Into::into)
            .as_ref()
            .map(reconfiguration_record);
        expected.burst = runtime
            .burst
            .clone()
            .map(Into::into)
            .as_ref()
            .map(burst_record);
    }
    Some(expected)
}

/// Whether `cluster_id`'s current managed state still equals `expected`. A
/// missing or unmanaged cluster never matches. This is the compare half of the
/// compare-and-append, evaluated inside the catalog transaction so the check and
/// the commit cannot be separated.
pub(crate) fn cluster_matches_expected(
    state: &CatalogState,
    cluster_id: ClusterId,
    expected: &ExpectedClusterState,
) -> bool {
    project_deployment_expected(state, cluster_id).as_ref() == Some(expected)
}

fn reconfiguration_record(record: &ReconfigurationState) -> ReconfigurationRecord {
    // Exhaustive destructure (no `..`), like `project_expected`: a field added
    // to either catalog type is a compile error here until we decide whether the
    // witness must carry it.
    let ReconfigurationState {
        target,
        deadline,
        on_timeout: on_timeout_action,
        status,
    } = record;
    let crate::memory::objects::ReconfigurationTarget {
        size,
        replication_factor,
        availability_zones,
        logging,
        arrangement_compression,
    } = target;
    ReconfigurationRecord {
        target: ReconfigurationTarget {
            size: size.clone(),
            replication_factor: *replication_factor,
            availability_zones: AvailabilityZones(availability_zones.clone()),
            logging: logging.clone(),
            arrangement_compression: *arrangement_compression,
        },
        deadline: *deadline,
        on_timeout: on_timeout(*on_timeout_action),
        status: reconfiguration_status(*status),
    }
}

fn reconfiguration_status(
    status: crate::memory::objects::ReconfigurationStatus,
) -> ReconfigurationStatus {
    match status {
        crate::memory::objects::ReconfigurationStatus::InProgress => {
            ReconfigurationStatus::InProgress
        }
        crate::memory::objects::ReconfigurationStatus::Finalized => {
            ReconfigurationStatus::Finalized
        }
        crate::memory::objects::ReconfigurationStatus::TimedOut => ReconfigurationStatus::TimedOut,
        crate::memory::objects::ReconfigurationStatus::Cancelled => {
            ReconfigurationStatus::Cancelled
        }
        crate::memory::objects::ReconfigurationStatus::ResourceExhausted => {
            ReconfigurationStatus::ResourceExhausted
        }
    }
}

fn on_timeout(action: OnTimeoutAction) -> OnTimeout {
    match action {
        OnTimeoutAction::Commit => OnTimeout::Commit,
        OnTimeoutAction::Rollback => OnTimeout::Rollback,
    }
}

fn cluster_schedule(schedule: &mz_sql::plan::ClusterSchedule) -> ClusterSchedule {
    match schedule {
        mz_sql::plan::ClusterSchedule::Manual => ClusterSchedule::Manual,
        mz_sql::plan::ClusterSchedule::Refresh {
            hydration_time_estimate,
        } => ClusterSchedule::Refresh {
            hydration_time_estimate: *hydration_time_estimate,
        },
    }
}

fn burst_record(record: &BurstState) -> BurstRecord {
    // Exhaustive destructure (no `..`), like `project_expected`: a field added
    // to the catalog type is a compile error here until the witness accounts for
    // it.
    let BurstState {
        burst_size,
        linger_duration,
        steady_hydrated_at,
    } = record;
    BurstRecord {
        burst_size: burst_size.clone(),
        linger_duration: *linger_duration,
        steady_hydrated_at: *steady_hydrated_at,
    }
}

fn auto_scaling_policy(strategy: &mz_sql::plan::AutoScalingStrategy) -> AutoScalingPolicy {
    let mz_sql::plan::AutoScalingStrategy { on_hydration } = strategy;
    AutoScalingPolicy {
        on_hydration: on_hydration.as_ref().map(|on_hydration| {
            let mz_sql::plan::OnHydration {
                hydration_size,
                linger_duration,
            } = on_hydration;
            OnHydrationPolicy {
                hydration_size: hydration_size.clone(),
                linger_duration: *linger_duration,
            }
        }),
    }
}
