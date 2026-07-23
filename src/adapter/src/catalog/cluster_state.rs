// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Projects a managed cluster's durable catalog config into the
//! [`ExpectedClusterState`] compare-and-append witness, and checks a witness
//! against the current config.
//!
//! [`project_expected`] is the one projection from catalog config to the
//! witness. Building the witness the same way wherever a write is conditioned
//! and wherever it is checked keeps the compared fields from drifting apart.

use mz_adapter_types::cluster_state::{
    AutoScalingPolicy, AvailabilityZones, BurstRecord, ExpectedClusterState, OnHydrationPolicy,
    OnTimeout, ReconfigurationRecord, ReconfigurationStatus, ReconfigurationTarget,
};
use mz_catalog::memory::objects::{
    BurstState, ClusterVariant, ClusterVariantManaged, ReconfigurationState,
};
use mz_controller_types::ClusterId;
use mz_sql::plan::OnTimeoutAction;

use crate::catalog::CatalogState;

/// Project a managed cluster's durable config into the compare-and-append
/// witness: the fields a conditional write is conditioned on.
pub(crate) fn project_expected(managed: &ClusterVariantManaged) -> ExpectedClusterState {
    // Exhaustive destructure (no `..`): a field added to the managed config is a
    // compile error here until we decide whether the witness must cover it.
    let ClusterVariantManaged {
        size,
        availability_zones,
        logging,
        arrangement_compression,
        replication_factor,
        optimizer_feature_overrides: _,
        schedule: _,
        auto_scaling_strategy,
        reconfiguration,
        burst,
    } = managed;
    ExpectedClusterState {
        size: size.clone(),
        replication_factor: *replication_factor,
        availability_zones: AvailabilityZones(availability_zones.clone()),
        logging: logging.clone(),
        arrangement_compression: *arrangement_compression,
        auto_scaling_policy: auto_scaling_strategy.as_ref().map(auto_scaling_policy),
        reconfiguration: reconfiguration.as_ref().map(reconfiguration_record),
        burst: burst.as_ref().map(burst_record),
    }
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
    let Some(cluster) = state.try_get_cluster(cluster_id) else {
        return false;
    };
    let ClusterVariant::Managed(managed) = &cluster.config.variant else {
        return false;
    };
    project_expected(managed) == *expected
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
    let mz_catalog::memory::objects::ReconfigurationTarget {
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
    status: mz_catalog::memory::objects::ReconfigurationStatus,
) -> ReconfigurationStatus {
    match status {
        mz_catalog::memory::objects::ReconfigurationStatus::InProgress => {
            ReconfigurationStatus::InProgress
        }
        mz_catalog::memory::objects::ReconfigurationStatus::Finalized => {
            ReconfigurationStatus::Finalized
        }
        mz_catalog::memory::objects::ReconfigurationStatus::TimedOut => {
            ReconfigurationStatus::TimedOut
        }
        mz_catalog::memory::objects::ReconfigurationStatus::Cancelled => {
            ReconfigurationStatus::Cancelled
        }
        mz_catalog::memory::objects::ReconfigurationStatus::ResourceExhausted => {
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
