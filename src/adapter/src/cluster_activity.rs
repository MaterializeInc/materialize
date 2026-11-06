// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Ad-hoc activity on `SCHEDULE = ON IDLE` clusters.
//!
//! Statement sequencing records when ad-hoc work starts on an idle cluster,
//! and the cluster controller's on-idle strategy turns the cluster on and off
//! by it. Recording happens on the frontend sequencing paths too, which run
//! off the coordinator, so the record is shared memory rather than coordinator
//! state.
//!
//! Ad-hoc work against a suspended cluster is not rejected for want of a
//! replica. It is issued against the replica-less cluster, and the compute
//! controller hands it to the replica the controller creates in response.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use mz_catalog::memory::objects::{ClusterVariant, ClusterVariantManaged};
use mz_controller::clusters::WorkloadClass;
use mz_controller_types::ClusterId;
use mz_ore::now::{EpochMillis, NowFn};
use mz_sql::plan::{ClusterSchedule, Plan};
use mz_sql::session::user::User;
use tokio::sync::Notify;

use crate::catalog::CatalogState;
use crate::coord::catalog_serving::plan_workload;

/// When ad-hoc work last started on each `ON IDLE` cluster, since
/// [`Self::tracked_since`].
#[derive(Debug)]
pub struct ClusterActivity {
    last_activity: Mutex<BTreeMap<ClusterId, EpochMillis>>,
    tracked_since: EpochMillis,
    now: NowFn,
    /// The cluster controller's wake-up, so that a suspended cluster resumes
    /// without waiting out a controller tick.
    reconcile_now: Arc<Notify>,
}

impl ClusterActivity {
    pub(crate) fn new(now: NowFn, reconcile_now: Arc<Notify>) -> Self {
        Self {
            last_activity: Mutex::new(BTreeMap::new()),
            tracked_since: now(),
            now,
            reconcile_now,
        }
    }

    /// When activity tracking began.
    pub(crate) fn tracked_since(&self) -> EpochMillis {
        self.tracked_since
    }

    /// When ad-hoc work last started on `cluster_id`, if any did since
    /// [`Self::tracked_since`].
    pub(crate) fn last_activity(&self, cluster_id: ClusterId) -> Option<EpochMillis> {
        self.last_activity
            .lock()
            .expect("lock poisoned")
            .get(&cluster_id)
            .copied()
    }

    /// Records that ad-hoc work starts on `cluster_id` now, and wakes the
    /// cluster controller if the cluster is suspended.
    pub(crate) fn record(&self, cluster_id: ClusterId, suspended: bool) {
        let now = (self.now)();
        self.last_activity
            .lock()
            .expect("lock poisoned")
            .insert(cluster_id, now);
        if suspended {
            self.reconcile_now.notify_one();
        }
    }

    /// Records ad-hoc activity for `plan` if `user` runs it on the `ON IDLE`
    /// cluster `cluster_id`.
    pub(crate) fn record_plan(
        &self,
        state: &CatalogState,
        cluster_id: ClusterId,
        user: &User,
        plan: &Plan,
    ) {
        let cluster = state.get_cluster(cluster_id);
        if resumes_on_demand(state, cluster_id, user) && is_ad_hoc(state, plan) {
            self.record(cluster_id, cluster.replicas().next().is_none());
        }
    }

    /// Forgets the activity of a dropped cluster.
    pub(crate) fn forget(&self, cluster_id: ClusterId) {
        self.last_activity
            .lock()
            .expect("lock poisoned")
            .remove(&cluster_id);
    }
}

/// Whether ad-hoc work by `user` resumes `cluster_id` when it is suspended,
/// instead of failing for want of a replica.
///
/// NOTE: Work by internal users neither resumes an idle cluster nor restarts
/// its idle timeout, so that support access does not run up the cluster's
/// cost. It still holds a running cluster on while in flight.
pub(crate) fn resumes_on_demand(state: &CatalogState, cluster_id: ClusterId, user: &User) -> bool {
    !user.is_internal()
        && matches!(
            &state.get_cluster(cluster_id).config.variant,
            ClusterVariant::Managed(ClusterVariantManaged {
                schedule: ClusterSchedule::Idle { .. },
                ..
            })
        )
}

/// Whether `plan` is ad-hoc work: a query, `SUBSCRIBE`, or the read of a
/// write. Indexes, materialized views, and sinks are maintained work, which
/// does not keep an idle cluster on.
fn is_ad_hoc(state: &CatalogState, plan: &Plan) -> bool {
    match plan {
        Plan::Subscribe(_) => true,
        _ => matches!(
            plan_workload(state, plan),
            Some((
                WorkloadClass::ComputeInteractive | WorkloadClass::ComputeIntrospection,
                _
            ))
        ),
    }
}
