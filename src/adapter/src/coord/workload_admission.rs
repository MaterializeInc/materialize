// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Workload restrictions on running work.
//!
//! The compute and storage controllers replay a cluster's running work onto
//! every replica it gains: subscribes, `COPY TO` sinks, pending peeks with their
//! transient dataflows, and `COPY ... FROM` ingestions. So the classes a cluster
//! runs include its running work, not only its catalog objects.
//!
//! Two checks keep that work within what every replica's size allows. Replica
//! admission ([`Coordinator::ensure_size_allows_cluster_workloads`]) rejects a
//! size that does not allow a class of the registered work. Registration
//! ([`Coordinator::ensure_cluster_allows_running_workload`]) rechecks the
//! cluster before work registers and ships. Both run on the coordinator loop,
//! so a replica admitted before registration fails the recheck, and work
//! registered before admission fails admission.
//!
//! NOTE: The recheck at registration is needed even though statements are
//! checked when sequenced. Sequencing can run off the coordinator loop against
//! a catalog snapshot, and it can pause (for example on a timestamp or a
//! failpoint) between that check and dispatch.

use mz_adapter_types::connection::ConnectionId;
use mz_controller::clusters::WorkloadClass;
use mz_controller_types::ClusterId;
use mz_sql::session::metadata::SessionMetadata;
use mz_sql::session::user::User;

use crate::AdapterError;
use crate::active_compute_sink::ActiveComputeSink;
use crate::coord::{Coordinator, catalog_serving};
use crate::session::Session;

impl Coordinator {
    /// Fails if `size` does not allow a workload class that `cluster_id` runs,
    /// counting its registered work as well as its objects.
    pub(crate) fn ensure_size_allows_cluster_workloads(
        &self,
        cluster_id: ClusterId,
        size: &str,
        session: &Session,
    ) -> Result<(), AdapterError> {
        let state = self.catalog().state();
        let sinks = self
            .active_compute_sinks
            .values()
            .filter(|sink| sink.cluster_id() == cluster_id)
            .filter_map(|sink| {
                let conn_id = sink.connection_id()?;
                Some((
                    self.conn_user(conn_id),
                    self.active_compute_sink_workload(sink),
                ))
            });
        let peeks = self
            .pending_peeks
            .values()
            .filter(|peek| peek.cluster_id == cluster_id)
            .map(|peek| {
                (
                    self.conn_user(&peek.conn_id),
                    catalog_serving::peek_workload(state, peek.depends_on.iter().copied()),
                )
            });
        let copies = self
            .active_copies
            .values()
            .filter(|copy| copy.cluster_id == cluster_id)
            .map(|copy| (Some(copy.ctx.session().user()), WorkloadClass::Ingest));
        let running = sinks
            .chain(peeks)
            .chain(copies)
            .filter(|(user, _)| user.is_some_and(|user| !user.is_internal()))
            .map(|(_, class)| class);
        state.ensure_size_allows_cluster_workloads(
            state.get_cluster(cluster_id),
            size,
            session.user(),
            running,
        )
    }

    /// Fails if `cluster_id` does not allow `class` for the user of `conn_id`.
    ///
    /// Call this on the coordinator loop before registering work in the state
    /// that [`Self::ensure_size_allows_cluster_workloads`] reads, and before
    /// shipping it. A cluster that no longer exists passes, since dispatch
    /// reports that.
    pub(crate) fn ensure_cluster_allows_running_workload(
        &self,
        cluster_id: ClusterId,
        class: WorkloadClass,
        conn_id: &ConnectionId,
        action: &'static str,
    ) -> Result<(), AdapterError> {
        let Some(cluster) = self.catalog().try_get_cluster(cluster_id) else {
            return Ok(());
        };
        self.catalog().state().ensure_cluster_allows_workload(
            cluster,
            class,
            self.conn_user(conn_id),
            || action.to_string(),
        )
    }

    /// The workload class of a registered compute sink.
    fn active_compute_sink_workload(&self, sink: &ActiveComputeSink) -> WorkloadClass {
        match sink {
            ActiveComputeSink::Subscribe(subscribe) => catalog_serving::subscribe_workload(
                self.catalog().state(),
                subscribe.depends_on.iter().copied(),
            ),
            ActiveComputeSink::CopyTo(_) => WorkloadClass::ComputeInteractive,
        }
    }

    /// The user of `conn_id`, or `None` if the connection is gone. Work of a
    /// gone connection is about to be torn down, so it is not restricted.
    fn conn_user(&self, conn_id: &ConnectionId) -> Option<&User> {
        self.active_conns.get(conn_id).map(|conn| conn.user())
    }
}
