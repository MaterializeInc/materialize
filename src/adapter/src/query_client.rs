// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Adapter-owned read protection and query execution, independent of maintained lifecycle.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use mz_catalog::memory::objects::{CatalogItem, TableDataSource};
use mz_compute_client::protocol::command::Peek;
use mz_compute_client::protocol::response::{PeekError, PeekResponse};
use mz_compute_types::ComputeInstanceId;
use mz_controller_types::ReplicaId;
use mz_ore::tracing::OpenTelemetryContext;
use mz_persist_client::{Diagnostics, PersistClient};
use mz_persist_types::{PersistLocation, ShardId};
use mz_repr::{GlobalId, Timestamp};
use mz_sql::catalog::SessionCatalog;
use mz_storage_types::StorageDiff;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::sources::SourceData;
use timely::PartialOrder;
use timely::progress::Antichain;
use tokio::sync::{oneshot, watch};
use uuid::Uuid;

use crate::catalog::Catalog;
use crate::command::Command;
use crate::optimize::dataflows::ComputeInstanceSnapshot;
use crate::peek_client::CoordinatorClient;
use crate::{AdapterError, CollectionIdBundle, ReadHolds};

pub(crate) mod compute;
pub(crate) mod connections;
pub(crate) mod dataflows;
pub(crate) mod read_protection;

use compute::{QueryError, ReplicaQueryClient};
use connections::QueryReplicaConnections;
use read_protection::ClientReadProtection;

#[derive(Debug)]
pub(crate) struct PreparedRead {
    pub(crate) bundle: CollectionIdBundle,
    pub(crate) frontiers: BTreeMap<GlobalId, Timestamp>,
    pub(crate) index_inputs: BTreeMap<GlobalId, BTreeSet<GlobalId>>,
    pub(crate) upper: Antichain<Timestamp>,
    pub(crate) read_ts: Option<Timestamp>,
}

#[derive(Debug)]
pub(crate) struct QueryClient {
    coordinator: CoordinatorClient,
    pub(crate) protection: ClientReadProtection,
    persist: PersistClient,
    persist_location: PersistLocation,
    txns_shard: ShardId,
    pub(crate) connections: Arc<QueryReplicaConnections>,
    last_publication: Mutex<Instant>,
    pending_peeks: Arc<Mutex<BTreeMap<Uuid, watch::Sender<Option<PeekResponse>>>>>,
}

/// Registers cancellation before a peek is visible to adapter bookkeeping.
#[derive(Debug)]
pub(crate) struct QueryPeekRegistration {
    uuid: Uuid,
    pending: Arc<Mutex<BTreeMap<Uuid, watch::Sender<Option<PeekResponse>>>>>,
    canceled: watch::Receiver<Option<PeekResponse>>,
}

impl Drop for QueryPeekRegistration {
    fn drop(&mut self) {
        self.pending
            .lock()
            .expect("query peeks mutex poisoned")
            .remove(&self.uuid);
    }
}

impl QueryClient {
    pub(crate) fn new(
        incarnation: u64,
        coordinator: CoordinatorClient,
        persist: PersistClient,
        persist_location: PersistLocation,
        txns_shard: ShardId,
        connections: Arc<QueryReplicaConnections>,
    ) -> Self {
        Self {
            coordinator,
            protection: ClientReadProtection::new(incarnation),
            persist,
            persist_location,
            txns_shard,
            connections,
            last_publication: Mutex::new(Instant::now()),
            pending_peeks: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub(crate) fn last_publication(&self) -> Instant {
        *self
            .last_publication
            .lock()
            .expect("query publication mutex poisoned")
    }

    pub(crate) fn published(&self) {
        *self
            .last_publication
            .lock()
            .expect("query publication mutex poisoned") = Instant::now();
    }

    pub(crate) fn replica_clients(
        &self,
        cluster: ComputeInstanceId,
        target: Option<ReplicaId>,
    ) -> Vec<ReplicaQueryClient> {
        self.connections.ready_clients(cluster, target)
    }

    /// Return requested indexes with a nonempty actual read frontier on at least
    /// one ready replica. This only inspects cached observations, without waiting
    /// for replicas or granting read protection.
    pub(crate) fn readable_indexes(
        &self,
        cluster: ComputeInstanceId,
        ids: &BTreeSet<GlobalId>,
    ) -> BTreeSet<GlobalId> {
        let replicas = self.replica_clients(cluster, None);
        ids.iter()
            .copied()
            .filter(|id| {
                replicas.iter().any(|replica| {
                    replica
                        .collection_frontiers(*id)
                        .ok()
                        .flatten()
                        .and_then(|frontiers| frontiers.read_frontier)
                        .is_some_and(|frontier| !frontier.is_empty())
                })
            })
            .collect()
    }

    /// Ordinary indexes are offered once observed. Logging sources have no
    /// storage access path, so their catalog-owned indexes must remain available
    /// to planning while execution waits for query-protocol readiness.
    pub(crate) fn instance_snapshot(
        &self,
        catalog: &Catalog,
        cluster: ComputeInstanceId,
    ) -> ComputeInstanceSnapshot {
        let mut ids = BTreeSet::new();
        if let Some(instance) = catalog.try_get_cluster(cluster) {
            ids.extend(instance.log_indexes.values().copied());
        }
        for client in self.replica_clients(cluster, None) {
            if let Ok(frontiers) = client.frontiers() {
                ids.extend(frontiers.into_iter().filter_map(|(id, frontiers)| {
                    let readable = frontiers
                        .read_frontier
                        .as_ref()
                        .is_some_and(|f| !f.is_empty());
                    (readable
                        && catalog
                            .try_get_entry_by_global_id(&id)
                            .is_some_and(|entry| matches!(entry.item(), CatalogItem::Index(_))))
                    .then_some(id)
                }));
            }
        }
        ComputeInstanceSnapshot::new_from_parts(cluster, ids)
    }

    pub(crate) fn collection_metadata(
        &self,
        catalog: &Catalog,
        id: GlobalId,
    ) -> Result<CollectionMetadata, AdapterError> {
        let session = catalog.for_system_session();
        let collection = session
            .try_get_item_by_global_id(&id)
            .ok_or_else(|| unavailable(id))?;
        let relation_desc = collection
            .relation_desc()
            .ok_or_else(|| unavailable(id))?
            .into_owned();
        let entry = catalog
            .try_get_entry_by_global_id(&id)
            .ok_or_else(|| unavailable(id))?;
        let in_txns = matches!(entry.item(), CatalogItem::Table(table)
            if matches!(table.data_source, TableDataSource::TableWrites { .. }));
        Ok(CollectionMetadata {
            persist_location: self.persist_location.clone(),
            data_shard: catalog
                .state()
                .storage_metadata()
                .get_collection_shard(id)?,
            relation_desc,
            txns_shard: in_txns.then_some(self.txns_shard),
        })
    }

    /// Observes the bundle's write frontier without acquiring read protection.
    /// Storage uppers come from Persist, including the transaction WAL for lazy
    /// tables. Compute uppers use cached observations, taking the maximum across
    /// replicas for each index and then the meet across all bundle members.
    /// An index with no observed upper contributes MIN, not the empty frontier.
    pub(crate) async fn write_frontier(
        &self,
        catalog: &Catalog,
        bundle: &CollectionIdBundle,
    ) -> Result<Antichain<Timestamp>, AdapterError> {
        let mut upper = Antichain::new();
        let mut txns_upper = None;
        for id in &bundle.storage_ids {
            let metadata = self.collection_metadata(catalog, *id)?;
            let write = if let Some(shard) = metadata.txns_shard {
                // Lazy table shards may trail the transaction WAL. Its complete
                // prefix is the logical write frontier, not the data shard upper.
                if txns_upper.is_none() {
                    txns_upper = Some(
                        self.persist
                            .recent_upper::<SourceData, (), Timestamp, StorageDiff>(
                                shard,
                                diagnostics(*id),
                            )
                            .await
                            .map_err(|error| AdapterError::Unstructured(error.into()))?,
                    );
                }
                txns_upper
                    .as_ref()
                    .expect("fetched transaction upper")
                    .clone()
            } else {
                self.persist
                    .recent_upper::<SourceData, (), Timestamp, StorageDiff>(
                        metadata.data_shard,
                        diagnostics(*id),
                    )
                    .await
                    .map_err(|error| AdapterError::Unstructured(error.into()))?
            };
            upper.extend(write);
        }
        for (cluster, ids) in &bundle.compute_ids {
            let replicas = self.replica_clients(*cluster, None);
            for id in ids {
                let mut write = Antichain::from_elem(Timestamp::MIN);
                for replica in &replicas {
                    if let Ok(Some(observed)) = replica.collection_frontiers(*id)
                        && let Some(new) = observed.write_frontier.as_ref()
                        && PartialOrder::less_than(&write, new)
                    {
                        write = new.clone();
                    }
                }
                upper.extend(write);
            }
        }
        Ok(upper)
    }

    /// Observes candidate frontiers. This method grants no protection.
    /// `timestamp` chooses a desired read time from the observed upper, or None
    /// to reuse an established window without a timestamp preference. Grants
    /// later than that time require fresh readability and permission observations.
    /// The returned floor can exceed the desired time, so callers must validate
    /// timestamp constraints against the acquired holds before reading.
    pub(crate) async fn prepare_read(
        &self,
        catalog: &Catalog,
        bundle: &CollectionIdBundle,
        timestamp: impl FnOnce(&Antichain<Timestamp>) -> Result<Option<Timestamp>, AdapterError>,
    ) -> Result<PreparedRead, AdapterError> {
        let upper = self.write_frontier(catalog, bundle).await?;
        let read_ts = timestamp(&upper)?;
        let mut index_inputs = BTreeMap::new();
        let mut storage = bundle.storage_ids.clone();
        for id in bundle.compute_ids.values().flatten() {
            let entry = catalog
                .try_get_entry_by_global_id(id)
                .ok_or_else(|| unavailable(*id))?;
            let CatalogItem::Index(index) = entry.item() else {
                return Err(unavailable(*id));
            };
            let inputs: BTreeSet<_> = catalog
                .state()
                .logical_collection_inputs([index.on])
                .into_iter()
                .filter(|input| {
                    !matches!(
                        catalog.get_entry_by_global_id(input).item(),
                        CatalogItem::Log(_)
                    )
                })
                .collect();
            storage.extend(inputs.iter().copied());
            index_inputs.insert(*id, inputs);
        }
        let mut frontiers = BTreeMap::new();
        for id in storage {
            let frontier = if let Some(granted) = self.protection.reusable_frontier(id, read_ts) {
                granted
            } else {
                let shard = catalog
                    .state()
                    .storage_metadata()
                    .get_collection_shard(id)?;
                let mut since = self
                    .persist
                    .recent_since::<SourceData, (), Timestamp, StorageDiff>(shard, diagnostics(id))
                    .await
                    .map_err(|error| AdapterError::Unstructured(error.into()))?;
                if let Some(bound) = catalog.state().collection_compaction_bounds().get(&id) {
                    use differential_dataflow::lattice::Lattice;
                    since.join_assign(bound);
                }
                since
                    .into_option()
                    .ok_or_else(|| unavailable(id))?
                    .max(read_ts.unwrap_or(Timestamp::MIN))
            };
            frontiers.insert(id, frontier);
        }
        for (cluster, ids) in &bundle.compute_ids {
            let replicas = self.replica_clients(*cluster, None);
            for id in ids {
                let mut since = self
                    .protection
                    .reusable_frontier(*id, read_ts)
                    .unwrap_or_else(|| read_ts.unwrap_or(Timestamp::MIN));
                let bound = catalog.state().collection_compaction_bounds().get(id);
                let observed = replicas
                    .iter()
                    .filter_map(|replica| {
                        replica
                            .collection_frontiers(*id)
                            .ok()??
                            .read_frontier
                            .as_ref()?
                            .as_option()
                            .copied()
                    })
                    .min();
                if let Some(readable) = observed {
                    since = since.max(readable);
                } else if bound.is_none()
                    && !catalog.try_get_cluster(*cluster).is_some_and(|instance| {
                        instance.log_indexes.values().any(|index| index == id)
                    })
                {
                    return Err(unavailable(*id));
                }
                // Logging traces initialize at MIN and are held there until
                // first permission publication. They have no Persist inputs to
                // observe. Execution still waits for their actual trace frontier.
                if let Some(bound) = bound {
                    since = since.max(*bound.as_option().ok_or_else(|| unavailable(*id))?);
                }
                for input in &index_inputs[id] {
                    since = since.max(frontiers[input]);
                }
                frontiers.insert(*id, since);
            }
        }
        Ok(PreparedRead {
            bundle: bundle.clone(),
            frontiers,
            index_inputs,
            upper,
            read_ts,
        })
    }

    pub(crate) async fn acquire_read_holds_and_upper(
        &self,
        catalog: &Catalog,
        bundle: &CollectionIdBundle,
        timestamp: impl FnOnce(&Antichain<Timestamp>) -> Result<Option<Timestamp>, AdapterError>,
    ) -> Result<(ReadHolds, Antichain<Timestamp>), AdapterError> {
        let prepared = self.prepare_read(catalog, bundle, timestamp).await?;
        if let Some(holds) = self
            .protection
            .try_acquire(
                &prepared.bundle,
                &prepared.frontiers,
                &prepared.index_inputs,
            )
            .map_err(|error| AdapterError::Unstructured(error.into()))?
        {
            return Ok((holds, prepared.upper));
        }
        // Misses use the adapter's catalog writer. The coordinator re-observes
        // after any pending publication rather than relying on this snapshot.
        let (tx, rx) = oneshot::channel();
        self.coordinator.send(Command::AcquireClientReadProtection {
            incarnation: self.protection.incarnation(),
            bundle: bundle.clone(),
            read_ts: prepared.read_ts,
            tx,
        });
        let (holds, _) = rx
            .await
            .map_err(|error| AdapterError::Unstructured(error.into()))??;
        // Keep the upper used to choose the acquisition target. Replica changes
        // during publication must not substitute a different timestamp preference.
        Ok((holds, prepared.upper))
    }

    pub(crate) async fn peek(
        &self,
        cluster: ComputeInstanceId,
        target: Option<ReplicaId>,
        peek: Peek,
        mut registration: QueryPeekRegistration,
    ) -> Result<(PeekResponse, OpenTelemetryContext), AdapterError> {
        use futures::StreamExt;
        assert_eq!(registration.uuid, peek.uuid);
        let execute = async {
            let clients = if let mz_compute_client::protocol::command::PeekTarget::Index { id } =
                &peek.target
            {
                self.readable_replicas(cluster, target, &BTreeSet::from([*id]), peek.timestamp)
                    .await?
            } else {
                self.connections.clients(cluster, target).await?
            };
            let mut responses: futures::stream::FuturesUnordered<_> = clients
                .into_iter()
                .map(|client| {
                    let peek = peek.clone();
                    async move { client.peek(peek).await }
                })
                .collect();
            let mut last_error = None;
            while let Some(response) = responses.next().await {
                match response {
                    Ok(response) => return Ok(response),
                    Err(error) => last_error = Some(error),
                }
            }
            Err(AdapterError::Unstructured(
                last_error.map(anyhow::Error::from).unwrap_or_else(|| {
                    anyhow::anyhow!("no query replica is connected for cluster {cluster}")
                }),
            ))
        };
        let canceled = async {
            loop {
                let response = registration.canceled.borrow_and_update().clone();
                if let Some(response) = response {
                    return response;
                }
                registration
                    .canceled
                    .changed()
                    .await
                    .expect("registration retains cancellation sender");
            }
        };
        tokio::select! {
            biased;
            response = canceled => Ok((response, OpenTelemetryContext::obtain())),
            result = execute => finish_peek(result, target),
        }
    }

    /// Creates and peeks transient exports on the same connections. Cancellation
    /// covers admission as well as execution, and dropping the group releases
    /// installed exports and pending creation holds without replaying either.
    pub(crate) async fn peek_dataflow(
        self: &Arc<Self>,
        catalog: Arc<Catalog>,
        cluster: ComputeInstanceId,
        target: Option<ReplicaId>,
        dataflow: mz_compute_types::dataflows::DataflowDescription<
            mz_compute_types::plan::LirRelationExpr,
        >,
        creation_holds: ReadHolds,
        peek: Peek,
        mut registration: QueryPeekRegistration,
    ) -> Result<(PeekResponse, OpenTelemetryContext), AdapterError> {
        use futures::StreamExt;
        assert_eq!(registration.uuid, peek.uuid);
        let execute = async {
            let dataflows = self
                .create_dataflow(catalog, cluster, target, dataflow, creation_holds)
                .await?;
            let mut changes = dataflows.client_changes();
            let mut issued = BTreeSet::new();
            let mut responses = futures::stream::FuturesUnordered::new();
            let mut closed = false;
            let mut last_error = None;
            loop {
                // Mark the watch before inspecting, so an ACK racing inspection
                // either appears in the snapshot or wakes the next iteration.
                changes.borrow_and_update();
                for (replica, client) in dataflows.acknowledged_clients() {
                    if issued.insert(replica) {
                        let peek = peek.clone();
                        responses.push(async move { client.peek(peek).await });
                    }
                }
                if closed && responses.is_empty() {
                    return Err(AdapterError::Unstructured(last_error.unwrap_or_else(
                        || anyhow::anyhow!("all query dataflow connections failed"),
                    )));
                }
                tokio::select! {
                    response = responses.next(), if !responses.is_empty() => {
                        match response.expect("nonempty responses") {
                            // Execution errors and cancellation are terminal too,
                            // matching the controller's first-response arbitration.
                            Ok(response) => return Ok(response),
                            // A local protocol rejection (such as UUID reuse) is
                            // not a connection loss. Its export owner can remain
                            // live indefinitely, so waiting for disconnect would hang.
                            Err(error @ compute::QueryError::Rejected(_)) => {
                                return Err(AdapterError::Unstructured(error.into()));
                            }
                            Err(error) => last_error = Some(anyhow::Error::from(error)),
                        }
                    }
                    result = changes.changed(), if !closed => {
                        closed = result.is_err();
                    }
                }
            }
        };
        let canceled = async {
            loop {
                let response = registration.canceled.borrow_and_update().clone();
                if let Some(response) = response {
                    return response;
                }
                registration
                    .canceled
                    .changed()
                    .await
                    .expect("registration retains cancellation sender");
            }
        };
        tokio::select! {
            biased;
            response = canceled => Ok((response, OpenTelemetryContext::obtain())),
            result = execute => finish_peek(result, target),
        }
    }

    pub(crate) fn register_peek(&self, uuid: Uuid) -> QueryPeekRegistration {
        let (tx, canceled) = watch::channel(None);
        let previous = self
            .pending_peeks
            .lock()
            .expect("query peeks mutex poisoned")
            .insert(uuid, tx);
        assert!(previous.is_none(), "peek UUID cannot be reused");
        QueryPeekRegistration {
            uuid,
            pending: Arc::clone(&self.pending_peeks),
            canceled,
        }
    }

    pub(crate) fn cancel_peek(&self, uuid: Uuid, response: PeekResponse) -> bool {
        if let Some(sender) = self
            .pending_peeks
            .lock()
            .expect("query peeks mutex poisoned")
            .get(&uuid)
        {
            sender.send_replace(Some(response));
            true
        } else {
            false
        }
    }

    /// Query traffic can overtake catalog application on a slow replica. Use
    /// observed readable imports, waiting for missing observations rather than
    /// letting that replica's missing-index error beat a ready sibling's result.
    pub(crate) async fn readable_replicas(
        &self,
        cluster: ComputeInstanceId,
        target: Option<ReplicaId>,
        inputs: &BTreeSet<GlobalId>,
        timestamp: Timestamp,
    ) -> Result<Vec<ReplicaQueryClient>, AdapterError> {
        use futures::StreamExt;
        loop {
            let mut topology = self.connections.changes();
            self.connections.clients(cluster, target).await?;
            let (desired, clients) = self.connections.ready_snapshot(cluster, target);
            if clients.is_empty() {
                continue;
            }
            let pending = desired > clients.len();
            let mut changes: Vec<_> = clients
                .iter()
                .map(|client| client.frontier_changes())
                .collect();
            for changes in &mut changes {
                changes.borrow_and_update();
            }
            let mut unknown = false;
            let mut ready = Vec::new();
            for client in clients {
                if !client.is_connected() {
                    unknown = true;
                    continue;
                }
                let mut readable = true;
                for id in inputs {
                    match client
                        .collection_frontiers(*id)
                        .ok()
                        .flatten()
                        .and_then(|f| f.read_frontier)
                    {
                        Some(since) => readable &= since.less_equal(&timestamp),
                        None => {
                            readable = false;
                            unknown = true;
                        }
                    }
                }
                if readable {
                    ready.push(client);
                }
            }
            if !ready.is_empty() {
                return Ok(ready);
            }
            if !unknown && !pending {
                return Err(AdapterError::CollectionUnreadable {
                    id: inputs
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", "),
                });
            }
            let mut notifications: futures::stream::FuturesUnordered<_> = changes
                .iter_mut()
                .map(|changes| changes.changed())
                .collect();
            tokio::select! {
                _ = topology.changed() => {},
                _ = notifications.next() => {},
            }
        }
    }
}

fn diagnostics(id: GlobalId) -> Diagnostics {
    Diagnostics {
        shard_name: id.to_string(),
        handle_purpose: "query frontier observation".into(),
    }
}

fn unavailable(id: GlobalId) -> AdapterError {
    AdapterError::CollectionUnreadable { id: id.to_string() }
}

pub(crate) fn is_target_replica_failure(error: &AdapterError, target: Option<ReplicaId>) -> bool {
    target.is_some()
        && matches!(
            error,
            AdapterError::Unstructured(error)
                if matches!(error.downcast_ref::<QueryError>(), Some(QueryError::Disconnected(_)))
        )
}

fn finish_peek(
    result: Result<(PeekResponse, OpenTelemetryContext), AdapterError>,
    target: Option<ReplicaId>,
) -> Result<(PeekResponse, OpenTelemetryContext), AdapterError> {
    // Match controller peeks: losing the selected replica is a terminal execution
    // response, not a failure to plan or admit the query.
    match result {
        Err(error) if is_target_replica_failure(&error, target) => Ok((
            PeekResponse::Error(PeekError::unstructured(
                mz_compute_client::controller::error::ERROR_TARGET_REPLICA_FAILED,
            )),
            OpenTelemetryContext::obtain(),
        )),
        result => result,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Op;
    use crate::coord::Message;
    use crate::metrics::Metrics;
    use connections::QueryReplicaConnectionsConfig;
    use futures::stream::BoxStream;
    use mz_catalog::durable::{TestCatalogStateBuilder, test_bootstrap_args};
    use mz_ore::metrics::MetricsRegistry;
    use mz_ore::now::SYSTEM_TIME;
    use mz_repr::CatalogItemId;
    use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;
    use mz_storage_client::controller::StorageTxn;

    #[mz_ore::test]
    fn targeted_peek_disconnect_is_an_execution_error() {
        let disconnected = || {
            Err(AdapterError::Unstructured(
                compute::QueryError::Disconnected("removed".into()).into(),
            ))
        };
        let (response, _) = finish_peek(disconnected(), Some(ReplicaId::User(1)))
            .expect("target loss is a terminal peek response");
        assert!(
            matches!(response, PeekResponse::Error(PeekError::Unstructured(error))
            if error == mz_compute_client::controller::error::ERROR_TARGET_REPLICA_FAILED)
        );
        assert!(finish_peek(disconnected(), None).is_err());
    }

    // Storage-only acquisition must not consult replica lifecycle services.
    #[derive(Debug)]
    struct NoReplicas;

    #[async_trait::async_trait]
    impl mz_orchestrator::NamespacedOrchestrator for NoReplicas {
        fn service_addresses(
            &self,
            _: &str,
            _: std::num::NonZero<u16>,
            _: &mz_orchestrator::ServicePort,
        ) -> Result<Vec<String>, anyhow::Error> {
            unreachable!("storage-only read")
        }
        fn ensure_service(
            &self,
            _: &str,
            _: mz_orchestrator::ServiceConfig,
        ) -> Result<Box<dyn mz_orchestrator::Service>, anyhow::Error> {
            unreachable!("storage-only read")
        }
        fn drop_service(&self, _: &str) -> Result<(), anyhow::Error> {
            unreachable!("storage-only read")
        }
        async fn list_services(&self) -> Result<Vec<String>, anyhow::Error> {
            unreachable!("storage-only read")
        }
        fn watch_services(
            &self,
        ) -> BoxStream<'static, Result<mz_orchestrator::ServiceEvent, anyhow::Error>> {
            unreachable!("storage-only read")
        }
        async fn fetch_service_metrics(
            &self,
            _: &str,
        ) -> Result<Vec<mz_orchestrator::ServiceProcessMetrics>, anyhow::Error> {
            unreachable!("storage-only read")
        }
        fn update_scheduling_config(
            &self,
            _: mz_orchestrator::scheduling_config::ServiceSchedulingConfig,
        ) {
            unreachable!("storage-only read")
        }
    }

    #[mz_ore::test(tokio::test)]
    async fn historical_acquisition_publishes_before_returning_and_preserves_upper() {
        let persist = PersistClient::new_for_tests().await;
        let bootstrap = test_bootstrap_args();
        let organization = Uuid::new_v4();
        let mut storage = TestCatalogStateBuilder::new(persist.clone())
            .with_organization_id(organization)
            .with_default_deploy_generation()
            .unwrap_build()
            .await
            .open(SYSTEM_TIME().into(), &bootstrap)
            .await
            .expect("can open test catalog");
        // The adapter projection is reconstructed below. Drain initialization
        // updates before opening another transaction on the durable writer.
        storage
            .sync_to_current_updates()
            .await
            .expect("can consume catalog initialization");
        let id = GlobalId::User(100_000);
        let shard = ShardId::new();
        let incarnation = {
            let mut tx = storage
                .transaction()
                .await
                .expect("can start creation transaction");
            let schema = tx
                .get_schemas()
                .find(|s| s.name == "public")
                .expect("public schema exists");
            tx.insert_item(
                CatalogItemId::User(100_000),
                mz_pgrepr::oid::FIRST_USER_OID,
                id,
                schema.id,
                "history",
                "CREATE MATERIALIZED VIEW materialize.public.history IN CLUSTER quickstart AS SELECT 1 AS a".into(),
                MZ_SYSTEM_ROLE_ID,
                vec![],
                BTreeMap::new(),
                None,
            ).expect("can insert test MV");
            // The native catalog harness does not provision storage. Install the
            // shard identity and its initial permission in the same transaction.
            tx.insert_collection_metadata(BTreeMap::from([(id, shard)]))
                .expect("can install shard metadata");
            tx.set_collection_compaction_bound(id, Some(Timestamp::from(40)))
                .expect("can set initial permission");
            let incarnation = tx
                .create_client_incarnation()
                .expect("can create client incarnation");
            tx.publish_client_read_requirements(
                incarnation,
                BTreeMap::from([(id, Timestamp::from(100))]),
            )
            .expect("can publish initial grant");
            let _ = tx.get_and_commit_op_updates();
            let ts = tx.upper();
            tx.commit(ts).await.expect("can commit creation");
            incarnation
        };
        storage.expire().await;
        let storage = TestCatalogStateBuilder::new(persist.clone())
            .with_organization_id(organization)
            .with_default_deploy_generation()
            .unwrap_build()
            .await
            .open(SYSTEM_TIME().into(), &bootstrap)
            .await
            .expect("can reopen seeded catalog");
        let mut catalog = Box::pin(Catalog::open_debug_catalog_inner(
            persist.clone(),
            storage,
            SYSTEM_TIME.clone(),
            Some(
                format!("local-az1-{organization}-0")
                    .parse()
                    .expect("valid test environment ID"),
            ),
            &mz_build_info::DUMMY_BUILD_INFO,
            BTreeMap::from([("enable_catalog_read_protection".into(), "true".into())]),
            &bootstrap,
            None,
            None,
        ))
        .await
        .expect("can reconstruct debug catalog");
        let (tx, mut commands) = tokio::sync::mpsc::unbounded_channel();
        let client = QueryClient::new(
            incarnation,
            CoordinatorClient::Background {
                tx,
                metrics: Metrics::register_into(&MetricsRegistry::new()),
            },
            persist.clone(),
            PersistLocation {
                blob_uri: "mem://".parse().expect("valid memory blob URL"),
                consensus_uri: "mem://".parse().expect("valid memory consensus URL"),
            },
            ShardId::new(),
            Arc::new(QueryReplicaConnections::new(
                QueryReplicaConnectionsConfig {
                    orchestrator: Arc::new(NoReplicas),
                    deploy_generation: 0,
                    build_info: &mz_build_info::DUMMY_BUILD_INFO,
                },
            )),
        );
        let metadata = client
            .collection_metadata(&catalog, id)
            .expect("collection metadata exists");
        assert!(metadata.txns_shard.is_none());
        let (mut writer, mut reader) = persist
            .open::<SourceData, (), Timestamp, StorageDiff>(
                shard,
                Arc::new(metadata.relation_desc),
                Arc::new(mz_persist_types::codec_impls::UnitSchema),
                diagnostics(id),
                false,
            )
            .await
            .expect("can open Persist shard");
        let frontier = |t| Antichain::from_elem(Timestamp::from(t));
        writer
            .compare_and_append(
                Vec::<((SourceData, ()), Timestamp, StorageDiff)>::new(),
                frontier(0),
                frontier(80),
            )
            .await
            .expect("valid append usage")
            .expect("initial upper matches");
        reader.downgrade_since(&frontier(40)).await;
        assert_eq!(
            persist
                .recent_since::<SourceData, (), Timestamp, StorageDiff>(shard, diagnostics(id))
                .await
                .expect("can observe Persist since"),
            frontier(40)
        );
        assert_eq!(
            catalog.state().collection_compaction_bounds()[&id],
            frontier(40)
        );
        client
            .protection
            .prepare_publication(BTreeMap::from([(id, Timestamp::from(100))]));
        client.protection.finish_publication(true);
        let bundle = CollectionIdBundle {
            storage_ids: BTreeSet::from([id]),
            compute_ids: BTreeMap::new(),
        };
        let snapshot = catalog.clone();
        let read = client.acquire_read_holds_and_upper(&snapshot, &bundle, |upper| {
            assert_eq!(upper, &frontier(80));
            Ok(Some(Timestamp::from(50)))
        });
        tokio::pin!(read);
        let command = tokio::select! {
            result = &mut read => panic!("historical read bypassed publication: {result:?}"),
            command = commands.recv() => command.expect("coordinator receives acquisition"),
        };
        let Message::Command(
            _,
            Command::AcquireClientReadProtection {
                incarnation: requested_incarnation,
                bundle: requested_bundle,
                read_ts,
                tx,
            },
        ) = command
        else {
            panic!("expected acquisition miss")
        };
        assert_eq!(requested_incarnation, incarnation);
        assert_eq!(requested_bundle.storage_ids, bundle.storage_ids);
        assert_eq!(requested_bundle.compute_ids, bundle.compute_ids);
        assert_eq!(read_ts, Some(Timestamp::from(50)));

        // Service the miss with a real catalog commit, without controller workers.
        // The writer re-observes readability and permission, not the cached G=100.
        writer
            .compare_and_append(
                Vec::<((SourceData, ()), Timestamp, StorageDiff)>::new(),
                frontier(80),
                frontier(90),
            )
            .await
            .expect("valid append usage")
            .expect("upper matches before advancing");
        let prepared = client
            .prepare_read(&catalog, &requested_bundle, |_| Ok(read_ts))
            .await
            .expect("can prepare historical acquisition");
        assert_eq!(prepared.frontiers[&id], Timestamp::from(50));
        assert_eq!(prepared.upper, frontier(90));
        let requirements = client
            .protection
            .prepare_publication(prepared.frontiers.clone());
        assert!(
            client
                .protection
                .try_acquire(&bundle, &prepared.frontiers, &prepared.index_inputs)
                .expect("client remains open")
                .is_none()
        );
        let ts = catalog.current_upper().await;
        catalog
            .transact(
                None,
                ts,
                None,
                vec![Op::PublishClientReadRequirements {
                    incarnation,
                    requirements,
                }],
            )
            .await
            .expect("can commit historical grant");
        client.protection.finish_publication(true);
        let holds = client
            .protection
            .try_acquire(&bundle, &prepared.frontiers, &prepared.index_inputs)
            .expect("client remains open")
            .expect("committed grant covers the read");
        tx.send(Ok((holds, prepared.upper)))
            .expect("read still awaits publication");
        let (holds, upper) = read.await.expect("historical acquisition succeeds");
        assert_eq!(holds.since(&id), frontier(50));
        assert_eq!(upper, frontier(80));
        drop(holds);

        let ordinary = client
            .acquire_read_holds_and_upper(&catalog, &bundle, |_| Ok(Some(Timestamp::from(120))));
        tokio::pin!(ordinary);
        let (holds, upper) = tokio::select! {
            biased;
            command = commands.recv() => panic!("covered read published: {command:?}"),
            result = &mut ordinary => result.expect("covered acquisition succeeds"),
        };
        assert_eq!(holds.since(&id), frontier(50));
        assert_eq!(upper, frontier(90));
        assert!(matches!(
            commands.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        reader.expire().await;
        writer.expire().await;
    }
}
