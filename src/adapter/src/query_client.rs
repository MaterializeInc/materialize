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
use mz_compute_client::protocol::response::PeekResponse;
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

use compute::ReplicaQueryClient;
use connections::QueryReplicaConnections;
use read_protection::ClientReadProtection;

#[derive(Debug)]
pub(crate) struct PreparedRead {
    pub(crate) bundle: CollectionIdBundle,
    pub(crate) frontiers: BTreeMap<GlobalId, Timestamp>,
    pub(crate) index_inputs: BTreeMap<GlobalId, BTreeSet<GlobalId>>,
    pub(crate) upper: Antichain<Timestamp>,
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
    pub(crate) async fn prepare_read(
        &self,
        catalog: &Catalog,
        bundle: &CollectionIdBundle,
    ) -> Result<PreparedRead, AdapterError> {
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
            let frontier = if let Some(granted) = self.protection.granted_frontier(id) {
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
                since.into_option().ok_or_else(|| unavailable(id))?
            };
            frontiers.insert(id, frontier);
        }
        let upper = self.write_frontier(catalog, bundle).await?;
        for (cluster, ids) in &bundle.compute_ids {
            let replicas = self.replica_clients(*cluster, None);
            for id in ids {
                let mut since = self
                    .protection
                    .granted_frontier(*id)
                    .unwrap_or(Timestamp::MIN);
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
        })
    }

    pub(crate) async fn acquire_read_holds_and_upper(
        &self,
        catalog: &Catalog,
        bundle: &CollectionIdBundle,
    ) -> Result<(ReadHolds, Antichain<Timestamp>), AdapterError> {
        let prepared = self.prepare_read(catalog, bundle).await?;
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
            tx,
        });
        rx.await
            .map_err(|error| AdapterError::Unstructured(error.into()))?
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
            result = execute => result,
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
            result = execute => result,
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
