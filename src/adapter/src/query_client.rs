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

use futures::future::{BoxFuture, FutureExt};
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
use mz_storage_client::collection_reader::CollectionReader;
use mz_storage_types::StorageDiff;
use mz_storage_types::controller::{CollectionMetadata, TxnsCodecRow};
use mz_storage_types::sources::SourceData;
use mz_txn_wal::txn_read::TxnsRead;
use timely::PartialOrder;
use timely::progress::Antichain;
use tokio::sync::{OnceCell, oneshot, watch};
use uuid::Uuid;

use crate::catalog::{Catalog, CatalogState};
use crate::command::Command;
use crate::optimize::dataflows::ComputeInstanceSnapshot;
use crate::peek_client::CoordinatorClient;
use crate::{AdapterError, CollectionIdBundle, ReadHolds};

pub(crate) mod compute;
pub(crate) mod connections;
pub(crate) mod dataflows;
pub(crate) mod read_protection;
mod rtr;
mod storage;

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

impl PreparedRead {
    /// A definitive permission rejection is retryable only when publication
    /// overtook an observed floor. Repreparation must retain `read_ts`, and the
    /// caller must still validate its timestamp against the resulting holds.
    pub(crate) async fn retry_publication(
        &self,
        client: &QueryClient,
        catalog: &Catalog,
        error: &AdapterError,
    ) -> Result<Option<Self>, AdapterError> {
        let raced = matches!(error, AdapterError::Catalog(error) if matches!(
            &error.kind,
            mz_catalog::memory::error::ErrorKind::Durable(
                mz_catalog::durable::DurableCatalogError::InvalidReadProtection(_)
            )
        )) && self.frontiers.iter().any(|(id, frontier)| {
            catalog
                .state()
                .collection_compaction_bounds()
                .get(id)
                .is_some_and(|bound| !bound.less_equal(frontier))
        });
        if !raced
            || !catalog
                .state()
                .client_incarnations()
                .contains_key(&client.protection.incarnation())
        {
            return Ok(None);
        }
        let fresh = client
            .prepare_read(catalog, &self.bundle, |_| Ok(self.read_ts))
            .await?;
        // Cached grants or unrelated validation failures must not cause an
        // endless retry. A permission race changes acquisition requirements.
        Ok((fresh.frontiers != self.frontiers).then_some(fresh))
    }
}

#[derive(Debug)]
pub(crate) struct QueryClient {
    coordinator: CoordinatorClient,
    pub(crate) protection: ClientReadProtection,
    persist: PersistClient,
    persist_location: PersistLocation,
    txns_shard: ShardId,
    collection_reader: OnceCell<CollectionReader>,
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
            collection_reader: OnceCell::new(),
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

    /// Return indexes eligible for a maintained plan at `read_ts`. Any published
    /// permission must allow the timestamp, and every selected ready replica must
    /// report a readable trace. At least one selected replica must be ready.
    ///
    /// These cached observations neither wait for replicas nor grant protection.
    /// Callers must acquire read protection before relying on the selected paths.
    pub(crate) fn maintained_indexes_at(
        &self,
        catalog: &CatalogState,
        cluster: ComputeInstanceId,
        target: Option<ReplicaId>,
        read_ts: Timestamp,
    ) -> BTreeSet<GlobalId> {
        let replicas = self.replica_clients(cluster, target);
        if replicas.is_empty() {
            return BTreeSet::new();
        }
        catalog
            .get_entries()
            .filter_map(|(_, entry)| match entry.item() {
                CatalogItem::Index(index) if index.cluster_id == cluster => Some(index.global_id()),
                _ => None,
            })
            .filter(|id| {
                catalog
                    .collection_compaction_bounds()
                    .get(id)
                    .is_none_or(|bound| bound.less_equal(&read_ts))
                    // Maintained dataflows install on every selected replica,
                    // unlike a peek that can use one readable sibling.
                    && replicas.iter().all(|replica| {
                        replica
                            .collection_frontiers(*id)
                            .ok()
                            .flatten()
                            .and_then(|frontiers| frontiers.read_frontier)
                            .is_some_and(|since| since.less_equal(&read_ts))
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

    /// Initializes the independent WAL reader on first use, after WAL bootstrap.
    /// Callers must await this off the coordinator loop, within their read timeout.
    pub(crate) async fn collection_reader(&self) -> &CollectionReader {
        self.collection_reader
            .get_or_init(|| async {
                let txns =
                    TxnsRead::start::<TxnsCodecRow>(self.persist.clone(), self.txns_shard).await;
                CollectionReader::new(self.persist.clone(), txns)
            })
            .await
    }

    pub(crate) fn collection_metadata(
        &self,
        catalog: &Catalog,
        id: GlobalId,
    ) -> Result<CollectionMetadata, AdapterError> {
        let entry = catalog
            .try_get_entry_by_global_id(&id)
            .ok_or_else(|| unavailable(id))?;
        let session = catalog.for_system_session();
        let collection = session
            .try_get_item_by_global_id(&id)
            .ok_or_else(|| unavailable(id))?;
        let relation_desc = if matches!(entry.item(), CatalogItem::Sink(_)) {
            mz_storage_types::sources::kafka::KAFKA_PROGRESS_DESC.clone()
        } else {
            collection
                .relation_desc()
                .ok_or_else(|| unavailable(id))?
                .into_owned()
        };
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

    /// Diagnostic observations only. Call off the coordinator, within the
    /// request's cancellation/deadline boundary, and only when requested.
    /// Missing observations are independent for read and write. In particular,
    /// protection grants and catalog permissions are not physical read frontiers.
    pub(crate) async fn explain_timestamp(
        &self,
        catalog: &Catalog,
        conn_id: &mz_adapter_types::connection::ConnectionId,
        session_wall_time: chrono::DateTime<chrono::Utc>,
        bundle: &CollectionIdBundle,
        determination: crate::coord::timestamp_selection::TimestampDetermination,
    ) -> crate::TimestampExplanation {
        use crate::coord::timestamp_selection::TimestampSource;

        let name = |id, kind| {
            let name = catalog
                .try_get_entry_by_global_id(&id)
                .map(|item| {
                    catalog
                        .resolve_full_name(item.name(), Some(conn_id))
                        .to_string()
                })
                .unwrap_or_else(|| id.to_string());
            format!("{name} ({id}, {kind})")
        };
        let mut sources = Vec::new();
        // This cache lasts only for this diagnostic request. Lazy table uppers
        // come from the WAL, but their physical sinces come from their data shards.
        let mut txns_upper = None;
        for id in &bundle.storage_ids {
            let (mut read_frontier, mut write_frontier) = (None, None);
            if let Ok(metadata) = self.collection_metadata(catalog, *id) {
                read_frontier = self
                    .persist
                    .recent_since::<SourceData, (), Timestamp, StorageDiff>(
                        metadata.data_shard,
                        diagnostics(*id),
                    )
                    .await
                    .ok()
                    .map(|f| f.elements().to_vec());
                write_frontier = if let Some(shard) = metadata.txns_shard {
                    if txns_upper.is_none() {
                        txns_upper = Some(
                            self.persist
                                .recent_upper::<SourceData, (), Timestamp, StorageDiff>(
                                    shard,
                                    diagnostics(*id),
                                )
                                .await
                                .ok()
                                .map(|f| f.elements().to_vec()),
                        );
                    }
                    txns_upper.as_ref().expect("observed WAL upper").clone()
                } else {
                    self.persist
                        .recent_upper::<SourceData, (), Timestamp, StorageDiff>(
                            metadata.data_shard,
                            diagnostics(*id),
                        )
                        .await
                        .ok()
                        .map(|f| f.elements().to_vec())
                };
            }
            sources.push(TimestampSource {
                name: name(*id, "storage"),
                read_frontier,
                write_frontier,
            });
        }
        for (cluster, ids) in &bundle.compute_ids {
            let replicas = self.replica_clients(*cluster, None);
            for id in ids {
                let mut since: Option<Antichain<Timestamp>> = None;
                for replica in &replicas {
                    if let Ok(Some(frontiers)) = replica.collection_frontiers(*id)
                        && let Some(frontier) = frontiers.read_frontier
                    {
                        since.get_or_insert_with(Antichain::new).extend(frontier);
                    }
                }
                sources.push(TimestampSource {
                    name: name(*id, "compute"),
                    read_frontier: since.map(|f| f.elements().to_vec()),
                    write_frontier: Self::observed_compute_frontier(&replicas, *id)
                        .map(|f| f.elements().to_vec()),
                });
            }
        }
        crate::TimestampExplanation {
            respond_immediately: determination.respond_immediately(),
            determination,
            sources,
            session_wall_time,
        }
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
                let write = Self::observed_compute_frontier(&replicas, *id)
                    .unwrap_or_else(|| Antichain::from_elem(Timestamp::MIN));
                upper.extend(write);
            }
        }
        Ok(upper)
    }

    fn observed_compute_frontier(
        replicas: &[ReplicaQueryClient],
        id: GlobalId,
    ) -> Option<Antichain<Timestamp>> {
        let mut upper = None;
        for replica in replicas {
            if let Ok(Some(frontiers)) = replica.collection_frontiers(id)
                && let Some(frontier) = frontiers.write_frontier
                && upper
                    .as_ref()
                    .is_none_or(|current| PartialOrder::less_than(current, &frontier))
            {
                upper = Some(frontier);
            }
        }
        upper
    }

    /// Waits for an actual compute observation, optionally beyond a timestamp.
    /// Unknown installation is not an observation at MIN.
    pub(crate) async fn wait_for_compute_frontier(
        &self,
        cluster: ComputeInstanceId,
        id: GlobalId,
        after: Option<Timestamp>,
    ) -> Antichain<Timestamp> {
        use futures::StreamExt;
        loop {
            let mut topology = self.connections.changes();
            let replicas = self.replica_clients(cluster, None);
            let changes: Vec<_> = replicas
                .iter()
                .map(|replica| replica.frontier_changes())
                .collect();
            if let Some(upper) = Self::observed_compute_frontier(&replicas, id)
                && after.is_none_or(|timestamp| !upper.less_equal(&timestamp))
            {
                return upper;
            }
            let mut notifications: futures::stream::FuturesUnordered<_> = changes
                .into_iter()
                .map(|mut changed| async move {
                    let _ = changed.changed().await;
                })
                .collect();
            tokio::select! {
                _ = topology.changed() => {},
                _ = notifications.next(), if !notifications.is_empty() => {},
                _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {},
            }
        }
    }

    /// Waits for durable storage or observed compute progress, without read holds.
    pub(crate) async fn wait_for_progress(
        &self,
        catalog: &Catalog,
        bundle: &CollectionIdBundle,
        timestamp: Timestamp,
    ) -> Result<(), AdapterError> {
        let mut shards = BTreeSet::new();
        for id in &bundle.storage_ids {
            let metadata = self.collection_metadata(catalog, *id)?;
            shards.insert(metadata.txns_shard.unwrap_or(metadata.data_shard));
        }
        let mut waits: Vec<BoxFuture<'_, Result<(), AdapterError>>> = Vec::new();
        for shard in shards {
            waits.push(
                async move {
                    self.persist
                        .wait_for_upper_past::<SourceData, (), Timestamp, StorageDiff>(
                            shard,
                            &Antichain::from_elem(timestamp),
                            Diagnostics {
                                shard_name: shard.to_string(),
                                handle_purpose: "query progress".into(),
                            },
                        )
                        .await
                        .map_err(|error| AdapterError::Unstructured(error.into()))
                }
                .boxed(),
            );
        }
        for (cluster, ids) in &bundle.compute_ids {
            for id in ids {
                waits.push(
                    async move {
                        self.wait_for_compute_frontier(*cluster, *id, Some(timestamp))
                            .await;
                        Ok(())
                    }
                    .boxed(),
                );
            }
        }
        futures::future::try_join_all(waits).await?;
        Ok(())
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
    use mz_sql::session::metadata::SessionMetadata;
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
        acquisition_catalog_harness(None).await;
    }

    #[mz_ore::test(tokio::test)]
    async fn competing_permission_publication_reobserves_without_reselecting_timestamp() {
        for read_ts in [None, Some(Timestamp::from(50))] {
            acquisition_catalog_harness(Some(read_ts)).await;
        }
    }

    async fn acquisition_catalog_harness(race: Option<Option<Timestamp>>) {
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
            tx.set_collection_compaction_bound(id, Some(Timestamp::MIN))
                .expect("can set initial permission");
            if race.is_some() {
                tx.set_config("catalog_read_protection_enabled".into(), Some(1))
                    .expect("can enable joined protection writers");
            }
            let incarnation = tx
                .create_client_incarnation()
                .expect("can create client incarnation");
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
        let bundle = CollectionIdBundle {
            storage_ids: BTreeSet::from([id]),
            compute_ids: BTreeMap::new(),
        };
        if let Some(read_ts) = race {
            let selected = std::cell::Cell::new(0);
            let prepared = client
                .prepare_read(&catalog, &bundle, |_| {
                    selected.set(selected.get() + 1);
                    Ok(read_ts)
                })
                .await
                .expect("can observe candidate before competing publication");
            let requirements = client
                .protection
                .prepare_publication(prepared.frontiers.clone());

            // Deterministically interleave a real permission commit between
            // observation and grant publication. No grant protects the candidate.
            let mut peer = TestCatalogStateBuilder::new(persist.clone())
                .with_organization_id(organization)
                .with_default_deploy_generation()
                .unwrap_build()
                .await
                .join()
                .await
                .expect("peer joins the active generation");
            peer.sync_to_current_updates()
                .await
                .expect("peer consumes its initial projection");
            let mut publication = peer.transaction().await.expect("peer starts publication");
            publication
                .set_collection_compaction_bound(id, Some(Timestamp::from(60)))
                .expect("peer can advance permission");
            let _ = publication.get_and_commit_op_updates();
            let ts = publication.upper();
            publication
                .commit(ts)
                .await
                .expect("peer publishes permission");
            reader.downgrade_since(&frontier(60)).await;
            let op = Op::PublishClientReadRequirements {
                incarnation,
                requirements,
            };
            let ts = catalog.current_upper().await;
            let conflict = catalog
                .transact(None, ts, None, vec![op.clone()])
                .await
                .err()
                .expect("peer publication invalidates the catalog snapshot");
            assert!(matches!(conflict, AdapterError::Catalog(error) if matches!(
                error.kind,
                mz_catalog::memory::error::ErrorKind::Durable(
                    mz_catalog::durable::DurableCatalogError::CatalogOutOfSync { .. }
                )
            )));
            catalog
                .sync_to_current_updates()
                .await
                .expect("writer consumes the competing permission");
            // The transaction retry refreshes the projection but retains the
            // original operation. Acquisition must rebuild after its rejection.
            let ts = catalog.current_upper().await;
            let error = catalog
                .transact(None, ts, None, vec![op])
                .await
                .err()
                .expect("stale acquisition must not grant compactable history");
            client.protection.finish_publication(false);
            assert_eq!(client.protection.granted_frontier(id), None);
            assert!(!client.protection.publication_pending());
            let fresh = prepared
                .retry_publication(&client, &catalog, &error)
                .await
                .expect("can reobserve after definitive rejection")
                .expect("advanced permission requires a fresh candidate");
            assert_eq!(selected.get(), 1);
            assert_eq!(fresh.read_ts, read_ts);
            assert_eq!(fresh.frontiers[&id], Timestamp::from(60));
            assert!(
                fresh
                    .retry_publication(&client, &catalog, &error)
                    .await
                    .expect("can classify unchanged rejection")
                    .is_none(),
                "InvalidReadProtection without a permission race is terminal"
            );
            assert!(
                prepared
                    .retry_publication(&client, &catalog, &AdapterError::ReadOnly)
                    .await
                    .expect("can classify unrelated failure")
                    .is_none()
            );
            let requirements = client
                .protection
                .prepare_publication(fresh.frontiers.clone());
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
                .expect("fresh acquisition commits");
            client.protection.finish_publication(true);
            let holds = client
                .protection
                .try_acquire(&bundle, &fresh.frontiers, &fresh.index_inputs)
                .expect("client remains open")
                .expect("fresh publication grants protection");
            assert_eq!(holds.since(&id), frontier(60));
            if let Some(read_ts) = read_ts {
                // Acquisition returns the actual floor. An exact historical
                // caller must reject it, not silently read at the newer floor.
                assert!(!holds.since(&id).less_equal(&read_ts));
            }
            drop(holds);

            let ts = catalog.current_upper().await;
            let expected_heartbeat = catalog.state().client_incarnations()[&incarnation];
            catalog
                .transact(
                    None,
                    ts,
                    None,
                    vec![Op::ReclaimClientIncarnation {
                        incarnation,
                        expected_heartbeat,
                    }],
                )
                .await
                .expect("can close incarnation");
            assert!(
                prepared
                    .retry_publication(&client, &catalog, &error)
                    .await
                    .expect("can classify closure")
                    .is_none(),
                "closed incarnations do not retry"
            );
            peer.expire().await;
            reader.expire().await;
            writer.expire().await;
            return;
        }
        // A timeline window requests its oracle floor on first acquisition,
        // even when the collection is readable all the way back to MIN.
        let initial = client
            .prepare_read(&catalog, &bundle, |_| Ok(Some(Timestamp::from(100))))
            .await
            .expect("can prepare initial oracle window");
        assert_eq!(
            initial.frontiers,
            BTreeMap::from([(id, Timestamp::from(100))])
        );
        assert!(
            client
                .protection
                .try_acquire(&bundle, &initial.frontiers, &initial.index_inputs)
                .expect("open")
                .is_none(),
            "observing the oracle window is not a grant"
        );
        let requirements = client.protection.prepare_publication(initial.frontiers);
        let ts = catalog.current_upper().await;
        catalog
            .transact(
                None,
                ts,
                None,
                vec![
                    Op::PublishClientReadRequirements {
                        incarnation,
                        requirements,
                    },
                    Op::SetReadProtection {
                        requirements: vec![],
                        bounds: vec![mz_catalog::durable::objects::CollectionCompactionBound {
                            id,
                            frontier: Some(Timestamp::from(40)),
                        }],
                    },
                ],
            )
            .await
            .expect("can grant oracle window and advance initial permission");
        client.protection.finish_publication(true);
        assert_eq!(
            client.protection.granted_frontier(id),
            Some(Timestamp::from(100))
        );
        assert_eq!(
            catalog.state().client_read_requirements()[&(incarnation, id)],
            Timestamp::from(100)
        );
        // Neither the grant at 100 nor the permission at 40 is the physical
        // since, which remains MIN until the reader advances it.
        let explanation = client
            .explain_timestamp(
                &catalog,
                crate::session::Session::dummy().conn_id(),
                chrono::DateTime::UNIX_EPOCH,
                &CollectionIdBundle {
                    storage_ids: BTreeSet::from([id, GlobalId::User(100_001)]),
                    compute_ids: BTreeMap::from([(
                        ComputeInstanceId::User(1),
                        BTreeSet::from([GlobalId::User(100_002)]),
                    )]),
                },
                crate::coord::timestamp_selection::TimestampDetermination {
                    timestamp_context:
                        crate::coord::timestamp_selection::TimestampContext::NoTimestamp,
                    since: frontier(100),
                    upper: frontier(80),
                    largest_not_in_advance_of_upper: Timestamp::from(79),
                    oracle_read_ts: Some(Timestamp::from(100)),
                    session_oracle_read_ts: None,
                    real_time_recency_ts: None,
                    constraints: Default::default(),
                },
            )
            .await;
        assert_eq!(
            explanation.sources[0].read_frontier,
            Some(vec![Timestamp::MIN])
        );
        assert_eq!(
            explanation.sources[0].write_frontier,
            Some(vec![Timestamp::from(80)])
        );
        for source in &explanation.sources[1..] {
            assert_eq!(source.read_frontier, None);
            assert_eq!(source.write_frontier, None);
        }
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

        let (holds, upper) = {
            let ordinary = client.acquire_read_holds_and_upper(&catalog, &bundle, |_| {
                Ok(Some(Timestamp::from(120)))
            });
            tokio::pin!(ordinary);
            tokio::select! {
                biased;
                command = commands.recv() => panic!("covered read published: {command:?}"),
                result = &mut ordinary => result.expect("covered acquisition succeeds"),
            }
        };
        assert_eq!(holds.since(&id), frontier(50));
        assert_eq!(upper, frontier(90));
        assert!(matches!(
            commands.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        let mut holds = holds;
        holds.downgrade(Timestamp::from(120));
        // An advancing aggregate and an idle renewal both bump the heartbeat in
        // the same catalog transaction as the requirements.
        for elapsed in [
            std::time::Duration::from_secs(1),
            read_protection::CLIENT_PROTECTION_HEARTBEAT_INTERVAL,
        ] {
            let requirements = client
                .protection
                .prepare_publication_if_needed(elapsed)
                .expect("advancement or renewal is due");
            assert_eq!(requirements, BTreeMap::from([(id, Timestamp::from(120))]));
            let heartbeat = catalog.state().client_incarnations()[&incarnation];
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
                .expect("can publish aggregate and heartbeat");
            client.protection.finish_publication(true);
            client.published();
            assert_eq!(
                catalog.state().client_incarnations()[&incarnation],
                heartbeat + 1
            );
            assert_eq!(
                client.protection.granted_frontier(id),
                Some(Timestamp::from(120))
            );
            assert_eq!(
                catalog.state().client_read_requirements()[&(incarnation, id)],
                Timestamp::from(120)
            );
            assert_eq!(
                client
                    .protection
                    .prepare_publication_if_needed(client.last_publication().elapsed()),
                None
            );
        }
        writer
            .compare_and_append(
                Vec::<((SourceData, ()), Timestamp, StorageDiff)>::new(),
                frontier(90),
                Antichain::new(),
            )
            .await
            .expect("valid append usage")
            .expect("upper matches before sealing");
        let sealed = client
            .explain_timestamp(
                &catalog,
                crate::session::Session::dummy().conn_id(),
                explanation.session_wall_time,
                &bundle,
                explanation.determination,
            )
            .await;
        assert_eq!(
            sealed.sources[0].read_frontier,
            Some(vec![Timestamp::from(40)])
        );
        assert_eq!(sealed.sources[0].write_frontier, Some(vec![]));
        reader.expire().await;
        writer.expire().await;
    }
}
