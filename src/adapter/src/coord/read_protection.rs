// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Publishes durable recovery requirements and compaction permission together.

use std::collections::{BTreeMap, BTreeSet};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[cfg(test)]
use mz_catalog::durable::objects::{CollectionCompactionBound, MaintainedReadRequirement};
use mz_catalog::memory::objects::CatalogItem;
#[cfg(test)]
use mz_catalog::read_protection::publication::PublicationCandidates;
use mz_catalog::read_protection::publication::publication_candidates;
use mz_repr::{GlobalId, Timestamp};
#[cfg(test)]
use mz_storage_client::storage_collections::CollectionFrontiers;
use timely::progress::Antichain;

use crate::catalog::{BuiltinTableUpdate, Catalog, CatalogState, Op};
use crate::coord::{Coordinator, Message};
use crate::query_client::{PreparedRead, QueryClient};
use crate::{AdapterError, CollectionIdBundle, ReadHolds, TimelineContext};

pub(super) const CATALOG_SUBSCRIPTION_INTERVAL: Duration = Duration::from_secs(1);

type ReadProtectionResponse = Result<(ReadHolds, Antichain<Timestamp>), AdapterError>;

/// An ungranted request, not a pending catalog publication. Repreparation may
/// update the obtainable floor, but not the requested timestamp or first upper.
#[derive(Debug)]
pub(super) struct ReadProtectionRequest {
    incarnation: u64,
    bundle: CollectionIdBundle,
    read_ts: Option<Timestamp>,
    upper: Option<Antichain<Timestamp>>,
}

impl ReadProtectionRequest {
    pub(super) fn new(
        incarnation: u64,
        bundle: CollectionIdBundle,
        read_ts: Option<Timestamp>,
    ) -> Self {
        Self {
            incarnation,
            bundle,
            read_ts,
            upper: None,
        }
    }
}

/// The frontend caller owns timeout and cancellation by retaining the receiver.
#[derive(Debug)]
pub struct PendingReadProtection {
    request: ReadProtectionRequest,
    tx: tokio::sync::oneshot::Sender<ReadProtectionResponse>,
    otel_ctx: mz_ore::tracing::OpenTelemetryContext,
}

impl PendingReadProtection {
    async fn retry_after(
        mut self,
        sender: tokio::sync::mpsc::UnboundedSender<Message>,
        delay: Duration,
    ) {
        let retry = tokio::select! {
            _ = self.tx.closed() => false,
            _ = tokio::time::sleep(delay) => true,
        };
        if retry {
            let _ = sender.send(Message::ClientReadProtectionReady(Box::new(self)));
        }
    }
}

pub(super) fn is_read_protection_conflict(error: &AdapterError) -> bool {
    matches!(error, AdapterError::DDLTransactionRace)
        || matches!(error, AdapterError::Catalog(error) if matches!(
            &error.kind,
            mz_catalog::memory::error::ErrorKind::Durable(
                mz_catalog::durable::DurableCatalogError::CatalogOutOfSync { .. }
            )
        ))
}

/// Both timers publish the same client's protection. Neither may consume the
/// other's backoff, or attempts slower than the delay can alternate forever
/// ahead of queued messages in the coordinator's biased select.
pub(super) fn defer_protection_retry(
    retry: Pin<&mut tokio::time::Sleep>,
    mut sibling: Pin<&mut tokio::time::Sleep>,
    delay: Duration,
) {
    let deadline = tokio::time::Instant::now() + delay;
    retry.reset(deadline);
    let sibling_deadline = sibling.deadline();
    sibling.as_mut().reset(sibling_deadline.max(deadline));
}

/// Clip to the next renewal threshold, but never spin on an expired deadline.
/// Delaying an attempt does not extend the incarnation's protection validity.
fn limit_conflict_delay(delay: Duration, age: Duration) -> Duration {
    let heartbeat = mz_catalog::read_protection::client_protection_heartbeat_interval();
    let safety = mz_catalog::read_protection::client_protection_unchanged_grace() - heartbeat;
    let remaining = if age < heartbeat {
        heartbeat - age
    } else if age < safety {
        safety - age
    } else {
        delay
    };
    delay.min(remaining)
}

/// A pending publication that protects a serving timeline window at admission.
/// Finish only after a definitive transaction outcome. Cancellation must leave
/// the publication barrier in place, just like any unresolved catalog write.
pub(super) struct IndexTimelinePublication {
    client: Arc<QueryClient>,
    bundle: CollectionIdBundle,
    requested: BTreeMap<GlobalId, Timestamp>,
    inputs: BTreeMap<GlobalId, BTreeSet<GlobalId>>,
    requirements: BTreeMap<GlobalId, Timestamp>,
}

impl IndexTimelinePublication {
    pub(super) fn op(&self) -> Op {
        Op::PublishClientReadRequirements {
            incarnation: self.client.protection.incarnation(),
            requirements: self.requirements.clone(),
        }
    }

    pub(super) fn finish(self, committed: bool) -> ReadHolds {
        self.client.protection.finish_publication(committed);
        if !committed {
            return ReadHolds::new();
        }
        self.client.published();
        // Adopt every grant synchronously. Another publication aggregates active
        // tokens and could otherwise release an acknowledged but unused grant.
        self.client
            .protection
            .try_acquire(&self.bundle, &self.requested, &self.inputs)
            .expect("committed timeline publication has a live local client")
            .expect("committed timeline publication covers its requested tokens")
    }
}

enum ReadProtectionPublication<'a> {
    Runtime,
    Bootstrap(&'a mut Vec<BuiltinTableUpdate>),
}

impl ReadProtectionPublication<'_> {
    fn catalog<'a>(&self, coord: &'a Coordinator) -> &'a crate::catalog::Catalog {
        match self {
            Self::Runtime => coord.client_read_catalog(),
            Self::Bootstrap(_) => coord.catalog(),
        }
    }
}

impl Coordinator {
    pub(super) async fn start_client_read_protection(
        &mut self,
        incarnation: u64,
        bundle: CollectionIdBundle,
        read_ts: Option<Timestamp>,
        tx: tokio::sync::oneshot::Sender<ReadProtectionResponse>,
    ) {
        self.resume_client_read_protection(PendingReadProtection {
            request: ReadProtectionRequest::new(incarnation, bundle, read_ts),
            tx,
            otel_ctx: mz_ore::tracing::OpenTelemetryContext::obtain(),
        })
        .await;
    }

    pub(super) async fn resume_client_read_protection(
        &mut self,
        mut pending: PendingReadProtection,
    ) {
        pending.otel_ctx.attach_as_parent();
        if pending.tx.is_closed() {
            return;
        }
        let result = self
            .try_client_read_protection(&mut pending.request, || pending.tx.is_closed())
            .await;
        match result {
            Ok(Some(grant)) => {
                let _ = pending.tx.send(Ok(grant));
            }
            Err(error) => {
                let _ = pending.tx.send(Err(error));
            }
            Ok(None) => {
                let delay = self.read_protection_conflict_delay();
                let sender = self.internal_cmd_tx.clone();
                mz_ore::task::spawn(
                    || "retry_read_protection",
                    pending.retry_after(sender, delay),
                );
            }
        }
    }

    /// Attempts acquisition to a definitive outcome. `None` requires yielding
    /// before retrying, while `Some` returns holds and the first observed upper.
    /// The caller must still validate its timestamp against the returned holds.
    /// Cancellation is checked after preparation, never during publication.
    pub(super) async fn try_client_read_protection(
        &mut self,
        request: &mut ReadProtectionRequest,
        canceled: impl FnOnce() -> bool,
    ) -> Result<Option<(ReadHolds, Antichain<Timestamp>)>, AdapterError> {
        let client = self.query_client.clone().ok_or(AdapterError::ReadOnly)?;
        if client.protection.incarnation() != request.incarnation {
            return Err(AdapterError::internal(
                "query read protection",
                "incarnation is no longer active",
            ));
        }
        // Other requests and maintenance can change the catalog during the
        // wait. Rebuild the requested scope before using even a cached grant.
        let prepared = client
            .prepare_read(self.client_read_catalog(), &request.bundle, |_| {
                Ok(request.read_ts)
            })
            .await?;
        request.upper.get_or_insert_with(|| prepared.upper.clone());
        if canceled() {
            return Err(AdapterError::Canceled);
        }
        match self
            .try_acquire_prepared_read(
                &client,
                &prepared,
                &mut ReadProtectionPublication::Runtime,
                true,
            )
            .await
        {
            Ok(holds) => Ok(Some((
                holds,
                request.upper.clone().expect("prepared before grant"),
            ))),
            Err(error) => {
                if is_read_protection_conflict(&error)
                    || prepared
                        .retry_publication(&client, self.client_read_catalog(), &error)
                        .await?
                        .is_some()
                {
                    Ok(None)
                } else {
                    Err(error)
                }
            }
        }
    }

    pub(super) fn read_protection_conflict_delay(&self) -> Duration {
        let delay = mz_catalog::retry::sample_duration(
            Duration::from_millis(10),
            Duration::from_millis(100),
        );
        match &self.query_client {
            Some(client) => limit_conflict_delay(delay, client.last_publication().elapsed()),
            None => delay,
        }
    }

    /// Prepares client protection for indexes in a candidate or committed catalog.
    /// The catalog still owns admission and bound validation. These requirements
    /// preserve the serving adapter's oracle window, independently of installation.
    pub(super) async fn prepare_index_timeline_publication(
        &mut self,
        client: Arc<QueryClient>,
        candidate: &CatalogState,
        indexes: BTreeSet<GlobalId>,
    ) -> Result<IndexTimelinePublication, AdapterError> {
        let mut by_timeline = BTreeMap::new();
        for id in indexes {
            let Some(entry) = candidate.try_get_entry_by_global_id(&id) else {
                continue;
            };
            let CatalogItem::Index(index) = entry.item() else {
                continue;
            };
            let Some(floor) = candidate
                .collection_compaction_bounds()
                .get(&id)
                .and_then(|bound| bound.as_option())
                .copied()
            else {
                continue;
            };
            let inputs = candidate.logical_collection_inputs([index.on]);
            if inputs.iter().any(|id| {
                matches!(
                    candidate.get_entry_by_global_id(id).item(),
                    CatalogItem::Log(_)
                )
            }) {
                continue;
            }
            let context = Catalog::validate_timeline_context_in(candidate, [id])?;
            if let TimelineContext::TimelineDependent(timeline) = context {
                by_timeline.entry(timeline).or_insert_with(Vec::new).push((
                    id,
                    index.cluster_id,
                    floor,
                    inputs,
                ));
            }
        }
        let mut bundle = CollectionIdBundle::default();
        let mut requested = BTreeMap::new();
        let mut inputs = BTreeMap::new();
        for (timeline, indexes) in by_timeline {
            let read_ts = self
                .ensure_timeline_state(&timeline)
                .await
                .oracle
                .read_ts()
                .await;
            for (id, cluster, floor, leaves) in indexes {
                bundle.compute_ids.entry(cluster).or_default().insert(id);
                requested.insert(id, floor.max(read_ts));
                inputs.insert(id, leaves);
            }
        }
        let requested = candidate
            .expand_client_read_requirements(client.protection.incarnation(), requested)?;
        let requirements = client.protection.prepare_publication(requested.clone());
        Ok(IndexTimelinePublication {
            client,
            bundle,
            requested,
            inputs,
            requirements,
        })
    }

    /// Transfers admission tokens into windows established by committed catalog
    /// implications. Failed asynchronous acquisition must not discard birth grants.
    pub(super) fn adopt_index_timeline_holds(&mut self, holds: ReadHolds) {
        let protected = holds.id_bundle();
        for state in self.global_timelines.values_mut() {
            let pending = state.pending_read_holds.intersection(&protected);
            let missing = pending.difference(&state.read_holds.id_bundle());
            state.read_holds.extend(holds.subset(&missing));
            state.pending_read_holds = state.pending_read_holds.difference(&pending);
        }
    }

    fn client_read_catalog(&self) -> &crate::catalog::Catalog {
        self.client_protection_catalog
            .as_ref()
            .unwrap_or_else(|| self.catalog())
    }

    /// Client metadata is live even when SQL uses a prewarming savepoint. This
    /// writer validates its own projection but does not enact maintained objects.
    async fn transact_client_protection(&mut self, op: Op) -> Result<Vec<u64>, AdapterError> {
        self.transact_client_protection_inner(op, false).await
    }

    async fn sync_client_protection_catalog(&mut self) -> Result<(), AdapterError> {
        let catalog = self
            .client_protection_catalog
            .as_mut()
            .expect("private client protection writer exists");
        if let Err(error) = catalog.sync_to_current_updates().await {
            if matches!(
                &error,
                mz_catalog::durable::CatalogError::Durable(
                    mz_catalog::durable::DurableCatalogError::Fence(_)
                )
            ) {
                // A cached incarnation is not evidence that this writer can
                // still renew protection or issue protected grants.
                if let Some(client) = &self.query_client {
                    client.protection.mark_closed();
                }
            }
            return Err(error.into());
        }
        Ok(())
    }

    /// Timer owners request one attempt and rebuild the aggregate after yielding.
    /// Bootstrap and foreground acquisition retain their inline conflict recovery.
    async fn transact_client_protection_inner(
        &mut self,
        op: Op,
        once: bool,
    ) -> Result<Vec<u64>, AdapterError> {
        assert!(matches!(
            &op,
            Op::CreateClientIncarnation { .. } | Op::PublishClientReadRequirements { .. }
        ));
        if self.client_protection_catalog.is_none() {
            if once {
                return self.catalog_transact_once(vec![op]).await;
            }
            return self
                .catalog_transact_with_results(None, None, vec![op])
                .await;
        }
        loop {
            self.sync_client_protection_catalog().await?;
            let catalog = self
                .client_protection_catalog
                .as_mut()
                .expect("checked above");
            let ts = catalog.current_upper().await;
            match catalog
                .transact(
                    Some(&mut self.controller.storage_collections),
                    ts,
                    None,
                    vec![op.clone()],
                )
                .await
            {
                Ok(result) => return Ok(result.created_client_incarnations),
                Err(error)
                    if matches!(&error, AdapterError::Catalog(error) if matches!(
                        &error.kind,
                        mz_catalog::memory::error::ErrorKind::Durable(
                            mz_catalog::durable::DurableCatalogError::CatalogOutOfSync { .. }
                        )
                    )) =>
                {
                    if once {
                        self.sync_client_protection_catalog().await?;
                        return Err(error);
                    }
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Builds a client for an existing incarnation without activating runtime routing.
    pub(super) async fn build_query_client(
        &self,
        incarnation: u64,
    ) -> Result<Arc<QueryClient>, AdapterError> {
        use crate::peek_client::CoordinatorClient;
        use crate::query_client::connections::{
            QueryReplicaConnections, QueryReplicaConnectionsConfig,
        };

        let txns_shard = self.catalog().txn_wal_shard().await?;
        let connections = std::sync::Arc::new(QueryReplicaConnections::new(
            QueryReplicaConnectionsConfig {
                orchestrator: std::sync::Arc::clone(&self.query_orchestrator),
                deploy_generation: self.query_deploy_generation,
                build_info: self.catalog().config().build_info,
                observations: self
                    .controller
                    .replica_owned_compute()
                    .then(|| self.controller.storage.replica_observations()),
            },
        ));
        connections.sync_catalog(self.catalog());
        Ok(Arc::new(QueryClient::new(
            incarnation,
            CoordinatorClient::Background {
                tx: self.internal_cmd_tx.clone(),
                metrics: self.metrics.clone(),
            },
            self.persist_client.clone(),
            self.query_persist_location.clone(),
            txns_shard,
            connections,
        )))
    }

    pub(super) async fn initialize_query_client(
        &mut self,
        prepared: Option<Arc<QueryClient>>,
    ) -> Result<(), AdapterError> {
        use mz_ore::collections::CollectionExt;

        if !self.catalog().state().catalog_read_protection_enabled() {
            return Ok(());
        }
        let client = match prepared {
            Some(client) => {
                // Bootstrap may outlive an incarnation's reclamation grace.
                // Renew through the catalog before exposing its cached grants.
                let requirements = client.protection.prepare_publication(BTreeMap::new());
                let result = self
                    .transact_client_protection(Op::PublishClientReadRequirements {
                        incarnation: client.protection.incarnation(),
                        requirements,
                    })
                    .await;
                client.protection.finish_publication(result.is_ok());
                result?;
                client.published();
                client
            }
            None => {
                let incarnation = self
                    .transact_client_protection(Op::CreateClientIncarnation { replica_id: None })
                    .await?
                    .into_element();
                self.build_query_client(incarnation).await?
            }
        };
        self.query_client = Some(client);

        // Keep bootstrap constraints until the client has secured its readable
        // windows. Missing replicas must not block startup or turn installation
        // into a catalog-write handshake. Their index windows remain pending.
        let bootstrap_holds: Vec<_> = self
            .global_timelines
            .values_mut()
            .map(|state| {
                state
                    .pending_read_holds
                    .extend(&state.read_holds.id_bundle());
                std::mem::take(&mut state.read_holds)
            })
            .collect();
        self.acquire_pending_query_timeline_holds_inner(true)
            .await?;
        drop(bootstrap_holds);
        Ok(())
    }

    /// Establishes client-owned oracle windows from committed admission.
    /// Makes one attempt per eligible timeline, with retries owned by ordinary
    /// timeline maintenance rather than catalog implication processing.
    /// Unknown index frontiers remain pending without delaying healthy clusters.
    pub(super) async fn acquire_pending_query_timeline_holds(
        &mut self,
    ) -> Result<(), AdapterError> {
        self.acquire_pending_query_timeline_holds_inner(false).await
    }

    async fn acquire_pending_query_timeline_holds_inner(
        &mut self,
        retry_inline: bool,
    ) -> Result<(), AdapterError> {
        let Some(client) = self.query_client.clone() else {
            return Ok(());
        };
        if client.protection.publication_pending() {
            return Ok(());
        }
        fn retain_live(catalog: &crate::catalog::Catalog, ids: &mut crate::CollectionIdBundle) {
            ids.storage_ids
                .retain(|id| catalog.try_get_entry_by_global_id(id).is_some());
            for ids in ids.compute_ids.values_mut() {
                ids.retain(|id| catalog.try_get_entry_by_global_id(id).is_some());
            }
        }
        // In-flight work is outside TimelineState. A publication can consume
        // catalog drops, so membership must be rechecked before returning it.
        let pending: Vec<_> = self
            .global_timelines
            .iter_mut()
            .filter(|(_, state)| !state.pending_read_holds.is_empty())
            .filter(|(_, state)| {
                retry_inline
                    || state
                        .read_hold_retry_after
                        .is_none_or(|due| due <= Instant::now())
            })
            .map(|(timeline, state)| {
                (
                    timeline.clone(),
                    std::mem::take(&mut state.pending_read_holds),
                    std::sync::Arc::clone(&state.oracle),
                )
            })
            .collect();
        let mut first_error = None;
        for (timeline, mut ids, oracle) in pending {
            retain_live(self.client_read_catalog(), &mut ids);
            let Some(state) = self.global_timelines.get(&timeline) else {
                continue;
            };
            ids = ids.difference(&state.read_holds.id_bundle());
            let mut ready = ids.clone();
            for (cluster, ids) in &mut ready.compute_ids {
                let readable = client.readable_indexes(*cluster, ids);
                // Admission, not installation, authorizes persisted index holds.
                // Replica-local logs still use their observed trace readiness.
                ids.retain(|id| {
                    self.client_read_catalog()
                        .state()
                        .collection_compaction_bounds()
                        .get(id)
                        .is_some_and(|bound| !bound.is_empty())
                        || readable.contains(id)
                });
            }
            if !ready.is_empty() {
                // Protect the oracle window in the initial durable grant, rather
                // than publishing unused history below it. This is maintenance,
                // not a cached timestamp for subsequent queries.
                let read_ts = oracle.read_ts().await;
                let result = if retry_inline {
                    self.acquire_client_read_protection(
                        client.protection.incarnation(),
                        ready.clone(),
                        |_| Ok(Some(read_ts)),
                    )
                    .await
                    .map(|(holds, _)| holds)
                } else {
                    match client
                        .prepare_read(self.client_read_catalog(), &ready, |_| Ok(Some(read_ts)))
                        .await
                    {
                        Ok(prepared) => {
                            self.try_acquire_prepared_read(
                                &client,
                                &prepared,
                                &mut ReadProtectionPublication::Runtime,
                                true,
                            )
                            .await
                        }
                        Err(error) => Err(error),
                    }
                };
                match result {
                    Ok(holds) => {
                        // Publication can consume a peer drop. In-flight IDs were
                        // not in timeline state for its cleanup, so recheck them
                        // before installing a window or restoring pending work.
                        retain_live(self.client_read_catalog(), &mut ready);
                        // Index tokens keep their derived leaf protection. Do not
                        // insert a second direct leaf token already in the window.
                        let mut holds = holds.subset(&ready);
                        holds.downgrade(read_ts);
                        if let Some(state) = self.global_timelines.get_mut(&timeline) {
                            state.read_hold_retry_after = None;
                            let missing = ready.difference(&state.read_holds.id_bundle());
                            state.read_holds.extend(holds.subset(&missing));
                        }
                        ids = ids.difference(&ready);
                    }
                    Err(error) => {
                        if !retry_inline && is_read_protection_conflict(&error) {
                            let due = Instant::now() + self.read_protection_conflict_delay();
                            if let Some(state) = self.global_timelines.get_mut(&timeline) {
                                state.read_hold_retry_after = Some(due);
                            }
                        }
                        first_error.get_or_insert(error);
                    }
                }
            }
            retain_live(self.client_read_catalog(), &mut ids);
            if let Some(state) = self.global_timelines.get_mut(&timeline) {
                state.pending_read_holds.extend(&ids);
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    pub(crate) async fn acquire_client_read_protection(
        &mut self,
        incarnation: u64,
        bundle: crate::CollectionIdBundle,
        timestamp: impl FnOnce(&Antichain<Timestamp>) -> Result<Option<Timestamp>, AdapterError>,
    ) -> Result<(crate::ReadHolds, Antichain<Timestamp>), AdapterError> {
        let client = self.query_client.clone().ok_or(AdapterError::ReadOnly)?;
        if client.protection.incarnation() != incarnation {
            return Err(AdapterError::internal(
                "query read protection",
                "incarnation is no longer active",
            ));
        }
        self.acquire_read_protection(
            client,
            bundle,
            timestamp,
            ReadProtectionPublication::Runtime,
        )
        .await
    }

    /// Protects actual bootstrap plan imports without activating runtime routing.
    /// Keep the returned holds through selection commit, and publish the client's
    /// aggregate in that commit to reject an incarnation reclaimed in the meantime.
    /// A requested timestamp must still be validated against the returned holds.
    pub(super) async fn acquire_bootstrap_read_protection(
        &mut self,
        client: Arc<QueryClient>,
        bundle: crate::CollectionIdBundle,
        read_ts: Option<Timestamp>,
        builtin_table_updates: &mut Vec<BuiltinTableUpdate>,
    ) -> Result<(crate::ReadHolds, Antichain<Timestamp>), AdapterError> {
        self.acquire_read_protection(
            client,
            bundle,
            |_| Ok(read_ts),
            ReadProtectionPublication::Bootstrap(builtin_table_updates),
        )
        .await
    }

    async fn acquire_read_protection(
        &mut self,
        client: Arc<QueryClient>,
        bundle: crate::CollectionIdBundle,
        timestamp: impl FnOnce(&Antichain<Timestamp>) -> Result<Option<Timestamp>, AdapterError>,
        mut publication: ReadProtectionPublication<'_>,
    ) -> Result<(crate::ReadHolds, Antichain<Timestamp>), AdapterError> {
        let incarnation = client.protection.incarnation();
        if !publication
            .catalog(self)
            .state()
            .client_incarnations()
            .contains_key(&incarnation)
        {
            client.protection.mark_closed();
            return Err(AdapterError::internal(
                "query read protection",
                "incarnation is closed",
            ));
        }
        let mut prepared = client
            .prepare_read(publication.catalog(self), &bundle, timestamp)
            .await?;
        // Timestamp selection is FnOnce. Contention may change the obtainable
        // floor, but must not change the request or the upper that selected it.
        let upper = prepared.upper.clone();
        loop {
            match self
                .try_acquire_prepared_read(&client, &prepared, &mut publication, false)
                .await
            {
                Ok(holds) => return Ok((holds, upper)),
                Err(error) => {
                    if let Some(fresh) = prepared
                        .retry_publication(&client, publication.catalog(self), &error)
                        .await?
                    {
                        prepared = fresh;
                    } else {
                        return Err(error);
                    }
                }
            }
        }
    }

    /// No pending publication survives a definitive return. A caller requesting
    /// one attempt owns resampling and resumption after a catalog conflict.
    async fn try_acquire_prepared_read(
        &mut self,
        client: &Arc<QueryClient>,
        prepared: &PreparedRead,
        publication: &mut ReadProtectionPublication<'_>,
        once: bool,
    ) -> Result<ReadHolds, AdapterError> {
        let incarnation = client.protection.incarnation();
        if !publication
            .catalog(self)
            .state()
            .client_incarnations()
            .contains_key(&incarnation)
        {
            client.protection.mark_closed();
            return Err(AdapterError::internal(
                "query read protection",
                "incarnation is closed",
            ));
        }
        if let Some(holds) = client
            .protection
            .try_acquire(
                &prepared.bundle,
                &prepared.frontiers,
                &prepared.index_inputs,
            )
            .map_err(|error| AdapterError::Unstructured(error.into()))?
        {
            return Ok(holds);
        }
        let extra = publication
            .catalog(self)
            .state()
            .expand_client_read_requirements(incarnation, prepared.frontiers.clone())?;
        let requirements = client.protection.prepare_publication(extra);
        let op = Op::PublishClientReadRequirements {
            incarnation,
            requirements,
        };
        let result = match publication {
            ReadProtectionPublication::Runtime => {
                self.transact_client_protection_inner(op, once).await
            }
            ReadProtectionPublication::Bootstrap(updates) => {
                self.bootstrap_catalog_transact(vec![op], updates).await
            }
        };
        // Indeterminate outcomes terminate before this point. Only a definitive
        // result can release the pending barrier or authorize the local grant.
        client.protection.finish_publication(result.is_ok());
        if !publication
            .catalog(self)
            .state()
            .client_incarnations()
            .contains_key(&incarnation)
        {
            client.protection.mark_closed();
        }
        result?;
        client.published();
        client
            .protection
            .try_acquire(
                &prepared.bundle,
                &prepared.frontiers,
                &prepared.index_inputs,
            )
            .map_err(|error| AdapterError::Unstructured(error.into()))?
            .ok_or_else(|| {
                AdapterError::internal("query read protection", "published scope was not acquired")
            })
    }

    /// Attempts one publication of the client aggregate and heartbeat.
    pub(super) async fn publish_client_read_protection(&mut self) -> Result<(), AdapterError> {
        let Some(client) = self.query_client.clone() else {
            return Ok(());
        };
        let Some(requirements) = client
            .protection
            .prepare_publication_if_needed(client.last_publication().elapsed())
        else {
            return Ok(());
        };
        let incarnation = client.protection.incarnation();
        let result = self
            .transact_client_protection_inner(
                Op::PublishClientReadRequirements {
                    incarnation,
                    requirements,
                },
                true,
            )
            .await;
        // Definitive failure releases only the pending barrier, not committed
        // protection. No barrier survives the timer owner's yield between attempts.
        client.protection.finish_publication(result.is_ok());
        if !self
            .client_read_catalog()
            .state()
            .client_incarnations()
            .contains_key(&incarnation)
        {
            client.protection.mark_closed();
        }
        result?;
        client.published();
        Ok(())
    }

    pub(super) async fn reclaim_client_read_protection(&mut self) -> Result<(), AdapterError> {
        if self.controller.read_only() || !self.pending_compute_installations.is_empty() {
            return Ok(());
        }
        let active: Vec<_> = self
            .catalog()
            .state()
            .client_incarnations()
            .iter()
            .map(|(&id, value)| (id, value.heartbeat))
            .collect();
        let expired = self
            .client_protection_reclaimer
            .observe(active, Instant::now());
        if !expired.is_empty() {
            self.catalog_transact_once(
                expired
                    .into_iter()
                    .map(
                        |(incarnation, expected_heartbeat)| Op::ReclaimClientIncarnation {
                            incarnation,
                            expected_heartbeat,
                        },
                    )
                    .collect(),
            )
            .await?;
        }
        Ok(())
    }

    /// Restores published compute bounds before installing clusters and dataflows.
    pub(super) async fn restore_compute_read_protection(&mut self) -> Result<(), AdapterError> {
        if self.controller.replica_owned_compute() {
            return Ok(());
        }
        if !self.catalog().state().catalog_read_protection_enabled() {
            return Ok(());
        }
        let state = self.catalog().state();
        let storage = &state.storage_metadata().collection_metadata;
        let bounds = state
            .collection_compaction_bounds()
            .iter()
            .filter(|(id, _)| !storage.contains_key(*id))
            .map(|(&id, bound)| (id, bound.clone()))
            .collect::<Vec<_>>();
        for (id, bound) in bounds {
            self.controller
                .compute
                .apply_compaction_bound(id, bound)
                .map_err(|error| AdapterError::Unstructured(error.into()))?;
        }
        self.sync_compute_read_protection().await
    }

    /// Delivers changed published bounds to compute lifetimes in the local catalog.
    pub(super) async fn sync_compute_read_protection(&mut self) -> Result<(), AdapterError> {
        if self.controller.replica_owned_compute() {
            return Ok(());
        }
        let Some(subscriber) = &mut self.compaction_bound_subscriber else {
            return Ok(());
        };
        let changed = subscriber.sync().await?;
        for (id, bound) in changed {
            if !self
                .catalog()
                .try_get_entry_by_global_id(&id)
                .is_some_and(|entry| matches!(entry.item(), CatalogItem::Index(_)))
            {
                continue;
            }
            self.controller
                .compute
                .apply_compaction_bound(id, bound)
                .map_err(|error| AdapterError::Unstructured(error.into()))?;
        }
        Ok(())
    }

    /// Publishes recovery progress and compatible bounds in enabled, writable environments.
    pub(super) async fn publish_read_protection(&mut self) -> Result<(), AdapterError> {
        if self.controller.read_only()
            || !self.catalog().state().catalog_read_protection_enabled()
            || !self.pending_compute_installations.is_empty()
        {
            return Ok(());
        }

        let start = Instant::now();
        let catalog_changes = self.catalog_mut().take_read_protection_changes();
        self.read_protection_pending.extend(catalog_changes);
        let mut storage = self
            .controller
            .storage_collections
            .take_read_protection_frontiers(&self.read_protection_pending);
        let inputs = storage
            .keys()
            .filter_map(|id| {
                self.catalog()
                    .state()
                    .maintained_read_requirements()
                    .get(id)
            })
            .flat_map(|requirement| requirement.inputs.iter().copied())
            .collect();
        storage.extend(
            self.controller
                .storage_collections
                .take_read_protection_frontiers(&inputs),
        );
        let frontiers = storage
            .values()
            .map(|(frontiers, _)| frontiers.clone())
            .collect::<Vec<_>>();
        let compaction_frontiers = storage
            .iter()
            .map(|(&id, (_, frontier))| (id, frontier.clone()))
            .collect();
        let compute_changes = self
            .controller
            .compute
            .take_compaction_bound_proposals(&self.read_protection_pending);
        self.read_protection_pending.extend(storage.keys().copied());
        self.read_protection_pending
            .extend(compute_changes.keys().copied());
        let bounds = self.catalog().state().collection_compaction_bounds();
        let mut compute_proposals: BTreeMap<_, _> = compute_changes
            .into_iter()
            .filter_map(|(id, proposal)| {
                let entry = self.catalog().try_get_entry_by_global_id(&id)?;
                if !matches!(entry.item(), CatalogItem::Index(_)) {
                    return None;
                }
                let frontier = if bounds.contains_key(&id) {
                    proposal
                } else {
                    // First publication records installed readability.
                    self.controller
                        .compute
                        .collection_frontiers(id, None)
                        .expect("proposal belongs to installed index")
                        .read_frontier
                };
                Some((id, frontier))
            })
            .collect();
        for (id, proposal) in self.catalog().state().index_retention_proposals(&frontiers) {
            // Input progress also covers indexes without replicas. Where both
            // paths propose a bound, retain the history required by either.
            compute_proposals.entry(id).or_default().extend(proposal);
        }
        let candidates = publication_candidates(
            self.catalog().state().maintained_read_requirements(),
            self.catalog().state().collection_compaction_bounds(),
            &frontiers,
            &compaction_frontiers,
            &compute_proposals,
            |id| {
                let Some(entry) = self.catalog().try_get_entry_by_global_id(&id) else {
                    return false;
                };
                match entry.item() {
                    // A pending replacement observes the target's shared upper, not its own
                    // progress. Retired writers' requirements are completed by replacement.
                    CatalogItem::MaterializedView(mv) => {
                        mv.replacement_target.is_none() && mv.global_id_writes() == id
                    }
                    CatalogItem::Source(_) | CatalogItem::Table(_) | CatalogItem::Sink(_) => true,
                    _ => false,
                }
            },
            |id, excluding| {
                self.catalog()
                    .state()
                    .maintained_read_frontier(id, excluding)
                    .into_iter()
                    .chain(self.catalog().state().client_read_frontier(id))
                    .min()
            },
        );
        let requirement_updates = candidates.requirements.len();
        let bound_updates = candidates.bounds.len();
        if requirement_updates == 0 && bound_updates == 0 {
            self.read_protection_pending.clear();
            return Ok(());
        }

        // Applying each record as a separate op would rescan accumulated pending
        // catalog updates for every record on the coordinator loop.
        let ops = vec![Op::SetReadProtection {
            requirements: candidates.requirements,
            bounds: candidates.bounds,
        }];
        self.catalog_transact_once(ops).await?;
        self.read_protection_pending.clear();

        tracing::info!(
            changed_storage_collections = frontiers.len(),
            requirement_updates,
            bound_updates,
            duration_seconds = start.elapsed().as_secs_f64(),
            max_policy_lag_ts = candidates.max_policy_lag_ts,
            unbounded_policy_lag = candidates.unbounded_policy_lag,
            "published catalog read protection"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test(tokio::test)]
    async fn canceled_acquisition_does_not_enqueue_a_retry() {
        let (tx, response) = tokio::sync::oneshot::channel();
        let (commands, mut incoming) = tokio::sync::mpsc::unbounded_channel();
        let pending = PendingReadProtection {
            request: ReadProtectionRequest {
                upper: Some(Antichain::from_elem(Timestamp::from(11))),
                ..ReadProtectionRequest::new(
                    1,
                    CollectionIdBundle::default(),
                    Some(Timestamp::from(7)),
                )
            },
            tx,
            otel_ctx: mz_ore::tracing::OpenTelemetryContext::obtain(),
        };
        let retry = pending.retry_after(commands, Duration::from_secs(60));
        tokio::pin!(retry);
        tokio::select! {
            biased;
            _ = &mut retry => panic!("retry did not wait"),
            _ = std::future::ready(()) => (),
        }
        drop(response);
        tokio::time::timeout(Duration::from_secs(1), &mut retry)
            .await
            .expect("closing the reply must cancel the wait");
        assert!(incoming.try_recv().is_err(), "canceled retry was enqueued");
    }

    #[mz_ore::test(tokio::test)]
    async fn overlapping_protection_retries_leave_queued_work_ready() {
        for heartbeat_first in [false, true] {
            let due = tokio::time::Instant::now() - Duration::from_secs(1);
            let heartbeat = tokio::time::sleep_until(due);
            let publication = tokio::time::sleep_until(due);
            tokio::pin!(heartbeat, publication);
            // An attempt outlived both timers. Exercise either owner returning a
            // conflict before polling the same maintenance-first select ordering.
            let (retry, sibling) = if heartbeat_first {
                (heartbeat.as_mut(), publication.as_mut())
            } else {
                (publication.as_mut(), heartbeat.as_mut())
            };
            defer_protection_retry(retry, sibling, Duration::from_secs(60));
            tokio::select! {
                biased;
                _ = heartbeat.as_mut() => panic!("heartbeat bypassed backoff"),
                _ = publication.as_mut() => panic!("publication bypassed backoff"),
                _ = std::future::ready(()) => (),
            }
        }
    }

    #[mz_ore::test]
    fn conflict_delay_respects_renewal_headroom_without_expired_deadline_spin() {
        let heartbeat = mz_catalog::read_protection::client_protection_heartbeat_interval();
        let safety = mz_catalog::read_protection::client_protection_unchanged_grace() - heartbeat;
        let delay = Duration::from_millis(100).min(heartbeat / 2);
        let epsilon = Duration::from_nanos(1);
        for (age, expected) in [
            (Duration::ZERO, delay),
            (heartbeat - epsilon, epsilon),
            (heartbeat, delay),
            (heartbeat + epsilon, delay),
            (safety - epsilon, epsilon),
            (safety, delay),
            (safety + epsilon, delay),
        ] {
            assert_eq!(limit_conflict_delay(delay, age), expected, "age={age:?}");
        }
    }

    fn publication_candidates(
        requirements: &BTreeMap<GlobalId, MaintainedReadRequirement>,
        bounds: &BTreeMap<GlobalId, Antichain<Timestamp>>,
        frontiers: &[CollectionFrontiers],
        compaction_frontiers: &BTreeMap<GlobalId, Antichain<Timestamp>>,
        compute_proposals: &BTreeMap<GlobalId, Antichain<Timestamp>>,
        owns_durable_progress: impl Fn(GlobalId) -> bool,
    ) -> PublicationCandidates {
        super::publication_candidates(
            &requirements
                .iter()
                .map(|(&id, value)| (id, value.clone()))
                .collect(),
            &bounds
                .iter()
                .map(|(&id, value)| (id, value.clone()))
                .collect(),
            frontiers,
            compaction_frontiers,
            compute_proposals,
            owns_durable_progress,
            |input, excluding| {
                requirements
                    .values()
                    .filter(|requirement| {
                        requirement.inputs.contains(&input) && !excluding.contains(&requirement.id)
                    })
                    .filter_map(|requirement| requirement.frontier)
                    .min()
            },
        )
    }

    fn frontier(t: u64) -> Antichain<Timestamp> {
        Antichain::from_elem(Timestamp::from(t))
    }

    fn policy_frontiers(
        frontiers: &[CollectionFrontiers],
    ) -> BTreeMap<GlobalId, Antichain<Timestamp>> {
        frontiers
            .iter()
            .map(|f| (f.id, f.implied_capability.clone()))
            .collect()
    }

    fn collection(id: GlobalId, upper: Option<u64>, policy: Option<u64>) -> CollectionFrontiers {
        CollectionFrontiers {
            id,
            write_frontier: upper.map(Timestamp::from).into_iter().collect(),
            implied_capability: policy.map(Timestamp::from).into_iter().collect(),
            read_capabilities: frontier(0),
        }
    }

    fn requirement(id: GlobalId, inputs: &[GlobalId], t: u64) -> MaintainedReadRequirement {
        MaintainedReadRequirement {
            id,
            inputs: inputs.iter().copied().collect(),
            frontier: Some(Timestamp::from(t)),
        }
    }

    #[mz_ore::test]
    fn durable_progress_preserves_pending_refresh_and_completion() {
        let input = GlobalId::User(1);
        let output = GlobalId::User(2);
        let requirement = requirement(output, &[input], 10);
        let requirements = BTreeMap::from([(output, requirement.clone())]);
        let bounds = BTreeMap::from([(input, frontier(10))]);

        for (upper, expected) in [
            (Some(0), Some(10)),
            (Some(10), Some(10)),
            (Some(11), Some(10)),
            (Some(12), Some(11)),
            (None, None),
        ] {
            let frontiers = [
                collection(input, None, None),
                collection(output, upper, Some(0)),
            ];
            let candidates = publication_candidates(
                &requirements,
                &bounds,
                &frontiers,
                &policy_frontiers(&frontiers),
                &BTreeMap::new(),
                |_| true,
            );
            let expected = expected.map(Timestamp::from);
            if expected == requirement.frontier {
                assert!(candidates.requirements.is_empty());
                assert!(candidates.bounds.is_empty());
            } else {
                assert_eq!(
                    candidates.requirements,
                    vec![MaintainedReadRequirement {
                        frontier: expected,
                        ..requirement.clone()
                    }]
                );
                assert_eq!(
                    candidates.bounds,
                    vec![CollectionCompactionBound {
                        id: input,
                        frontier: expected,
                    }]
                );
            }
        }

        let completed = BTreeMap::from([(
            output,
            MaintainedReadRequirement {
                frontier: None,
                ..requirement
            },
        )]);
        let candidates = publication_candidates(
            &completed,
            &bounds,
            &[collection(output, Some(20), Some(0))],
            &BTreeMap::new(),
            &BTreeMap::new(),
            |_| true,
        );
        assert!(candidates.requirements.is_empty(), "completion is final");
    }

    #[mz_ore::test]
    fn all_consumers_limit_exact_input_versions() {
        let input = GlobalId::User(1);
        let other_version = GlobalId::User(2);
        let writer = GlobalId::User(3);
        let pending = GlobalId::User(4);
        let uninstalled = GlobalId::User(5);
        let requirements = BTreeMap::from([
            (writer, requirement(writer, &[input], 10)),
            (pending, requirement(pending, &[input], 15)),
            (uninstalled, requirement(uninstalled, &[input], 18)),
        ]);
        let bounds = BTreeMap::from([(input, frontier(10)), (other_version, frontier(10))]);
        let frontiers = [
            collection(input, Some(100), Some(90)),
            collection(other_version, Some(100), Some(90)),
            collection(writer, Some(51), Some(0)),
            collection(pending, Some(100), Some(0)),
        ];
        for (requirements, limit) in [
            (requirements.clone(), 15),
            (
                requirements
                    .into_iter()
                    .filter(|(id, _)| *id != pending)
                    .collect(),
                18,
            ),
        ] {
            let candidates = publication_candidates(
                &requirements,
                &bounds,
                &frontiers,
                &policy_frontiers(&frontiers),
                &BTreeMap::new(),
                |id| id != pending,
            );
            assert_eq!(
                candidates.requirements,
                vec![requirement(writer, &[input], 50)]
            );
            assert_eq!(
                candidates.bounds,
                vec![
                    CollectionCompactionBound {
                        id: input,
                        frontier: Some(Timestamp::from(limit)),
                    },
                    CollectionCompactionBound {
                        id: other_version,
                        frontier: Some(Timestamp::from(90)),
                    },
                ]
            );
            assert_eq!(candidates.max_policy_lag_ts, 90 - limit);
        }
    }

    #[mz_ore::test]
    fn storage_consumers_protect_self_and_dependencies_until_durable_progress() {
        let remap = GlobalId::User(1);
        let source = GlobalId::User(2);
        let sink = GlobalId::User(3);
        let requirements = BTreeMap::from([
            (remap, requirement(remap, &[remap], 10)),
            (source, requirement(source, &[source, remap], 10)),
            (sink, requirement(sink, &[sink, source], 10)),
        ]);
        let bounds = BTreeMap::from([
            (remap, frontier(10)),
            (source, frontier(10)),
            (sink, frontier(10)),
        ]);
        for sink_installed in [false, true] {
            let mut frontiers = vec![
                collection(remap, Some(101), Some(200)),
                collection(source, Some(51), Some(200)),
            ];
            if sink_installed {
                frontiers.push(collection(sink, Some(21), Some(200)));
            }
            let candidates = publication_candidates(
                &requirements,
                &bounds,
                &frontiers,
                &policy_frontiers(&frontiers),
                &BTreeMap::new(),
                |_| true,
            );
            let mut expected = vec![
                requirement(remap, &[remap], 100),
                requirement(source, &[source, remap], 50),
            ];
            let mut expected_bounds = vec![CollectionCompactionBound {
                id: remap,
                frontier: Some(Timestamp::from(50)),
            }];
            if sink_installed {
                expected.push(requirement(sink, &[sink, source], 20));
                expected_bounds.extend([source, sink].map(|id| CollectionCompactionBound {
                    id,
                    frontier: Some(Timestamp::from(20)),
                }));
            }
            assert_eq!(candidates.requirements, expected);
            assert_eq!(candidates.bounds, expected_bounds);
        }
    }

    #[mz_ore::test]
    fn local_creation_holds_limit_permission() {
        let input = GlobalId::User(1);
        let frontiers = [collection(input, Some(100), Some(90))];
        let candidates = publication_candidates(
            &BTreeMap::new(),
            &BTreeMap::from([(input, frontier(10))]),
            &frontiers,
            &BTreeMap::from([(input, frontier(20))]),
            &BTreeMap::new(),
            |_| false,
        );
        assert_eq!(
            candidates.bounds,
            vec![CollectionCompactionBound {
                id: input,
                frontier: Some(Timestamp::from(20)),
            }]
        );
    }

    #[mz_ore::test]
    fn compute_publication_preserves_existing_bounds() {
        let index = GlobalId::User(1);
        let uninstalled = GlobalId::User(3);
        for (old, proposal, expected) in [
            (Some(10), Some(20), Some(Some(20))),
            (Some(10), Some(10), None),
            (Some(10), Some(5), None),
            (Some(10), None, Some(None)),
            (None, Some(20), None),
        ] {
            let bounds = BTreeMap::from([
                (index, old.map(Timestamp::from).into_iter().collect()),
                (uninstalled, frontier(10)),
            ]);
            let proposals =
                BTreeMap::from([(index, proposal.map(Timestamp::from).into_iter().collect())]);
            let candidates = publication_candidates(
                &BTreeMap::new(),
                &bounds,
                &[],
                &BTreeMap::new(),
                &proposals,
                |_| false,
            );
            assert!(candidates.requirements.is_empty());
            assert_eq!(
                candidates.bounds,
                expected
                    .into_iter()
                    .map(|frontier| CollectionCompactionBound {
                        id: index,
                        frontier: frontier.map(Timestamp::from),
                    })
                    .collect::<Vec<_>>()
            );
        }
    }

    #[mz_ore::test]
    fn first_logging_publication_needs_no_birth_record() {
        let index = GlobalId::System(1);
        let candidates = publication_candidates(
            &BTreeMap::new(),
            &BTreeMap::new(),
            &[],
            &BTreeMap::new(),
            &BTreeMap::from([(index, frontier(20))]),
            |_| false,
        );
        assert_eq!(
            candidates.bounds,
            vec![CollectionCompactionBound {
                id: index,
                frontier: Some(20.into()),
            }]
        );
    }

    #[mz_ore::test]
    fn mixed_publication_batches_progress_and_independently_held_bounds() {
        let input = GlobalId::User(1);
        let output = GlobalId::User(2);
        let index = GlobalId::User(3);
        let requirements = BTreeMap::from([(output, requirement(output, &[input], 10))]);
        let bounds = BTreeMap::from([(input, frontier(10)), (index, frontier(10))]);
        let frontiers = [
            collection(input, Some(100), Some(90)),
            collection(output, Some(51), Some(0)),
        ];
        // Proposals already include live holds. Releasing one controller's hold
        // must not release the other's or the logical storage input requirement.
        for (storage_proposal, compute_proposal, expected_storage) in
            [(20, 30, 20), (90, 30, 50), (20, 90, 20)]
        {
            let candidates = publication_candidates(
                &requirements,
                &bounds,
                &frontiers,
                &BTreeMap::from([(input, frontier(storage_proposal))]),
                &BTreeMap::from([(index, frontier(compute_proposal))]),
                |_| true,
            );
            assert_eq!(
                candidates.requirements,
                vec![requirement(output, &[input], 50)]
            );
            assert_eq!(
                candidates.bounds,
                vec![
                    CollectionCompactionBound {
                        id: input,
                        frontier: Some(Timestamp::from(expected_storage)),
                    },
                    CollectionCompactionBound {
                        id: index,
                        frontier: Some(Timestamp::from(compute_proposal)),
                    },
                ]
            );
        }
    }

    #[mz_ore::test]
    fn bounds_require_installation_and_governance_and_never_regress() {
        let advancing = GlobalId::User(1);
        let regressing_policy = GlobalId::User(2);
        let uninstalled = GlobalId::User(3);
        let ungoverned = GlobalId::User(4);
        let complete = GlobalId::User(5);
        let bounds = BTreeMap::from([
            (advancing, frontier(10)),
            (regressing_policy, frontier(10)),
            (uninstalled, frontier(10)),
            (complete, Antichain::new()),
        ]);
        let frontiers = [
            collection(advancing, Some(100), Some(90)),
            collection(regressing_policy, Some(100), Some(5)),
            collection(ungoverned, Some(100), Some(90)),
            collection(complete, None, Some(90)),
        ];
        let candidates = publication_candidates(
            &BTreeMap::new(),
            &bounds,
            &frontiers,
            &policy_frontiers(&frontiers),
            &BTreeMap::new(),
            |_| false,
        );
        assert_eq!(
            candidates.bounds,
            vec![CollectionCompactionBound {
                id: advancing,
                frontier: Some(Timestamp::from(90)),
            }]
        );
        assert_eq!(candidates.max_policy_lag_ts, 0);

        let mut bounds = bounds;
        bounds.insert(advancing, frontier(90));
        let candidates = publication_candidates(
            &BTreeMap::new(),
            &bounds,
            &frontiers,
            &policy_frontiers(&frontiers),
            &BTreeMap::new(),
            |_| false,
        );
        assert!(candidates.requirements.is_empty());
        assert!(
            candidates.bounds.is_empty(),
            "unchanged publication is a no-op"
        );
    }
}
