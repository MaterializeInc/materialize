// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Query-scoped creation and redundant execution, without lifecycle authority.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use mz_compute_client::protocol::response::SubscribeResponse;
use mz_compute_types::ComputeInstanceId;
use mz_compute_types::dataflows::{BuildDesc, DataflowDescription, SourceImport};
use mz_compute_types::plan::LirRelationExpr;
use mz_compute_types::plan::render_plan::RenderPlan;
use mz_compute_types::sinks::{ComputeSinkConnection, ComputeSinkDesc};
use mz_compute_types::sources::SourceInstanceDesc;
use mz_controller_types::ReplicaId;
use mz_ore::task::AbortOnDropHandle;
use mz_repr::{GlobalId, Timestamp};
use mz_storage_types::StorageDiff;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::sources::SourceData;
use timely::PartialOrder;
use timely::progress::Antichain;
use tokio::sync::{mpsc, watch};

use super::QueryClient;
use super::compute::{DataflowResponse, QueryError, ReplicaQueryClient};
use crate::catalog::Catalog;
use crate::{AdapterError, ReadHolds};

#[derive(Debug)]
enum Event {
    Created,
    Response(ReplicaId, DataflowResponse),
    Failed(ReplicaId, AdapterError),
}

struct Subscribe {
    frontier: Antichain<Timestamp>,
    replicas: BTreeSet<ReplicaId>,
}

/// Owns all pending creations and installed exports for one query dataflow.
///
/// Keep this alive through peeks as well as sink execution. Dropping it aborts
/// only its tasks, whose SDK guards release only their own exports. Tasks own
/// neither this handle nor a QueryClient, so there is no ownership cycle.
pub(crate) struct QueryDataflows {
    _tasks: Vec<AbortOnDropHandle<()>>,
    clients: watch::Receiver<BTreeMap<ReplicaId, ReplicaQueryClient>>,
    events: mpsc::UnboundedReceiver<Event>,
    remaining: BTreeSet<ReplicaId>,
    subscribes: BTreeMap<GlobalId, Subscribe>,
    copies: BTreeSet<GlobalId>,
    last_error: Option<AdapterError>,
}

impl QueryDataflows {
    /// Connections that ACKed these exports, without waiting for a reported
    /// frontier. The caller owns peek cancellation and must retain this handle.
    pub(crate) fn acknowledged_clients(&self) -> BTreeMap<ReplicaId, ReplicaQueryClient> {
        self.clients
            .borrow()
            .iter()
            .filter(|(_, client)| client.frontiers().is_ok())
            .map(|(id, client)| (*id, client.clone()))
            .collect()
    }

    /// Subscribe before inspecting `acknowledged_clients` to avoid lost wakeups.
    /// Changes include ACKs and connection failures, not frontier progress.
    pub(crate) fn client_changes(
        &self,
    ) -> watch::Receiver<BTreeMap<ReplicaId, ReplicaQueryClient>> {
        self.clients.clone()
    }

    /// Receive merged sink responses. Cancel safe. A replica failure is terminal
    /// only when no pending or installed replica can still answer. Execution
    /// errors remain in their original SUBSCRIBE/COPY response variants.
    pub(crate) async fn recv(&mut self) -> Option<Result<DataflowResponse, AdapterError>> {
        loop {
            if self.subscribes.is_empty() && self.copies.is_empty() {
                return None;
            }
            if self.remaining.is_empty() {
                return self.last_error.take().map(Err);
            }
            // A DroppedAt on one replica followed by failure of the last
            // alternative must not leave this sink waiting on a live but
            // already-dropped response route.
            if let Some(id) = self
                .subscribes
                .iter()
                .find_map(|(id, subscribe)| subscribe.replicas.is_empty().then_some(*id))
            {
                let subscribe = self.subscribes.remove(&id).expect("present subscribe");
                return Some(Ok(DataflowResponse::Subscribe(
                    id,
                    SubscribeResponse::DroppedAt(subscribe.frontier),
                )));
            }
            match self.events.recv().await? {
                Event::Created => {}
                Event::Failed(replica, error) => self.failed(replica, error),
                Event::Response(replica, response) => {
                    if let Some(response) = self.merge(replica, response) {
                        return Some(Ok(response));
                    }
                }
            }
        }
    }

    fn failed(&mut self, replica: ReplicaId, error: AdapterError) {
        self.remaining.remove(&replica);
        // Keep failed replicas out of per-sink DroppedAt accounting too.
        for subscribe in self.subscribes.values_mut() {
            subscribe.replicas.remove(&replica);
        }
        self.last_error = Some(error);
    }

    fn merge(
        &mut self,
        replica: ReplicaId,
        response: DataflowResponse,
    ) -> Option<DataflowResponse> {
        match response {
            DataflowResponse::CopyTo(id, response) => self
                .copies
                .remove(&id)
                .then_some(DataflowResponse::CopyTo(id, response)),
            DataflowResponse::Subscribe(id, response) => {
                let subscribe = self.subscribes.get_mut(&id)?;
                if !subscribe.replicas.contains(&replica) {
                    return None;
                }
                let response = match response {
                    SubscribeResponse::Batch(mut batch) => {
                        if !PartialOrder::less_than(&subscribe.frontier, &batch.upper) {
                            return None;
                        }
                        // Match Instance::handle_subscribe_response: accept only
                        // advancing uppers and trim overlap at the global lower.
                        batch.lower =
                            std::mem::replace(&mut subscribe.frontier, batch.upper.clone());
                        if let Ok(updates) = &mut batch.updates {
                            updates.retain_mut(|updates| {
                                let offset = updates
                                    .times()
                                    .partition_point(|t| !batch.lower.less_equal(t));
                                let (_, retained) = std::mem::take(updates).split_at(offset);
                                *updates = retained;
                                updates.len() > 0
                            });
                        }
                        if batch.upper.is_empty() {
                            self.subscribes.remove(&id);
                        }
                        SubscribeResponse::Batch(batch)
                    }
                    SubscribeResponse::DroppedAt(_) => {
                        subscribe.replicas.remove(&replica);
                        if !subscribe.replicas.is_empty() {
                            return None;
                        }
                        // Nothing beyond the merged frontier was delivered.
                        let frontier = subscribe.frontier.clone();
                        self.subscribes.remove(&id);
                        SubscribeResponse::DroppedAt(frontier)
                    }
                };
                Some(DataflowResponse::Subscribe(id, response))
            }
        }
    }
}

impl QueryClient {
    /// Install only transient indexes, SUBSCRIBEs, and COPY sinks on catalog
    /// replicas. `creation_holds` must come from this client's committed read
    /// protection and cover every import at `as_of`. No protection is acquired
    /// here. Each replica retains its own clone until importer protection is ACKed
    /// or creation fails, including while its connection is pending.
    ///
    /// Returns after the first successful creation, without gating on siblings.
    /// Once a connection is selected, its query is never replayed on replacement.
    pub(crate) async fn create_dataflow(
        self: &Arc<Self>,
        catalog: Arc<Catalog>,
        cluster: ComputeInstanceId,
        target: Option<ReplicaId>,
        dataflow: DataflowDescription<LirRelationExpr, ()>,
        creation_holds: ReadHolds,
    ) -> Result<QueryDataflows, AdapterError> {
        let as_of = dataflow
            .as_of
            .as_ref()
            .ok_or_else(|| rejected("query dataflow requires as_of"))?;
        if dataflow.export_ids().next().is_none()
            || dataflow.export_ids().any(|id| !id.is_transient())
        {
            return Err(rejected("query dataflow requires transient exports"));
        }
        for id in dataflow
            .source_imports
            .keys()
            .chain(dataflow.index_imports.keys())
        {
            let hold = if dataflow.source_imports.contains_key(id) {
                creation_holds.storage_holds.get(id)
            } else {
                creation_holds.compute_holds.get(&(cluster, *id))
            };
            if !hold.is_some_and(|hold| PartialOrder::less_equal(hold.since(), as_of))
                || !self.protection.granted_frontier(*id).is_some_and(|grant| {
                    PartialOrder::less_equal(&Antichain::from_elem(grant), as_of)
                })
            {
                return Err(super::unavailable(*id));
            }
        }
        let replicas: BTreeSet<_> = catalog
            .try_get_cluster(cluster)
            .into_iter()
            .flat_map(|cluster| cluster.replicas())
            .map(|replica| replica.replica_id)
            .filter(|id| target.is_none_or(|target| target == *id))
            .collect();
        if replicas.is_empty() {
            return Err(rejected(format!(
                "no desired query replica for cluster {cluster}, target {target:?}"
            )));
        }
        let dataflow = self.enrich_dataflow(&catalog, dataflow).await?;
        let (events, receiver) = mpsc::unbounded_channel();
        let (clients, client_rx) = watch::channel(BTreeMap::new());
        let mut result = QueryDataflows {
            _tasks: Vec::new(),
            clients: client_rx,
            events: receiver,
            remaining: replicas.clone(),
            subscribes: dataflow
                .subscribe_ids()
                .map(|id| {
                    (
                        id,
                        Subscribe {
                            frontier: Antichain::from_elem(Timestamp::MIN),
                            replicas: replicas.clone(),
                        },
                    )
                })
                .collect(),
            copies: dataflow.copy_to_ids().collect(),
            last_error: None,
        };
        for replica in replicas {
            let query_client = Arc::clone(self);
            let dataflow = dataflow.clone();
            let holds = creation_holds.clone();
            let events = events.clone();
            let clients = clients.clone();
            result._tasks.push(
                mz_ore::task::spawn(|| "query-dataflow", async move {
                    let execute = async {
                        let ready = match dataflow.as_of.as_ref().and_then(|f| f.as_option()) {
                            Some(timestamp) => {
                                query_client
                                    .readable_replicas(
                                        cluster,
                                        Some(replica),
                                        &dataflow.index_imports.keys().copied().collect(),
                                        *timestamp,
                                    )
                                    .await?
                            }
                            None => {
                                query_client
                                    .connections
                                    .clients(cluster, Some(replica))
                                    .await?
                            }
                        };
                        let client = ready
                            .into_iter()
                            .next()
                            .ok_or_else(|| rejected("query replica disappeared"))?;
                        // SDK registration precedes creation. The returned guard owns
                        // both exports and early responses, and lives through recv.
                        let mut handle = client
                            .create_dataflow(dataflow)
                            .await
                            .map_err(query_error)?;
                        drop(holds);
                        clients.send_modify(|clients| {
                            clients.insert(replica, client);
                        });
                        let _ = events.send(Event::Created);
                        while let Some(response) = handle.recv().await {
                            let response = response.map_err(query_error)?;
                            if events.send(Event::Response(replica, response)).is_err() {
                                return Ok(());
                            }
                        }
                        Err(query_error(QueryError::Disconnected(
                            "query dataflow response route closed".into(),
                        )))
                    }
                    .await;
                    clients.send_modify(|clients| {
                        clients.remove(&replica);
                    });
                    if let Err(error) = execute {
                        let _ = events.send(Event::Failed(replica, error));
                    }
                })
                .abort_on_drop(),
            );
        }
        drop(creation_holds);
        drop(events);
        drop(clients);
        // A replica sends Created before forwarding any buffered responses, so
        // stopping here cannot consume or drop the first sink response.
        while let Some(event) = result.events.recv().await {
            match event {
                Event::Created => return Ok(result),
                Event::Failed(replica, error) => {
                    result.failed(replica, error);
                    if result.remaining.is_empty() {
                        break;
                    }
                }
                Event::Response(..) => unreachable!("response precedes creation ACK"),
            }
        }
        Err(result
            .last_error
            .take()
            .unwrap_or_else(|| rejected("all query dataflow creation tasks ended without an ACK")))
    }

    async fn enrich_dataflow(
        &self,
        catalog: &Catalog,
        dataflow: DataflowDescription<LirRelationExpr, ()>,
    ) -> Result<DataflowDescription<RenderPlan, CollectionMetadata>, AdapterError> {
        let mut sink_exports = BTreeMap::new();
        for (id, sink) in dataflow.sink_exports {
            let connection = match sink.connection {
                ComputeSinkConnection::Subscribe(conn) => ComputeSinkConnection::Subscribe(conn),
                ComputeSinkConnection::CopyToS3Oneshot(conn) => {
                    ComputeSinkConnection::CopyToS3Oneshot(conn)
                }
                ComputeSinkConnection::MaterializedView(_)
                | ComputeSinkConnection::MetricSink(_) => {
                    return Err(rejected("maintained sinks are not query-local exports"));
                }
            };
            sink_exports.insert(
                id,
                ComputeSinkDesc {
                    from: sink.from,
                    from_desc: sink.from_desc,
                    connection,
                    with_snapshot: sink.with_snapshot,
                    up_to: sink.up_to,
                    non_null_assertions: sink.non_null_assertions,
                    refresh_schedule: sink.refresh_schedule,
                },
            );
        }
        let mut source_imports = BTreeMap::new();
        let mut txns_upper = None;
        for (id, import) in dataflow.source_imports {
            let metadata = self.collection_metadata(catalog, id)?;
            let upper = if metadata.txns_shard.is_some() {
                if txns_upper.is_none() {
                    txns_upper = Some(
                        self.persist
                            .recent_upper::<SourceData, (), Timestamp, StorageDiff>(
                                self.txns_shard,
                                super::diagnostics(id),
                            )
                            .await
                            .map_err(|error| AdapterError::Unstructured(error.into()))?,
                    );
                }
                txns_upper
                    .as_ref()
                    .expect("observed transaction upper")
                    .clone()
            } else {
                self.persist
                    .recent_upper::<SourceData, (), Timestamp, StorageDiff>(
                        metadata.data_shard,
                        super::diagnostics(id),
                    )
                    .await
                    .map_err(|error| AdapterError::Unstructured(error.into()))?
            };
            source_imports.insert(
                id,
                SourceImport {
                    desc: SourceInstanceDesc {
                        storage_metadata: metadata,
                        arguments: import.desc.arguments,
                        typ: import.desc.typ,
                    },
                    monotonic: import.monotonic,
                    with_snapshot: import.with_snapshot,
                    upper,
                },
            );
        }
        let objects_to_build = dataflow
            .objects_to_build
            .into_iter()
            .map(|object| {
                Ok(BuildDesc {
                    id: object.id,
                    plan: RenderPlan::try_from(object.plan)
                        .map_err(|()| rejected(format!("invalid render plan for {}", object.id)))?,
                })
            })
            .collect::<Result<_, AdapterError>>()?;
        Ok(DataflowDescription {
            source_imports,
            index_imports: dataflow.index_imports,
            objects_to_build,
            index_exports: dataflow.index_exports,
            sink_exports,
            as_of: dataflow.as_of,
            until: dataflow.until,
            initial_storage_as_of: dataflow.initial_storage_as_of,
            refresh_schedule: dataflow.refresh_schedule,
            debug_name: dataflow.debug_name,
            // Unknown is not wall-clock independent. The planner must supply
            // known time dependence if replica expiration is to account for it.
            time_dependence: dataflow.time_dependence,
        })
    }
}

fn query_error(error: QueryError) -> AdapterError {
    AdapterError::Unstructured(error.into())
}

fn rejected(message: impl Into<String>) -> AdapterError {
    query_error(QueryError::Rejected(message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_compute_client::protocol::response::{CopyToResponse, SubscribeBatch};
    use mz_repr::{Row, UpdateCollection};

    const SINK: GlobalId = GlobalId::Transient(1);
    const A: ReplicaId = ReplicaId::User(1);
    const B: ReplicaId = ReplicaId::User(2);

    fn stream() -> (QueryDataflows, mpsc::UnboundedSender<Event>) {
        let (events, receiver) = mpsc::unbounded_channel();
        let replicas = BTreeSet::from([A, B]);
        (
            QueryDataflows {
                _tasks: Vec::new(),
                clients: watch::channel(BTreeMap::new()).1,
                events: receiver,
                remaining: replicas.clone(),
                subscribes: BTreeMap::from([(
                    SINK,
                    Subscribe {
                        frontier: Antichain::from_elem(Timestamp::MIN),
                        replicas,
                    },
                )]),
                copies: BTreeSet::new(),
                last_error: None,
            },
            events,
        )
    }

    fn batch(lower: u64, upper: u64, times: &[u64]) -> DataflowResponse {
        let row = Row::default();
        let times: Vec<_> = times.iter().copied().map(Timestamp::from).collect();
        let updates: UpdateCollection = times
            .iter()
            .map(|time| (row.as_row_ref(), time, 1.into()))
            .collect();
        DataflowResponse::Subscribe(
            SINK,
            SubscribeResponse::Batch(SubscribeBatch {
                lower: Antichain::from_elem(lower.into()),
                upper: Antichain::from_elem(upper.into()),
                updates: Ok(vec![updates]),
            }),
        )
    }

    #[mz_ore::test(tokio::test)]
    async fn subscribe_trims_overlap_and_ignores_nonadvancing_batches() {
        let (mut stream, events) = stream();
        events
            .send(Event::Response(A, batch(0, 5, &[1, 4])))
            .expect("query dataflow event receiver should remain open");
        events
            .send(Event::Response(B, batch(0, 3, &[1, 2])))
            .expect("query dataflow event receiver should remain open");
        events
            .send(Event::Response(B, batch(3, 7, &[4, 5, 6])))
            .expect("query dataflow event receiver should remain open");
        let Some(Ok(DataflowResponse::Subscribe(_, SubscribeResponse::Batch(first)))) =
            stream.recv().await
        else {
            panic!("expected first batch");
        };
        assert_eq!(first.upper, Antichain::from_elem(5.into()));
        let Some(Ok(DataflowResponse::Subscribe(_, SubscribeResponse::Batch(next)))) =
            stream.recv().await
        else {
            panic!("expected advancing batch");
        };
        assert_eq!(next.lower, first.upper);
        assert_eq!(next.upper, Antichain::from_elem(7.into()));
        assert_eq!(
            next.updates
                .expect("advancing batch should contain successful updates")[0]
                .times(),
            &[Timestamp::from(5), Timestamp::from(6)]
        );
    }

    #[mz_ore::test(tokio::test)]
    async fn canceled_recv_and_failed_replica_do_not_lose_survivor_response() {
        let (mut stream, events) = stream();
        events
            .send(Event::Failed(A, rejected("first creation failed")))
            .expect("query dataflow event receiver should remain open");
        // B can still be awaiting connection or ACK. Canceling recv must keep
        // the stream, including B's eventual first response, alive.
        assert!(futures::poll!(Box::pin(stream.recv())).is_pending());
        events
            .send(Event::Created)
            .expect("query dataflow event receiver should remain open");
        events
            .send(Event::Response(B, batch(0, 5, &[1])))
            .expect("query dataflow event receiver should remain open");
        assert!(matches!(
            stream.recv().await,
            Some(Ok(DataflowResponse::Subscribe(..)))
        ));
        events
            .send(Event::Failed(
                B,
                query_error(QueryError::Disconnected("last connection".into())),
            ))
            .expect("query dataflow event receiver should remain open");
        let Some(Err(AdapterError::Unstructured(error))) = stream.recv().await else {
            panic!("expected terminal connection failure");
        };
        assert!(matches!(
            error.downcast_ref::<QueryError>(),
            Some(QueryError::Disconnected(_))
        ));
        assert!(stream.recv().await.is_none());
    }

    #[mz_ore::test(tokio::test)]
    async fn copy_returns_first_terminal_result_once() {
        let (mut stream, events) = stream();
        stream.subscribes.clear();
        stream.copies.insert(SINK);
        events
            .send(Event::Response(
                A,
                DataflowResponse::CopyTo(SINK, CopyToResponse::Error("upload failed".into())),
            ))
            .expect("query dataflow event receiver should remain open");
        events
            .send(Event::Response(
                B,
                DataflowResponse::CopyTo(SINK, CopyToResponse::RowCount(10)),
            ))
            .expect("query dataflow event receiver should remain open");
        assert!(
            matches!(stream.recv().await, Some(Ok(DataflowResponse::CopyTo(SINK, CopyToResponse::Error(error)))) if error == "upload failed")
        );
        assert!(stream.recv().await.is_none());
    }

    #[mz_ore::test(tokio::test)]
    async fn dropped_subscribe_waits_for_alternatives_but_not_failed_ones() {
        let (mut stream, events) = stream();
        events
            .send(Event::Response(
                A,
                DataflowResponse::Subscribe(
                    SINK,
                    SubscribeResponse::DroppedAt(Antichain::from_elem(0.into())),
                ),
            ))
            .expect("query dataflow event receiver should remain open");
        assert!(futures::poll!(Box::pin(stream.recv())).is_pending());
        events
            .send(Event::Failed(B, rejected("second creation failed")))
            .expect("query dataflow event receiver should remain open");
        assert!(matches!(
            stream.recv().await,
            Some(Ok(DataflowResponse::Subscribe(
                SINK,
                SubscribeResponse::DroppedAt(_)
            )))
        ));
        assert!(stream.recv().await.is_none());
    }
}
