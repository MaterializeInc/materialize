// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Request-scoped compute transport for one replica, without lifecycle authority.
//!
//! The supplied client must aggregate all workers of the replica, including query
//! readiness and creation acknowledgements. Disconnection is terminal. Neither
//! commands nor cached frontiers are carried over to a replacement connection.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use mz_compute_client::protocol::command::{ComputeCommand, Peek};
use mz_compute_client::protocol::response::{
    ComputeResponse, CopyToResponse, FrontiersResponse, PeekError, PeekResponse, SubscribeResponse,
};
use mz_compute_client::service::ComputeClient;
use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::plan::render_plan::RenderPlan;
use mz_ore::tracing::OpenTelemetryContext;
use mz_repr::GlobalId;
use mz_storage_types::controller::CollectionMetadata;
use timely::progress::Antichain;
use tokio::sync::{mpsc, oneshot, watch};
use uuid::Uuid;

type Dataflow = DataflowDescription<RenderPlan, CollectionMetadata>;
type PeekResult = (PeekResponse, OpenTelemetryContext);
type Reply<T> = oneshot::Sender<Result<T, QueryError>>;

/// A connection failure or rejected query-local request.
#[derive(Clone, Debug, thiserror::Error)]
pub enum QueryError {
    #[error("query connection lost: {0}")]
    Disconnected(String),
    #[error("query request rejected: {0}")]
    Rejected(String),
}

/// A sink response tagged with its export ID. Compute execution errors retain
/// their existing response representation, distinct from transport failures.
#[derive(Debug)]
pub enum DataflowResponse {
    Subscribe(GlobalId, SubscribeResponse),
    CopyTo(GlobalId, CopyToResponse),
}

#[derive(Default)]
struct Observations {
    frontiers: BTreeMap<GlobalId, FrontiersResponse>,
    error: Option<QueryError>,
}

struct Connection {
    commands: mpsc::UnboundedSender<Request>,
    observations: Arc<Mutex<Observations>>,
    changes: watch::Receiver<()>,
    _task: mz_ore::task::AbortOnDropHandle<()>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        // Also interrupt a blocked transport send. The actor owns no Connection,
        // including through pending replies, so this cannot form an ownership cycle.
        self.observations
            .lock()
            .expect("lock poisoned")
            .frontiers
            .clear();
    }
}

/// A clonable connection to one replica. Replica choice belongs to the caller.
#[derive(Clone)]
pub struct ReplicaQueryClient(Arc<Connection>);

impl ReplicaQueryClient {
    /// Perform HelloQuery and wait for aggregated QueryReady.
    ///
    /// `max_result_size` is the response ceiling in bytes, installed through
    /// SetQueryMaxResultSize before query work. The supplied transport must forward
    /// this configuration to every aggregation layer and compute worker.
    /// Dropping this future closes the connection, even during the handshake.
    pub async fn connect(
        client: Box<dyn ComputeClient>,
        max_result_size: usize,
    ) -> Result<Self, QueryError> {
        let (commands, requests) = mpsc::unbounded_channel();
        let (changed, changes) = watch::channel(());
        let observations = Arc::new(Mutex::new(Observations::default()));
        let (ready, waiting) = oneshot::channel();
        let mut actor = Actor {
            client,
            requests,
            observations: Arc::clone(&observations),
            changed,
            ready: Some(ready),
            peeks: BTreeMap::new(),
            dataflows: BTreeMap::new(),
            exports: BTreeMap::new(),
            max_result_size,
        };
        let task = mz_ore::task::spawn(|| "replica-query-client", async move {
            let result = actor.run().await;
            actor.fail(QueryError::Disconnected(match result {
                Ok(()) => "connection closed".into(),
                Err(error) => format!("{error:#}"),
            }));
        });
        let handle = Self(Arc::new(Connection {
            commands,
            observations,
            changes,
            _task: task.abort_on_drop(),
        }));
        handle.receive(waiting).await?;
        Ok(handle)
    }

    fn error(&self) -> QueryError {
        self.0
            .observations
            .lock()
            .expect("lock poisoned")
            .error
            .clone()
            .unwrap_or_else(|| QueryError::Disconnected("connection closed".into()))
    }

    fn send(&self, request: Request) -> Result<(), QueryError> {
        self.0.commands.send(request).map_err(|_| self.error())
    }

    async fn receive<T>(
        &self,
        reply: oneshot::Receiver<Result<T, QueryError>>,
    ) -> Result<T, QueryError> {
        reply.await.map_err(|_| self.error())?
    }

    /// Whether the connection has not reported a terminal error.
    pub fn is_connected(&self) -> bool {
        self.0
            .observations
            .lock()
            .expect("lock poisoned")
            .error
            .is_none()
    }

    /// Return only actually observed frontiers. Missing optional fields remain
    /// unknown until reported. Completed entries remain until disconnection.
    pub fn frontiers(&self) -> Result<BTreeMap<GlobalId, FrontiersResponse>, QueryError> {
        let state = self.0.observations.lock().expect("lock poisoned");
        match &state.error {
            Some(error) => Err(error.clone()),
            None => Ok(state.frontiers.clone()),
        }
    }

    /// Return the cached observation for one collection, or `None` if unobserved.
    /// Optional fields remain unknown until reported. Connection loss returns an
    /// error rather than observations from the disconnected replica.
    pub fn collection_frontiers(
        &self,
        id: GlobalId,
    ) -> Result<Option<FrontiersResponse>, QueryError> {
        let state = self.0.observations.lock().expect("lock poisoned");
        match &state.error {
            Some(error) => Err(error.clone()),
            None => Ok(state.frontiers.get(&id).cloned()),
        }
    }

    /// Notifications to re-read cached frontiers, including on connection loss.
    /// Notifications may coalesce and do not themselves keep the connection alive.
    pub fn frontier_changes(&self) -> watch::Receiver<()> {
        self.0.changes.clone()
    }

    /// Update the connection-local response ceiling, including replica aggregation.
    pub async fn set_max_result_size(&self, bytes: usize) -> Result<(), QueryError> {
        let (reply, waiting) = oneshot::channel();
        self.send(Request::SetMaxResultSize(bytes, reply))?;
        self.receive(waiting).await
    }

    /// Execute a peek with its supplied UUID and OTel context. UUIDs must never
    /// be reused on this connection. Dropping the future cancels a pending peek.
    /// The response context is returned for attachment by the receiving task.
    pub async fn peek(&self, peek: Peek) -> Result<PeekResult, QueryError> {
        let uuid = peek.uuid;
        let token = Uuid::new_v4();
        let (reply, waiting) = oneshot::channel();
        self.send(Request::Peek(Box::new(peek), token, reply))?;
        let _cleanup = PeekGuard {
            client: self.clone(),
            uuid,
            token,
        };
        self.receive(waiting).await
    }

    /// Cancel a pending peek. Unknown or already completed IDs are harmless.
    pub fn cancel_peek(&self, uuid: Uuid) -> Result<(), QueryError> {
        self.send(Request::CancelPeek(uuid))
    }

    /// Install a query-local dataflow and schedule all its exports after ACK.
    ///
    /// Success means rendering and actual importer protection are established,
    /// not that the snapshot has completed. The caller may release its creation
    /// grants after success. It must retain them while awaiting this method.
    /// Sink response routes are installed before sending the create command.
    /// Dropping the future or returned handle drops only this request's exports.
    /// Export IDs must be transient and never reused on this connection.
    pub async fn create_dataflow(&self, dataflow: Dataflow) -> Result<QueryDataflow, QueryError> {
        let request_id = Uuid::new_v4();
        let (reply, waiting) = oneshot::channel();
        let (responses, receiver) = mpsc::unbounded_channel();
        self.send(Request::Create {
            request_id,
            dataflow: Box::new(dataflow),
            reply,
            responses,
        })?;
        let handle = QueryDataflow {
            client: self.clone(),
            request_id,
            responses: receiver,
        };
        self.receive(waiting).await?;
        Ok(handle)
    }
}

struct PeekGuard {
    client: ReplicaQueryClient,
    uuid: Uuid,
    token: Uuid,
}

impl Drop for PeekGuard {
    fn drop(&mut self) {
        let _ = self
            .client
            .send(Request::AbandonPeek(self.uuid, self.token));
    }
}

/// Owns a query-local dataflow's exports and buffered sink responses.
pub struct QueryDataflow {
    client: ReplicaQueryClient,
    request_id: Uuid,
    responses: mpsc::UnboundedReceiver<Result<DataflowResponse, QueryError>>,
}

impl QueryDataflow {
    /// Receive the next sink response or connection error. Cancel safe.
    /// A transient-index dataflow has no sink responses.
    pub async fn recv(&mut self) -> Option<Result<DataflowResponse, QueryError>> {
        self.responses.recv().await
    }
}

impl Drop for QueryDataflow {
    fn drop(&mut self) {
        let _ = self.client.send(Request::DropDataflow(self.request_id));
    }
}

enum Request {
    SetMaxResultSize(usize, Reply<()>),
    Peek(Box<Peek>, Uuid, Reply<PeekResult>),
    CancelPeek(Uuid),
    AbandonPeek(Uuid, Uuid),
    Create {
        request_id: Uuid,
        dataflow: Box<Dataflow>,
        reply: Reply<()>,
        responses: mpsc::UnboundedSender<Result<DataflowResponse, QueryError>>,
    },
    DropDataflow(Uuid),
}

struct InstalledDataflow {
    exports: BTreeSet<GlobalId>,
    reply: Option<Reply<()>>,
    responses: mpsc::UnboundedSender<Result<DataflowResponse, QueryError>>,
}

struct Actor {
    client: Box<dyn ComputeClient>,
    requests: mpsc::UnboundedReceiver<Request>,
    observations: Arc<Mutex<Observations>>,
    changed: watch::Sender<()>,
    ready: Option<Reply<()>>,
    peeks: BTreeMap<Uuid, (Uuid, Reply<PeekResult>)>,
    dataflows: BTreeMap<Uuid, InstalledDataflow>,
    exports: BTreeMap<GlobalId, Uuid>,
    max_result_size: usize,
}

impl Actor {
    async fn run(&mut self) -> Result<(), anyhow::Error> {
        self.client
            .send(ComputeCommand::HelloQuery {
                nonce: Uuid::new_v4(),
            })
            .await?;
        self.client
            .send(ComputeCommand::SetQueryMaxResultSize {
                max_result_size: u64::try_from(self.max_result_size)?,
            })
            .await?;
        loop {
            tokio::select! {
                request = self.requests.recv(), if self.ready.is_none() => {
                    match request {
                        Some(request) => self.request(request).await?,
                        None => return Ok(()),
                    }
                }
                response = self.client.recv() => {
                    match response? {
                        Some(response) => self.response(response).await?,
                        None => return Ok(()),
                    }
                }
            }
        }
    }

    async fn request(&mut self, request: Request) -> Result<(), anyhow::Error> {
        match request {
            Request::SetMaxResultSize(bytes, reply) => {
                self.max_result_size = bytes;
                self.client
                    .send(ComputeCommand::SetQueryMaxResultSize {
                        max_result_size: u64::try_from(bytes)?,
                    })
                    .await?;
                let _ = reply.send(Ok(()));
            }
            Request::Peek(peek, token, reply) => {
                if self.peeks.contains_key(&peek.uuid) {
                    let _ = reply.send(Err(QueryError::Rejected("duplicate peek UUID".into())));
                    return Ok(());
                }
                self.peeks.insert(peek.uuid, (token, reply));
                self.client.send(ComputeCommand::Peek(peek)).await?;
            }
            Request::CancelPeek(uuid) => {
                if self.peeks.contains_key(&uuid) {
                    self.client
                        .send(ComputeCommand::CancelPeek { uuid })
                        .await?;
                }
            }
            Request::AbandonPeek(uuid, token) => {
                // A rejected duplicate UUID must not cancel the original caller.
                if self
                    .peeks
                    .get(&uuid)
                    .is_some_and(|(owner, _)| *owner == token)
                {
                    self.peeks.remove(&uuid);
                    self.client
                        .send(ComputeCommand::CancelPeek { uuid })
                        .await?;
                }
            }
            Request::Create {
                request_id,
                dataflow,
                reply,
                responses,
            } => {
                let exports: BTreeSet<_> = dataflow.export_ids().collect();
                if exports.is_empty()
                    || exports
                        .iter()
                        .any(|id| !id.is_transient() || self.exports.contains_key(id))
                {
                    let _ = reply.send(Err(QueryError::Rejected(
                        "exports must be fresh query-local IDs".into(),
                    )));
                    return Ok(());
                }
                // Register BEFORE send, including before the creation ACK. Rendering
                // may produce sink responses immediately, even while admission waits.
                for id in &exports {
                    self.exports.insert(*id, request_id);
                }
                self.dataflows.insert(
                    request_id,
                    InstalledDataflow {
                        exports,
                        reply: Some(reply),
                        responses,
                    },
                );
                self.client
                    .send(ComputeCommand::CreateQueryDataflow {
                        request_id,
                        dataflow,
                    })
                    .await?;
            }
            Request::DropDataflow(id) => self.drop_dataflow(id).await?,
        }
        Ok(())
    }

    async fn drop_dataflow(&mut self, id: Uuid) -> Result<(), anyhow::Error> {
        if let Some(dataflow) = self.dataflows.remove(&id) {
            for id in dataflow.exports {
                self.exports.remove(&id);
                self.client
                    .send(ComputeCommand::AllowCompaction {
                        id,
                        frontier: Antichain::new(),
                    })
                    .await?;
            }
        }
        Ok(())
    }

    async fn response(&mut self, response: ComputeResponse) -> Result<(), anyhow::Error> {
        match response {
            ComputeResponse::QueryReady => {
                if let Some(ready) = self.ready.take() {
                    let _ = ready.send(Ok(()));
                }
            }
            ComputeResponse::Frontiers(id, update) => {
                let mut state = self.observations.lock().expect("lock poisoned");
                let cached = state.frontiers.entry(id).or_default();
                if update.write_frontier.is_some() {
                    cached.write_frontier = update.write_frontier;
                }
                if update.input_frontier.is_some() {
                    cached.input_frontier = update.input_frontier;
                }
                if update.output_frontier.is_some() {
                    cached.output_frontier = update.output_frontier;
                }
                if update.read_frontier.is_some() {
                    cached.read_frontier = update.read_frontier;
                }
                self.changed.send_replace(());
            }
            ComputeResponse::PeekResponse(uuid, mut response, context) => {
                if let Some((_, reply)) = self.peeks.remove(&uuid) {
                    if response.inline_byte_len() > self.max_result_size {
                        response = PeekResponse::Error(PeekError::ResultExceedsMaxSize {
                            max_result_size: self.max_result_size,
                        });
                    }
                    let _ = reply.send(Ok((response, context)));
                }
            }
            ComputeResponse::QueryDataflowResponse { request_id, error } => {
                if let Some(dataflow) = self.dataflows.get_mut(&request_id) {
                    if let Some(error) = error {
                        let error = QueryError::Rejected(error);
                        if let Some(reply) = dataflow.reply.take() {
                            let _ = reply.send(Err(error.clone()));
                        }
                        let _ = dataflow.responses.send(Err(error));
                        self.drop_dataflow(request_id).await?;
                    } else if dataflow.reply.is_some() {
                        for id in &dataflow.exports {
                            self.client.send(ComputeCommand::Schedule(*id)).await?;
                        }
                        if let Some(reply) = dataflow.reply.take() {
                            let _ = reply.send(Ok(()));
                        }
                    }
                }
            }
            ComputeResponse::SubscribeResponse(id, mut response) => {
                response.to_error_if_exceeds(self.max_result_size);
                self.route(id, DataflowResponse::Subscribe(id, response));
            }
            ComputeResponse::CopyToResponse(id, response) => {
                self.route(id, DataflowResponse::CopyTo(id, response));
            }
            ComputeResponse::Status(_) => {}
        }
        Ok(())
    }

    fn route(&self, id: GlobalId, response: DataflowResponse) {
        if let Some(dataflow) = self.exports.get(&id).and_then(|id| self.dataflows.get(id)) {
            let _ = dataflow.responses.send(Ok(response));
        }
    }

    fn fail(&mut self, error: QueryError) {
        {
            let mut state = self.observations.lock().expect("lock poisoned");
            state.frontiers.clear();
            state.error.get_or_insert_with(|| error.clone());
        }
        self.changed.send_replace(());
        self.requests.close();
        if let Some(ready) = self.ready.take() {
            let _ = ready.send(Err(error.clone()));
        }
        for (_, (_, reply)) in std::mem::take(&mut self.peeks) {
            let _ = reply.send(Err(error.clone()));
        }
        for (_, dataflow) in std::mem::take(&mut self.dataflows) {
            if let Some(reply) = dataflow.reply {
                let _ = reply.send(Err(error.clone()));
            }
            let _ = dataflow.responses.send(Err(error.clone()));
        }
        self.exports.clear();
    }
}

impl Drop for Actor {
    fn drop(&mut self) {
        self.fail(QueryError::Disconnected("connection closed".into()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::time::Duration;

    use mz_compute_client::protocol::command::PeekTarget;
    use mz_compute_types::sinks::{
        ComputeSinkConnection, ComputeSinkDesc, SubscribeSinkConnection,
    };
    use mz_repr::{RelationDesc, Timestamp};
    use mz_service::client::GenericClient;

    #[derive(Debug)]
    struct Wire {
        commands: mpsc::UnboundedSender<ComputeCommand>,
        responses: mpsc::UnboundedReceiver<ComputeResponse>,
    }

    #[async_trait::async_trait]
    impl GenericClient<ComputeCommand, ComputeResponse> for Wire {
        async fn send(&mut self, command: ComputeCommand) -> Result<(), anyhow::Error> {
            self.commands
                .send(command)
                .map_err(|_| anyhow::anyhow!("test peer closed"))
        }

        async fn recv(&mut self) -> Result<Option<ComputeResponse>, anyhow::Error> {
            Ok(self.responses.recv().await)
        }
    }

    struct Peer {
        commands: mpsc::UnboundedReceiver<ComputeCommand>,
        responses: mpsc::UnboundedSender<ComputeResponse>,
    }

    impl Peer {
        async fn command(&mut self) -> ComputeCommand {
            bounded(self.commands.recv())
                .await
                .expect("connection closed")
        }

        fn respond(&self, response: ComputeResponse) {
            self.responses.send(response).expect("connection closed");
        }
    }

    async fn bounded<T>(future: impl Future<Output = T>) -> T {
        tokio::time::timeout(Duration::from_secs(10), future)
            .await
            .expect("test timed out")
    }

    fn wire() -> (Box<dyn ComputeClient>, Peer) {
        let (commands, command_rx) = mpsc::unbounded_channel();
        let (responses, response_rx) = mpsc::unbounded_channel();
        (
            Box::new(Wire {
                commands,
                responses: response_rx,
            }),
            Peer {
                commands: command_rx,
                responses,
            },
        )
    }

    async fn connect() -> (ReplicaQueryClient, Peer) {
        let (wire, mut peer) = wire();
        let mut connecting = Box::pin(ReplicaQueryClient::connect(wire, 1024));
        assert!(futures::poll!(&mut connecting).is_pending());
        assert!(matches!(
            peer.command().await,
            ComputeCommand::HelloQuery { .. }
        ));
        assert!(matches!(
            peer.command().await,
            ComputeCommand::SetQueryMaxResultSize {
                max_result_size: 1024
            }
        ));
        assert!(futures::poll!(&mut connecting).is_pending());
        peer.respond(ComputeResponse::QueryReady);
        (
            bounded(connecting)
                .await
                .expect("query handshake should succeed"),
            peer,
        )
    }

    fn peek(uuid: Uuid) -> Peek {
        Peek {
            target: PeekTarget::Index {
                id: GlobalId::User(1),
            },
            result_desc: RelationDesc::empty(),
            literal_constraints: None,
            uuid,
            timestamp: Timestamp::from(1),
            finishing: mz_expr::RowSetFinishing::trivial(0),
            map_filter_project: mz_expr::SafeMfpPlan::from_mfp(mz_expr::MapFilterProject::new(0)),
            otel_ctx: OpenTelemetryContext::empty(),
        }
    }

    fn subscribe(id: GlobalId) -> Dataflow {
        let mut dataflow = Dataflow::new("query wire test".into());
        dataflow.set_as_of(Antichain::from_elem(Timestamp::from(1)));
        dataflow.sink_exports.insert(
            id,
            ComputeSinkDesc {
                from: GlobalId::User(1),
                from_desc: RelationDesc::empty(),
                connection: ComputeSinkConnection::Subscribe(SubscribeSinkConnection {
                    output: vec![],
                }),
                with_snapshot: true,
                up_to: Antichain::new(),
                non_null_assertions: vec![],
                refresh_schedule: None,
            },
        );
        dataflow
    }

    #[mz_ore::test(tokio::test)]
    async fn observed_frontiers_survive_completion_but_not_disconnect() {
        let (client, peer) = connect().await;
        let id = GlobalId::User(1);
        let mut changed = client.frontier_changes();
        assert_eq!(
            client
                .collection_frontiers(id)
                .expect("connected client should return frontier observations"),
            None
        );
        let initial = FrontiersResponse {
            write_frontier: Some(Antichain::from_elem(Timestamp::from(5))),
            input_frontier: Some(Antichain::from_elem(Timestamp::from(3))),
            output_frontier: None,
            read_frontier: Some(Antichain::from_elem(Timestamp::from(2))),
        };
        peer.respond(ComputeResponse::Frontiers(id, initial.clone()));
        bounded(changed.changed())
            .await
            .expect("frontier watcher should receive a change notification");
        assert_eq!(
            client
                .collection_frontiers(id)
                .expect("connected client should return frontier observations"),
            Some(initial.clone())
        );
        assert_eq!(
            client
                .collection_frontiers(GlobalId::User(2))
                .expect("connected client should return frontier observations"),
            None
        );
        peer.respond(ComputeResponse::Frontiers(
            id,
            FrontiersResponse {
                write_frontier: Some(Antichain::new()),
                ..Default::default()
            },
        ));
        bounded(changed.changed())
            .await
            .expect("frontier watcher should receive a change notification");
        let cached = client
            .collection_frontiers(id)
            .expect("connected client should return frontier observations")
            .expect("reported collection frontiers should be cached");
        assert_eq!(cached.write_frontier, Some(Antichain::new()));
        assert_eq!(cached.input_frontier, initial.input_frontier);
        assert_eq!(cached.output_frontier, None);
        assert_eq!(cached.read_frontier, initial.read_frontier);
        for read_frontier in [Antichain::from_elem(Timestamp::from(4)), Antichain::new()] {
            peer.respond(ComputeResponse::Frontiers(
                id,
                FrontiersResponse {
                    read_frontier: Some(read_frontier.clone()),
                    ..Default::default()
                },
            ));
            bounded(changed.changed())
                .await
                .expect("frontier watcher should receive a change notification");
            let cached = client
                .collection_frontiers(id)
                .expect("connected client should return frontier observations")
                .expect("reported collection frontiers should be cached");
            assert_eq!(cached.read_frontier, Some(read_frontier));
            assert_eq!(cached.write_frontier, Some(Antichain::new()));
        }
        drop(peer);
        bounded(changed.changed())
            .await
            .expect("frontier watcher should receive a change notification");
        assert!(matches!(
            client.collection_frontiers(id),
            Err(QueryError::Disconnected(_))
        ));
    }

    #[mz_ore::test(tokio::test)]
    async fn peeks_route_by_uuid_and_drop_cancels_only_owner() {
        let (client, mut peer) = connect().await;
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();
        let mut first_result = Box::pin(client.peek(peek(first)));
        let mut second_result = Box::pin(client.peek(peek(second)));
        assert!(futures::poll!(&mut first_result).is_pending());
        assert!(futures::poll!(&mut second_result).is_pending());
        assert!(matches!(peer.command().await, ComputeCommand::Peek(p) if p.uuid == first));
        assert!(matches!(peer.command().await, ComputeCommand::Peek(p) if p.uuid == second));
        let response = PeekResponse::Error(PeekError::unstructured("second"));
        let context = OpenTelemetryContext::empty();
        peer.respond(ComputeResponse::PeekResponse(
            second,
            response.clone(),
            context.clone(),
        ));
        assert_eq!(
            bounded(second_result)
                .await
                .expect("second peek should receive its response"),
            (response, context)
        );
        drop(first_result);
        assert!(
            matches!(peer.command().await, ComputeCommand::CancelPeek { uuid } if uuid == first)
        );
        let mut pending = Box::pin(client.peek(peek(Uuid::new_v4())));
        assert!(futures::poll!(&mut pending).is_pending());
        assert!(matches!(peer.command().await, ComputeCommand::Peek(_)));
        drop(peer);
        assert!(matches!(
            bounded(pending).await,
            Err(QueryError::Disconnected(_))
        ));
    }

    #[mz_ore::test(tokio::test)]
    async fn creation_buffers_early_responses_and_schedules_only_after_ack() {
        let (client, mut peer) = connect().await;
        let id = GlobalId::Transient(1);
        let mut creating = Box::pin(client.create_dataflow(subscribe(id)));
        assert!(futures::poll!(&mut creating).is_pending());
        let ComputeCommand::CreateQueryDataflow { request_id, .. } = peer.command().await else {
            panic!("expected creation");
        };
        let response =
            SubscribeResponse::Batch(mz_compute_client::protocol::response::SubscribeBatch {
                lower: Antichain::from_elem(Timestamp::from(0)),
                upper: Antichain::from_elem(Timestamp::from(2)),
                updates: Ok(vec![]),
            });
        peer.respond(ComputeResponse::SubscribeResponse(id, response.clone()));
        assert!(futures::poll!(&mut creating).is_pending());
        assert!(peer.commands.try_recv().is_err());
        peer.respond(ComputeResponse::QueryDataflowResponse {
            request_id,
            error: None,
        });
        let mut dataflow = bounded(creating)
            .await
            .expect("acknowledged dataflow creation should succeed");
        assert!(matches!(peer.command().await, ComputeCommand::Schedule(export) if export == id));
        let Some(Ok(DataflowResponse::Subscribe(export, batch))) = bounded(dataflow.recv()).await
        else {
            panic!("expected buffered subscribe response");
        };
        assert_eq!(export, id);
        assert_eq!(batch, response);
        // The dataflow guard, not the caller's handle, owns the live connection.
        drop(client);
        drop(dataflow);
        bounded(peer.responses.closed()).await;
    }

    #[mz_ore::test(tokio::test)]
    async fn abandoned_creation_drops_exports_without_waiting_for_ack() {
        let (client, mut peer) = connect().await;
        let id = GlobalId::Transient(1);
        let mut creating = Box::pin(client.create_dataflow(subscribe(id)));
        assert!(futures::poll!(&mut creating).is_pending());
        let ComputeCommand::CreateQueryDataflow { request_id, .. } = peer.command().await else {
            panic!("expected creation");
        };
        drop(creating);
        let ComputeCommand::AllowCompaction {
            id: export,
            frontier,
        } = peer.command().await
        else {
            panic!("expected abandoned export cleanup");
        };
        assert_eq!(export, id);
        assert!(frontier.is_empty());
        // A late ACK must not resurrect or schedule a dropped export.
        peer.respond(ComputeResponse::QueryDataflowResponse {
            request_id,
            error: None,
        });
        let uuid = Uuid::new_v4();
        let mut barrier = Box::pin(client.peek(peek(uuid)));
        assert!(futures::poll!(&mut barrier).is_pending());
        assert!(matches!(peer.command().await, ComputeCommand::Peek(p) if p.uuid == uuid));
        peer.respond(ComputeResponse::PeekResponse(
            uuid,
            PeekResponse::Canceled,
            OpenTelemetryContext::empty(),
        ));
        bounded(barrier)
            .await
            .expect("barrier peek should receive its response");
        assert!(peer.commands.try_recv().is_err());
    }

    #[mz_ore::test(tokio::test)]
    async fn result_limit_preserves_structured_peek_error() {
        let (client, mut peer) = connect().await;
        let uuid = Uuid::new_v4();
        let mut result = Box::pin(client.peek(peek(uuid)));
        assert!(futures::poll!(&mut result).is_pending());
        assert!(matches!(peer.command().await, ComputeCommand::Peek(_)));
        let text = "x".repeat(2048);
        let row = mz_repr::Row::pack([mz_repr::Datum::String(&text)]);
        let rows = mz_expr::row::RowCollection::new(
            vec![(
                row,
                std::num::NonZeroUsize::new(1).expect("row multiplicity is nonzero"),
            )],
            &[],
        );
        peer.respond(ComputeResponse::PeekResponse(
            uuid,
            PeekResponse::Rows(vec![rows]),
            OpenTelemetryContext::empty(),
        ));
        assert!(matches!(
            bounded(result)
                .await
                .expect("peek should receive its response")
                .0,
            PeekResponse::Error(PeekError::ResultExceedsMaxSize {
                max_result_size: 1024
            })
        ));
    }

    #[mz_ore::test(tokio::test)]
    async fn dataflow_drop_is_local_and_connection_loss_fails_survivor() {
        let (client, mut peer) = connect().await;
        let first = GlobalId::Transient(1);
        let second = GlobalId::Transient(2);
        let mut creating_first = Box::pin(client.create_dataflow(subscribe(first)));
        let mut creating_second = Box::pin(client.create_dataflow(subscribe(second)));
        assert!(futures::poll!(&mut creating_first).is_pending());
        assert!(futures::poll!(&mut creating_second).is_pending());
        let mut requests = vec![];
        for _ in 0..2 {
            let ComputeCommand::CreateQueryDataflow { request_id, .. } = peer.command().await
            else {
                panic!("expected creation");
            };
            requests.push(request_id);
        }
        for request_id in requests {
            peer.respond(ComputeResponse::QueryDataflowResponse {
                request_id,
                error: None,
            });
        }
        let first_dataflow = bounded(creating_first)
            .await
            .expect("first acknowledged dataflow creation should succeed");
        let mut second_dataflow = bounded(creating_second)
            .await
            .expect("second acknowledged dataflow creation should succeed");
        assert!(matches!(peer.command().await, ComputeCommand::Schedule(id) if id == first));
        assert!(matches!(peer.command().await, ComputeCommand::Schedule(id) if id == second));
        drop(first_dataflow);
        assert!(matches!(peer.command().await,
            ComputeCommand::AllowCompaction { id, frontier } if id == first && frontier.is_empty()
        ));
        drop(peer);
        assert!(matches!(
            bounded(second_dataflow.recv()).await,
            Some(Err(QueryError::Disconnected(_)))
        ));
    }

    #[mz_ore::test(tokio::test)]
    async fn dropping_handshake_closes_transport() {
        let (wire, mut peer) = wire();
        let mut connecting = Box::pin(ReplicaQueryClient::connect(wire, 1024));
        assert!(futures::poll!(&mut connecting).is_pending());
        assert!(matches!(
            peer.command().await,
            ComputeCommand::HelloQuery { .. }
        ));
        drop(connecting);
        bounded(peer.responses.closed()).await;
    }
}
