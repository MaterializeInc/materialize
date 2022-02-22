// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! This module provides a TCP-based boundary.

use serde::{Deserialize, Serialize};
use timely::dataflow::operators::capture::EventCore;
use timely::logging::WorkerIdentifier;
use tokio_serde::formats::Bincode;
use tokio_util::codec::LengthDelimitedCodec;

use mz_dataflow_types::DataflowError;
use mz_expr::GlobalId;

/// Type alias for source subscriptions, (dataflow_id, source_id).
pub type SourceId = (GlobalId, GlobalId);
/// Type alias for a source subscription including a source worker.
pub type SubscriptionId = (SourceId, WorkerIdentifier);

pub mod server {
    //! TCP boundary server
    use crate::server::boundary::StorageCapture;
    use crate::server::tcp_boundary::{length_delimited_codec, Command, Framed, Response};
    use crate::tcp_boundary::SubscriptionId;
    use differential_dataflow::Collection;
    use futures::{SinkExt, TryStreamExt};
    use mz_dataflow_types::{DataflowError, SourceInstanceKey};
    use mz_expr::GlobalId;
    use mz_repr::{Diff, Row, Timestamp};
    use std::any::Any;
    use std::collections::{HashMap, HashSet};
    use std::rc::Rc;
    use std::sync::{Arc, Weak};
    use timely::dataflow::operators::capture::{EventCore, EventPusherCore};
    use timely::dataflow::operators::Capture;
    use timely::dataflow::Scope;
    use tokio::io::{AsyncRead, AsyncWrite};
    use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
    use tokio::select;
    use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
    use tokio::sync::Mutex;
    use tokio::task::JoinHandle;
    use tokio_serde::formats::Bincode;
    use tracing::{debug, info, warn};

    /// A framed connection from the server's perspective.
    type FramedServer<C> = Framed<C, Command, Response>;

    /// Constructs a framed connection for the server.
    fn framed_server<C>(conn: C) -> FramedServer<C>
    where
        C: AsyncRead + AsyncWrite,
    {
        tokio_serde::Framed::new(
            tokio_util::codec::Framed::new(conn, length_delimited_codec()),
            Bincode::default(),
        )
    }

    /// Unique client identifier, only used internally
    type ClientId = u64;

    /// Shared state between main server loop and client-specific task.
    #[derive(Debug, Default)]
    struct Shared {
        subscriptions: HashMap<SubscriptionId, SourceState>,
        channels: HashMap<ClientId, UnboundedSender<Response>>,
    }

    /// Shared per-subscription state
    #[derive(Debug, Default)]
    struct SourceState {
        /// Pending responses stashed before client registered.
        stash: Vec<Response>,
        /// Client, once it registered
        client_id: Option<ClientId>,
        /// Tokens to drop to terminate source.
        token: Option<Arc<()>>,
    }

    /// Response from Timely workers to network handler.
    #[derive(Debug)]
    enum WorkerResponse {
        /// Announce the presence of a captured source.
        Announce {
            subscription_id: SubscriptionId,
            token: Arc<()>,
        },
        /// Data from a source
        Response(Response),
    }

    /// Start the boundary server listening for connections on `addr`.
    ///
    /// Returns a handle implementing [StorageCapture] and a join handle to await termination.
    pub async fn serve<A: ToSocketAddrs + std::fmt::Display>(
        addr: A,
    ) -> std::io::Result<(TcpEventLinkHandle, JoinHandle<std::io::Result<()>>)> {
        let (worker_tx, worker_rx) = unbounded_channel();

        info!("About to bind to {addr}");
        let listener = TcpListener::bind(addr).await?;
        info!(
            "listening for storage connection on  {:?}...",
            listener.local_addr()
        );

        let thread = mz_ore::task::spawn(|| "storage server", accept(listener, worker_rx));

        Ok((TcpEventLinkHandle { worker_tx }, thread))
    }

    /// Accept connections on `listener` and listening for data on `worker_rx`.
    async fn accept(
        listener: TcpListener,
        mut worker_rx: UnboundedReceiver<WorkerResponse>,
    ) -> std::io::Result<()> {
        debug!("server listening {listener:?}");

        let shared = Default::default();
        let state = Arc::new(Mutex::new(shared));

        loop {
            select! {
                _ = async {
                    let mut client_id = 0;
                    loop {
                        client_id += 1;
                        match listener.accept().await {
                            Ok((socket, addr)) => {
                                debug!("Accepting client {client_id} on {addr}...");
                                let state = Arc::clone(&state);
                                mz_ore::task::spawn(|| "client loop", async move {
                                    handle_compute(client_id, Arc::clone(&state), socket).await
                                });
                            }
                            Err(e) => {
                                warn!("Failed to accept connection: {e}");
                            }
                        }
                    }
                } => {}
                worker_response = worker_rx.recv() => match worker_response {
                    Some(WorkerResponse::Announce{subscription_id, token}) => {
                        debug!("Subscription announced: {subscription_id:?}");
                        let mut state = state.lock().await;
                        let subscription = state.subscriptions.entry(subscription_id).or_default();
                        subscription.token = Some(token);
                    }
                    Some(WorkerResponse::Response(response)) => {
                        let mut state = state.lock().await;
                        if let Some(subscription) = state.subscriptions.get_mut(&response.subscription_id) {
                            if let Some(client_id) = subscription.client_id {
                                state.channels.get_mut(&client_id).unwrap().send(response).unwrap();
                            } else {
                                subscription.stash.push(response);
                            }
                        }
                    }
                    None => return Ok(()),
                },
            }
        }
    }

    /// Handle the server side of a compute client connection in `socket`, identified by
    /// `client_id`. `state` is a handle to the shared state.
    ///
    /// This function calls into the inner part and upon client termination cleans up state.
    async fn handle_compute(
        client_id: ClientId,
        state: Arc<Mutex<Shared>>,
        socket: TcpStream,
    ) -> std::io::Result<()> {
        let mut source_ids = HashSet::new();
        let result =
            handle_compute_inner(client_id, Arc::clone(&state), socket, &mut source_ids).await;
        let mut state = state.lock().await;
        for source_id in source_ids {
            state.subscriptions.remove(&source_id);
        }
        state.channels.remove(&client_id);
        result
    }

    /// Inner portion to handle a compute client.
    async fn handle_compute_inner(
        client_id: ClientId,
        state: Arc<Mutex<Shared>>,
        socket: TcpStream,
        active_source_ids: &mut HashSet<SubscriptionId>,
    ) -> std::io::Result<()> {
        let mut connection = framed_server(socket);

        let (client_tx, mut client_rx) = unbounded_channel();
        state.lock().await.channels.insert(client_id, client_tx);

        loop {
            select! {
                cmd = connection.try_next() => match cmd? {
                    Some(Command::Subscribe(subscription_id, )) => {
                        debug!("Subscribe client {client_id} to {subscription_id:?}");
                        let new = active_source_ids.insert(subscription_id);
                        assert!(new, "Duplicate key: {subscription_id:?}");
                        let stashed = {
                            let mut state = state.lock().await;
                            let subscription = state.subscriptions.entry(subscription_id).or_default();
                            assert!(subscription.client_id.is_none());
                            subscription.client_id = Some(client_id);
                            std::mem::take(&mut subscription.stash)
                        };
                        for stashed in stashed {
                            connection.send(stashed).await?;
                        }
                    }
                    Some(Command::Unsubscribe(subscription_id)) => {
                        debug!("Unsubscribe client {client_id} from {subscription_id:?}");
                        let mut state = state.lock().await;
                        state.subscriptions.remove(&subscription_id);
                        let removed = active_source_ids.remove(&subscription_id);
                        assert!(removed, "Unknown key: {subscription_id:?}");
                    }
                    None => {
                        return Ok(());
                    }
                },
                response = client_rx.recv() => connection.send(response.expect("Channel closed before dropping source")).await?,
            }
        }
    }

    /// A handle to the boundary service. Implements [StorageCapture] to capture sources.
    #[derive(Clone, Debug)]
    pub struct TcpEventLinkHandle {
        worker_tx: UnboundedSender<WorkerResponse>,
    }

    impl StorageCapture for TcpEventLinkHandle {
        fn capture<G: Scope<Timestamp = Timestamp>>(
            &mut self,
            id: SourceInstanceKey,
            ok: Collection<G, Row, Diff>,
            err: Collection<G, DataflowError, Diff>,
            token: Rc<dyn Any>,
            _name: &str,
            dataflow_id: GlobalId,
        ) {
            let subscription_id = ((dataflow_id, id.identifier), ok.inner.scope().index());
            let client_token = Arc::new(());

            // Announce that we're capturing data, with the source ID and a token. Once the token
            // is dropped, we drop the `token` to terminate the source.
            self.worker_tx
                .send(WorkerResponse::Announce {
                    subscription_id,
                    token: Arc::clone(&client_token),
                })
                .unwrap();

            ok.inner.capture_into(UnboundedEventPusher::new(
                self.worker_tx.clone(),
                Rc::clone(&token),
                &client_token,
                move |event| {
                    WorkerResponse::Response(Response {
                        subscription_id,
                        data: Ok(event),
                    })
                },
            ));
            err.inner.capture_into(UnboundedEventPusher::new(
                self.worker_tx.clone(),
                token,
                &client_token,
                move |event| {
                    WorkerResponse::Response(Response {
                        subscription_id,
                        data: Err(event),
                    })
                },
            ));
        }
    }

    /// Helper struct to capture data into a sender and drop tokens once dropped itself.
    struct UnboundedEventPusher<R, F> {
        /// The sender to pass all data to.
        sender: UnboundedSender<R>,
        /// A function to convert the input data into something the sender can transport.
        convert: F,
        /// A token that's dropped once `client_token` is dropped.
        token: Option<Rc<dyn Any>>,
        /// A weak reference to a token that e.g. the network layer can drop.
        client_token: Weak<()>,
    }

    impl<R, F> UnboundedEventPusher<R, F> {
        /// Construct a new pusher. It'll retain a weak reference to `client_token`.
        fn new(
            sender: UnboundedSender<R>,
            token: Rc<dyn Any>,
            client_token: &Arc<()>,
            convert: F,
        ) -> Self {
            Self {
                sender,
                client_token: Arc::downgrade(client_token),
                convert,
                token: Some(token),
            }
        }
    }

    impl<T, D, R, F: Fn(EventCore<T, D>) -> R> EventPusherCore<T, D> for UnboundedEventPusher<R, F> {
        fn push(&mut self, event: EventCore<T, D>) {
            if self.client_token.upgrade().is_none() {
                self.token.take();
            }
            if self.token.is_some() {
                match self.sender.send((self.convert)(event)) {
                    Err(_) => {
                        self.token.take();
                    }
                    _ => {}
                }
            }
        }
    }
}

pub mod client {
    //! TCP boundary client
    use crate::activator::{ActivatorTrait, ArcActivator};
    use crate::replay::MzReplay;
    use crate::server::boundary::ComputeReplay;
    use crate::server::tcp_boundary::{
        length_delimited_codec, Command, Framed, Response, SourceId,
    };
    use differential_dataflow::{AsCollection, Collection};
    use futures::{Sink, SinkExt, TryStreamExt};
    use mz_dataflow_types::{DataflowError, SourceInstanceKey};
    use mz_expr::GlobalId;
    use mz_repr::{Diff, Row, Timestamp};
    use std::any::Any;
    use std::collections::HashMap;
    use std::rc::Rc;
    use std::time::Duration;
    use timely::dataflow::operators::capture::event::EventIteratorCore;
    use timely::dataflow::operators::capture::EventCore;
    use timely::dataflow::Scope;
    use timely::logging::WorkerIdentifier;
    use tokio::io::{AsyncRead, AsyncWrite};
    use tokio::net::{TcpStream, ToSocketAddrs};
    use tokio::select;
    use tokio::sync::mpsc::error::TryRecvError;
    use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
    use tokio::task::JoinHandle;
    use tokio_serde::formats::Bincode;
    use tracing::{debug, info, warn};

    /// A framed connection from the client's perspective.
    type FramedClient<C> = Framed<C, Response, Command>;

    /// Constructs a framed connection for the client.
    fn framed_client<C>(conn: C) -> FramedClient<C>
    where
        C: AsyncRead + AsyncWrite,
    {
        tokio_serde::Framed::new(
            tokio_util::codec::Framed::new(conn, length_delimited_codec()),
            Bincode::default(),
        )
    }

    /// A handle to the storage client. Implements [ComputeReplay].
    #[derive(Debug, Clone)]
    pub struct TcpEventLinkClientHandle {
        announce_tx: UnboundedSender<Announce>,
        storage_workers: usize,
    }

    /// State per worker and source.
    #[derive(Debug)]
    struct WorkerState<T, D, A: ActivatorTrait> {
        sender: UnboundedSender<EventCore<T, D>>,
        activator: Option<A>,
    }

    impl<T, D, A: ActivatorTrait> WorkerState<T, D, A> {
        /// Construct a new state object with the provided `sender` and no activator.
        fn new(sender: UnboundedSender<EventCore<T, D>>) -> Self {
            Self {
                sender,
                activator: None,
            }
        }

        /// Register an `activator` to wake the Timely operator.
        fn register(&mut self, activator: A) {
            self.activator = Some(activator);
        }

        /// Send data at the channel and activate if the activator is set.
        fn send_and_activate(&mut self, event: EventCore<T, D>) {
            let res = self.sender.send(event);
            if res.is_err() {
                warn!("Receiver hung up");
            }
            if let Some(activator) = &mut self.activator {
                activator.activate();
            }
        }
    }

    #[derive(Debug, Default)]
    struct SubscriptionState<T, R> {
        ok_state: HashMap<WorkerIdentifier, WorkerState<T, Vec<(Row, T, R)>, ArcActivator>>,
        err_state:
            HashMap<WorkerIdentifier, WorkerState<T, Vec<(DataflowError, T, R)>, ArcActivator>>,
    }

    #[derive(Debug)]
    struct ClientState {
        subscriptions: HashMap<SourceId, SubscriptionState<mz_repr::Timestamp, mz_repr::Diff>>,
        workers: usize,
    }

    impl ClientState {
        fn new(workers: usize) -> Self {
            Self {
                workers,
                subscriptions: Default::default(),
            }
        }

        async fn handle_announce<C: Sink<Command, Error = std::io::Error> + Unpin>(
            &mut self,
            client: &mut C,
            announce: Announce,
        ) -> std::io::Result<()> {
            match announce {
                Announce::Register {
                    source_id,
                    worker,
                    storage_workers,
                    ok_tx,
                    ok_activator,
                    err_activator,
                    err_tx,
                } => {
                    let state = self.subscriptions.entry(source_id).or_default();
                    state.ok_state.insert(worker, WorkerState::new(ok_tx));
                    state.err_state.insert(worker, WorkerState::new(err_tx));
                    let worker_state = state.ok_state.get_mut(&worker).unwrap();
                    worker_state.register(ok_activator);
                    let worker_state = state.err_state.get_mut(&worker).unwrap();
                    worker_state.register(err_activator);

                    // Send subscription command to server
                    debug!("Subscribing to {source_id:?}");
                    client
                        .send_all(&mut futures::stream::iter(storage_workers.into_iter().map(
                            |storage_worker| Ok(Command::Subscribe((source_id, storage_worker))),
                        )))
                        .await?;
                }
                Announce::Drop(source_id, worker, storage_workers) => {
                    // Announce to unsubscribe from the source.
                    if let Some(state) = self.subscriptions.get_mut(&source_id) {
                        state.ok_state.remove(&worker);
                        state.err_state.remove(&worker);
                        debug!("Unsubscribing from {source_id:?}");
                        client
                            .send_all(&mut futures::stream::iter(storage_workers.into_iter().map(
                                |storage_worker| {
                                    Ok(Command::Unsubscribe((source_id, storage_worker)))
                                },
                            )))
                            .await?;
                        let cleanup = state.ok_state.is_empty();
                        if cleanup {
                            self.subscriptions.remove(&source_id);
                        }
                    }
                }
            }
            Ok(())
        }

        async fn handle_response(
            &mut self,
            Response {
                subscription_id,
                data,
            }: Response,
        ) {
            if let Some(state) = self.subscriptions.get_mut(&subscription_id.0) {
                let worker = subscription_id.1 % self.workers;
                match data {
                    Ok(event) => {
                        // We might have dropped the local worker already but still receive data
                        if let Some(channel) = state.ok_state.get_mut(&worker) {
                            channel.send_and_activate(event);
                        }
                    }
                    Err(event) => {
                        // We might have dropped the local worker already but still receive data
                        if let Some(channel) = state.err_state.get_mut(&worker) {
                            channel.send_and_activate(event);
                        }
                    }
                }
            }
        }
    }

    /// Connect to a storage boundary server. Returns a handle to replay sources and a join handle
    /// to await termination.
    pub async fn connect<A: ToSocketAddrs + std::fmt::Debug>(
        addr: A,
        workers: usize,
        storage_workers: usize,
    ) -> std::io::Result<(TcpEventLinkClientHandle, JoinHandle<std::io::Result<()>>)> {
        let (announce_tx, announce_rx) = unbounded_channel();
        info!("About to connect to {addr:?}");
        let stream = TcpStream::connect(addr).await?;
        info!("Connected to storage server");
        let thread = mz_ore::task::spawn(
            || "storage client",
            run_client(stream, announce_rx, workers),
        );

        Ok((
            TcpEventLinkClientHandle {
                announce_tx,
                storage_workers,
            },
            thread,
        ))
    }

    /// Communicate data and subscriptions on `stream`, receiving local replay notifications on
    /// `announce_rx`. `workers` is the number of local Timely workers.
    async fn run_client(
        stream: TcpStream,
        mut announce_rx: UnboundedReceiver<Announce>,
        workers: usize,
    ) -> std::io::Result<()> {
        debug!("client: connected to server");
        let mut client = framed_client(stream);

        let mut state = ClientState::new(workers);

        loop {
            select! {
                response = client.try_next() => match response? {
                    Some(response) => state.handle_response(response).await,
                    None => break,
                },
                announce = announce_rx.recv() => match announce {
                    Some(announce) => state.handle_announce(&mut client, announce).await?,
                    None => break,
                }
            }
        }

        Ok(())
    }

    /// Announcement protocol from a Timely worker to the storage client.
    #[derive(Debug)]
    enum Announce {
        /// Provide activators for the source
        Register {
            source_id: SourceId,
            worker: WorkerIdentifier,
            storage_workers: Vec<WorkerIdentifier>,
            ok_activator: ArcActivator,
            err_activator: ArcActivator,
            ok_tx: UnboundedSender<EventCore<Timestamp, Vec<(Row, Timestamp, Diff)>>>,
            err_tx: UnboundedSender<EventCore<Timestamp, Vec<(DataflowError, Timestamp, Diff)>>>,
        },
        /// Replayer dropped
        Drop(SourceId, WorkerIdentifier, Vec<WorkerIdentifier>),
    }

    impl ComputeReplay for TcpEventLinkClientHandle {
        fn replay<G: Scope<Timestamp = Timestamp>>(
            &mut self,
            id: SourceInstanceKey,
            scope: &mut G,
            name: &str,
            dataflow_id: GlobalId,
        ) -> (
            Collection<G, Row, Diff>,
            Collection<G, DataflowError, Diff>,
            Rc<dyn Any>,
        ) {
            let source_id = (dataflow_id, id.identifier);
            // Create a channel to receive worker/replay-specific data channels
            let (ok_tx, ok_rx) = unbounded_channel();
            let (err_tx, err_rx) = unbounded_channel();
            let ok_activator = ArcActivator::new(format!("{name}-ok-activator"), 1);
            let err_activator = ArcActivator::new(format!("{name}-err-activator"), 1);

            let storage_workers = (0..self.storage_workers)
                .into_iter()
                .map(|x| scope.index() + x * scope.peers())
                .filter(|x| *x < self.storage_workers)
                .collect::<Vec<_>>();

            // Register with the storage client
            let register = Announce::Register {
                source_id,
                worker: scope.index(),
                storage_workers: storage_workers.clone(),
                ok_tx,
                err_tx,
                ok_activator: ok_activator.clone(),
                err_activator: err_activator.clone(),
            };
            self.announce_tx.send(register).unwrap();

            // Construct activators and replay data
            let mut ok_rx = Some(ok_rx);
            let ok = storage_workers
                .iter()
                .map(|_| {
                    UnboundedEventPuller::new(ok_rx.take().unwrap_or_else(|| unbounded_channel().1))
                })
                .mz_replay(scope, &format!("{name}-ok"), Duration::MAX, ok_activator)
                .as_collection();
            let mut err_rx = Some(err_rx);
            let err = storage_workers
                .iter()
                .map(|_| {
                    UnboundedEventPuller::new(
                        err_rx.take().unwrap_or_else(|| unbounded_channel().1),
                    )
                })
                .mz_replay(scope, &format!("{name}-err"), Duration::MAX, err_activator)
                .as_collection();

            // Construct token to unsubscribe from source
            let token = Rc::new(DropReplay {
                announce_tx: self.announce_tx.clone(),
                message: Some(Announce::Drop(source_id, scope.index(), storage_workers)),
            });

            (ok, err, Rc::new(token))
        }
    }

    /// Utility to send a message on drop.
    struct DropReplay {
        /// Channel where to send the message
        announce_tx: UnboundedSender<Announce>,
        /// Pre-defined message to send
        message: Option<Announce>,
    }

    impl Drop for DropReplay {
        fn drop(&mut self) {
            if let Some(message) = self.message.take() {
                // Igore errors on send as it indicates the client is no longer running
                let _ = self.announce_tx.send(message);
            }
        }
    }

    /// Puller reading from a `receiver` channel.
    struct UnboundedEventPuller<D> {
        /// The receiver to read from.
        receiver: UnboundedReceiver<D>,
        /// Current element
        element: Option<D>,
    }

    impl<D> UnboundedEventPuller<D> {
        /// Construct a new puller reading from `receiver`.
        fn new(receiver: UnboundedReceiver<D>) -> Self {
            Self {
                receiver,
                element: None,
            }
        }
    }

    impl<T, D> EventIteratorCore<T, D> for UnboundedEventPuller<EventCore<T, D>> {
        fn next(&mut self) -> Option<&EventCore<T, D>> {
            match self.receiver.try_recv() {
                Ok(element) => {
                    self.element = Some(element);
                    self.element.as_ref()
                }
                Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => {
                    self.element.take();
                    None
                }
            }
        }
    }
}

/// A command sent from the storage client to the storage server.
#[derive(Clone, Debug, Serialize, Deserialize)]
enum Command {
    /// Subscribe the client to `source_id`.
    Subscribe(SubscriptionId),
    /// Unsubscribe the client from `source_id`.
    Unsubscribe(SubscriptionId),
}

/// Data provided to the storage client from the storage server upon successful registration.
///
/// Each message has a source_id to identify the subscription, a worker that produced the data,
/// and the data itself. When replaying, the mapping of storage worker to compute worker needs
/// (at least) to be static.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Response {
    subscription_id: SubscriptionId,
    /// Contents of the Ok-collection
    data: Result<
        EventCore<mz_repr::Timestamp, Vec<(mz_repr::Row, mz_repr::Timestamp, mz_repr::Diff)>>,
        EventCore<mz_repr::Timestamp, Vec<(DataflowError, mz_repr::Timestamp, mz_repr::Diff)>>,
    >,
}

/// A framed connection to a storage server.
pub type Framed<C, T, U> =
    tokio_serde::Framed<tokio_util::codec::Framed<C, LengthDelimitedCodec>, T, U, Bincode<T, U>>;

fn length_delimited_codec() -> LengthDelimitedCodec {
    // NOTE(benesch): using an unlimited maximum frame length is problematic
    // because Tokio never shrinks its buffer. Sending or receiving one large
    // message of size N means the client will hold on to a buffer of size
    // N forever. We should investigate alternative transport protocols that
    // do not have this limitation.
    let mut codec = LengthDelimitedCodec::new();
    codec.set_max_frame_length(usize::MAX);
    codec
}
