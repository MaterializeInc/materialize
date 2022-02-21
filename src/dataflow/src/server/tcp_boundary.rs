// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! This module provides a TCP-based boundary.

use std::any::Any;
use std::rc::Rc;

use differential_dataflow::Collection;
use serde::{Deserialize, Serialize};
use timely::dataflow::operators::capture::EventCore;
use timely::dataflow::Scope;
use timely::logging::WorkerIdentifier;
use tokio_serde::formats::Bincode;
use tokio_util::codec::LengthDelimitedCodec;

use mz_dataflow_types::{DataflowError, SourceInstanceKey};
use mz_expr::GlobalId;
use mz_repr::{Diff, Row, Timestamp};

use crate::server::boundary::{ComputeReplay, StorageCapture};
use crate::server::tcp_boundary::client::TcpEventLinkClientHandle;
use crate::server::tcp_boundary::server::TcpEventLinkHandle;

pub type SourceId = (GlobalId, GlobalId);

pub mod server {
    use crate::server::boundary::StorageCapture;
    use crate::server::tcp_boundary::{
        length_delimited_codec, Command, Framed, Response, SourceId,
    };
    use differential_dataflow::Collection;
    use futures::{SinkExt, StreamExt};
    use mz_dataflow_types::{DataflowError, SourceInstanceKey};
    use mz_expr::GlobalId;
    use mz_repr::{Diff, Row, Timestamp};
    use std::any::Any;
    use std::collections::{HashMap, HashSet};
    use std::net::SocketAddr;
    use std::rc::Rc;
    use std::sync::{Arc, Weak};
    use std::thread::JoinHandle;
    use timely::dataflow::operators::capture::{EventCore, EventPusherCore};
    use timely::dataflow::operators::Capture;
    use timely::dataflow::Scope;
    use tokio::io::{AsyncRead, AsyncWrite};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::select;
    use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
    use tokio::sync::{oneshot, Mutex};
    use tokio_serde::formats::Bincode;

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
        stash: HashMap<SourceId, Vec<Response>>,
        channels: HashMap<ClientId, UnboundedSender<Response>>,
        association: HashMap<SourceId, ClientId>,
        tokens: HashMap<SourceId, Vec<Arc<()>>>,
    }

    /// Start the boundary server on the specified address.
    ///
    /// Returns a handle implementing [StorageCapture] and a join handle to await termination.
    pub fn serve(addr: SocketAddr) -> (TcpEventLinkHandle, JoinHandle<()>) {
        let (announce_tx, announce_rx) = unbounded_channel();

        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .build()
                .unwrap();
            let shared = Default::default();
            let state = Arc::new(Mutex::new(shared));

            runtime.block_on(accept(&addr, state, announce_rx)).unwrap();
        });

        (
            TcpEventLinkHandle {
                announce: announce_tx,
            },
            thread,
        )
    }

    /// Accept connections on the socket.
    async fn accept(
        addr: &SocketAddr,
        state: Arc<Mutex<Shared>>,
        mut announce_rx: UnboundedReceiver<(
            SourceId,
            oneshot::Sender<UnboundedSender<Response>>,
            Arc<()>,
        )>,
    ) -> std::io::Result<()> {
        let listener = TcpListener::bind(addr).await?;
        tracing::debug!("server listening {listener:?}");

        let (tx, mut rx) = unbounded_channel();

        loop {
            select! {
                _ = async {
                    let mut client_id = 0;
                    loop {
                        client_id += 1;
                        match listener.accept().await {
                            Ok((socket, _)) => {
                                let state = Arc::clone(&state);
                                tokio::spawn(async move {
                                    handle_compute(client_id, Arc::clone(&state), socket).await
                                });
                            }
                            Err(e) => {
                                tracing::warn!("Failed to accept connection: {e}");
                            }
                        }
                    }
                } => {}
                announce = announce_rx.recv() => match announce {
                    Some((source_id, channel, token)) => {
                        state.lock().await.tokens.entry(source_id).or_default().push(token);
                        channel.send(tx.clone()).unwrap();
                    }
                    None => return Ok(()),
                },
                response = rx.recv() => {
                    let response = response.expect("Channel closed prematurely");
                    let mut state = state.lock().await;
                    let source_id = match &response {
                        Response::Err(source_id, _, _) | Response::Data(source_id, _, _) => source_id
                    };
                    if let Some(client_id) = state.association.get(source_id).copied() {
                        state.channels.get_mut(&client_id).unwrap().send(response).unwrap();
                    } else {
                        state.stash.entry(*source_id).or_default().push(response);
                    }
                }
            }
        }
    }

    /// Handle the server side of a compute client connection.
    ///
    /// This function calls into the inner part and upon client termination cleans up state.
    async fn handle_compute(
        client_id: ClientId,
        state: Arc<Mutex<Shared>>,
        socket: TcpStream,
    ) -> std::io::Result<()> {
        let mut active_keys = HashSet::new();

        let result =
            handle_compute_inner(client_id, Arc::clone(&state), socket, &mut active_keys).await;
        let mut state = state.lock().await;
        for key in active_keys {
            state.association.remove(&key);
        }
        state.channels.remove(&client_id);
        result
    }

    /// Inner portion to handle a compute client.
    async fn handle_compute_inner(
        client_id: ClientId,
        state: Arc<Mutex<Shared>>,
        socket: TcpStream,
        active_keys: &mut HashSet<SourceId>,
    ) -> std::io::Result<()> {
        let mut connection = framed_server(socket);

        let (client_tx, mut client_rx) = unbounded_channel();
        state.lock().await.channels.insert(client_id, client_tx);

        loop {
            select! {
                cmd = connection.next() => match cmd {
                    Some(Ok(Command::Subscribe(key))) => {
                        let new = active_keys.insert(key);
                        assert!(new, "Duplicate key: {key:?}");
                        let stashed = {
                            let mut state = state.lock().await;
                            state.association.insert(key, client_id);
                            state.stash.remove(&key).unwrap_or_default()
                        };
                        for stashed in stashed {
                            connection.send(stashed).await?;
                        }
                    }
                    Some(Ok(Command::Unsubscribe(key))) => {
                        let mut state = state.lock().await;
                        state.tokens.remove(&key);
                        state.association.remove(&key);
                        let removed = active_keys.remove(&key);
                        assert!(removed, "Unknown key: {key:?}");
                    }
                    Some(Err(e)) => {
                        return Err(e);
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
        announce: UnboundedSender<(
            SourceId,
            oneshot::Sender<UnboundedSender<Response>>,
            Arc<()>,
        )>,
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
            let key = (dataflow_id, id.identifier);
            let client_token = Arc::new(());

            let (tx, rx) = oneshot::channel();
            self.announce
                .send((key, tx, Arc::clone(&client_token)))
                .unwrap();

            let sender = rx.blocking_recv().unwrap();

            let partition = ok.inner.scope().index().try_into().unwrap();

            ok.inner.capture_into(UnboundedEventPusher {
                sender: sender.clone(),
                convert: move |event| Response::Data(key, partition, event),
                token: Some(Rc::clone(&token)),
                client_token: Arc::downgrade(&client_token),
            });
            err.inner.capture_into(UnboundedEventPusher {
                sender,
                convert: move |event| Response::Err(key, partition, event),
                token: Some(token),
                client_token: Arc::downgrade(&client_token),
            });
        }
    }

    struct UnboundedEventPusher<F> {
        sender: UnboundedSender<Response>,
        convert: F,
        token: Option<Rc<dyn Any>>,
        client_token: Weak<()>,
    }

    impl<T, D, F: Fn(EventCore<T, D>) -> Response> EventPusherCore<T, D> for UnboundedEventPusher<F> {
        fn push(&mut self, event: EventCore<T, D>) {
            if self.client_token.upgrade().is_none() && self.token.is_some() {
                self.token.take();
            }
            match self.sender.send((self.convert)(event)) {
                Err(_) => {
                    self.token.take();
                }
                _ => {}
            }
        }
    }
}

/// A struct providing both halfs of the tcp boundary. This only serves as a slot-in replacement
/// for the event link boundary but should disappear soon.
#[derive(Debug, Clone)]
pub struct TcpBoundary {
    storage_handle: TcpEventLinkHandle,
    compute_handle: TcpEventLinkClientHandle,
}

impl TcpBoundary {
    pub fn new(
        storage_handle: TcpEventLinkHandle,
        compute_handle: TcpEventLinkClientHandle,
    ) -> Self {
        Self {
            storage_handle,
            compute_handle,
        }
    }
}

impl StorageCapture for TcpBoundary {
    fn capture<G: Scope<Timestamp = Timestamp>>(
        &mut self,
        id: SourceInstanceKey,
        ok: Collection<G, Row, Diff>,
        err: Collection<G, DataflowError, Diff>,
        token: Rc<dyn Any>,
        name: &str,
        dataflow_id: GlobalId,
    ) {
        self.storage_handle
            .capture(id, ok, err, token, name, dataflow_id)
    }
}

impl ComputeReplay for TcpBoundary {
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
        self.compute_handle.replay(id, scope, name, dataflow_id)
    }
}

pub mod client {
    use crate::activator::{ActivatorTrait, ArcActivator};
    use crate::replay::MzReplay;
    use crate::server::boundary::ComputeReplay;
    use crate::server::tcp_boundary::{
        length_delimited_codec, Command, Framed, Response, SourceId,
    };
    use differential_dataflow::{AsCollection, Collection};
    use futures::{SinkExt, StreamExt};
    use mz_dataflow_types::{DataflowError, SourceInstanceKey};
    use mz_expr::GlobalId;
    use mz_repr::{Diff, Row, Timestamp};
    use std::any::Any;
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::rc::Rc;
    use std::thread::JoinHandle;
    use std::time::Duration;
    use timely::dataflow::operators::capture::event::EventIteratorCore;
    use timely::dataflow::operators::capture::EventCore;
    use timely::dataflow::Scope;
    use timely::logging::WorkerIdentifier;
    use tokio::io::{AsyncRead, AsyncWrite};
    use tokio::net::TcpStream;
    use tokio::select;
    use tokio::sync::mpsc::error::TryRecvError;
    use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
    use tokio::sync::oneshot;
    use tokio_serde::formats::Bincode;

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
    }

    /// State per worker and source.
    struct WorkerState<T, D, A: ActivatorTrait> {
        sender: UnboundedSender<EventCore<T, D>>,
        activator: Option<A>,
    }

    /// Connect to a storage boundary server. Returns a handle to replay sources and a join handle
    /// to await termination.
    pub fn connect(addr: SocketAddr, workers: usize) -> (TcpEventLinkClientHandle, JoinHandle<()>) {
        let (announce_tx, announce_rx) = unbounded_channel();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .enable_time()
                .build()
                .unwrap();

            runtime
                .block_on(run_client(&addr, announce_rx, workers))
                .unwrap();
        });

        (TcpEventLinkClientHandle { announce_tx }, thread)
    }

    async fn run_client(
        addr: &SocketAddr,
        mut announce_rx: UnboundedReceiver<Announce>,
        workers: usize,
    ) -> std::io::Result<()> {
        let stream = {
            // TODO(mh@): Hack to retry connecting to the storage server.
            let mut retries = 10;
            loop {
                match TcpStream::connect(addr).await {
                    Ok(stream) => break stream,
                    Err(_) if retries > 0 => {
                        retries -= 1;
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                    Err(e) => return Err(e),
                }
            }
        };
        tracing::debug!("client: connected to server");
        let mut client = framed_client(stream);

        // State per source_id: (ok state, err state)
        let mut channels: HashMap<
            SourceId,
            (
                HashMap<WorkerIdentifier, WorkerState<_, _, ArcActivator>>,
                HashMap<WorkerIdentifier, WorkerState<_, _, ArcActivator>>,
            ),
        > = Default::default();

        loop {
            select! {
                response = client.next() => match response {
                    Some(Ok(Response::Data(source_id, partition, event))) => {
                        let channel_queue = &mut channels.get_mut(&source_id).unwrap().0;
                        let worker = partition % workers;
                        let channel = channel_queue.get_mut(&worker).unwrap();
                        channel.sender.send(event).unwrap();
                        if let Some(activator) = &mut channel.activator {
                            activator.activate();
                        }
                    }
                    Some(Ok(Response::Err(source_id, partition, event))) => {
                        let channel_queue = &mut channels.get_mut(&source_id).unwrap().1;
                        let worker = partition % workers;
                        let channel = channel_queue.get_mut(&worker).unwrap();
                        channel.sender.send(event).unwrap();
                        if let Some(activator) = &mut channel.activator {
                            activator.activate();
                        }
                    }
                    Some(Err(e)) => return Err(e),
                    None => break,
                },
                announce = announce_rx.recv() => match announce {
                    Some(Announce::Register(source_id, worker, sender )) => {
                        let (ok_tx, ok_rx) = unbounded_channel();
                        let (err_tx, err_rx) = unbounded_channel();
                        sender.send((ok_rx, err_rx)).unwrap();

                        let state = channels.entry(source_id).or_default();
                        state.0.insert(worker, WorkerState { sender: ok_tx, activator: None});
                        state.1.insert(worker, WorkerState { sender: err_tx, activator: None});

                    }
                    Some(Announce::Activate(source_id, worker, ok_activator, err_activator)) => {
                        let state = channels.get_mut(&source_id).unwrap();
                        state.0.get_mut(&worker).unwrap().activator = Some(ok_activator);
                        state.1.get_mut(&worker).unwrap().activator = Some(err_activator);

                        // TODO: We only want to send a `Subscribe` message once all local workers
                        // announced their presence. Otherwise, we'd need to stash data locally.
                        // The following statement counts the number of activated workers.
                        let active_workers = state.0.values().into_iter().flat_map(|state| &state.activator).count();
                        if active_workers == workers {
                            client.send(Command::Subscribe(source_id)).await?;
                        }
                    }
                    Some(Announce::Drop(source_id)) => {
                        if let Some(_state) = channels.remove(&source_id) {
                            client.send(Command::Unsubscribe(source_id)).await.unwrap();
                        }
                    }
                    None => break,
                }
            }
        }

        Ok(())
    }

    #[derive(Debug)]
    enum Announce {
        Activate(SourceId, WorkerIdentifier, ArcActivator, ArcActivator),
        Drop(SourceId),
        Register(
            SourceId,
            WorkerIdentifier,
            oneshot::Sender<(
                UnboundedReceiver<EventCore<Timestamp, Vec<(Row, Timestamp, Diff)>>>,
                UnboundedReceiver<EventCore<Timestamp, Vec<(DataflowError, Timestamp, Diff)>>>,
            )>,
        ),
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
            let key = (dataflow_id, id.identifier);
            let (tx, rx) = oneshot::channel();
            self.announce_tx
                .send(Announce::Register(key, scope.index(), tx))
                .unwrap();
            let (ok_rx, err_rx) = rx.blocking_recv().unwrap();

            let token = Rc::new(DropReplay {
                announce_tx: self.announce_tx.clone(),
                message: Some(Announce::Drop(key)),
            });

            let ok_activator = ArcActivator::new(format!("{name}-ok-activator"), 1);
            let ok = Some(UnboundedEventPuller::new(ok_rx))
                .mz_replay(
                    scope,
                    &format!("{name}-ok"),
                    Duration::MAX,
                    ok_activator.clone(),
                )
                .as_collection();
            let err_activator = ArcActivator::new(format!("{name}-err-activator"), 1);
            let err = Some(UnboundedEventPuller::new(err_rx))
                .mz_replay(
                    scope,
                    &format!("{name}-err"),
                    Duration::MAX,
                    err_activator.clone(),
                )
                .as_collection();

            self.announce_tx
                .send(Announce::Activate(
                    key,
                    scope.index(),
                    ok_activator,
                    err_activator,
                ))
                .unwrap();

            (ok, err, Rc::new(token))
        }
    }

    struct DropReplay {
        announce_tx: UnboundedSender<Announce>,
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

    struct UnboundedEventPuller<T, D> {
        receiver: UnboundedReceiver<EventCore<T, D>>,
        element: Option<EventCore<T, D>>,
    }

    impl<T, D> UnboundedEventPuller<T, D> {
        fn new(receiver: UnboundedReceiver<EventCore<T, D>>) -> Self {
            Self {
                receiver,
                element: None,
            }
        }
    }

    impl<T, D> EventIteratorCore<T, D> for UnboundedEventPuller<T, D> {
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

#[derive(Clone, Debug, Serialize, Deserialize)]
enum Command {
    Subscribe(SourceId),
    Unsubscribe(SourceId),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum Response {
    Data(
        SourceId,
        WorkerIdentifier,
        EventCore<mz_repr::Timestamp, Vec<(mz_repr::Row, mz_repr::Timestamp, mz_repr::Diff)>>,
    ),
    Err(
        SourceId,
        WorkerIdentifier,
        EventCore<mz_repr::Timestamp, Vec<(DataflowError, mz_repr::Timestamp, mz_repr::Diff)>>,
    ),
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
