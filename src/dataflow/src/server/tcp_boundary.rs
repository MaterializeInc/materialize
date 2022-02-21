// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! This module provides a TCP-based boundary.
//!
//! # Architecture
//!
//! STORAGE hosts a [TcpEveltLinkServer], which has its own runtime. Sources connect to it using
//! handles, which provide means to register a source.
//!

use std::any::Any;
use std::rc::Rc;

use differential_dataflow::Collection;
use serde::{Deserialize, Serialize};
use timely::dataflow::operators::capture::EventCore;
use timely::dataflow::Scope;
use timely::logging::WorkerIdentifier;
use tokio::io::{AsyncRead, AsyncWrite};
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
    use crate::server::tcp_boundary::{framed_server, Command, Response, SourceId};
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
    use tokio::net::{TcpListener, TcpStream};
    use tokio::select;
    use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
    use tokio::sync::{oneshot, Mutex};

    type ClientId = u64;

    #[derive(Debug, Default)]
    struct Shared {
        stash: HashMap<SourceId, Vec<Response>>,
        channels: HashMap<ClientId, UnboundedSender<Response>>,
        association: HashMap<SourceId, ClientId>,
        tokens: HashMap<SourceId, Vec<Arc<()>>>,
    }

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
        println!("server listening {listener:?}");

        let (tx, mut rx) = unbounded_channel();

        loop {
            select! {
                _ = async {
                    let mut client_id = 0;
                    loop {
                        client_id += 1;
                        let (socket, _) = listener.accept().await?;
                        println!("server: client connected {socket:?}");
                        let state = Arc::clone(&state);
                        tokio::spawn(async move {
                            println!("server: spawned");
                            handle_compute(client_id, state, socket).await
                        });
                    }
                    Ok::<_, std::io::Error>(())
                } => {}
                announce = announce_rx.recv() => match announce {
                    Some((source_id, channel, token)) => {
                        println!("server: announce {source_id:?}");
                        state.lock().await.tokens.entry(source_id).or_default().push(token);
                        channel.send(tx.clone()).unwrap();
                    }
                    None => {
                        eprintln!("announce closed");
                        todo!();
                    }
                },
                response = rx.recv() => match response {
                    Some(response) => {
                        // println!("server: rx response");
                        let mut state = state.lock().await;
                        let source_id = match &response {
                            Response::Err(source_id, _, _) | Response::Data(source_id, _, _) => source_id
                        };
                        // println!("server: rx response {source_id:?}");
                        if let Some(client_id) = state.association.get(source_id).copied() {
                            // println!("server: sending to client");
                            state.channels.get_mut(&client_id).unwrap().send(response).unwrap();
                        } else {
                            println!("server: stashing");
                            state.stash.entry(*source_id).or_default().push(response);
                        }
                    }
                    None => {
                        println!("server: rx none");
                        todo!();
                    }
                }
            }
        }
    }

    async fn handle_compute(
        client_id: ClientId,
        state: Arc<Mutex<Shared>>,
        socket: TcpStream,
    ) -> std::io::Result<()> {
        let mut connection = framed_server(socket);

        let (client_tx, mut client_rx) = unbounded_channel();
        state.lock().await.channels.insert(client_id, client_tx);

        let mut active_keys = HashSet::new();

        loop {
            select! {
                cmd = connection.next() => match cmd {
                    Some(Ok(Command::Subscribe(key))) => {
                        println!("{key:?} server: client subscribe");
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
                        println!("{key:?} server: client unsubscribe");
                        let mut state = state.lock().await;
                        state.association.remove(&key);
                        let removed = active_keys.remove(&key);
                        state.tokens.remove(&key);
                        assert!(removed, "Unknown key: {key:?}");
                    }
                    Some(Err(e)) => {
                        eprintln!("Connection receiver error: {e:?}");
                        let mut state = state.lock().await;
                        for key in active_keys {
                            state.association.remove(&key);
                        }
                        return Err(e);
                    }
                    None => {
                        eprintln!("connection closed");
                        let mut state = state.lock().await;
                        for key in active_keys {
                            state.association.remove(&key);
                        }
                        return Ok(());
                    }
                },
                response = client_rx.recv() => match response {
                    Some(response) => {
                        // println!("server: sending to client");
                        connection.send(response).await?
                    },
                    None => {
                        eprintln!("channel closed");
                        todo!();
                        // TODO: clean up
                        return Ok(());
                    }
                }
            }
        }
    }

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
            name: &str,
            dataflow_id: GlobalId,
        ) {
            let key = (dataflow_id, id.identifier);
            println!("{key:?} capture {name}: about to announce");
            let client_token = Arc::new(());
            let (tx, rx) = oneshot::channel();
            self.announce
                .send((key, tx, Arc::clone(&client_token)))
                .unwrap();
            println!("{key:?} capture {name}: announce sent");
            let sender = rx.blocking_recv().unwrap();
            println!("{key:?} capture {name}: announce got sender");

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
            // println!("pusher: push");
            if self.client_token.upgrade().is_none() && self.token.is_some() {
                println!("pusher: client_token dropped");
                self.token.take();
            }
            match self.sender.send((self.convert)(event)) {
                Err(_) => {
                    println!("pusher: push -> err");
                    self.token.take();
                }
                _ => {}
            }
        }
    }
}

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
    use crate::server::tcp_boundary::{framed_client, Command, Response, SourceId};
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
    use tokio::net::TcpStream;
    use tokio::select;
    use tokio::sync::mpsc::error::TryRecvError;
    use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
    use tokio::sync::oneshot;

    #[derive(Debug, Clone)]
    pub struct TcpEventLinkClientHandle {
        announce_tx: UnboundedSender<Announce>,
    }

    struct WorkerState<T, D, A: ActivatorTrait> {
        sender: UnboundedSender<EventCore<T, D>>,
        activator: Option<A>,
    }

    pub fn connect(addr: SocketAddr, workers: usize) -> (TcpEventLinkClientHandle, JoinHandle<()>) {
        let (announce_tx, announce_rx) = unbounded_channel();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .enable_time()
                .build()
                .unwrap();

            runtime
                .block_on(connect_inner(&addr, announce_rx, workers))
                .unwrap();
        });

        (TcpEventLinkClientHandle { announce_tx }, thread)
    }

    async fn connect_inner(
        addr: &SocketAddr,
        mut announce_rx: UnboundedReceiver<Announce>,
        workers: usize,
    ) -> std::io::Result<()> {
        let stream = {
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
        println!("client: connected to server");
        let mut client = framed_client(stream);

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
                        // println!("client: data response {source_id:?}");
                        let channel_queue = &mut channels.get_mut(&source_id).unwrap().0;
                        let worker = partition % workers;
                        let channel = channel_queue.get_mut(&worker).unwrap();
                        channel.sender.send(event).unwrap();
                        if let Some(activator) = &mut channel.activator {
                            activator.activate();
                        }
                    }
                    Some(Ok(Response::Err(source_id, partition, event))) => {
                        // println!("client: err response {source_id:?}");
                        let channel_queue = &mut channels.get_mut(&source_id).unwrap().1;
                        let worker = partition % workers;
                        let channel = channel_queue.get_mut(&worker).unwrap();
                        channel.sender.send(event).unwrap();
                        if let Some(activator) = &mut channel.activator {
                            activator.activate();
                        }
                    }
                    Some(Err(e)) => {
                        println!("client: err {e:?}");
                        todo!();
                    }
                    None => {
                        println!("client: remote disconnected");
                        todo!();
                        break;
                    }

                },
                announce = announce_rx.recv() => match announce {
                    Some(Announce::Register(source_id, worker, sender )) => {
                        println!("{source_id:?} client: announce register worker {worker}");
                        let (ok_tx, ok_rx) = unbounded_channel();
                        let (err_tx, err_rx) = unbounded_channel();
                        sender.send((ok_rx, err_rx)).unwrap();

                        let state = channels.entry(source_id).or_default();
                        state.0.insert(worker, WorkerState { sender: ok_tx, activator: None});
                        state.1.insert(worker, WorkerState { sender: err_tx, activator: None});

                    }
                    Some(Announce::Activate(source_id, worker, ok_activator, err_activator)) => {
                        println!("{source_id:?} client: announce activate worker {worker}");
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
                        println!("{source_id:?} client: announce drop");
                        if let Some(_state) = channels.remove(&source_id) {
                            client.send(Command::Unsubscribe(source_id)).await.unwrap();
                        }
                    }
                    None => {
                        println!("client: announce none");
                        todo!();
                    }
                }
            }
        }

        println!("client: done");

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
            println!("{key:?} replay: about to register");
            self.announce_tx
                .send(Announce::Register(key, scope.index(), tx))
                .unwrap();
            println!("{key:?} replay: register sent, waiting for channels");
            let (ok_rx, err_rx) = rx.blocking_recv().unwrap();
            println!("{key:?} replay: got channels");

            let token = Rc::new(DropReplay {
                announce_tx: self.announce_tx.clone(),
                message: Some(Announce::Drop(key)),
            });

            let ok_activator = ArcActivator::new(format!("{name}-ok-activator"), 1);
            let ok = Some(UnboundedEventPuller::new(ok_rx))
                .mz_replay(
                    scope,
                    &format!("{name}-ok"),
                    Duration::from_millis(100),
                    ok_activator.clone(),
                )
                .as_collection();
            let err_activator = ArcActivator::new(format!("{name}-ok-activator"), 1);
            let err = Some(UnboundedEventPuller::new(err_rx))
                .mz_replay(
                    scope,
                    &format!("{name}-err"),
                    Duration::from_millis(100),
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
                println!("DropReplay: Sending {message:?}");
                // If the channel is closed, another thread
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
            // println!("puller: next");
            match self.receiver.try_recv() {
                Ok(element) => {
                    // println!("puller: next -> OK");
                    self.element = Some(element);
                    self.element.as_ref()
                }
                Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => {
                    // println!("puller: next -> {e:?}");
                    self.element.take();
                    None
                }
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Command {
    Subscribe(SourceId),
    Unsubscribe(SourceId),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Response {
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

/// A framed connection from the server's perspective.
pub type FramedServer<C> = Framed<C, Command, Response>;

/// A framed connection from the client's perspective.
pub type FramedClient<C> = Framed<C, Response, Command>;

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

/// Constructs a framed connection for the server.
pub fn framed_server<C>(conn: C) -> FramedServer<C>
where
    C: AsyncRead + AsyncWrite,
{
    tokio_serde::Framed::new(
        tokio_util::codec::Framed::new(conn, length_delimited_codec()),
        Bincode::default(),
    )
}

/// Constructs a framed connection for the client.
pub fn framed_client<C>(conn: C) -> FramedClient<C>
where
    C: AsyncRead + AsyncWrite,
{
    tokio_serde::Framed::new(
        tokio_util::codec::Framed::new(conn, length_delimited_codec()),
        Bincode::default(),
    )
}
