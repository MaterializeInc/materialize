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

use crate::server::boundary::{ComputeReplay, StorageCapture};
use crate::server::tcp_boundary::client::TcpEventLinkClientHandle;
use crate::server::tcp_boundary::server::TcpEventLinkHandle;
use differential_dataflow::Collection;
use mz_dataflow_types::{DataflowError, SourceInstanceKey};
use mz_expr::GlobalId;
use mz_repr::{Diff, Row, Timestamp};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::rc::Rc;
use timely::dataflow::operators::capture::EventCore;
use timely::dataflow::Scope;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_serde::formats::Bincode;
use tokio_util::codec::LengthDelimitedCodec;

pub type SourceId = GlobalId;
pub mod server {
    use crate::server::boundary::StorageCapture;
    use crate::server::tcp_boundary::{framed_server, Command, Response, SourceId};
    use differential_dataflow::Collection;
    use futures::{SinkExt, StreamExt};
    use mz_dataflow_types::{DataflowError, SourceInstanceKey};
    use mz_repr::{Diff, Row, Timestamp};
    use std::any::Any;
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::thread::JoinHandle;
    use timely::dataflow::operators::capture::{EventCore, EventPusherCore};
    use timely::dataflow::operators::Capture;
    use timely::dataflow::Scope;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::select;
    use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
    use tokio::sync::{oneshot, Mutex};

    #[derive(Debug)]
    pub struct TcpEventLinkServer {
        state: Arc<Mutex<Shared>>,
        announce_rx: UnboundedReceiver<(SourceId, oneshot::Sender<UnboundedSender<Response>>)>,
    }

    type ClientId = u64;

    #[derive(Debug, Default)]
    struct Shared {
        stash: HashMap<SourceId, Vec<Response>>,
        pending_sources: HashMap<SourceId, Vec<oneshot::Sender<UnboundedSender<Response>>>>,
        channels: HashMap<ClientId, UnboundedSender<Response>>,
        association: HashMap<SourceId, ClientId>,
    }

    #[derive(Debug)]
    struct ConnectionState {
        rx: UnboundedReceiver<Response>,
    }

    impl TcpEventLinkServer {
        pub fn serve(addr: SocketAddr) -> (TcpEventLinkHandle, JoinHandle<()>) {
            let (announce_tx, announce_rx) = unbounded_channel();

            let thread = std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_io()
                    .build()
                    .unwrap();
                let shared = Default::default();
                let server = Self {
                    announce_rx,
                    state: Arc::new(Mutex::new(shared)),
                };

                runtime.block_on(server.accept(&addr)).unwrap();
            });

            (
                TcpEventLinkHandle {
                    announce: announce_tx,
                },
                thread,
            )
        }

        async fn accept(self, addr: &SocketAddr) -> std::io::Result<()> {
            let listener = TcpListener::bind(addr).await?;
            println!("server listening {listener:?}");
            // let mut conn_state: BTreeMap<SourceId, ConnectionState> = BTreeMap::new();
            // let mut ok_stash : HashMap<_, Vec<_>> = HashMap::new();
            // let mut err_stash: HashMap<_, Vec<_>> = HashMap::new();
            let state = self.state;
            let mut announce_rx = self.announce_rx;

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
                                Self::handle_compute(client_id, state, socket).await
                            });
                        }
                        Ok::<_, std::io::Error>(())
                    } => {}
                    announce = announce_rx.recv() => match announce {
                        Some((source_id, channel)) => {
                            println!("server: announce {source_id}");
                            channel.send(tx.clone()).unwrap();
                        }
                        None => {
                            eprintln!("announce closed");
                            // TODO
                        }
                    },
                    response = rx.recv() => match response {
                        Some(response) => {
                            println!("server: rx response");
                            let mut state = state.lock().await;
                            let source_id = match &response {
                                Response::Err(source_id, _) | Response::Data(source_id, _) => source_id
                            };
                            println!("server: rx response {source_id}");
                            if let Some(client_id) = state.association.get(source_id).copied() {
                                println!("server: sending to client");
                                state.channels.get_mut(&client_id).unwrap().send(response).unwrap();
                            } else {
                                println!("server: stashing");
                                state.stash.entry(*source_id).or_default().push(response);
                            }
                        }
                        None => {
                            println!("server: rx none");
                            // TODO
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
            println!("server: waiting for register");
            let mut client = match connection.next().await {
                Some(Ok(Command::Register)) => {
                    println!("server: client register");
                    let (tx, rx) = unbounded_channel();
                    state.lock().await.channels.insert(client_id, tx);
                    ConnectionState { rx }
                }
                Some(cmd) => {
                    eprintln!("Unexpected command: {cmd:?}");
                    return Ok(());
                }
                None => {
                    eprintln!("server: client disconnected");
                    return Ok(());
                }
            };
            println!("server: client register done, entering loop");
            loop {
                select! {
                    cmd = connection.next() => match cmd {
                        Some(Ok(Command::Subscribe(key))) => {
                            println!("server: client subscribe {key}");
                            let stashed = {
                                let mut state = state.lock().await;
                                state.association.insert(key, client_id);
                                let channel = state.channels.get(&client_id).cloned().unwrap();
                                // for waiting in state.pending_sources.remove(&key).unwrap_or_default() {
                                //     waiting.send(channel.clone()).unwrap();
                                // }
                                state.stash.remove(&key).unwrap_or_default()
                            };
                            for stashed in stashed {
                                connection.send(stashed).await?;
                            }
                        }
                        Some(Ok(cmd)) => {
                            eprintln!("Unexpected command: {cmd:?}");
                            // TODO: Cleanup
                            return Ok(());
                        }
                        Some(Err(e)) => {
                            eprintln!("Connection receiver error: {e:?}");
                            return Err(e);
                        }
                        None => {
                            eprintln!("connection closed");
                            // TODO: clean up
                            return Ok(());
                        }
                    },
                    response = client.rx.recv() => match response {
                        Some(response) => {
                            println!("server: client response");
                            connection.send(response).await?
                        },
                        None => {
                            eprintln!("channel closed");
                            // TODO: clean up
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    #[derive(Clone, Debug)]
    pub struct TcpEventLinkHandle {
        announce: UnboundedSender<(SourceId, oneshot::Sender<UnboundedSender<Response>>)>,
    }

    impl StorageCapture for TcpEventLinkHandle {
        fn capture<G: Scope<Timestamp = Timestamp>>(
            &mut self,
            id: SourceInstanceKey,
            ok: Collection<G, Row, Diff>,
            err: Collection<G, DataflowError, Diff>,
            token: Rc<dyn Any>,
            name: &str,
        ) {
            println!("capture {name}: about to announce");
            let (tx, rx) = oneshot::channel();
            self.announce.send((id.identifier, tx)).unwrap();
            println!("capture {name}: announce sent");
            let sender = rx.blocking_recv().unwrap();
            println!("capture {name}: announce got sender");

            let source_id = id.identifier;
            ok.inner.capture_into(UnboundedEventPusher {
                sender: sender.clone(),
                convert: move |event| Response::Data(source_id, event),
                token: Some(token),
            });
            err.inner.capture_into(UnboundedEventPusher {
                sender,
                convert: move |event| Response::Err(source_id, event),
                token: None,
            });
        }
    }

    struct UnboundedEventPusher<F> {
        sender: UnboundedSender<Response>,
        convert: F,
        token: Option<Rc<dyn Any>>,
    }

    impl<T, D, F: Fn(EventCore<T, D>) -> Response> EventPusherCore<T, D> for UnboundedEventPusher<F> {
        fn push(&mut self, event: EventCore<T, D>) {
            println!("pusher: push");
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
    ) {
        self.storage_handle.capture(id, ok, err, token, name)
    }
}

impl ComputeReplay for TcpBoundary {
    fn replay<G: Scope<Timestamp = Timestamp>>(
        &mut self,
        id: SourceInstanceKey,
        scope: &mut G,
        name: &str,
    ) -> (
        Collection<G, Row, Diff>,
        Collection<G, DataflowError, Diff>,
        Rc<dyn Any>,
    ) {
        self.compute_handle.replay(id, scope, name)
    }
}

pub mod client {
    use crate::activator::ArcActivator;
    use crate::replay::MzReplay;
    use crate::server::boundary::ComputeReplay;
    use crate::server::tcp_boundary::{framed_client, Command, Response, SourceId};
    use differential_dataflow::{AsCollection, Collection};
    use futures::{SinkExt, StreamExt};
    use mz_dataflow_types::{DataflowError, SourceInstanceKey};
    use mz_repr::{Diff, Row, Timestamp};
    use std::any::Any;
    use std::collections::{HashMap, VecDeque};
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

    pub struct TcpEventLinkClient {}
    #[derive(Debug, Clone)]
    pub struct TcpEventLinkClientHandle {
        announce_tx: UnboundedSender<Announce>,
    }

    impl TcpEventLinkClient {
        pub fn connect(addr: SocketAddr) -> (TcpEventLinkClientHandle, JoinHandle<()>) {
            let (announce_tx, announce_rx) = unbounded_channel();
            let thread = std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_io()
                    .enable_time()
                    .build()
                    .unwrap();

                runtime
                    .block_on(Self::connect_inner(&addr, announce_rx))
                    .unwrap();
            });

            (TcpEventLinkClientHandle { announce_tx }, thread)
        }
        async fn connect_inner(
            addr: &SocketAddr,
            mut announce_rx: UnboundedReceiver<Announce>,
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

            println!("client: next: register");
            client.send(Command::Register).await?;
            println!("client: register sent");

            let mut channels: HashMap<
                SourceId,
                (
                    VecDeque<(WorkerIdentifier, UnboundedSender<_>, Option<ArcActivator>)>,
                    VecDeque<(WorkerIdentifier, UnboundedSender<_>, Option<ArcActivator>)>,
                ),
            > = Default::default();

            loop {
                select! {
                    response = client.next() => match response {
                        Some(Ok(Response::Data(source_id, event))) => {
                            println!("client: data response {source_id}");
                            let channel_queue = &mut channels.get_mut(&source_id).unwrap().0;
                            let mut channel = channel_queue.pop_front().unwrap();
                            channel.1.send(event).unwrap();
                            if let Some(activator) = &mut channel.2 {
                                activator.activate();
                            }
                            channel_queue.push_back(channel);
                        }
                        Some(Ok(Response::Err(source_id, event))) => {
                            println!("client: err response {source_id}");
                            let channel_queue = &mut channels.get_mut(&source_id).unwrap().1;
                            let mut channel = channel_queue.pop_front().unwrap();
                            channel.1.send(event).unwrap();
                            if let Some(activator) = &mut channel.2 {
                                activator.activate();
                            }
                            channel_queue.push_back(channel);
                        }
                        Some(Err(e)) => {
                            println!("client: err {e:?}");
                            // TODO
                        }
                        None => {
                            println!("client: remote disconnected");
                            break;
                        }

                    },
                    announce = announce_rx.recv() => match announce {
                        Some(Announce::Register(source_id, worker, sender )) => {
                            println!("client: announce register {source_id} {worker}");
                            let (ok_tx, ok_rx) = unbounded_channel();
                            let (err_tx, err_rx) = unbounded_channel();
                            sender.send((ok_rx, err_rx)).unwrap();
                            let state = channels.entry(source_id).or_default();
                            state.0.push_back((worker, ok_tx, None));
                            state.1.push_back((worker, err_tx, None));

                            client.send(Command::Subscribe(source_id)).await?;
                        }
                        Some(Announce::Activate(source_id, worker, ok_activator, err_activator)) => {
                            println!("client: announce activate {source_id} {worker}");
                            let state = channels.entry(source_id).or_default();
                            {
                                let mut ok_activator = Some(ok_activator);
                                for (state_worker, _, activator) in &mut state.0 {
                                    if worker == *state_worker {
                                        *activator = ok_activator.take();
                                        break;
                                    }
                                }
                                assert!(ok_activator.is_none());
                            }
                            {
                                let mut err_activator = Some(err_activator);
                                for (state_worker, _, activator) in &mut state.0 {
                                    if worker == *state_worker {
                                        *activator = err_activator.take();
                                        break;
                                    }
                                }
                                assert!(err_activator.is_none());
                            }
                        }
                        None => {
                            println!("client: announce none");
                            // TODO
                        }
                    }
                }
            }

            println!("client: done");

            Ok(())
        }
    }

    #[derive(Debug)]
    enum Announce {
        Register(
            SourceId,
            WorkerIdentifier,
            oneshot::Sender<(
                UnboundedReceiver<EventCore<Timestamp, Vec<(Row, Timestamp, Diff)>>>,
                UnboundedReceiver<EventCore<Timestamp, Vec<(DataflowError, Timestamp, Diff)>>>,
            )>,
        ),
        Activate(SourceId, WorkerIdentifier, ArcActivator, ArcActivator),
    }

    impl ComputeReplay for TcpEventLinkClientHandle {
        fn replay<G: Scope<Timestamp = Timestamp>>(
            &mut self,
            id: SourceInstanceKey,
            scope: &mut G,
            name: &str,
        ) -> (
            Collection<G, Row, Diff>,
            Collection<G, DataflowError, Diff>,
            Rc<dyn Any>,
        ) {
            let (tx, rx) = oneshot::channel();
            println!("replay: about to register");
            self.announce_tx
                .send(Announce::Register(id.identifier, scope.index(), tx))
                .unwrap();
            println!("replay: register sent, waiting for channels");
            let (ok_rx, err_rx) = rx.blocking_recv().unwrap();
            println!("replay: got channels");

            let ok_activator = ArcActivator::new(format!("{name}-ok-activator"), 1);
            let ok = Some(UnboundedEventPuller::new(ok_rx))
                .mz_replay(
                    scope,
                    &format!("{name}-ok"),
                    Duration::MAX,
                    ok_activator.clone(),
                )
                .as_collection();
            let err_activator = ArcActivator::new(format!("{name}-ok-activator"), 1);
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
                    id.identifier,
                    scope.index(),
                    ok_activator,
                    err_activator,
                ))
                .unwrap();

            (ok, err, Rc::new(()))
        }
    }

    struct UnboundedEventPuller<T, D> {
        sender: UnboundedReceiver<EventCore<T, D>>,
        element: Option<EventCore<T, D>>,
    }

    impl<T, D> UnboundedEventPuller<T, D> {
        fn new(sender: UnboundedReceiver<EventCore<T, D>>) -> Self {
            Self {
                sender,
                element: None,
            }
        }
    }

    impl<T, D> EventIteratorCore<T, D> for UnboundedEventPuller<T, D> {
        fn next(&mut self) -> Option<&EventCore<T, D>> {
            // println!("puller: next");
            match self.sender.try_recv() {
                Ok(element) => {
                    println!("puller: next -> OK");
                    self.element = Some(element);
                    self.element.as_ref()
                }
                Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => {
                    // println!("puller: next -> Err");
                    self.element.take();
                    None
                }
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Command {
    Register,
    Subscribe(SourceId),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Response {
    Data(
        SourceId,
        EventCore<mz_repr::Timestamp, Vec<(mz_repr::Row, mz_repr::Timestamp, mz_repr::Diff)>>,
    ),
    Err(
        SourceId,
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
