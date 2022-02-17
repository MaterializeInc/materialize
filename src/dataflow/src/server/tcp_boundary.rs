// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

use crate::activator::RcActivator;
use crate::replay::MzReplay;
use crate::server::boundary::{ComputeReplay, StorageCapture};
use differential_dataflow::{AsCollection, Collection};
use futures::stream::TryStreamExt;
use futures::SinkExt;
use mz_dataflow_types::{DataflowError, SourceInstanceKey};
use mz_expr::GlobalId;
use mz_repr::{Diff, Row, Timestamp};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::rc::Rc;
use std::thread::JoinHandle;
use std::time::Duration;
use timely::dataflow::operators::capture::{EventCore, EventPusherCore};
use timely::dataflow::operators::generic::source;
use timely::dataflow::operators::Capture;
use timely::dataflow::Scope;
use timely::logging::WorkerIdentifier;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::mpsc::UnboundedSender;
use tokio_serde::formats::Bincode;
use tokio_util::codec::LengthDelimitedCodec;

#[derive(Debug)]
pub struct TcpEventLinkServer {
    handle: TcpEventLinkHandle,
    thread: JoinHandle<()>,
}

pub type SourceId = GlobalId;

#[derive(Debug, Default)]
struct ConnectionState {
    connections: VecDeque<FramedServer<TcpStream>>,
}

impl TcpEventLinkServer {
    pub fn new() -> Self {
        let (ok_sender, mut ok_receiver) = tokio::sync::mpsc::unbounded_channel();
        let (err_sender, mut err_receiver) = tokio::sync::mpsc::unbounded_channel();

        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            runtime.block_on(async {
                let listener = TcpListener::bind("127.0.0.1:45678").await.unwrap();

                let mut conn_state: BTreeMap<SourceId, ConnectionState> = BTreeMap::new();
                let mut ok_stash : HashMap<_, Vec<_>> = HashMap::new();
                let mut err_stash: HashMap<_, Vec<_>> = HashMap::new();

                loop {
                    select! {
                        incoming = listener.accept() => match incoming {
                            Ok((socket, _)) => {
                                if let Some((source_id, mut conn)) = Self::register_client(socket).await {
                                    if let Some(events) = ok_stash.remove(&source_id) {
                                        for event in events {
                                            conn.send(Response::Data(event)).await.unwrap();
                                        }
                                    }
                                    if let Some(events) = err_stash.remove(&source_id) {
                                        for event in events {
                                            conn.send(Response::Err(event)).await.unwrap();
                                        }
                                    }
                                    let state = conn_state.entry(source_id).or_default();
                                    state.connections.push_back(conn);
                                }
                            }
                            Err(e) => {
                                eprintln!("listening error: {e}");
                                break
                            }
                        },
                        ok = ok_receiver.recv() => match ok {
                            Some((source_id, _worker, event)) => {
                                let state = conn_state.entry(source_id).or_default();
                                if let Some(mut conn) = state.connections.pop_front() {
                                    conn.send(Response::Data(event)).await.unwrap();
                                    state.connections.push_back(conn);
                                } else {
                                    ok_stash.entry(source_id).or_default().push(event);
                                }
                            }
                            None => break
                        },
                        err = err_receiver.recv() => match err {
                            Some((source_id, _worker, event)) => {
                                let state = conn_state.entry(source_id).or_default();
                                if let Some(mut conn) = state.connections.pop_front() {
                                    conn.send(Response::Err(event)).await.unwrap();
                                    state.connections.push_back(conn);
                                } else {
                                    err_stash.entry(source_id).or_default().push(event);
                                }
                            }
                            None => break
                        }
                    }
                }
            });
        });

        Self {
            handle: TcpEventLinkHandle {
                ok_sender,
                err_sender,
            },
            thread,
        }
    }

    async fn register_client(socket: TcpStream) -> Option<(SourceId, FramedServer<TcpStream>)> {
        let mut conn = framed_server(socket);
        let cmd = conn.try_next().await.ok()?;
        if let Some(Command::Register(key, worker)) = cmd {
            Some((key, conn))
        } else {
            None
        }
    }

    pub fn handle(&self) -> TcpEventLinkHandle {
        self.handle.clone()
    }
}

#[derive(Clone, Debug)]
struct TcpEventLinkHandle {
    ok_sender: UnboundedSender<(
        SourceId,
        WorkerIdentifier,
        EventCore<mz_repr::Timestamp, Vec<(mz_repr::Row, mz_repr::Timestamp, mz_repr::Diff)>>,
    )>,
    err_sender: UnboundedSender<(
        SourceId,
        WorkerIdentifier,
        EventCore<mz_repr::Timestamp, Vec<(DataflowError, mz_repr::Timestamp, mz_repr::Diff)>>,
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
    ) {
        let index = ok.inner.scope().index();
        let source_id = id.identifier;
        ok.inner.capture_into(UnboundedEventPusher {
            index,
            source_id,
            sender: self.ok_sender.clone(),
        });
        err.inner.capture_into(UnboundedEventPusher {
            index,
            source_id,
            sender: self.err_sender.clone(),
        });
    }
}

impl ComputeReplay for TcpEventLinkHandle {
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
        let socket = TcpStream::connect("127.0.0.1:45678").unwrap();

        let conn = framed_client(socket);
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            runtime.block_on(async {});
        });

        let activator = RcActivator::new("blubb".to_owned(), 1);
        let ok = Some(source.ok.inner)
            .mz_replay(
                scope,
                &format!("{name}-ok"),
                Duration::from_millis(10),
                activator.clone(),
            )
            .as_collection();
        let err = Some(source.err.inner)
            .mz_replay(
                scope,
                &format!("{name}-err"),
                Duration::from_millis(10),
                activator,
            )
            .as_collection();

        (ok, err, source.token)
    }
}

struct UnboundedEventPusher<T, D> {
    sender: UnboundedSender<(SourceId, WorkerIdentifier, EventCore<T, D>)>,
    source_id: SourceId,
    index: WorkerIdentifier,
}

impl<T, D> EventPusherCore<T, D> for UnboundedEventPusher<T, D> {
    fn push(&mut self, event: EventCore<T, D>) {
        self.sender
            .send((self.source_id, self.index, event))
            .ok()
            .unwrap();
    }
}

struct TcpEventLinkClient {}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Command {
    Register(SourceId, WorkerIdentifier),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Response {
    Data(EventCore<mz_repr::Timestamp, Vec<(mz_repr::Row, mz_repr::Timestamp, mz_repr::Diff)>>),
    Err(EventCore<mz_repr::Timestamp, Vec<(DataflowError, mz_repr::Timestamp, mz_repr::Diff)>>),
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
