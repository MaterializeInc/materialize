// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Traffic routing.

use std::cmp::Ordering;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::{self, Future, FutureExt, TryFutureExt};
use futures::stream::{FuturesOrdered, StreamExt, TryStreamExt};
use log::error;
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::io;
use tokio::net::{UnixListener, UnixStream};
use tokio::runtime::Runtime;
use uuid::Uuid;

use crate::broadcast;
use crate::mpsc;
use crate::protocol;
use crate::router;
use crate::util::TryConnectFuture;

/// Router for incoming and outgoing communication traffic.
///
/// A switchboard is responsible for allocating channels and, within this
/// process, routing incoming traffic to the appropriate channel receiver, which
/// may be located on any thread. Outbound traffic does not presently involve
/// the switchboard.
///
/// The membership of the cluster (i.e., the addresses of every node in the
/// cluster) must be known at the time of switchboard creation. It is not
/// possible to add or remove peers once a switchboard has been constructed.
///
/// Switchboards are both [`Send`] and [`Sync`], and so may be freely shared
/// and sent between threads.
pub struct Switchboard<C>(Arc<SwitchboardInner<C>>)
where
    C: protocol::Connection;

impl<C> Clone for Switchboard<C>
where
    C: protocol::Connection,
{
    fn clone(&self) -> Switchboard<C> {
        Switchboard(self.0.clone())
    }
}

struct SwitchboardInner<C>
where
    C: protocol::Connection,
{
    /// Addresses of all the nodes in the cluster, including of this node.
    nodes: Vec<C::Addr>,
    /// The index of this node's address in `nodes`.
    id: usize,
    /// Routing for channel traffic.
    channel_table: Mutex<router::RoutingTable<Uuid, protocol::Framed<C>>>,
    /// Routing for rendezvous traffic.
    rendezvous_table: Mutex<router::RoutingTable<u64, C>>,
    /// Task executor, so that background work can be spawned.
    executor: tokio::runtime::Handle,
}

impl Switchboard<UnixStream> {
    /// Constructs a new `Switchboard` for a single-process cluster. A Tokio
    /// [`Runtime`] that manages traffic for the switchboard is also returned;
    /// this runtime must live at least as long as the switchboard for correct
    /// operation.
    ///
    /// This function is intended for test and example programs. Production code
    /// will likely want to configure its own Tokio runtime and handle its own
    /// network binding.
    pub fn local() -> Result<(Switchboard<UnixStream>, Runtime), io::Error> {
        let mut rng = rand::thread_rng();
        let suffix: String = (0..6)
            .map(|_| rng.sample(rand::distributions::Alphanumeric))
            .collect();
        let mut path = std::env::temp_dir();
        path.push(format!("comm.switchboard.{}", suffix));
        let runtime = Runtime::new()?;
        let mut listener = runtime.enter(|| UnixListener::bind(&path))?;
        let switchboard =
            Switchboard::new(vec![path.to_str().unwrap()], 0, runtime.handle().clone());
        runtime.spawn({
            let switchboard = switchboard.clone();
            async move {
                let mut incoming = listener.incoming();
                while let Some(conn) = incoming.next().await {
                    let conn =
                        conn.unwrap_or_else(|err| panic!("local switchboard: accept: {}", err));
                    switchboard
                        .handle_connection(conn)
                        .await
                        .unwrap_or_else(|err| {
                            error!("local switchboard: handle connection: {}", err)
                        });
                }
            }
        });
        Ok((switchboard, runtime))
    }
}

impl<C> Switchboard<C>
where
    C: protocol::Connection,
{
    /// Constructs a new `Switchboard`. The addresses of all nodes in the
    /// cluster, including the address for this node, must be provided in
    /// `nodes`, and the index of this node's address in the list must be
    /// specified as `id`.
    ///
    /// The consumer of a `Switchboard` must separately arrange to listen on the
    /// local node's address and route `comm` traffic to this `Switchboard`
    /// via [`Switchboard::handle_connection`].
    pub fn new<I>(nodes: I, id: usize, executor: tokio::runtime::Handle) -> Switchboard<C>
    where
        I: IntoIterator,
        I::Item: Into<C::Addr>,
    {
        Switchboard(Arc::new(SwitchboardInner {
            nodes: nodes.into_iter().map(Into::into).collect(),
            id,
            channel_table: Mutex::default(),
            rendezvous_table: Mutex::default(),
            executor,
        }))
    }

    /// Waits for all nodes to become available. Returns a vector of connections
    /// to each node in the order that the addresses were provided to
    /// [`Switchboard::new`]. Note that the stream for the current node will be
    /// `None`, while all other nodes will be `Some`.
    ///
    /// Attempting to send on channels before a successful rendezvous may fail,
    /// as other nodes in the cluster may not have yet started listening on
    /// their declared port. Rendezvous may be skipped if another external means
    /// of synchronizing switchboard startup is used.
    ///
    /// Rendezvous will listen for connections from nodes before this node in
    /// the address list, while it will attempt connections for nodes after this
    /// node. It is therefore critical that addresses be provided in the same
    /// order across all processes in the cluster.
    pub async fn rendezvous(
        &self,
        timeout: impl Into<Option<Duration>>,
    ) -> Result<Vec<Option<C>>, io::Error> {
        let timeout = timeout.into();
        let mut futures = FuturesOrdered::<
            Pin<Box<dyn Future<Output = Result<Option<C>, io::Error>> + Send>>,
        >::new();
        for (i, addr) in self.0.nodes.iter().enumerate() {
            match i.cmp(&self.0.id) {
                // Earlier node. Wait for it to connect to us.
                Ordering::Less => futures.push(Box::pin(
                    self.0
                        .rendezvous_table
                        .lock()
                        .expect("lock poisoned")
                        .add_dest(i as u64)
                        .into_future()
                        .map(|(conn, _stream)| Ok(conn)),
                )),

                // Ourselves. Nothing to do.
                Ordering::Equal => futures.push(Box::pin(future::ok(None))),

                // Later node. Attempt to initiate connection.
                Ordering::Greater => {
                    let id = self.0.id as u64;
                    futures.push(Box::pin(
                        TryConnectFuture::new(addr.clone(), timeout)
                            .and_then(move |conn| protocol::send_rendezvous_handshake(conn, id))
                            .map_ok(|conn| Some(conn)),
                    ));
                }
            }
        }
        futures.try_collect().await
    }

    /// Routes an incoming connection to the appropriate channel receiver. This
    /// function assumes that the connection is using the `comm` protocol,
    /// either because the protocol has been sniffed with
    /// [`protocol::match_handshake`], or because the connection is from a
    /// dedicated port that does not serve traffic from other protocols.
    ///
    /// # Examples
    /// Basic usage:
    /// ```
    /// use comm::{Connection, Switchboard};
    /// use tokio::io::{self, AsyncReadExt};
    /// #
    /// # async fn handle_other_protocol<C: Connection>(buf: &[u8], conn: C) -> Result<(), io::Error> {
    /// #     Ok(())
    /// # }
    ///
    /// async fn handle_connection<C>(
    ///     switchboard: Switchboard<C>,
    ///     mut conn: C
    /// ) -> Result<(), io::Error>
    /// where
    ///     C: Connection,
    /// {
    ///     let mut buf = [0; 8];
    ///     conn.read_exact(&mut buf).await?;
    ///     if comm::protocol::match_handshake(&buf) {
    ///         switchboard.handle_connection(conn).await
    ///     } else {
    ///         handle_other_protocol(&buf, conn).await
    ///     }
    /// }
    /// ```
    pub fn handle_connection(&self, conn: C) -> impl Future<Output = Result<(), io::Error>> {
        let inner = self.0.clone();
        protocol::recv_handshake(conn).map_ok(move |conn| inner.route_connection(conn))
    }

    /// Attempts to recycle an incoming channel connection for use with a new
    /// channel. The connection is expected to be in a state where all messages
    /// from the previous channel have been drained, and a new channel will
    /// be initialized via an abbreviated channel handshake, rather than the
    /// full protocol handshake.
    pub(crate) fn recycle_connection(
        &self,
        conn: protocol::Framed<C>,
    ) -> impl Future<Output = Result<(), io::Error>> {
        let inner = self.0.clone();
        protocol::recv_channel_handshake(conn).map_ok(move |conn| inner.route_connection(conn))
    }

    /// Returns a reference to the [`tokio::runtime::Handle`] that this
    /// `Switchboard` was initialized with. Useful for spawning background work.
    pub fn executor(&self) -> &tokio::runtime::Handle {
        &self.0.executor
    }

    /// Allocates a transmitter for the broadcast channel identified by `token`.
    pub fn broadcast_tx<T>(&self, token: T) -> broadcast::Sender<T::Item>
    where
        T: broadcast::Token + 'static,
    {
        let uuid = token.uuid();
        if token.loopback() {
            broadcast::Sender::new::<C, _>(uuid, self.0.nodes.iter().cloned())
        } else {
            broadcast::Sender::new::<C, _>(uuid, self.peers().cloned())
        }
    }

    /// Allocates a receiver for the broadcast channel identified by `token`.
    ///
    /// # Panics
    ///
    /// Panics if this switchboard has already allocated a broadcast receiver
    /// for `token`.
    pub fn broadcast_rx<T>(&self, token: T) -> broadcast::Receiver<T::Item>
    where
        T: broadcast::Token + 'static,
    {
        let uuid = token.uuid();
        broadcast::Receiver::new(self.new_rx(uuid), self.clone())
    }

    /// Allocates a new multiple-producer, single-consumer (MPSC) channel and
    /// returns both a transmitter and receiver. The transmitter can be cloned
    /// and serialized, so it can be shared with other threads or processes. The
    /// receiver cannot be cloned or serialized, but it can be sent to other
    /// threads in the same process.
    pub fn mpsc<D>(&self) -> (mpsc::Sender<D>, mpsc::Receiver<D>)
    where
        D: Serialize + for<'de> Deserialize<'de> + Send + Unpin + 'static,
    {
        self.mpsc_limited(usize::max_value())
    }

    /// Like [`Switchboard::mpsc`], but limits the number of producers to
    /// `max_producers`. Once `max_producers` have connected, no additional
    /// producers will be permitted to connect; once each permitted producer has
    /// disconnected, the receiver will be closed.
    pub fn mpsc_limited<D>(&self, max_producers: usize) -> (mpsc::Sender<D>, mpsc::Receiver<D>)
    where
        D: Serialize + for<'de> Deserialize<'de> + Send + Unpin + 'static,
    {
        let uuid = Uuid::new_v4();
        let addr = self.0.nodes[self.0.id].clone();
        let tx = mpsc::Sender::new(addr, uuid);
        let sb = self.clone();
        let rx = mpsc::Receiver::new(
            self.new_rx(uuid).take(max_producers),
            self.clone(),
            Some(Box::new(move || {
                sb.0.channel_table
                    .lock()
                    .expect("lock poisoned")
                    .remove_dest(uuid)
            })),
        );
        (tx, rx)
    }

    /// Reports the size of (i.e., the number of nodes in) the cluster that this
    /// switchboard is managing.
    pub fn size(&self) -> usize {
        self.0.nodes.len()
    }

    fn new_rx(&self, uuid: Uuid) -> futures::channel::mpsc::UnboundedReceiver<protocol::Framed<C>> {
        let mut channel_table = self.0.channel_table.lock().expect("lock poisoned");
        channel_table.add_dest(uuid)
    }

    fn peers(&self) -> impl Iterator<Item = &C::Addr> {
        let id = self.0.id;
        self.0
            .nodes
            .iter()
            .enumerate()
            .filter_map(move |(i, addr)| if i == id { None } else { Some(addr) })
    }
}

impl<C> SwitchboardInner<C>
where
    C: protocol::Connection,
{
    fn route_connection(&self, handshake: protocol::RecvHandshake<C>) {
        match handshake {
            protocol::RecvHandshake::Channel(uuid, conn) => {
                let mut router = self.channel_table.lock().expect("lock poisoned");
                router.route(uuid, conn);
            }
            protocol::RecvHandshake::Rendezvous(id, conn) => {
                let mut router = self.rendezvous_table.lock().expect("lock poisoned");
                router.route(id, conn);
            }
        }
    }
}
