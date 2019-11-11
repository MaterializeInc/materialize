// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Traffic routing.

use futures::stream::FuturesOrdered;
use futures::{future, Future, Stream};
use log::error;
use ore::future::StreamExt;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io;
use tokio::net::unix::{UnixListener, UnixStream};
use tokio::runtime::{Runtime, TaskExecutor};
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
    executor: TaskExecutor,
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
        let listener = UnixListener::bind(&path)?;
        let mut runtime = Runtime::new()?;
        let switchboard = Switchboard::new(vec![path.to_str().unwrap()], 0, runtime.executor());
        runtime.spawn({
            let switchboard = switchboard.clone();
            listener
                .incoming()
                .map_err(|err| panic!("local switchboard: accept: {}", err))
                .for_each(move |conn| switchboard.handle_connection(conn))
                .map_err(|err| error!("local switchboard: handle connection: {}", err))
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
    pub fn new<I>(nodes: I, id: usize, executor: TaskExecutor) -> Switchboard<C>
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
    pub fn rendezvous(
        &self,
        timeout: impl Into<Option<Duration>>,
    ) -> impl Future<Item = Vec<Option<C>>, Error = io::Error> {
        let timeout = timeout.into();
        let mut futures =
            FuturesOrdered::<Box<dyn Future<Item = Option<C>, Error = io::Error> + Send>>::new();
        for (i, addr) in self.0.nodes.iter().enumerate() {
            if i < self.0.id {
                // Earlier node. Wait for it to connect to us.
                futures.push(Box::new(
                    self.0
                        .rendezvous_table
                        .lock()
                        .expect("lock poisoned")
                        .add_dest(i as u64)
                        .map_err(|()| unreachable!())
                        .recv()
                        .map(|(conn, _stream)| Some(conn)),
                ));
            } else if i == self.0.id {
                // Ourselves. Nothing to do.
                futures.push(Box::new(future::ok(None)));
            } else {
                // Later node. Attempt to initiate connection.
                let id = self.0.id as u64;
                futures.push(Box::new(
                    TryConnectFuture::new(addr.clone(), timeout)
                        .and_then(move |conn| protocol::send_rendezvous_handshake(conn, id))
                        .map(|conn| Some(conn)),
                ));
            }
        }
        futures.collect()
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
    /// use futures::Future;
    /// use futures::future::Either;
    /// use tokio::io;
    /// #
    /// # fn handle_other_protocol<C: Connection>(buf: &[u8], conn: C) -> impl Future<Item = (), Error = io::Error> {
    /// #     futures::future::ok(())
    /// # }
    ///
    /// fn handle_connection<C>(
    ///     switchboard: Switchboard<C>,
    ///     conn: C
    /// ) -> impl Future<Item = (), Error = io::Error>
    /// where
    ///     C: Connection,
    /// {
    ///     io::read_exact(conn, [0; 8]).and_then(move |(conn, buf)| {
    ///         if comm::protocol::match_handshake(&buf) {
    ///             Either::A(switchboard.handle_connection(conn))
    ///         } else {
    ///             Either::B(handle_other_protocol(&buf, conn))
    ///         }
    ///     })
    /// }
    /// ```
    pub fn handle_connection(&self, conn: C) -> impl Future<Item = (), Error = io::Error> {
        let inner = self.0.clone();
        protocol::recv_handshake(conn).map(move |conn| inner.route_connection(conn))
    }

    /// Attempts to recycle an incoming channel connection for use with a new
    /// channel. The connection is expected to be in a state where all messages
    /// from the previous channel have been drained, and a new channel will
    /// be initialized via an abbreviated channel handshake, rather than the
    /// full protocol handshake.
    pub(crate) fn recycle_connection(
        &self,
        conn: protocol::Framed<C>,
    ) -> impl Future<Item = (), Error = io::Error> {
        let inner = self.0.clone();
        protocol::recv_channel_handshake(conn).map(move |conn| inner.route_connection(conn))
    }

    /// Returns a reference to the [`TaskExecutor`] that this `Switchboard`
    /// was initialized with. Useful for spawning background work.
    pub fn executor(&self) -> &TaskExecutor {
        &self.0.executor
    }

    /// Allocates a transmitter for the broadcast channel identified by `token`.
    pub fn broadcast_tx<T>(&self, token: T) -> broadcast::Sender<T::Item>
    where
        T: broadcast::Token + 'static,
    {
        let uuid = token.uuid();
        if token.loopback() {
            broadcast::Sender::new::<C, _>(uuid, self.0.nodes.iter())
        } else {
            broadcast::Sender::new::<C, _>(uuid, self.peers())
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
        D: Serialize + for<'de> Deserialize<'de> + Send + 'static,
    {
        self.mpsc_limited(u64::max_value())
    }

    /// Like [`Switchboard::mpsc`], but limits the number of producers to
    /// `max_producers`. Once `max_producers` have connected, no additional
    /// producers will be permitted to connect; once each permitted producer has
    /// disconnected, the receiver will be closed.
    pub fn mpsc_limited<D>(&self, max_producers: u64) -> (mpsc::Sender<D>, mpsc::Receiver<D>)
    where
        D: Serialize + for<'de> Deserialize<'de> + Send + 'static,
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

    fn new_rx(&self, uuid: Uuid) -> futures::sync::mpsc::UnboundedReceiver<protocol::Framed<C>> {
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
