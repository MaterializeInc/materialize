// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Methods common to servers listening for TCP connections.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;

use async_trait::async_trait;
use futures::stream::{Stream, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tracing::error;

use mz_ore::task;

/// A future that handles a connection.
pub type ConnectionHandler = Pin<Box<dyn Future<Output = Result<(), anyhow::Error>> + Send>>;

/// A server handles incoming network connections.
pub trait Server {
    /// Returns the name of the connection handler for use in e.g. log messages.
    const NAME: &'static str;

    /// Handles a single connection.
    fn handle_connection(&self, conn: TcpStream) -> ConnectionHandler;
}

/// A stream of incoming connections.
pub trait ConnectionStream: Stream<Item = io::Result<TcpStream>> + Unpin + Send {}

impl<T> ConnectionStream for T where T: Stream<Item = io::Result<TcpStream>> + Unpin + Send {}

/// A handle to a listener created by [`listen`].
pub struct ListenerHandle {
    local_addr: SocketAddr,
    _trigger: oneshot::Sender<()>,
}

impl ListenerHandle {
    /// Returns the local address to which the listener is bound.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

/// Listens for incoming TCP connections on the specified address.
///
/// Returns a handle to the listener and the stream of incoming connections
/// produced by the listener. When the handle is dropped, the listener is
/// closed, and the stream of incoming connections terminates.
pub async fn listen(
    addr: SocketAddr,
) -> Result<(ListenerHandle, impl ConnectionStream), io::Error> {
    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    let (trigger, tripwire) = oneshot::channel();
    let handle = ListenerHandle {
        local_addr,
        _trigger: trigger,
    };
    // TODO(benesch): replace `TCPListenerStream`s with `listener.incoming()` if
    // that is restored when the `Stream` trait stabilizes.
    let stream = TcpListenerStream::new(listener).take_until(tripwire);
    Ok((handle, stream))
}

/// Serves incoming TCP connections from `conns` using `server`.
pub async fn serve<C, S>(mut conns: C, server: S)
where
    C: ConnectionStream,
    S: Server,
{
    let task_name = format!("handle_{}_connection", S::NAME);
    while let Some(conn) = conns.next().await {
        let conn = match conn {
            Ok(conn) => conn,
            Err(err) => {
                error!("error accepting connection: {}", err);
                continue;
            }
        };
        // Set TCP_NODELAY to disable tinygram prevention (Nagle's
        // algorithm), which forces a 40ms delay between each query
        // on linux. According to John Nagle [0], the true problem
        // is delayed acks, but disabling those is a receive-side
        // operation (TCP_QUICKACK), and we can't always control the
        // client. PostgreSQL sets TCP_NODELAY on both sides of its
        // sockets, so it seems sane to just do the same.
        //
        // If set_nodelay fails, it's a programming error, so panic.
        //
        // [0]: https://news.ycombinator.com/item?id=10608356
        conn.set_nodelay(true).expect("set_nodelay failed");
        let fut = server.handle_connection(conn);
        task::spawn(|| &task_name, async {
            if let Err(e) = fut.await {
                error!("error handling connection in {}: {:#}", S::NAME, e);
            }
        });
    }
}

#[async_trait]
impl Server for mz_pgwire::Server {
    const NAME: &'static str = "pgwire";

    fn handle_connection(&self, conn: TcpStream) -> ConnectionHandler {
        // Using fully-qualified syntax means we won't accidentally call
        // ourselves (i.e., silently infinitely recurse) if the name or type of
        // `mz_pgwire::Server::handle_connection` changes.
        Box::pin(mz_pgwire::Server::handle_connection(self, conn))
    }
}
