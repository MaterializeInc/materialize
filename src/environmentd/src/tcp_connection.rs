// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Methods common to servers listening for TCP connections.

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::{Stream, StreamExt};
use tokio::io::{self};
use tokio::net::TcpStream;
use tracing::error;

use mz_ore::task;

use crate::http;

async fn handle_connection<S: ConnectionHandler>(server_ref: Arc<S>, conn: TcpStream) {
    if let Err(e) = server_ref.handle_connection(conn).await {
        error!(
            "error handling connection in {}: {:#}",
            server_ref.name(),
            e
        );
    }
}

/// A connection handler manages an incoming network connection.
#[async_trait]
pub trait ConnectionHandler {
    /// Returns the name of the connection handler for use in e.g. log messages.
    fn name(&self) -> &str;

    /// Serves incoming TCP traffic from `listener`.
    async fn serve<S>(self, mut incoming: S)
    where
        S: Stream<Item = io::Result<TcpStream>> + Unpin + Send,
        Self: Sized + Sync + 'static,
    {
        let self_ref = Arc::new(self);
        while let Some(conn) = incoming.next().await {
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
            task::spawn(
                || "mux_serve",
                handle_connection(Arc::clone(&self_ref), conn),
            );
        }
    }

    /// Handles the connection.
    async fn handle_connection(&self, conn: TcpStream) -> Result<(), anyhow::Error>;
}

#[async_trait]
impl ConnectionHandler for mz_pgwire::Server {
    fn name(&self) -> &str {
        "pgwire server"
    }

    async fn handle_connection(&self, conn: TcpStream) -> Result<(), anyhow::Error> {
        // Using fully-qualified syntax means we won't accidentally call
        // ourselves (i.e., silently infinitely recurse) if the name or type of
        // `mz_pgwire::Server::handle_connection` changes.
        mz_pgwire::Server::handle_connection(self, conn).await
    }
}

#[async_trait]
impl ConnectionHandler for http::Server {
    fn name(&self) -> &str {
        "http server"
    }

    async fn handle_connection(&self, conn: TcpStream) -> Result<(), anyhow::Error> {
        // Using fully-qualified syntax means we won't accidentally call
        // ourselves (i.e., silently infinitely recurse) if the name or type of
        // `http::Server::handle_connection` changes.
        http::Server::handle_connection(self, conn).await
    }
}
