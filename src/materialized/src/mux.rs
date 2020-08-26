// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use futures::future::TryFutureExt;
use futures::stream::{Stream, StreamExt};
use log::error;
use tokio::io::{self, AsyncWriteExt};
use tokio::net::TcpStream;

use ore::netio::{self, SniffedStream, SniffingStream};

use crate::http;

type Handlers = Vec<Box<dyn ConnectionHandler + Send + Sync>>;

/// A mux routes incoming TCP connections to a dynamic set of connection
/// handlers. It enables serving multiple protocols over the same port.
///
/// Connections are routed by sniffing the first several bytes sent over the
/// wire and matching them against each handler, in order. The first handler
/// to match the connection will be invoked.
pub struct Mux {
    handlers: Handlers,
}

impl Mux {
    /// Constructs a new `Mux`.
    pub fn new() -> Mux {
        Mux { handlers: vec![] }
    }

    /// Adds a new connection handler to this mux.
    pub fn add_handler<H>(&mut self, handler: H)
    where
        H: ConnectionHandler + Send + Sync + 'static,
    {
        self.handlers.push(Box::new(handler));
    }

    /// Serves incoming TCP traffic from `listener`.
    pub async fn serve<S>(self, mut incoming: S)
    where
        S: Stream<Item = io::Result<TcpStream>> + Unpin,
    {
        let handlers = Arc::new(self.handlers);
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
            tokio::spawn(handle_connection(handlers.clone(), conn));
        }
    }
}

async fn handle_connection(handlers: Arc<Handlers>, conn: TcpStream) {
    // Sniff out what protocol we've received. Choosing how many bytes to
    // sniff is a delicate business. Read too many bytes and you'll stall
    // out protocols with small handshakes, like pgwire. Read too few bytes
    // and you won't be able to tell what protocol you have. For now, eight
    // bytes is the magic number, but this may need to change if we learn to
    // speak new protocols.
    let mut ss = SniffingStream::new(conn);
    let mut buf = [0; 8];
    let nread = match netio::read_exact_or_eof(&mut ss, &mut buf).await {
        Ok(nread) => nread,
        Err(err) => {
            error!("error handling request: {}", err);
            return;
        }
    };
    let buf = &buf[..nread];

    for handler in &*handlers {
        if handler.match_handshake(buf) {
            if let Err(e) = handler.handle_connection(ss.into_sniffed()).await {
                error!("error handling connection: {}", e);
            }
            return;
        }
    }

    log::warn!("unknown protocol connection!");
    let _ = ss.into_sniffed().write_all(b"unknown protocol\n").await;
}

/// A connection handler manages an incoming network connection.
#[async_trait]
pub trait ConnectionHandler {
    /// Determines whether this handler can accept the connection based on the
    /// first several bytes in the stream.
    fn match_handshake(&self, buf: &[u8]) -> bool;

    /// Handles the connection.
    async fn handle_connection(&self, conn: SniffedStream<TcpStream>) -> Result<(), anyhow::Error>;
}

#[async_trait]
impl ConnectionHandler for pgwire::Server {
    fn match_handshake(&self, buf: &[u8]) -> bool {
        pgwire::match_handshake(buf)
    }

    async fn handle_connection(&self, conn: SniffedStream<TcpStream>) -> Result<(), anyhow::Error> {
        self.handle_connection(conn)
            .await
            .context("in pgwire server")
    }
}

#[async_trait]
impl ConnectionHandler for http::Server {
    fn match_handshake(&self, buf: &[u8]) -> bool {
        self.match_handshake(buf)
    }

    async fn handle_connection(&self, conn: SniffedStream<TcpStream>) -> Result<(), anyhow::Error> {
        self.handle_connection(conn).await.context("in http server")
    }
}

#[async_trait]
impl ConnectionHandler for comm::Switchboard<SniffedStream<TcpStream>> {
    fn match_handshake(&self, buf: &[u8]) -> bool {
        comm::protocol::match_handshake(buf)
    }

    async fn handle_connection(&self, conn: SniffedStream<TcpStream>) -> Result<(), anyhow::Error> {
        self.handle_connection(conn).err_into().await
    }
}
