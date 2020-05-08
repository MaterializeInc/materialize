// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;

use async_trait::async_trait;
use futures::future::TryFutureExt;
use log::error;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use ore::netio::{self, SniffedStream, SniffingStream};

use crate::http;

/// A mux routes incoming network connections to a dynamic set of connection
/// handlers. It enables serving multiple protocols over the same port.
///
/// Connections are routed by sniffing the first several bytes sent over the
/// wire and matching them against each handler, in order. The first handler
/// to match the connection will be invoked.
pub struct Mux<A> {
    handlers: Vec<Box<dyn ConnectionHandler<A> + Send + Sync>>,
}

impl<A> Mux<A> {
    /// Constructs a new `Mux`.
    pub fn new() -> Mux<A> {
        Mux { handlers: vec![] }
    }

    /// Adds a new connection handler to this mux.
    pub fn add_handler<H>(&mut self, handler: H)
    where
        H: ConnectionHandler<A> + Send + Sync + 'static,
    {
        self.handlers.push(Box::new(handler));
    }

    /// Handles a connection by routing it to the first underlying handler for
    /// which [`ConnectionHandler::match_handshake`] returns true.
    pub async fn handle_connection(&self, conn: A)
    where
        A: AsyncRead + AsyncWrite + Unpin + fmt::Debug + Send + Sync + 'static,
    {
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

        for handler in &self.handlers {
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
}

/// A connection handler manages an incoming network connection.
#[async_trait]
pub trait ConnectionHandler<A> {
    /// Determines whether this handler can accept the connection based on the
    /// first several bytes in the stream.
    fn match_handshake(&self, buf: &[u8]) -> bool;

    /// Handles the connection.
    async fn handle_connection(&self, conn: SniffedStream<A>) -> Result<(), failure::Error>
    where
        A: AsyncRead + AsyncWrite + Unpin + fmt::Debug + Send + Sync + 'static;
}

#[async_trait]
impl<A> ConnectionHandler<A> for pgwire::Server {
    fn match_handshake(&self, buf: &[u8]) -> bool {
        pgwire::match_handshake(buf)
    }

    async fn handle_connection(&self, conn: SniffedStream<A>) -> Result<(), failure::Error>
    where
        A: AsyncRead + AsyncWrite + Unpin + fmt::Debug + Send + Sync + 'static,
    {
        self.handle_connection(conn).await
    }
}

#[async_trait]
impl<A> ConnectionHandler<A> for http::Server {
    fn match_handshake(&self, buf: &[u8]) -> bool {
        self.match_handshake(buf)
    }

    async fn handle_connection(&self, conn: SniffedStream<A>) -> Result<(), failure::Error>
    where
        A: AsyncRead + AsyncWrite + Unpin + fmt::Debug + Send + Sync + 'static,
    {
        self.handle_connection(conn).await
    }
}

#[async_trait]
impl<A> ConnectionHandler<A> for comm::Switchboard<SniffedStream<A>>
where
    A: comm::protocol::Connection,
{
    fn match_handshake(&self, buf: &[u8]) -> bool {
        comm::protocol::match_handshake(buf)
    }

    async fn handle_connection(&self, conn: SniffedStream<A>) -> Result<(), failure::Error>
    where
        A: AsyncRead + AsyncWrite + Unpin + fmt::Debug + Send + Sync + 'static,
    {
        self.handle_connection(conn).err_into().await
    }
}
