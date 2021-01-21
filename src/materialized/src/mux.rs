// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use async_trait::async_trait;
use futures::future::TryFutureExt;
use futures::stream::{Stream, StreamExt};
use log::error;
use tokio::io::{self, AsyncRead, AsyncWrite, AsyncWriteExt};

use ore::netio::{self, AsyncReady, SniffedStream, SniffingStream};

use crate::http;

type Handlers<S> = Vec<Box<dyn ConnectionHandler<S> + Send + Sync>>;

/// A mux routes incoming TCP connections to a dynamic set of connection
/// handlers. It enables serving multiple protocols over the same port.
///
/// Connections are routed by sniffing the first several bytes sent over the
/// wire and matching them against each handler, in order. The first handler
/// to match the connection will be invoked.
pub struct Mux<S> where S: AsyncRead + AsyncWrite + Unpin + Send + Sync {
    handlers: Handlers<S>,
}

impl <S>Mux<S> where S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static {
    /// Constructs a new `Mux`.
    pub fn new() -> Mux<S> {
        Mux { handlers: vec![] }
    }

    /// Adds a new connection handler to this mux.
    pub fn add_handler<H>(&mut self, handler: H)
    where
        H: ConnectionHandler<S> + Send + Sync + 'static,
    {
        self.handlers.push(Box::new(handler));
    }

    /// Serves incoming traffic from `listener`.
    pub async fn serve<U>(self, mut incoming: U)
    where
        U: Stream<Item = io::Result<S>> + Unpin,
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
            tokio::spawn(handle_connection(handlers.clone(), conn));
        }
    }
}

async fn handle_connection<S>(handlers: Arc<Handlers<S>>, conn: S) 
    where S: AsyncRead + AsyncWrite + Unpin + Send + Sync {
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
                error!("error handling connection in {}: {:#}", handler.name(), e);
            }
            return;
        }
    }

    log::warn!(
        "dropped connection using unknown protocol (sniffed: 0x{})",
        hex::encode(buf)
    );
    let _ = ss.into_sniffed().write_all(b"unknown protocol\n").await;
}

/// A connection handler manages an incoming network connection.
#[async_trait]
pub trait ConnectionHandler<S> where S: AsyncRead + AsyncWrite + Unpin + Send + Sync {
    /// Returns the name of the connection handler for use in e.g. log messages.
    fn name(&self) -> &str;

    /// Determines whether this handler can accept the connection based on the
    /// first several bytes in the stream.
    fn match_handshake(&self, buf: &[u8]) -> bool;

    /// Handles the connection.
    async fn handle_connection(&self, conn: SniffedStream<S>) -> Result<(), anyhow::Error>;
}

#[async_trait]
impl <S>ConnectionHandler<S> for pgwire::Server 
    where S: AsyncRead + AsyncWrite + AsyncReady + Unpin + Send + Sync + 'static {
    fn name(&self) -> &str {
        "pgwire server"
    }

    fn match_handshake(&self, buf: &[u8]) -> bool {
        pgwire::match_handshake(buf)
    }

    async fn handle_connection(&self, conn: SniffedStream<S>) -> Result<(), anyhow::Error> {
        self.handle_connection(conn).await
    }
}

#[async_trait]
impl <S>ConnectionHandler<S> for http::Server
    where S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static {
    fn name(&self) -> &str {
        "http server"
    }

    fn match_handshake(&self, buf: &[u8]) -> bool {
        self.match_handshake(buf)
    }

    async fn handle_connection(&self, conn: SniffedStream<S>) -> Result<(), anyhow::Error> {
        self.handle_connection(conn).await
    }
}

#[async_trait]
impl <S>ConnectionHandler<S> for comm::Switchboard<SniffedStream<S>>
    where S: comm::Connection + Unpin + Sync {
    fn name(&self) -> &str {
        "switchboard"
    }

    fn match_handshake(&self, buf: &[u8]) -> bool {
        comm::protocol::match_handshake(buf)
    }

    async fn handle_connection(&self, conn: SniffedStream<S>) -> Result<(), anyhow::Error> {
        self.handle_connection(conn).err_into().await
    }
}
