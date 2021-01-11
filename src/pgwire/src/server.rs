// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};

use anyhow::bail;
use async_trait::async_trait;
use log::trace;
use openssl::ssl::{Ssl, SslContext};
use tokio::io::{self, AsyncRead, AsyncWrite, AsyncWriteExt, Interest, ReadBuf, Ready};
use tokio_openssl::SslStream;

use coord::session::Session;
use ore::netio::AsyncReady;

use crate::codec::{self, FramedConn, ACCEPT_SSL_ENCRYPTION, REJECT_ENCRYPTION};
use crate::id_alloc::{IdAllocator, IdExhaustionError};
use crate::message::FrontendStartupMessage;
use crate::protocol::StateMachine;
use crate::secrets::SecretManager;

pub struct Server {
    id_alloc: IdAllocator,
    secrets: SecretManager,
    tls: Option<SslContext>,
    coord_client: coord::Client,
}

impl Server {
    pub fn new(tls: Option<SslContext>, coord_client: coord::Client) -> Server {
        Server {
            id_alloc: IdAllocator::new(1, 1 << 16),
            secrets: SecretManager::new(),
            tls,
            coord_client,
        }
    }

    pub async fn handle_connection<A>(&self, conn: A) -> Result<(), anyhow::Error>
    where
        A: AsyncRead + AsyncWrite + AsyncReady + Send + Sync + Unpin + fmt::Debug + 'static,
    {
        // Allocate state for this connection.
        let conn_id = match self.id_alloc.alloc() {
            Ok(id) => id,
            Err(IdExhaustionError) => {
                bail!("maximum number of connections reached");
            }
        };
        self.secrets.generate(conn_id);

        let res = self.handle_connection_inner(conn_id, conn).await;

        // Clean up state tied to this specific connection.
        self.id_alloc.free(conn_id);
        self.secrets.free(conn_id);

        res
    }

    pub async fn handle_connection_inner<A>(
        &self,
        conn_id: u32,
        conn: A,
    ) -> Result<(), anyhow::Error>
    where
        A: AsyncRead + AsyncWrite + AsyncReady + Send + Sync + Unpin + fmt::Debug + 'static,
    {
        let mut conn = Conn::Unencrypted(conn);
        loop {
            let message = codec::decode_startup(&mut conn).await?;

            match &message {
                Some(message) => trace!("cid={} recv={:?}", conn_id, message),
                None => trace!("cid={} recv=<eof>", conn_id),
            }

            conn = match message {
                // Clients sometimes hang up during the startup sequence, e.g.
                // because they receive an unacceptable response to an
                // `SslRequest`. This is considered a graceful termination.
                None => return Ok(()),

                Some(FrontendStartupMessage::Startup { version, params }) => {
                    let coord_client = self.coord_client.for_session(Session::new(conn_id));
                    let machine = StateMachine {
                        conn: FramedConn::new(conn_id, conn),
                        conn_id,
                        secret_key: self.secrets.get(conn_id).unwrap(),
                        coord_client,
                    };
                    machine.run(version, params).await?;
                    return Ok(());
                }

                Some(FrontendStartupMessage::CancelRequest {
                    conn_id,
                    secret_key,
                }) => {
                    if self.secrets.verify(conn_id, secret_key) {
                        self.coord_client.clone().cancel_request(conn_id).await;
                    }
                    // For security, the client is not told whether the cancel
                    // request succeeds or fails.
                    return Ok(());
                }

                Some(FrontendStartupMessage::SslRequest) => match conn {
                    // NOTE(benesch): we can match on `self.tls` properly,
                    // instead of checking `is_some` and `unwrap`ping, when
                    // the move_ref_patterns feature stabilizes.
                    // See: https://github.com/rust-lang/rust/issues/68354
                    Conn::Unencrypted(mut conn) if self.tls.is_some() => {
                        trace!("cid={} send=AcceptSsl", conn_id);
                        conn.write_all(&[ACCEPT_SSL_ENCRYPTION]).await?;
                        let tls = self.tls.as_ref().unwrap();
                        let mut ssl_stream = SslStream::new(Ssl::new(tls)?, conn)?;
                        Pin::new(&mut ssl_stream).accept().await?;
                        Conn::Ssl(ssl_stream)
                    }
                    mut conn => {
                        trace!("cid={} send=RejectSsl", conn_id);
                        conn.write_all(&[REJECT_ENCRYPTION]).await?;
                        conn
                    }
                },

                Some(FrontendStartupMessage::GssEncRequest) => {
                    trace!("cid={} send=RejectGssEnc", conn_id);
                    conn.write_all(&[REJECT_ENCRYPTION]).await?;
                    conn
                }
            }
        }
    }
}

#[derive(Debug)]
enum Conn<A> {
    Unencrypted(A),
    Ssl(SslStream<A>),
}

impl<A> AsyncRead for Conn<A>
where
    A: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut ReadBuf,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Conn::Unencrypted(inner) => Pin::new(inner).poll_read(cx, buf),
            Conn::Ssl(inner) => Pin::new(inner).poll_read(cx, buf),
        }
    }
}

impl<A> AsyncWrite for Conn<A>
where
    A: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Conn::Unencrypted(inner) => Pin::new(inner).poll_write(cx, buf),
            Conn::Ssl(inner) => Pin::new(inner).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Conn::Unencrypted(inner) => Pin::new(inner).poll_flush(cx),
            Conn::Ssl(inner) => Pin::new(inner).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Conn::Unencrypted(inner) => Pin::new(inner).poll_shutdown(cx),
            Conn::Ssl(inner) => Pin::new(inner).poll_shutdown(cx),
        }
    }
}

#[async_trait]
impl<A> AsyncReady for Conn<A>
where
    A: AsyncRead + AsyncWrite + AsyncReady + Sync + Unpin,
{
    async fn ready(&self, interest: Interest) -> io::Result<Ready> {
        match self {
            Conn::Unencrypted(inner) => inner.ready(interest).await,
            Conn::Ssl(inner) => inner.ready(interest).await,
        }
    }
}
