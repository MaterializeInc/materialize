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

use failure::bail;
use futures::sink::SinkExt;
use openssl::ssl::SslAcceptor;
use tokio::io::{self, AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio_openssl::SslStream;
use tokio_util::codec::Framed;

use sql::Session;

use crate::codec::{self, Codec, ACCEPT_SSL_ENCRYPTION, REJECT_ENCRYPTION};
use crate::id_alloc::{IdAllocator, IdExhaustionError};
use crate::message::FrontendStartupMessage;
use crate::protocol::StateMachine;
use crate::secrets::SecretManager;

pub struct Server {
    id_alloc: IdAllocator,
    secrets: SecretManager,
    tls: Option<SslAcceptor>,
    cmdq_tx: futures::channel::mpsc::UnboundedSender<coord::Command>,
}

impl Server {
    pub fn new(
        tls: Option<SslAcceptor>,
        cmdq_tx: futures::channel::mpsc::UnboundedSender<coord::Command>,
    ) -> Server {
        Server {
            id_alloc: IdAllocator::new(1, 1 << 16),
            secrets: SecretManager::new(),
            tls,
            cmdq_tx,
        }
    }

    pub async fn handle_connection<A>(&self, conn: A) -> Result<(), failure::Error>
    where
        A: AsyncRead + AsyncWrite + Unpin + fmt::Debug + Send + Sync + 'static,
    {
        let mut conn = Conn::Unencrypted(conn);
        loop {
            conn = match codec::decode_startup(&mut conn).await? {
                FrontendStartupMessage::Startup { version, params } => {
                    let conn_id = match self.id_alloc.alloc() {
                        Ok(id) => id,
                        Err(IdExhaustionError) => {
                            bail!("maximum number of connections reached");
                        }
                    };
                    self.secrets.generate(conn_id);

                    let mut machine = StateMachine {
                        conn: &mut Framed::new(conn, Codec::new()).buffer(32),
                        conn_id,
                        secret_key: self.secrets.get(conn_id).unwrap(),
                        cmdq_tx: self.cmdq_tx.clone(),
                    };
                    let res = machine.start(Session::default(), version, params).await;

                    // Clean up state tied to this specific connection.
                    self.cmdq_tx
                        .clone()
                        .send(coord::Command::TerminateConnectionObjects { conn_id })
                        .await?;
                    self.id_alloc.free(conn_id);
                    self.secrets.free(conn_id);
                    return Ok(res?);
                }

                FrontendStartupMessage::CancelRequest {
                    conn_id,
                    secret_key,
                } => {
                    if self.secrets.verify(conn_id, secret_key) {
                        self.cmdq_tx
                            .clone()
                            .send(coord::Command::CancelRequest { conn_id })
                            .await?;
                    }
                    // For security, the client is not told whether the cancel
                    // request succeeds or fails.
                    return Ok(());
                }

                FrontendStartupMessage::SslRequest => match conn {
                    // NOTE(benesch): we can match on `self.tls` properly,
                    // instead of checking `is_some` and `unwrap`ping, when
                    // the move_ref_patterns feature stabilizes.
                    // See: https://github.com/rust-lang/rust/issues/68354
                    Conn::Unencrypted(mut conn) if self.tls.is_some() => {
                        conn.write_all(&[ACCEPT_SSL_ENCRYPTION]).await?;
                        let tls = self.tls.as_ref().unwrap();
                        Conn::Ssl(tokio_openssl::accept(tls, conn).await?)
                    }
                    mut conn => {
                        conn.write_all(&[REJECT_ENCRYPTION]).await?;
                        conn
                    }
                },

                FrontendStartupMessage::GssEncRequest => {
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
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
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
