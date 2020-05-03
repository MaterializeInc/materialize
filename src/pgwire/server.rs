// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Weak;

use failure::bail;
use futures::sink::SinkExt;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio_util::codec::Framed;

use sql::Session;

use crate::codec::{self, Codec};
use crate::id_alloc::{IdAllocator, IdExhaustionError};
use crate::message::FrontendStartupMessage;
use crate::protocol::StateMachine;
use crate::secrets::SecretManager;

pub struct Server {
    id_alloc: IdAllocator,
    secrets: SecretManager,
    cmdq_tx: Weak<futures::channel::mpsc::UnboundedSender<coord::Command>>,
    gather_metrics: bool,
}

impl Server {
    pub fn new(
        cmdq_tx: Weak<futures::channel::mpsc::UnboundedSender<coord::Command>>,
        gather_metrics: bool,
    ) -> Server {
        Server {
            id_alloc: IdAllocator::new(1, 1 << 16),
            secrets: SecretManager::new(),
            cmdq_tx,
            gather_metrics,
        }
    }

    pub async fn handle_connection<A>(&self, mut conn: A) -> Result<(), failure::Error>
    where
        A: AsyncRead + AsyncWrite + Unpin,
    {
        let mut cmdq_tx = match self.cmdq_tx.upgrade() {
            Some(cmdq_tx) => (*cmdq_tx).clone(),
            None => {
                // The server is terminating. Drop the connection on the floor.
                return Ok(());
            }
        };

        loop {
            match codec::decode_startup(&mut conn).await? {
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
                        cmdq_tx,
                        gather_metrics: self.gather_metrics,
                    };
                    let res = machine.start(Session::default(), version, params).await;

                    self.id_alloc.free(conn_id);
                    self.secrets.free(conn_id);
                    break Ok(res?);
                }

                FrontendStartupMessage::CancelRequest {
                    conn_id,
                    secret_key,
                } => {
                    if self.secrets.verify(conn_id, secret_key) {
                        cmdq_tx
                            .send(coord::Command::CancelRequest { conn_id })
                            .await?;
                    }
                    // For security, the client is not told whether the cancel
                    // request succeeds or fails.
                    break Ok(());
                }

                FrontendStartupMessage::GssEncRequest => conn.write_all(&[b'N']).await?,
                FrontendStartupMessage::SslRequest => conn.write_all(&[b'N']).await?,
            }
        }
    }
}
