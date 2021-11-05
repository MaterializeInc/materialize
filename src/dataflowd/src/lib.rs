// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Independent dataflow server support.
//!
//! This crate provides types that facilitate communicating with a remote
//! dataflow server.

#![deny(missing_docs)]

use std::net::SocketAddr;

use async_trait::async_trait;
use futures::sink::SinkExt;
use futures::StreamExt;
use log::trace;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_serde::formats::Bincode;
use tokio_util::codec::LengthDelimitedCodec;

use dataflow::{Command, Response};

/// A framed connection to a dataflowd server.
pub type Framed<C, T, U> =
    tokio_serde::Framed<tokio_util::codec::Framed<C, LengthDelimitedCodec>, T, U, Bincode<T, U>>;

/// A framed connection from the server's perspective.
pub type FramedServer<C> = Framed<C, Command, Response>;

/// A framed connection from the client's perspective.
pub type FramedClient<C> = Framed<C, Response, Command>;

/// Constructs a framed connection for the server.
pub fn framed_server<C>(conn: C) -> FramedServer<C>
where
    C: AsyncRead + AsyncWrite,
{
    tokio_serde::Framed::new(
        tokio_util::codec::Framed::new(conn, LengthDelimitedCodec::new()),
        Bincode::default(),
    )
}

/// Constructs a framed connection for the client.
pub fn framed_client<C>(conn: C) -> FramedClient<C>
where
    C: AsyncRead + AsyncWrite,
{
    tokio_serde::Framed::new(
        tokio_util::codec::Framed::new(conn, LengthDelimitedCodec::new()),
        Bincode::default(),
    )
}

/// A client to a remote dataflow server.
pub struct RemoteClient {
    // TODO: the client could discover the number of workers from the server.
    num_workers: usize,
    conns: Vec<FramedClient<TcpStream>>,
}

impl RemoteClient {
    /// Connects a remote client to the specified remote dataflow server.
    pub async fn connect(
        num_workers: usize,
        addrs: &[SocketAddr],
    ) -> Result<RemoteClient, anyhow::Error> {
        let mut conns = Vec::new();
        for addr in addrs {
            conns.push(framed_client(TcpStream::connect(addr).await?));
        }
        Ok(RemoteClient { num_workers, conns })
    }
}

#[async_trait]
impl dataflow::Client for RemoteClient {
    fn num_workers(&self) -> usize {
        self.num_workers
    }

    async fn send(&mut self, cmd: dataflow::Command) {
        // TODO: something better than panicking.
        trace!("Broadcasting dataflow command: {:?}", cmd);
        let num_workers = self.num_workers();
        if num_workers == 1 {
            // This special case avoids a clone of the whole plan.
            self.conns[0]
                .send(cmd)
                .await
                .expect("worker command receiver should not drop first");
        } else {
            for (index, sendpoint) in self.conns.iter_mut().enumerate() {
                sendpoint
                    .send(cmd.clone_for_worker(index, num_workers))
                    .await
                    .expect("worker command receiver should not drop first")
            }
        }
    }

    async fn recv(&mut self) -> Option<dataflow::Response> {
        // TODO: something better than panicking.
        // Attempt to read from each of `self.conns`.
        futures::stream::select_all(self.conns.iter_mut().map(|stream| stream.by_ref()))
            .next()
            .await
            .map(|x| x.expect("connection to dataflow server broken"))
    }
}
