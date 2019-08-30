// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use futures::{Future, Poll, Sink, Stream};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::marker::PhantomData;
use std::net::ToSocketAddrs;
use tokio::codec::Framed;
use tokio::io::{self, AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_bincode::BinCodec;
use uuid::Uuid;

use crate::protocol;

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct Sender<T> {
    addr: String,
    uuid: Uuid,
    _data: std::marker::PhantomData<T>,
}

impl<T> Sender<T> {
    pub(crate) fn new(addr: String, uuid: Uuid) -> Sender<T> {
        Sender {
            addr,
            uuid,
            _data: PhantomData,
        }
    }

    pub fn connect(
        &self,
    ) -> impl Future<Item = impl Sink<SinkItem = T, SinkError = bincode::Error>, Error = io::Error>
    where
        T: Serialize + Send,
        for<'de> T: Deserialize<'de>,
    {
        let addr = self.addr.to_socket_addrs().unwrap().next().unwrap();
        let uuid = self.uuid;
        TcpStream::connect(&addr)
            .and_then(move |conn| protocol::send_handshake(conn, uuid))
            .map(|conn| Framed::new(conn, BinCodec::new()))
    }
}

pub struct Receiver<T>(Box<dyn Stream<Item = T, Error = bincode::Error> + Send>);

impl<T> Receiver<T> {
    pub(crate) fn new<C>(conn_rx: impl Stream<Item = C, Error = ()> + Send + 'static) -> Receiver<T>
    where
        C: AsyncRead + AsyncWrite + Send + 'static,
        T: Serialize + Send,
        for<'de> T: Deserialize<'de>,
    {
        Receiver(Box::new(
            conn_rx
                .map_err(|_| -> bincode::Error { unreachable!() })
                .map(|tcp_stream| Framed::new(tcp_stream, BinCodec::new()))
                .flatten(),
        ))
    }
}

impl<T> Stream for Receiver<T> {
    type Item = T;
    type Error = bincode::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.0.poll()
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("mpsc receiver")
    }
}
