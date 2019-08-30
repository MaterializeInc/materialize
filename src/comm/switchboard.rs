// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use futures::{Future, Sink, Stream};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use tokio::io::{self, AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use uuid::Uuid;

use crate::mpsc;
use crate::protocol;

pub struct Switchboard<C>(Arc<SwitchboardInner<C>>);

impl<C> Clone for Switchboard<C> {
    fn clone(&self) -> Switchboard<C> {
        Switchboard(self.0.clone())
    }
}

struct SwitchboardInner<C> {
    nodes: Vec<String>,
    id: usize,
    txs: Mutex<HashMap<Uuid, futures::sync::mpsc::Sender<C>>>,
}

impl Switchboard<TcpStream> {
    pub fn local() -> Result<Switchboard<TcpStream>, io::Error> {
        let addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 0);
        let listener = TcpListener::bind(&addr)?;
        let switchboard = Switchboard::new(vec![listener.local_addr()?.to_string()], 0);
        let switchboard2 = switchboard.clone();
        std::thread::spawn(move || {
            let server = listener
                .incoming()
                .map_err(|err| panic!("local switchboard: accept: {}", err))
                .for_each(move |conn| switchboard2.handle_connection(conn))
                .map_err(|err| panic!("local switchboard: handle connection: {}", err));
            tokio::runtime::current_thread::run(server)
        });
        Ok(switchboard)
    }
}

impl<C> Switchboard<C>
where
    C: AsyncRead + AsyncWrite + Send + 'static,
{
    pub fn new(nodes: Vec<String>, id: usize) -> Switchboard<C> {
        Switchboard(Arc::new(SwitchboardInner {
            nodes,
            id,
            txs: Mutex::new(HashMap::new()),
        }))
    }

    pub fn handle_connection(
        &self,
        conn: C,
    ) -> impl Future<Item = (), Error = SwitchboardError<C>> {
        let inner = self.0.clone();
        protocol::recv_handshake(conn)
            .map_err(|err| SwitchboardError::from(err))
            .and_then(move |(conn, uuid)| {
                let tx = {
                    let txs = inner.txs.lock().expect("lock poisoned");
                    txs[&uuid].clone()
                };
                tx.send(conn).from_err()
            })
            .map(|_| ())
    }

    pub fn mpsc<T>(&self) -> (mpsc::Sender<T>, mpsc::Receiver<T>)
    where
        T: Serialize + Send,
        for<'de> T: Deserialize<'de>,
    {
        let uuid = Uuid::new_v4();
        let addr = self.0.nodes[self.0.id].clone();
        let (conn_tx, conn_rx) = futures::sync::mpsc::channel(self.0.nodes.len());
        {
            let mut txs = self.0.txs.lock().expect("lock poisoned");
            txs.insert(uuid, conn_tx);
        }
        let tx = mpsc::Sender::new(addr, uuid);
        let rx = mpsc::Receiver::new(conn_rx);
        (tx, rx)
    }
}

pub enum SwitchboardError<T> {
    Io(io::Error),
    Send(futures::sync::mpsc::SendError<T>),
}

impl<T> From<io::Error> for SwitchboardError<T> {
    fn from(err: io::Error) -> SwitchboardError<T> {
        SwitchboardError::Io(err)
    }
}

impl<T> From<futures::sync::mpsc::SendError<T>> for SwitchboardError<T> {
    fn from(err: futures::sync::mpsc::SendError<T>) -> SwitchboardError<T> {
        SwitchboardError::Send(err)
    }
}

impl<T> fmt::Debug for SwitchboardError<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("switchboard error: ")?;
        match self {
            SwitchboardError::Io(err) => write!(f, "{:?}", err),
            SwitchboardError::Send(err) => write!(f, "{:?}", err),
        }
    }
}

impl<T> fmt::Display for SwitchboardError<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("switchboard error: ")?;
        match self {
            SwitchboardError::Io(err) => write!(f, "{}", err),
            SwitchboardError::Send(err) => write!(f, "{}", err),
        }
    }
}

impl<T> Error for SwitchboardError<T> {}
