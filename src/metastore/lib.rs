// Copyright 2019 Timely Data, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Timely Data, Inc.

use futures::{future, Future};
use std::borrow::Cow;
use std::net::SocketAddr;
use tokio_zookeeper::error::Create;
use tokio_zookeeper::{Acl, CreateMode, ZooKeeper, ZooKeeperBuilder};

use self::dataflow::DataflowManager;

pub mod dataflow;

type SetupFuture = Box<dyn Future<Item = (ZooKeeper), Error = failure::Error> + Send>;

/// MetaStore manages storage of persistent, distributed metadata.
///
/// At the moment, it uses ZooKeeper as its backend, but care has been taken to
/// avoid tightly coupling its API to the ZooKeeper API.
pub struct MetaStore {
    prefix: Cow<'static, str>,
    setup: future::Shared<SetupFuture>,
}

impl MetaStore {
    /// Creates a new `MetaStore` from a ZooKeeper address and a path prefix.
    /// All data managed by this `MetaStore` will be stored in child nodes of
    /// the specified path prefix. If the path prefix does not exist, it will be
    /// created automatically.
    ///
    /// TODO(benesch): support ZooKeeper clusters with automatic failover. This
    /// is something we'd get for free with a client in any other language.
    pub fn new<P>(addr: &SocketAddr, prefix: P) -> MetaStore
    where
        P: Into<Cow<'static, str>>,
    {
        let mut prefix = prefix.into();
        if !prefix.starts_with('/') {
            prefix.to_mut().insert(0, '/');
        }
        if prefix.ends_with('/') {
            let _ = prefix.to_mut().pop();
        }

        let setup = make_setup(addr, &prefix).shared();

        MetaStore { prefix, setup }
    }

    fn dataflows<D>(&self) -> DataflowManager<D> {
        let prefix = format!("{}/dataflows", self.prefix);
        DataflowManager::new(self.setup.clone(), prefix)
    }
}

fn make_setup(addr: &SocketAddr, prefix: &str) -> SetupFuture {
    let prefix = prefix.to_owned();
    let mut builder = ZooKeeperBuilder::default();
    builder.set_logger(ore::log::slog_adapter());
    let fut = builder
        .connect(addr)
        .and_then(move |(zk, _watch)| {
            zk.create(
                &prefix,
                &b""[..],
                Acl::open_unsafe(),
                CreateMode::Persistent,
            )
        })
        .and_then(move |(zk, res)| match res {
            Ok(_) | Err(Create::NodeExists) => Ok(zk),
            Err(err) => Err(err.into()),
        });
    Box::new(fut)
}
