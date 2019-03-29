// Copyright 2019 Timely Data, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Timely Data, Inc.

use futures::{future, Async, Future, Poll};
use std::collections::HashMap;
use std::fmt;
use tokio_zookeeper::error::Create;
use tokio_zookeeper::{Acl, CreateMode, ZooKeeper};

use super::SetupFuture;

#[derive(Clone)]
pub struct DataflowManager<D> {
    setup: future::Shared<SetupFuture>,
    prefix: String,
    dataflows: HashMap<String, D>,
}

impl<D> DataflowManager<D> {
    pub fn new(parent_setup: future::Shared<SetupFuture>, prefix: String) -> DataflowManager<D> {
        let setup = make_setup(parent_setup, &prefix).shared();
        DataflowManager {
            setup,
            prefix,
            dataflows: HashMap::new(),
        }
    }

    fn wait_for_setup(&self) -> impl Future<Item = ZooKeeper, Error = failure::Error> {
        // NOTE(benesch): losing the structure of the underlying failure::Error
        // is unfortunate, but since failure::Error isn't clonable, no better
        // means presents itself.
        //
        // See: https://github.com/rust-lang-nursery/failure/issues/148
        self.setup.clone().extract_shared()
    }
}

fn make_setup(parent_setup: future::Shared<SetupFuture>, prefix: &str) -> SetupFuture {
    let prefix = prefix.to_string();
    let fut = parent_setup
        .extract_shared()
        .and_then(move |zk| {
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

trait SharedFutureExt<T> {
    fn extract_shared(self) -> ExtractShared<T>
    where
        T: Future;
}

impl<T, I, E> SharedFutureExt<T> for future::Shared<T>
where
    T: Future<Item = I, Error = E>,
    I: Clone,
    E: fmt::Debug,
{
    fn extract_shared(self) -> ExtractShared<T> {
        ExtractShared { inner: self }
    }
}

struct ExtractShared<T>
where
    T: Future,
{
    inner: future::Shared<T>,
}

impl<T, I, E> Future for ExtractShared<T>
where
    T: Future<Item = I, Error = E>,
    I: Clone,
    E: fmt::Debug,
{
    type Item = T::Item;
    type Error = failure::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.inner.poll() {
            Ok(Async::Ready(val)) => Ok(Async::Ready((*val).clone())),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(err) => Err(failure::err_msg(format!("{:?}", err)))
        }
    }
}
