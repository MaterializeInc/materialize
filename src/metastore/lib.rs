// Copyright 2019 Timely Data, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Timely Data, Inc.

use failure::{bail, format_err};
use futures::{future, stream, Future, Stream};
use lazy_static::lazy_static;
use log::error;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;
use std::net::SocketAddr;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};
use tokio_zookeeper::error::Create;
use tokio_zookeeper::{
    Acl, CreateMode, KeeperState, WatchedEvent, WatchedEventType, ZooKeeper, ZooKeeperBuilder,
};

use ore::closure;
use ore::fatal;
use ore::future::FutureExt;

/// MetaStore manages storage of persistent, distributed metadata.
///
/// At the moment, it uses ZooKeeper as its backend, but care has been taken to
/// avoid tightly coupling its API to the ZooKeeper API.
#[derive(Clone)]
pub struct MetaStore<D> {
    prefix: Cow<'static, str>,
    addr: SocketAddr,
    setup: future::Shared<Box<dyn Future<Item = (), Error = failure::Error> + Send>>,
    inner: Arc<Mutex<Inner<D>>>,

    // This field ties the lifetime of this channel to the lifetime of this
    // struct, which makes for a convenient cancel signal when the struct is
    // dropped.
    _cancel_tx: futures::sync::mpsc::Sender<()>,
}

struct Inner<D> {
    names: HashSet<String>,
    dataflows: Vec<D>,
    senders: Vec<Sender<D>>,
}

impl<D> MetaStore<D>
where
    D: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    pub fn new<P>(addr: &SocketAddr, prefix: P) -> MetaStore<D>
    where
        P: Into<Cow<'static, str>>,
    {
        let addr = addr.clone();
        let mut prefix = prefix.into();
        if prefix.ends_with("/") {
            let _ = prefix.to_mut().pop();
        }

        let inner = Arc::new(Mutex::new(Inner {
            names: HashSet::new(),
            dataflows: Vec::new(),
            senders: Vec::new(),
        }));

        let setup = setup(&prefix, &addr).shared();
        tokio::spawn(setup.clone().then(|r| {
            match r {
                Err(e) => fatal!("MetaStore setup failed: {}", e),
                _ => (),
            };
            Ok(())
        }));

        let (cancel_tx, cancel_rx) = futures::sync::mpsc::channel(0);

        let ms = MetaStore {
            prefix: prefix,
            addr: addr,
            setup: setup,
            inner: inner.clone(),
            _cancel_tx: cancel_tx,
        };
        ms.start_watching(cancel_rx);
        ms
    }

    pub fn read_dataflows(
        &self,
        dataflows: Vec<String>,
    ) -> impl Future<Item = HashMap<String, D>, Error = failure::Error> {
        let addr = self.addr.clone();
        let prefix = self.prefix.clone();
        self.wait_for_setup()
            .and_then(move |_| connect(&addr))
            .and_then(move |(zk, _)| {
                let mut futures = Vec::new();
                for name in dataflows {
                    futures.push(
                        zk.clone()
                            .get_data(&format!("/{}/dataflows/{}", prefix, name))
                            .map(|(zk, data)| (zk, name, data)),
                    )
                }
                stream::futures_unordered(futures)
                    .and_then(|(_zk, name, data)| match data {
                        Some((bytes, _stat)) => Ok((name, bytes)),
                        None => bail!("dataflow {} does not exist", name),
                    })
                    .fold(HashMap::new(), move |mut out, (name, bytes)| {
                        let dataflow: D = match BINCODER.deserialize(&bytes) {
                            Ok(d) => d,
                            Err(err) => return Err(failure::Error::from_boxed_compat(err)),
                        };
                        out.insert(name.to_owned(), dataflow);
                        Ok(out)
                    })
            })
    }

    pub fn register_dataflow_watch(&self) -> Receiver<D> {
        let (tx, rx) = mpsc::channel();
        let mut inner = self.inner.lock().unwrap();
        for d in &inner.dataflows {
            tx.send(d.to_owned()).unwrap();
        }
        inner.senders.push(tx);
        rx
    }

    // TODO: this needs to maintain consistency by running a transaction that
    // checks that all dependent dataflows have the correct version.
    pub fn new_dataflow(
        &self,
        name: &str,
        dataflow: &D,
    ) -> impl Future<Item = (), Error = failure::Error> {
        let encoded = match BINCODER.serialize(dataflow) {
            Ok(encoded) => encoded,
            Err(err) => return future::err(failure::Error::from_boxed_compat(err)).left(),
        };
        let path = format!("/{}/dataflows/{}", self.prefix, name);
        let addr = self.addr.clone();
        self.wait_for_setup()
            .and_then(move |_| connect(&addr))
            .and_then(move |(zk, _)| {
                zk.create(&path, encoded, Acl::open_unsafe(), CreateMode::Persistent)
            })
            .and_then(|(_zk, res)| match res {
                Ok(_name) => Ok(()),
                Err(err) => Err(err.into()),
            })
            .right()
    }

    fn start_watching(&self, cancel_signal: futures::sync::mpsc::Receiver<()>) {
        let dataflow_path = format!("/{}/dataflows", self.prefix);
        let prefix = self.prefix.to_string();
        let inner = self.inner.clone();
        let connect = connect(&self.addr);
        let events = stream::once(Ok(WatchedEvent {
            event_type: WatchedEventType::NodeChildrenChanged,
            keeper_state: KeeperState::SyncConnected,
            path: dataflow_path.clone(),
        }));

        let fut =
            self.wait_for_setup()
                .and_then(move |_| connect)
                .and_then(move |(zk, watch)| {
                    events
                        .chain(watch.map_err(|_| format_err!("unreachable")))
                        .fold((zk, inner, prefix), move |(zk, inner, prefix), event| {
                            if event.event_type != WatchedEventType::NodeChildrenChanged
                                || event.path != dataflow_path
                            {
                                return future::ok((zk, inner, prefix)).left();
                            }
                            zk.watch()
                                .get_children(&dataflow_path)
                                .and_then(move |(zk, res)| match res {
                                    None => future::err(format_err!("dataflows dir went missing"))
                                        .left(),
                                    Some(children) => notify(zk, inner, prefix, children).right(),
                                })
                                .right()
                        })
                        .discard()
                })
                .map_err(|e| fatal!("{}", e))
                .watch_for_cancel(cancel_signal.into_future().discard());
        tokio::spawn(fut);
    }

    fn wait_for_setup(&self) -> impl Future<Item = (), Error = failure::Error> {
        // NOTE(benesch): losing the structure of the underlying failure::Error
        // is unfortunate, but since failure::Error isn't clonable, no better
        // means presents itself.
        //
        // See: https://github.com/rust-lang-nursery/failure/issues/148
        self.setup
            .clone()
            .map(|_: future::SharedItem<()>| ())
            .map_err(|e| failure::err_msg(e))
    }
}

fn setup(
    prefix: &str,
    addr: &SocketAddr,
) -> Box<dyn Future<Item = (), Error = failure::Error> + Send> {
    let root1 = format!("/{}", prefix);
    let root2 = format!("/{}/dataflows", prefix);
    Box::new(
        ZooKeeper::connect(addr)
            .and_then(move |(zk, _watch)| {
                zk.create(&root1, &b""[..], Acl::open_unsafe(), CreateMode::Persistent)
            })
            .and_then(move |(zk, res)| match res {
                Ok(_) | Err(Create::NodeExists) => Ok(zk),
                Err(err) => Err(err.into()),
            })
            .and_then(move |zk| {
                zk.create(&root2, &b""[..], Acl::open_unsafe(), CreateMode::Persistent)
            })
            .and_then(move |(_zk, res)| match res {
                Ok(_) | Err(Create::NodeExists) => Ok(()),
                Err(err) => Err(err.into()),
            }),
    )
}

fn connect(
    addr: &SocketAddr,
) -> impl Future<Item = (ZooKeeper, impl Stream<Item = WatchedEvent, Error = ()>), Error = failure::Error>
{
    let mut builder = ZooKeeperBuilder::default();
    builder.set_logger(ore::log::slog_adapter());
    builder.connect(addr)
}

fn notify<D>(
    zk: ZooKeeper,
    inner: Arc<Mutex<Inner<D>>>,
    prefix: String,
    children: Vec<String>,
) -> impl Future<Item = (ZooKeeper, Arc<Mutex<Inner<D>>>, String), Error = failure::Error>
where
    D: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    // Determine which names are new.
    let new_names = {
        let inner = inner.lock().unwrap();
        &HashSet::from_iter(children) - &inner.names
    };

    // Construct a vector of futures that will collectively read the contents of
    // each new name.
    let data_futs: Vec<_> = new_names
        .into_iter()
        .map(closure!([clone zk, ref prefix] |name| {
            zk.clone()
                .get_data(&format!("/{}/dataflows/{}", prefix, name))
                .map(|(zk, res)| (zk, name, res))
        }))
        .collect();

    future::join_all(data_futs).and_then(move |results| {
        // Sort by creation zxid to enforce a stable ordering.
        let mut results: Vec<_> = results
            .into_iter()
            .filter_map(|(_zk, name, data)| data.map(|(bytes, stat)| (stat.czxid, name, bytes)))
            .collect();
        results.sort_by_key(|r| r.0);

        {
            let mut inner = inner.lock().unwrap();
            for (_czxid, name, bytes) in results {
                let dataflow: D = match BINCODER.deserialize(&bytes) {
                    Ok(d) => d,
                    Err(err) => return Err(failure::Error::from_boxed_compat(err)),
                };
                for tx in &inner.senders {
                    let _ignore = tx.send(dataflow.clone());
                }
                inner.names.insert(name);
                inner.dataflows.push(dataflow);
            }
        }

        Ok((zk, inner, prefix))
    })
}

lazy_static! {
    static ref BINCODER: bincode::Config = {
        let mut c = bincode::config();
        c.limit(1 << 20);
        c
    };
}
