// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::iter::FromIterator;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use failure::{bail, format_err};
use futures::stream::futures_unordered::FuturesUnordered;
use futures::sync::mpsc;
use futures::sync::mpsc::Sender;
use futures::{future, stream, Future, Sink, Stream};
use futures::{Async, Poll};
use lazy_static::lazy_static;
use linked_hash_map::LinkedHashMap;
use log::error;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio_zookeeper::error::Create;
use tokio_zookeeper::error::Multi as MultiError;
use tokio_zookeeper::{
    Acl, CreateMode, KeeperState, MultiResponse, WatchedEvent, WatchedEventType, ZooKeeper,
    ZooKeeperBuilder,
};

use ore::closure;
use ore::fatal;
use ore::future::FutureExt;
use ore::future::StreamExt;

/// MetaStore manages storage of persistent, distributed metadata.
///
/// At the moment, it uses ZooKeeper as its backend, but care has been taken to
/// avoid tightly coupling its API to the ZooKeeper API.
#[derive(Clone)]
pub struct MetaStore<D> {
    prefix: Cow<'static, str>,
    addr: SocketAddr,
    setup: future::Shared<Box<dyn Future<Item = ZooKeeper, Error = failure::Error> + Send>>,
    inner: Arc<Mutex<Inner<D>>>,

    // This field ties the lifetime of this channel to the lifetime of this
    // struct, which makes for a convenient cancel signal when the struct is
    // dropped.
    _cancel_tx: Sender<()>,
}

impl<D> fmt::Debug for MetaStore<D> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Metastore")
            .field("prefix", &self.prefix)
            .field("addr", &self.addr)
            .finish()
    }
}

struct Inner<D> {
    // Dataflows are stored in a LinkedHashMap to maintain topological ordering.
    dataflows: LinkedHashMap<String, D>,
    senders: Vec<Sender<DataflowEvent<D>>>,
}

impl<D> MetaStore<D>
where
    D: Dataflow,
{
    pub fn new<P>(addr: &SocketAddr, prefix: P) -> MetaStore<D>
    where
        P: Into<Cow<'static, str>>,
    {
        let addr = *addr;
        let mut prefix = prefix.into();
        if prefix.ends_with('/') {
            let _ = prefix.to_mut().pop();
        }

        let inner = Arc::new(Mutex::new(Inner {
            dataflows: LinkedHashMap::new(),
            senders: Vec::new(),
        }));

        let setup = setup(&prefix, &addr).shared();
        tokio::spawn(setup.clone().then(|r| match r {
            Err(e) => fatal!("MetaStore setup failed: {}", e),
            _ => Ok(()),
        }));

        let (cancel_tx, cancel_rx) = futures::sync::mpsc::channel(0);

        let ms = MetaStore {
            prefix,
            addr,
            setup,
            inner: inner.clone(),
            _cancel_tx: cancel_tx,
        };
        ms.start_watching(cancel_rx);
        ms
    }

    pub fn read_dataflows(
        &self,
        dataflows: Vec<String>,
    ) -> impl Future<Item = HashMap<String, StoredDataflow<D>>, Error = failure::Error> {
        let prefix = self.prefix.to_string();
        self.wait_for_setup()
            .and_then(move |zk| read_dataflows(zk, prefix, dataflows))
            .map(|(_zk, dataflow_map)| dataflow_map)
    }

    pub fn register_dataflow_watch(&self) -> impl Stream<Item = DataflowEvent<D>, Error = ()> {
        let (tx, rx) = mpsc::channel(0);
        let events: Vec<_> = {
            let mut inner = self.inner.lock().unwrap();
            inner.senders.push(tx);
            inner
                .dataflows
                .iter()
                .map(|(_, dataflow)| DataflowEvent::Created(dataflow.clone()))
                .collect()
        };
        stream::iter_ok(events).chain(rx)
    }

    pub fn create_dataflow<U>(
        &self,
        name: &str,
        dataflow: D,
        uses: U,
    ) -> impl Future<Item = StoredDataflow<D>, Error = failure::Error>
    where
        U: IntoIterator<Item = StoredDataflow<D>>,
    {
        let mut uses_names = Vec::new();
        let mut uses_data = Vec::new();
        for mut sd in uses {
            let path = format!("/{}/dataflows/{}", self.prefix, sd.name);
            sd.used_by.push(name.to_owned());
            let encoded = match BINCODER.serialize(&sd) {
                Ok(encoded) => encoded,
                Err(err) => return future::err(failure::Error::from_boxed_compat(err)).left(),
            };
            uses_data.push((path, sd.version, encoded));
            uses_names.push(sd.name);
        }
        let sd = StoredDataflow {
            name: name.to_owned(),
            inner: dataflow,
            version: 0,
            used_by: Vec::new(),
            uses: uses_names,
        };
        let encoded = match BINCODER.serialize(&sd) {
            Ok(encoded) => encoded,
            Err(err) => return future::err(failure::Error::from_boxed_compat(err)).left(),
        };
        let path = format!("/{}/dataflows/{}", self.prefix, name);
        self.wait_for_setup()
            .and_then(move |zk| {
                let mut multi = zk.multi();
                multi = multi.create(&path, encoded, Acl::open_unsafe(), CreateMode::Persistent);
                for (path, version, encoded) in uses_data {
                    multi = multi.set_data(&path, Some(version), encoded);
                }
                multi.run()
            })
            .and_then(move |(_zk, res)| {
                let res: Result<Vec<tokio_zookeeper::MultiResponse>, _> = Result::from_iter(res);
                match res {
                    Ok(_) => Ok(sd),
                    Err(err) => Err(format_err!("{}", err)),
                }
            })
            .right()
    }

    pub fn delete_dataflow(&self, name: &str) -> impl Future<Item = (), Error = failure::Error> {
        let name = name.to_owned();
        let path = format!("/{}/dataflows/{}", self.prefix, name);
        let prefix = self.prefix.to_string();
        self.wait_for_setup()
            .and_then(closure!([clone path] |zk| zk.get_data(&path)))
            .and_then(move |(zk, res)| {
                let (bytes, version) = match res {
                    Some((bytes, stat)) => (bytes, stat.version),
                    _ => bail!("no such dataflow: {}", name),
                };
                let dataflow: StoredDataflow<D> = match BINCODER.deserialize(&bytes) {
                    Ok(d) => d,
                    Err(err) => return Err(failure::Error::from_boxed_compat(err)),
                };
                if dataflow.used_by.is_empty() {
                    Ok((zk, dataflow.uses, name, version))
                } else {
                    Err(format_err!(
                        "cannot delete {}: still depended upon by dataflow '{}'",
                        name,
                        dataflow.used_by[0]
                    ))
                }
            })
            .and_then(move |(zk, uses, name, version)| {
                read_dataflows::<D>(zk, prefix.clone(), uses)
                    .map(move |(zk, dataflow_map)| (zk, dataflow_map, prefix, name, version))
            })
            .and_then(move |(zk, dataflow_map, prefix, name, version)| {
                let mut multi = zk.multi();
                multi = multi.delete(&path, Some(version));
                for (_, mut sd) in dataflow_map {
                    let path = format!("/{}/dataflows/{}", prefix, sd.name);
                    sd.used_by
                        .remove(sd.used_by.iter().position(|u| u == &name).unwrap());
                    let encoded = match BINCODER.serialize(&sd) {
                        Ok(encoded) => encoded,
                        Err(err) => {
                            return future::err(failure::Error::from_boxed_compat(err)).left();
                        }
                    };
                    multi = multi.set_data(&path, Some(sd.version), encoded);
                }
                multi.run().right()
            })
            .and_then(|(_zk, res)| handle_multi_response(res))
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
                // TODO(benesch): it'd be nice to reuse the main connection
                // here, but this code is much harder to write without a
                // dedicated watch stream.
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

    fn wait_for_setup(&self) -> impl Future<Item = ZooKeeper, Error = failure::Error> {
        self.setup.clone().extract_shared()
    }
}

fn handle_multi_response(
    res: Vec<Result<MultiResponse, MultiError>>,
) -> Result<(), failure::Error> {
    let mut saw_error = false;
    for r in res {
        match r {
            Ok(_) => (),
            Err(MultiError::RolledBack) | Err(MultiError::Skipped) => saw_error = true,
            Err(err) => return Err(format_err!("{}", err)),
        }
    }
    if saw_error {
        bail!("multi request failed, but without an error");
    }
    Ok(())
}

pub trait Dataflow: Serialize + DeserializeOwned + fmt::Debug + Clone + Send + 'static {}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoredDataflow<D> {
    pub name: String,
    pub inner: D,
    #[serde(skip)]
    version: i32,
    uses: Vec<String>,
    used_by: Vec<String>,
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum DataflowEvent<D> {
    Created(D),
    Deleted(String),
}

fn setup(
    prefix: &str,
    addr: &SocketAddr,
) -> Box<dyn Future<Item = ZooKeeper, Error = failure::Error> + Send> {
    let root1 = format!("/{}", prefix);
    let root2 = format!("/{}/dataflows", prefix);
    Box::new(
        connect(addr)
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
            .and_then(move |(zk, res)| match res {
                Ok(_) | Err(Create::NodeExists) => Ok(zk),
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
    let (created_names, deleted_names) = {
        // TODO(benesch): these should be HashSet<&String>s, not
        // HashSet<String>s, but making that change causes the compiler to
        // panic. https://github.com/rust-lang/rust/issues/58840
        let new_names: HashSet<String> = children.into_iter().collect();
        let inner = inner.lock().unwrap();
        let current_names = HashSet::from_iter(inner.dataflows.keys().cloned());
        let created_names = &new_names - &current_names;
        let deleted_names = &current_names - &new_names;
        (created_names, deleted_names)
    };

    // Construct a vector of futures that will collectively read the contents of
    // each created name.
    let data_futs: Vec<_> = created_names
        .into_iter()
        .map(closure!([clone zk, ref prefix] |name| {
            zk.clone()
                .get_data(&format!("/{}/dataflows/{}", prefix, name))
                .map(|(zk, res)| (zk, name, res))
        }))
        .collect();

    future::join_all(data_futs).and_then(move |results| {
        let mut results: Vec<_> = results
            .into_iter()
            .filter_map(|(_zk, name, data)| data.map(|(bytes, stat)| (stat.czxid, name, bytes)))
            .collect();
        // It is very important to present dataflows in topological order, or
        // building the dataflow will crash because the dataflows upon which
        // it is built will not yet be present. Sorting by order of creation is
        // implicitly a topological sort, as you can't create a dataflow unless
        // all the dataflows upon which it depends are already created.
        //
        // Warning: this assumption breaks if we start altering dataflows
        // in place. That's not on the horizon, though.
        results.sort_by_key(|r| r.0);

        let mut send_futs: FuturesUnordered<
            Box<dyn Future<Item = (), Error = failure::Error> + Send>,
        > = FuturesUnordered::new();
        {
            let mut inner = inner.lock().unwrap();
            for (_czxid, name, bytes) in results {
                let dataflow: StoredDataflow<D> = match BINCODER.deserialize(&bytes) {
                    Ok(d) => d,
                    Err(err) => return future::err(failure::Error::from_boxed_compat(err)).left(),
                };
                for tx in &inner.senders {
                    // TODO(benesch): we're ignoring send errors. That's not
                    // wrong, because a send can only fail if the receiving end
                    // hangs up. Ideally we'd remove the broken tx from
                    // inner.senders, but the synchronization is rather tricky
                    // to get right, and the only downside to ignoring the error
                    // is that we do a bit of extra work on every event.
                    let send_fut = tx
                        .clone()
                        .send(DataflowEvent::Created(dataflow.inner.clone()))
                        .discard()
                        .or_else(|_| Ok(()));
                    send_futs.push(Box::new(send_fut));
                }
                inner.dataflows.insert(name, dataflow.inner);
            }
            for name in deleted_names {
                for tx in &inner.senders {
                    let send_fut = tx
                        .clone()
                        .send(DataflowEvent::Deleted(name.clone()))
                        .discard()
                        .or_else(|_| Ok(()));
                    send_futs.push(Box::new(send_fut));
                }
                inner.dataflows.remove(&name);
            }
        }
        send_futs.drain().map(|_| (zk, inner, prefix)).right()
    })
}

fn read_dataflows<D: Dataflow>(
    zk: ZooKeeper,
    prefix: String,
    dataflows: Vec<String>,
) -> impl Future<Item = (ZooKeeper, HashMap<String, StoredDataflow<D>>), Error = failure::Error> {
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
            Some((bytes, stat)) => Ok((name, bytes, stat.version)),
            None => bail!("dataflow {} does not exist", name),
        })
        .fold(HashMap::new(), move |mut out, (name, bytes, version)| {
            let mut dataflow: StoredDataflow<D> = match BINCODER.deserialize(&bytes) {
                Ok(d) => d,
                Err(err) => return Err(failure::Error::from_boxed_compat(err)),
            };
            dataflow.version = version;
            out.insert(name.to_owned(), dataflow);
            Ok(out)
        })
        .map(|dataflow_map| (zk, dataflow_map))
}

lazy_static! {
    static ref BINCODER: bincode::Config = {
        let mut c = bincode::config();
        c.limit(1 << 20);
        c
    };
}

// TODO(benesch): see if this helper can be moved into ore::future.
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
            // NOTE(benesch): losing the structure of the underlying
            // failure::Error is unfortunate, but since failure::Error isn't
            // clonable, no better means presents itself.
            //
            // See: https://github.com/rust-lang-nursery/failure/issues/148
            Err(err) => Err(failure::err_msg(format!("{:?}", err))),
        }
    }
}
