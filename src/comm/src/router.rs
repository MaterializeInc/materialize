// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An intra-process connection router.

use std::collections::{hash_map, HashMap};
use std::fmt;
use std::hash::Hash;

pub struct RoutingTable<K, C>(HashMap<K, RoutingTableEntry<C>>)
where
    K: Eq + Hash;

impl<K, C> RoutingTable<K, C>
where
    K: Eq + Hash + fmt::Debug,
{
    pub fn route(&mut self, key: K, conn: C) {
        let entry = self.0.entry(key).or_default();
        match entry {
            RoutingTableEntry::AwaitingRx(conns) => conns.push(conn),
            RoutingTableEntry::AwaitingConn(tx) => match tx.unbounded_send(conn) {
                Ok(()) => (),
                Err(_) => {
                    *entry = RoutingTableEntry::Full;
                }
            },
            RoutingTableEntry::Full => (),
        }
    }

    pub fn remove_dest(&mut self, key: K) {
        self.0.remove(&key);
    }

    pub fn add_dest(&mut self, key: K) -> futures::channel::mpsc::UnboundedReceiver<C> {
        let (conn_tx, conn_rx) = futures::channel::mpsc::unbounded();
        match self.0.entry(key) {
            hash_map::Entry::Occupied(mut entry) => match entry.get_mut() {
                RoutingTableEntry::AwaitingRx(conns) => {
                    for conn in conns.drain(..) {
                        conn_tx.unbounded_send(conn).unwrap();
                    }
                    *entry.get_mut() = RoutingTableEntry::AwaitingConn(conn_tx);
                }
                _ => panic!("router: attempting to add dest {:?} twice", entry.key()),
            },
            hash_map::Entry::Vacant(entry) => {
                entry.insert(RoutingTableEntry::AwaitingConn(conn_tx));
            }
        }
        conn_rx
    }
}

impl<K, C> Default for RoutingTable<K, C>
where
    K: Eq + Hash,
{
    fn default() -> RoutingTable<K, C> {
        RoutingTable(HashMap::default())
    }
}

enum RoutingTableEntry<C> {
    /// Connections have arrived, but the channel receiver has not yet been
    /// constructed. This state is only possible for broadcast channels at the
    /// moment, as MPSC transmitters and receivers are constructed
    /// simultaneously.
    AwaitingRx(Vec<C>),
    /// A receiver has been constructed and is awaiting an incoming connection.
    AwaitingConn(futures::channel::mpsc::UnboundedSender<C>),
    /// The channel is no longer awaiting an incoming connection. It may be
    /// actively receiving messages, or it may be closed, but either way
    /// new connections will not be attached to the channel receiver.
    Full,
}

impl<C> Default for RoutingTableEntry<C> {
    fn default() -> RoutingTableEntry<C> {
        RoutingTableEntry::AwaitingRx(Vec::new())
    }
}
