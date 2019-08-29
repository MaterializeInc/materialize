// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Multiple-producer, multiple-consumer communication primitives.

use failure::{ensure, format_err};
use futures::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::{Arc, RwLock};

/// A multiple-producer, multiple-consumer (mpmc) channel where receivers are
/// keyed by K.
pub type Mux<K, T> = Arc<RwLock<MuxInner<K, T>>>;

/// The implementation of a [`Mux`].
#[derive(Debug)]
pub struct MuxInner<K, T>
where
    K: Hash + Eq,
{
    receivers: HashMap<K, UnboundedReceiver<T>>,
    senders: HashMap<K, UnboundedSender<T>>,
}

impl<K, T> Default for MuxInner<K, T>
where
    K: Hash + Eq,
{
    fn default() -> Self {
        MuxInner {
            receivers: Default::default(),
            senders: Default::default(),
        }
    }
}

impl<K, T> MuxInner<K, T>
where
    K: Clone + Hash + Eq + Debug,
{
    /// Registers a new channel for the specified key.
    pub fn channel(&mut self, key: K) -> Result<(), failure::Error> {
        // We might hold onto closed senders for arbitrary amounts of time, but
        // by GCing on channel creation we limit the *growth* of wasted memory.
        self.gc();
        ensure!(
            self.senders.get(&key).is_none(),
            "Key {:?} is already registered",
            key
        );
        let (sender, receiver) = mpsc::unbounded();
        self.senders.insert(key.clone(), sender);
        self.receivers.insert(key, receiver);
        Ok(())
    }

    /// Takes (and consumes) the receiver for the specified key
    pub fn receiver(&mut self, key: &K) -> Result<UnboundedReceiver<T>, failure::Error> {
        self.receivers
            .remove(key)
            .ok_or_else(|| format_err!("Key {:?} is not registered", key))
    }

    /// Gets a sender for the specified key.
    pub fn sender(&self, key: &K) -> Result<&UnboundedSender<T>, failure::Error> {
        self.senders
            .get(key)
            .ok_or_else(|| format_err!("Key {:?} is not registered", key))
    }

    /// Returns an iterator over all the senders in the mux.
    pub fn senders(&self) -> impl Iterator<Item = (&K, &UnboundedSender<T>)> {
        self.senders.iter()
    }

    /// Closes the sender for the specified key. It is not an error if no
    /// such key exists.
    ///
    /// This is not necessary in general, as the sender will be automatically
    /// garbage collected when the receiver is dropped. It can be useful,
    /// however, to eagerly reclaim the key so that the key can be reused.
    pub fn close(&mut self, key: &K) {
        self.senders.remove(key);
    }

    /// Removes references to channels where the receiver has been closed or
    /// dropped.
    fn gc(&mut self) {
        self.senders.retain(|_, sender| !sender.is_closed())
    }
}
