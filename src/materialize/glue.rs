// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Types and data-structures used to glue all the various components of materialize together

use crate::clock::Timestamp;
use crate::dataflow::{Dataflow, View};
use crate::repr::{Datum, RelationType};
use failure::{ensure, format_err};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::{Arc, RwLock};

pub use uuid::Uuid;

// These work in both async and sync settings, so prefer them over std::sync::mpsc
// (For sync settings, use `sender.unbounded_send`, `receiver.try_next` and `receiver.wait`)
pub use futures::sync::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};

/// Various metadata that gets attached to commands at all stages
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommandMeta {
    /// The pgwire connection on which this command originated
    pub connection_uuid: Uuid,
    /// The time this command was inserted in the command queue
    pub timestamp: Option<Timestamp>,
}

/// Incoming sql from users
pub type SqlCommand = String;

/// Responses from the planner to sql commands
#[derive(Debug)]
pub enum SqlResponse {
    CreatedDataSink,
    CreatedDataSource,
    CreatedView,
    CreatedTable,
    DroppedDataSource,
    DroppedView,
    DroppedTable,
    EmptyQuery,
    Inserted(usize),
    Peeking { typ: RelationType },
}

pub type SqlResponseMux = Arc<RwLock<Mux<Uuid, Result<SqlResponse, failure::Error>>>>;

/// The commands that a running dataflow server can accept.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DataflowCommand {
    CreateDataflow(Dataflow),
    DropDataflows(Vec<Dataflow>),
    PeekExisting(Dataflow),
    PeekTransient(View),
    Tail(String),
    Insert(String, Vec<(Vec<Datum>, Timestamp, isize)>),
    Shutdown,
}

pub type PeekResults = Vec<Vec<Datum>>;
pub type PeekResultsMux = Arc<RwLock<Mux<Uuid, PeekResults>>>;

/// A multiple-sender, multiple-receiver channel where receivers are keyed by K
#[derive(Debug)]
pub struct Mux<K, T>
where
    K: Hash + Eq,
{
    senders: HashMap<K, UnboundedSender<T>>,
}

impl<K, T> Default for Mux<K, T>
where
    K: Hash + Eq,
{
    fn default() -> Self {
        Mux {
            senders: Default::default(),
        }
    }
}

impl<K, T> Mux<K, T>
where
    K: Hash + Eq + Debug,
{
    /// Register a new channel for uuid
    pub fn channel(&mut self, key: K) -> Result<UnboundedReceiver<T>, failure::Error> {
        // We might hold onto closed senders for arbitrary amounts of time, but by gc-ing on channel creation we limit the *growth* of wasted memory
        self.gc();
        ensure!(
            self.senders.get(&key).is_none(),
            "Key {:?} is already registered",
            key
        );
        let (sender, receiver) = unbounded();
        self.senders.insert(key, sender);
        Ok(receiver)
    }

    /// Get a sender for uuid
    pub fn sender(&self, key: &K) -> Result<&UnboundedSender<T>, failure::Error> {
        self.senders
            .get(key)
            .ok_or_else(|| format_err!("Key {:?} is not registered", key))
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

    /// Remove references to channels where the receiver has been closed or dropped
    fn gc(&mut self) {
        self.senders.retain(|_, sender| !sender.is_closed())
    }
}
