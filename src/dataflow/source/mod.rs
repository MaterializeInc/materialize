// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types related to the creation of dataflow sources.

use std::cell::RefCell;
use std::rc::Rc;

use serde::{Deserialize, Serialize};
use timely::dataflow::{
    channels::pact::{Exchange, ParallelizationContract},
    operators::Capability,
};
use timely::{scheduling::Activator, Data};

use dataflow_types::{Consistency, Timestamp};
use expr::SourceInstanceId;

use crate::server::{TimestampChanges, TimestampHistories};

mod file;
mod kafka;
mod kinesis;
mod util;

use differential_dataflow::Hashable;
pub use file::{file, read_file_task, FileReadStyle};
pub use kafka::kafka;
pub use kinesis::kinesis;

/// Shared configuration information for all source types.
pub struct SourceConfig<'a, G> {
    /// The name to attach to the underlying timely operator.
    pub name: String,
    /// The ID of this instantiation of this source.
    pub id: SourceInstanceId,
    /// The timely scope in which to build the source.
    pub scope: &'a G,
    /// Whether this worker has been chosen to actually receive data. All
    /// workers must build the same dataflow operators to keep timely channel
    /// IDs in sync, but only one worker will receive the data, to avoid
    /// duplicates.
    pub active: bool,
    // Timestamping fields.
    // TODO: document these.
    /// TODO(ncrooks)
    pub timestamp_histories: TimestampHistories,
    /// TODO(ncrooks)
    pub timestamp_tx: TimestampChanges,
    /// TODO(ncrooks)
    pub consistency: Consistency,
}

#[derive(Clone, Serialize, Deserialize)]
/// A record produced by a source
pub struct SourceOutput<K, V>
where
    K: Data,
    V: Data,
{
    /// The record's key (or some empty/default value for sources without the concept of key)
    pub key: K,
    /// The record's value
    pub value: V,
    /// The position in the source, if such a concept exists (e.g., Kafka offset, file line number)
    pub position: Option<i64>,
}

impl<K, V> SourceOutput<K, V>
where
    K: Data,
    V: Data,
{
    /// Build a new SourceOutput
    pub fn new(key: K, value: V, position: Option<i64>) -> SourceOutput<K, V> {
        SourceOutput {
            key,
            value,
            position,
        }
    }
}
impl<K, V> SourceOutput<K, V>
where
    K: Data + Serialize + for<'a> Deserialize<'a> + Send + Sync,
    V: Data + Serialize + for<'a> Deserialize<'a> + Send + Sync,
{
    /// A parallelization contract that hashes by keys
    pub fn key_contract() -> impl ParallelizationContract<Timestamp, Self>
    where
        K: Hashable<Output = u64>,
    {
        Exchange::new(|x: &Self| x.key.hashed())
    }
    /// A parallelization contract that hashes by values
    pub fn value_contract() -> impl ParallelizationContract<Timestamp, Self>
    where
        V: Hashable<Output = u64>,
    {
        Exchange::new(|x: &Self| x.value.hashed())
    }
}

/// A `SourceToken` manages interest in a source.
///
/// When the `SourceToken` is dropped the associated source will be stopped.
pub struct SourceToken {
    id: SourceInstanceId,
    capability: Rc<RefCell<Option<Capability<Timestamp>>>>,
    activator: Activator,
    timestamp_drop: Option<TimestampChanges>,
}

impl SourceToken {
    /// Re-activates the associated timely source operator.
    pub fn activate(&self) {
        self.activator.activate();
    }
}

impl Drop for SourceToken {
    fn drop(&mut self) {
        *self.capability.borrow_mut() = None;
        self.activator.activate();
        if self.timestamp_drop.is_some() {
            self.timestamp_drop
                .as_ref()
                .unwrap()
                .borrow_mut()
                .push((self.id, None));
        }
    }
}

/// The status of a source.
pub enum SourceStatus {
    /// The source is still alive.
    Alive,
    /// The source is complete.
    Done,
}
