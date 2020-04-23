// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cell::RefCell;
use std::rc::Rc;

use timely::dataflow::operators::Capability;
use timely::scheduling::Activator;

use dataflow_types::{Consistency, Timestamp};
use expr::SourceInstanceId;

use crate::server::{TimestampChanges, TimestampHistories};

mod file;
mod kafka;
mod kinesis;
mod util;

pub use file::{file, read_file_task, FileReadStyle};
pub use kafka::kafka;
pub use kinesis::kinesis;

// Shared configuration information for all source types.
pub struct SourceConfig<'a, G> {
    /// The name to attach to the underlying timely operator.
    pub name: String,
    /// The ID
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
    pub timestamp_histories: TimestampHistories,
    pub timestamp_tx: TimestampChanges,
    pub consistency: Consistency,
}

// A `SourceToken` indicates interest in a source. When the `SourceToken` is
// dropped, its associated source will be stopped.
pub struct SourceToken {
    id: SourceInstanceId,
    capability: Rc<RefCell<Option<Capability<Timestamp>>>>,
    activator: Activator,
    timestamp_drop: Option<TimestampChanges>,
}

impl SourceToken {
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

pub enum SourceStatus {
    Alive,
    Done,
}
