// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dataflow_types::Timestamp;
use std::cell::RefCell;
use std::rc::Rc;
use timely::dataflow::operators::Capability;
use timely::scheduling::Activator;

use crate::server::TimestampChanges;

mod file;
mod kafka;
mod kinesis;
mod util;

use expr::SourceInstanceId;
pub use file::{file, FileReadStyle};
pub use kafka::kafka;
pub use kinesis::kinesis;

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
