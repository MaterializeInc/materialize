// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use dataflow_types::Timestamp;
use std::cell::RefCell;
use std::rc::Rc;
use timely::dataflow::operators::Capability;
use timely::scheduling::Activator;

mod file;
mod kafka;
mod util;

pub use file::{file, FileReadStyle};
pub use kafka::kafka;

// A `SourceToken` indicates interest in a source. When the `SourceToken` is
// dropped, its associated source will be stopped.
pub struct SourceToken {
    capability: Rc<RefCell<Option<Capability<Timestamp>>>>,
    activator: Activator,
}

impl Drop for SourceToken {
    fn drop(&mut self) {
        *self.capability.borrow_mut() = None;
        self.activator.activate();
    }
}

pub enum SourceStatus {
    Alive,
    Done,
}
