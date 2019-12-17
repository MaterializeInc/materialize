// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use dataflow_types::Timestamp;
use std::cell::RefCell;
use std::rc::Rc;
use timely::dataflow::operators::Capability;

mod file;
mod kafka;
mod util;

pub use file::{file, FileReadStyle};
pub use kafka::kafka;

pub type SharedCapability = Rc<RefCell<Option<Capability<Timestamp>>>>;

pub enum SourceStatus {
    ScheduleAgain,
    Done,
}
