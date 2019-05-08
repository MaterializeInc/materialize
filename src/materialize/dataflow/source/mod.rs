// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

mod kafka;
mod local;

pub use kafka::kafka;
pub use local::{local, InsertMux};
