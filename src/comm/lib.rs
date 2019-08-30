// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Communication fabric that abstracts over thread/process boundaries.

pub mod mpsc;
pub mod protocol;
pub mod switchboard;

pub use switchboard::Switchboard;
