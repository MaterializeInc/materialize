// Copyright 2019 Timely Data, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Timely Data, Inc.

//! The differential dataflow driver.
//!
//! This module is very much a work in progress. Don't look too closely yet.

mod render;
pub mod server;
mod source;
mod trace;
mod types;

pub use types::*;
