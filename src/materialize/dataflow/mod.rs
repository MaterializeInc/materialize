// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Driver for timely/differential dataflow.
//!
//! This module is very much a work in progress. Don't look too closely yet.

pub mod func;
mod render;
pub mod server;
mod source;
mod trace;
mod types;

pub use server::{serve, PeekResultsHandler};
pub use types::*;
