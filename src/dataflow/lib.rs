// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Driver for timely/differential dataflow.

// the prometheus macros (e.g. `register*`) all depend on each other, including on
// internal `__register*` macros, instead of doing the right thing and I assume using
// something like `$crate::__register_*`. That means that without using a macro_use here,
// we would end up needing to import several internal macros everywhere we want to use
// any of the prometheus macros.
#[macro_use]
extern crate prometheus;

mod arrangement;
mod decode;
mod render;
mod sink;
mod source;

pub mod logging;
pub mod server;

pub use server::{serve, BroadcastToken, SequencedCommand, WorkerFeedback, WorkerFeedbackWithMeta};
