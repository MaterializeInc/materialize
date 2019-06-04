// Copyright 2019 Materialize, Inc. All rights reserved.

//! A SQL stream processor built on top of [timely dataflow] and
//! [differential dataflow].
//!
//! The main entry point is the [`server`] module. Other modules are exported
//! for documentation purposes only, i.e., they were not built with the goal
//! of being useful outside of this crate.
//!
//! [differential dataflow]: ../differential_dataflow/index.html
//! [timely dataflow]: ../timely/index.html

pub mod clock;
pub mod dataflow;
pub mod glue;
pub mod interchange;
pub mod pgwire;
pub mod queue;
pub mod repr;
pub mod server;
pub mod sql;
