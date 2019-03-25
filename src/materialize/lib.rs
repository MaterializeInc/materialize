// Copyright 2019 Timely Data, Inc. All rights reserved.

//! A SQL stream processor built on top of [differential dataflow].
//!
//! The main entry point is the [`server`] module. Other modules are exported
//! for documentation purposes only, i.e., they were not built with the goal
//! of being useful outside of this crate.
//!
//! [differential dataflow]: ../differential_dataflow/

pub mod dataflow;
pub mod pgwire;
pub mod repr;
pub mod server;
pub mod sql;
