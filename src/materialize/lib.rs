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

pub mod pgwire;
pub mod server;

// the prometheus macros (e.g. `register*`) all depend on each other, including on
// internal `__register*` macros, instead of doing the right thing and I assume using
// something like `$crate::__register_*`. That means that without using a macro_use here,
// we would end up needing to import several internal macros everywhere we want to use
// any of the prometheus macros.
#[macro_use]
extern crate prometheus;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;
