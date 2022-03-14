// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The types for the dataflow crate.
//!
//! These are extracted into their own crate so that crates that only depend
//! on the interface of the dataflow crate, and not its implementation, can
//! avoid the dependency, as the dataflow crate is very slow to compile.

pub mod client;
pub mod logging;
pub mod plan;

mod errors;
mod explain;
mod gen;
mod types;

pub use errors::*;
pub use explain::DataflowGraphFormatter;
pub use explain::Explanation;
pub use explain::JsonViewFormatter;
pub use explain::TimestampExplanation;
pub use explain::TimestampSource;
pub use gen::*;
pub use plan::Plan;
pub use types::*;
