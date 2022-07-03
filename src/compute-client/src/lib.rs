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

use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::ord::{OrdKeySpine, OrdValSpine};

use mz_repr::{Diff, Row, Timestamp};
use mz_storage::client::errors::DataflowError;

pub mod client;
pub mod logging;
pub mod plan;
pub mod reconciliation;

mod explain;
mod types;

pub use explain::DataflowGraphFormatter;
pub use explain::Explanation;
pub use explain::JsonViewFormatter;
pub use explain::TimestampExplanation;
pub use explain::TimestampSource;
pub use plan::Plan;
pub use types::*;

pub type RowSpine<K, V, T, R, O = usize> = OrdValSpine<K, V, T, R, O>;
pub type ErrSpine<K, T, R, O = usize> = OrdKeySpine<K, T, R, O>;

pub type TraceRowHandle<K, V, T, R> = TraceAgent<RowSpine<K, V, T, R>>;
pub type TraceErrHandle<K, T, R> = TraceAgent<ErrSpine<K, T, R>>;
pub type KeysValsHandle = TraceRowHandle<Row, Row, Timestamp, Diff>;
pub type ErrsHandle = TraceErrHandle<DataflowError, Timestamp, Diff>;
