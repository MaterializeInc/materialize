// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![warn(missing_docs)]

//! Driver for timely/differential dataflow.

use differential_dataflow::lattice::Lattice;
use differential_dataflow::{ExchangeData, Hashable};
use timely::dataflow::channels::pact::Exchange;

mod arrangement;
mod decode;
mod metrics;
mod operator;
mod render;
mod server;
mod sink;

pub mod logging;
pub mod source;

pub use render::plan::Plan;
pub use server::{serve, Command, Config, Response, TimestampBindingFeedback, WorkerFeedback};

/// The exchange parallelization contract for Materialize.
pub type MzExchange<D, F> = Exchange<D, F>;

/// The default exchange function to distribute data for arrangements.
pub fn arrange_exchange_fn<T, K, V, R>(update: &((K, V), T, R)) -> u64
where
    T: Lattice,
    K: Hashable,
    V: ExchangeData,
    R: ExchangeData,
{
    update.0 .0.hashed().into()
}
