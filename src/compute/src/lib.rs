// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![warn(missing_docs)]

//! Materialize's compute layer.

pub mod memory_limiter;
pub mod server;

mod arrangement;
mod command_channel;
mod compute_state;
mod extensions;
mod logging;
mod metrics;
mod render;
/// Rendering internals exposed for benchmarks.
///
/// Not a supported API. The contents track whatever `benches/` needs, and the
/// modules behind them stay private so the crate's real surface does not grow.
#[cfg(feature = "bench")]
pub mod bench {
    pub use crate::render::RenderTimestamp;
    pub use crate::render::columnar::{
        CollectionEdge, ColumnarCollection, columnar_consolidate, columnar_negate, columnar_to_vec,
        concat_many, vec_to_columnar,
    };
}
/// MV sink machinery, exposed for benchmarks.
#[cfg(feature = "bench")]
pub mod sink;
#[cfg(not(feature = "bench"))]
mod sink;
mod typedefs;
