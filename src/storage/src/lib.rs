// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Materialize's storage layer.

// The `fuzzing` feature re-exports internal upsert types (see `fuzz_exports`)
// that are intentionally undocumented. Don't require docs for them in that
// build. The normal public API is still linted.
#![cfg_attr(not(feature = "fuzzing"), warn(missing_docs))]

pub mod decode;
pub mod internal_control;
pub mod metrics;
pub mod render;
pub mod server;
pub mod sink;
pub mod source;
pub mod statistics;
pub mod storage_state;
pub(crate) mod upsert;
mod upsert_continual_feedback;
mod upsert_continual_feedback_v2;

/// Internal upsert types re-exported under `cfg(feature = "fuzzing")` so the
/// storage fuzz crate can drive the upsert state machine and value encodings
/// directly. The modules themselves stay crate-private; this facade exposes
/// only the items the fuzz targets need (mirroring `mz-persist-client` and
/// `mz-pgwire`). Not part of the public API.
#[cfg(feature = "fuzzing")]
pub mod fuzz_exports {
    pub use crate::upsert::types::{
        FuzzUpsertParts, StateValue, UpsertValueAndSize, upsert_bincode_opts,
    };
    pub use crate::upsert::{UpsertKey, UpsertValue, fuzz_drain_staged_input};
    pub use crate::upsert_continual_feedback_v2::{datum_seq_to_upsert_value, upsert_value_to_row};
}

pub(crate) mod healthcheck;

pub use server::serve;
