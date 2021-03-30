// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Prometheus metrics for decode

use lazy_static::lazy_static;
use prometheus::{register_uint_gauge_vec, UIntGaugeVec};

lazy_static! {
    pub(crate) static ref DEBEZIUM_UPSERT_COUNT: UIntGaugeVec = register_uint_gauge_vec!(
        "mz_source_debezium_upsert_state_size",
        "The number of keys that we are tracking in an upsert map.",
        &["source_id", "worker_id"]
    )
    .unwrap();
}
