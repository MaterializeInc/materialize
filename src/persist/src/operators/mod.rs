// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Timely and Differential Dataflow operators for persisting and replaying
//! data.

pub mod input;
pub mod replay;
pub mod source;
pub mod stream;
pub mod upsert;

// This was selected to match the heuristic in differential dataflow's join.
//
// https://docs.rs/differential-dataflow/0.12.0/src/differential_dataflow/operators/join.rs.html#487-488
const DEFAULT_OUTPUTS_PER_YIELD: usize = 1_000_000;

pub(crate) fn split_ok_err<K, V>(
    x: (Result<(K, V), String>, u64, isize),
) -> Result<((K, V), u64, isize), (String, u64, isize)> {
    match x {
        (Ok(kv), ts, diff) => Ok((kv, ts, diff)),
        (Err(err), ts, diff) => Err((err, ts, diff)),
    }
}
