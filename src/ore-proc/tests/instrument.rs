// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_ore_proc::instrument;

#[instrument(name = "my_span", level = "trace", ret, fields(next = 1, shard = %"abc"))]
fn test_instrument() {}

#[instrument]
fn test_instrument_skipall() {}

#[instrument]
async fn test_instrument_async() {}
