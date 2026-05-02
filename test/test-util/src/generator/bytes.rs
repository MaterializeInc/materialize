// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use rand::Rng;
use rand::distr::Alphanumeric;

/// Generates and returns bytes of length `len`.
pub fn generate_bytes(len: usize) -> Vec<u8> {
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .collect::<Vec<u8>>()
}
