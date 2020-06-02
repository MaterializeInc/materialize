// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use rand::distributions::Alphanumeric;
use rand::Rng;

/// Generates and returns bytes of length `len`.
pub fn generate_bytes(len: usize) -> Vec<u8> {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(|c| c as u8)
        .collect::<Vec<u8>>()
}
