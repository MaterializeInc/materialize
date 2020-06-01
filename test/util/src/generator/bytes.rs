// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;
use rand::distributions::Alphanumeric;
use rand::Rng;

const DEFAULT_LEN: usize = 30;

/// Generates and returns `n` number of bytes structs of
/// default length DEFAULT_LEN.
pub fn generate_bytes(n: u64) -> Vec<Bytes> {
    generate_bytes_of_len(n, DEFAULT_LEN)
}

/// Generates and returns `n` number of bytes structs, each of
/// length `len`.
pub fn generate_bytes_of_len(n: u64, len: usize) -> Vec<Bytes> {
    let mut bytes = Vec::with_capacity(n as usize);
    for _ in 0..n {
        bytes.push(Bytes::from(
            rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(len)
                .collect::<String>(),
        ));
    }
    bytes
}
