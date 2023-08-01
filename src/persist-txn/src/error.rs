// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Errors for the crate

use mz_persist_client::ShardId;

/// The data shard was not registered.
#[derive(Debug)]
pub struct NotRegistered {
    /// The data shard that was not registered.
    pub data_id: ShardId,
    /// The exclusive txns shard time at which it was not registered.
    pub ts: u64,
}
