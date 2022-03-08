// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A Timely Dataflow operator that turns a stream of keyed upserts into a stream of differential updates.

use std::fmt::Debug;

use crate::client::{StreamReadHandle, StreamWriteHandle};

use mz_persist_types::Codec;

/// Persist configuration for persistent upsert.
#[derive(Debug, Clone)]
pub struct PersistentUpsertConfig<K: Codec, V: Codec> {
    /// The timestamp up to which which data should be read when restoring.
    pub upper_seal_ts: u64,

    /// [`StreamReadHandle`] for the collection that we should persist to.
    pub read_handle: StreamReadHandle<K, V>,

    /// [`StreamWriteHandle`] for the collection that we should persist to.
    pub write_handle: StreamWriteHandle<K, V>,
}

impl<K: Codec, V: Codec> PersistentUpsertConfig<K, V> {
    /// Creates a new [`PersistentUpsertConfig`] from the given parts.
    pub fn new(
        upper_seal_ts: u64,
        read_handle: StreamReadHandle<K, V>,
        write_handle: StreamWriteHandle<K, V>,
    ) -> Self {
        PersistentUpsertConfig {
            upper_seal_ts,
            read_handle,
            write_handle,
        }
    }
}
