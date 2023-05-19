// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Reclocking compatibility code until the whole ingestion pipeline is transformed to native
//! timestamps

use mz_persist_client::error::UpperMismatch;
use mz_repr::Diff;
use timely::progress::frontier::Antichain;
use timely::progress::Timestamp;

/// A handle that can produce the data expressing the translation of FromTime to
/// IntoTime.
///
/// This trait is a subtrait of `RemapHandle` so it can be shared between the
/// primary reclocking implementation and remap collection migrations.
#[async_trait::async_trait(?Send)]
pub trait RemapHandleReader {
    type FromTime: Timestamp;
    type IntoTime: Timestamp;

    /// Produces the next batch of data contained in the remap collection and the upper frontier of
    /// that batch. The return batch should contain all the updates that happened at times not
    /// beyond ther returned upper.
    async fn next(
        &mut self,
    ) -> Option<(
        Vec<(Self::FromTime, Self::IntoTime, Diff)>,
        Antichain<Self::IntoTime>,
    )>;
}

/// A handle that can be used to durably persist a remap collection translating FromTime to
/// IntoTime.
#[async_trait::async_trait(?Send)]
pub trait RemapHandle: RemapHandleReader {
    /// Attempt to write the batch of remap collection updates to the collection. If the remap
    /// collection was already written by some other process an error will return with the current
    /// upper.
    async fn compare_and_append(
        &mut self,
        updates: Vec<(Self::FromTime, Self::IntoTime, Diff)>,
        upper: Antichain<Self::IntoTime>,
        new_upper: Antichain<Self::IntoTime>,
    ) -> Result<(), UpperMismatch<Self::IntoTime>>;

    fn upper(&self) -> &Antichain<Self::IntoTime>;
}
