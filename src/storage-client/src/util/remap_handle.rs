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

use timely::progress::frontier::Antichain;
use timely::progress::Timestamp;

use mz_persist_client::Upper;
use mz_repr::Diff;

/// A handle that can be used to durably persist a remap collection translating FromTime to
/// IntoTime.
#[async_trait::async_trait(?Send)]
pub trait RemapHandle {
    type FromTime: Timestamp;
    type IntoTime: Timestamp;

    /// Attempt to write the batch of remap collection updates to the collection. If the remap
    /// collection was already written by some other process an error will return with the current
    /// upper.
    async fn compare_and_append(
        &mut self,
        updates: Vec<(Self::FromTime, Self::IntoTime, Diff)>,
        upper: Antichain<Self::IntoTime>,
        new_upper: Antichain<Self::IntoTime>,
    ) -> Result<(), Upper<Self::IntoTime>>;

    /// Produces the next batch of data contained in the remap collection and the upper frontier of
    /// that batch. The return batch should contain all the updates that happened at times not
    /// beyond ther returned upper.
    async fn next(
        &mut self,
    ) -> Option<(
        Vec<(Self::FromTime, Self::IntoTime, Diff)>,
        Antichain<Self::IntoTime>,
    )>;

    async fn compact(&mut self, since: Antichain<Self::IntoTime>);

    fn upper(&self) -> &Antichain<Self::IntoTime>;

    fn since(&self) -> &Antichain<Self::IntoTime>;
}
