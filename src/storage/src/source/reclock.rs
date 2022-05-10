// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use timely::progress::{Antichain, Timestamp as _};
use timely::PartialOrder;

use mz_dataflow_types::client::controller::storage::CollectionMetadata;
use mz_dataflow_types::sources::MzOffset;
use mz_expr::PartitionId;
use mz_ore::now::NowFn;
use mz_persist_client::{
    read::{ListenEvent, ReadHandle},
    write::WriteHandle,
};
use mz_repr::{Diff, Timestamp};

pub struct Timestamper {
    /// The upper frontier of the remap collection
    upper: Antichain<Timestamp>,
    /// The state of the remap collection at self.upper
    remap_collection: HashMap<PartitionId, MzOffset>,
    /// A read handle to the remap collection
    read: ReadHandle<PartitionId, (), Timestamp, Diff>,
    /// A write handle to the remap collection
    write: WriteHandle<PartitionId, (), Timestamp, Diff>,
    /// The function to return a now time.
    now: NowFn,
}

impl Timestamper {
    /// Initialize a timestamper for a storage collection given its storage metadata and a
    /// timestamp to start as of.
    pub async fn new(
        storage_metadata: CollectionMetadata,
        as_of: Antichain<Timestamp>,
        now: NowFn,
    ) -> Self {
        // Here we initialize the timestamper by reading the data from the given persist shard as
        // of the time requested
        let persist_client = storage_metadata.persist_location.open().await.unwrap();
        let (write, read) = persist_client
            .open::<PartitionId, (), Timestamp, Diff>(storage_metadata.remap_shard)
            .await
            .unwrap();

        let mut remap_collection: HashMap<PartitionId, MzOffset> = HashMap::new();
        // If there is data in the collection we must first get a snapshot at the as_of frontier
        if !PartialOrder::less_equal(&Antichain::from_elem(Timestamp::minimum()), write.upper()) {
            let mut snapshot = read.snapshot(as_of.clone()).await.unwrap();
            while let Some(batch) = snapshot.next().await {
                for ((partition, _), _, diff) in batch {
                    let partition = partition.expect("decode error");
                    remap_collection.entry(partition).or_default().offset += diff;
                }
            }
        }
        Self {
            upper: write.upper().clone(),
            remap_collection,
            read,
            write,
            now,
        }
    }

    /// The current upper frontier of the timestamper
    pub fn upper(&self) -> Antichain<Timestamp> {
        self.upper.clone()
    }

    /// Produces a list of partition offset pairs where the offset is latest remapped offset is
    /// given.
    pub fn partition_cursors(&self) -> Vec<(PartitionId, MzOffset)> {
        self.remap_collection.clone().into_iter().collect()
    }

    /// Request for a timestamp to be assigned to the batch of partition and offset pairs. The
    /// caller promises that any subsequent call to this function will have greater than or equal
    /// offsets for all partitions that had been previously timestamped
    pub async fn timestamp_offsets(
        &mut self,
        mut observed_offsets: HashMap<PartitionId, MzOffset>,
    ) -> HashMap<PartitionId, (Timestamp, MzOffset)> {
        loop {
            let ts = (self.now)();

            let mut updates = vec![];
            // Mint new timestamp bindings and write them to persist
            for (partition, offset) in observed_offsets.drain() {
                let prev = self.remap_collection.insert(partition.clone(), offset);
                let diff = offset.offset - prev.unwrap_or_default().offset;
                if diff != 0 {
                    updates.push(((partition, ()), ts, diff));
                }
            }

            let new_upper = Antichain::from_elem(ts + 1);
            match self
                .write
                .compare_and_append(updates, self.upper(), new_upper.clone())
                .await
            {
                // Happy case, we won the compare and append
                Ok(Ok(Ok(_))) => {
                    self.upper = new_upper;
                    return HashMap::new();
                }
                // We lost the race with some other process, catch up and try again
                Ok(Ok(Err(actual_upper))) => {
                    let mut listen = self.read.listen(self.upper()).await.unwrap();
                    'catchup: loop {
                        for event in listen.next().await {
                            match event {
                                ListenEvent::Progress(cur_upper) => {
                                    if cur_upper == actual_upper.0 {
                                        // We're now up to date
                                        self.upper = actual_upper.0;
                                        break 'catchup;
                                    }
                                }
                                ListenEvent::Updates(bindings) => {
                                    for ((partition, _), _, diff) in bindings {
                                        let partition = partition.expect("decode error");
                                        self.remap_collection
                                            .entry(partition)
                                            .or_default()
                                            .offset += diff;
                                    }
                                }
                            }
                        }
                    }
                }
                _ => panic!(),
            }
        }
    }
}
