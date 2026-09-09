// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{Manifest, RowHandle, StoreError};

/// Operator-defined index fields with an optional payload locator.
#[derive(Clone, Debug)]
pub struct Record<K, M> {
    /// The exact grouping key. Hashes alone require collision resolution upstream.
    pub key: K,
    /// Operator-defined metadata, such as source order or `(time, diff)`.
    pub metadata: M,
    /// `None` can represent an operator-defined deletion or payload-free record.
    pub row: Option<RowHandle>,
}

/// A resident sorted index owning its independently stored payload blocks.
pub struct Batch<K, M> {
    records: Vec<Record<K, M>>,
    payloads: Manifest,
}

impl<K: Ord, M> Batch<K, M> {
    /// Sort records by exact key and retain their payloads before releasing `owners`.
    pub fn new(records: Vec<Record<K, M>>, owners: &[&Manifest]) -> Result<Self, StoreError> {
        let payloads = Manifest::retain(
            records.iter().filter_map(|record| record.row),
            owners.iter().copied(),
        )?;
        let mut records = records;
        records.sort_by(|left, right| left.key.cmp(&right.key));
        Ok(Self { records, payloads })
    }

    /// Access the index without fetching payload bytes.
    pub fn records(&self) -> &[Record<K, M>] {
        &self.records
    }

    /// Payload ownership used to prepare reads or construct another batch.
    pub fn payloads(&self) -> &Manifest {
        &self.payloads
    }
}

impl<K: Ord + Clone, M: Clone> Batch<K, M> {
    /// Publish a selection with independent ownership and no payload copying.
    pub fn select(&self, indices: impl IntoIterator<Item = usize>) -> Result<Self, StoreError> {
        let records = indices
            .into_iter()
            .map(|index| {
                self.records
                    .get(index)
                    .cloned()
                    .ok_or(StoreError::InvalidIndex)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Self::new(records, &[&self.payloads])
    }
}
