// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Abstractions over files, cloud storage, etc used in persistence.

use std::ops::Range;

use abomonation_derive::Abomonation;

use crate::error::Error;

/// A "sequence number", uniquely associated with an entry in a Buffer.
#[derive(Clone, Copy, Debug, PartialOrd, Ord, PartialEq, Eq, Abomonation)]
pub struct SeqNo(pub u64);

impl timely::PartialOrder for SeqNo {
    fn less_equal(&self, other: &Self) -> bool {
        self <= other
    }
}

/// An abstraction over an append-only bytes log.
///
/// Each written entry is assigned a unique, incrementing SeqNo, which can be
/// later used when draining data back out of the buffer.
///
/// - Invariant: Implementations are responsible for ensuring that they are
///   exclusive writers to this location.
pub trait Buffer {
    /// Synchronously appends an entry.
    ///
    /// TODO: Figure out our async story so we can batch up multiple of these
    /// into one disk flush.
    fn write_sync(&mut self, buf: Vec<u8>) -> Result<SeqNo, Error>;

    /// Returns a consistent snapshot of all written but not yet truncated
    /// entries.
    ///
    /// - Invariant: all returned entries must have a sequence number within
    ///   the declared [lower, upper) range of sequence numbers.
    fn snapshot<F>(&self, logic: F) -> Result<Range<SeqNo>, Error>
    where
        F: FnMut(SeqNo, &[u8]) -> Result<(), Error>;

    /// Removes all entries with a SeqNo strictly less than the given upper
    /// bound.
    fn truncate(&mut self, upper: SeqNo) -> Result<(), Error>;

    /// Synchronously closes the buffer, releasing exclusive-writer locks and
    /// causing all future commands to error.
    ///
    /// Implementations must be idempotent. Returns true if the buffer had not
    /// previously been closed.
    fn close(&mut self) -> Result<bool, Error>;
}

/// An abstraction over a `bytes key`->`bytes value` store.
///
/// - Invariant: Implementations are responsible for ensuring that they are
///   exclusive writers to this location.
pub trait Blob {
    /// Returns a reference to the value corresponding to the key.
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error>;

    /// Inserts a key-value pair into the map.
    fn set(&mut self, key: &str, value: Vec<u8>, allow_overwrite: bool) -> Result<(), Error>;

    /// Synchronously closes the buffer, releasing exclusive-writer locks and
    /// causing all future commands to error.
    ///
    /// Implementations must be idempotent. Returns true if the buffer had not
    /// previously been closed.
    fn close(&mut self) -> Result<bool, Error>;
}

#[cfg(test)]
pub mod tests {
    use std::ops::RangeInclusive;

    use crate::error::Error;
    use crate::storage::Blob;
    use crate::storage::Buffer;
    use crate::storage::SeqNo;

    fn slurp<U: Buffer>(buf: &U) -> Result<Vec<Vec<u8>>, Error> {
        let mut entries = Vec::new();
        buf.snapshot(|_, x| {
            entries.push(x.to_vec());
            Ok(())
        })?;
        Ok(entries)
    }

    pub fn buffer_impl_test<U: Buffer, F: FnMut(&str) -> Result<U, Error>>(
        mut new_fn: F,
    ) -> Result<(), Error> {
        let entries = vec![
            "entry0".as_bytes().to_vec(),
            "entry1".as_bytes().to_vec(),
            "entry2".as_bytes().to_vec(),
            "entry3".as_bytes().to_vec(),
            "entry4".as_bytes().to_vec(),
        ];
        let sub_entries =
            |r: RangeInclusive<usize>| -> Vec<Vec<u8>> { entries[r].iter().cloned().collect() };

        let mut buf0 = new_fn("0")?;

        // We can create a second buffer writing to a different place.
        let _ = new_fn("1")?;

        // But the buffer impl prevents us from opening the same place for
        // writing twice.
        assert!(new_fn("0").is_err());

        // Empty writer is empty.
        assert!(slurp(&buf0)?.is_empty());

        // First write is assigned SeqNo(0).
        assert_eq!(buf0.write_sync(entries[0].clone())?, SeqNo(0));
        assert_eq!(slurp(&buf0)?, sub_entries(0..=0));

        // Second write is assigned SeqNo(1). Now contains 2 entries.
        assert_eq!(buf0.write_sync(entries[1].clone())?, SeqNo(1));
        assert_eq!(slurp(&buf0)?, sub_entries(0..=1));

        // Truncate removes the first entry.
        buf0.truncate(SeqNo(1))?;
        assert_eq!(slurp(&buf0)?, sub_entries(1..=1));

        // We are not allowed to truncate to places outside the current range.
        assert!(buf0.truncate(SeqNo(0)).is_err());
        assert!(buf0.truncate(SeqNo(3)).is_err());

        // Write works after a truncate has happened.
        assert_eq!(buf0.write_sync(entries[2].clone())?, SeqNo(2));
        assert_eq!(slurp(&buf0)?, sub_entries(1..=2));

        // Truncate everything.
        buf0.truncate(SeqNo(3))?;
        assert!(slurp(&buf0)?.is_empty());

        // Cannot reuse a buffer once it is closed.
        assert_eq!(buf0.close(), Ok(true));
        assert!(buf0.write_sync(entries[1].clone()).is_err());
        assert!(slurp(&buf0).is_err());
        assert!(buf0.truncate(SeqNo(4)).is_err());

        // Close must be idempotent and must return false if it did no work.
        assert_eq!(buf0.close(), Ok(false));

        // But we can reopen it and use it.
        let mut buf0 = new_fn("0")?;
        assert_eq!(buf0.write_sync(entries[3].clone())?, SeqNo(3));
        assert_eq!(slurp(&buf0)?, sub_entries(3..=3));
        assert_eq!(buf0.close(), Ok(true));
        let mut buf0 = new_fn("0")?;
        assert_eq!(buf0.write_sync(entries[4].clone())?, SeqNo(4));
        assert_eq!(slurp(&buf0)?, sub_entries(3..=4));
        assert_eq!(buf0.close(), Ok(true));

        Ok(())
    }

    pub fn blob_impl_test<L: Blob, F: FnMut(&str) -> Result<L, Error>>(
        mut new_fn: F,
    ) -> Result<(), Error> {
        let values = vec!["v0".as_bytes().to_vec(), "v1".as_bytes().to_vec()];
        // let sub_entries =
        //     |r: RangeInclusive<usize>| -> Vec<Vec<u8>> { entries[r].iter().cloned().collect() };

        let mut blob0 = new_fn("0")?;

        // We can create a second blob writing to a different place.
        let _ = new_fn("1")?;

        // But the blob impl prevents us from opening the same place for
        // writing twice.
        assert!(new_fn("0").is_err());

        // Empty key is empty.
        assert_eq!(blob0.get("k0")?, None);

        // Set a key and get it back.
        blob0.set("k0", values[0].clone(), false)?;
        assert_eq!(blob0.get("k0")?, Some(values[0].clone()));

        // Can only overwrite a key without allow_overwrite.
        assert!(blob0.set("k0", values[1].clone(), false).is_err());
        assert_eq!(blob0.get("k0")?, Some(values[0].clone()));
        blob0.set("k0", values[1].clone(), true)?;
        assert_eq!(blob0.get("k0")?, Some(values[1].clone()));

        // Cannot reuse a blob once it is closed.
        assert_eq!(blob0.close(), Ok(true));
        assert!(blob0.get("k0").is_err());
        assert!(blob0.set("k1", values[0].clone(), true).is_err());

        // Close must be idempotent and must return false if it did no work.
        assert_eq!(blob0.close(), Ok(false));

        // But we can reopen it and use it.
        let blob0 = new_fn("0")?;
        assert_eq!(blob0.get("k0")?, Some(values[1].clone()));

        Ok(())
    }
}
