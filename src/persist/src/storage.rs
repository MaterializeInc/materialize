// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Abstractions over files, cloud storage, etc used in persistence.

use std::cell::RefCell;
use std::ops::Range;

use abomonation_derive::Abomonation;

use crate::error::Error;
use crate::mem::MemBuffer;

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
    /// - Invariant: all data needs to be shown in order.
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

/// An abstraction over a reader of a shared (SPSC, within the same process)
/// append-only bytes log that may be persisted to disk.
pub trait BufferRead {
    /// Returns a consistent snapshot of all written but not yet truncated
    /// entries.
    ///
    /// - Invariant: all returned entries must have a sequence number within
    ///   the declared [lower, upper) range of sequence numbers.
    /// - Invariant: all data needs to be shown in order.
    fn snapshot<F>(&self, logic: F) -> Result<Range<SeqNo>, Error>
    where
        F: FnMut(SeqNo, &[u8]) -> Result<(), Error>;

    /// Removes all entries with a SeqNo strictly less than the given upper
    /// bound.
    fn allow_truncation(&mut self, upper: SeqNo) -> Result<(), Error>;

    /// Synchronously closes the buffer reader, causing all future commands to error.
    ///
    /// Implementations must be idempotent. Returns true if the buffer had not
    /// previously been closed.
    fn close(&mut self) -> Result<bool, Error>;
}

/// An abstraction over a writer of a shared (SPSC, within the same process)
/// append-only bytes log that may be persisted to disk.
pub trait BufferWrite {
    /// Synchronously appends an entry.
    ///
    /// TODO: Figure out our async story so we can batch up multiple of these
    /// into one disk flush.
    fn write_sync(&mut self, buf: Vec<u8>) -> Result<SeqNo, Error>;

    /// Removes all entries with a SeqNo strictly less than an upper bound provided
    /// by the reader.
    /// TODO: this may need to return the truncation bound.
    fn truncate(&mut self) -> Result<(), Error>;

    /// Synchronously closes the buffer, releasing exclusive-writer locks and
    /// causing all future commands to error.
    ///
    /// Implementations must be idempotent. Returns true if the buffer had not
    /// previously been closed.
    fn close(&mut self) -> Result<bool, Error>;
}

/// Extension trait to share access to a Buffer.
pub trait BufferShared {
    /// The type of reader returned by [Self::read_write].
    type Reader: BufferRead;

    /// The type of writer returned by [Self::read_write].
    type Writer: BufferWrite;

    /// Return handles to a reader and a writer for the buffer.
    fn read_write(self) -> Result<(Self::Reader, Self::Writer), Error>;
}

impl<U: Buffer + Sized> BufferShared for U {
    type Reader = BufferReadHandle;

    type Writer = BufferHandle<U>;

    /// Create two separate reader-writer handles to share access to an underlying
    /// Buffer.
    fn read_write(self) -> Result<(Self::Reader, Self::Writer), Error> {
        // TODO: Is an unbounded channel the right thing to do here?
        let (data_tx, data_rx) = crossbeam_channel::unbounded();
        let (truncate_tx, truncate_rx) = crossbeam_channel::unbounded();
        // WIP: TODO: is there a better name?
        let mem = MemBuffer::new("buffer_read_handle");

        let buffer_handle = BufferHandle {
            inner: self,
            tx: data_tx,
            rx: truncate_rx,
        };
        let buffer_read_handle = BufferReadHandle {
            rx: data_rx,
            inner: RefCell::new(mem),
            tx: truncate_tx,
        };

        buffer_handle.inner.snapshot(|_, data| {
            if let Err(crossbeam_channel::SendError(_)) = buffer_handle.tx.send(data.to_vec()) {
                return Err(Error::from("initializing read handle failed"));
            }

            Ok(())
        })?;
        Ok((buffer_read_handle, buffer_handle))
    }
}

/// Struct to share Buffer-ed data between a writer and a reader.
/// This is the writer half, and it takes care of persisting data and sending it
/// over to the reader.
pub struct BufferHandle<U: Buffer + Sized> {
    // The underlying Buffer, used to persist writes.
    inner: U,
    // Send persisted buffer writes to the reader.
    tx: crossbeam_channel::Sender<Vec<u8>>,
    // Reader can tell us what sequence numbers are safe to compact up to.
    rx: crossbeam_channel::Receiver<SeqNo>,
}

/// Reader of Buffer-ed data shared between two threads.
/// This is the reader-half and it actually consumes the data and notifies the
/// writer about truncations.
pub struct BufferReadHandle {
    // Data from the Buffer writer.
    rx: crossbeam_channel::Receiver<Vec<u8>>,
    // Store data in a MemBuffer so that users of this handle can continue to use the
    // Buffer API
    inner: RefCell<MemBuffer>,
    // Channel to send updates about what sequence numbers can be compacted up to.
    tx: crossbeam_channel::Sender<SeqNo>,
}

impl<U: Buffer + Sized> BufferWrite for BufferHandle<U> {
    fn write_sync(&mut self, buf: Vec<u8>) -> Result<SeqNo, Error> {
        let seqno = self.inner.write_sync(buf.clone())?;
        if let Err(crossbeam_channel::SendError(_)) = self.tx.send(buf) {
            Err(Error::from("write_sync failed because receiver shut down"))
        } else {
            Ok(seqno)
        }
    }

    fn truncate(&mut self) -> Result<(), Error> {
        for upper in self.rx.try_iter() {
            self.inner.truncate(upper)?;
        }

        Ok(())
    }

    fn close(&mut self) -> Result<bool, Error> {
        // TODO: not sure if I should do something else with the channels.
        self.inner.close()
    }
}

impl BufferRead for BufferReadHandle {
    fn snapshot<F>(&self, logic: F) -> Result<Range<SeqNo>, Error>
    where
        F: FnMut(SeqNo, &[u8]) -> Result<(), Error>,
    {
        for buf in self.rx.try_iter() {
            self.inner.borrow_mut().write_sync(buf)?;
        }

        self.inner.borrow().snapshot(logic)
    }

    fn allow_truncation(&mut self, upper: SeqNo) -> Result<(), Error> {
        self.inner.borrow_mut().truncate(upper)?;
        if let Err(crossbeam_channel::SendError(_)) = self.tx.send(upper) {
            return Err(Error::from("truncate failed because receiver shut down"));
        }

        Ok(())
    }

    fn close(&mut self) -> Result<bool, Error> {
        // TODO: not sure if I need to do anything else with the channels.
        self.inner.borrow_mut().close()
    }
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
    use crate::storage::SeqNo;
    use crate::storage::{Buffer, BufferRead, BufferShared, BufferWrite};

    fn slurp<U: Buffer>(buf: &U) -> Result<Vec<Vec<u8>>, Error> {
        let mut entries = Vec::new();
        buf.snapshot(|_, x| {
            entries.push(x.to_vec());
            Ok(())
        })?;
        Ok(entries)
    }

    fn slurp_reader<U: BufferRead>(buf: &U) -> Result<Vec<Vec<u8>>, Error> {
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

    pub fn buffer_shared_test<U: Buffer, F: FnMut(&str) -> Result<U, Error>>(
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

        let buf0 = new_fn("0")?;

        let (mut reader, mut writer) = buf0.read_write()?;
        // First write is assigned SeqNo(0).
        assert_eq!(writer.write_sync(entries[0].clone())?, SeqNo(0));
        assert_eq!(slurp_reader(&reader)?, sub_entries(0..=0));
        // Second write is assigned SeqNo(1). Now contains 2 entries.
        assert_eq!(writer.write_sync(entries[1].clone())?, SeqNo(1));
        assert_eq!(slurp_reader(&reader)?, sub_entries(0..=1));

        // Truncate removes the first entry after we allow the truncation to
        // happen and wait for it to be done
        reader.allow_truncation(SeqNo(1))?;
        // The reader observes the truncation immediately.
        assert_eq!(slurp_reader(&reader)?, sub_entries(1..=1));
        // The writer physically frees the data after a call to `truncate`.
        writer.truncate()?;
        // Consecutive calls to truncate are a no-op
        writer.truncate()?;

        // We are not allowed to truncate to places outside the current range.
        assert!(reader.allow_truncation(SeqNo(0)).is_err());
        assert!(reader.allow_truncation(SeqNo(3)).is_err());

        // Write works after a truncate has happened.
        assert_eq!(writer.write_sync(entries[2].clone())?, SeqNo(2));
        assert_eq!(slurp_reader(&reader)?, sub_entries(1..=2));

        // Truncate everything.
        reader.allow_truncation(SeqNo(3))?;
        assert!(slurp_reader(&reader)?.is_empty());
        writer.truncate()?;

        // Perform one last write before close
        assert_eq!(writer.write_sync(entries[3].clone())?, SeqNo(3));
        assert_eq!(slurp_reader(&reader)?, sub_entries(3..=3));

        // Cannot reuse a buffer reader or writer once it is closed.
        assert_eq!(writer.close(), Ok(true));
        assert!(writer.write_sync(entries[1].clone()).is_err());
        reader.allow_truncation(SeqNo(4))?;
        assert!(writer.truncate().is_err());
        assert_eq!(reader.close(), Ok(true));
        assert!(slurp_reader(&reader).is_err());
        assert!(reader.allow_truncation(SeqNo(3)).is_err());

        // Close must be idempotent and must return false if it did no work.
        assert_eq!(writer.close(), Ok(false));
        assert_eq!(reader.close(), Ok(false));

        // But we can reopen it and use it.
        let buf0 = new_fn("0")?;
        let (mut reader, mut writer) = buf0.read_write()?;

        assert_eq!(writer.write_sync(entries[4].clone())?, SeqNo(4));
        assert_eq!(slurp_reader(&reader)?, sub_entries(3..=4));
        assert_eq!(writer.close(), Ok(true));
        assert_eq!(reader.close(), Ok(true));

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
