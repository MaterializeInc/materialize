// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Test utilities for injecting latency and errors.

use std::ops::Range;
use std::sync::{Arc, Mutex};

use crate::error::Error;
use crate::storage::{Blob, Buffer, SeqNo};

struct UnreliableCore {
    unavailable: bool,
    // TODO: Delays, what else?
}

/// A handle for controlling the behavior of an unreliable delegate.
#[derive(Clone)]
pub struct UnreliableHandle {
    core: Arc<Mutex<UnreliableCore>>,
}

impl Default for UnreliableHandle {
    fn default() -> Self {
        UnreliableHandle {
            core: Arc::new(Mutex::new(UnreliableCore { unavailable: false })),
        }
    }
}

impl UnreliableHandle {
    fn check_unavailable(&self, details: &str) -> Result<(), Error> {
        let unavailable = self
            .core
            .lock()
            .expect("never panics while holding lock")
            .unavailable;
        if unavailable {
            Err(format!("unavailable: {}", details).into())
        } else {
            Ok(())
        }
    }

    /// Cause all later operators to return an "unavailable" error.
    pub fn make_unavailable(&mut self) -> &mut UnreliableHandle {
        self.core
            .lock()
            .expect("never panics while holding lock")
            .unavailable = true;
        self
    }

    /// Cause all later operators to succeed.
    pub fn make_available(&mut self) -> &mut UnreliableHandle {
        self.core
            .lock()
            .expect("never panics while holding lock")
            .unavailable = false;
        self
    }
}

/// An unreliable delegate to [Buffer].
pub struct UnreliableBuffer<U> {
    handle: UnreliableHandle,
    buf: U,
}

impl<U: Buffer> UnreliableBuffer<U> {
    /// Returns a new [UnreliableBuffer] and a handle for controlling it.
    pub fn new(buf: U) -> (Self, UnreliableHandle) {
        let h = UnreliableHandle::default();
        let buf = Self::from_handle(buf, h.clone());
        (buf, h)
    }

    /// Returns a new [UnreliableBuffer] sharing the given handle.
    pub fn from_handle(buf: U, handle: UnreliableHandle) -> Self {
        UnreliableBuffer { handle, buf }
    }
}

impl<U: Buffer> Buffer for UnreliableBuffer<U> {
    fn write_sync(&mut self, buf: Vec<u8>) -> Result<SeqNo, Error> {
        self.handle.check_unavailable("buffer write")?;
        self.buf.write_sync(buf)
    }

    fn snapshot<F>(&self, logic: F) -> Result<Range<SeqNo>, Error>
    where
        F: FnMut(SeqNo, &[u8]) -> Result<(), Error>,
    {
        self.handle.check_unavailable("buffer snapshot")?;
        self.buf.snapshot(logic)
    }

    fn truncate(&mut self, upper: SeqNo) -> Result<(), Error> {
        self.handle.check_unavailable("buffer truncate")?;
        self.buf.truncate(upper)
    }

    fn close(&mut self) -> Result<bool, Error> {
        // TODO: This check_unavailable is a different order from the others
        // mostly for convenience in the nemesis tests. While we do want to
        // prevent a normal read/write from going though when the storage is
        // unavailable, it makes for a very uninteresting test if we can't clean
        // up LOCK files. OTOH this feels like a smell, revisit.
        let did_work = self.buf.close()?;
        self.handle.check_unavailable("buffer close")?;
        Ok(did_work)
    }
}

/// An unreliable delegate to [Blob].
pub struct UnreliableBlob<L> {
    handle: UnreliableHandle,
    blob: L,
}

impl<L: Blob> UnreliableBlob<L> {
    /// Returns a new [UnreliableBlob] and a handle for controlling it.
    pub fn new(blob: L) -> (Self, UnreliableHandle) {
        let h = UnreliableHandle::default();
        let blob = Self::from_handle(blob, h.clone());
        (blob, h)
    }

    /// Returns a new [UnreliableBuffer] sharing the given handle.
    pub fn from_handle(blob: L, handle: UnreliableHandle) -> Self {
        UnreliableBlob { handle, blob }
    }
}

impl<L: Blob> Blob for UnreliableBlob<L> {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        self.handle.check_unavailable("blob get")?;
        self.blob.get(key)
    }

    fn set(&mut self, key: &str, value: Vec<u8>, allow_overwrite: bool) -> Result<(), Error> {
        self.handle.check_unavailable("blob set")?;
        self.blob.set(key, value, allow_overwrite)
    }

    fn close(&mut self) -> Result<bool, Error> {
        // TODO: This check_unavailable is a different order from the others
        // mostly for convenience in the nemesis tests. While we do want to
        // prevent a normal read/write from going though when the storage is
        // unavailable, it makes for a very uninteresting test if we can't clean
        // up LOCK files. OTOH this feels like a smell, revisit.
        let did_work = self.blob.close()?;
        self.handle.check_unavailable("blob close")?;
        Ok(did_work)
    }
}

#[cfg(test)]
mod tests {
    use crate::mem::{MemBlob, MemBuffer};

    use super::*;

    #[test]
    fn buffer() {
        let (mut buffer, mut handle) = UnreliableBuffer::new(MemBuffer::new("unreliable"));

        // Initially starts reliable.
        assert!(buffer.write_sync(vec![]).is_ok());
        assert!(buffer.snapshot(|_, _| { Ok(()) }).is_ok());
        assert!(buffer.truncate(SeqNo(1)).is_ok());

        // Setting it to unavailable causes all operations to fail.
        handle.make_unavailable();
        assert!(buffer.write_sync(vec![]).is_err());
        assert!(buffer.snapshot(|_, _| { Ok(()) }).is_err());
        assert!(buffer.truncate(SeqNo(1)).is_err());

        // Can be set back to working.
        handle.make_available();
        assert!(buffer.write_sync(vec![]).is_ok());
        assert!(buffer.snapshot(|_, _| { Ok(()) }).is_ok());
        assert!(buffer.truncate(SeqNo(2)).is_ok());
    }

    #[test]
    fn blob() {
        let (mut blob, mut handle) = UnreliableBlob::new(MemBlob::new("unreliable"));

        // Initially starts reliable.
        assert!(blob.set("a", b"1".to_vec(), true).is_ok());
        assert!(blob.get("a").is_ok());

        // Setting it to unavailable causes all operations to fail.
        handle.make_unavailable();
        assert!(blob.set("a", b"2".to_vec(), true).is_err());
        assert!(blob.get("a").is_err());

        // Can be set back to working.
        handle.make_available();
        assert!(blob.set("a", b"3".to_vec(), true).is_ok());
        assert!(blob.get("a").is_ok());
    }
}
