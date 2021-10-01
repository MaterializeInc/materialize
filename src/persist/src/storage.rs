// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Abstractions over files, cloud storage, etc used in persistence.

use std::fmt;
use std::io::Read;
use std::ops::Range;
use std::str::FromStr;

use abomonation_derive::Abomonation;

use crate::error::Error;

/// A "sequence number", uniquely associated with an entry in a Log.
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
/// later used when draining data back out of the log.
///
/// - Invariant: Implementations are responsible for ensuring that they are
///   exclusive writers to this location.
pub trait Log {
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

    /// Synchronously closes the log, releasing exclusive-writer locks and
    /// causing all future commands to error.
    ///
    /// Implementations must be idempotent. Returns true if the log had not
    /// previously been closed.
    fn close(&mut self) -> Result<bool, Error>;
}

/// An abstraction over a `bytes key`->`bytes value` store.
///
/// - Invariant: Implementations are responsible for ensuring that they are
///   exclusive writers to this location.
pub trait Blob: Send + 'static {
    /// Returns a reference to the value corresponding to the key.
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error>;

    /// Inserts a key-value pair into the map.
    ///
    /// When allow_overwrite is true, writes must be atomic and either succeed
    /// or leave the previous value intact.
    fn set(&mut self, key: &str, value: Vec<u8>, allow_overwrite: bool) -> Result<(), Error>;

    /// Remove a key from the map.
    ///
    /// Succeeds if the key does not exist.
    fn delete(&mut self, key: &str) -> Result<(), Error>;

    /// Synchronously closes the blob, releasing exclusive-writer locks and
    /// causing all future commands to error.
    ///
    /// Implementations must be idempotent. Returns true if the blob had not
    /// previously been closed.
    fn close(&mut self) -> Result<bool, Error>;
}

/// The partially structured information stored in an exclusive-writer lock.
///
/// To allow for restart without operator intervention in situations where we've
/// crashed and are unable to cleanly unlock (s3 has no flock equivalent, for
/// example), this supports reentrance. We define the "same" process as any that
/// have the same reentrance id, which is an opaque user-provided string.
///
/// This, in essence, delegates the problem of ensuring writer-exclusivity to
/// the persist user, which may have access to better (possibly distributed)
/// locking primitives than we do.
///
/// Concretely, MZ Cloud will initially depend on the exclusivity of attaching
/// an EBS volume. We'll store the reentrant_id somewhere on the EBS volume that
/// holds the catalog. Any process that starts and has access to this volume is
/// guaranteed that the previous machine is no longer available and thus the
/// previous mz process is no longer running. Similarly, a second process cannot
/// accidentally start up pointed at the same storage locations because it will
/// not have the reentrant_id available.
///
/// Violating writer-exclusivity will cause undefined behavior including data
/// loss. It's always correct, and MUCH safer, to provide a random reentrant_id
/// here (e.g. a UUID). It will simply require operator intervention to verify
/// that the previous process is no longer running in the event of a crash and
/// to confirm this by manually removing the previous lock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockInfo {
    reentrance_id: String,
    details: String,
}

impl LockInfo {
    /// Returns a new LockInfo from its component parts.
    ///
    /// Errors if reentrance_id contains a newline.
    pub fn new(reentrance_id: String, details: String) -> Result<Self, Error> {
        if reentrance_id.contains('\n') {
            return Err(Error::from(format!(
                "reentrance_id cannot contain newlines got:\n{}",
                reentrance_id
            )));
        }
        Ok(LockInfo {
            reentrance_id,
            details,
        })
    }

    /// Constructs a new, empty LockInfo with a unique reentrance id.
    ///
    /// Helper for tests that don't care about locking reentrance (which is most
    /// of them).
    pub fn new_no_reentrance(details: String) -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let reentrance_id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            .to_string();
        Self::new(reentrance_id, details).expect("reentrance_id was valid")
    }

    /// Returns Ok if this lock information represents a process that may
    /// proceed in the presence of some existing lock.
    pub fn check_reentrant_for<D: fmt::Debug, R: Read>(
        &self,
        location: &D,
        mut existing: R,
    ) -> Result<(), Error> {
        let mut existing_contents = String::new();
        existing.read_to_string(&mut existing_contents)?;
        if existing_contents.is_empty() {
            // Even if reentrant_id and details are both empty, we'll still have
            // a "\n" in the serialized representation, so it's safe to treat
            // empty as meaning no lock.
            return Ok(());
        }
        let existing = Self::from_str(&existing_contents)?;
        if self.reentrance_id == existing.reentrance_id {
            Ok(())
        } else {
            Err(Error::from(format!(
                "location {:?} was already_locked:\n{}",
                location, existing
            )))
        }
    }
}

impl fmt::Display for LockInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.write_str(&self.reentrance_id)?;
        f.write_str("\n")?;
        f.write_str(&self.details)?;
        Ok(())
    }
}

impl FromStr for LockInfo {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (reentrance_id, details) = s
            .split_once("\n")
            .ok_or_else(|| Error::from(format!("invalid LOCK format: {}", s)))?;
        LockInfo::new(reentrance_id.to_owned(), details.to_owned())
    }
}

#[cfg(test)]
impl From<(&str, &str)> for LockInfo {
    fn from(x: (&str, &str)) -> Self {
        let (reentrance_id, details) = x;
        LockInfo {
            reentrance_id: reentrance_id.to_owned(),
            details: details.to_owned(),
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::ops::RangeInclusive;

    use crate::error::Error;

    use super::*;

    fn slurp<L: Log>(buf: &L) -> Result<Vec<Vec<u8>>, Error> {
        let mut entries = Vec::new();
        buf.snapshot(|_, x| {
            entries.push(x.to_vec());
            Ok(())
        })?;
        Ok(entries)
    }

    pub struct PathAndReentranceId<'a> {
        pub path: &'a str,
        pub reentrance_id: &'a str,
    }

    pub fn log_impl_test<L: Log, F: FnMut(PathAndReentranceId<'_>) -> Result<L, Error>>(
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

        let _ = new_fn(PathAndReentranceId {
            path: "path0",
            reentrance_id: "reentrance0",
        })?;

        // We can create a second log writing to a different place.
        let _ = new_fn(PathAndReentranceId {
            path: "path1",
            reentrance_id: "reentrance0",
        })?;

        // We're allowed to open the place if the node_id matches. In this
        // scenario, the previous process using the log has crashed and
        // orphaned the lock.
        let mut log0 = new_fn(PathAndReentranceId {
            path: "path0",
            reentrance_id: "reentrance0",
        })?;

        // But the log impl prevents us from opening the same place for
        // writing twice if the node_id doesn't match.
        assert!(new_fn(PathAndReentranceId {
            path: "path0",
            reentrance_id: "reentrance1",
        })
        .is_err());

        // Empty writer is empty.
        assert!(slurp(&log0)?.is_empty());

        // First write is assigned SeqNo(0).
        assert_eq!(log0.write_sync(entries[0].clone())?, SeqNo(0));
        assert_eq!(slurp(&log0)?, sub_entries(0..=0));

        // Second write is assigned SeqNo(1). Now contains 2 entries.
        assert_eq!(log0.write_sync(entries[1].clone())?, SeqNo(1));
        assert_eq!(slurp(&log0)?, sub_entries(0..=1));

        // Truncate removes the first entry.
        log0.truncate(SeqNo(1))?;
        assert_eq!(slurp(&log0)?, sub_entries(1..=1));

        // We are not allowed to truncate to places outside the current range.
        assert!(log0.truncate(SeqNo(0)).is_err());
        assert!(log0.truncate(SeqNo(3)).is_err());

        // Write works after a truncate has happened.
        assert_eq!(log0.write_sync(entries[2].clone())?, SeqNo(2));
        assert_eq!(slurp(&log0)?, sub_entries(1..=2));

        // Truncate everything.
        log0.truncate(SeqNo(3))?;
        assert!(slurp(&log0)?.is_empty());

        // Cannot reuse a log once it is closed.
        assert_eq!(log0.close(), Ok(true));
        assert!(log0.write_sync(entries[1].clone()).is_err());
        assert!(slurp(&log0).is_err());
        assert!(log0.truncate(SeqNo(4)).is_err());

        // Close must be idempotent and must return false if it did no work.
        assert_eq!(log0.close(), Ok(false));

        // But we can reopen it and use it.
        let mut log0 = new_fn(PathAndReentranceId {
            path: "path0",
            reentrance_id: "reentrance0",
        })?;
        assert_eq!(log0.write_sync(entries[3].clone())?, SeqNo(3));
        assert_eq!(slurp(&log0)?, sub_entries(3..=3));
        assert_eq!(log0.close(), Ok(true));
        let mut log0 = new_fn(PathAndReentranceId {
            path: "path0",
            reentrance_id: "reentrance0",
        })?;
        assert_eq!(log0.write_sync(entries[4].clone())?, SeqNo(4));
        assert_eq!(slurp(&log0)?, sub_entries(3..=4));
        assert_eq!(log0.close(), Ok(true));

        Ok(())
    }

    pub fn blob_impl_test<B: Blob, F: FnMut(PathAndReentranceId<'_>) -> Result<B, Error>>(
        mut new_fn: F,
    ) -> Result<(), Error> {
        let values = vec!["v0".as_bytes().to_vec(), "v1".as_bytes().to_vec()];

        let _ = new_fn(PathAndReentranceId {
            path: "path0",
            reentrance_id: "reentrance0",
        })?;

        // We can create a second blob writing to a different place.
        let _ = new_fn(PathAndReentranceId {
            path: "path1",
            reentrance_id: "reentrance0",
        })?;

        // We're allowed to open the place if the node_id matches. In this
        // scenario, the previous process using the blob has crashed and
        // orphaned the lock.
        let mut blob0 = new_fn(PathAndReentranceId {
            path: "path0",
            reentrance_id: "reentrance0",
        })?;

        // But the blob impl prevents us from opening the same place for
        // writing twice.
        assert!(new_fn(PathAndReentranceId {
            path: "path0",
            reentrance_id: "reentrance1",
        })
        .is_err());

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

        // Can delete a key.
        blob0.delete("k0")?;
        // Can no longer get a deleted key.
        assert_eq!(blob0.get("k0")?, None);
        // Double deleting a key succeeds.
        assert_eq!(blob0.delete("k0"), Ok(()));
        // Deleting a key that does not exist succeeds.
        assert_eq!(blob0.delete("nope"), Ok(()));
        // Can reset a deleted key to some other value.
        blob0.set("k0", values[1].clone(), false)?;
        assert_eq!(blob0.get("k0")?, Some(values[1].clone()));

        // Cannot reuse a blob once it is closed.
        assert_eq!(blob0.close(), Ok(true));
        assert!(blob0.get("k0").is_err());
        assert!(blob0.set("k1", values[0].clone(), true).is_err());

        // Close must be idempotent and must return false if it did no work.
        assert_eq!(blob0.close(), Ok(false));

        // But we can reopen it and use it.
        let blob0 = new_fn(PathAndReentranceId {
            path: "path0",
            reentrance_id: "reentrance0",
        })?;
        assert_eq!(blob0.get("k0")?, Some(values[1].clone()));

        Ok(())
    }

    #[test]
    pub fn lock_info() -> Result<(), Error> {
        // Invalid reentrance_id.
        assert_eq!(
            LockInfo::new("foo\n".to_string(), "bar".to_string()),
            Err("reentrance_id cannot contain newlines got:\nfoo\n".into())
        );

        // Roundtrip-able through the display format.
        let l = LockInfo::new("foo".to_owned(), "bar".to_owned())?;
        assert_eq!(l.to_string(), "foo\nbar");
        assert_eq!(l.to_string().parse::<LockInfo>()?, l);

        // Reentrance
        assert_eq!(
            LockInfo::new("foo".to_owned(), "".to_owned())?
                .check_reentrant_for(&"", l.to_string().as_bytes()),
            Ok(())
        );
        assert_eq!(
            LockInfo::new("baz".to_owned(), "".to_owned())?
                .check_reentrant_for(&"", l.to_string().as_bytes()),
            Err("location \"\" was already_locked:\nfoo\nbar".into())
        );

        Ok(())
    }
}
