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
use std::ops::{Add, Range};
use std::str::FromStr;

use async_trait::async_trait;
use futures_executor::block_on;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::error::Error;
use crate::gen::persist::ProtoMeta;

/// Sanity check whether we can decode the Blob's persisted meta object, and delete
/// all data if the encoded version is less than what the current implementation supports.
///
/// TODO: this is a hack and we will need to get rid of this once we have a
/// proper backwards compatibility policy.
pub fn check_meta_version_maybe_delete_data<B: Blob>(b: &mut B) -> Result<(), Error> {
    let meta = match block_on(b.get("META"))? {
        None => return Ok(()),
        Some(bytes) => bytes,
    };

    let current_version = ProtoMeta::ENCODING_VERSION;
    let persisted_version = ProtoMeta::encoded_version(&meta)?;

    if current_version == persisted_version {
        // Nothing to do here, everything is working as expected.
        Ok(())
    } else if current_version > persisted_version {
        // Delete all the keys, as we are upgrading to a new version.
        info!(
            "Persistence beta detected version mismatch. Deleting all previously persisted data as part of upgrade from version {} to {}.",
            persisted_version,
            current_version
        );
        let keys = block_on(b.list_keys())?;
        for key in keys {
            block_on(b.delete(&key))?;
        }

        Ok(())
    } else {
        // We are reading a version further in advance than current,
        // likely because a user has downgraded to an older version of
        // Materialize.
        Err(Error::from(format!(
            "invalid persistence version found {} can only read {}. hint: try upgrading Materialize or deleting the previously persisted data.",
            persisted_version, current_version
        )))
    }
}

/// The "sequence number" of a persist state change.
///
/// Persist is a state machine, with all mutating requests modeled as input
/// state changes sequenced into a log. This reflects that ordering.
///
/// This ordering also includes requests that were sequenced and applied to the
/// persist state machine, but that application was deterministically made into
/// a no-op because it was contextually invalid (a write or seal at a sealed
/// timestamp, an allow_compactions at an unsealed timestamp, etc).
///
/// Read-only requests are assigned the SeqNo of a write, indicating that all
/// mutating requests up to and including that one are reflected in the read
/// state.
#[derive(Clone, Copy, Debug, PartialOrd, Ord, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeqNo(pub u64);

impl timely::PartialOrder for SeqNo {
    fn less_equal(&self, other: &Self) -> bool {
        self <= other
    }
}

impl Add<u64> for SeqNo {
    type Output = SeqNo;

    fn add(self, rhs: u64) -> SeqNo {
        SeqNo(self.0 + rhs)
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

/// Configuration of whether a [Blob::set] must occur atomically.
#[derive(Debug)]
pub enum Atomicity {
    /// Require the write be atomic and either succeed or leave the previous
    /// value intact.
    RequireAtomic,
    /// Allow the write to leave partially written data in the event of an
    /// interruption. This is a performance optimization allowable for
    /// write-once modify-never blobs (everything but META). It's only exploited
    /// in some Blob implementations (File), others are naturally always atomic
    /// (S3, Mem).
    AllowNonAtomic,
}

/// An abstraction over read-only access to a `bytes key`->`bytes value` store.
#[async_trait]
pub trait BlobRead: Send + 'static {
    /// Returns a reference to the value corresponding to the key.
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error>;

    /// List all of the keys in the map.
    async fn list_keys(&self) -> Result<Vec<String>, Error>;

    /// Synchronously closes the blob, causing all future commands to error.
    ///
    /// If `Self` also implements [Blob], this releases the exclusive-writer
    /// lock.
    ///
    /// Implementations must be idempotent. Returns true if the blob had not
    /// previously been closed.
    ///
    /// NB: It's confusing for this to be on BlobRead, since it's a no-op on the
    /// various concrete {Mem,File,S3}Read impls, but in various places we need
    /// to be able to close something that we only know is a BlobRead. Possible
    /// there's something better we could be doing here.
    async fn close(&mut self) -> Result<bool, Error>;
}

/// An abstraction over read-write access to a `bytes key`->`bytes value` store.
///
/// Blob and BlobRead impls are allowed to be concurrently opened for the same
/// location in the same process (which is often used in tests), but this is not
/// idiomatic for production usage. Instead, within a process, only single Blob
/// or BlobRead impl should be used for each unique storage location.
///
/// - Invariant: Implementations are responsible for ensuring that they are
///   exclusive writers to this location.
#[async_trait]
pub trait Blob: BlobRead + Sized {
    /// The configuration necessary to open this type of storage.
    type Config;
    /// The corresponding [BlobRead] implementation.
    type Read: BlobRead;

    /// Opens the given location for exclusive read-write access.
    ///
    /// Implementations are responsible for storing the given LockInfo in such a
    /// way that no other calls to open_exclusive succeed before this one has
    /// been closed. However, it must be possible to continue to open this
    /// location for reads via [Blob::open_read].
    fn open_exclusive(config: Self::Config, lock_info: LockInfo) -> Result<Self, Error>;

    /// Opens the given location for non-exclusive read-only access.
    ///
    /// Implementations are responsible for ensuring that this works regardless
    /// of whether anyone has opened the same location is opened for exclusive
    /// read-write access. Said another way, this should succeed even if there
    /// is no LOCK file present. If it's pointed at a meaningless location, the
    /// first read (META) will fail anyway.
    fn open_read(config: Self::Config) -> Result<Self::Read, Error>;

    /// Inserts a key-value pair into the map.
    ///
    /// When atomicity is required, writes must be atomic and either succeed or
    /// leave the previous value intact.
    async fn set(&mut self, key: &str, value: Vec<u8>, atomic: Atomicity) -> Result<(), Error>;

    /// Remove a key from the map.
    ///
    /// Succeeds if the key does not exist.
    async fn delete(&mut self, key: &str) -> Result<(), Error>;
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
            .split_once('\n')
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

    use mz_persist_types::Codec;

    use crate::error::Error;
    use crate::gen::persist::ProtoMeta;
    use crate::mem::MemRegistry;
    use crate::storage::Atomicity::{AllowNonAtomic, RequireAtomic};

    use super::*;

    fn slurp<L: Log>(buf: &L) -> Result<Vec<Vec<u8>>, Error> {
        let mut entries = Vec::new();
        buf.snapshot(|_, x| {
            entries.push(x.to_vec());
            Ok(())
        })?;
        Ok(entries)
    }

    #[derive(Debug)]
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

    fn keys(baseline: &[String], new: &[&str]) -> Vec<String> {
        let mut ret = baseline.to_vec();
        ret.extend(new.iter().map(|x| x.to_string()));
        ret.sort();
        ret
    }

    pub async fn blob_impl_test<
        BF: Blob,
        F: FnMut(PathAndReentranceId<'_>) -> Result<BF, Error>,
        R: FnMut(&str) -> Result<BF::Read, Error>,
    >(
        mut new_full_fn: F,
        mut new_read_fn: R,
    ) -> Result<(), Error> {
        let values = vec!["v0".as_bytes().to_vec(), "v1".as_bytes().to_vec()];

        let _ = new_full_fn(PathAndReentranceId {
            path: "path0",
            reentrance_id: "reentrance0",
        })?;

        // We can create a second blob writing to a different place.
        let _ = new_full_fn(PathAndReentranceId {
            path: "path1",
            reentrance_id: "reentrance0",
        })?;

        // We're allowed to open the place if the node_id matches. In this
        // scenario, the previous process using the blob has crashed and
        // orphaned the lock.
        let mut full0 = new_full_fn(PathAndReentranceId {
            path: "path0",
            reentrance_id: "reentrance0",
        })?;

        // But the blob impl prevents us from opening the same place for
        // writing twice.
        assert!(new_full_fn(PathAndReentranceId {
            path: "path0",
            reentrance_id: "reentrance1",
        })
        .is_err());

        // We are, however, allowed to open the same location read-only.
        let mut read0 = new_read_fn("path0")?;

        // We can open two readers, even.
        let _ = new_read_fn("path0")?;

        // Empty key is empty.
        assert_eq!(full0.get("k0").await?, None);
        assert_eq!(read0.get("k0").await?, None);

        // Blob might create one or more keys on startup (e.g. lock files)
        let mut empty_keys: Vec<String> = full0.list_keys().await?;
        empty_keys.sort();

        // List keys is idempotent
        let mut blob_keys = full0.list_keys().await?;
        blob_keys.sort();
        assert_eq!(blob_keys, empty_keys);
        let mut blob_keys = read0.list_keys().await?;
        blob_keys.sort();
        assert_eq!(blob_keys, empty_keys);

        // Set a key with AllowNonAtomic and get it back.
        full0.set("k0", values[0].clone(), AllowNonAtomic).await?;
        assert_eq!(full0.get("k0").await?, Some(values[0].clone()));
        assert_eq!(read0.get("k0").await?, Some(values[0].clone()));

        // Set a key with RequireAtomic and get it back.
        full0.set("k0a", values[0].clone(), RequireAtomic).await?;
        assert_eq!(full0.get("k0a").await?, Some(values[0].clone()));
        assert_eq!(read0.get("k0a").await?, Some(values[0].clone()));

        // Blob contains the key we just inserted.
        let mut blob_keys = full0.list_keys().await?;
        blob_keys.sort();
        assert_eq!(blob_keys, keys(&empty_keys, &["k0", "k0a"]));
        let mut blob_keys = read0.list_keys().await?;
        blob_keys.sort();
        assert_eq!(blob_keys, keys(&empty_keys, &["k0", "k0a"]));

        // Can overwrite a key with AllowNonAtomic.
        full0.set("k0", values[1].clone(), AllowNonAtomic).await?;
        assert_eq!(full0.get("k0").await?, Some(values[1].clone()));
        assert_eq!(read0.get("k0").await?, Some(values[1].clone()));
        // Can overwrite a key with RequireAtomic.
        full0.set("k0a", values[1].clone(), RequireAtomic).await?;
        assert_eq!(full0.get("k0a").await?, Some(values[1].clone()));
        assert_eq!(read0.get("k0a").await?, Some(values[1].clone()));

        // Can delete a key.
        full0.delete("k0").await?;
        // Can no longer get a deleted key.
        assert_eq!(full0.get("k0").await?, None);
        assert_eq!(read0.get("k0").await?, None);
        // Double deleting a key succeeds.
        assert_eq!(full0.delete("k0").await, Ok(()));
        // Deleting a key that does not exist succeeds.
        assert_eq!(full0.delete("nope").await, Ok(()));

        // Empty blob contains no keys.
        full0.delete("k0a").await?;
        let mut blob_keys = full0.list_keys().await?;
        blob_keys.sort();
        assert_eq!(blob_keys, empty_keys);
        let mut blob_keys = read0.list_keys().await?;
        blob_keys.sort();
        assert_eq!(blob_keys, empty_keys);
        // Can reset a deleted key to some other value.
        full0.set("k0", values[1].clone(), AllowNonAtomic).await?;
        assert_eq!(read0.get("k0").await?, Some(values[1].clone()));
        assert_eq!(full0.get("k0").await?, Some(values[1].clone()));

        // Insert multiple keys back to back and validate that we can list
        // them all out.
        let mut expected_keys = empty_keys;
        for i in 1..=5 {
            let key = format!("k{}", i);
            full0.set(&key, values[0].clone(), AllowNonAtomic).await?;
            expected_keys.push(key);
        }

        // Blob contains the key we just inserted.
        let mut blob_keys = full0.list_keys().await?;
        blob_keys.sort();
        assert_eq!(blob_keys, keys(&expected_keys, &["k0"]));
        let mut blob_keys = read0.list_keys().await?;
        blob_keys.sort();
        assert_eq!(blob_keys, keys(&expected_keys, &["k0"]));

        // Cannot reuse a blob once it is closed.
        assert_eq!(full0.close().await, Ok(true));
        assert!(full0.get("k0").await.is_err());
        assert!(full0.list_keys().await.is_err());
        assert!(full0
            .set("k1", values[0].clone(), RequireAtomic)
            .await
            .is_err());
        assert!(full0.delete("k0").await.is_err());

        // Close must be idempotent and must return false if it did no work.
        assert_eq!(full0.close().await, Ok(false));

        // Closing the exclusive-writer doesn't affect the availability of any
        // readers.
        assert_eq!(read0.get("k0").await?, Some(values[1].clone()));

        // We can reopen the exclusive-writer with a different reentrance_id and
        // use it.
        let full0 = new_full_fn(PathAndReentranceId {
            path: "path0",
            reentrance_id: "reentrance1",
        })?;
        assert_eq!(full0.get("k0").await?, Some(values[1].clone()));

        // Reader is still available
        assert_eq!(read0.get("k0").await?, Some(values[1].clone()));

        // Cannot reuse a reader once it is closed.
        assert_eq!(read0.close().await, Ok(true));
        assert!(read0.get("k0").await.is_err());
        assert!(read0.list_keys().await.is_err());

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

    #[test]
    fn check_meta_version() -> Result<(), Error> {
        let registry = MemRegistry::new();
        let mut blob = registry.blob_no_reentrance()?;

        let meta = ProtoMeta::default();
        let mut val = Vec::new();
        meta.encode(&mut val);
        let current_version = val[0];

        // This test needs to be able to increment and decrement the current
        // version.
        assert!(current_version > 0 && current_version < u8::MAX);
        let (future_version, prev_version) = (current_version + 1, current_version - 1);

        // Blob without meta. No-op.
        check_meta_version_maybe_delete_data(&mut blob)?;
        assert_eq!(block_on(blob.list_keys())?, Vec::<String>::new());

        // encoded_version == current version. No-op.
        block_on(blob.set("META", val.clone(), RequireAtomic))?;
        check_meta_version_maybe_delete_data(&mut blob)?;
        assert_eq!(block_on(blob.list_keys())?, vec!["META".to_string()]);

        // encoded_version > current_version. Should return an error indicating
        // encoded_version is from the future, and not modify the blob.
        val[0] = future_version;
        block_on(blob.set("META", val.clone(), RequireAtomic))?;
        assert!(check_meta_version_maybe_delete_data(&mut blob).is_err());
        assert_eq!(block_on(blob.list_keys())?, vec!["META".to_string()]);

        // encoded_version < current_version. Should delete all existing keys,
        // and not return any errors.
        val[0] = prev_version;
        block_on(blob.set("META", val.clone(), RequireAtomic))?;
        check_meta_version_maybe_delete_data(&mut blob)?;
        assert_eq!(block_on(blob.list_keys())?, Vec::<String>::new());

        Ok(())
    }
}
