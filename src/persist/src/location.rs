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
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use anyhow::anyhow;
use async_trait::async_trait;
use azure_core::StatusCode;
use bytes::Bytes;
use futures_util::Stream;
use mz_ore::bytes::SegmentedBytes;
use mz_ore::cast::u64_to_usize;
use mz_postgres_client::error::PostgresError;
use mz_proto::RustType;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use tracing::{Instrument, Span};

use crate::error::Error;

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
#[derive(
    Arbitrary, Clone, Copy, Debug, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize,
)]
pub struct SeqNo(pub u64);

impl std::fmt::Display for SeqNo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}", self.0)
    }
}

impl timely::PartialOrder for SeqNo {
    fn less_equal(&self, other: &Self) -> bool {
        self <= other
    }
}

impl std::str::FromStr for SeqNo {
    type Err = String;

    fn from_str(encoded: &str) -> Result<Self, Self::Err> {
        let encoded = match encoded.strip_prefix('v') {
            Some(x) => x,
            None => return Err(format!("invalid SeqNo {}: incorrect prefix", encoded)),
        };
        let seqno =
            u64::from_str(encoded).map_err(|err| format!("invalid SeqNo {}: {}", encoded, err))?;
        Ok(SeqNo(seqno))
    }
}

impl SeqNo {
    /// Returns the next SeqNo in the sequence.
    pub fn next(self) -> SeqNo {
        SeqNo(self.0 + 1)
    }

    /// A minimum value suitable as a default.
    pub fn minimum() -> Self {
        SeqNo(0)
    }

    /// A maximum value.
    pub fn maximum() -> Self {
        SeqNo(u64::MAX)
    }
}

impl RustType<u64> for SeqNo {
    fn into_proto(&self) -> u64 {
        self.0
    }

    fn from_proto(proto: u64) -> Result<Self, mz_proto::TryFromProtoError> {
        Ok(SeqNo(proto))
    }
}

/// An error coming from an underlying durability system (e.g. s3) indicating
/// that the operation _definitely did NOT succeed_ (e.g. permission denied).
#[derive(Debug)]
pub struct Determinate {
    inner: anyhow::Error,
}

impl std::fmt::Display for Determinate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "determinate: ")?;
        self.inner.fmt(f)
    }
}

impl std::error::Error for Determinate {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.inner.source()
    }
}

impl From<anyhow::Error> for Determinate {
    fn from(inner: anyhow::Error) -> Self {
        Self::new(inner)
    }
}

impl Determinate {
    /// Return a new Determinate wrapping the given error.
    ///
    /// Exposed for testing via [crate::unreliable].
    pub fn new(inner: anyhow::Error) -> Self {
        Determinate { inner }
    }
}

/// An error coming from an underlying durability system (e.g. s3) indicating
/// that the operation _might have succeeded_ (e.g. timeout).
#[derive(Debug)]
pub struct Indeterminate {
    pub(crate) inner: anyhow::Error,
}

impl Indeterminate {
    /// Return a new Indeterminate wrapping the given error.
    ///
    /// Exposed for testing.
    pub fn new(inner: anyhow::Error) -> Self {
        Indeterminate { inner }
    }
}

impl std::fmt::Display for Indeterminate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "indeterminate: ")?;
        self.inner.fmt(f)
    }
}

impl std::error::Error for Indeterminate {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.inner.source()
    }
}

/// An impl of PartialEq purely for convenience in tests and debug assertions.
#[cfg(any(test, debug_assertions))]
impl PartialEq for Indeterminate {
    fn eq(&self, other: &Self) -> bool {
        self.to_string() == other.to_string()
    }
}

/// An error coming from an underlying durability system (e.g. s3) or from
/// invalid data received from one.
#[derive(Debug)]
pub enum ExternalError {
    /// A determinate error from an external system.
    Determinate(Determinate),
    /// An indeterminate error from an external system.
    Indeterminate(Indeterminate),
}

impl ExternalError {
    /// Returns a new error representing a timeout.
    ///
    /// TODO: When we overhaul errors, this presumably should instead be a type
    /// that can be matched on.
    #[track_caller]
    pub fn new_timeout(deadline: Instant) -> Self {
        ExternalError::Indeterminate(Indeterminate {
            inner: anyhow!("timeout at {:?}", deadline),
        })
    }

    /// Returns whether this error represents a timeout.
    ///
    /// TODO: When we overhaul errors, this presumably should instead be a type
    /// that can be matched on.
    pub fn is_timeout(&self) -> bool {
        // Gross...
        self.to_string().contains("timeout")
    }
}

impl std::fmt::Display for ExternalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExternalError::Determinate(x) => std::fmt::Display::fmt(x, f),
            ExternalError::Indeterminate(x) => std::fmt::Display::fmt(x, f),
        }
    }
}

impl std::error::Error for ExternalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ExternalError::Determinate(e) => e.source(),
            ExternalError::Indeterminate(e) => e.source(),
        }
    }
}

/// An impl of PartialEq purely for convenience in tests and debug assertions.
#[cfg(any(test, debug_assertions))]
impl PartialEq for ExternalError {
    fn eq(&self, other: &Self) -> bool {
        self.to_string() == other.to_string()
    }
}

impl From<PostgresError> for ExternalError {
    fn from(x: PostgresError) -> Self {
        match x {
            PostgresError::Determinate(e) => ExternalError::Determinate(Determinate::new(e)),
            PostgresError::Indeterminate(e) => ExternalError::Indeterminate(Indeterminate::new(e)),
        }
    }
}

impl From<Indeterminate> for ExternalError {
    fn from(x: Indeterminate) -> Self {
        ExternalError::Indeterminate(x)
    }
}

impl From<Determinate> for ExternalError {
    fn from(x: Determinate) -> Self {
        ExternalError::Determinate(x)
    }
}

impl From<anyhow::Error> for ExternalError {
    fn from(inner: anyhow::Error) -> Self {
        ExternalError::Indeterminate(Indeterminate { inner })
    }
}

impl From<Error> for ExternalError {
    fn from(x: Error) -> Self {
        ExternalError::Indeterminate(Indeterminate {
            inner: anyhow::Error::new(x),
        })
    }
}

impl From<std::io::Error> for ExternalError {
    fn from(x: std::io::Error) -> Self {
        ExternalError::Indeterminate(Indeterminate {
            inner: anyhow::Error::new(x),
        })
    }
}

impl From<deadpool_postgres::tokio_postgres::Error> for ExternalError {
    fn from(e: deadpool_postgres::tokio_postgres::Error) -> Self {
        let code = match e.as_db_error().map(|x| x.code()) {
            Some(x) => x,
            None => {
                return ExternalError::Indeterminate(Indeterminate {
                    inner: anyhow::Error::new(e),
                });
            }
        };
        match code {
            // Feel free to add more things to this allowlist as we encounter
            // them as long as you're certain they're determinate.
            &deadpool_postgres::tokio_postgres::error::SqlState::T_R_SERIALIZATION_FAILURE => {
                ExternalError::Determinate(Determinate {
                    inner: anyhow::Error::new(e),
                })
            }
            _ => ExternalError::Indeterminate(Indeterminate {
                inner: anyhow::Error::new(e),
            }),
        }
    }
}

impl From<azure_core::Error> for ExternalError {
    fn from(value: azure_core::Error) -> Self {
        let definitely_determinate = if let Some(http) = value.as_http_error() {
            match http.status() {
                // There are many other status codes that _ought_ to be determinate, according to
                // the HTTP spec, but this includes only codes that we've observed in practice for now.
                StatusCode::TooManyRequests => true,
                _ => false,
            }
        } else {
            false
        };
        if definitely_determinate {
            ExternalError::Determinate(Determinate {
                inner: anyhow!(value),
            })
        } else {
            ExternalError::Indeterminate(Indeterminate {
                inner: anyhow!(value),
            })
        }
    }
}

impl From<deadpool_postgres::PoolError> for ExternalError {
    fn from(x: deadpool_postgres::PoolError) -> Self {
        match x {
            // We have logic for turning a postgres Error into an ExternalError,
            // so use it.
            deadpool_postgres::PoolError::Backend(x) => ExternalError::from(x),
            x => ExternalError::Indeterminate(Indeterminate {
                inner: anyhow::Error::new(x),
            }),
        }
    }
}

impl From<tokio::task::JoinError> for ExternalError {
    fn from(x: tokio::task::JoinError) -> Self {
        ExternalError::Indeterminate(Indeterminate {
            inner: anyhow::Error::new(x),
        })
    }
}

/// An abstraction for a single arbitrarily-sized binary blob and an associated
/// version number (sequence number).
#[derive(Debug, Clone, PartialEq)]
pub struct VersionedData {
    /// The sequence number of the data.
    pub seqno: SeqNo,
    /// The data itself.
    pub data: Bytes,
}

/// Helper constant to scan all states in [Consensus::scan].
/// The maximum possible SeqNo is i64::MAX.
// TODO(benesch): find a way to express this without `as`.
#[allow(clippy::as_conversions)]
pub const SCAN_ALL: usize = u64_to_usize(i64::MAX as u64);

/// A key usable for liveness checks via [Consensus::head].
pub const CONSENSUS_HEAD_LIVENESS_KEY: &str = "LIVENESS";

/// Return type to indicate whether [Consensus::compare_and_set] succeeded or failed.
#[derive(Debug, PartialEq)]
pub enum CaSResult {
    /// The compare-and-set succeeded and committed new state.
    Committed,
    /// The compare-and-set failed due to expectation mismatch.
    ExpectationMismatch,
}

/// Wraps all calls to a backing store in a new tokio task. This adds extra overhead,
/// but insulates the system from callers who fail to drive futures promptly to completion,
/// which can cause timeouts or resource exhaustion in a store.
#[derive(Debug)]
pub struct Tasked<A>(pub Arc<A>);

impl<A> Tasked<A> {
    fn clone_backing(&self) -> Arc<A> {
        Arc::clone(&self.0)
    }
}

/// A boxed stream, similar to what `async_trait` desugars async functions to, but hardcoded
/// to our standard result type.
pub type ResultStream<'a, T> = Pin<Box<dyn Stream<Item = Result<T, ExternalError>> + Send + 'a>>;

/// An abstraction for [VersionedData] held in a location in persistent storage
/// where the data are conditionally updated by version.
///
/// Users are expected to use this API with consistently increasing sequence numbers
/// to allow multiple processes across multiple machines to agree to a total order
/// of the evolution of the data. To make roundtripping through various forms of durable
/// storage easier, sequence numbers used with [Consensus] need to be restricted to the
/// range [0, i64::MAX].
#[async_trait]
pub trait Consensus: std::fmt::Debug + Send + Sync {
    /// Returns all the keys ever created in the consensus store.
    fn list_keys(&self) -> ResultStream<'_, String>;

    /// Returns a recent version of `data`, and the corresponding sequence number, if
    /// one exists at this location.
    async fn head(&self, key: &str) -> Result<Option<VersionedData>, ExternalError>;

    /// Update the [VersionedData] stored at this location to `new`, iff the
    /// current sequence number is exactly `expected` and `new`'s sequence
    /// number > the current sequence number.
    ///
    /// It is invalid to call this function with a `new` and `expected` such
    /// that `new`'s sequence number is <= `expected`. It is invalid to call
    /// this function with a sequence number outside of the range `[0, i64::MAX]`.
    ///
    /// This data is initialized to None, and the first call to compare_and_set
    /// needs to happen with None as the expected value to set the state.
    async fn compare_and_set(
        &self,
        key: &str,
        expected: Option<SeqNo>,
        new: VersionedData,
    ) -> Result<CaSResult, ExternalError>;

    /// Return `limit` versions of data stored for this `key` at sequence numbers >= `from`,
    /// in ascending order of sequence number.
    ///
    /// Returns an empty vec if `from` is greater than the current sequence
    /// number or if there is no data at this key.
    async fn scan(
        &self,
        key: &str,
        from: SeqNo,
        limit: usize,
    ) -> Result<Vec<VersionedData>, ExternalError>;

    /// Deletes all historical versions of the data stored at `key` that are <
    /// `seqno`, iff `seqno` <= the current sequence number.
    ///
    /// Returns the number of versions deleted on success. Returns an error if
    /// `seqno` is greater than the current sequence number, or if there is no
    /// data at this key.
    async fn truncate(&self, key: &str, seqno: SeqNo) -> Result<usize, ExternalError>;

    /// Returns true if [`truncate`] returns the number of versions deleted.
    fn truncate_counts(&self) -> bool {
        true
    }
}

#[async_trait]
impl<A: Consensus + 'static> Consensus for Tasked<A> {
    fn list_keys(&self) -> ResultStream<'_, String> {
        // Similarly to Blob::list_keys_and_metadata, this is difficult to make into a task.
        // (If we use an unbounded channel between the task and the caller, we can buffer forever;
        // if we use a bounded channel, we lose the isolation benefits of Tasked.)
        // However, this should only be called in administrative contexts
        // and not in the main state-machine impl.
        self.0.list_keys()
    }

    async fn head(&self, key: &str) -> Result<Option<VersionedData>, ExternalError> {
        let backing = self.clone_backing();
        let key = key.to_owned();
        mz_ore::task::spawn(
            || "persist::task::head",
            async move { backing.head(&key).await }.instrument(Span::current()),
        )
        .await?
    }

    async fn compare_and_set(
        &self,
        key: &str,
        expected: Option<SeqNo>,
        new: VersionedData,
    ) -> Result<CaSResult, ExternalError> {
        let backing = self.clone_backing();
        let key = key.to_owned();
        mz_ore::task::spawn(
            || "persist::task::cas",
            async move { backing.compare_and_set(&key, expected, new).await }
                .instrument(Span::current()),
        )
        .await?
    }

    async fn scan(
        &self,
        key: &str,
        from: SeqNo,
        limit: usize,
    ) -> Result<Vec<VersionedData>, ExternalError> {
        let backing = self.clone_backing();
        let key = key.to_owned();
        mz_ore::task::spawn(
            || "persist::task::scan",
            async move { backing.scan(&key, from, limit).await }.instrument(Span::current()),
        )
        .await?
    }

    async fn truncate(&self, key: &str, seqno: SeqNo) -> Result<usize, ExternalError> {
        let backing = self.clone_backing();
        let key = key.to_owned();
        mz_ore::task::spawn(
            || "persist::task::truncate",
            async move { backing.truncate(&key, seqno).await }.instrument(Span::current()),
        )
        .await?
    }
}

/// Metadata about a particular blob stored by persist
#[derive(Debug)]
pub struct BlobMetadata<'a> {
    /// The key for the blob
    pub key: &'a str,
    /// Size of the blob
    pub size_in_bytes: u64,
}

/// A key usable for liveness checks via [Blob::get].
pub const BLOB_GET_LIVENESS_KEY: &str = "LIVENESS";

/// An abstraction over read-write access to a `bytes key`->`bytes value` store.
///
/// Implementations are required to be _linearizable_.
///
/// TODO: Consider whether this can be relaxed. Since our usage is write-once
/// modify-never, it certainly seems like we could by adding retries around
/// `get` to wait for a non-linearizable `set` to show up. However, the tricky
/// bit comes once we stop handing out seqno capabilities to readers and have to
/// start reasoning about "this set hasn't show up yet" vs "the blob has already
/// been deleted". Another tricky problem is the same but for a deletion when
/// the first attempt timed out.
#[async_trait]
pub trait Blob: std::fmt::Debug + Send + Sync {
    /// Returns a reference to the value corresponding to the key.
    async fn get(&self, key: &str) -> Result<Option<SegmentedBytes>, ExternalError>;

    /// List all of the keys in the map with metadata about the entry.
    ///
    /// Can be optionally restricted to only list keys starting with a
    /// given prefix.
    async fn list_keys_and_metadata(
        &self,
        key_prefix: &str,
        f: &mut (dyn FnMut(BlobMetadata) + Send + Sync),
    ) -> Result<(), ExternalError>;

    /// Inserts a key-value pair into the map.
    ///
    /// Writes must be atomic and either succeed or leave the previous value
    /// intact.
    async fn set(&self, key: &str, value: Bytes) -> Result<(), ExternalError>;

    /// Remove a key from the map.
    ///
    /// Returns Some and the size of the deleted blob if if exists. Succeeds and
    /// returns None if it does not exist.
    async fn delete(&self, key: &str) -> Result<Option<usize>, ExternalError>;

    /// Restores a previously-deleted key to the map, if possible.
    ///
    /// Returns successfully if the key exists after this call: perhaps because it already existed
    /// or was restored. (In particular, this makes restore idempotent.)
    /// Fails if we were unable to restore any value for that key:
    /// perhaps the key was never written, or was permanently deleted.
    ///
    /// It is acceptable for [Blob::restore] to be unable
    /// to restore keys, in which case this method should succeed iff the key exists.
    async fn restore(&self, key: &str) -> Result<(), ExternalError>;
}

#[async_trait]
impl<A: Blob + 'static> Blob for Tasked<A> {
    async fn get(&self, key: &str) -> Result<Option<SegmentedBytes>, ExternalError> {
        let backing = self.clone_backing();
        let key = key.to_owned();
        mz_ore::task::spawn(
            || "persist::task::get",
            async move { backing.get(&key).await }.instrument(Span::current()),
        )
        .await?
    }

    /// List all of the keys in the map with metadata about the entry.
    ///
    /// Can be optionally restricted to only list keys starting with a
    /// given prefix.
    async fn list_keys_and_metadata(
        &self,
        key_prefix: &str,
        f: &mut (dyn FnMut(BlobMetadata) + Send + Sync),
    ) -> Result<(), ExternalError> {
        // TODO: No good way that I can see to make this one a task because of
        // the closure and Blob needing to be object-safe.
        self.0.list_keys_and_metadata(key_prefix, f).await
    }

    /// Inserts a key-value pair into the map.
    async fn set(&self, key: &str, value: Bytes) -> Result<(), ExternalError> {
        let backing = self.clone_backing();
        let key = key.to_owned();
        mz_ore::task::spawn(
            || "persist::task::set",
            async move { backing.set(&key, value).await }.instrument(Span::current()),
        )
        .await?
    }

    /// Remove a key from the map.
    ///
    /// Returns Some and the size of the deleted blob if if exists. Succeeds and
    /// returns None if it does not exist.
    async fn delete(&self, key: &str) -> Result<Option<usize>, ExternalError> {
        let backing = self.clone_backing();
        let key = key.to_owned();
        mz_ore::task::spawn(
            || "persist::task::delete",
            async move { backing.delete(&key).await }.instrument(Span::current()),
        )
        .await?
    }

    async fn restore(&self, key: &str) -> Result<(), ExternalError> {
        let backing = self.clone_backing();
        let key = key.to_owned();
        mz_ore::task::spawn(
            || "persist::task::restore",
            async move { backing.restore(&key).await }.instrument(Span::current()),
        )
        .await?
    }
}

/// Test helpers for the crate.
#[cfg(test)]
pub mod tests {
    use std::future::Future;

    use anyhow::anyhow;
    use futures_util::TryStreamExt;
    use mz_ore::{assert_err, assert_ok};
    use uuid::Uuid;

    use crate::location::Blob;

    use super::*;

    fn keys(baseline: &[String], new: &[&str]) -> Vec<String> {
        let mut ret = baseline.to_vec();
        ret.extend(new.iter().map(|x| x.to_string()));
        ret.sort();
        ret
    }

    async fn get_keys(b: &impl Blob) -> Result<Vec<String>, ExternalError> {
        let mut keys = vec![];
        b.list_keys_and_metadata("", &mut |entry| keys.push(entry.key.to_string()))
            .await?;
        Ok(keys)
    }

    async fn get_keys_with_prefix(
        b: &impl Blob,
        prefix: &str,
    ) -> Result<Vec<String>, ExternalError> {
        let mut keys = vec![];
        b.list_keys_and_metadata(prefix, &mut |entry| keys.push(entry.key.to_string()))
            .await?;
        Ok(keys)
    }

    /// Common test impl for different blob implementations.
    pub async fn blob_impl_test<
        B: Blob,
        F: Future<Output = Result<B, ExternalError>>,
        NewFn: Fn(&'static str) -> F,
    >(
        new_fn: NewFn,
    ) -> Result<(), ExternalError> {
        let values = ["v0".as_bytes().to_vec(), "v1".as_bytes().to_vec()];

        let blob0 = new_fn("path0").await?;

        // We can create a second blob writing to a different place.
        let _ = new_fn("path1").await?;

        // We can open two blobs to the same place, even.
        let blob1 = new_fn("path0").await?;

        let k0 = "foo/bar/k0";

        // Empty key is empty.
        assert_eq!(blob0.get(k0).await?, None);
        assert_eq!(blob1.get(k0).await?, None);

        // Empty list keys is empty.
        let empty_keys = get_keys(&blob0).await?;
        assert_eq!(empty_keys, Vec::<String>::new());
        let empty_keys = get_keys(&blob1).await?;
        assert_eq!(empty_keys, Vec::<String>::new());

        // Set a key and get it back.
        blob0.set(k0, values[0].clone().into()).await?;
        assert_eq!(
            blob0.get(k0).await?.map(|s| s.into_contiguous()),
            Some(values[0].clone())
        );
        assert_eq!(
            blob1.get(k0).await?.map(|s| s.into_contiguous()),
            Some(values[0].clone())
        );

        // Set another key and get it back.
        blob0.set("k0a", values[0].clone().into()).await?;
        assert_eq!(
            blob0.get("k0a").await?.map(|s| s.into_contiguous()),
            Some(values[0].clone())
        );
        assert_eq!(
            blob1.get("k0a").await?.map(|s| s.into_contiguous()),
            Some(values[0].clone())
        );

        // Blob contains the key we just inserted.
        let mut blob_keys = get_keys(&blob0).await?;
        blob_keys.sort();
        assert_eq!(blob_keys, keys(&empty_keys, &[k0, "k0a"]));
        let mut blob_keys = get_keys(&blob1).await?;
        blob_keys.sort();
        assert_eq!(blob_keys, keys(&empty_keys, &[k0, "k0a"]));

        // Can overwrite a key.
        blob0.set(k0, values[1].clone().into()).await?;
        assert_eq!(
            blob0.get(k0).await?.map(|s| s.into_contiguous()),
            Some(values[1].clone())
        );
        assert_eq!(
            blob1.get(k0).await?.map(|s| s.into_contiguous()),
            Some(values[1].clone())
        );
        // Can overwrite another key.
        blob0.set("k0a", values[1].clone().into()).await?;
        assert_eq!(
            blob0.get("k0a").await?.map(|s| s.into_contiguous()),
            Some(values[1].clone())
        );
        assert_eq!(
            blob1.get("k0a").await?.map(|s| s.into_contiguous()),
            Some(values[1].clone())
        );

        // Can delete a key.
        assert_eq!(blob0.delete(k0).await, Ok(Some(2)));
        // Can no longer get a deleted key.
        assert_eq!(blob0.get(k0).await?, None);
        assert_eq!(blob1.get(k0).await?, None);
        // Double deleting a key succeeds but indicates that it did no work.
        assert_eq!(blob0.delete(k0).await, Ok(None));
        // Deleting a key that does not exist succeeds.
        assert_eq!(blob0.delete("nope").await, Ok(None));
        // Deleting a key with an empty value indicates it did work but deleted
        // no bytes.
        blob0.set("empty", Bytes::new()).await?;
        assert_eq!(blob0.delete("empty").await, Ok(Some(0)));

        // Attempt to restore a key. Not all backends will be able to restore, but
        // we can confirm that our data is visible iff restore reported success.
        blob0.set("undelete", Bytes::from("data")).await?;
        // Restoring should always succeed when the key exists.
        blob0.restore("undelete").await?;
        assert_eq!(blob0.delete("undelete").await?, Some("data".len()));
        let expected = match blob0.restore("undelete").await {
            Ok(()) => Some(Bytes::from("data").into()),
            Err(ExternalError::Determinate(_)) => None,
            Err(other) => return Err(other),
        };
        assert_eq!(blob0.get("undelete").await?, expected);
        blob0.delete("undelete").await?;

        // Empty blob contains no keys.
        blob0.delete("k0a").await?;
        let mut blob_keys = get_keys(&blob0).await?;
        blob_keys.sort();
        assert_eq!(blob_keys, empty_keys);
        let mut blob_keys = get_keys(&blob1).await?;
        blob_keys.sort();
        assert_eq!(blob_keys, empty_keys);
        // Can reset a deleted key to some other value.
        blob0.set(k0, values[1].clone().into()).await?;
        assert_eq!(
            blob1.get(k0).await?.map(|s| s.into_contiguous()),
            Some(values[1].clone())
        );
        assert_eq!(
            blob0.get(k0).await?.map(|s| s.into_contiguous()),
            Some(values[1].clone())
        );

        // Insert multiple keys back to back and validate that we can list
        // them all out.
        let mut expected_keys = empty_keys;
        for i in 1..=5 {
            let key = format!("k{}", i);
            blob0.set(&key, values[0].clone().into()).await?;
            expected_keys.push(key);
        }

        // Blob contains the key we just inserted.
        let mut blob_keys = get_keys(&blob0).await?;
        blob_keys.sort();
        assert_eq!(blob_keys, keys(&expected_keys, &[k0]));
        let mut blob_keys = get_keys(&blob1).await?;
        blob_keys.sort();
        assert_eq!(blob_keys, keys(&expected_keys, &[k0]));

        // Insert multiple keys with a different prefix and validate that we can
        // list out keys by their prefix
        let mut expected_prefix_keys = vec![];
        for i in 1..=3 {
            let key = format!("k-prefix-{}", i);
            blob0.set(&key, values[0].clone().into()).await?;
            expected_prefix_keys.push(key);
        }
        let mut blob_keys = get_keys_with_prefix(&blob0, "k-prefix").await?;
        blob_keys.sort();
        assert_eq!(blob_keys, expected_prefix_keys);
        let mut blob_keys = get_keys_with_prefix(&blob0, "k").await?;
        blob_keys.sort();
        expected_keys.extend(expected_prefix_keys);
        expected_keys.sort();
        assert_eq!(blob_keys, expected_keys);

        // We can open a new blob to the same path and use it.
        let blob3 = new_fn("path0").await?;
        assert_eq!(
            blob3.get(k0).await?.map(|s| s.into_contiguous()),
            Some(values[1].clone())
        );

        Ok(())
    }

    /// Common test impl for different consensus implementations.
    pub async fn consensus_impl_test<
        C: Consensus,
        F: Future<Output = Result<C, ExternalError>>,
        NewFn: FnMut() -> F,
    >(
        mut new_fn: NewFn,
    ) -> Result<(), ExternalError> {
        let consensus = new_fn().await?;

        // Use a random key so independent runs of this test don't interfere
        // with each other.
        let key = Uuid::new_v4().to_string();

        // Starting value of consensus data is None.
        assert_eq!(consensus.head(&key).await, Ok(None));

        // Can scan a key that has no data.
        assert_eq!(consensus.scan(&key, SeqNo(0), SCAN_ALL).await, Ok(vec![]));

        // Cannot truncate data from a key that doesn't have any data
        assert_err!(consensus.truncate(&key, SeqNo(0)).await);

        let state = VersionedData {
            seqno: SeqNo(5),
            data: Bytes::from("abc"),
        };

        // Incorrectly setting the data with a non-None expected should fail.
        assert_eq!(
            consensus
                .compare_and_set(&key, Some(SeqNo(0)), state.clone())
                .await,
            Ok(CaSResult::ExpectationMismatch),
        );

        // Correctly updating the state with the correct expected value should succeed.
        assert_eq!(
            consensus.compare_and_set(&key, None, state.clone()).await,
            Ok(CaSResult::Committed),
        );

        // The new key is visible in state.
        let keys: Vec<_> = consensus.list_keys().try_collect().await?;
        assert_eq!(keys, vec![key.to_owned()]);

        // We can observe the a recent value on successful update.
        assert_eq!(consensus.head(&key).await, Ok(Some(state.clone())));

        // Can scan a key that has data with a lower bound sequence number < head.
        assert_eq!(
            consensus.scan(&key, SeqNo(0), SCAN_ALL).await,
            Ok(vec![state.clone()])
        );

        // Can scan a key that has data with a lower bound sequence number == head.
        assert_eq!(
            consensus.scan(&key, SeqNo(5), SCAN_ALL).await,
            Ok(vec![state.clone()])
        );

        // Can scan a key that has data with a lower bound sequence number >
        // head.
        assert_eq!(consensus.scan(&key, SeqNo(6), SCAN_ALL).await, Ok(vec![]));

        // Can truncate data with an upper bound <= head, even if there is no data in the
        // range [0, upper).
        assert_eq!(consensus.truncate(&key, SeqNo(0)).await, Ok(0));
        assert_eq!(consensus.truncate(&key, SeqNo(5)).await, Ok(0));

        // Cannot truncate data with an upper bound > head.
        assert_err!(consensus.truncate(&key, SeqNo(6)).await);

        let new_state = VersionedData {
            seqno: SeqNo(10),
            data: Bytes::from("def"),
        };

        // Trying to update without the correct expected seqno fails, (even if expected > current)
        assert_eq!(
            consensus
                .compare_and_set(&key, Some(SeqNo(7)), new_state.clone())
                .await,
            Ok(CaSResult::ExpectationMismatch),
        );

        // Trying to update without the correct expected seqno fails, (even if expected < current)
        assert_eq!(
            consensus
                .compare_and_set(&key, Some(SeqNo(3)), new_state.clone())
                .await,
            Ok(CaSResult::ExpectationMismatch),
        );

        let invalid_constant_seqno = VersionedData {
            seqno: SeqNo(5),
            data: Bytes::from("invalid"),
        };

        // Trying to set the data to a sequence number == current fails even if
        // expected is correct.
        assert_eq!(
            consensus
                .compare_and_set(&key, Some(state.seqno), invalid_constant_seqno)
                .await,
            Err(ExternalError::from(anyhow!(
                "new seqno must be strictly greater than expected. Got new: SeqNo(5) expected: SeqNo(5)"
            )))
        );

        let invalid_regressing_seqno = VersionedData {
            seqno: SeqNo(3),
            data: Bytes::from("invalid"),
        };

        // Trying to set the data to a sequence number < current fails even if
        // expected is correct.
        assert_eq!(
            consensus
                .compare_and_set(&key, Some(state.seqno), invalid_regressing_seqno)
                .await,
            Err(ExternalError::from(anyhow!(
                "new seqno must be strictly greater than expected. Got new: SeqNo(3) expected: SeqNo(5)"
            )))
        );

        // Can correctly update to a new state if we provide the right expected seqno
        assert_eq!(
            consensus
                .compare_and_set(&key, Some(state.seqno), new_state.clone())
                .await,
            Ok(CaSResult::Committed),
        );

        // We can observe the a recent value on successful update.
        assert_eq!(consensus.head(&key).await, Ok(Some(new_state.clone())));

        // We can observe both states in the correct order with scan if pass
        // in a suitable lower bound.
        assert_eq!(
            consensus.scan(&key, SeqNo(5), SCAN_ALL).await,
            Ok(vec![state.clone(), new_state.clone()])
        );

        // We can observe only the most recent state if the lower bound is higher
        // than the previous insertion's sequence number.
        assert_eq!(
            consensus.scan(&key, SeqNo(6), SCAN_ALL).await,
            Ok(vec![new_state.clone()])
        );

        // We can still observe the most recent insert as long as the provided
        // lower bound == most recent 's sequence number.
        assert_eq!(
            consensus.scan(&key, SeqNo(10), SCAN_ALL).await,
            Ok(vec![new_state.clone()])
        );

        // We can scan if the provided lower bound > head's sequence number.
        assert_eq!(consensus.scan(&key, SeqNo(11), SCAN_ALL).await, Ok(vec![]));

        // We can scan with limits that don't cover all states
        assert_eq!(
            consensus.scan(&key, SeqNo::minimum(), 1).await,
            Ok(vec![state.clone()])
        );
        assert_eq!(
            consensus.scan(&key, SeqNo(5), 1).await,
            Ok(vec![state.clone()])
        );

        // We can scan with limits to cover exactly the number of states
        assert_eq!(
            consensus.scan(&key, SeqNo::minimum(), 2).await,
            Ok(vec![state.clone(), new_state.clone()])
        );

        // We can scan with a limit larger than the number of states
        assert_eq!(
            consensus.scan(&key, SeqNo(4), 100).await,
            Ok(vec![state.clone(), new_state.clone()])
        );

        // Can remove the previous write with the appropriate truncation.
        if consensus.truncate_counts() {
            assert_eq!(consensus.truncate(&key, SeqNo(6)).await, Ok(1));
        } else {
            assert_ok!(consensus.truncate(&key, SeqNo(6)).await);
        }

        // Verify that the old write is indeed deleted.
        assert_eq!(
            consensus.scan(&key, SeqNo(0), SCAN_ALL).await,
            Ok(vec![new_state.clone()])
        );

        // Truncate is idempotent and can be repeated. The return value
        // indicates we didn't do any work though.
        if consensus.truncate_counts() {
            assert_eq!(consensus.truncate(&key, SeqNo(6)).await, Ok(0));
        } else {
            assert_ok!(consensus.truncate(&key, SeqNo(6)).await);
        }

        // Make sure entries under different keys don't clash.
        let other_key = Uuid::new_v4().to_string();

        assert_eq!(consensus.head(&other_key).await, Ok(None));

        let state = VersionedData {
            seqno: SeqNo(1),
            data: Bytes::from("einszweidrei"),
        };

        assert_eq!(
            consensus
                .compare_and_set(&other_key, None, state.clone())
                .await,
            Ok(CaSResult::Committed),
        );

        assert_eq!(consensus.head(&other_key).await, Ok(Some(state.clone())));

        // State for the first key is still as expected.
        assert_eq!(consensus.head(&key).await, Ok(Some(new_state.clone())));

        // Trying to update from a stale version of current doesn't work.
        let invalid_jump_forward = VersionedData {
            seqno: SeqNo(11),
            data: Bytes::from("invalid"),
        };
        assert_eq!(
            consensus
                .compare_and_set(&key, Some(state.seqno), invalid_jump_forward)
                .await,
            Ok(CaSResult::ExpectationMismatch),
        );

        // Writing a large (~10 KiB) amount of data works fine.
        let large_state = VersionedData {
            seqno: SeqNo(11),
            data: std::iter::repeat(b'a').take(10240).collect(),
        };
        assert_eq!(
            consensus
                .compare_and_set(&key, Some(new_state.seqno), large_state)
                .await,
            Ok(CaSResult::Committed),
        );

        // Truncate can delete more than one version at a time.
        let v12 = VersionedData {
            seqno: SeqNo(12),
            data: Bytes::new(),
        };
        assert_eq!(
            consensus.compare_and_set(&key, Some(SeqNo(11)), v12).await,
            Ok(CaSResult::Committed),
        );
        if consensus.truncate_counts() {
            assert_eq!(consensus.truncate(&key, SeqNo(12)).await, Ok(2));
        } else {
            assert_ok!(consensus.truncate(&key, SeqNo(12)).await);
        }

        // Sequence numbers used within Consensus have to be within [0, i64::MAX].

        assert_eq!(
            consensus
                .compare_and_set(
                    &Uuid::new_v4().to_string(),
                    None,
                    VersionedData {
                        seqno: SeqNo(0),
                        data: Bytes::new(),
                    }
                )
                .await,
            Ok(CaSResult::Committed),
        );
        assert_eq!(
            consensus
                .compare_and_set(
                    &Uuid::new_v4().to_string(),
                    None,
                    VersionedData {
                        seqno: SeqNo(i64::MAX.try_into().expect("i64::MAX fits in u64")),
                        data: Bytes::new(),
                    }
                )
                .await,
            Ok(CaSResult::Committed),
        );
        assert_err!(
            consensus
                .compare_and_set(
                    &Uuid::new_v4().to_string(),
                    None,
                    VersionedData {
                        seqno: SeqNo(1 << 63),
                        data: Bytes::new(),
                    }
                )
                .await
        );
        assert_err!(
            consensus
                .compare_and_set(
                    &Uuid::new_v4().to_string(),
                    None,
                    VersionedData {
                        seqno: SeqNo(u64::MAX),
                        data: Bytes::new(),
                    }
                )
                .await
        );

        Ok(())
    }

    #[mz_ore::test]
    fn timeout_error() {
        assert!(ExternalError::new_timeout(Instant::now()).is_timeout());
        assert!(!ExternalError::from(anyhow!("foo")).is_timeout());
    }
}
