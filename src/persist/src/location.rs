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
use std::time::Instant;

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use mz_persist_types::Codec;
use serde::{Deserialize, Serialize};
use tokio_postgres::error::SqlState;

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
#[derive(Clone, Copy, Debug, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

impl SeqNo {
    /// Returns the next SeqNo in the sequence.
    pub fn next(self) -> SeqNo {
        SeqNo(self.0 + 1)
    }

    /// A minimum value suitable as a default.
    pub fn minimum() -> Self {
        SeqNo(0)
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

impl std::error::Error for Determinate {}

impl Determinate {
    /// Return a new Determinate wrapping the given error.
    ///
    /// Exposed for testing via [crate::unreliable].
    pub(crate) fn new(inner: anyhow::Error) -> Self {
        Determinate { inner }
    }
}

/// An error coming from an underlying durability system (e.g. s3) indicating
/// that the operation _might have succeeded_ (e.g. timeout).
#[derive(Debug)]
pub struct Indeterminate {
    pub(crate) inner: anyhow::Error,
}

impl std::fmt::Display for Indeterminate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "indeterminate: ")?;
        self.inner.fmt(f)
    }
}

impl std::error::Error for Indeterminate {}

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

impl std::error::Error for ExternalError {}

/// An impl of PartialEq purely for convenience in tests and debug assertions.
#[cfg(any(test, debug_assertions))]
impl PartialEq for ExternalError {
    fn eq(&self, other: &Self) -> bool {
        self.to_string() == other.to_string()
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

impl From<tokio_postgres::Error> for ExternalError {
    fn from(e: tokio_postgres::Error) -> Self {
        let code = match e.as_db_error().map(|x| x.code()) {
            Some(x) => x,
            None => {
                return ExternalError::Indeterminate(Indeterminate {
                    inner: anyhow::Error::new(e),
                })
            }
        };
        match code {
            // Feel free to add more things to this whitelist as we encounter
            // them as long as you're certain they're determinate.
            &SqlState::T_R_SERIALIZATION_FAILURE => ExternalError::Determinate(Determinate {
                inner: anyhow::Error::new(e),
            }),
            _ => ExternalError::Indeterminate(Indeterminate {
                inner: anyhow::Error::new(e),
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

/// Configuration of whether a [BlobMulti::set] must occur atomically.
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

/// An abstraction for a single arbitrarily-sized binary blob and a associated
/// version number (sequence number).
#[derive(Debug, Clone, PartialEq)]
pub struct VersionedData {
    /// The sequence number of the data.
    pub seqno: SeqNo,
    /// The data itself.
    pub data: Bytes,
}

impl<T: Codec> From<(SeqNo, &T)> for VersionedData {
    fn from(x: (SeqNo, &T)) -> Self {
        let (seqno, t) = x;
        let mut data = BytesMut::new();
        Codec::encode(t, &mut data);
        VersionedData {
            seqno,
            data: Bytes::from(data),
        }
    }
}

impl<T: Codec> TryFrom<&VersionedData> for (SeqNo, T) {
    type Error = ExternalError;

    fn try_from(x: &VersionedData) -> Result<Self, Self::Error> {
        let t = T::decode(&x.data).map_err(|err| {
            ExternalError::from(anyhow!(
                "invalid {} at {}: {}",
                T::codec_name(),
                x.seqno,
                err
            ))
        })?;
        Ok((x.seqno, t))
    }
}

/// An abstraction for [VersionedData] held in a location in persistent storage
/// where the data are conditionally updated by version.
///
/// Users are expected to use this API with consistently increasing sequence numbers
/// to allow multiple processes across multiple machines to agree to a total order
/// of the evolution of the data. To make roundtripping through various forms of durable
/// storage easier, sequence numbers used with [Consensus] need to be restricted to the
/// range [0, i64::MAX].
#[async_trait]
pub trait Consensus: std::fmt::Debug {
    /// Returns a recent version of `data`, and the corresponding sequence number, if
    /// one exists at this location.
    async fn head(
        &self,
        deadline: Instant,
        key: &str,
    ) -> Result<Option<VersionedData>, ExternalError>;

    /// Update the [VersionedData] stored at this location to `new`, iff the current
    /// sequence number is exactly `expected` and `new`'s sequence number > the current
    /// sequence number.
    ///
    /// Returns a recent version and data from this location iff the current sequence
    /// number does not equal `expected` or if `new`'s sequence number is less than or
    /// equal to the current sequence number. It is invalid to call this function with
    /// a `new` and `expected` such that `new`'s sequence number is <= `expected`.
    /// It is invalid to call this function with a sequence number outside of the range
    /// [0, i64::MAX].
    ///
    /// This data is initialized to None, and the first call to compare_and_set needs to
    /// happen with None as the expected value to set the state.
    async fn compare_and_set(
        &self,
        deadline: Instant,
        key: &str,
        expected: Option<SeqNo>,
        new: VersionedData,
    ) -> Result<Result<(), Option<VersionedData>>, ExternalError>;

    /// Return all versions of data stored for this `key` at sequence numbers
    /// >= `from`, in ascending order of sequence number.
    ///
    /// Returns an error if `from` is greater than the current sequence number
    /// or if there is no data at this key.
    async fn scan(
        &self,
        deadline: Instant,
        key: &str,
        from: SeqNo,
    ) -> Result<Vec<VersionedData>, ExternalError>;

    /// Deletes all historical versions of the data stored at `key` that are < `seqno`,
    /// iff `seqno` <= the current sequence number.
    ///
    /// Returns an error if `seqno` is greater than the current sequence number,
    /// or if there is no data at this key.
    async fn truncate(
        &self,
        deadline: Instant,
        key: &str,
        seqno: SeqNo,
    ) -> Result<(), ExternalError>;
}

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
///
/// TODO: Rename this to Blob when we delete Blob.
#[async_trait]
pub trait BlobMulti: std::fmt::Debug {
    /// Returns a reference to the value corresponding to the key.
    async fn get(&self, deadline: Instant, key: &str) -> Result<Option<Vec<u8>>, ExternalError>;

    /// List all of the keys in the map.
    async fn list_keys(&self, deadline: Instant) -> Result<Vec<String>, ExternalError>;

    /// Inserts a key-value pair into the map.
    ///
    /// When atomicity is required, writes must be atomic and either succeed or
    /// leave the previous value intact.
    async fn set(
        &self,
        deadline: Instant,
        key: &str,
        value: Bytes,
        atomic: Atomicity,
    ) -> Result<(), ExternalError>;

    /// Remove a key from the map.
    ///
    /// Succeeds if the key does not exist.
    async fn delete(&self, deadline: Instant, key: &str) -> Result<(), ExternalError>;
}

#[cfg(test)]
pub mod tests {
    use std::future::Future;
    use std::time::Duration;

    use anyhow::anyhow;
    use uuid::Uuid;

    use crate::location::Atomicity::{AllowNonAtomic, RequireAtomic};

    use super::*;

    fn keys(baseline: &[String], new: &[&str]) -> Vec<String> {
        let mut ret = baseline.to_vec();
        ret.extend(new.iter().map(|x| x.to_string()));
        ret.sort();
        ret
    }

    pub async fn blob_multi_impl_test<
        B: BlobMulti,
        F: Future<Output = Result<B, ExternalError>>,
        NewFn: Fn(&'static str) -> F,
    >(
        new_fn: NewFn,
    ) -> Result<(), ExternalError> {
        let no_timeout = Instant::now() + Duration::from_secs(1_000_000);
        let values = vec!["v0".as_bytes().to_vec(), "v1".as_bytes().to_vec()];

        let blob0 = new_fn("path0").await?;

        // We can create a second blob writing to a different place.
        let _ = new_fn("path1").await?;

        // We can open two blobs to the same place, even.
        let blob1 = new_fn("path0").await?;

        // Empty key is empty.
        assert_eq!(blob0.get(no_timeout, "k0").await?, None);
        assert_eq!(blob1.get(no_timeout, "k0").await?, None);

        // Empty list keys is empty.
        let empty_keys = blob0.list_keys(no_timeout).await?;
        assert_eq!(empty_keys, Vec::<String>::new());
        let empty_keys = blob1.list_keys(no_timeout).await?;
        assert_eq!(empty_keys, Vec::<String>::new());

        // Set a key with AllowNonAtomic and get it back.
        blob0
            .set(no_timeout, "k0", values[0].clone().into(), AllowNonAtomic)
            .await?;
        assert_eq!(blob0.get(no_timeout, "k0").await?, Some(values[0].clone()));
        assert_eq!(blob1.get(no_timeout, "k0").await?, Some(values[0].clone()));

        // Set a key with RequireAtomic and get it back.
        blob0
            .set(no_timeout, "k0a", values[0].clone().into(), RequireAtomic)
            .await?;
        assert_eq!(blob0.get(no_timeout, "k0a").await?, Some(values[0].clone()));
        assert_eq!(blob1.get(no_timeout, "k0a").await?, Some(values[0].clone()));

        // Blob contains the key we just inserted.
        let mut blob_keys = blob0.list_keys(no_timeout).await?;
        blob_keys.sort();
        assert_eq!(blob_keys, keys(&empty_keys, &["k0", "k0a"]));
        let mut blob_keys = blob1.list_keys(no_timeout).await?;
        blob_keys.sort();
        assert_eq!(blob_keys, keys(&empty_keys, &["k0", "k0a"]));

        // Can overwrite a key with AllowNonAtomic.
        blob0
            .set(no_timeout, "k0", values[1].clone().into(), AllowNonAtomic)
            .await?;
        assert_eq!(blob0.get(no_timeout, "k0").await?, Some(values[1].clone()));
        assert_eq!(blob1.get(no_timeout, "k0").await?, Some(values[1].clone()));
        // Can overwrite a key with RequireAtomic.
        blob0
            .set(no_timeout, "k0a", values[1].clone().into(), RequireAtomic)
            .await?;
        assert_eq!(blob0.get(no_timeout, "k0a").await?, Some(values[1].clone()));
        assert_eq!(blob1.get(no_timeout, "k0a").await?, Some(values[1].clone()));

        // Can delete a key.
        blob0.delete(no_timeout, "k0").await?;
        // Can no longer get a deleted key.
        assert_eq!(blob0.get(no_timeout, "k0").await?, None);
        assert_eq!(blob1.get(no_timeout, "k0").await?, None);
        // Double deleting a key succeeds.
        assert_eq!(blob0.delete(no_timeout, "k0").await, Ok(()));
        // Deleting a key that does not exist succeeds.
        assert_eq!(blob0.delete(no_timeout, "nope").await, Ok(()));

        // Empty blob contains no keys.
        blob0.delete(no_timeout, "k0a").await?;
        let mut blob_keys = blob0.list_keys(no_timeout).await?;
        blob_keys.sort();
        assert_eq!(blob_keys, empty_keys);
        let mut blob_keys = blob1.list_keys(no_timeout).await?;
        blob_keys.sort();
        assert_eq!(blob_keys, empty_keys);
        // Can reset a deleted key to some other value.
        blob0
            .set(no_timeout, "k0", values[1].clone().into(), AllowNonAtomic)
            .await?;
        assert_eq!(blob1.get(no_timeout, "k0").await?, Some(values[1].clone()));
        assert_eq!(blob0.get(no_timeout, "k0").await?, Some(values[1].clone()));

        // Insert multiple keys back to back and validate that we can list
        // them all out.
        let mut expected_keys = empty_keys;
        for i in 1..=5 {
            let key = format!("k{}", i);
            blob0
                .set(no_timeout, &key, values[0].clone().into(), AllowNonAtomic)
                .await?;
            expected_keys.push(key);
        }

        // Blob contains the key we just inserted.
        let mut blob_keys = blob0.list_keys(no_timeout).await?;
        blob_keys.sort();
        assert_eq!(blob_keys, keys(&expected_keys, &["k0"]));
        let mut blob_keys = blob1.list_keys(no_timeout).await?;
        blob_keys.sort();
        assert_eq!(blob_keys, keys(&expected_keys, &["k0"]));

        // We can open a new blob to the same path and use it.
        let blob3 = new_fn("path0").await?;
        assert_eq!(blob3.get(no_timeout, "k0").await?, Some(values[1].clone()));

        Ok(())
    }

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

        // Enforce that this entire test completes within 10 minutes.
        let deadline = Instant::now() + Duration::from_secs(600);

        // Starting value of consensus data is None.
        assert_eq!(consensus.head(deadline, &key).await, Ok(None));

        // Cannot scan a key that has no data.
        assert!(consensus.scan(deadline, &key, SeqNo(0)).await.is_err());

        // Cannot truncate data from a key that doesn't have any data
        assert!(consensus.truncate(deadline, &key, SeqNo(0)).await.is_err(),);

        let state = VersionedData {
            seqno: SeqNo(5),
            data: Bytes::from("abc"),
        };

        // Incorrectly setting the data with a non-None expected should fail.
        assert_eq!(
            consensus
                .compare_and_set(deadline, &key, Some(SeqNo(0)), state.clone())
                .await,
            Ok(Err(None))
        );

        // Correctly updating the state with the correct expected value should succeed.
        assert_eq!(
            consensus
                .compare_and_set(deadline, &key, None, state.clone())
                .await,
            Ok(Ok(()))
        );

        // We can observe the a recent value on successful update.
        assert_eq!(
            consensus.head(deadline, &key).await,
            Ok(Some(state.clone()))
        );

        // Can scan a key that has data with a lower bound sequence number < head.
        assert_eq!(
            consensus.scan(deadline, &key, SeqNo(0)).await,
            Ok(vec![state.clone()])
        );

        // Can scan a key that has data with a lower bound sequence number == head.
        assert_eq!(
            consensus.scan(deadline, &key, SeqNo(5)).await,
            Ok(vec![state.clone()])
        );

        // Cannot scan a key that has data with a lower bound sequence number > head.
        assert!(consensus.scan(deadline, &key, SeqNo(6)).await.is_err());

        // Can truncate data with an upper bound <= head, even if there is no data in the
        // range [0, upper).
        assert_eq!(consensus.truncate(deadline, &key, SeqNo(0)).await, Ok(()));
        assert_eq!(consensus.truncate(deadline, &key, SeqNo(5)).await, Ok(()));

        // Cannot truncate data with an upper bound > head.
        assert!(consensus.truncate(deadline, &key, SeqNo(6)).await.is_err(),);

        let new_state = VersionedData {
            seqno: SeqNo(10),
            data: Bytes::from("def"),
        };

        // Trying to update without the correct expected seqno fails, (even if expected > current)
        assert_eq!(
            consensus
                .compare_and_set(deadline, &key, Some(SeqNo(7)), new_state.clone())
                .await,
            Ok(Err(Some(state.clone())))
        );

        // Trying to update without the correct expected seqno fails, (even if expected < current)
        assert_eq!(
            consensus
                .compare_and_set(deadline, &key, Some(SeqNo(3)), new_state.clone())
                .await,
            Ok(Err(Some(state.clone())))
        );

        let invalid_constant_seqno = VersionedData {
            seqno: SeqNo(5),
            data: Bytes::from("invalid"),
        };

        // Trying to set the data to a sequence number == current fails even if
        // expected is correct.
        assert_eq!(
            consensus
                .compare_and_set(deadline, &key, Some(state.seqno), invalid_constant_seqno)
                .await,
            Err(ExternalError::from(anyhow!("new seqno must be strictly greater than expected. Got new: SeqNo(5) expected: SeqNo(5)")))
        );

        let invalid_regressing_seqno = VersionedData {
            seqno: SeqNo(3),
            data: Bytes::from("invalid"),
        };

        // Trying to set the data to a sequence number < current fails even if
        // expected is correct.
        assert_eq!(
            consensus
                .compare_and_set(deadline, &key, Some(state.seqno), invalid_regressing_seqno)
                .await,
            Err(ExternalError::from(anyhow!("new seqno must be strictly greater than expected. Got new: SeqNo(3) expected: SeqNo(5)")))
        );

        // Can correctly update to a new state if we provide the right expected seqno
        assert_eq!(
            consensus
                .compare_and_set(deadline, &key, Some(state.seqno), new_state.clone())
                .await,
            Ok(Ok(()))
        );

        // We can observe the a recent value on successful update.
        assert_eq!(
            consensus.head(deadline, &key).await,
            Ok(Some(new_state.clone()))
        );

        // We can observe both states in the correct order with scan if pass
        // in a suitable lower bound.
        assert_eq!(
            consensus.scan(deadline, &key, SeqNo(5)).await,
            Ok(vec![state.clone(), new_state.clone()])
        );

        // We can observe only the most recent state if the lower bound is higher
        // than the previous insertion's sequence number.
        assert_eq!(
            consensus.scan(deadline, &key, SeqNo(6)).await,
            Ok(vec![new_state.clone()])
        );

        // We can still observe the most recent insert as long as the provided
        // lower bound == most recent 's sequence number.
        assert_eq!(
            consensus.scan(deadline, &key, SeqNo(10)).await,
            Ok(vec![new_state.clone()])
        );

        // We cannot scan if the provided lower bound > head's sequence number.
        assert!(consensus.scan(deadline, &key, SeqNo(11)).await.is_err());

        // Can remove the previous write with the appropriate truncation.
        assert_eq!(consensus.truncate(deadline, &key, SeqNo(6)).await, Ok(()));

        // Verify that the old write is indeed deleted.
        assert_eq!(
            consensus.scan(deadline, &key, SeqNo(0)).await,
            Ok(vec![new_state.clone()])
        );

        // Truncate is idempotent and can be repeated.
        assert_eq!(consensus.truncate(deadline, &key, SeqNo(6)).await, Ok(()));

        // Make sure entries under different keys don't clash.
        let other_key = Uuid::new_v4().to_string();

        assert_eq!(consensus.head(deadline, &other_key).await, Ok(None));

        let state = VersionedData {
            seqno: SeqNo(1),
            data: Bytes::from("einszweidrei"),
        };

        assert_eq!(
            consensus
                .compare_and_set(deadline, &other_key, None, state.clone())
                .await,
            Ok(Ok(()))
        );

        assert_eq!(
            consensus.head(deadline, &other_key).await,
            Ok(Some(state.clone()))
        );

        // State for the first key is still as expected.
        assert_eq!(
            consensus.head(deadline, &key).await,
            Ok(Some(new_state.clone()))
        );

        // Trying to update from a stale version of current doesn't work.
        let invalid_jump_forward = VersionedData {
            seqno: SeqNo(11),
            data: Bytes::from("invalid"),
        };
        assert_eq!(
            consensus
                .compare_and_set(deadline, &key, Some(state.seqno), invalid_jump_forward)
                .await,
            Ok(Err(Some(new_state.clone())))
        );

        // Writing a large (~10 KiB) amount of data works fine.
        let large_state = VersionedData {
            seqno: SeqNo(11),
            data: std::iter::repeat('a' as u8).take(10240).collect(),
        };
        assert_eq!(
            consensus
                .compare_and_set(deadline, &key, Some(new_state.seqno), large_state)
                .await,
            Ok(Ok(()))
        );

        // Sequence numbers used within Consensus have to be within [0, i64::MAX].

        assert_eq!(
            consensus
                .compare_and_set(
                    deadline,
                    &Uuid::new_v4().to_string(),
                    None,
                    VersionedData {
                        seqno: SeqNo(0),
                        data: Bytes::new(),
                    }
                )
                .await,
            Ok(Ok(()))
        );
        assert_eq!(
            consensus
                .compare_and_set(
                    deadline,
                    &Uuid::new_v4().to_string(),
                    None,
                    VersionedData {
                        seqno: SeqNo(i64::MAX.try_into().expect("i64::MAX fits in u64")),
                        data: Bytes::new(),
                    }
                )
                .await,
            Ok(Ok(()))
        );
        assert!(consensus
            .compare_and_set(
                deadline,
                &Uuid::new_v4().to_string(),
                None,
                VersionedData {
                    seqno: SeqNo(1 << 63),
                    data: Bytes::new(),
                }
            )
            .await
            .is_err());
        assert!(consensus
            .compare_and_set(
                deadline,
                &Uuid::new_v4().to_string(),
                None,
                VersionedData {
                    seqno: SeqNo(u64::MAX),
                    data: Bytes::new(),
                }
            )
            .await
            .is_err());

        Ok(())
    }

    #[test]
    fn timeout_error() {
        assert!(ExternalError::new_timeout(Instant::now()).is_timeout());
        assert!(!ExternalError::from(anyhow!("foo")).is_timeout());
    }
}
