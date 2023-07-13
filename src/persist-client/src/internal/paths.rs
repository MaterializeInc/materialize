// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::{Display, Formatter, Write};
use std::ops::Deref;
use std::str::FromStr;

use mz_persist::location::SeqNo;
use proptest_derive::Arbitrary;
use semver::Version;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::internal::encoding::parse_id;
use crate::{ShardId, WriterId};

/// An opaque identifier for an individual batch of a persist durable TVC (aka
/// shard).
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct PartId(pub(crate) [u8; 16]);

impl std::fmt::Display for PartId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "p{}", Uuid::from_bytes(self.0))
    }
}

impl std::fmt::Debug for PartId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PartId({})", Uuid::from_bytes(self.0))
    }
}

impl FromStr for PartId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_id('p', "PartId", s).map(PartId)
    }
}

impl PartId {
    pub(crate) fn new() -> Self {
        PartId(*Uuid::new_v4().as_bytes())
    }
}

/// A component that provides information about the writer of a blob.
/// For older blobs, this is a UUID for the specific writer instance;
/// for newer blobs, this is a string representing the version at which the blob was written.
/// In either case, it's used to help determine whether a blob may eventually
/// be linked into state, or whether it's junk that we can clean up.
/// Note that the ordering is meaningful: all writer-id keys are considered smaller than
/// all version keys.
#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone)]
pub enum WriterKey {
    Id(WriterId),
    Version(String),
}

impl WriterKey {
    /// This uses the version numbering scheme specified in [mz_build_info::BuildInfo::version_num].
    /// (And asserts that the version isn't so large that that scheme is no longer sufficient.)
    pub fn for_version(version: &Version) -> WriterKey {
        assert!(version.major <= 99);
        assert!(version.minor <= 999);
        assert!(version.patch <= 99);
        WriterKey::Version(format!(
            "{:02}{:03}{:02}",
            version.major, version.minor, version.patch
        ))
    }
}

impl FromStr for WriterKey {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err("empty version string".to_owned());
        }

        let key = match &s[..1] {
            "w" => WriterKey::Id(WriterId::from_str(s)?),
            "n" => WriterKey::Version(s[1..].to_owned()),
            c => {
                return Err(format!("unknown prefix for version: {c}"));
            }
        };
        Ok(key)
    }
}

impl Display for WriterKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            WriterKey::Id(id) => id.fmt(f),
            WriterKey::Version(s) => {
                f.write_char('n')?;
                f.write_str(s)
            }
        }
    }
}

/// Partially encoded path used in [mz_persist::location::Blob] storage.
/// Composed of a [WriterId] and [PartId]. Can be completed with a [ShardId] to
/// form a full [BlobKey].
///
/// Used to reduce the bytes needed to refer to a blob key in memory and in
/// persistent state, all access to blobs are always within the context of an
/// individual shard.
#[derive(Arbitrary, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PartialBatchKey(pub(crate) String);

impl PartialBatchKey {
    pub fn new(version: &WriterKey, part_id: &PartId) -> Self {
        PartialBatchKey(format!("{}/{}", version, part_id))
    }

    pub fn complete(&self, shard_id: &ShardId) -> BlobKey {
        BlobKey(format!("{}/{}", shard_id, self))
    }
}

impl std::fmt::Display for PartialBatchKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl Deref for PartialBatchKey {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// An opaque identifier for an individual blob of a persist durable TVC (aka shard).
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct RollupId(pub(crate) [u8; 16]);

impl std::fmt::Display for RollupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "r{}", Uuid::from_bytes(self.0))
    }
}

impl std::fmt::Debug for RollupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RollupId({})", Uuid::from_bytes(self.0))
    }
}

impl FromStr for RollupId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_id('r', "RollupId", s).map(RollupId)
    }
}

impl RollupId {
    pub(crate) fn new() -> Self {
        RollupId(*Uuid::new_v4().as_bytes())
    }
}

/// Partially encoded path used in [mz_persist::location::Blob] storage.
/// Composed of a [SeqNo] and [RollupId]. Can be completed with a [ShardId] to
/// form a full [BlobKey].
///
/// Used to reduce the bytes needed to refer to a blob key in memory and in
/// persistent state, all access to blobs are always within the context of an
/// individual shard.
#[derive(Arbitrary, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PartialRollupKey(pub(crate) String);

impl PartialRollupKey {
    pub fn new(seqno: SeqNo, rollup_id: &RollupId) -> Self {
        PartialRollupKey(format!("{}/{}", seqno, rollup_id))
    }

    pub fn complete(&self, shard_id: &ShardId) -> BlobKey {
        BlobKey(format!("{}/{}", shard_id, self))
    }
}

impl std::fmt::Display for PartialRollupKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl Deref for PartialRollupKey {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// A parsed, partial path used in [mz_persist::location::Blob] storage.
///
/// This enumerates all types of partial blob keys used in persist.
#[derive(Debug, PartialEq)]
pub enum PartialBlobKey {
    /// A parsed [PartialBatchKey].
    Batch(WriterKey, PartId),
    /// A parsed [PartialRollupKey].
    Rollup(SeqNo, RollupId),
}

/// Fully encoded path used in [mz_persist::location::Blob] storage. Composed of
/// a [ShardId], [WriterId] and [PartId].
///
/// Use when directly interacting with a [mz_persist::location::Blob], otherwise
/// use [PartialBatchKey] or [PartialRollupKey] to refer to a blob without
/// needing to copy the [ShardId].
#[derive(Clone, Debug, PartialEq)]
pub struct BlobKey(String);

impl std::fmt::Display for BlobKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl Deref for BlobKey {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl BlobKey {
    pub fn parse_ids(key: &str) -> Result<(ShardId, PartialBlobKey), String> {
        let ids = key.split('/').collect::<Vec<_>>();

        match ids[..] {
            [shard, writer, part] if writer.starts_with('w') | writer.starts_with('n') => Ok(
                (ShardId::from_str(shard)?, PartialBlobKey::Batch(WriterKey::from_str(writer)?, PartId::from_str(part)?))
            ),
            [shard, seqno, rollup] if seqno.starts_with('v') => Ok(
                (ShardId::from_str(shard)?, PartialBlobKey::Rollup(SeqNo::from_str(seqno)?, RollupId::from_str(rollup)?))
            ),
            _ => Err(format!("invalid blob key format. expected either <shard_id>/<writer_id>/<part_id> or <shard_id>/<seqno>/<rollup_id>. got: {}", key)),
        }
    }
}

/// Represents the prefix of a blob path. Used for selecting subsets of blobs
#[derive(Debug)]
pub enum BlobKeyPrefix<'a> {
    /// For accessing all blobs
    All,
    /// Scoped to the batch and state rollup blobs of an individual shard
    Shard(&'a ShardId),
    /// Scoped to the batch blobs of an individual writer
    #[cfg(test)]
    Writer(&'a ShardId, &'a WriterKey),
    /// Scoped to all state rollup blobs  of an individual shard
    #[cfg(test)]
    Rollups(&'a ShardId),
}

impl std::fmt::Display for BlobKeyPrefix<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            BlobKeyPrefix::All => "".into(),
            BlobKeyPrefix::Shard(shard) => format!("{}", shard),
            #[cfg(test)]
            BlobKeyPrefix::Writer(shard, writer) => format!("{}/{}", shard, writer),
            #[cfg(test)]
            BlobKeyPrefix::Rollups(shard) => format!("{}/v", shard),
        };
        f.write_str(&s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use semver::Version;

    fn gen_version() -> impl Strategy<Value = Version> {
        (0u64..=99, 0u64..=999, 0u64..=99)
            .prop_map(|(major, minor, patch)| Version::new(major, minor, patch))
    }

    #[mz_ore::test]
    fn key_ordering_compatible() {
        // The WriterKey's ordering should never disagree with the Version ordering.
        // (Though the writer key might compare equal when the version does not.)
        proptest!(|(a in gen_version(), b in gen_version())| {
            let a_key = WriterKey::for_version(&a);
            let b_key = WriterKey::for_version(&b);
            if a >= b {
                assert!(a_key >= b_key);
            }
            if a <= b {
                assert!(a_key <= b_key);
            }
        })
    }

    #[mz_ore::test]
    fn partial_blob_key_completion() {
        let (shard_id, writer_id, part_id) = (ShardId::new(), WriterId::new(), PartId::new());
        let partial_key = PartialBatchKey::new(&WriterKey::Id(writer_id.clone()), &part_id);
        assert_eq!(
            partial_key.complete(&shard_id),
            BlobKey(format!("{}/{}/{}", shard_id, writer_id, part_id))
        );
    }

    #[mz_ore::test]
    fn blob_key_parse() -> Result<(), String> {
        let (shard_id, writer_id, part_id) = (ShardId::new(), WriterId::new(), PartId::new());

        // can parse full blob key
        assert_eq!(
            BlobKey::parse_ids(&format!("{}/{}/{}", shard_id, writer_id, part_id)),
            Ok((
                shard_id,
                PartialBlobKey::Batch(WriterKey::Id(writer_id), part_id)
            ))
        );

        // fails on invalid blob key formats
        assert!(matches!(
            BlobKey::parse_ids(&format!("{}/{}", WriterId::new(), PartId::new())),
            Err(_)
        ));
        assert!(matches!(
            BlobKey::parse_ids(&format!(
                "{}/{}/{}/{}",
                ShardId::new(),
                WriterId::new(),
                PartId::new(),
                PartId::new()
            )),
            Err(_)
        ));
        assert!(matches!(BlobKey::parse_ids("abc/def/ghi"), Err(_)));
        assert!(matches!(BlobKey::parse_ids(""), Err(_)));

        // fails if shard/writer/part id are in the wrong spots
        assert!(matches!(
            BlobKey::parse_ids(&format!(
                "{}/{}/{}",
                PartId::new(),
                ShardId::new(),
                WriterId::new()
            )),
            Err(_)
        ));

        Ok(())
    }
}
