// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::Deref;
use std::str::FromStr;
use uuid::Uuid;

use crate::r#impl::encoding::parse_id;
use crate::{ShardId, WriterId};

/// An opaque identifier for an individual blob of a persist durable TVC (aka shard).
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

/// Partially encoded path used in [mz_persist::location::Blob] storage.
/// Composed of a [crate::write::WriterId] and [crate::impl::paths::PartId]. Can
/// be completed with a [crate::ShardId] to form a full [crate::impl::paths::BlobKey].
///
/// Used to reduce the bytes needed to refer to a blob key in memory and
/// in persistent state, all access to blobs are always within the context
/// of an individual shard.
#[derive(Clone, Debug, PartialEq)]
pub struct PartialBlobKey(pub(crate) String);

impl PartialBlobKey {
    pub fn new(writer_id: &WriterId, part_id: &PartId) -> PartialBlobKey {
        PartialBlobKey(format!("{}/{}", writer_id, part_id))
    }

    pub fn complete(&self, shard_id: &ShardId) -> BlobKey {
        if self.starts_with('w') {
            BlobKey(format!("{}/{}", shard_id, self))
        } else {
            // the legacy key format is a plain UUID (which cannot start with 'w')
            // so it should not be prepended with the shard id.
            //
            // TODO: this can be removed when there are no more legacy keys.
            //       see [crate::impl::paths::BlobKey::parse_ids] for more details
            BlobKey(self.0.to_owned())
        }
    }
}

impl std::fmt::Display for PartialBlobKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl Deref for PartialBlobKey {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Fully encoded path used in [mz_persist::location::Blob] storage.
/// Composed of a [crate::ShardId], [crate::write::WriterId] and
/// [crate::impl::paths::PartId].
///
/// Use when directly interacting with a [mz_persist::location::Blob],
/// otherwise use [crate::impl::paths::PartialBlobKey] to refer to
/// a blob without needing to copy the [crate::ShardId].
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
    pub fn parse_ids(key: &str) -> Result<(Option<(ShardId, WriterId)>, PartId), String> {
        let ids = key.split('/').collect::<Vec<_>>();

        match ids[..] {
            [shard, writer, part] => Ok((
                Some((ShardId::from_str(shard)?, WriterId::from_str(writer)?)),
                PartId::from_str(part)?,
            )),
            // TODO: this blob key format can be removed (and return type updated to not include
            //       Option) once all deployed environments have been wiped clean after
            //       https://github.com/MaterializeInc/materialize/pull/13617 has been merged
            [part] => Ok((None, PartId::from_str(part)?)),
            _ => Err(format!("invalid blob key format. expected either <shard_id>/<writer_id>/<part_id> or legacy <part_id> format. got: {}", key)),
        }
    }
}

/// Represents the prefix of a blob path. Used for selecting subsets of blobs
#[derive(Debug)]
pub enum BlobKeyPrefix<'a> {
    /// For accessing all blobs
    #[allow(dead_code)]
    All,
    /// Scoped to the blobs of an individual shard
    Shard(&'a ShardId),
    /// Scoped to the blobs of an individual writer
    #[allow(dead_code)]
    Writer(&'a ShardId, &'a WriterId),
}

impl std::fmt::Display for BlobKeyPrefix<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            BlobKeyPrefix::All => "".into(),
            BlobKeyPrefix::Shard(shard) => format!("{}", shard),
            BlobKeyPrefix::Writer(shard, writer) => format!("{}/{}", shard, writer),
        };
        f.write_str(&s)
    }
}

#[cfg(test)]
mod tests {
    use crate::r#impl::paths::BlobKey;

    use super::*;

    #[test]
    fn partial_blob_key_completion() {
        let (shard_id, writer_id, part_id) = (ShardId::new(), WriterId::new(), PartId::new());
        let partial_key = PartialBlobKey::new(&writer_id, &part_id);
        assert_eq!(
            partial_key.complete(&shard_id),
            BlobKey(format!("{}/{}/{}", shard_id, writer_id, part_id))
        );

        let partial_key = PartialBlobKey("random-legacy-key".to_string());
        assert_eq!(
            partial_key.complete(&shard_id),
            BlobKey("random-legacy-key".to_string())
        );
    }

    #[test]
    fn blob_key_parse() -> Result<(), String> {
        let (shard_id, writer_id, part_id) = (ShardId::new(), WriterId::new(), PartId::new());

        // can parse full blob key
        assert_eq!(
            BlobKey::parse_ids(&format!("{}/{}/{}", shard_id, writer_id, part_id)),
            Ok((Some((shard_id, writer_id)), part_id.clone()))
        );

        // can parse legacy blob key
        assert_eq!(
            BlobKey::parse_ids(&format!("{}", part_id)),
            Ok((None, part_id))
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
