// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types related to storage clusters.

use std::fmt;
use std::str::FromStr;

use anyhow::bail;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use mz_proto::{RustType, TryFromProtoError};

include!(concat!(
    env!("OUT_DIR"),
    "/mz_storage_client.types.clusters.rs"
));

/// Identifier of a storage cluster.
#[derive(
    Arbitrary, Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize,
)]
pub enum StorageClusterId {
    /// A system storage cluster.
    System(u64),
    /// A user storage cluster.
    User(u64),
}

impl StorageClusterId {
    pub fn inner_id(&self) -> u64 {
        match self {
            StorageClusterId::System(id) | StorageClusterId::User(id) => *id,
        }
    }

    pub fn is_user(&self) -> bool {
        matches!(self, Self::User(_))
    }

    pub fn is_system(&self) -> bool {
        matches!(self, Self::System(_))
    }
}

impl FromStr for StorageClusterId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() < 2 {
            bail!("couldn't parse compute instance id {}", s);
        }
        let val: u64 = s[1..].parse()?;
        match s.chars().next().unwrap() {
            's' => Ok(Self::System(val)),
            'u' => Ok(Self::User(val)),
            _ => bail!("couldn't parse compute instance id {}", s),
        }
    }
}

impl fmt::Display for StorageClusterId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::System(id) => write!(f, "s{}", id),
            Self::User(id) => write!(f, "u{}", id),
        }
    }
}

impl RustType<ProtoStorageClusterId> for StorageClusterId {
    fn into_proto(&self) -> ProtoStorageClusterId {
        use proto_storage_cluster_id::Kind::*;
        ProtoStorageClusterId {
            kind: Some(match self {
                StorageClusterId::System(x) => System(*x),
                StorageClusterId::User(x) => User(*x),
            }),
        }
    }

    fn from_proto(proto: ProtoStorageClusterId) -> Result<Self, TryFromProtoError> {
        use proto_storage_cluster_id::Kind::*;
        match proto.kind {
            Some(System(x)) => Ok(StorageClusterId::System(x)),
            Some(User(x)) => Ok(StorageClusterId::User(x)),
            None => Err(TryFromProtoError::missing_field(
                "ProtoStorageClusterId::kind",
            )),
        }
    }
}
