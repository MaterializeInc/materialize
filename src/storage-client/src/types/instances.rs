// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types related to storage instances.

use std::fmt;
use std::str::FromStr;

use anyhow::bail;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use mz_proto::{RustType, TryFromProtoError};

include!(concat!(
    env!("OUT_DIR"),
    "/mz_storage_client.types.instances.rs"
));

/// Identifier of a storage instance.
#[derive(
    Arbitrary, Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize,
)]
pub enum StorageInstanceId {
    /// A system storage instance.
    System(u64),
    /// A user storage instance.
    User(u64),
}

impl StorageInstanceId {
    pub fn inner_id(&self) -> u64 {
        match self {
            StorageInstanceId::System(id) | StorageInstanceId::User(id) => *id,
        }
    }

    pub fn is_user(&self) -> bool {
        matches!(self, Self::User(_))
    }

    pub fn is_system(&self) -> bool {
        matches!(self, Self::System(_))
    }
}

impl FromStr for StorageInstanceId {
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

impl fmt::Display for StorageInstanceId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::System(id) => write!(f, "s{}", id),
            Self::User(id) => write!(f, "u{}", id),
        }
    }
}

impl RustType<ProtoStorageInstanceId> for StorageInstanceId {
    fn into_proto(&self) -> ProtoStorageInstanceId {
        use proto_storage_instance_id::Kind::*;
        ProtoStorageInstanceId {
            kind: Some(match self {
                StorageInstanceId::System(x) => System(*x),
                StorageInstanceId::User(x) => User(*x),
            }),
        }
    }

    fn from_proto(proto: ProtoStorageInstanceId) -> Result<Self, TryFromProtoError> {
        use proto_storage_instance_id::Kind::*;
        match proto.kind {
            Some(System(x)) => Ok(StorageInstanceId::System(x)),
            Some(User(x)) => Ok(StorageInstanceId::User(x)),
            None => Err(TryFromProtoError::missing_field(
                "ProtoStorageInstanceId::kind",
            )),
        }
    }
}
