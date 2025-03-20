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
use mz_proto::{RustType, TryFromProtoError};
use proptest::prelude::{Arbitrary, Strategy};
use proptest::strategy::BoxedStrategy;
use serde::{Deserialize, Serialize};
use tracing::error;

include!(concat!(env!("OUT_DIR"), "/mz_storage_types.instances.rs"));

/// Identifier of a storage instance.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum StorageInstanceId {
    /// A system storage instance.
    System(u64),
    /// A user storage instance.
    User(u64),
}

impl StorageInstanceId {
    /// Creates a new `StorageInstanceId` in the system namespace. The top 16 bits of `id` must be
    /// 0, because this ID is packed into 48 bits of
    /// [`mz_repr::GlobalId::IntrospectionSourceIndex`].
    pub fn system(id: u64) -> Option<Self> {
        Self::new(id, Self::System)
    }

    /// Creates a new `StorageInstanceId` in the user namespace. The top 16 bits of `id` must be
    /// 0, because this ID is packed into 48 bits of
    /// [`mz_repr::GlobalId::IntrospectionSourceIndex`].
    pub fn user(id: u64) -> Option<Self> {
        Self::new(id, Self::User)
    }

    fn new(id: u64, variant: fn(u64) -> Self) -> Option<Self> {
        const MASK: u64 = 0xFFFF << 48;
        const WARN_MASK: u64 = 1 << 47;
        if MASK & id == 0 {
            if WARN_MASK & id != 0 {
                error!("{WARN_MASK} or more `StorageInstanceId`s allocated, we will run out soon");
            }
            Some(variant(id))
        } else {
            None
        }
    }

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
            Some(System(x)) => StorageInstanceId::system(x).ok_or_else(|| {
                TryFromProtoError::InvalidPersistState(format!(
                    "{x} is not a valid StorageInstanceId"
                ))
            }),
            Some(User(x)) => StorageInstanceId::user(x).ok_or_else(|| {
                TryFromProtoError::InvalidPersistState(format!(
                    "{x} is not a valid StorageInstanceId"
                ))
            }),
            None => Err(TryFromProtoError::missing_field(
                "ProtoStorageInstanceId::kind",
            )),
        }
    }
}

impl Arbitrary for StorageInstanceId {
    type Parameters = ();

    fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
        const UPPER_BOUND: u64 = 1 << 47;
        (0..2, 0..UPPER_BOUND)
            .prop_map(|(variant, id)| match variant {
                0 => StorageInstanceId::System(id),
                1 => StorageInstanceId::User(id),
                _ => unreachable!(),
            })
            .boxed()
    }

    type Strategy = BoxedStrategy<StorageInstanceId>;
}
