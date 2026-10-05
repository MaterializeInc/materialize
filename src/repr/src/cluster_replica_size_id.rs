// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;

#[cfg(any(test, feature = "proptest"))]
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

/// The identifier for a cluster replica size.
///
/// System sizes come from the `--cluster-replica-sizes` flag.
#[derive(
    Clone,
    Copy,
    Debug,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    Serialize,
    Deserialize
)]
#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]
pub enum ClusterReplicaSizeId {
    System(u64),
    User(u64),
}

impl ClusterReplicaSizeId {
    pub fn is_system(&self) -> bool {
        matches!(self, Self::System(_))
    }
}

impl fmt::Display for ClusterReplicaSizeId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::System(id) => write!(f, "s{id}"),
            Self::User(id) => write!(f, "u{id}"),
        }
    }
}
