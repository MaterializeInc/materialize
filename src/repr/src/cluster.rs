// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::adt::interval::Interval;

#[derive(Copy, Clone, Debug, Default, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord)]
pub enum ClusterState {
    #[default]
    Active,
    Suspended,
}

impl ClusterState {
    pub fn as_str(&self) -> &'static str {
        match self {
            ClusterState::Active => "active",
            ClusterState::Suspended => "suspended",
        }
    }
}

#[derive(Copy, Clone, Debug, Default, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord)]
pub enum ClusterTransition {
    /// Cluster is not scheduled to be transitioned.
    #[default]
    Never,
    /// Cluster will be transitioned after the interval elapses.
    Interval(Interval),
    /// Cluster will be transitioned at a point in time.
    DateTime(DateTime<Utc>),
}
