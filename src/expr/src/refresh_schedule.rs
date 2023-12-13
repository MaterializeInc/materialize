// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use mz_repr::Timestamp;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RefreshSchedule {
    // `REFRESH EVERY`s
    pub everies: Vec<RefreshEvery>,
    // `REFRESH AT`s
    pub ats: Vec<Timestamp>,
}

impl RefreshSchedule {
    pub fn empty() -> RefreshSchedule {
        RefreshSchedule {
            everies: Vec::new(),
            ats: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RefreshEvery {
    pub interval: Duration,
    pub aligned_to: Timestamp,
}
