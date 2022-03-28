// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Errors for the crate

use mz_persist::location::SeqNo;

/// An error resulting from invalid usage of the API.
#[derive(Debug)]
pub struct InvalidUsage(pub anyhow::Error);

impl std::fmt::Display for InvalidUsage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid usage: {}", self.0)
    }
}

impl std::error::Error for InvalidUsage {}

/// A sentinel indicating that a state transition was a no-op.
#[derive(Debug)]
pub struct NoOp {
    /// The version of the state at which the input was evaluated to be a no-op.
    pub seqno: SeqNo,
}

impl std::fmt::Display for NoOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "no-op: {:?}", self.seqno)
    }
}

impl std::error::Error for NoOp {}
