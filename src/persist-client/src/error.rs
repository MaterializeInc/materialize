// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Errors for the crate

/// An error coming from an underlying durability system (e.g. s3) or from
/// invalid data received from one.
#[derive(Debug)]
pub struct LocationError {
    inner: anyhow::Error,
}

impl std::fmt::Display for LocationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "timeout: {}", self.inner)
    }
}

impl std::error::Error for LocationError {}

/// An error resulting from invalid usage of the API.
#[derive(Debug)]
pub struct InvalidUsage(pub anyhow::Error);

impl std::fmt::Display for InvalidUsage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid usage: {}", self.0)
    }
}

impl std::error::Error for InvalidUsage {}
