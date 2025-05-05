// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq)]
pub enum AuthenticatorKind {
    Frontegg,
    Password,
    None,
}

/// Whether to allow internal users (ie: mz_system) and/or normal users.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq)]
pub enum AllowedRoles {
    Normal,
    Internal,
    NormalAndInternal,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct HttpRoutesEnabled {
    pub base: bool,
    pub webhook: bool,
    pub internal: bool,
    pub metrics: bool,
    pub profiling: bool,
}
