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
    /// Authenticate users using Frontegg.
    Frontegg,
    /// Authenticate users using internally stored password hashes.
    Password,
    /// Do not authenticate users. Trust they are who they say they are without verification.
    None,
}

/// Whether to allow internal users (ie: mz_system) and/or normal users.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq)]
pub enum AllowedRoles {
    /// Allow normal (ie: customer) users, but not internal (ie: mz_support/mz_system) users.
    Normal,
    /// Allow internal (ie: mz_support/mz_system) users, but not normal (ie: customer) users.
    Internal,
    /// Allow both normal and internal users.
    NormalAndInternal,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct HttpRoutesEnabled {
    /// Include the primary customer-facing endpoints, including the SQL APIs and static files.
    pub base: bool,
    /// Include the /api/webhook/** endpoints.
    pub webhook: bool,
    /// Include internal endpoints including promotion, catalog, coordinator, and internal-console
    /// APIs.
    pub internal: bool,
    /// Include metrics and liveness/readiness probe endpoints.
    pub metrics: bool,
    /// Include /prof/ endpoint, and enable profiling in the / endpoint (included in base).
    pub profiling: bool,
}
