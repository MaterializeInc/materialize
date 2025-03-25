// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

/// Whether to use the constraint-based timestamp selection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ConstraintBasedTimestampSelection {
    Enabled,
    Disabled,
    Verify,
}

impl std::default::Default for ConstraintBasedTimestampSelection {
    fn default() -> Self {
        Self::Verify
    }
}

impl ConstraintBasedTimestampSelection {
    pub const fn const_default() -> Self {
        Self::Verify
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "enabled" => Self::Enabled,
            "disabled" => Self::Disabled,
            "verify" => Self::Verify,
            _ => {
                tracing::error!("invalid value for ConstraintBasedTimestampSelection: {}", s);
                ConstraintBasedTimestampSelection::default()
            }
        }
    }

    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Enabled => "enabled",
            Self::Disabled => "disabled",
            Self::Verify => "verify",
        }
    }
}
