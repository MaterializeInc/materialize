// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! SQL-dataflow translation.

#![deny(missing_debug_implementations)]

macro_rules! unsupported {
    ($feature:expr) => {
        return Err(crate::plan::error::PlanError::Unsupported {
            feature: $feature.to_string(),
            issue_no: None,
        }
        .into());
    };
    ($issue:expr, $feature:expr) => {
        return Err(crate::plan::error::PlanError::Unsupported {
            feature: $feature.to_string(),
            issue_no: Some($issue),
        }
        .into());
    };
}

mod kafka_util;

pub mod ast;
pub mod catalog;
pub mod names;
pub mod normalize;
pub mod parse;
pub mod plan;
pub mod pure;
