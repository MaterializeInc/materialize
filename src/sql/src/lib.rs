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

mod kafka_util;

pub use sql_parser::ast;
pub mod catalog;
pub mod names;
pub mod normalize;
pub mod parse;
pub mod plan;
pub mod pure;
pub mod session;

#[macro_export]
macro_rules! unsupported {
    ($feature:expr) => {
        bail!("{} not yet supported", $feature)
    };
    ($issue:expr, $feature:expr) => {
        bail!("{} not yet supported, see https://github.com/MaterializeInc/materialize/issues/{} for more details", $feature, $issue)
    };
}
