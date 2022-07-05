// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! DOT format `EXPLAIN` support for `QGM` structures.

use mz_repr::explain_new::DisplayDot;

/// A very naive way of representing a
/// [`mz_repr::explain_new::ExplainFormat::Dot`] explanation for an
/// [`mz_sql::query_model::Model`].
///
/// Because `mz_sql::query_model::dot::DotGenerator::generate` is
/// mutable, we cannot use `DotGenerator` as an DOT explanation type
/// for QGM graphs and call `generate` from within its [`DisplayDot`]
/// implementation.
///
/// To accomodate for the current code structure, we therefore generate
/// the DOT output in the [`mz_repr::explain_new::Explain::explain_dot`]
/// call for the [`mz_sql::query_model::Model`] implementation instead
/// and wrap the resulting string into this naive explanation type.
///
/// In the long term:
/// 1. `DotGenerator` should be moved to this package,
/// 2. The generate method should be moved to `DotFormat::fmt_dot,
/// 3. Attribute derivation should happen in the `explain_dot` method.
pub struct ModelDotExplanation(String);

impl From<String> for ModelDotExplanation {
    fn from(string: String) -> Self {
        ModelDotExplanation(string)
    }
}

impl DisplayDot for ModelDotExplanation {
    fn fmt_dot(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
