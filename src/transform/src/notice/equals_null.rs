// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Hosts [`EqualsNull`].

use std::collections::BTreeSet;
use std::fmt;

use mz_repr::GlobalId;
use mz_repr::explain::ExprHumanizer;

use crate::notice::{ActionKind, OptimizerNoticeApi};

/// A notice indicating that the query contains a comparison with NULL using `=` or `<>`.
///
/// Comparing a value to `NULL` using `=` or `<>` always results in `NULL`, not a boolean.
/// This is almost always a mistake. To check if a value is `NULL`, use `IS NULL` instead.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct EqualsNull;

impl OptimizerNoticeApi for EqualsNull {
    fn dependencies(&self) -> BTreeSet<GlobalId> {
        BTreeSet::new()
    }

    fn fmt_message(
        &self,
        f: &mut fmt::Formatter<'_>,
        _humanizer: &dyn ExprHumanizer,
        _redacted: bool,
    ) -> fmt::Result {
        write!(
            f,
            "Comparison with NULL using `=`, `<>`, or `!=` always evaluates to NULL."
        )
    }

    fn fmt_hint(
        &self,
        f: &mut fmt::Formatter<'_>,
        _humanizer: &dyn ExprHumanizer,
        _redacted: bool,
    ) -> fmt::Result {
        write!(f, "Use `IS NULL` or `IS NOT NULL` instead.")
    }

    fn fmt_action(
        &self,
        _f: &mut fmt::Formatter<'_>,
        _humanizer: &dyn ExprHumanizer,
        _redacted: bool,
    ) -> fmt::Result {
        Ok(())
    }

    fn action_kind(&self, _humanizer: &dyn ExprHumanizer) -> ActionKind {
        ActionKind::None
    }
}
