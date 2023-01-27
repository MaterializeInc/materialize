// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `EXPLAIN ... AS TEXT` support for FastPathPlan structures.

use std::fmt;

use mz_ore::str::Indent;
use mz_repr::explain_new::{DisplayText, ExprHumanizer};

use crate::{coord::peek::FastPathPlan, explain_new::Displayable};

impl<'a, C> DisplayText<C> for Displayable<'a, FastPathPlan>
where
    C: AsMut<Indent> + AsRef<&'a dyn ExprHumanizer>,
{
    fn fmt_text(&self, f: &mut fmt::Formatter<'_>, ctx: &mut C) -> fmt::Result {
        self.0.fmt_text(f, ctx)
    }
}
