// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `EXPLAIN` support for `Mir` structures.

use std::fmt;
use std::fmt::Display;

use mz_expr::OptimizedMirRelationExpr;
use mz_repr::explain_new::DisplayText;

use crate::explain_new::common::{DataflowGraphFormatter, Explanation};

impl<'a> DisplayText for Explanation<'a, DataflowGraphFormatter<'a>, OptimizedMirRelationExpr> {
    type Context = ();
    fn fmt_text(&self, _ctx: &mut Self::Context, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt(f)
    }
}
