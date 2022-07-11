// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! JSON format `EXPLAIN` support for `Lir~` structures.

use std::fmt;
use std::fmt::Display;

use mz_compute_client::plan::Plan;
use mz_repr::explain_new::DisplayJson;

use crate::explain_new::common::{Explanation, JsonViewFormatter};

impl<'a> DisplayJson for Explanation<'a, JsonViewFormatter, Plan> {
    type Context = ();
    fn fmt_json(&self, _ctx: &mut Self::Context, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt(f)
    }
}
