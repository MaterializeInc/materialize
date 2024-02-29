// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::action::ControlFlow;

pub fn run_skip_end() -> Result<ControlFlow, anyhow::Error> {
    Ok(ControlFlow::SkipEnd)
}
