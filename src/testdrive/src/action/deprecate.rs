// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::action::{ControlFlow, State};
use crate::parser::BuiltinCommand;

pub fn deprecate(mut cmd: BuiltinCommand, state: &mut State) -> Result<ControlFlow, anyhow::Error> {
    let deprecate = cmd.args.string("from-version")?;
    cmd.args.done()?;
    state.deprecate = Some(deprecate);
    Ok(ControlFlow::Continue)
}
