// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Utilities for process-handling

use std::env;

use itertools::Itertools;
use shell_words::quote as escape;

/// Returns a human-readable version of how the current process was invoked.
///
/// Only env vars that start with `env_prefix` are included.
pub fn invocation(env_prefix: &str) -> String {
    env::vars_os()
        .map(|(name, value)| {
            (
                name.to_string_lossy().into_owned(),
                value.to_string_lossy().into_owned(),
            )
        })
        .filter(|(name, _value)| name.starts_with(env_prefix))
        .map(|(name, value)| format!("{}={}", escape(&name), escape(&value)))
        .chain(env::args().into_iter().map(|arg| escape(&arg).into_owned()))
        .join(" ")
}
