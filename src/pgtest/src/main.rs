// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{anyhow, Result};
use getopts::Options;

use std::env;

fn main() -> Result<()> {
    let args: Vec<_> = env::args().collect();

    let mut opts = Options::new();

    opts.optopt(
        "",
        "addr",
        "database address, default localhost:6875",
        "HOSTNAME:PORT",
    );
    opts.optopt("", "user", "database user, default materialize", "USERNAME");

    let usage_details = anyhow!(opts.usage("usage: pgtest [options] DIRECTORY"));
    let opts = match opts.parse(&args[1..]) {
        Ok(opts) => opts,
        Err(_) => return Err(usage_details),
    };
    if opts.free.len() != 1 {
        return Err(usage_details);
    }
    let directory = &opts.free[0];

    pgtest::walk(
        &opts
            .opt_str("addr")
            .unwrap_or_else(|| "localhost:6875".to_string()),
        &opts
            .opt_str("user")
            .unwrap_or_else(|| "materialize".to_string()),
        std::time::Duration::from_secs(5),
        directory,
    );

    Ok(())
}
