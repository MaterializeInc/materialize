// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use structopt::StructOpt;

/// Verifies the correctness of a PostgreSQL-like server.
#[derive(StructOpt)]
struct Args {
    /// Database address.
    #[structopt(long, value_name = "HOSTNAME:PORT", default_value = "localhost:6875")]
    addr: String,
    /// Database user.
    #[structopt(long, value_name = "USERNAME", default_value = "materialize")]
    user: String,
    /// Directory containing test files.
    directory: String,
}

fn main() {
    let args: Args = ore::cli::parse_args();
    pgtest::walk(
        &args.addr,
        &args.user,
        std::time::Duration::from_secs(5),
        &args.directory,
    );
}
