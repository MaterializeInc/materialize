// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// Verifies the correctness of a Materialized coordinator.
#[derive(clap::Parser)]
struct Args {
    /// Directory containing test files.
    directory: String,
    /// Stress by running in a loop.
    #[clap(long)]
    stress: bool,
}

#[tokio::main]
async fn main() {
    let args: Args = ore::cli::parse_args();
    let mut iter = 0;
    loop {
        iter += 1;
        if iter > 1 {
            eprintln!("stress iteration {}", iter);
        }
        coordtest::walk(&args.directory).await;
        if !args.stress {
            break;
        }
    }
}
