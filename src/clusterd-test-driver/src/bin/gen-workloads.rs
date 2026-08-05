// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Generates the workload corpus, as one JSON file per workload plus a coverage
//! report.
//!
//! The corpus is committed so nightly runs are deterministic and a failure is
//! bisectable, but it is *generated* from the checked-in taxonomy rather than
//! hand-maintained. Re-running with the same arguments must reproduce it
//! byte-for-byte, which is what lets CI lint the committed corpus against a fresh
//! generation and catch drift.
//!
//! ```text
//! gen-workloads --out test/clusterd-test-driver/workloads [--seed N]
//!               [--max-draws N] [--patience N] [--no-config-matrix]
//! ```

use std::path::PathBuf;

use mz_clusterd_test_driver::generate::{
    STRATEGY_FLAGS, coverage_report, generate, pairwise_configs,
};

/// The default seed. Fixed, because the committed corpus must be reproducible.
const DEFAULT_SEED: u64 = 0x5EED;
/// How many candidates to draw before giving up on finding new coverage.
const DEFAULT_MAX_DRAWS: usize = 6000;
/// How many consecutive draws may add nothing before generation stops.
///
/// Set generously: greedy set cover's tail is long, and a small patience stops
/// while cells are still being found. The loop is cheap (lowering only, no
/// rendering), so over-drawing costs little.
const DEFAULT_PATIENCE: usize = 1500;

fn main() -> anyhow::Result<()> {
    let mut out: Option<PathBuf> = None;
    let mut seed = DEFAULT_SEED;
    let mut max_draws = DEFAULT_MAX_DRAWS;
    let mut patience = DEFAULT_PATIENCE;
    let mut config_matrix = true;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--out" => out = Some(PathBuf::from(next_arg(&mut args, "--out")?)),
            "--seed" => seed = next_arg(&mut args, "--seed")?.parse()?,
            "--max-draws" => max_draws = next_arg(&mut args, "--max-draws")?.parse()?,
            "--patience" => patience = next_arg(&mut args, "--patience")?.parse()?,
            "--no-config-matrix" => config_matrix = false,
            other => anyhow::bail!("unknown argument {other:?}"),
        }
    }
    let out = out.ok_or_else(|| anyhow::anyhow!("--out is required"))?;

    let configs = if config_matrix {
        pairwise_configs(STRATEGY_FLAGS)
    } else {
        Vec::new()
    };

    let corpus = generate(seed, max_draws, patience, &configs)?;

    // Clear any previously generated workloads, so a run that produces fewer
    // files does not leave stale ones behind to be executed.
    if out.exists() {
        for entry in std::fs::read_dir(&out)? {
            let path = entry?.path();
            if path.extension().is_some_and(|e| e == "json") {
                std::fs::remove_file(path)?;
            }
        }
    } else {
        std::fs::create_dir_all(&out)?;
    }

    for workload in &corpus.workloads {
        let path = out.join(format!("{}.json", workload.name));
        // Pretty-printed with a trailing newline: the corpus is committed, so it
        // has to be reviewable in a diff.
        let mut json = serde_json::to_string_pretty(workload)?;
        json.push('\n');
        std::fs::write(&path, json)?;
    }

    let report = coverage_report(&corpus);
    std::fs::write(out.join("COVERAGE.md"), &report)?;
    print!("{report}");
    println!(
        "wrote {} workloads to {}",
        corpus.workloads.len(),
        out.display()
    );
    Ok(())
}

fn next_arg(args: &mut impl Iterator<Item = String>, flag: &str) -> anyhow::Result<String> {
    args.next()
        .ok_or_else(|| anyhow::anyhow!("{flag} requires a value"))
}
