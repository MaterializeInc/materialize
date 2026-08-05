// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Dumps a corpus to disk, as one JSON file per workload plus a coverage report.
//!
//! Runs do not use this: `headless-driver` generates the corpus in process from a
//! seed. This exists to *look at* what a seed produces, which is what you want
//! when a workload fails and you need its plan and inputs in a readable form, or
//! when checking what a generator change did to coverage. The written files are a
//! debugging artifact, not an input, so nothing has to keep them in step.
//!
//! ```text
//! gen-workloads --out test/clusterd-test-driver/workloads [--seed N]
//!               [--max-draws N] [--patience N] [--no-config-matrix]
//!               [--soak N]
//! ```
//!
//! `--soak N` dumps what a soak run of `N` draws from `--seed` would execute,
//! which is how a soak failure gets from a seed in a CI log to a plan you can
//! read.

use std::path::PathBuf;

use mz_clusterd_test_driver::generate::{
    DEFAULT_MAX_DRAWS, DEFAULT_PATIENCE, DEFAULT_SEED, STRATEGY_FLAGS, coverage_report, generate,
    pairwise_configs, soak_corpus,
};

fn main() -> anyhow::Result<()> {
    let mut out: Option<PathBuf> = None;
    let mut seed = DEFAULT_SEED;
    let mut max_draws = DEFAULT_MAX_DRAWS;
    let mut patience = DEFAULT_PATIENCE;
    let mut config_matrix = true;
    let mut soak: Option<usize> = None;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--out" => out = Some(PathBuf::from(next_arg(&mut args, "--out")?)),
            "--seed" => seed = next_arg(&mut args, "--seed")?.parse()?,
            "--max-draws" => max_draws = next_arg(&mut args, "--max-draws")?.parse()?,
            "--patience" => patience = next_arg(&mut args, "--patience")?.parse()?,
            "--no-config-matrix" => config_matrix = false,
            "--soak" => soak = Some(next_arg(&mut args, "--soak")?.parse()?),
            other => anyhow::bail!("unknown argument {other:?}"),
        }
    }
    let out = out.ok_or_else(|| anyhow::anyhow!("--out is required"))?;

    let configs = if config_matrix {
        pairwise_configs(STRATEGY_FLAGS)
    } else {
        Vec::new()
    };

    let corpus = match soak {
        Some(count) => soak_corpus(seed, count)?,
        None => generate(seed, max_draws, patience, &configs)?,
    };

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
        // Pretty-printed: these are read by a person diagnosing a failure.
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
