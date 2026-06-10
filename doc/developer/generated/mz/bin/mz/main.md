---
source: src/mz/src/bin/mz/main.rs
revision: 30d929249e
---

# mz (bin)::main

Entry point for the `mz` binary: parses the top-level clap `Args` struct (global flags for config file, output format, color, region, profile), loads a `Context`, optionally runs the `UpgradeChecker`, and dispatches to the selected subcommand's `run` function.
