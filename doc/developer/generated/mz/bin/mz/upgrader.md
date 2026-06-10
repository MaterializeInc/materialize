---
source: src/mz/src/bin/mz/upgrader.rs
revision: b61ba0aec7
---

# mz (bin)::upgrader

Implements `UpgradeChecker`, which checks for newer `mz` releases by fetching the latest version from `https://binaries.materialize.com/mz-latest.version` and comparing it against the current build version.
Uses a cache file (`~/.cache/.mz.ver`) to avoid checking more than once per day, and prints a warning via `OutputFormatter` when an upgrade is available.
