---
source: src/ore/src/env.rs
revision: ec107156f8
---

# mz-ore::env

Provides `is_var_truthy`, a single function that reads an environment variable and returns `false` for missing, empty, `"0"`, or `"false"` values and `true` for anything else.
This is used by `assert::SOFT_ASSERTIONS` to determine the default state of soft assertions at process start.
