---
source: src/testdrive/src/action/set.rs
revision: e757b4d11b
---

# testdrive::action::set

Implements several `set`-prefixed builtin commands that mutate per-session state.
`run_regex_set` / `run_regex_unset` configure a regex pattern and replacement string applied to all SQL output before comparison.
Additional functions set the retry timeout, initial backoff, backoff factor, maximum tries, and maximum errors; read environment variables into testdrive variables; and append file content to testdrive variables.
