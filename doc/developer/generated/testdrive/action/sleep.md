---
source: src/testdrive/src/action/sleep.rs
revision: bffa995dc9
---

# testdrive::action::sleep

Implements the `sleep` and `random-sleep` builtin commands.
`run_sleep` blocks the current thread for a fixed duration specified via the `duration` argument (parsed with `humantime`).
`run_random_sleep` picks a uniformly random duration between zero and the specified maximum before sleeping.
