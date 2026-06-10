---
source: src/testdrive/src/action/skip_end.rs
revision: 18a20b3d96
---

# testdrive::action::skip_end

Implements the `skip-end` builtin command by returning `ControlFlow::SkipEnd`.
The main dispatch loop in `lib.rs` handles the transition out of skipping mode; this file is a trivial one-liner.
