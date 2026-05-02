---
source: src/environmentd/src/environmentd/sys.rs
revision: f6f7d70cc4
---

# environmentd::environmentd::sys

Provides OS-level support utilities for `environmentd`: `adjust_rlimits` raises the open-file descriptor limit to the maximum allowed value, and signal handler helpers install handlers for `SIGUSR2` (to dump LLVM coverage profiles) and common termination signals (to flush coverage data before exiting).
