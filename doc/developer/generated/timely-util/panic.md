---
source: src/timely-util/src/panic.rs
revision: 47704f13fe
---

# timely-util::panic

Provides `halt_on_timely_communication_panic`, which installs a panic hook that intercepts timely communication errors (identified by the `"timely communication error:"` message prefix) and downgrades them from panics to `halt!` calls.
This ensures that when one process in a timely cluster crashes, surviving workers cleanly terminate rather than producing misleading secondary panics.
