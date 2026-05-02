---
source: src/persist/src/retry.rs
revision: bffa995dc9
---

# persist::retry

Provides `Retry` (configuration) and `RetryStream` (iterator of sleep durations) for exponential backoff with jitter and optional fixed initial sleep.
`Retry::persist_defaults` returns the standard persist retry configuration: 4 ms initial backoff, 2× multiplier, 16 s clamp.
Mirrors the structure of `mz_ore::retry` but is kept separate as a low-level building block within this crate.
