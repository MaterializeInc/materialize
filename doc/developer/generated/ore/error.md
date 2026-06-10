---
source: src/ore/src/error.rs
revision: 6dc0b37208
---

# mz-ore::error

Provides `ErrorExt`, a blanket extension trait on `std::error::Error` that adds `display_with_causes` and `to_string_with_causes` methods for rendering an error together with its full `source` chain.

The formatting is handled by `ErrorChainFormatter`, which iterates `Error::source` and joins each cause with `": "`, matching the conventional `"outer: inner: root"` style.
`ErrorExt` is automatically implemented for every `Error` type, including `Arc<dyn Error + Send + Sync>`, making it suitable for use with the structured error types common in the planner.
