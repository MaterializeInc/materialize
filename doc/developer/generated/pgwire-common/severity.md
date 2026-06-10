---
source: src/pgwire-common/src/severity.rs
revision: 7d6d670654
---

# mz-pgwire-common::severity

Defines the `Severity` enum covering all pgwire message severity levels: `Panic`, `Fatal`, `Error`, `Warning`, `Notice`, `Debug`, `Info`, `Log`.
Provides `is_error` (true for Panic/Fatal/Error), `is_fatal`, and `as_str` (returns the uppercase string used in protocol messages).
