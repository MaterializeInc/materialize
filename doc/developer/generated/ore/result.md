---
source: src/ore/src/result.rs
revision: 1e3922ec98
---

# mz-ore::result

Provides the `ResultExt` trait, which extends `std::result::Result` with ergonomic helper methods.
The key additions are `err_into` (analogous to `Into`-based error conversion), `err_to_string_with_causes` and `map_err_to_string_with_causes` (which format the full error chain using `ErrorExt`), and `infallible_unwrap` (a safe unwrap for `Result<T, Infallible>`).
