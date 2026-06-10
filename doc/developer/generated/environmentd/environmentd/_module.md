---
source: src/environmentd/src/environmentd.rs
revision: 7defc6a810
---

# environmentd::environmentd

Thin module that re-exports the `main` function from its `main` submodule and keeps `sys` (OS-level helpers) private to the module.
Serves as the crate's binary entry-point module.
