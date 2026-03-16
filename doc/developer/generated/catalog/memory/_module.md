---
source: src/catalog/src/memory.rs
revision: 136fbaed84
---

# catalog::memory

Groups the in-memory catalog state into two submodules: `objects` (rich Rust types for all catalog entities) and `error` (error types for in-memory constraint violations).
This module's types are the primary interface for code outside the `catalog` crate; durable types from `crate::durable::objects` should not be accessed directly by consumers.
