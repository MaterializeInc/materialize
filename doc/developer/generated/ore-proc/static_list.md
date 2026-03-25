---
source: src/ore-proc/src/static_list.rs
revision: e757b4d11b
---

# ore-proc::static_list

Implements the `#[static_list]` procedural macro attribute, which collects all `pub static` items of a specified type from an annotated module (and its nested submodules) into a single `&[&'static T]` slice declared at the call site.
The macro requires three arguments — `ty`, `name`, and `expected_count` — where `expected_count` acts as a compile-time smoke test that the number of collected items matches the author's expectation.
Internal helpers `StaticItem`, `StaticItems`, `StaticListArgs`, and `collect_items` handle recursive item traversal, argument parsing, and token generation.
