---
source: src/ore/src/path.rs
revision: e757b4d11b
---

# mz-ore::path

Adds a `clean` method to `std::path::Path` via the `PathExt` extension trait.
`clean` performs purely lexical path normalization — removing `.` components, resolving `..` against preceding components, collapsing redundant separators, and substituting `.` for empty paths — ported from Go's `path.Clean` function.
