---
source: src/expr/src/scalar/like_pattern.rs
revision: ddfb7ab6ee
---

# mz-expr::scalar::like_pattern

Implements SQL `LIKE` and `ILIKE` pattern matching via a compiled `Matcher` type.
Patterns are parsed into a chain of `Subpattern` structs (wildcard counts + literal suffix); simple patterns use direct string search while case-insensitive patterns, patterns with more than `MAX_SUBPATTERNS` (5) subpatterns, or patterns with more than one `%` subpattern whose suffix is non-empty (a backtracking `%`) fall back to a compiled `Regex` to avoid super-linear backtracking. A trailing `%` with an empty suffix short-circuits without backtracking and does not count toward this limit, so common "contains" patterns like `%foo%` remain on the fast string matcher.
Exports `compile`, `normalize_pattern`, `EscapeBehavior`, and `Matcher`; the 8 KiB pattern size limit matches SQL Server behavior.
