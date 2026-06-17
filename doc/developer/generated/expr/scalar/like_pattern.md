---
source: src/expr/src/scalar/like_pattern.rs
revision: 7287deb6f7
---

# mz-expr::scalar::like_pattern

Implements SQL `LIKE` and `ILIKE` pattern matching via a compiled `Matcher` type.
Patterns are parsed into a chain of `Subpattern` structs (wildcard counts + literal suffix); simple patterns use direct string search while case-insensitive patterns, patterns with more than `MAX_SUBPATTERNS` (5) subpatterns, or patterns with more than one `%` wildcard (a `many` subpattern) fall back to a compiled `Regex` to avoid super-linear backtracking.
Exports `compile`, `normalize_pattern`, `EscapeBehavior`, and `Matcher`; the 8 KiB pattern size limit matches SQL Server behavior.
