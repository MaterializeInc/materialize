---
source: src/expr/src/scalar/like_pattern.rs
revision: 1125338d45
---

# mz-expr::scalar::like_pattern

Implements SQL `LIKE` and `ILIKE` pattern matching via a compiled `Matcher` type.
Patterns are parsed into a chain of `Subpattern` structs (wildcard counts + literal suffix); simple patterns use direct string search while complex or case-insensitive patterns fall back to a compiled `Regex`.
Exports `compile`, `normalize_pattern`, `EscapeBehavior`, and `Matcher`; the 8 KiB pattern size limit matches SQL Server behavior.
