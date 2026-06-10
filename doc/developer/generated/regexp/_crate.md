---
source: src/regexp/src/lib.rs
revision: 1125338d45
---

# mz-regexp

Provides `regexp_split_to_array`, a PostgreSQL-compatible implementation of `regexp_split_to_array` on top of `mz_repr::adt::regex::Regex`.
The implementation diverges from the standard `Regex::split` to match PostgreSQL's rule of ignoring zero-length matches at the start or end of the string and immediately after a previous match.
