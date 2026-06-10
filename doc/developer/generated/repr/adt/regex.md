---
source: src/repr/src/adt/regex.rs
revision: 931f79a1d8
---

# mz-repr::adt::regex

Defines a `Regex` newtype wrapping `regex::Regex` with `Serialize`/`Deserialize`, `PartialOrd`, `Hash`, and `Arbitrary` implementations needed to use compiled regexes as `Datum` values.
The struct stores `case_insensitive` and `dot_matches_new_line` flags alongside the compiled `regex::Regex`. Equality, ordering, and hashing are based on the pattern string plus these flags rather than language equivalence. Serialization is handled by a manual `Serialize`/`Deserialize` implementation that stores the pattern and flags as struct fields and reconstructs the compiled regex on deserialization, avoiding reliance on the underlying `regex::Regex` serialization.
