---
source: src/repr/src/adt/regex.rs
revision: 1125338d45
---

# mz-repr::adt::regex

Defines a `Regex` newtype wrapping `regex::Regex` with `Serialize`/`Deserialize`, `PartialOrd`, `Hash`, and `Arbitrary` implementations needed to use compiled regexes as `Datum` values.
