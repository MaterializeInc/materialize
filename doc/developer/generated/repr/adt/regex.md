---
source: src/repr/src/adt/regex.rs
revision: 243171af3f
---

# mz-repr::adt::regex

Defines a `Regex` newtype wrapping `regex::Regex` with `Serialize`/`Deserialize`, `PartialOrd`, `Hash`, and `Arbitrary` implementations needed to use compiled regexes as `Datum` values.
`Regex::non_nullable_capture_groups` returns a `BTreeSet<u32>` of capture group indices that unconditionally participate in every match of the regex (i.e. are non-optional). It parses the regex AST via `regex_syntax` and collects groups that are not wrapped in any alternation or optional/star/question repetition. This is used by encoding layers to infer non-nullable output columns for regex-based source formats.
